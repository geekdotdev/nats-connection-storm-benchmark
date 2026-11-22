# SPDX-License-Identifier: Apache-2.0
"""Profiles of the broker (Go's pprof), harvested by the CPU probe and read at the end of a run.

The probe (components/cpuprobe) takes the broker's block profile every couple of seconds from inside the cluster and keeps the snapshots,
with their times and the pod's UID, until the run is over. The driver takes no part while the run goes on: when the run has completed it
reads the snapshots out of the probes (as it reads Prometheus's series) into raw/profile/<pod uid>/<time ms>.block.pb.gz, and
raw/profile/index.json lists them. A block profile is cumulative since the broker started, so the waits of a storm are the difference
between the last snapshot before it and the first after it (the storm's times are in its report: `startedAtMs`, `endedAtMs`).

nats-server serves pprof on `prof_port` and sets the block profile rate from `prof_block_rate`; both belong to a variant's extra config
(`conf_file`, see profile.conf). It never sets the mutex profile, so only the block profile is useful. Block profiling at rate 1 slows the
broker: a run with it on is for the profiles, and its timings are not compared with runs without it.

Most of a block profile is idle goroutines (a connection's write loop waits on a sync.Cond for as long as the connection lives), so
the summary counts only what matters for contention: waits in sync.Mutex and sync.RWMutex locks, and, separately, the wait for the auth
callout's reply. Which function waited is read from the whole stack (`go tool pprof -traces`), so "in addClient" means "in a stack that
passes through addClient".
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

from . import k8s
from .config import NAMESPACE
from .runner import Runner

PROBE_PORT = 9100
LOCK = re.compile(r"sync\.\(\*(RW)?Mutex\)\.(R)?Lock")
CALLOUT = re.compile(r"processClientOrLeafCallout")
FOCUS = (                   # name -> a regular expression over the frames of a stack
    ("accConnsUpdate", re.compile(r"accConnsUpdate|pushAccConnsUpdate")),
    ("addClient", re.compile(r"Account\)\.addClient")),
    ("removeClient", re.compile(r"Account\)\.removeClient")),
    ("createWSClient", re.compile(r"createWSClient")),
    ("authentication", re.compile(r"processClientOrLeafAuthentication")),
    ("processConnect", re.compile(r"client\)\.processConnect")),
    ("processPing", re.compile(r"client\)\.processPing")),
)


def is_measured(label: str) -> bool:
    """A burst storm (`<variant>-b<block>-n<connections>`), as opposed to the warm and idle phases."""
    return re.search(r"-n\d+$", label) is not None


# -- reading them out of the probes, when the run has completed -----------------------------------------------------------

def _proxy(pod: str, rest: str) -> str:
    return f"/api/v1/namespaces/{NAMESPACE}/pods/{pod}:{PROBE_PORT}/proxy/{rest}"


def _probe_pods(run: Runner, target: str) -> list[str]:
    out = run.output(k8s.kubectl_command("-n", NAMESPACE, "get", "pods", "-l", "app.kubernetes.io/name=cpuprobe", "-o", "json", target=target), quiet=True)
    return [p["metadata"]["name"] for p in json.loads(out)["items"]]


def clear(run: Runner, target: str) -> None:
    """Delete every probe's snapshots: a new run starts with none."""
    for pod in _probe_pods(run, target):
        run(k8s.kubectl_command("delete", "--raw", _proxy(pod, "profiles"), target=target))


def export(run: Runner, target: str, out: Path, *, say=print) -> list[dict]:
    """Copy every probe's snapshots into out/raw/profile/ (the files, as binary) and write index.json. Returns the snapshots."""
    folder = out / "raw" / "profile"
    kept: list[dict] = []
    for pod in _probe_pods(run, target):
        doc = json.loads(run.output(k8s.kubectl_command("get", "--raw", _proxy(pod, "profiles"), target=target), quiet=True))
        for snap in doc.get("profiles") or []:   # a probe with none says null: Go encodes an empty slice that way
            body = subprocess.run(k8s.kubectl_command("get", "--raw", _proxy(pod, f"profiles/{snap['file']}"), target=target),
                                  capture_output=True, check=True, timeout=120).stdout
            path = folder / snap["file"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            kept.append({**snap, "node": doc.get("node", "")})
    if kept:
        kept.sort(key=lambda s: s["atMs"])
        (folder / "index.json").write_text(json.dumps({"snapshots": kept}, indent=1) + "\n")
        say(f"kept {len(kept)} block profile snapshots of the broker from the probes in {folder}")
    return kept


# -- reading them ----------------------------------------------------------------------------------------------------------

_UNITS = {"ns": 1e-9, "us": 1e-6, "µs": 1e-6, "ms": 1e-3, "s": 1.0, "mins": 60.0, "min": 60.0, "hrs": 3600.0, "hr": 3600.0}
_VALUE = re.compile(r"^\s*([\d.]+)(ns|us|µs|ms|mins|min|hrs|hr|s)\s+(\S.*)$")
_SEP = "-----------+-------------------------------------------------------"


def parse_traces(text: str) -> list[tuple[float, list[str]]]:
    """`go tool pprof -traces` as (seconds, frames from the innermost out), one per distinct stack."""
    out: list[tuple[float, list[str]]] = []
    for block in text.split(_SEP)[1:]:
        lines = [l for l in block.splitlines() if l.strip()]
        m = _VALUE.match(lines[0]) if lines else None
        if m:
            out.append((float(m.group(1)) * _UNITS[m.group(2)], [m.group(3).strip(), *[l.strip() for l in lines[1:]]]))
    return out


def _traces(before: Path, after: Path) -> list[tuple[float, list[str]]]:
    done = subprocess.run(["go", "tool", "pprof", "-traces", "-sample_index=delay", f"-base={before}", str(after)],
                          capture_output=True, text=True, timeout=120, check=True)
    return parse_traces(done.stdout)


def waits(stacks: list[tuple[float, list[str]]]) -> dict:
    """The waits of a storm that bear on contention: total time in mutex locks, the share of it in each function of FOCUS (a stack that
    passes through two counts in both), and the time waiting for the callout's reply."""
    locks = [(s, f) for s, f in stacks if any(LOCK.search(x) for x in f)]
    return {"lock_s": sum(s for s, _ in locks),
            "in": {name: sum(s for s, f in locks if any(rx.search(x) for x in f)) for name, rx in FOCUS},
            "callout_s": sum(s for s, f in stacks if any(CALLOUT.search(x) for x in f) and not any(LOCK.search(x) for x in f))}


_PREFIX = "github.com/nats-io/nats-server/v2/server."
_PLUMBING = re.compile(r"^(sync\.|runtime\.|internal/)")


def short(frame: str) -> str:
    return frame.removeprefix(_PREFIX).replace(" (inline)", "")


def holder_of(frames: list[str]) -> str:
    """The function that held a lock others waited for, from a mutex-profile stack: the innermost frame that is not the lock's own Unlock
    (a mutex profile records the stack of the goroutine that releases)."""
    return short(next((f for f in frames if not _PLUMBING.match(f)), frames[0]))


def holders(stacks: list[tuple[float, list[str]]]) -> dict:
    """What a mutex profile says about a storm: every sample is time that waiters lost on a lock, charged to the code that released it.
    Returns the total, the time by releasing function, and the share through each function of FOCUS (a stack through two counts in both)."""
    by: dict[str, float] = {}
    for s, f in stacks:
        by[holder_of(f)] = by.get(holder_of(f), 0.0) + s
    return {"total_s": sum(s for s, _ in stacks), "by": dict(sorted(by.items(), key=lambda kv: -kv[1])),
            "in": {name: sum(s for s, f in stacks if any(rx.search(x) for x in f)) for name, rx in FOCUS}}


def storm_window(run_dir: Path, label: str) -> tuple[float, float] | None:
    """(start, end) in epoch seconds of a storm's bursts, from its report; None when the generator recorded no times."""
    try:
        runs = json.loads((run_dir / "raw" / f"{label}.json").read_text()).get("runs", [])
    except (OSError, ValueError):
        return None
    starts = [b["startedAtMs"] for b in runs if b.get("startedAtMs")]
    ends = [b["endedAtMs"] for b in runs if b.get("endedAtMs")]
    return (min(starts) / 1000, max(ends) / 1000) if starts and ends else None


def pick(snapshots: list[dict], start: float, end: float) -> tuple[dict, dict] | None:
    """The snapshots that bracket a storm: of one broker pod, the last at or before `start` and the first at or after `end`. A pod that was
    not there at both ends (the broker is replaced for every variant's turn) cannot give a difference."""
    best = None
    for uid in {s["uid"] for s in snapshots}:
        mine = [s for s in snapshots if s["uid"] == uid]
        before = max((s for s in mine if s["atMs"] / 1000 <= start), key=lambda s: s["atMs"], default=None)
        after = min((s for s in mine if s["atMs"] / 1000 >= end), key=lambda s: s["atMs"], default=None)
        if before and after and (best is None or after["atMs"] - before["atMs"] < best[1]["atMs"] - best[0]["atMs"]):
            best = (before, after)
    return best


def _pairs(run_dir: Path, kind: str = "block") -> list[tuple[str, Path, Path]]:
    """(storm, before, after) for every burst storm that can be read, in profiles of this kind (block or mutex)."""
    folder = run_dir / "raw" / "profile"
    out: list[tuple[str, Path, Path]] = []
    if (folder / "index.json").is_file():   # the snapshots the probes kept
        snaps = [s for s in json.loads((folder / "index.json").read_text())["snapshots"] if s.get("kind", "block") == kind]
        for path in sorted((run_dir / "raw").glob("*-n*.json")):
            label = path.stem
            window = storm_window(run_dir, label) if is_measured(label) else None
            got = pick(snaps, *window) if window else None
            if got:
                out.append((label, folder / got[0]["file"], folder / got[1]["file"]))
    if folder.is_dir() and kind == "block":  # profiles that the driver itself took around each storm (a run made before the probe took this over)
        for p in sorted(folder.glob("*.block.before.pb.gz")):
            label = p.name[: -len(".block.before.pb.gz")]
            if is_measured(label) and (folder / f"{label}.block.after.pb.gz").is_file():
                out.append((label, p, folder / f"{label}.block.after.pb.gz"))
    return out


def summarize(run_dir: Path) -> list[str]:
    """The report's profile section: for every burst storm that can be bracketed, the time goroutines waited on locks, where, and the time
    waiting for the callout. Empty when the run kept no profiles; says what is missing when `go` is not installed."""
    pairs = _pairs(run_dir)
    if not pairs:
        return []
    if not shutil.which("go"):
        return ["profile: block profiles were kept but `go` is not installed, so they cannot be read (go tool pprof): raw/profile/"]
    names = [n for n, _ in FOCUS]
    out = ["profile: seconds that goroutines spent waiting during each burst storm, summed over goroutines (so more than the storm's own length), "
           "from the broker's block profile. The broker ran with block profiling on: its timings are perturbed and not comparable with runs without it.",
           f"  {'storm':<22}{'locks':>9}{'callout':>9}   locks, by function: " + " ".join(f"{n:>14}" for n in names)]
    for label, b, a in pairs:
        try:
            w = waits(_traces(b, a))
        except (subprocess.SubprocessError, OSError) as e:
            out.append(f"  {label:<22}unreadable: {e}")
            continue
        cells = [f"{w['in'][n]:>8.1f} {100 * w['in'][n] / w['lock_s']:>4.0f}%" if w["lock_s"] else f"{'-':>14}" for n in names]
        out.append(f"  {label:<22}{w['lock_s']:>9.1f}{w['callout_s']:>9.1f}   {'':<19} " + " ".join(cells))
    out.append("  (locks: time in sync.Mutex and sync.RWMutex Lock calls; callout: time waiting for the auth callout's reply; "
               "a share is of the lock time, and a stack through two of the functions counts in both)")
    return out + _held(run_dir)


def _held(run_dir: Path) -> list[str]:
    """The mutex-profile half of the section: who held the locks that others waited for. Only brokers built with the mutex-profile patch
    have one (nats-server never turns it on), so a run without it has nothing here."""
    rows = []
    for label, b, a in _pairs(run_dir, "mutex"):
        try:
            h = holders(_traces(b, a))
        except (subprocess.SubprocessError, OSError) as e:
            rows.append(f"  {label:<22}unreadable: {e}")
            continue
        if h["total_s"] > 0:
            top = ", ".join(f"{name} {100 * s / h['total_s']:.0f}%" for name, s in list(h["by"].items())[:5])
            rows.append(f"  {label:<22}{h['total_s']:>9.1f}  {top}")
    if not rows:
        return []
    return ["", "held: seconds that waiters lost on locks, charged to the function that released the lock (the broker's mutex profile: "
            "a patched build), and the five functions that held the most",
            f"  {'storm':<22}{'lost s':>9}  held by", *rows]
