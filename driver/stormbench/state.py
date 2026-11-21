# SPDX-License-Identifier: Apache-2.0
"""A run's state, kept in a ConfigMap in the cluster, so the driver can be stopped and started again in the middle of a run.

A run takes an hour or more, and the driver is code that changes while it runs: an improvement to the report, a fix for a
network blip. A Python process keeps the code it loaded when it started, so without this an improvement waits for the run to end,
or the run is thrown away. With it, the driver can be stopped at any point and started again with `bench run --resume`: it reads
what was done from the cluster, skips the finished units and carries on, with whatever code is on disk by then.

The ConfigMap `storm-run` in `storm-bench` has three keys, each one JSON document:

  plan.json      written once when the run starts: its name and results directory, the target and overlays, the variants in full, the
                 blocks, sizes and threads, and the design (phases and pauses). `--resume` rebuilds the plan from this, so it does not
                 need the original command line or variants file.
  progress.json  every unit (one variant's turn in one block: a fresh broker and all its phases) as running or done, the driver
                 revision that ran it, whether the run is complete, and when it was resumed.
  lease.json     who is driving the run and when they last said so (see below).

The measurements stay on the machine running the driver (results/<name>-<date>/raw): the state only says how far the run got. A unit
the state calls done whose files are not all there is redone. A unit that was running when the driver stopped is redone too, from a
restarted broker: its phases only mean something on one broker from the first warm-up storm on, so none of an interrupted unit is kept.

The lease stops two drivers from running one run (they would delete each other's Jobs). A driver writes its token and the time at
every unit and storm; another one refuses to start while that is under five minutes old, unless told to take over. A driver that is
stopped normally (or killed with SIGTERM or Ctrl-C) releases it, so restarting at once needs nothing; after a kill -9 or a crash of the
machine, wait five minutes or pass --takeover.

The store is a small interface so the logic is tested without a cluster. Reads and writes that fail are retried like the job poll in
kube.py is (a dropped connection must not end a run); creating the ConfigMap is not retried, since a retry could race its own success.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Callable, Protocol

from . import k8s, qualify
from .ab import Plan, Unit, Variant
from .config import REPO_ROOT
from .kube import NAMESPACE
from .runner import Runner

CONFIGMAP = "storm-run"
SCHEMA = 1
LEASE_S = 300.0


class LeaseHeld(RuntimeError):
    pass


class StateError(RuntimeError):
    """The recorded state cannot be used (another schema, a damaged document) or there is none where one is needed."""


class Store(Protocol):
    def read(self) -> dict[str, str] | None: ...
    def create(self, data: dict[str, str]) -> None: ...
    def patch(self, data: dict[str, str]) -> None: ...
    def delete(self) -> None: ...


class MemoryStore:
    """For tests, and anywhere no cluster is wanted."""
    def __init__(self) -> None:
        self.data: dict[str, str] | None = None

    def read(self):
        return None if self.data is None else dict(self.data)

    def create(self, data):
        if self.data is not None:
            raise StateError("the run state already exists")
        self.data = dict(data)

    def patch(self, data):
        if self.data is None:
            raise StateError("the run state does not exist")
        self.data.update(data)

    def delete(self):
        self.data = None


class KubeStore:
    """The ConfigMap `storm-run` in the target's cluster, through kubectl with the benchmark's own kubeconfig."""
    def __init__(self, run: Runner, target: str, *, retries: int = 60, wait_s: float = 10.0,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.run, self.target, self.retries, self.wait_s, self.sleep = run, target, retries, wait_s, sleep

    def _k(self, *args: str) -> list[str]:
        return k8s.kubectl_command("-n", NAMESPACE, *args, target=self.target)

    def _try(self, what: str, fn: Callable[[], str]) -> str:
        for attempt in range(self.retries + 1):
            try:
                return fn()
            except subprocess.CalledProcessError as e:
                if attempt == self.retries:
                    raise
                print(f"bench: {what} failed (exit {e.returncode}); trying again in {self.wait_s:.0f} s ({attempt + 1}/{self.retries})",
                      file=sys.stderr, flush=True)
                self.sleep(self.wait_s)
        raise AssertionError("unreachable")

    def read(self) -> dict[str, str] | None:
        out = self._try("reading the run state", lambda: self.run.output(
            self._k("get", "configmap", CONFIGMAP, "-o", "json", "--ignore-not-found"), quiet=True))
        return (json.loads(out).get("data") or {}) if out.strip() else None

    def create(self, data: dict[str, str]) -> None:
        manifest = {"apiVersion": "v1", "kind": "ConfigMap",
                    "metadata": {"name": CONFIGMAP, "namespace": NAMESPACE, "labels": {"app.kubernetes.io/part-of": "storm-bench"}},
                    "data": data}
        self.run.output(self._k("create", "-f", "-"), input=json.dumps(manifest), quiet=True)

    def patch(self, data: dict[str, str]) -> None:
        # A merge patch replaces only the keys it names. quiet: the command is not echoed, so the run's log is not a stream of them.
        self._try("writing the run state", lambda: self.run.output(
            self._k("patch", "configmap", CONFIGMAP, "--type", "merge", "-p", json.dumps({"data": data})), quiet=True))

    def delete(self) -> None:
        self._try("deleting the run state", lambda: self.run.output(
            self._k("delete", "configmap", CONFIGMAP, "--ignore-not-found"), quiet=True))


def driver_revision() -> str:
    """The commit the driver and manifests are at, with `+dirty` when they have uncommitted changes: what code produced a unit's
    results, which matters once the code may change in the middle of a run. "unknown" outside a git checkout."""
    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=REPO_ROOT, text=True, capture_output=True, check=True).stdout.strip()
    try:
        rev = git("rev-parse", "--short=12", "HEAD")
        dirty = bool(git("status", "--porcelain", "--", "driver", "deploy", "components", "images"))
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"
    return rev + ("+dirty" if dirty else "")


def _iso(epoch: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(epoch))


# -- a unit's files in the results directory ---------------------------------------------------------------------------------------

def unit_complete(raw: Path, unit: Unit, *, say: bool = False) -> bool:
    """Whether every file a finished unit leaves (its meta file and one report per phase) is here and is JSON."""
    for n in [f"{unit.key}.meta.json", *(f"{st.label}.json" for st in unit.steps)]:
        try:
            json.loads((raw / n).read_text())
        except (OSError, ValueError):
            if say:
                print(f"bench: {unit.key} is recorded as done but {n} is missing or unreadable here (the measurements are kept on the "
                      "machine that ran them): it will be run again", file=sys.stderr)
            return False
    return True


def unit_problems(raw: Path, unit: Unit) -> list[str]:
    """What is wrong with a unit's measurements, in words: a burst in which not every connection was admitted. Said as facts: whether
    that is the broker's own doing or something done to it is for the person reading to judge."""
    out: list[str] = []
    for st in unit.steps:
        try:
            d = json.loads((raw / f"{st.label}.json").read_text())
        except (OSError, ValueError):
            continue
        bursts = d["runs"] if "runs" in d else [d]
        want = sum(b["connections"] for b in bursts)
        got = sum(b["outcomes"]["admitted"] for b in bursts)
        if got != want:
            reasons: dict[str, int] = {}
            for b in bursts:
                for r, n in b.get("failures", {}).items():
                    reasons[r] = reasons.get(r, 0) + n
            top = ", ".join(f"{r} x{n}" for r, n in sorted(reasons.items(), key=lambda kv: -kv[1])[:2])
            out.append(f"{st.label}: {got} of {want} connections admitted" + (f" ({top})" if top else ""))
    return out


def move_unit_files(raw: Path, unit: Unit, into: Path) -> list[str]:
    """Set a unit's files aside (into `into`, made if needed) and return their names. Nothing is deleted: what an unfinished or
    invalid attempt measured is kept where it can be looked at."""
    files = [p for p in [raw / f"{unit.key}.meta.json", *raw.glob(f"{unit.key}-*.json"), *raw.glob(f"{unit.key}-*.txt")] if p.exists()]
    if files:
        into.mkdir(parents=True, exist_ok=True)
    for p in files:
        p.rename(into / p.name)
    return [p.name for p in files]


def new_run_id() -> str:
    """The key that ties a run's state in the cluster to its results directory: written into both (the plan, environment.json, every
    unit's meta file, the charts), and checked when a run is resumed."""
    return uuid.uuid4().hex[:12]


def plan_document(plan: Plan, *, name: str, out: str, target: str, stack_overlay: str, loadgen_overlay: str, settle_s: float,
                  design: dict, revision: str, now: float, run_id: str | None = None, namespace: str = NAMESPACE,
                  started: str | None = None) -> dict:
    """The run's plan, as plan.json keeps it. `namespace` is where the run's components are (and this ConfigMap): recorded so that a copy
    of the document says so, and so that --resume can tell when the driver would look somewhere else."""
    return {"schema": SCHEMA, "run_id": run_id or new_run_id(), "name": name, "out": out, "target": target, "cluster_kind": qualify.cluster_kind(target), "namespace": namespace,
            "started": started or _iso(now),
            "stack_overlay": stack_overlay, "loadgen_overlay": loadgen_overlay, "settle_s": settle_s,
            "created": _iso(now), "driver_revision": revision,
            "blocks": plan.blocks, "sizes": list(plan.sizes), "threads": plan.threads,
            "variants": [v.describe() for v in plan.variants], "design": design}


def plan_from_document(doc: dict) -> Plan:
    return Plan(variants=tuple(Variant.from_dict(v) for v in doc["variants"]), blocks=doc["blocks"],
                sizes=tuple(doc["sizes"]), threads=doc["threads"])


class RunState:
    """The recorded state of a run, and what run_blocks reports to (it implements ab.Progress)."""

    def __init__(self, store: Store, data: dict[str, str], *, now: Callable[[], float] = time.time, revision: str | None = None) -> None:
        self.store, self.now = store, now
        self.revision = revision if revision is not None else driver_revision()
        self.token = uuid.uuid4().hex[:8]
        try:
            self.plan = json.loads(data["plan.json"])
            self.progress = json.loads(data["progress.json"])
            self.lease = json.loads(data.get("lease.json", "{}"))
        except (KeyError, ValueError) as e:
            raise StateError(f"the run state in the cluster is damaged ({e!r}); `bench run --discard-state` removes it") from e
        if self.plan.get("schema") != SCHEMA:
            raise StateError(f"the run state has schema {self.plan.get('schema')!r}, this driver reads {SCHEMA}: it was made by a "
                             "different driver; `bench run --discard-state` removes it")

    # -- making and finding it -----------------------------------------------------------------------------------
    @classmethod
    def create(cls, store: Store, plan_doc: dict, *, units: dict | None = None, now: Callable[[], float] = time.time,
               revision: str | None = None) -> "RunState":
        data = {"plan.json": json.dumps(plan_doc, indent=1), "progress.json": json.dumps({"units": units or {}, "complete": False, "resumes": []}),
                "lease.json": "{}"}
        store.create(data)
        return cls(store, data, now=now, revision=revision)

    @classmethod
    def load(cls, store: Store, *, now: Callable[[], float] = time.time, revision: str | None = None) -> "RunState | None":
        data = store.read()
        return None if data is None else cls(store, data, now=now, revision=revision)

    @property
    def run_id(self) -> str:
        return self.plan["run_id"]

    def qualifiers(self) -> dict:
        """What every file this driver writes for the run says about it (qualify.py). The namespace is the one this driver works in: a
        state from before it was recorded has none, and the driver that resumes it is operating in this one. The start is the run's
        (an adopted run's is the original's, not the time it was adopted)."""
        return {"run_id": self.run_id, "cluster": qualify.cluster_kind(self.plan.get("target")), "target": self.plan.get("target"),
                "namespace": self.plan.get("namespace") or NAMESPACE, "started": self.plan.get("started") or self.plan.get("created")}

    # -- the lease -----------------------------------------------------------------------------------------------
    def check_lease(self, takeover: bool = False) -> None:
        """Raise LeaseHeld when another driver wrote to the run recently, unless taking over. Changes nothing."""
        owner, beat = self.lease.get("owner", ""), self.lease.get("heartbeat", 0.0)
        age = self.now() - beat
        if owner and owner != self.token and age < LEASE_S and not takeover:
            raise LeaseHeld(f"another driver (token {owner}, pid {self.lease.get('pid')}) wrote to this run {age:.0f} s ago. If it is still "
                            f"running, stop it first. If it was killed, wait until that is {LEASE_S:.0f} s or pass --takeover.")

    def acquire(self, takeover: bool = False) -> None:
        self.check_lease(takeover)
        self._write_lease(self.token)

    def _write_lease(self, owner: str) -> None:
        self.lease = {"owner": owner, "pid": os.getpid(), "heartbeat": self.now(), "heartbeat_time": _iso(self.now())}
        self.store.patch({"lease.json": json.dumps(self.lease)})

    def release(self) -> None:
        """Hand the run back, so the next driver can start at once. Best effort: this runs while the driver is already ending."""
        try:
            if self.lease.get("owner") == self.token:
                self._write_lease("")
        except Exception as e:  # noqa: BLE001 -- the driver is stopping; a failure to say so must not hide why
            print(f"bench: could not release the run's lease ({e}); it expires in {LEASE_S:.0f} s", file=sys.stderr)

    # -- ab.Progress ---------------------------------------------------------------------------------------------
    def _save_progress(self) -> None:
        self.store.patch({"progress.json": json.dumps(self.progress)})

    def is_done(self, unit: Unit, raw: Path) -> bool:
        rec = self.progress["units"].get(unit.key)
        if not rec or rec.get("status") != "done":
            return False
        return unit_complete(raw, unit, say=True)

    def running_record(self, unit: Unit) -> dict | None:
        rec = self.progress["units"].get(unit.key)
        return rec if rec and rec.get("status") == "running" else None

    def begin(self, unit: Unit) -> str:
        """Start an attempt at the unit and return its id. The storms' Jobs carry it, so that a later driver reading Jobs off the cluster
        only ever takes those of the attempt that is recorded here."""
        attempt = uuid.uuid4().hex[:8]
        self.progress["units"][unit.key] = {"status": "running", "started": _iso(self.now()), "driver_revision": self.revision,
                                            "attempt": attempt}
        self._save_progress()
        self._write_lease(self.token)
        return attempt

    def broker_up(self, unit: Unit, facts: dict) -> None:
        """The broker pod this attempt runs on, once it is up: what a resumed run checks before it finishes the unit from the cluster."""
        self.progress["units"][unit.key]["pod_uid"] = facts.get("pod_uid", "")
        self._save_progress()

    def beat(self) -> None:
        self._write_lease(self.token)

    def finish(self, unit: Unit) -> None:
        rec = self.progress["units"][unit.key]
        rec.update(status="done", finished=_iso(self.now()))
        self._save_progress()
        self._write_lease(self.token)

    def complete(self) -> None:
        self.progress["complete"] = True
        self.progress["finished"] = _iso(self.now())
        self._save_progress()

    def note_resume(self, done: int) -> None:
        self.progress.setdefault("resumes", []).append({"time": _iso(self.now()), "driver_revision": self.revision, "units_done": done})
        self._save_progress()

    # -- reading it ----------------------------------------------------------------------------------------------
    @property
    def is_complete(self) -> bool:
        return bool(self.progress.get("complete"))

    def total_units(self) -> int:
        return self.plan["blocks"] * len(self.plan["variants"])

    def done_units(self) -> int:
        return sum(1 for r in self.progress["units"].values() if r.get("status") == "done")


def format_status(state: RunState) -> str:
    """The run's progress as text, for `bench status`."""
    p, pr = state.plan, state.progress
    total, done = state.total_units(), state.done_units()
    ns = f", namespace {p['namespace']}" if p.get("namespace") else ""   # a state written before the namespace was recorded has none
    lines = [f"run {p['name']!r} on {p['target']}{ns}: {done} of {total} units done" + ("  (COMPLETE)" if state.is_complete else ""),
             f"  started {p.get('started') or p['created']}, run id {p['run_id']}, results in results/{p['out']}, driver revision {p['driver_revision']}",
             f"  {p['blocks']} blocks x {len(p['variants'])} variants ({', '.join(v['label'] for v in p['variants'])}), sizes {p['sizes']}"]
    running = [k for k, r in pr["units"].items() if r.get("status") == "running"]
    if running:
        r = pr["units"][running[0]]
        lines.append(f"  running: {running[0]} since {r['started']} (revision {r['driver_revision']})")
    lease = state.lease
    if lease.get("owner"):
        age = state.now() - lease.get("heartbeat", 0)
        lines.append(f"  driver: token {lease['owner']}, pid {lease.get('pid')}, last wrote {age:.0f} s ago"
                     + ("" if age < LEASE_S else "  (stale: it has stopped)"))
    else:
        lines.append("  driver: none holds the run" + ("" if state.is_complete else " (stopped: `bench run --resume` continues it)"))
    for r in pr.get("resumes", []):
        lines.append(f"  resumed {r['time']} by revision {r['driver_revision']} with {r['units_done']} units done")
    revisions = sorted({r["driver_revision"] for r in pr["units"].values() if r.get("driver_revision")})
    if len(revisions) > 1:
        lines.append(f"  note: the units were run by {len(revisions)} different driver revisions: {', '.join(revisions)}")
    return "\n".join(lines)
