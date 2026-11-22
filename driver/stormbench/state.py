# SPDX-License-Identifier: Apache-2.0
"""A run's state, kept in a ConfigMap in the cluster, so the driver can be stopped and started again in the middle of a run.

A run takes an hour or more, and the driver is code that changes while it runs: an improvement to the report, a fix for a
network blip. A Python process keeps the code it loaded when it started, so without this an improvement waits for the run to end,
or the run is thrown away. With it, the driver can be stopped at any point and started again with `bench run --resume`: it reads
what was done from the cluster, skips the finished units and carries on, with whatever code is on disk by then.

The ConfigMap `storm-run` in `storm-bench` has two keys, each one JSON document (who is driving the run is not here: it is a Kubernetes
Lease, below):

  plan.json      written once when the run starts: its name and results directory, the target and overlays, the variants in full, the
                 blocks, sizes and threads, and the design (phases and pauses). `--resume` rebuilds the plan from this, so it does not
                 need the original command line or variants file.
  progress.json  every unit (one variant's turn in one block: a fresh broker and all its phases) as running or done, the driver
                 revision that ran it, whether the run is complete, and when it was resumed.

The measurements stay on the machine running the driver (results/<name>-<date>/raw): the state only says how far the run got. A unit
the state calls done whose files are not all there is redone. A unit that was running when the driver stopped is redone too, from a
restarted broker: its phases only mean something on one broker from the first warm-up storm on, so none of an interrupted unit is kept.

The lease stops two drivers from running one run (they would delete each other's Jobs). It is a Kubernetes Lease (coordination.k8s.io/v1),
`storm-run` in `storm-bench`, next to the ConfigMap: spec.holderIdentity is the driver's token, spec.renewTime the last time it said so
(at every unit and storm), spec.leaseDurationSeconds is 300, and spec.leaseTransitions counts how often the holder changed. It is atomic: it
is created with `kubectl create` (which fails if it exists) and changed with `kubectl replace` carrying the resourceVersion it was read at
(which fails if anyone changed it since; a Lease that was deleted meanwhile is simply written again, which is harmless to a driver that was taking, renewing or releasing it anyway), so two drivers starting at once cannot both win. And it is owned: every renewal reads the Lease
first, and a driver that finds another holder has been taken over and stops (LeaseLost) instead of carrying on beside the new one. Another
driver refuses to start while the renewal is under five minutes old, unless told to take over. A driver that is stopped normally (or killed
with SIGTERM or Ctrl-C) releases it, so restarting at once needs nothing; after a kill -9 or a crash of the machine, wait five minutes or
pass --takeover (the old driver, if it is not dead after all, loses the Lease at its next renewal). The driver's clock decides whether a
renewal is stale: a machine whose clock is minutes off will misjudge, as with any lease.

The store is a small interface so the logic is tested without a cluster. Reads and writes that fail are retried like the job poll in
kube.py is (a dropped connection must not end a run); creating the ConfigMap is not retried, since a retry could race its own success. A
Lease write that fails for any reason but a conflict is retried too: if the first try did succeed, the retry meets a conflict, the caller reads
the Lease again, sees itself as the holder and writes again.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
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


class LeaseLost(LeaseHeld):
    """This driver held the run's Lease and no longer does: another driver took it over. It must stop."""


class LeaseConflict(RuntimeError):
    """A Lease write found the Lease changed (or missing, or already there) since it was read: nothing was written."""


@dataclasses.dataclass
class Lease:
    holder: str            # spec.holderIdentity: the driver's token, or "" once released
    renew: float           # spec.renewTime as epoch seconds
    duration: float = 300.0
    transitions: int = 0   # spec.leaseTransitions
    pid: int = 0           # annotation storm-bench/pid
    rv: str = ""           # metadata.resourceVersion: what a replace must still find


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
        self.leases = MemoryLeaseStore()

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
        self.leases.delete()


class MemoryLeaseStore:
    """A Lease with the real one's atomicity (create fails if it exists, replace fails if the resourceVersion moved), for tests."""
    def __init__(self) -> None:
        self.lease: Lease | None = None
        self._rv = 0

    def get(self) -> Lease | None:
        return None if self.lease is None else dataclasses.replace(self.lease)

    def create(self, lease: Lease) -> None:
        if self.lease is not None:
            raise LeaseConflict("the Lease already exists")
        self._rv += 1
        self.lease = dataclasses.replace(lease, rv=str(self._rv))

    def replace(self, lease: Lease, rv: str) -> None:
        if self.lease is None or self.lease.rv != rv:
            raise LeaseConflict("the Lease changed")
        self._rv += 1
        self.lease = dataclasses.replace(lease, rv=str(self._rv))

    def delete(self) -> None:
        self.lease = None


LEASE_NAME = "storm-run"


def _micro(epoch: float) -> str:
    return dt.datetime.fromtimestamp(epoch, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _epoch(text: str | None) -> float:
    return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() if text else 0.0


class KubeLeaseStore:
    """The Lease `storm-run` (coordination.k8s.io/v1) in the target's cluster. kubectl is run here, not through Runner, because telling a
    conflict from a network error needs kubectl's stderr, which Runner does not capture."""
    def __init__(self, target: str, *, retries: int = 60, wait_s: float = 10.0, sleep: Callable[[float], None] = time.sleep,
                 runner: Callable[..., subprocess.CompletedProcess] = subprocess.run) -> None:
        self.target, self.retries, self.wait_s, self.sleep, self.runner = target, retries, wait_s, sleep, runner

    def _kubectl(self, *args: str, input: str | None = None) -> subprocess.CompletedProcess:
        return self.runner(k8s.kubectl_command("-n", NAMESPACE, *args, target=self.target), input=input, text=True, capture_output=True, cwd=REPO_ROOT)

    def _try(self, what: str, args: tuple[str, ...], input: str | None = None, conflict: tuple[str, ...] = ()) -> str:
        for attempt in range(self.retries + 1):
            done = self._kubectl(*args, input=input)
            if done.returncode == 0:
                return done.stdout
            if any(c in done.stderr for c in conflict):
                raise LeaseConflict(done.stderr.strip().splitlines()[-1] if done.stderr.strip() else what)
            if attempt == self.retries:
                raise subprocess.CalledProcessError(done.returncode, args, done.stdout, done.stderr)
            print(f"bench: {what} failed (exit {done.returncode}); trying again in {self.wait_s:.0f} s ({attempt + 1}/{self.retries})", file=sys.stderr, flush=True)
            self.sleep(self.wait_s)
        raise AssertionError("unreachable")

    @staticmethod
    def _manifest(lease: Lease, rv: str | None) -> str:
        meta = {"name": LEASE_NAME, "namespace": NAMESPACE, "labels": {"app.kubernetes.io/part-of": "storm-bench"},
                "annotations": {"storm-bench/pid": str(lease.pid)}, **({"resourceVersion": rv} if rv else {})}
        spec = {"holderIdentity": lease.holder, "leaseDurationSeconds": int(lease.duration), "renewTime": _micro(lease.renew),
                "acquireTime": _micro(lease.renew), "leaseTransitions": lease.transitions}
        return json.dumps({"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": meta, "spec": spec})

    def get(self) -> Lease | None:
        out = self._try("reading the run's lease", ("get", "lease", LEASE_NAME, "-o", "json", "--ignore-not-found"))
        if not out.strip():
            return None
        doc = json.loads(out)
        spec = doc.get("spec", {})
        return Lease(holder=spec.get("holderIdentity") or "", renew=_epoch(spec.get("renewTime")), duration=float(spec.get("leaseDurationSeconds") or LEASE_S),
                     transitions=int(spec.get("leaseTransitions") or 0), pid=int(doc["metadata"].get("annotations", {}).get("storm-bench/pid", 0) or 0),
                     rv=doc["metadata"].get("resourceVersion", ""))

    def create(self, lease: Lease) -> None:
        self._try("creating the run's lease", ("create", "-f", "-"), self._manifest(lease, None), conflict=("AlreadyExists",))

    def replace(self, lease: Lease, rv: str) -> None:
        self._try("renewing the run's lease", ("replace", "-f", "-"), self._manifest(lease, rv), conflict=("Conflict", "has been modified", "NotFound"))

    def delete(self) -> None:
        self._try("deleting the run's lease", ("delete", "lease", LEASE_NAME, "--ignore-not-found"))


class KubeStore:
    """The ConfigMap `storm-run` in the target's cluster, through kubectl with the benchmark's own kubeconfig. Its Lease is `leases`."""
    def __init__(self, run: Runner, target: str, *, retries: int = 60, wait_s: float = 10.0,
                 sleep: Callable[[float], None] = time.sleep, leases: "MemoryLeaseStore | KubeLeaseStore | None" = None) -> None:
        self.run, self.target, self.retries, self.wait_s, self.sleep = run, target, retries, wait_s, sleep
        self.leases = leases or KubeLeaseStore(target, retries=retries, wait_s=wait_s, sleep=sleep)

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
        self.leases.delete()


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
                  started: str | None = None, github_user: str | None = None) -> dict:
    """The run's plan, as plan.json keeps it. `namespace` is where the run's components are (and this ConfigMap): recorded so that a copy
    of the document says so, and so that --resume can tell when the driver would look somewhere else."""
    return {"schema": SCHEMA, "run_id": run_id or new_run_id(), "name": name, "out": out, "target": target, "cluster_kind": qualify.cluster_kind(target), "namespace": namespace,
            **({"github_user": github_user} if github_user else {}),
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
        except (KeyError, ValueError) as e:
            raise StateError(f"the run state in the cluster is damaged ({e!r}); `bench run --discard-state` removes it") from e
        if self.plan.get("schema") != SCHEMA:
            raise StateError(f"the run state has schema {self.plan.get('schema')!r}, this driver reads {SCHEMA}: it was made by a "
                             "different driver; `bench run --discard-state` removes it")

    # -- making and finding it -----------------------------------------------------------------------------------
    @classmethod
    def create(cls, store: Store, plan_doc: dict, *, units: dict | None = None, now: Callable[[], float] = time.time,
               revision: str | None = None) -> "RunState":
        data = {"plan.json": json.dumps(plan_doc, indent=1), "progress.json": json.dumps({"units": units or {}, "complete": False, "resumes": []})}
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
                "namespace": self.plan.get("namespace") or NAMESPACE, "started": self.plan.get("started") or self.plan.get("created"),
                "github_user": self.plan.get("github_user") or qualify.NOT_RECORDED}

    # -- the lease -----------------------------------------------------------------------------------------------
    @property
    def lease(self) -> dict:
        """The Lease as the status shows it: {} when there is none, else its holder ('' once released), the holder's pid and when it last renewed."""
        held = self.store.leases.get()
        return {} if held is None else {"owner": held.holder, "pid": held.pid, "heartbeat": held.renew}

    def _check(self, held: Lease | None, takeover: bool) -> None:
        if held is None or takeover:
            return
        age = self.now() - held.renew
        if held.holder and held.holder != self.token and age < held.duration:
            raise LeaseHeld(f"another driver (token {held.holder}, pid {held.pid}) wrote to this run {age:.0f} s ago. If it is still "
                            f"running, stop it first. If it was killed, wait until that is {held.duration:.0f} s or pass --takeover.")

    def check_lease(self, takeover: bool = False) -> None:
        """Raise LeaseHeld when another driver renewed the run's Lease recently, unless taking over. Changes nothing."""
        self._check(self.store.leases.get(), takeover)

    def _mine(self, transitions: int) -> Lease:
        return Lease(self.token, self.now(), LEASE_S, transitions, os.getpid())

    def acquire(self, takeover: bool = False) -> None:
        """Take the Lease: create it, or replace it at the resourceVersion read when it is free, stale or being taken over. If anyone wrote it in
        between, read it again; three losses in a row mean another driver is starting at the same moment."""
        leases = self.store.leases
        for _ in range(3):
            held = leases.get()
            self._check(held, takeover)
            try:
                if held is None:
                    leases.create(self._mine(0))
                else:
                    leases.replace(self._mine(held.transitions + (0 if held.holder == self.token else 1)), held.rv)
                return
            except LeaseConflict:
                continue
        raise LeaseHeld("another driver took the run's Lease at the same moment: run `bench status`")

    def _renew(self) -> None:
        """Say the run is still being driven, and check that it still is by this driver: the Lease is read first, and another holder means this
        driver was taken over (--takeover from another machine, or a clock that made this one look dead) and must stop."""
        leases = self.store.leases
        for _ in range(3):
            held = leases.get()
            if held is not None and held.holder != self.token:
                raise LeaseLost(f"the run is no longer this driver's: driver {held.holder or '(none)'} (pid {held.pid}) holds it now"
                                + (" (it took the run over)" if held.holder else " (it was released)") + ". This driver stops so that two do not run one run.")
            try:
                if held is None:   # never acquired (a caller that did not take it first), or deleted: take it
                    leases.create(self._mine(0))
                else:
                    leases.replace(self._mine(held.transitions), held.rv)
                return
            except LeaseConflict:
                continue
        raise LeaseLost("could not renew the run's Lease: it kept changing under this driver")

    def release(self) -> None:
        """Hand the run back, so the next driver can start at once. Best effort: this runs while the driver is already ending, and only the
        holder releases (a driver that was taken over leaves the new holder's Lease alone)."""
        try:
            held = self.store.leases.get()
            if held is not None and held.holder == self.token:
                self.store.leases.replace(Lease("", self.now(), LEASE_S, held.transitions, os.getpid()), held.rv)
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
        self._renew()
        return attempt

    def broker_up(self, unit: Unit, facts: dict) -> None:
        """The broker pod this attempt runs on, once it is up: what a resumed run checks before it finishes the unit from the cluster."""
        self.progress["units"][unit.key]["pod_uid"] = facts.get("pod_uid", "")
        self._save_progress()

    def beat(self) -> None:
        self._renew()

    def finish(self, unit: Unit) -> None:
        rec = self.progress["units"][unit.key]
        rec.update(status="done", finished=_iso(self.now()))
        self._save_progress()
        self._renew()

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
             f"  started {p.get('started') or p['created']}, run id {p['run_id']}{', run by ' + p['github_user'] if p.get('github_user') else ''}, results in results/{p['github_user'] + '/' if p.get('github_user') else ''}{p['out']}, driver revision {p['driver_revision']}",
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
