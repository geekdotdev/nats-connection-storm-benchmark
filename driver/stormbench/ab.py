# SPDX-License-Identifier: Apache-2.0
"""The run loop, independent of Kubernetes: two or more variants, compared in blocks.

A variant is one configuration of the stack: a broker image and, optionally, a topology or resource overlay, extra broker
configuration, broker and callout settings, and generator settings (see Variant). For each block, each variant in turn gets a
freshly restarted broker and runs the same phases of storms; each phase's report is saved. The order rotates from block to
block, so no variant always goes first (with two variants that is alternating). The unit of replication is the block, as in
the analysis: `bench report` tests the block medians, not the bursts.

`run_blocks` only orchestrates. What a "fresh broker" and "a storm" mean is the Backend's business (see kube.py), so the
loop is tested with a fake one.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterator, Protocol

REPORT_PREFIX = "STORM_REPORT_JSON "


@dataclass(frozen=True)
class Variant:
    """One configuration under test. Only label and image are needed; everything else defaults to "as the target's overlay has it".
    variants.py reads and validates these from the command line or a TOML file."""
    label: str                    # used in file names: letters, digits, underscore
    commit: str                   # full nats-server commit sha the image was built from; "" if it was given as an image only
    image: str                    # the image reference the cluster runs for this variant
    stack: str = ""               # kustomize overlay under deploy/stack/overlays (topology, sizes): "" = the target's default
    conf: str = ""                # extra nats-server configuration, included after everything else (the broker's extra.conf)
    broker_env: dict = field(default_factory=dict)        # environment variables for the broker container
    broker_resources: dict = field(default_factory=dict)  # {"cpu": "2", "memory": "4Gi"}: requests and limits, equal
    callout_resources: dict = field(default_factory=dict)  # the callout's, the same way
    generator_resources: dict = field(default_factory=dict)  # the load generator Job's, the same way
    callout_env: dict = field(default_factory=dict)       # environment variables for the callout container
    callout_args: tuple = ()      # the callout's whole argument list, replacing the overlay's
    no_tls: bool = False          # a diagnostic: the broker's WebSocket listener without TLS and the generator on ws:// (the two are set together)
    callout_replicas: int = 0     # how many callout pods (they share one queue group, so they share the work); 0 = as the overlay has it (1)
    storm: dict = field(default_factory=dict)             # generator STORM_* variables, on top of the plan's

    def settings(self) -> dict:
        """What differs from the defaults, for the record in environment.json and the report."""
        out: dict = {}
        for name in ("stack", "conf", "broker_env", "broker_resources", "callout_resources", "generator_resources", "callout_env", "callout_args", "callout_replicas", "no_tls", "storm"):
            value = getattr(self, name)
            if value:
                out[name] = list(value) if isinstance(value, tuple) else value
        return out

    def describe(self) -> dict:
        return {"label": self.label, "commit": self.commit, "image": self.image, **self.settings()}

    @classmethod
    def from_dict(cls, d: dict) -> "Variant":
        """The inverse of describe(): a variant from its recorded form (the run state in the cluster keeps variants this way)."""
        known = {"label", "commit", "image", "stack", "conf", "broker_env", "broker_resources", "callout_resources", "generator_resources",
                 "callout_env", "callout_args", "callout_replicas", "no_tls", "storm"}
        if set(d) - known:
            raise ValueError(f"a recorded variant has unknown key(s) {sorted(set(d) - known)}: it was made by a different driver")
        return cls(label=d["label"], commit=d.get("commit", ""), image=d["image"], stack=d.get("stack", ""), conf=d.get("conf", ""),
                   broker_env=dict(d.get("broker_env", {})), broker_resources=dict(d.get("broker_resources", {})),
                   callout_resources=dict(d.get("callout_resources", {})), generator_resources=dict(d.get("generator_resources", {})),
                   callout_env=dict(d.get("callout_env", {})), callout_args=tuple(d.get("callout_args", ())), callout_replicas=int(d.get("callout_replicas", 0)),
                   no_tls=bool(d.get("no_tls", False)), storm=dict(d.get("storm", {})))


Arm = Variant  # the name this class had when a run compared exactly two commits


@dataclass(frozen=True)
class Phase:
    name: str
    connections: int
    runs: int
    pause_ms: int
    hold_ms: int


NO_TLS_URL = "ws://broker:8443"   # where the generator goes when a variant's broker listens without TLS (the broker Service's websocket port)


@dataclass
class Plan:
    variants: tuple[Variant, ...]
    blocks: int = 8
    sizes: tuple[int, ...] = (800, 1600)
    threads: int = 4
    phases: tuple[Phase, ...] = field(init=False)

    def __post_init__(self) -> None:
        if len(self.variants) < 2:
            raise ValueError("a run compares at least two variants")
        for v in self.variants:
            if not v.label.replace("_", "").isalnum():
                raise ValueError(f"variant label {v.label!r} must be letters, digits and underscores")
        if len({v.label for v in self.variants}) != len(self.variants):
            raise ValueError("the variants need different labels")
        if self.blocks < 1:
            raise ValueError("blocks must be at least 1")
        # Warm-up (not analysed), one connection at a time, then one storm phase per size.
        self.phases = (Phase("warm", 200, 3, 1500, 1000), Phase("idle", 1, 40, 100, 100),
                       *(Phase(f"n{n}", n, 8, 1500, 1000) for n in self.sizes))

    @property
    def order(self) -> str:
        return "alternating by block" if len(self.variants) == 2 else "rotating by block"

    def storm_env(self, phase: Phase, label: str, variant: Variant | None = None) -> dict[str, str]:
        env = {"STORM_CONNECTIONS": str(phase.connections), "STORM_RUNS": str(phase.runs),
               "STORM_PAUSE_MS": str(phase.pause_ms), "STORM_HOLD_MS": str(phase.hold_ms),
               "STORM_THREADS": str(self.threads), "STORM_LABEL": label, "STORM_JSON": "-"}
        if phase.name != "warm":
            env["STORM_SAMPLES"] = "1"  # every connection's timings, which is what the queue curves (queue.py) are drawn from
        if variant:
            env.update(variant.storm)  # the variant's own generator settings (variants.py keeps them off the phase variables)
            if variant.no_tls:
                env["STORM_URL"] = NO_TLS_URL   # the manifests' wss:// is reserved against a variant's own settings; this is the one way it changes
        return env


@dataclass(frozen=True)
class Step:
    block: int
    variant: Variant
    phase: Phase
    label: str  # <variant>-b<block>-<phase>


def order_in_block(variants: tuple[Variant, ...], block: int) -> tuple[Variant, ...]:
    """Block 1 runs the variants as listed, each later block starts one variant further along (a rotation). For two
    variants that is a, b, then b, a, then a, b."""
    k = (block - 1) % len(variants)
    return variants[k:] + variants[:k]


def schedule(plan: Plan) -> Iterator[Step]:
    for blk in range(1, plan.blocks + 1):
        for v in order_in_block(plan.variants, blk):
            for ph in plan.phases:
                yield Step(blk, v, ph, f"{v.label}-b{blk}-{ph.name}")


@dataclass(frozen=True)
class Unit:
    """One variant's turn in one block: a freshly restarted broker and every phase run on it. This is the unit a run is resumed
    in: a unit that finished is not repeated, and one that did not is redone from the broker restart, because its phases only
    mean something together (the same broker, from its first warm-up storm on)."""
    block: int
    variant: Variant
    steps: tuple[Step, ...]

    @property
    def key(self) -> str:
        return f"{self.variant.label}-b{self.block}"


def units(plan: Plan) -> Iterator[Unit]:
    """The schedule grouped into units, in the same order."""
    current: list[Step] = []
    for st in schedule(plan):
        if current and (current[0].block, current[0].variant.label) != (st.block, st.variant.label):
            yield Unit(current[0].block, current[0].variant, tuple(current))
            current = []
        current.append(st)
    if current:
        yield Unit(current[0].block, current[0].variant, tuple(current))


class Progress(Protocol):
    """What run_blocks tells whoever records the run's progress (state.RunState keeps it in the cluster)."""
    revision: str
    run_id: str

    def qualifiers(self) -> dict: ...
    def is_done(self, unit: Unit, raw: Path) -> bool: ...
    def running_record(self, unit: Unit) -> dict | None: ...
    def begin(self, unit: Unit) -> str: ...                       # returns the attempt's id
    def broker_up(self, unit: Unit, facts: dict) -> None: ...
    def beat(self) -> None: ...
    def finish(self, unit: Unit) -> None: ...
    def complete(self) -> None: ...


def _labelled(report: dict, progress: "Progress | None", unit: "Unit", st: "Step", cpu: dict | None = None, varz: list | None = None) -> dict:
    """A storm's report with what qualifies it added (the cluster kind, namespace, run id and start, and which unit and storm it is), so
    that the file says where it came from even when it is alone. A run with no progress record has nothing to add."""
    out = {**report, **({"cpu": cpu} if cpu else {}), **({"varz": varz} if varz else {})}
    if progress is None:
        return out
    return {**out, "qualifiers": {**progress.qualifiers(), "unit": unit.key, "storm": st.label}}


def _valid_json(path: Path) -> bool:
    try:
        json.loads(path.read_text())
        return True
    except (OSError, ValueError):
        return False


def rescue_unit(backend, raw: Path, unit: Unit, progress: Progress, log: Callable[[str], None]) -> bool:
    """Finish a unit that a stopped driver left running, by reading the storms that already ran for it from the cluster, and report
    whether that made it whole.

    A storm's Job (and so its log) stays on the cluster until the run is over, so a storm that finished, or was still running, when
    the driver stopped is not lost: its report is read now. That makes the unit whole only when nothing is missing and nothing is
    doubtful: the unit must have recorded the broker pod it ran on and that pod must still be the one running (a broker that was
    replaced since measured something else), and each missing phase must have a Job of this very attempt on the cluster. A phase that
    never started cannot be run now, on a broker that has been idle for minutes, and still be a phase of this unit: then the unit is
    run again from a fresh broker, as always."""
    rec = progress.running_record(unit)
    if not rec or not rec.get("attempt") or not rec.get("pod_uid"):
        return False
    if not (hasattr(backend, "collect_storm") and hasattr(backend, "broker_uid")):
        return False
    if not _valid_json(raw / f"{unit.key}.meta.json"):
        return False
    if backend.broker_uid() != rec["pod_uid"]:
        log(f"{unit.key}: the broker it was measured on has been replaced since: it will be run again")
        return False
    got = []
    for st in unit.steps:
        if _valid_json(raw / f"{st.label}.json"):
            continue
        text = backend.collect_storm(st.label, rec["attempt"])
        if text is None:
            log(f"{unit.key}: {st.phase.name} has no storm on the cluster from this attempt: the unit will be run again")
            return False
        try:
            report = parse_report(text)
        except StormFailed:
            log(f"{unit.key}: the log of {st.phase.name} has no report: the unit will be run again")
            return False
        (raw / f"{st.label}.txt").write_text(text)
        (raw / f"{st.label}.json").write_text(json.dumps(_labelled(report, progress, unit, st), indent=1) + "\n")
        got.append(st.phase.name)
    log(f"block {unit.block}, variant {unit.variant.label}: finished from the cluster"
        + (f" (read the storms of {', '.join(got)} that a stopped driver had left)" if got else " (every storm had been read already)"))
    return True


def clear_unit(raw: Path, unit: Unit) -> None:
    """Remove what an earlier, unfinished attempt at this unit left, so no file of it outlives the broker it was measured on."""
    for p in [raw / f"{unit.key}.meta.json", *raw.glob(f"{unit.key}-*.json"), *raw.glob(f"{unit.key}-*.txt")]:
        p.unlink(missing_ok=True)


class StormFailed(RuntimeError):
    pass


class Backend(Protocol):
    def start_variant(self, variant: Variant) -> dict:
        """Put the stack in the variant's configuration with a freshly restarted broker (and callout) running variant.image;
        return facts about it for the meta file."""

    def run_storm(self, label: str, env: dict[str, str]) -> str:
        """Run one storm Job with these STORM_* settings and return its log; raise StormFailed if it fails."""


def parse_report(log: str) -> dict:
    """The generator's JSON report: the last line that starts with REPORT_PREFIX."""
    lines = [ln for ln in log.splitlines() if ln.startswith(REPORT_PREFIX)]
    if not lines:
        raise StormFailed("the generator's log has no STORM_REPORT_JSON line (did it crash?)")
    return json.loads(lines[-1][len(REPORT_PREFIX):])


def runs_of(report: dict) -> list[dict]:
    """The per-burst reports: the generator nests them under "runs" when it made more than one."""
    return report["runs"] if "runs" in report else [report]


def run_blocks(plan: Plan, backend: Backend, raw: Path, *, sleep: Callable[[float], None] | None = None,
               now: Callable[[], str] = lambda: time.strftime("%Y-%m-%dT%H:%M:%S%z"),
               log: Callable[[str], None] = print, progress: Progress | None = None) -> None:
    """Run every unit of the plan in schedule order. With a `progress`, a unit it says is done is skipped (the run is being
    resumed), an unfinished one is redone from the start, and each unit's start, heartbeat and end are reported to it."""
    sleep = sleep or time.sleep  # looked up here, not when the module loads, so a test that replaces time.sleep is obeyed
    raw.mkdir(parents=True, exist_ok=True)
    for unit in units(plan):
        v = unit.variant
        if progress and progress.is_done(unit, raw):
            log(f"block {unit.block}/{plan.blocks}, variant {v.label}: already done, skipped")
            continue
        if progress and rescue_unit(backend, raw, unit, progress, log):
            progress.finish(unit)
            continue
        log(f"block {unit.block}/{plan.blocks}, variant {v.label}: fresh broker on {v.image}")
        attempt = ""
        if progress:
            clear_unit(raw, unit)
            attempt = progress.begin(unit)
            if hasattr(backend, "clear_unit_jobs"):
                backend.clear_unit_jobs(unit.key)   # an earlier attempt's storms are not this unit's
        if hasattr(backend, "set_context"):
            backend.set_context(progress.run_id if progress else "", unit.key, attempt)
        facts = backend.start_variant(v)
        meta = {"variant": v.label, "block": unit.block, "commit": v.commit, "image": v.image, "time": now(),
                **({"driver_revision": progress.revision, "run_id": progress.run_id,
                    "qualifiers": {**progress.qualifiers(), "unit": unit.key}} if progress else {}), **facts}
        (raw / f"{unit.key}.meta.json").write_text(json.dumps(meta, indent=1) + "\n")
        if progress:
            progress.broker_up(unit, facts)
        for st in unit.steps:
            env = plan.storm_env(st.phase, st.label, v)
            log(f"  {st.label}: {st.phase.connections} connections x {st.phase.runs}")
            text = backend.run_storm(st.label, env)
            (raw / f"{st.label}.txt").write_text(text)
            (raw / f"{st.label}.json").write_text(
                json.dumps(_labelled(parse_report(text), progress, unit, st, getattr(backend, "last_cpu", None), getattr(backend, "last_varz", None)),
                          indent=1) + "\n")
            sleep(2)
            if progress:
                progress.beat()
        if progress:
            progress.finish(unit)
    (raw / "DONE").write_text(now() + "\n")
