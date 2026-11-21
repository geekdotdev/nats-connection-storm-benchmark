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
    callout_env: dict = field(default_factory=dict)       # environment variables for the callout container
    callout_args: tuple = ()      # the callout's whole argument list, replacing the overlay's
    storm: dict = field(default_factory=dict)             # generator STORM_* variables, on top of the plan's

    def settings(self) -> dict:
        """What differs from the defaults, for the record in environment.json and the report."""
        out: dict = {}
        for name in ("stack", "conf", "broker_env", "broker_resources", "callout_env", "callout_args", "storm"):
            value = getattr(self, name)
            if value:
                out[name] = list(value) if isinstance(value, tuple) else value
        return out

    def describe(self) -> dict:
        return {"label": self.label, "commit": self.commit, "image": self.image, **self.settings()}


Arm = Variant  # the name this class had when a run compared exactly two commits


@dataclass(frozen=True)
class Phase:
    name: str
    connections: int
    runs: int
    pause_ms: int
    hold_ms: int


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


def run_blocks(plan: Plan, backend: Backend, raw: Path, *, sleep: Callable[[float], None] = time.sleep,
               now: Callable[[], str] = lambda: time.strftime("%Y-%m-%dT%H:%M:%S%z"),
               log: Callable[[str], None] = print) -> None:
    raw.mkdir(parents=True, exist_ok=True)
    started: set[tuple[int, str]] = set()
    for st in schedule(plan):
        key = (st.block, st.variant.label)
        if key not in started:
            started.add(key)
            log(f"block {st.block}/{plan.blocks}, variant {st.variant.label}: fresh broker on {st.variant.image}")
            meta = {"variant": st.variant.label, "block": st.block, "commit": st.variant.commit, "image": st.variant.image,
                    "time": now(), **backend.start_variant(st.variant)}
            (raw / f"{st.variant.label}-b{st.block}.meta.json").write_text(json.dumps(meta, indent=1) + "\n")
        env = plan.storm_env(st.phase, st.label, st.variant)
        log(f"  {st.label}: {st.phase.connections} connections x {st.phase.runs}")
        text = backend.run_storm(st.label, env)
        (raw / f"{st.label}.txt").write_text(text)
        (raw / f"{st.label}.json").write_text(json.dumps(parse_report(text), indent=1) + "\n")
        sleep(2)
    (raw / "DONE").write_text(now() + "\n")
