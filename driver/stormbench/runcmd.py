# SPDX-License-Identifier: Apache-2.0
"""bench run: build the plan, optionally just print it, otherwise run the A/B blocks and summarise."""
from __future__ import annotations

import datetime as dt
import json
import os
import platform
from pathlib import Path

from . import __version__, config, preflight, report, variants as variants_mod
from .ab import Plan, Variant, run_blocks, schedule
from .config import REPO_ROOT
from .images import broker_tag
from .kube import LOADGEN_OVERLAYS, STACK_OVERLAYS, KubeBackend
from .runner import Runner


def image_prefix_for(target: str, given: str | None) -> str:
    """Where a target's cluster finds the broker images. local: the host's own images, `localhost/` (loaded with
    `images load-kind`). eks: the registry the images were pushed to, REGISTRY + "/" (a tag, so it follows the
    commit's short sha; the digest the pod really ran is recorded in the results)."""
    if given is not None:
        return given
    return "localhost/" if target == "local" else config.require("REGISTRY")["REGISTRY"].rstrip("/") + "/"


def arm_image(commit: str, prefix: str) -> str:
    """The image reference a commit's broker runs as. broker_tag also validates that the commit is a full sha."""
    return prefix + broker_tag(commit)


def make_plan(variants: tuple[Variant, ...], *, blocks: int, sizes: str, threads: int) -> Plan:
    return Plan(variants=variants, blocks=blocks, sizes=tuple(int(s) for s in sizes.split(",") if s.strip()), threads=threads)


def variants_from_args(args) -> tuple[Variant, ...]:
    """The run's variants: two commits (--a, --b), or two or more from a TOML file (--variants)."""
    prefix = image_prefix_for(args.target, args.image_prefix)
    if args.variants:
        if args.a or args.b:
            raise ValueError("give --variants or --a and --b, not both")
        return variants_mod.load(Path(args.variants), prefix)
    if not (args.a and args.b):
        raise ValueError("give --a and --b (two commits), or --variants FILE")
    return variants_mod.from_commits(args.a, args.b, args.a_label, args.b_label, prefix)


# How clients are authenticated in every run made from this code (deploy/stack/base). Recorded so that results are never confused with
# those of the earlier password-callout configuration, whose environment.json has no such key.
AUTH = {"mode": "operator", "resolver": "memory", "tenant_accounts": 1, "ticket": "signed by the AUTH account's scoped key, pre-signed before each run",
        "callout": "role-name mapper, user JWT valid 1 hour"}


def environment(plan: Plan, cluster: dict, settle_s: float, target: str = "local") -> dict:
    return {"target": target, "auth": AUTH,
            "date": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "variants": [v.describe() for v in plan.variants],
            "design": {"blocks": plan.blocks, "order": plan.order, "phases": [p.name for p in plan.phases],
                       "sizes": list(plan.sizes), "threads": plan.threads, "settle_s": settle_s,
                       "per_phase": {p.name: {"connections": p.connections, "runs": p.runs, "pause_ms": p.pause_ms, "hold_ms": p.hold_ms}
                                     for p in plan.phases}},
            "cluster": cluster,
            # Deliberately no host name, user name or machine model: results are meant to be published.
            "driver_host": {"system": platform.system(), "machine": platform.machine(), "cpus": os.cpu_count()},
            "driver_version": __version__}


def check_before_run(run: Runner, target: str, variants: tuple[Variant, ...], stack: str | None, loadgen: str | None) -> bool:
    """Run and print the preflight checks; True if all passed."""
    checks = preflight.preflight(run, target, variants, stack or STACK_OVERLAYS[target], loadgen or LOADGEN_OVERLAYS[target])
    print(f"preflight ({target}):\n{preflight.format_checks(checks)}", flush=True)
    return not preflight.failed(checks)


def execute_preflight(run: Runner, args) -> int:
    """bench preflight: the checks `run` makes first, without running anything."""
    variants = variants_from_args(args)
    if run.dry_run:
        print("bench preflight reads the cluster and the registry: skipped in a dry run")
        return 0
    return 0 if check_before_run(run, args.target, variants, args.stack_overlay, args.loadgen_overlay) else 1


def execute(run: Runner, args) -> int:
    plan = make_plan(variants_from_args(args), blocks=args.blocks, sizes=args.sizes, threads=args.threads)
    out = Path(args.out) if args.out else REPO_ROOT / "results" / f"{args.name}-{dt.date.today().isoformat()}"
    if run.dry_run:
        steps = list(schedule(plan))
        print(f"would write {out}: {plan.blocks} blocks x {len(plan.variants)} variants, {len(steps)} storms")
        for v in plan.variants:
            print(f"  variant {v.label}: {v.image}" + (f"  {json.dumps({k: x for k, x in v.settings().items() if k != 'conf'})}" if v.settings() else ""))
        for s in steps:
            print(f"  block {s.block}  {s.variant.label:<8} {s.label:<24} {s.phase.connections} x {s.phase.runs}")
        return 0
    if out.exists():
        print(f"bench: {out} already exists; pick another --name or --out", flush=True)
        return 1
    if not args.no_preflight and not check_before_run(run, args.target, plan.variants, args.stack_overlay, args.loadgen_overlay):
        print("bench: preflight failed: nothing was applied. Fix the lines marked FAIL, or pass --no-preflight to go ahead anyway.", flush=True)
        return 1
    with KubeBackend(run, target=args.target, stack=args.stack_overlay, loadgen=args.loadgen_overlay,
                     settle_s=args.settle_s) as backend:
        backend.prepare(plan.variants)
        out.mkdir(parents=True)
        (out / "environment.json").write_text(json.dumps(environment(plan, backend.describe(), args.settle_s, args.target), indent=1) + "\n")
        run_blocks(plan, backend, out / "raw")
    text = report.write(out)
    print("\n" + text)
    return 0
