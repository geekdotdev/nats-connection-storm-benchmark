# SPDX-License-Identifier: Apache-2.0
"""bench run: build the plan, optionally just print it, otherwise run the A/B blocks and summarise."""
from __future__ import annotations

import contextlib
import datetime as dt
import hashlib
import json
import os
import platform
import shutil
import signal
import sys
import time
import tomllib
from pathlib import Path

from . import __version__, config, k8s, monitoring, notes, preflight, profiling, qualify, report, secrets_cmd, state as state_mod, variants as variants_mod
from .ab import Plan, Variant, run_blocks, schedule, units
from .config import REPO_ROOT
from .images import broker_tag
from .kube import LOADGEN_OVERLAYS, STACK_OVERLAYS, KubeBackend
from .runner import Runner
from .state import plan_from_document


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


def design(plan: Plan, settle_s: float) -> dict:
    """The measurement design: what every run of this plan does. Kept in the run state so a resumed run can check that the code on disk
    still does the same (a change of pauses or phases half way through a run would make its units incomparable)."""
    return {"blocks": plan.blocks, "order": plan.order, "phases": [p.name for p in plan.phases],
            "sizes": list(plan.sizes), "threads": plan.threads, "settle_s": settle_s,
            "per_phase": {p.name: {"connections": p.connections, "runs": p.runs, "pause_ms": p.pause_ms, "hold_ms": p.hold_ms}
                          for p in plan.phases}}


def runner_login(args) -> str:
    """The GitHub login of whoever runs this (--github-user, else GITHUB_USER), checked. The one thing a run refuses to start without that
    the cluster cannot supply: it says who the results belong to."""
    return qualify.github_user(getattr(args, "github_user", None) or os.environ.get("GITHUB_USER"))


def recorded_results_dir(doc: dict) -> Path:
    """Where a recorded run's results are: under the login that ran it, or, for a run recorded before logins were, results/<out>. The recorded
    login wins over whoever resumes it: the results belong to the one who started the run."""
    base = REPO_ROOT / "results"
    candidates = ([base / doc["github_user"] / doc["out"]] if doc.get("github_user") else []) + [base / doc["out"]]
    return next((c for c in candidates if (c / "environment.json").is_file()), candidates[0])


def results_dir(login: str, name: str, today: str | None = None) -> Path:
    """results/<login>/<name>-<date>: a person's runs are under their own login."""
    return REPO_ROOT / "results" / login / f"{name}-{today or dt.date.today().isoformat()}"


def environment(plan: Plan, cluster: dict, settle_s: float, target: str = "local", github_user: str | None = None) -> dict:
    return {"target": target, "cluster_kind": qualify.cluster_kind(target), "namespace": config.NAMESPACE, "auth": AUTH,
            **({"github_user": github_user} if github_user else {}),
            "date": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            "variants": [v.describe() for v in plan.variants],
            "design": design(plan, settle_s),
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


DEFAULT_BLOCKS, DEFAULT_SIZES, DEFAULT_THREADS, DEFAULT_SETTLE_S, DEFAULT_NAME = 8, "800,1600", 4, 8.0, "ab"
# The options that describe a run's plan. They have no default on the command line (the defaults above are applied here), so that
# `--resume`, which takes the plan from the run state, can refuse any of them being given.
PLAN_OPTIONS = ("a", "b", "variants", "blocks", "sizes", "threads", "settle_s", "image_prefix", "stack_overlay", "loadgen_overlay", "name")


def _terminate(signum, frame):  # noqa: ARG001
    raise SystemExit(128 + signum)


def use_monitoring(run: Runner, args) -> bool:
    """Whether this run reads CPU from the probes and has Prometheus keep the series: when they are installed, unless --no-monitoring.
    Said either way, so a run's CPU source is never a surprise (and is recorded in environment.json)."""
    if args.no_monitoring:
        print("monitoring: not used (--no-monitoring); CPU of the broker and callout is read by the driver, whole storms only", flush=True)
        return False
    if monitoring.installed(run, args.target):
        print("monitoring: the CPU probes and Prometheus are installed: the generator reads CPU from the probes at every burst and Prometheus "
              "keeps the series", flush=True)
        return True
    if args.target == "eks":
        # A run without them has no per-burst CPU for the broker and callout, no Prometheus series and no broker profiles: data that
        # cannot be had afterwards. On a cluster that costs money by the hour that is not a default to fall into by forgetting a command.
        raise ValueError("monitoring is not installed on this eks cluster, and a run without it has no per-burst CPU for the broker and callout, "
                         "no Prometheus series and no broker profiles. Install it with `bench --target eks monitoring up` (`bench cluster up` "
                         "does this itself), or pass --no-monitoring to run without it on purpose")
    print(f"monitoring: not installed (`bench --target {args.target} monitoring up` adds it); CPU of the broker and callout is read by the "
          "driver, whole storms only", flush=True)
    return False


def prepare_cluster(run: Runner, target: str, with_monitoring: bool = True) -> None:
    """What a new cluster needs before a run, done by `bench cluster up` so that it is not a list of commands to remember: the TLS Secret, and
    the CPU probes with Prometheus (after the same checks `bench monitoring up` makes: the nodes, the StorageClass, the images). A failure here
    leaves the cluster up (and billing): the message says so and what to run to finish."""
    names = secrets_cmd.create(run, target)
    print("created Secret " + ", ".join(names) + f" in namespace {config.NAMESPACE} (values not shown)", flush=True)
    if not with_monitoring:
        print("monitoring: not installed (--no-monitoring); runs on this cluster will need --no-monitoring too", flush=True)
        return
    checks = preflight.monitoring_checks(run, target, monitoring.overlay_for(target))
    print(f"preflight ({target}):\n{preflight.format_checks(checks)}", flush=True)
    if preflight.failed(checks):
        raise MonitoringNotInstalled(f"the cluster is up (and billing) but monitoring was not installed: fix what failed and run `bench --target {target} monitoring up`")
    monitoring.up(run, target)
    print(f"monitoring is up: the probes on every node, Prometheus scraping them every {monitoring.STEP_S} s", flush=True)


class MonitoringNotInstalled(RuntimeError):
    pass


def note_probe_fallback(out: Path) -> None:
    """The generator's image did not report the probes' readings, so the driver read the cgroups itself from the second storm on: the
    environment says what the CPU numbers are, instead of claiming the probes."""
    path = out / "environment.json"
    env = json.loads(path.read_text())
    env["cpu_source"] = "exec (the load generator's image predates the CPU probe, so its bursts have no probe readings; the first storm has no CPU for the broker and callout)"
    path.write_text(json.dumps(env, indent=1) + "\n")


def export_monitoring(run: Runner, target: str, out: Path, env: dict) -> None:
    """Take the run's series out of Prometheus into out/prometheus. Best effort: the run's own results are on disk already, and what
    Prometheus holds stays there, so a failure is said and the series can be exported later (`bench monitoring export`)."""
    try:
        start = dt.datetime.fromisoformat(env["date"]).timestamp() - 15
        monitoring.export(run, target, out, start, time.time() + 2)
    except Exception as e:  # noqa: BLE001
        print(f"bench: could not export Prometheus's series ({e}); `bench --target {target} monitoring export {out}` tries again", file=sys.stderr, flush=True)
    export_profiles(run, target, out)


def write_notes(out: Path) -> None:
    """The run's NOTES.md: the facts from environment.json written for it, and three headings left for what only a person knows. A run
    that was resumed has its generated block refreshed; what was written by hand is never touched."""
    try:
        made = notes.ensure(out)
        notes.refresh(out)
        if made:
            print(f"\nnotes: {made} has the run's facts; write its Purpose, What it showed and What perturbs it, and add a line for the run "
                  "to results/INDEX.md (CLAUDE.md: every run gets notes)", flush=True)
    except Exception as e:  # noqa: BLE001 -- the run's results are on disk; a notes file must not turn that into a failure
        print(f"bench: could not write {out / 'NOTES.md'} ({e})", file=sys.stderr, flush=True)


def clear_profiles(run: Runner, target: str) -> None:
    """A new run starts with none of an earlier run's broker profiles in the probes. Best effort."""
    try:
        profiling.clear(run, target)
    except Exception as e:  # noqa: BLE001
        print(f"bench: could not clear the probes' broker profiles ({e}); the run's own are told apart by time", file=sys.stderr, flush=True)


def export_profiles(run: Runner, target: str, out: Path) -> None:
    """Take the broker profiles the probes kept (only when a variant enabled the broker's profiler, so most runs have none) into
    out/raw/profile, and say in environment.json that the broker was profiled: its timings are then perturbed. Best effort."""
    try:
        kept = profiling.export(run, target, out)
        if kept:
            path = out / "environment.json"
            env = json.loads(path.read_text())
            env["profiling"] = {"block_profiles": len(kept), "taken_by": "the CPU probes, every few seconds", "pod": kept[0]["pod"],
                                "note": "block profiling slows the broker: the timings of this run are perturbed and not comparable with runs without it"}
            path.write_text(json.dumps(env, indent=1) + "\n")
    except Exception as e:  # noqa: BLE001
        print(f"bench: could not read the broker profiles from the probes ({e})", file=sys.stderr, flush=True)


def save_variants_file(src: Path, out: Path) -> dict:
    """Keep a copy of the variants file that made a run in its results directory, as `variants.toml`, with the `conf_file`s it names
    (under the same relative paths, so the copy loads by itself). It is provenance only: the driver reads the parsed variants in
    environment.json and the run state, never this copy. Returns what environment.json records about it: where it came from (the path
    inside the repository, else just the file name, so no home directory leaks) and its SHA-256."""
    data = src.read_bytes()
    (out / "variants.toml").write_bytes(data)
    copied, notes = [], []
    for v in tomllib.loads(data.decode()).get("variant", []):
        cf = v.get("conf_file")
        if not cf:
            continue
        rel = Path(cf)
        if rel.is_absolute() or ".." in rel.parts or not (src.parent / rel).is_file():
            notes.append(f"conf_file {cf} was not copied (absolute, outside the variants file's directory, or missing): its text is in the variants above")
            continue
        (out / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src.parent / rel, out / rel)
        copied.append(rel.as_posix())
    try:
        shown = src.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        shown = src.name
    return {"path": shown, "sha256": hashlib.sha256(data).hexdigest(), **({"conf_files": copied} if copied else {}),
            **({"notes": notes} if notes else {})}


@contextlib.contextmanager
def holding(rs: state_mod.RunState, takeover: bool):
    """Hold the run's lease while the block loop runs, and give it back however the loop ends, SIGTERM included (the default action
    of SIGTERM skips `finally`, so it is turned into an exit)."""
    rs.acquire(takeover)
    previous = None
    try:
        previous = signal.signal(signal.SIGTERM, _terminate)
    except ValueError:  # not the main thread
        pass
    try:
        yield
    except state_mod.LeaseLost:
        raise  # another driver holds the run now: its message says so, and this one has nothing to hand back
    except BaseException:
        # Whatever ended the loop (a failed storm, a network that stayed down, Ctrl-C, SIGTERM), the run is still recorded.
        print(f"bench: the run is recorded in the cluster: `bench --target {rs.plan['target']} run --resume` continues it "
              "(add --takeover if this driver is killed rather than stopped)", file=sys.stderr, flush=True)
        raise
    finally:
        if previous is not None:
            signal.signal(signal.SIGTERM, previous)
        rs.release()


def execute(run: Runner, args, *, make_store=None, backend_factory=None) -> int:
    """bench run. `make_store(target)` and `backend_factory` are for tests; they default to the cluster's. The backend is looked up when
    this runs, not when the module loads, so that a test which replaces runcmd.KubeBackend is obeyed (a default argument would have
    kept the real class, and the test would have run against a real cluster)."""
    make_store = make_store or (lambda target: state_mod.KubeStore(run, target))
    backend_factory = backend_factory or KubeBackend
    if args.resume and args.discard_state:
        raise ValueError("--resume continues the recorded run and --discard-state throws it away: give one")
    if args.resume:
        given = [o for o in PLAN_OPTIONS if getattr(args, o, None) is not None]
        if given:
            raise ValueError("--resume takes the plan from the run's state in the cluster; drop " + ", ".join("--" + g.replace("_", "-") for g in given))
        if run.dry_run:
            print("bench run --resume reads the recorded run from the cluster: skipped in a dry run")
            return 0
        return _resume(run, args, make_store, backend_factory)
    return _start(run, args, make_store, backend_factory)


def _start(run: Runner, args, make_store, backend_factory) -> int:
    settle_s = DEFAULT_SETTLE_S if args.settle_s is None else args.settle_s
    plan = make_plan(variants_from_args(args), blocks=DEFAULT_BLOCKS if args.blocks is None else args.blocks,
                     sizes=args.sizes or DEFAULT_SIZES, threads=DEFAULT_THREADS if args.threads is None else args.threads)
    name = args.name or DEFAULT_NAME
    login = runner_login(args)
    out = Path(args.out) if args.out else results_dir(login, name)
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
    store = make_store(args.target)
    existing = state_mod.RunState.load(store)
    if existing and not existing.is_complete:
        if not args.discard_state:
            print(f"bench: a run is recorded in this cluster and is not finished:\n{state_mod.format_status(existing)}\n"
                  "bench: `bench run --resume` continues it; `bench run --discard-state ...` forgets it and starts this run "
                  "(its measurements stay where they are)", flush=True)
            return 1
        existing.check_lease(args.takeover)  # never discard the state under a driver that is still running
    if not args.no_preflight and not check_before_run(run, args.target, plan.variants, args.stack_overlay, args.loadgen_overlay):
        print("bench: preflight failed: nothing was applied. Fix the lines marked FAIL, or pass --no-preflight to go ahead anyway.", flush=True)
        return 1
    probes = use_monitoring(run, args)
    with backend_factory(run, target=args.target, stack=args.stack_overlay, loadgen=args.loadgen_overlay, settle_s=settle_s,
                         read_cpu=not args.no_cpu and not probes, cpuprobe=probes, read_varz=args.varz) as backend:
        backend.prepare(plan.variants)
        if hasattr(backend, "delete_storm_jobs"):
            backend.delete_storm_jobs()  # the storms of an earlier run, and the single Job older drivers made: a new run starts with none
        if probes:
            clear_profiles(run, args.target)
        out.mkdir(parents=True)
        run_id, revision = state_mod.new_run_id(), state_mod.driver_revision()
        env = {**environment(plan, backend.describe(), settle_s, args.target, login), "run_id": run_id, "driver_revision": revision,
               "cpu_source": "probe" if probes else ("exec" if not args.no_cpu else "none"),
               **({"monitoring": {"probe": True, "prometheus": True, "scrape_interval_s": monitoring.STEP_S}} if probes else {})}
        if args.variants:
            env["variants_file"] = save_variants_file(Path(args.variants), out)
        (out / "environment.json").write_text(json.dumps(env, indent=1) + "\n")
        if existing:
            store.delete()  # a finished run's state, or the one being discarded
        rs = state_mod.RunState.create(store, state_mod.plan_document(
            plan, name=name, out=out.name, target=args.target, stack_overlay=args.stack_overlay or STACK_OVERLAYS[args.target],
            loadgen_overlay=args.loadgen_overlay or LOADGEN_OVERLAYS[args.target], settle_s=settle_s, design=env["design"],
            revision=revision, now=time.time(), run_id=run_id, started=env["date"], github_user=login))
        with holding(rs, args.takeover):
            run_blocks(plan, backend, out / "raw", progress=rs)
            rs.complete()
            if hasattr(backend, "cleanup_jobs"):
                backend.cleanup_jobs(rs.run_id)   # the measurements are on disk now: the storms' Jobs and logs are not needed
    if getattr(backend, "probe_fallback", False):
        note_probe_fallback(out)
    if probes:
        export_monitoring(run, args.target, out, json.loads((out / "environment.json").read_text()))
    text = report.write(out)
    print("\n" + text)
    write_notes(out)
    return 0


def _resume(run: Runner, args, make_store, backend_factory) -> int:
    rs = state_mod.RunState.load(make_store(args.target))
    if rs is None:
        raise state_mod.StateError("no run is recorded in this cluster: nothing to resume. Start one with `bench run ...`")
    if rs.is_complete:
        print(f"bench: the recorded run {rs.plan['name']!r} finished; start a new one with `bench run ...`", flush=True)
        return 1
    doc = rs.plan
    if doc["target"] != args.target:
        raise ValueError(f"the recorded run is for --target {doc['target']}, this is --target {args.target}")
    if doc.get("namespace") and doc["namespace"] != config.NAMESPACE:  # (a state from before it was recorded has none: it was always this one)
        raise ValueError(f"the recorded run's components are in namespace {doc['namespace']!r}, but this driver looks in {config.NAMESPACE!r}: "
                         "it would be resuming a run it cannot find")
    plan = plan_from_document(doc)
    out = Path(args.out) if args.out else recorded_results_dir(doc)
    if not (out / "environment.json").is_file():
        print(f"bench: {out}/environment.json not found: the measurements are kept on the machine that ran them. Pass --out DIR "
              "to point at the results directory, or `bench run --discard-state ...` to start over.", flush=True)
        return 1
    recorded_id = json.loads((out / "environment.json").read_text()).get("run_id")
    if recorded_id and recorded_id != rs.run_id:
        print(f"bench: {out} belongs to a different run (its run id is {recorded_id}, the run recorded in the cluster is {rs.run_id}): "
              "that is not where this run's measurements are. Pass --out DIR for the right directory.", flush=True)
        return 1
    changed = design(plan, doc["settle_s"]) != doc["design"]
    if changed and not args.accept_design_change:
        now = design(plan, doc["settle_s"])
        differs = sorted(k for k in set(now) | set(doc["design"]) if now.get(k) != doc["design"].get(k))
        print(f"bench: this driver would run the plan differently from the way the run started (differs: {', '.join(differs)}), so the "
              "units would not be comparable. Pass --accept-design-change to go on anyway, and it is recorded.", flush=True)
        return 1
    if not args.no_preflight and not check_before_run(run, args.target, plan.variants, doc["stack_overlay"], doc["loadgen_overlay"]):
        print("bench: preflight failed: nothing was applied. Fix the lines marked FAIL, or pass --no-preflight to go ahead anyway.", flush=True)
        return 1
    rs.check_lease(args.takeover)  # before anything is applied: a restart of the brokers would pull the rug from a live driver
    print(f"resuming run {doc['name']!r}: {rs.done_units()} of {rs.total_units()} units done; revision {rs.revision} "
          f"(started by {doc['driver_revision']})", flush=True)
    probes = use_monitoring(run, args)
    with backend_factory(run, target=args.target, stack=doc["stack_overlay"], loadgen=doc["loadgen_overlay"],
                         settle_s=doc["settle_s"], read_cpu=not args.no_cpu and not probes, cpuprobe=probes, read_varz=args.varz) as backend:
        backend.prepare(plan.variants, apply=False)  # nothing is changed on the cluster until a unit needs a fresh broker (see KubeBackend.prepare)
        env_path = out / "environment.json"
        env = json.loads(env_path.read_text())
        if not env.get("run_id"):  # made by a driver from before run ids: it is recorded now, so the two are tied from here on
            env["run_id"] = rs.run_id
            print(f"bench: {out} had no run id (an older driver made it): recording {rs.run_id}", flush=True)
        env.setdefault("resumes", []).append({"time": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
                                              "driver_revision": rs.revision, "units_done": rs.done_units(),
                                              **({"design_changed": True} if changed else {})})
        env_path.write_text(json.dumps(env, indent=1) + "\n")
        rs.note_resume(rs.done_units())
        with holding(rs, args.takeover):
            run_blocks(plan, backend, out / "raw", progress=rs)
            rs.complete()
            if hasattr(backend, "cleanup_jobs"):
                backend.cleanup_jobs(rs.run_id)   # the measurements are on disk now: the storms' Jobs and logs are not needed
    if getattr(backend, "probe_fallback", False):
        note_probe_fallback(out)
    if probes:
        export_monitoring(run, args.target, out, json.loads((out / "environment.json").read_text()))
    text = report.write(out)
    print("\n" + text)
    write_notes(out)
    return 0


def execute_status(run: Runner, args, *, make_store=None) -> int:
    """bench status: the progress of the run recorded in the cluster. Read-only."""
    if run.dry_run:
        print("bench status reads the cluster: skipped in a dry run")
        return 0
    rs = state_mod.RunState.load((make_store or (lambda target: state_mod.KubeStore(run, target)))(args.target))
    print(state_mod.format_status(rs) if rs else f"no run is recorded in the {args.target} cluster")
    return 0


def execute_adopt(run: Runner, args, *, make_store=None) -> int:
    """bench adopt DIR: record a results directory that has no state (a run made before the state existed, or by a driver that was
    killed) as a run that `bench run --resume` can continue. Every unit whose files are all there counts as done, except the ones
    named with --redo (or, with --redo-failed, those with a burst in which not every connection was admitted): their files are
    moved to DIR/discarded/ and the unit will be run again, from a fresh broker, by --resume."""
    out = Path(args.dir)
    env_path, raw = out / "environment.json", out / "raw"
    if not env_path.is_file():
        print(f"bench: {env_path} not found: DIR is a results directory made by `bench run`", flush=True)
        return 1
    env = json.loads(env_path.read_text())
    if env.get("target") != args.target:
        raise ValueError(f"the results are for --target {env.get('target')}, this is --target {args.target}")
    if not (raw / "DONE").is_file() and not args.force:
        print(f"bench: {raw}/DONE does not exist: the run may still be going, and a second driver on one run would delete the first one's "
              "Jobs. Wait for it to end. If its driver was killed, pass --force.", flush=True)
        return 1
    d = env["design"]
    plan = Plan(variants=tuple(Variant.from_dict(v) for v in env["variants"]), blocks=d["blocks"], sizes=tuple(d["sizes"]), threads=d["threads"])
    every = list(units(plan))
    keys = {u.key for u in every}
    unknown = sorted(set(args.redo or []) - keys)
    if unknown:
        raise ValueError(f"no such unit: {', '.join(unknown)}. The units are <variant>-b<block>, e.g. {every[0].key}")
    complete = [u for u in every if state_mod.unit_complete(raw, u)]
    problems = {u.key: state_mod.unit_problems(raw, u) for u in complete}
    redo = set(args.redo or []) | ({k for k, p in problems.items() if p} if args.redo_failed else set())
    print(f"{out.name}: {len(complete)} of {len(every)} units have all their files")
    for u in every:
        if u not in complete:
            print(f"  {u.key}: incomplete (it will be run again)")
    for k, p in problems.items():
        for line in p:
            print(f"  {k}: {line}" + ("" if k in redo else "   <- not every connection was admitted; name the unit with --redo to run it again"))
    keep = [u for u in complete if u.key not in redo]
    print(f"recording {len(keep)} units as done; {len(every) - len(keep)} will be run by --resume: {', '.join(u.key for u in every if u not in keep)}")
    if run.dry_run:
        print("dry run: nothing recorded or moved")
        return 0
    store = (make_store or (lambda target: state_mod.KubeStore(run, target)))(args.target)
    existing = state_mod.RunState.load(store)
    if existing and not existing.is_complete and not args.discard_state:
        print(f"bench: a run is already recorded in this cluster and is not finished:\n{state_mod.format_status(existing)}\n"
              "bench: `bench run --resume` continues it; `bench adopt --discard-state ...` forgets it and records this one", flush=True)
        return 1
    if existing:
        existing.check_lease(args.takeover)
    if args.variants_file and (out / "variants.toml").exists():
        raise ValueError(f"{out} already has a variants.toml: not overwriting it")
    stamp = time.strftime("%Y%m%dT%H%M%S")
    moved = {u.key: state_mod.move_unit_files(raw, u, out / "discarded" / f"{u.key}-{stamp}") for u in every if u.key in redo or u not in complete}
    if moved:
        (raw / "DONE").unlink(missing_ok=True)  # the run is not done any more: --resume writes it again
    rev = state_mod.driver_revision()
    given = getattr(args, "github_user", None) or os.environ.get("GITHUB_USER")
    recorded_login = None
    if given and not env.get("github_user"):   # a run from before logins were recorded takes the one given, and says so
        recorded_login = env["github_user"] = qualify.github_user(given)
    run_id = env.get("run_id") or state_mod.new_run_id()  # a directory that already has one keeps it
    if existing:
        store.delete()
    state_mod.RunState.create(
        store, state_mod.plan_document(plan, name=out.name, out=out.name, target=args.target,
                                        stack_overlay=args.stack_overlay or STACK_OVERLAYS[args.target],
                                        loadgen_overlay=args.loadgen_overlay or LOADGEN_OVERLAYS[args.target],
                                        settle_s=d["settle_s"], design=d, revision=rev, now=time.time(), run_id=run_id,
                                        started=env.get("date"), github_user=env.get("github_user")),
        units={u.key: {"status": "done", "adopted": True, "driver_revision": "adopted"} for u in keep})
    env["run_id"] = run_id
    added = [k for k in ("cluster_kind", "namespace") if k not in env]   # a run from before they were recorded: said, so it is not mistaken for a fact the old driver wrote
    if recorded_login:
        added.append("github_user")
    env.setdefault("cluster_kind", qualify.cluster_kind(args.target))
    env.setdefault("namespace", config.NAMESPACE)
    if args.variants_file:
        env["variants_file"] = save_variants_file(Path(args.variants_file), out)
    env["adopted"] = {"time": dt.datetime.now().astimezone().isoformat(timespec="seconds"), "driver_revision": rev,
                      **({"qualifiers_added": added} if added else {}),
                      "units_done": [u.key for u in keep],
                      "redo": [{"unit": k, "files_moved_to": f"discarded/{k}-{stamp}", "files": moved[k], "problems": problems.get(k, []),
                                **({"reason": args.reason} if args.reason else {})} for k in sorted(moved)]}
    env_path.write_text(json.dumps(env, indent=1) + "\n")
    print(f"recorded. Continue with: bench --target {args.target} run --resume")
    return 0


def execute_monitoring(run: Runner, args) -> int:
    """bench monitoring up | down | status | export DIR."""
    target = args.target
    if args.action == "up":
        if not args.no_preflight:
            checks = preflight.monitoring_checks(run, target, monitoring.overlay_for(target))
            print(f"preflight ({target}):\n{preflight.format_checks(checks)}", flush=True)
            if preflight.failed(checks):
                print("monitoring was not installed: fix what failed, or --no-preflight to try anyway", file=sys.stderr)
                return 1
        monitoring.up(run, target)
        print(f"monitoring is up: the probes on every node, Prometheus scraping them every {monitoring.STEP_S} s")
    elif args.action == "down":
        monitoring.down(run, target, purge=args.purge)
        print("monitoring is down" + (" and its volume deleted" if args.purge else f"; its volume claim {monitoring.PROMETHEUS_PVC} (and the data) stays: --purge deletes it"))
    elif args.action == "status":
        if run.dry_run:
            print("bench monitoring status reads the cluster: skipped in a dry run")
            return 0
        there = monitoring.installed(run, target)
        print(f"monitoring on {target}: " + ("installed" if there else "not installed"))
        if there:
            run(k8s.kubectl_command("-n", config.NAMESPACE, "get", "pods,pvc", "-l", "app.kubernetes.io/part-of=storm-bench-monitoring", target=target))
    else:
        if not args.dir:
            raise ValueError("bench monitoring export needs the results directory")
        out = Path(args.dir)
        env = json.loads((out / "environment.json").read_text())
        done = out / "raw" / "DONE"
        end = time.time() + 2 if not done.is_file() else dt.datetime.strptime(done.read_text().strip(), "%Y-%m-%dT%H:%M:%S%z").timestamp() + 5
        monitoring.export(run, target, out, dt.datetime.fromisoformat(env["date"]).timestamp() - 15, end)
        export_profiles(run, target, out)
        text = report.write(out)
        print("\n" + text)
    return 0
