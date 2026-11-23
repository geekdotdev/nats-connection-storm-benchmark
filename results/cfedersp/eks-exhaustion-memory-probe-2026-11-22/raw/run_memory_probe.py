"""One-off diagnostic: calls KubeBackend.run_storm directly with a custom STORM_RUNS, bypassing the variants-file path (which
reserves STORM_RUNS for the plan's own phases). Not part of the project; follow-up to eks-exhaustion-calibration-2026-11-22.

40 reps of the same per-cycle load that run already used (size 6000, 80% bad-faith: 40% forged, 40% incompleteHandshake,
STORM_RAMP_MS=30000), to see whether the broker's memory keeps climbing across many more repeated cycles or plateaus.
"""
import json
import sys
from pathlib import Path

from stormbench import ab, kube
from stormbench.runner import Runner

RUN_DIR = Path(__file__).parent / "memory-probe"
RUN_DIR.mkdir(exist_ok=True)

VARIANT = ab.Variant(
    label="main_exhaustion",
    commit="582fb8e939a77b6a33ef845955a208bdc222d767",
    image="ghcr.io/cfedersp/nats-server@sha256:7e89e5a9ad5adf167322479a901b13df6750f4ff7ec3e254eaffa00113aedf5d",
)

run = Runner()
# job_timeout_s: 40 reps x ~36s (30s ramp + stragglers) + 39 x 1.5s pauses is ~25 minutes; the default 900s (15 min, sized for the
# framework's usual 8 reps) killed the first attempt at this script mid-run, deleting a still-running Job before it finished. 2400s
# (40 min) leaves real margin.
backend = kube.KubeBackend(run, target="eks", loadgen="deploy/loadgen/overlays/eks-restricted", read_varz=True, read_cpu=True, job_timeout_s=2400.0)
try:
    print("applying main_exhaustion and waiting for rollout...", file=sys.stderr)
    backend.prepare([VARIANT], apply=True)
    print("running the storm (40 reps, this will take a while)...", file=sys.stderr)
    env = {
        "STORM_CONNECTIONS": "6000", "STORM_RUNS": "40", "STORM_PAUSE_MS": "1500", "STORM_HOLD_MS": "1000",
        "STORM_THREADS": "4", "STORM_LABEL": "memory-probe-n6000x40", "STORM_JSON": "-", "STORM_SAMPLES": "1",
        "STORM_FORGED_FRACTION": "0.4", "STORM_INCOMPLETE_HANDSHAKE_FRACTION": "0.4", "STORM_RAMP_MS": "30000",
    }
    log = backend.run_storm("memory-probe-n6000x40", env)
    (RUN_DIR / "memory-probe-n6000x40.txt").write_text(log)
    report = ab.parse_report(log)
    report["varz"] = backend.last_varz
    report["cpu"] = backend.last_cpu
    (RUN_DIR / "memory-probe-n6000x40.json").write_text(json.dumps(report, indent=1))
    print(f"done: {len(report.get('runs', [report]))} reps, varz series has {len(backend.last_varz or [])} readings", file=sys.stderr)
    backend.delete_storm_jobs()   # only clean up on success: a failure leaves the Job for inspection instead of destroying its data
finally:
    backend.close()
