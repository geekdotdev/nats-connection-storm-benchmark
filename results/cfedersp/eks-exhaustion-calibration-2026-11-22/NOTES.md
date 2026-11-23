# eks-exhaustion-calibration-2026-11-22

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 054ebec42bcd · started 2026-11-22T14:41:18-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 1 blocks, sizes [6000], phases warm, idle, n6000
- **Variants file:** exhaustion-variants.toml (sha256 783565455af4…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `main_exhaustion` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |

<!-- end generated -->

## Purpose

The calibration step of [plans/held-connection-exhaustion.md](../../../plans/held-connection-exhaustion.md): before trusting any broker-side number from the planned ratio sweep, check that the generator itself can hold the sweep's largest point (size 6000, 80% bad-faith: 40% `forged`, 40% `incompleteHandshake`, `STORM_RAMP_MS=30000`) without becoming the limit. Also the first live exercise of the `/varz` poller (`driver/stormbench/varz.py`, `--varz`), both to check its field names against a real broker and to see whether a sustained volume of held connections actually shows up there. `main` (no bad faith, no ramp) runs alongside as the unchanged baseline, at the same size, on the `eks-restricted` `NetworkPolicy` overlay.

## What it showed

**Calibration passes for the plan as designed.** `main_exhaustion`'s `generatorSchedulingDelayP99Ms` held at ~2ms and its cgroup CPU throttling at 0% across all 8 reps — the 30s ramp keeps the generator's own instantaneous load low enough that it is nowhere near its 2 CPU limit. No change to `generator_resources` is needed for the sweep's largest planned size.

**Unplanned side finding: `main`'s own *unramped* burst at size 6000 throttles the generator significantly** (52-58% of periods, scheduling delay 82-175ms, vs ~2ms ramped) — a new ceiling on the generator at a size no earlier run tested (800/1600 were the largest before this). This does not affect the exhaustion design itself (it always ramps), but any future unramped comparison at size 6000 would need its own calibration first, or a bigger generator pod.

**The `/varz` poller worked end to end, and confirmed its own uncertain field names.** `mem`, `connections`, `total_connections`, `cpu` and `max_connections` all came back populated from the real broker — the "not fully verbatim" flag in `varz.py`'s docstring and `README.md` can be dropped. One correction to the plan's own Hypothesis: `max_connections` read **65536**, not unconfigured/unlimited as assumed from `broker-config.yaml` having no explicit setting — this is `nats-server`'s own documented default when the setting is absent, not "no limit." Still far above anything in the planned sweep, so not yet a binding constraint, but the Hypothesis should say "defaults to 65536," not "no limit."

**This size point is itself the sweep's largest, so its data is worth keeping, not just the calibration verdict.** Peak concurrently-held connections (from the `varz` series, which samples every ~2.6s through the whole 290s Job) reached **1999**, within 25% of the naive `arrival_rate × 10s` estimate of 1600 for this size/fraction/ramp combination (Prediction #1's "within a factor of 2" holds). Broker memory climbed from ~26MB to ~395MB across the 8 repeated bursts within this one Job, **not fully recovering between bursts** (each burst's trough was higher than the last: 117 → 211 → 199 → … → 341MB) — still only ~10% of the broker's 4Gi cgroup limit, nowhere near exhaustion at this scale, but the non-recovery is worth watching rather than assuming it is Go-runtime retention; a longer run (more reps, or a dedicated memory-over-time plan) would say which. **No good-faith degradation**: all 1200 good-faith connections per rep were admitted, zero refused/timeout/error, even at the ~2000-held peak. `forged` and `incompleteHandshake` stayed tightly clustered at 10000-10007ms (even tighter than the 20-at-a-time sanity run's 10020-10047ms) — no sign of the broker's own timeout handling degrading under this load (Falsifier #2 not triggered).

## What perturbs it

One block, one size: no p-value means anything here, and this was never meant to be a statistically powered point (it is calibration, incidentally also one real sweep point). The `varz` series' resolution is the poll interval (~2.6s observed, vs. the configured 2s — `kubectl get --raw` itself takes time, and the thread's `stop.wait` is measured from the end of the previous read, not a fixed clock), so a spike shorter than that can be missed entirely; the true peak held count could be higher than 1999. The 8 reps run back-to-back inside one Job, so the memory trend reflects one broker process's whole run, not 8 independent measurements — the non-recovery between bursts could come from this nesting in a way an isolated single-burst run would not show.
