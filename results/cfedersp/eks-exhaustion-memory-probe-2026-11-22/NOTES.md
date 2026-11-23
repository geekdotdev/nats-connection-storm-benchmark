# eks-exhaustion-memory-probe-2026-11-22

Not a `bench run`: a one-off diagnostic, `raw/run_memory_probe.py`, calling `KubeBackend.run_storm` directly with `STORM_RUNS=40`
(the plan's phases reserve `STORM_RUNS`, so this bypassed the variants-file path on purpose — see the script's own docstring). No
`environment.json`; the facts that would be in it:

- **Run:** cluster eks · namespace storm-bench · target eks, auth operator mode, memory resolver, run by cfedersp
- **What ran:** `main_exhaustion` (582fb8e939a7, same image as every other run on this cluster build), a single storm, 40 repeated
  bursts of size 6000 (80% bad-faith: 40% `forged`, 40% `incompleteHandshake`, `STORM_RAMP_MS=30000`), on `deploy/loadgen/overlays/eks-restricted`, `--varz` on.
- **A first attempt failed**: `KubeBackend`'s default `job_timeout_s` (900s, sized for the framework's usual 8 reps) was shorter than
  this request's real duration (~24 minutes). The timeout fired, raised `StormFailed`, and the script's `finally` block then deleted
  the still-running Job, destroying its data before it could finish. Fixed by passing `job_timeout_s=2400` and only cleaning up the
  Job on success, not in `finally`. The run below is the second, corrected attempt.

## Purpose

Follow-up to [eks-exhaustion-calibration-2026-11-22](../eks-exhaustion-calibration-2026-11-22/NOTES.md), whose memory reading climbed
26MB→395MB across 8 repeated bursts without recovering between them, left unresolved as "real accumulation vs. ordinary Go-runtime RSS
retention." This run is 5x as many cycles (40, not 8) of the same per-cycle load, to tell the two apart: retention plateaus once the
peak live working set stabilizes; a real accumulating cost keeps climbing with cumulative connections regardless of how many are
concurrently live.

## What it showed

**Memory does not plateau. It climbed monotonically for the entire 24-minute, 40-rep run**, from 15.9MB to **1301MB**, tracking
cumulative `total_connections` (from `/varz`) almost exactly, while the *live* `connections` count oscillated between ~200 and 1999
the whole time with no upward trend of its own. That is the signature of a real accumulating resource tied to connections ever
processed, not a stabilizing working-set cache — Go-runtime RSS retention would predict a plateau once the peak live heap is reached,
not 24 minutes of unbroken, roughly linear climb.

**This is not specific to bad faith.** The calibration run's own `main` unit — pure good faith, no bad-faith settings at all, unramped,
same size 6000 — showed the *same* pattern over its own (much shorter, 64s) window: 37MB → 1381MB over 48,641 connections, all
admitted. Normalizing both runs by **admitted** connections only (bad-faith connections in `main_exhaustion` are never admitted, so
they should not be expected to cost the same as a real one):

| Run | Window | Admitted connections | Memory growth | KB per admitted connection |
|---|---|---|---|---|
| `main` (calibration, unramped, 100% good faith) | 64s | 48,000 | 37→1381MB | **28.0** |
| `main_exhaustion` (this run, ramped, 20% good faith) | 1436s | 48,000 | 16→1301MB | **26.8** |

Two runs with almost nothing else in common — different ramp, different bad-faith fraction, 22x different duration — land within 5% of
each other once normalized by admitted connections. That is a strong, independently-triangulated signal that **something in the
connection-admission path retains roughly 27KB per admitted connection, indefinitely, with no sign of being freed** — a genuine
operational risk for ANY sustained high-churn workload against this broker configuration, attack or not, not a bad-faith-specific
finding. At this rate, extrapolating the observed growth to the broker's 4Gi cgroup limit implies roughly 150,000 further admitted
connections (at this run's own admission rate, well under two hours of this run's load) before the broker would be at serious risk of
being OOMKilled — this is an extrapolation from one run, not a measured ceiling, and should be treated as a rough order of magnitude,
not a prediction to rely on.

Bad-faith (never-admitted) connections contribute comparatively little: across all 240,000 connections in this run (only 48,000 of
them admitted), growth works out to 5.4 KB/connection overall — consistent with ~27 KB being charged only to the 20% that were
admitted, and close to nothing being retained for the 80% that were refused or timed out.

## What perturbs it

One run each, no replication — this is a lead, strongly triangulated by agreement between two very different configurations, not a
statistically powered result. Neither run was long enough to see the growth actually plateau or the broker approach its memory limit;
both stopped well short of that. The attribution to "connection admission" specifically (rather than something else correlated with
it, such as elapsed wall-clock time, bytes transferred, or the account/permission setup every admitted connection goes through) is
inferred from the two runs' agreement, not traced in the broker's own source or profiled — the next step, if this is pursued, is a
`pprof` heap profile (not currently harvested by this benchmark; only CPU/block/mutex profiles are) taken at two points during a long
admitted-only run, which would show what is actually growing rather than just that something is.