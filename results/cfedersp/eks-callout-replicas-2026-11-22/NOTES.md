# eks-callout-replicas-2026-11-22

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 728a37b9ec03 · started 2026-11-22T08:20:13-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 8 blocks, sizes [800, 1600], phases warm, idle, n800, n1600
- **Variants file:** callout-replicas-variants.toml (sha256 b95b1941a90d…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `main_callout3` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |

<!-- end generated -->

## Purpose

Is the auth callout what paces a synchronized burst? `main` with one callout pod (1 CPU, 16 workers) against `main_callout3` (three pods, 3 CPU and 48 workers in total, one queue group), same image digest, same blocks, interleaved: so the comparison is within one run. Plan: `plans/callout-replicas.md` (its prediction and falsifier were written before the run). Made on a new cluster with two client nodes, because three callout pods and the generator do not fit one.

## What it showed

**Manipulation check passed:** the callout was throttled in 42.5% (800) and 44.0% (1600) of its CPU periods with one pod and in 0.0% with three. Auth p50, mean of block medians, exact permutation p, 8 v 8:

| | 800 | 1600 |
|---|---|---|
| `main` (1 callout) | 133.9 ms | 236.2 ms |
| `main_callout3` | 116.1 ms, -13.3% (p 0.0003), lower in 8 of 8 blocks | 204.2 ms, -13.5% (p 0.007), lower in 6 of 8 blocks |
| Burst length, first CONNECT to last answer | 0.53 s to 0.53 s (0.0%) | 1.19 s to 1.04 s (-12.2%) |
| Drain rate, 10-90% | +14.1% (p 0.00016) | +8.7% (p 0.18) |
| Peak queue | -4.5% (p 0.13) | -8.5% (p 0.44) |
| 90% admitted by | -2.2% (p 0.51) | +0.3% (p 0.92) |

**The 1600 result rests on two blocks.** `main_callout3` had block medians of 144 and 140 ms in blocks 4 and 8 and 220-231 ms in the other six (`main`: 221-246 ms in all eight). The mean difference is -13.5%; the median of the eight block medians is 239 against 222.5 ms, -7%. Where the pods ran was not the cause: reconstructed from the Prometheus series, the placement was identical in every unit (two callout pods on one client node, the third with the generator on the other; with one callout it was alone on its node). The two fast blocks are not explained.

**Against the plan's prediction** (burst length 10 to 25% shorter, not a third; auth latency falling by more than the burst does): at 1600 the burst was 12% shorter, inside the predicted band, but the 90%-admitted time did not move; at 800 the burst did not move at all, which is the falsifier's case (the callout was not what paced the 800 burst), while per-connection latency fell 13% and the drain rate rose 14%. So **the callout is one of several stages near capacity, not the one that sets the burst length.** With its cap lifted the next stages show: the generator was throttled in 47% (800) and 57% (1600) of its periods, up from 39% and 37%, and the broker was at 2.4 of its 3 cores (throttled at most 1.4%). The CPU cost of a connection through the callout rose from 0.50 to 0.61 ms summed over three pods.

## What perturbs it

**The new cluster is not the old one.** The cluster was rebuilt for this run (new nodes). `main` at 1600 here is 236 ms against 272-279 ms in the two earlier 8-block runs, and the callout's CPU per connection is 0.49 ms against 0.57 ms: the same configuration differed by 13 to 15% between cluster builds, far more than between two runs on one cluster (+5.2% and -2.7%). Compare this run only with itself. In `main_callout3` one callout pod shares a client node with the generator, which is not so in `main`; that is the same in every block. The generator (2 CPU) was throttled in 37-57% of its periods in both variants, so a faster pipeline is still capped there. Unprofiled, one run, 8 blocks per variant.

### Correction, 2026-11-22 (later): where the connection's time went

The first reading above looked only at auth. `bench report` now splits a connection's time into setup (TCP, TLS, the WebSocket upgrade and INFO, before CONNECT) and auth; for this run (mean of block medians, 8 v 8): at 800 setup is 213 ms for `main` and 242 ms for `main_callout3` (+13.4%, p 0.0065) and total 345 and 357 ms (+3.7%, p 0.94); at 1600 setup is 441 and 548 ms (+24.4%, p 0.0096) and total 679 and 752 ms (+10.8%, p 0.42). So with the callout uncapped, auth fell 13% but setup rose, and the **total per-connection time did not improve**: the time moved to the stage before the callout. That points at a resource shared by both stages, the broker's CPU, and is why `plans/broker-cpu-dose-response.md` exists. My first account called setup unchanged (212 to 226 ms at 800); that used medians pooled over all bursts, not over blocks, and was the wrong statistic.

