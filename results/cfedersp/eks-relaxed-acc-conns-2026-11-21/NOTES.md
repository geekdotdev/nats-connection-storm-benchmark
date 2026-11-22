# eks-relaxed-acc-conns-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 6bd3204a85ad · started 2026-11-21T17:11:52-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 8 blocks, sizes [800, 1600], phases warm, idle, n800, n1600
- **Variants file:** relaxed-variants.toml (sha256 ad8ebc9733e7…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `nolock` | 37260075e531 | nats-server@sha256:4214ee07b2bb… |
| `relaxed` | 37260075e531 | nats-server@sha256:01b0f78412f0… |

<!-- end generated -->

## Purpose

Two questions at once: can `accConnsUpdate` be relaxed for a standalone single-account server (`relaxed` = `nolock` plus `images/nats-server/patches/relaxed-acc-conns.patch`: one connection-count update per account per 100 ms instead of an event per connect, no account lock on the connect path), and does `nolock` against `main` replicate? The plan is `plans/relaxed-acc-conns.md`; its prediction and the rule for dropping the idea were written before the run.

## What it showed

8 blocks x 3 variants, 800 and 1600 connections, unprofiled, probes on; every connection admitted (51,200 and 102,400 per variant). Auth p50, mean of block medians, exact permutation p, n = 8 v 8:

| | 800 | 1600 |
|---|---|---|
| `main` | 145.3 ms | 271.9 ms |
| `nolock` | 123.1 ms, -15.3% vs `main` (p 0.00016) | 236.5 ms, -13.0% vs `main` (p 0.00016) |
| `relaxed` | 108.4 ms, -25.4% vs `main` (p 0.00016) | 224.1 ms, -17.6% vs `main` (p 0.00016) |

**`nolock` vs `main` replicates** the earlier 8-block run (-11.4% and -18.1%): lower in 8 of 8 blocks at both sizes, with the smallest p an 8 v 8 test can give. Queue: peak -7.2% at 800 (p 0.033) and -3.0% at 1600 (p 0.036); 90%-admitted-by -4.4% at 1600 (p 0.0003); drain rate +5.6% at 800 (p 0.035); time to admit the whole burst unchanged (0.56 s, and 1.12 against 1.10 s).

**`relaxed` vs `nolock`, the question asked** (`bench report --baseline nolock`): auth p50 -12.0% at 800 (p 0.023) and -5.2% at 1600 (p 0.044); peak queue -6.3% at 800 (p 0.34) and -4.2% at 1600 (p 0.014); 90%-admitted-by, drain rate and burst length all within 1.5% with p over 0.4. After correcting for the two sizes the 800 result is marginal (0.045) and the 1600 result is not (0.088). The prediction (`relaxed` faster than `nolock`) is met in direction; it is not "clearly faster", and no throughput measure moves.

## What perturbs it

The callout was throttled in 53-59% of its CPU periods and the generator in 38-45% during bursts, in every variant (callout 1 CPU, generator 2 CPU): the pipeline in front of the broker still sets the pace, so a small broker-side gain may be understated. `relaxed` was not tried with them uncapped. `relaxed` is a patched image (cross-server limit enforcement is given up: five existing nats-server tests fail with it, see `images/PUBLISHED.md`); compare it only as this experiment. `nolock` is the commit `37260075`, which predates the PR's later fix for the shutdown race (one atomic load under the account lock, not re-measured). One run, one machine.
