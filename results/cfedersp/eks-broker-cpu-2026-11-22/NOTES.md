# eks-broker-cpu-2026-11-22

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id b2da4adbaaa1 · started 2026-11-22T10:03:45-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 8 blocks, sizes [800, 1600], phases warm, idle, n800, n1600
- **Variants file:** broker-cpu-variants.toml (sha256 96af4b50863d…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s

| Variant | Commit | Image |
|---|---|---|
| `cpu3` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `cpu15` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `cpu2` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `cpu35` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |

<!-- end generated -->

## Purpose

Does the broker's CPU limit pace a connection's setup (TCP, TLS, the WebSocket upgrade, INFO)? `main`, same image digest, same callout (1 pod, 1 CPU) and generator (2 CPU), the broker at 3 CPU (`cpu3`, baseline), 1.5, 2 and 3.5; 8 blocks, 800 and 1600, in the same blocks. Plan: `plans/broker-cpu-dose-response.md`: it predicted setup time inversely proportional to the limit (+100% at 1.5 CPU, +50% at 2).

## What it showed

**The prediction failed.** Setup time (total minus auth, mean of block medians; p against `cpu3`, 8 v 8 blocks):

| Broker CPU | Setup 800 | Setup 1600 | Auth 800 | Auth 1600 | Total 1600 | Burst length 800 / 1600 |
|---|---|---|---|---|---|---|
| 3 (baseline) | 276.7 ms | 444.8 ms | 127.7 ms | 244.8 ms | 689.4 ms | baseline |
| 1.5 | 300.1 (+8.4%, p 0.62) | 552.1 (+24.1%, p 0.0002) | 207.0 (+62%) | 423.5 (+73%) | 992.9 (+44%, p 0.0002) | +36.9% / +40.0% |
| 2 | 242.9 (-12.2%, p 0.47) | 497.9 (+11.9%, p 0.0002) | 174.0 | 312.0 | 820.1 (+18.9%, p 0.0002) | +13.5% / +15.5% |
| 3.5 | 210.3 (-24.0%, p 0.11) | 600.3 (+35.0%, p 0.46) | 126.2 | 223.8 | 816.9 (+18.5%, p 0.47) | -4.5% / +11.4% |

The plan predicted setup about 100% longer at 1.5 CPU and 50% at 2 CPU (about 880 and 660 ms at 1600): it rose 24% and 12%. Where the limit did bind (1.5 CPU), **the cost showed up in auth, not setup** (auth +62% at 800, +73% at 1600), and in burst length (+37%, +40%).

**Manipulation check:** passed only for 1.5 CPU. The broker was throttled in 70-76% of its CPU periods at 1.5 CPU, but only 7-8% at 2 CPU and 0-2% at 3 CPU; at 2 CPU it used 1.7-1.8 cores of 2. The broker never needed more than about 2.2 cores in these bursts (2.17-2.22 cores busy at 3 CPU), so a 2 CPU limit barely bound. At 3.5 CPU it used 2.3-2.6 cores and was no faster.

**CPU per connection moved with the limit** (1.37 ms at 1.5 CPU, 1.38 at 2, 1.61 at 3, 1.82-1.85 at 3.5): the more CPU the broker had, the more it spent per connection, which is what contention on a serialized section looks like (cores spinning or waiting instead of working). Together with the flat performance from 2 to 3.5 CPU and the lock profiles (`eks-mutex-profile`: connection set-up is where the waiting is), this reads as a **serialized section that caps the broker at about 2.2 cores' worth of work**: below that CPU limits it, above that extra CPU is wasted. Setup rate was about 1,800 connections per second at 3 CPU: 3 CPU / 1.6 ms (1,875 per second) was a coincidence, not the cause.

## What perturbs it

The generator (2 CPU) was throttled in about half its periods in every variant, so for the higher broker limits the generator may be the stage that stopped the improvement; this run cannot separate a lock ceiling from a generator ceiling above 3 CPU. The baseline's setup at 800 (277 ms) differs by 30% from `main`'s 213 ms in `eks-callout-replicas` on the same cluster (1600: 445 against 441 ms), so setup at 800 is noisy between runs: compare only within this run. The 2 CPU variant barely throttled (the limit did not bind); only 1.5 CPU is a clean test of a binding limit. Unprofiled, one run, 8 blocks per variant, the cluster build of 2026-11-22.
