# lock-mux-eks-retry-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id b18d36028497 · started 2026-11-21T11:44:36-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 8 blocks, sizes [800, 1600], phases warm, idle, n800, n1600
- **Variants file:** lock-variants.toml (sha256 f352327aedc6…, copy in `variants.toml`)
- **CPU source:** not recorded (before it was)
- **Stopped and resumed:** 2026-11-21T12:49:04-08:00 with 22 units done
- **Units run again** (earlier files in `discarded/`): main-b4, main-b6
- **Backfilled afterwards:** cluster_kind, namespace, github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `nolock` | 37260075e531 | nats-server@sha256:4214ee07b2bb… |
| `mux_main` | ec9244c3444d | nats-server@sha256:0676f13113d5… |
| `mux_nolock` | 7ea787e2b5ee | nats-server@sha256:d35b480b8d13… |

<!-- end generated -->

## Purpose

The four-way comparison, complete: `main`, `nolock`, `mux_main`, `mux_nolock`; 8 blocks x 4 variants, sizes 800 and 1600, 32 units; 3 broker nodes and 1 client node (`c6i.xlarge`).

## What it showed

Auth p50 against `main` (mean of block medians, exact permutation p, n = 8 v 8; three comparisons per size, **not corrected for them**):

| | 800 | 1600 |
|---|---|---|
| `nolock` | -11.4% (p 0.043) | -18.1% (p 0.00016) |
| `mux_main` | +3.9% (p 0.44) | -3.7% (p 0.037) |
| `mux_nolock` | -11.5% (p 0.015) | -14.0% (p 0.00016) |

Idle (no burst) differences are within noise (-0.8% to +0.1%). Queue features: `nolock` peak queue -9.8% at 1600 (p 0.043) and nothing detectable at 800.

## What perturbs it

CPU was not recorded. The callout and the generator were later found to be throttled (see `eks-cpu-check-2026-11-21`), which can mask or exaggerate a broker effect. Units `main-b4` (6400 of 12800 connections admitted: a test of mine reached the live cluster and the broker refused connections) and `main-b6` (set aside by `bench adopt`) were run again after the run; their first files are in `discarded/`. `environment.json` `cluster_kind` and `namespace` were backfilled afterwards (`backfilled` says so). The driver was stopped and resumed once at 12:49 (22 units done).
