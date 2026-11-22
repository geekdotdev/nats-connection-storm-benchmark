# eks-cpu-check-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 96891fa8d821 · started 2026-11-21T13:54:31-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 1 blocks, sizes [800], phases warm, idle, n800
- **Variants file:** lock-variants.toml (sha256 f352327aedc6…, copy in `variants.toml`)
- **CPU source:** not recorded (before it was)
- **Stopped and resumed:** 2026-11-21T13:57:24-08:00 with 1 units done
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `nolock` | 37260075e531 | nats-server@sha256:4214ee07b2bb… |
| `mux_main` | ec9244c3444d | nats-server@sha256:0676f13113d5… |
| `mux_nolock` | 7ea787e2b5ee | nats-server@sha256:d35b480b8d13… |

<!-- end generated -->

## Purpose

The first CPU and throttling readings: 1 block, all four variants, 800 connections, 4 units. The driver read the cgroups with `kubectl exec`, once per storm.

## What it showed

**The callout was throttled in 31-39% of its CPU periods and the generator in 33-60%; the broker barely at all (at most 1.4%).**

## What perturbs it

Resumed once at 13:57 with 1 unit done: this was the kill-and-restart test of the run state. One block only: the permutation p-values (1.0) mean nothing.
