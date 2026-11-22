# eks-probe-check-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id a9619710399b · started 2026-11-21T15:28:11-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 2 blocks, sizes [800], phases warm, idle, n800
- **Variants file:** eks-mon-variants.toml (sha256 83f3944dea0b…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `nolock` | 37260075e531 | nats-server@sha256:4214ee07b2bb… |

<!-- end generated -->

## Purpose

The same as `eks-monitoring-check-2026-11-21` with loadgen `src-a3d7265ebd6e`: to confirm `probeCpu` on EKS bursts.

## What it showed

`probeCpu` on every burst. During bursts the callout was throttled in about 60% of periods (limit 1 CPU, 0.8 cores busy), the generator in about 35% (limit 2), the broker in none (2.3 of 3 cores busy). The two variants' CPU was alike. Auth p50 `nolock` vs `main` -12.7% (p 0.33, n = 2 blocks: not evidence).

## What perturbs it

Two blocks only. The callout and generator limits shape what the broker can show.
