# eks-monitoring-check-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 1046283b412a · started 2026-11-21T15:17:09-08:00 · run by cfedersp
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

The first run with the CPU probe and Prometheus installed on EKS (an `infra` node and a gp3 volume): `main` against `nolock`, 2 blocks, 800.

## What it showed

The Prometheus export worked (925 samples per metric) and `cpu.svg` showed the throttling again. **The bursts had no `probeCpu`.**

## What perturbs it

The EKS overlay still pinned loadgen `src-16759d90c2a9`, which predates the probe, so the generator ignored `STORM_CPUPROBE`. The run recorded `cpu_source: probe` regardless, and its report has no broker or callout CPU. That silent loss is why the driver now falls back to its own reading and says so. Use `eks-probe-check-2026-11-21` for CPU.
