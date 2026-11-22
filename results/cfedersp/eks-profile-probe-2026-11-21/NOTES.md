# eks-profile-probe-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 17f98322cd0f · started 2026-11-21T16:06:45-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 2 blocks, sizes [800], phases warm, idle, n800
- **Variants file:** profile-variants.toml (sha256 04a28c099a75…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s
- **Profiled:** 328 snapshots; block profiling slows the broker: the timings of this run are perturbed and not comparable with runs without it
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `nolock` | 37260075e531 | nats-server@sha256:4214ee07b2bb… |

<!-- end generated -->

## Purpose

The same profile, harvested by the CPU probes this time (the design: the probe harvests during the run and the driver only exports at the end): `main` against `nolock`, 2 blocks, 800.

## What it showed

328 block snapshots from 5 broker pods over 660 s; the same shares as `eks-profile-lock-2026-11-21`, so the two runs agree. Summed totals differ a lot between them (518 s against 1415 s for `main`).

## What perturbs it

**Profiled: timings are perturbed.** The end-of-run export failed once on `"profiles": null` from probes with nothing to report; fixed, and the run re-exported with `bench monitoring export`.
