# lock-mux-eks-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace not recorded · run id not recorded · started 2026-11-21T11:06:36-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 8 blocks, sizes [800, 1600], phases warm, idle, n800, n1600
- **CPU source:** not recorded (before it was)
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `nolock` | 37260075e531 | nats-server@sha256:4214ee07b2bb… |
| `mux_main` | ec9244c3444d | nats-server@sha256:0676f13113d5… |
| `mux_nolock` | 7ea787e2b5ee | nats-server@sha256:d35b480b8d13… |

<!-- end generated -->

## Purpose

The first attempt at the four-way comparison: `main`, `nolock`, `mux_main`, `mux_nolock`; 8 blocks, 800 and 1600.

## What it showed

Nothing: it stopped early, at 8 of 32 units.

## What perturbs it

**Partial: 8 of 32 units.** It stopped early, apparently on a single transient `kubectl` DNS failure (the driver had no retries then; the environment does not record the cause). Kept as a record of the failure, not used for results. The complete run is `lock-mux-eks-retry-2026-11-21`.
