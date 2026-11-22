# eks-profile-lock-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 8a70a05ccd2a · started 2026-11-21T15:54:08-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 2 blocks, sizes [800], phases warm, idle, n800
- **Variants file:** profile-variants.toml (sha256 04a28c099a75…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s
- **Profiled:** ? snapshots; block profiling slows the broker: the timings of this run are perturbed and not comparable with runs without it
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `nolock` | 37260075e531 | nats-server@sha256:4214ee07b2bb… |

<!-- end generated -->

## Purpose

The first profile of where connects wait on locks: `main` against `nolock`, 2 blocks, 800 connections, the broker with `prof_port` and block profiling at rate 1.

## What it showed

Lock wait is about 99-100% in connection set-up (`createWSClient`), of which `processClientOrLeafAuthentication` about 52%; the account-lock paths (`accConnsUpdate`, `addClient`) are under 0.5%.

## What perturbs it

**Profiled: timings are perturbed; this run is for the profiles.** The profiles were taken by the driver around each storm (before the rule that the probe harvests), so `raw/profile/` has the older `<storm>.block.before/after` layout, which the report still reads. Summed lock-wait totals vary a lot between runs: trust shares, not totals.
