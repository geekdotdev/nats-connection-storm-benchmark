# eks-smoke-2026-11-18

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace not recorded · run id not recorded · started 2026-11-18T18:38:10-08:00 · run by cfedersp
- **Target:** eks
- **Design:** 2 blocks, sizes [200], phases warm, idle, n200
- **CPU source:** not recorded (before it was)
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `single` | 582fb8e939a7 | ghcr.io/cfedersp/nats-server:582fb8e939a7 |
| `cluster` | 582fb8e939a7 | ghcr.io/cfedersp/nats-server:582fb8e939a7 |

<!-- end generated -->

## Purpose

A first check that the benchmark ran on EKS: 2 blocks, 200 connections, `single` against `cluster` (one commit, 582fb8e).

## What it showed

That it ran; see `summary.txt`. Not a measurement.

## What perturbs it

Made before operator mode replaced the earlier authentication (a password callout, accounts in the broker's configuration, one shared password); the current code cannot reproduce it (git commit `bf93da8` and earlier) and its numbers are not comparable with later runs. See `results/README.md`.
