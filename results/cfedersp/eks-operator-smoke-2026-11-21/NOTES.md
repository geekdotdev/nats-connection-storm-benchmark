# eks-operator-smoke-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace not recorded · run id not recorded · started 2026-11-21T09:18:15-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 2 blocks, sizes [800], phases warm, idle, n800
- **CPU source:** not recorded (before it was)
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `single` | 582fb8e939a7 | ghcr.io/cfedersp/nats-server:582fb8e939a7 |
| `cluster` | 582fb8e939a7 | ghcr.io/cfedersp/nats-server:582fb8e939a7 |

<!-- end generated -->

## Purpose

The first run in operator mode on EKS, made before run state and run ids existed: 2 blocks, 800, `single` against `cluster`, 4 units. To prove the operator-mode stack, callout and generator work on EKS.

## What it showed

That the stack worked on EKS in operator mode. Not a measurement.

## What perturbs it

No run id and no qualifiers (they did not exist yet): `environment.json` has no `cluster_kind` or `namespace`.
