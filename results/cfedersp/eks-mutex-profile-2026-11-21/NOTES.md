# eks-mutex-profile-2026-11-21

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id 010580a217e0 · started 2026-11-21T16:37:12-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 2 blocks, sizes [800], phases warm, idle, n800
- **Variants file:** mutex-variants.toml (sha256 39e3d2dbfecd…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s
- **Profiled:** 398 snapshots; block profiling slows the broker: the timings of this run are perturbed and not comparable with runs without it
- **Backfilled afterwards:** github_user (see `environment.json`)

| Variant | Commit | Image |
|---|---|---|
| `main_mutex` | 582fb8e939a7 | nats-server@sha256:e5ad6a92cf95… |
| `nolock_mutex` | 37260075e531 | nats-server@sha256:cdd54d7983f1… |

<!-- end generated -->

## Purpose

Who HOLDS the locks that connects wait on: the mutex-profile images (`main_mutex`, `nolock_mutex`; `images/PUBLISHED.md`), 2 blocks, 800 connections.

## What it showed

398 snapshots of block and mutex profiles. Lock wait is again about 100% in connection set-up. The mutex profile says who held the locks others waited on: `processClientOrLeafAuthentication` 62-71% of the lost time, `createWSClient` 16-20%, `processConnect` 7-10%, `processPing` 6-9%; `accConnsUpdate` is not in the top five. From the source (`auth.go` on `main`), `processClientOrLeafAuthentication` takes `s.mu.Lock()` (the server-wide write lock) near its start and releases it roughly 230 lines later, around the config and account lookups and before the callout round trip; that is from reading the code, not tested. Callout reply wait was 137-210 s per storm, summed over goroutines. Implication for the single-account-leaf idea: relaxing `accConnsUpdate`'s account lock has no measurable payoff on this data.

## What perturbs it

**Profiled and patched: timings are perturbed, and the images carry a one-line patch (`SetMutexProfileFraction`); comparable with each other and with nothing else.** Two blocks per variant and one size: a strong lead, not a result.

### Correction, 2026-11-21 (later)

The sentence above, that relaxing `accConnsUpdate`'s account lock has no measurable payoff, is about the account lock and stands as that. It should not be read as "`accConnsUpdate` costs nothing". On `main` the function holds the server-wide read lock (`s.mu.RLock()` with `defer RUnlock`) across the whole event build, and `nolock`, which differs from `main` only by removing it, is faster at 800 in 6 of 7 runs (-11% to -26% in auth p50). The profiles attribute about 0.16 ms per connection of wait to that path (2 s of block time and 0.3 s of mutex blame per storm of 12,800 connections) against a difference of about 20 ms, so the size of the effect is not explained. See [plans/nolock-advantage-mechanism.md](../../../plans/nolock-advantage-mechanism.md).
