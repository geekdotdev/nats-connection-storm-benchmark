# eks-notls-2026-11-22

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id c1ac47e5ed7e · started 2026-11-22T11:22:48-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 8 blocks, sizes [800, 1600], phases warm, idle, n800, n1600
- **Variants file:** notls-variants.toml (sha256 8b7cf981b1b3…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s

| Variant | Commit | Image |
|---|---|---|
| `tls` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `notls` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |

<!-- end generated -->

## Purpose

How much of a connection's setup time (TCP, TLS, the WebSocket upgrade, INFO) is the TLS handshake? `tls` (baseline, ECDSA P-256 cert) against `notls` (the broker's WebSocket listener without TLS, the generator on ws://), same image digest, same callout (1 pod, 1 CPU) and broker (3 CPU); 8 blocks, 800 and 1600, in the same blocks. Plan: `plans/tls-cost.md`, predicted before the run: setup falls 10-30%, broker CPU/connection falls 10-25%, generator CPU/connection falls 18-36%, auth unchanged.

## What it showed

**Manipulation check passed:** every connection admitted in both variants; broker and generator CPU per connection fell in both; the switch took.

Setup, total and auth (mean of block medians; p against `tls`, 8 v 8 blocks):

| | 800: `tls` → `notls` | 1600: `tls` → `notls` |
|---|---|---|
| Setup | 230.7 → 108.8 ms (-52.8%, p 0.00016) | 439.3 → 217.6 ms (-50.5%, p 0.00016) |
| Auth | 129.1 → 162.0 ms (**+25.5%**) | 252.7 → 278.5 ms (**+10.2%**) |
| Total | 357.6 → 265.8 ms (-25.7%, p 0.00016) | 690.1 → 497.3 ms (-27.9%, p 0.00016) |
| Burst length | 0.55 → 0.44 s (-20.0%) | 1.14 → 0.89 s (-21.9%) |
| Broker CPU/connection | 1.61 → 1.21 ms (-24.8%) | 1.63 → 1.22 ms (-25.2%) |
| Generator CPU/connection | 0.88 → 0.44 ms (-50.0%) | 0.84 → 0.43 ms (-48.8%) |
| Callout CPU/connection | 0.51 → 0.50 ms (flat, as expected: TLS is not callout work) | 0.49 → 0.49 ms (flat) |
| Callout throttled % | 42.9 → 54.1 | 43.1 → 54.0 |

**Setup fell by about half, at the falsifier's own upper boundary** (the plan called over 50% "surprising for an ECDSA certificate"; 52.8% and 50.5% sit right at that line). TLS is a much larger share of setup than the hypothesis estimated, on both ends: broker CPU/connection fell 25% (top of the predicted 10-25% band) but **generator CPU/connection fell 49-50%, far past the predicted 18-36%**: the client-side handshake (negotiation, key exchange, record layer) costs noticeably more than the ballpark guess of 0.15-0.3 ms.

**Auth rose instead of staying flat, and the callout data explains why.** The callout's own CPU per connection is unchanged (TLS is not its work), but its throttled share rose from 43% to 54% in both sizes, and its cores busy rose (0.71 to 0.87 at 800, 0.65 to 0.88 at 1600) against its fixed 1 CPU limit. With setup twice as fast, connections reach the callout more concentrated in time; the callout, already the most CPU-starved stage, absorbs more queueing, and that shows up as auth latency even though total latency fell. TLS was incidentally pacing arrival at the callout.

## What perturbs it

Not profiled; one run, 8 blocks per variant, the cluster build of 2026-11-22 (do not compare its `tls` baseline, 230.7/439.3 ms setup, with `main`'s setup on a different cluster build: see the cross-build pitfall in CLAUDE.md). `no_tls` is a diagnostic: the broker warns `Websocket not configured with TLS. DO NOT USE IN PRODUCTION!` and nats-server does, so this is never a real deployment option; it isolates the handshake's cost, nothing more. The callout's 1 CPU limit is fixed in this run, so the auth increase is a statement about this stack's current bottleneck ordering, not a general law that removing TLS always makes auth worse.
