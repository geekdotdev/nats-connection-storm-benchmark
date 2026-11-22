# nolock-advantage-mechanism

**Status:** dropped, 2026-11-21: decided not to pursue. **Run:** none.

## Question

Why is `nolock` faster than `main`? The two commits differ only in `accConnsUpdate`'s server lock (`git diff` shows `events.go` and `server.go`, one atomic pointer), yet the profile attributes very little waiting to that path.

## Hypothesis

What is known. `main`'s `accConnsUpdate` takes `s.mu.RLock()` with `defer RUnlock` around the whole event build and queue push, so the server-wide lock is held in shared mode for that long, in front of four exclusive paths (`createWSClient`, authentication, `processConnect`, `processPing`). The effect replicates: `nolock` is faster at 800 connections in 6 of 7 runs (-11% to -26% in auth p50, including two of three profiled runs; the exception, `eks-profile-lock`, is +17% with 2 blocks), and in the 8-block run -11.4% at 800 (p 0.043) and -18.1% at 1600 (p 0.00016). What is not explained is the size. In the profiled `main` runs, wait through `accConnsUpdate` is about 2 s of block time and 0.3 s of mutex blame per 12,800 connections, about 0.16 ms per connection, against a difference of about 20 ms. Two mechanisms could close the gap and the profiles cannot tell them apart:

- **H1, hold time:** the long shared hold (event build, including waiting for `a.mu`) delays the exclusive paths. Block profiles blame the waiters and the mutex profile blames the last reader out, and both could under-attribute this in a convoy.
- **H2, any read lock:** a pending writer makes every new `RLock` park, so each connect pays a scheduler park and wake even for a short read lock, and that is on the critical path of a serialized server.

Neither is proven; a third possibility, that the cost is elsewhere and invisible to block and mutex profiles (CPU, scheduling), is not excluded.

## Prediction

- **E1 (instrument):** time spent inside `accConnsUpdate` per call on `main`, from a counting patch. Under H1 or H2 the mean per-connect time is large enough (milliseconds, not microseconds) to account for the difference.
- **E2 (discriminate):** a third arm, `shortlock`: `nolock` plus an `s.mu.RLock()` taken and released around only the cheap check, not the event build. Under H1 `shortlock` is about equal to `nolock`; under H2 it is about equal to `main`.
- **E3 (replicate):** the `main` and `nolock` arms of `relaxed-acc-conns` (8 more blocks, unprofiled): `nolock` again 10-20% faster at 800 and 1600.

## Falsifier

E1 shows the mean time in `accConnsUpdate` on `main` is around 0.2 ms: then lock behaviour inside that function does not explain a 20 ms difference, H1 and H2 are both wrong, and the next step is CPU and scheduler profiles of `main` against `nolock`. E3 failing to reproduce (`nolock` not faster) means the earlier result was chance or a stale effect, and the question is withdrawn.

## Design

E3 needs no new run. E1 and E2 are two small patches in the style of `mutex-profile` (`images/nats-server/patches/`), built on the commits compared, unprofiled, 8 blocks, 800 and 1600, `main`, `nolock`, `shortlock` with E1's counters read from the broker at the end of each storm (the probe could harvest them like the profiles: a new `/debug/vars` style endpoint is not available in nats-server, so the counters would be logged or exposed by the patch). CPU from the probes in every run. Because the callout and generator throttle, run once more with the callout at 2 CPU and a larger generator (needs a second client node) to see whether the gap changes when the pipeline in front of the broker is less constrained.

## Needs approval

E1 and E2 each need a patch (in the repo), image builds and GHCR pushes, and an EKS run (about an hour; the cluster bills about $0.80/h plus the infra node). The throttling follow-up needs a second client node (an eksctl change, billed by the hour).

## Outcome

Dropped before any run, by decision, 2026-11-21. The argument: a maintainer weighs end-to-end effect against the complexity a change adds. `nolock` is a small, general change with no behaviour trade and a measured effect of -11.4% at 800 and -18.1% at 1600 in auth p50 (8 blocks, uncorrected p 0.043 and 0.00016), faster in 6 of 7 runs; that is the result to carry, and the code already says why (the server read lock was held across the event build). Dissecting the mechanism with instrumented and `shortlock` builds is not needed for that, and the relaxed-`accConnsUpdate` line (narrow logic, a cross-server limit trade, an account-lock path the profile puts under 0.5% of lock wait) is judged not worth its complexity unless `relaxed-acc-conns` shows a large gain over `nolock`. E1, E2 and the throttling follow-up were not built. E3 (replication) comes free from the `main` and `nolock` arms of `relaxed-acc-conns`; reopen this plan only if a maintainer asks for the mechanism.

### Note, 2026-11-21 (later)

E3, the replication, is in: `eks-relaxed-acc-conns` ran `main` and `nolock` for 8 fresh blocks and `nolock` was again faster in auth p50, -15.3% at 800 and -13.0% at 1600 (p 0.00016 each, 8 of 8 blocks lower at both sizes), on top of -11.4% and -18.1% in `lock-mux-eks-retry`. The effect is real; the mechanism is still not established, and this plan stays dropped.
