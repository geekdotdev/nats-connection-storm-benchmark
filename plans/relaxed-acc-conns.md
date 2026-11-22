# relaxed-acc-conns

**Status:** done. **Run:** [eks-relaxed-acc-conns-2026-11-21](../results/cfedersp/eks-relaxed-acc-conns-2026-11-21/NOTES.md) (run id `6bd3204a85ad`, 2026-11-21 17:11 to 18:10): idea dropped by the rule set before the run.

## Question

Can `accConnsUpdate` be relaxed on a server where one account carries the connections and no peer relies on its connection count, and does that make a burst of connections faster?

## Hypothesis

Sending a connection-count event for every client added or removed (an event built under the account lock and two queue pushes per connect) costs time on the connect path; coalescing it to one update per account per 100 ms removes that cost. Correct only where no other server enforces limits from the count.

## Prediction

`relaxed` is faster than `nolock` in auth p50 and in the 90%-admitted-by time at 800 and 1600 connections (written before the run, in `relaxed-variants.toml`).

## Falsifier

`relaxed` is about equal to `nolock` (differences within noise, the permutation p large): then the event is cheap and the server lock was the cost, and relaxing the account lock is not worth a code change. Also falsified if `relaxed` is slower.

## Design

Variants `main`, `nolock` and `relaxed` (= `nolock` + `images/nats-server/patches/relaxed-acc-conns.patch`, so the only difference from `nolock` is the relaxation); 8 blocks, sizes 800 and 1600, 24 units, unprofiled, monitoring on (CPU from the probes). Stack `eks`, one broker. Known limits: the callout (about 60% throttled at 1 CPU) and the generator (about 35% at 2 CPU) sit in front of the broker and can mask a broker effect; the patch gives up immediate cross-server limit enforcement (five existing nats-server tests assert it and fail, see `images/PUBLISHED.md`), so it is for a standalone server only.

## Needs approval

Done for this run: the `relaxed` image push (GHCR) and the EKS run. A follow-up that gates the relaxation on "no routes, gateways or leaf remotes" would be a new plan.

## Outcome

Done, 2026-11-21. **`relaxed` against `nolock`:** auth p50 -12.0% at 800 (p 0.023) and -5.2% at 1600 (p 0.044); peak queue -6.3% (p 0.34) and -4.2% (p 0.014); 90%-admitted-by, drain rate and burst length unchanged. Corrected for the two sizes the 800 result is marginal and the 1600 result is not. The prediction held in direction; the falsifier ("about equal, p large") did not fire either; the decision rule fixed before the result ("unless clearly faster than `nolock`, drop it") applies: a gain of 5 to 12% in median auth time, with no change in how fast the burst is admitted, is not worth narrow broker logic that gives up cross-server limit enforcement and fails five existing nats-server tests. **The relaxed-`accConnsUpdate` line is dropped.** Not tried: `relaxed` with the callout and generator uncapped (the callout was throttled in 53-59% of periods in every arm, so a small broker gain may be understated); reopen only if a maintainer asks.

**The replication, which was the other half of this run:** `nolock` against `main` again, with 8 fresh blocks: -15.3% at 800 and -13.0% at 1600 (p 0.00016 each), lower in 8 of 8 blocks at both sizes. Together with `lock-mux-eks-retry` (-11.4%, -18.1%) that is two independent 8-block runs in the same direction. See the run's notes.
