# mux-uncapped-callout

**Status:** planned. **Run:** none yet.

## Question

Does auth-callout reply multiplexing (one shared reply subscription per account, branch `feature/auth-callout-mux-on-lockfree`) make the broker faster than `nolock` alone when the callout and the load generator are not what limits the burst?

## Hypothesis

The earlier null result is masked, not real. In `lock-mux-eks-retry` `mux_nolock` against `nolock` was -0.1% in auth p50 at 800 (p 0.97) and +5.0% at 1600 (p 0.19), but the callout (1 CPU, 16 workers) was throttled in about 60% of its CPU periods during bursts and the generator (2 CPU) in about 35% (`eks-probe-check`). When they set the pace, a saving inside the broker has nowhere to show. With the callout at 3 CPU and 48 workers (the same 16 workers per CPU as before) and the generator at 3 CPU, the pipeline's cap is lifted and a real mux effect should be easier to resolve. The in-process benchmark in PR 8908 gave 5-8% for this change on top of the lock-free one.

## Prediction

`mux_nolock` is faster than `nolock` in auth p50 at 1600 and at 800, with a lower peak queue, by an amount of the order of the 5-8% the in-process benchmark showed. Written before the run.

## Falsifier

First the **manipulation check**, read before the effect: from `probeCpu`, the callout's and the generator's share of throttled CPU periods in the bursts must fall well below the earlier 60% and 35% (below about 10%) in both variants. If they do not, the run did not test the hypothesis and is **inconclusive**, not a refutation. If the check passes and `mux_nolock` is within about 3% of `nolock` with a large p (over 0.1), mux gives nothing even when the pipeline is not capped, and the idea is dropped, including the "another 5-8%" sentence in the PR. If it is slower, say so.

## Design

`mux-uncapped-variants.toml`: variants `nolock` and `mux_nolock` (same images and digests as `lock-variants.toml`), both with the identical `callout_args` (`--workers 48`), `callout_resources` (cpu 3) and `generator_resources` (cpu 3); 8 blocks, sizes 800 and 1600, 16 units, about an hour; unprofiled, monitoring on. `nolock` is first, so the report's own baseline is the comparison asked. Placement: a 3 CPU callout and a 3 CPU generator do not fit one `c6i.xlarge` beside the cluster's own pods, so a second client node is needed (`bench cluster scale --target eks --client-nodes 2 --yes`; preflight's "fit together" check says whether it fits). The broker (3 CPU, 2.3 cores busy in bursts, never throttled) may become the next limit; that is a result, not a failure. Run only when no other run is active, and not while the node is being added.

## Needs approval

One extra `c6i.xlarge` for about 1.5 hours (about $0.17/h, on top of the cluster's roughly $0.80/h), then `bench cluster scale --client-nodes 1 --yes` to remove it. An AWS change, so it needs a yes. No image pushes (the digests are the existing ones). The only new code is `bench cluster scale`.

## Outcome

Not yet.

### Note, 2026-11-21 (later): the cap is already quantified

The config-mode runs (`eks-800-1600`, `eks-smoke`, `kind-main-vs-lockfree`) could not stand in for this run: they have no CPU readings and no per-connection samples, and they did different work (a password callout, an older generator). The operator-mode runs already show the callout is the pacing stage. From `probeCpu`, the callout spends 0.56-0.57 ms of CPU per connection in every variant, so 800 connections need 0.46 core-seconds and 1600 need 0.90. At a 1 CPU limit that is a floor on burst length, and the observed bursts (0.56 s and 1.12 s) are within 80-82% of it, with the callout throttled in 53-59% of its periods. That also explains why `nolock`, `relaxed` and `mux_nolock` move the median auth time but not the time to admit the burst. So the run's manipulation check is already predicted to matter, and its only remaining use is to see whether mux shows once the cap is lifted. That matters for a future mux PR and not for PR 8908, which does not include mux. Status left as planned; deferred unless a mux PR is wanted.

