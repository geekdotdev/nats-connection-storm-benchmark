# callout-replicas

**Status:** done. **Run:** [eks-callout-replicas-2026-11-22](../results/cfedersp/eks-callout-replicas-2026-11-22/NOTES.md) (run id `728a37b9ec03`, 2026-11-22 08:20 to about 08:58).

## Question

Is the auth callout what paces a synchronized connection burst? If three callout pods share the work instead of one, how much shorter is the burst, and how much lower is the per-connection latency?

## Hypothesis

In this setup (everything in one VPC, TLS WebSocket, one broker, callout at 1 CPU and 16 workers, generator at 2 CPU) a burst of 1,600 authenticated connections takes about 1.1 s to be fully admitted (800: about 0.55 s), and the callout's CPU limit is the largest single contributor. The evidence is a floor: the callout spends about 0.57 ms of CPU per connection, so 1,600 connections need 0.90 core-seconds, 80% of the 1.12 s burst (82% at 800). But the other stages are close behind, so the callout is the tightest stage, not necessarily the only one: the broker needs 1.6 ms per connection, a floor of 0.85 s at its 3 CPU limit (76%), and the generator 0.83 ms at 2 CPU, a floor of 0.66 s (59%). Measured on the `main` arms of `lock-mux-eks-retry` and `eks-relaxed-acc-conns`.

## Prediction

Written before the run. With the callout at 3 CPU the callout's floor drops to 0.30 s (800: 0.15 s), and the burst is then paced by the next-tightest stage, the broker, at about 0.85 s (800: 0.42 s). So `main_callout3` against `main`: **burst length about 10 to 25% shorter** (1,600: roughly 0.85 to 1.0 s against 1.12 s; 800: roughly 0.45 to 0.50 s against 0.56 s), **not a third**. Median auth latency falls by more than the burst length does, because the queue in front of the callout is shorter. The callout's throttled share of CPU periods falls from 53-59% to below about 10%.

## Falsifier

First the manipulation check, from `probeCpu`: the callout's throttled share must be under about 10% in `main_callout3`. If not, the replicas did not relieve the cap and the run is inconclusive. Then: if the burst length does not move (within about 3%, p over 0.1), the callout was not what paced the burst and the pacing stage is the broker or the generator, so the claim "the callout is the major contributor" is wrong. If it falls to about a third (1,600 near 0.4 s or less), the callout was the dominant stage and the broker's floor is not binding, so the 76% floor overstates what the broker needs.

## Design

`callout-replicas-variants.toml`: `main` and `main_callout3`, the same image digest (so no image push), both unprofiled with monitoring on; 8 blocks, sizes 800 and 1600, 16 units, about an hour. They run in the same blocks, interleaved, so the comparison is within one run: the report's own baseline (`main` first) is the comparison asked. Placement: three callout pods of 1 CPU and the 2 CPU generator do not fit one client node, so two client nodes are needed. The EKS cluster was taken down on 2026-11-21 evening (its kubeconfig is gone), so it is created again with them: `bench cluster up --target eks --client-nodes 2 --yes` (about 15 to 25 minutes), then `bench secrets create --target eks`, `bench monitoring up --target eks`, and preflight, which checks the placement (it could not be run for this plan: there is no cluster to read). The generator stays at 2 CPU in both variants (throttled 35-45%, floor 0.66 s at 1,600): if the burst stops falling near that, the generator is the next stage, which a follow-up with `generator_resources` raised would separate. Comparing a new `main` against `main` in old runs instead is possible but weaker: the same configuration differed by +5.2% (p 0.19) at 800 and -2.7% (p 0.15) at 1600 between the two 8-block runs, so differences under roughly 10% would not be resolved.

## Needs approval

A new EKS cluster (3 broker nodes, 2 client nodes, 1 infra node, the control plane: roughly $1.1/h) for about two hours including set-up, torn down with `bench cluster down` afterwards; the AWS SSO login must be renewed first (`aws sso login`). An AWS change, so it needs a yes. No image pushes. The code for the replica count (`callout_replicas`) is in the repository and tested.

## Outcome

Done, 2026-11-22. **The manipulation check passed** (callout throttled 42-44% with one pod, 0.0% with three). **The hypothesis, that the callout is the largest contributor and sets the burst length, is only partly supported.** At 800 connections the burst length did not move (0.53 s both), which is the falsifier's case, while median per-connection latency fell 13.3% (p 0.0003, 8 of 8 blocks) and the drain rate rose 14.1% (p 0.00016). At 1600 the burst was 12.2% shorter (1.19 to 1.04 s), inside the predicted 10 to 25% band, and median latency fell 13.5% (p 0.007), but that rests on two blocks of eight (the median of block medians fell 7%), and the 90%-admitted time, peak queue and drain rate did not move significantly. With the callout uncapped the generator became the more throttled stage (47-57% of periods) and the broker ran at 2.4 of 3 cores. So the callout is one of three stages near capacity, the tightest by its CPU floor but not the only one; lifting it buys per-connection latency, not a much shorter burst. Not tried: raising the generator's CPU as well (the follow-up the Design section named); that would separate the generator's cap. An unexpected finding in this run: the rebuilt cluster gave the same configuration 13-15% lower latency than earlier clusters (see the run's notes), so runs on different cluster builds must not be compared.
