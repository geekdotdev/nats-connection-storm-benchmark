# broker-cpu-dose-response

**Status:** done. **Run:** [eks-broker-cpu-2026-11-22](../results/cfedersp/eks-broker-cpu-2026-11-22/NOTES.md) (run id `b2da4adbaaa1`, started 2026-11-22 10:03).

## Question

What paces the first two thirds of a connection's latency: the broker's CPU? A connection's time from the start of the connection to the answer (`total`) is its setup (TCP, TLS, the WebSocket upgrade, INFO: everything before CONNECT is sent) plus the auth step. If the broker's CPU limit paces the setup, the setup time should change in proportion when the limit changes.

## Hypothesis

From `eks-callout-replicas`, `main`, one callout, 3 CPU broker: setup is 213 ms of a 345 ms total at 800 connections (62%) and 441 ms of 679 ms at 1600 (65%). Four observations point at the broker's CPU, none proves it:

1. **Setup grows in proportion to the burst** (213 to 441 ms for twice the connections). If all connections arrive at once and are served at a fixed rate, the median one finishes after half the burst has been served: that gives about 1,800 setups per second at both sizes. The broker spends about 1.6 ms of CPU per connection, so 3 CPU can do 3 / 1.6 ms = 1,875 per second.
2. **Conservation:** lifting the callout's cap (three pods) lowered auth by 13% but raised setup by 13.4% at 800 (p 0.0065) and 24.4% at 1600 (p 0.0096), and left the total unchanged (+3.7% and +10.8%, not significant). Faster auth did not make connections faster overall: the time moved to the stage before it. A resource shared by both stages would do that; the broker's CPU is the candidate (2.4 of its 3 cores were busy).
3. The broker's lock contention (`eks-mutex-profile`) is all in this connection set-up path.
4. The broker was not throttled (at most 1.4%), but at 2.4 of 3 cores busy that is 80% of its limit.

The alternatives: the setup is serialized on the server-wide lock and the CPU limit hardly matters; the generator (2 CPU, throttled in 37-57% of its periods) paces it from the client side; or the kernel's accept path (a SYN or accept queue) does. The dose-response separates the first two.

## Prediction

Written before the run. If setup time is CPU-bound its time is inversely proportional to the limit: setup time = N x 1.6 ms / (2 x CPU limit), with the measured 3 CPU values as the anchor. Predicted setup time (mean of block medians), `cpu3` as measured with one callout:

| Broker CPU | 800 connections | 1600 connections |
|---|---|---|
| 1.5 | about 430 ms | about 880 ms |
| 2 | about 320 ms | about 660 ms |
| 3 (`cpu3`, baseline) | 213 ms | 441 ms |
| 3.5 | about 183 ms | about 378 ms, or less gain if the generator becomes the limit |

So at 2 CPU setup is about **50% longer** and at 1.5 CPU about **twice** as long as at 3. Secondary: the broker's throttled share of CPU periods rises as the limit falls (small at 3 and 3.5, large at 2 and 1.5); the burst length rises when the broker becomes the slowest stage (at 2 CPU its floor is 1600 x 1.6 ms / 2 = 1.3 s, above the callout's 0.9 s), so about 1.5 s at 1600 against 1.2 s, and about 2 s at 1.5 CPU; the auth part may fall as the setup stretches, because connections then reach the callout more spread out, so the total rises by less than the setup does.

## Falsifier

First the manipulation check, read before the effect: the broker's throttled share (`probeCpu`) must rise clearly with the lower limits (over about 30% at 2 CPU and 1.5 CPU) and stay low at 3 and 3.5; the broker's cores busy must sit at about the limit at 1.5 and 2. If not, the limit did not bind and the run is inconclusive. Then: if setup time at 2 CPU is less than 15% above `cpu3` (the prediction is 50%), the broker's CPU does not pace the setup: it is serialized elsewhere (the lock), or paced from the client, and the hypothesis is wrong. If it rises by far more than the model (over 100% at 2 CPU), CPU matters even more than 1/CPU, for instance because throttling stalls amplify the wait. `cpu35` showing no gain does not falsify it: it would point at the generator as the next limit.

## Design

`broker-cpu-variants.toml`: four variants of the same `main` image, broker CPU 3 (baseline, first), 1.5, 2 and 3.5; callout (1 pod, 1 CPU) and generator (2 CPU) as in the earlier runs; 8 blocks, sizes 800 and 1600, 32 units, about 80 minutes; unprofiled, monitoring on. All in the same blocks, interleaved: a within-run comparison, which is the only kind the report tests; the report now prints setup and total per variant and tests each against the baseline (`bench report DIR`; the setup row is new). The pairwise p-values are not corrected for the three comparisons; the claim is the monotonic dose-response across the four levels, which the pairwise numbers should show in order. A broker's memory stays as the overlay has it. The cluster is the one `eks-callout-replicas` used; do not compare with runs made on other cluster builds (the same configuration differed by 13-15% between builds). Whether 3.5 CPU fits a broker node beside its own pods is checked by preflight; if it does not, use 3.4.

## Needs approval

EKS time: about 80 minutes of the cluster, which is up and billing (about $1.1/h) either way. No image pushes, no extra nodes. After it, tear the cluster down unless another run is planned.

## Outcome

Done, 2026-11-22. **The hypothesis, that setup time is inversely proportional to the broker's CPU limit, is falsified.** The check that the limit binds passed only at 1.5 CPU (throttled in 70-76% of periods; at 2 CPU just 7-8%, at 3 CPU 0-2%). Predicted against observed setup time at 1600: 1.5 CPU about 880 against 552 ms (+24%, not +100%); 2 CPU about 660 against 498 ms (+12%, not +50%); at 800 setup barely moved (+8.4%, and -12.2% at 2 CPU). The falsifier ("setup under 15% above `cpu3` at 2 CPU") fired. What the limit did affect, when it bound, was auth (+62% at 800, +73% at 1600) and burst length (+37%, +40%). The broker used at most 2.2 cores at 3 CPU and was no faster at 3.5 (its CPU per connection rose from 1.61 to 1.82-1.85 ms), the signature of a serialized section that wastes extra CPU. **Revised reading:** the broker's connection handling is capped by a serialized section (the server-wide lock the profiles found) at about 2.2 cores of useful work; a CPU limit below that costs auth time and burst length, above it buys nothing. Setup time is therefore not paced by the broker's CPU limit; what does pace it (the lock, the generator's own 2 CPU limit, or the kernel's accept path) is not separated here: `plans/tls-cost.md` (running next) and a generator CPU dose-response would separate the generator. The coincidence that setup ran at about 1,800 connections per second, near 3 CPU / 1.6 ms, was a coincidence.
