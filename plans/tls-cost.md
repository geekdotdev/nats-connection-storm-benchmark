# tls-cost

**Status:** done. **Run:** [eks-notls-2026-11-22](../results/cfedersp/eks-notls-2026-11-22/NOTES.md) (run id `c1ac47e5ed7e`, 2026-11-22 11:22 to about 12:02).

## Question

How much of a connection's setup time, and of the broker's and the generator's CPU per connection, is the TLS handshake? Setup (TCP, TLS, the WebSocket upgrade, INFO: everything before CONNECT) is about two thirds of a connection's latency (`eks-callout-replicas`: 213 of 345 ms at 800 connections, 441 of 679 ms at 1600). Which part of it is TLS?

## Hypothesis

The broker spends about 1.6 ms of CPU per connection and the generator about 0.83 ms; the broker's CPU appears to pace setup (`plans/broker-cpu-dose-response.md`, running). The certificate is ECDSA P-256 (`secretgen.py`) and TLS 1.3 with an elliptic-curve key exchange is cheap per handshake: of the order of 0.15 to 0.4 ms of CPU on the broker's side and 0.15 to 0.3 ms on the generator's. So TLS should be a minority of the broker's per-connection CPU, and the rest of the setup (accepting the TCP connection, the WebSocket upgrade, the server-wide lock held while the client is registered) the majority. This is an estimate, not a measurement; the run measures it.

## Prediction

Written before the run. With TLS off: the broker's CPU per connection falls by about 0.15 to 0.4 ms (roughly 10 to 25% of 1.6 ms); the generator's by about 0.15 to 0.3 ms (roughly 18 to 36% of 0.83 ms); setup time falls by about **10 to 30%** at both sizes; total p50 falls by less than that, because auth is unchanged. Every connection is admitted in both variants.

## Falsifier

First the check that the switch took: every connection admitted in `notls` and the broker's CPU per connection and the generator's lower than in `tls`. A failure to connect would stop the run loudly (the generator goes to `ws://` only for that variant). Then: if setup time falls by under about 5% and the broker's CPU per connection by under 5%, TLS is not a meaningful part of setup, and the time is in the accept path, the WebSocket upgrade or the lock, which the CPU dose-response and the profiles point to. If setup falls by over 50%, TLS dominates the setup, which would be surprising for an ECDSA certificate, and the cost of the handshake (session resumption, a larger broker CPU, or terminating TLS at a load balancer) becomes the next question.

## Design

`notls-variants.toml`: `tls` (the default, first) and `notls` (`no_tls = true`), the same `main` image digest, callout (1 pod, 1 CPU), generator (2 CPU) and broker (3 CPU) as in the earlier runs; 8 blocks, sizes 800 and 1600, 16 units, about 40 minutes; unprofiled, monitoring on. In the same blocks, interleaved: within-run, the report's own baseline. `bench report` prints setup and total per variant and tests each against the baseline. The broker side is the websocket block in `extra.conf` (checked against nats-server: it then listens on `ws://`, and logs that it is not for production); the generator side is `STORM_URL=ws://broker:8443`, set only for that variant. Run on the cluster `eks-callout-replicas` and `eks-broker-cpu` used; do not compare with runs from other cluster builds. This is a diagnostic: nothing here says production should run without TLS.

## Needs approval

About 40 minutes of the EKS cluster, which is up and billing (about $1.1/h). It runs after `eks-broker-cpu` finishes (one run at a time). No image pushes, no extra nodes.

## Outcome

Done, 2026-11-22. **Setup fell ~51% (800: -52.8%, p 0.00016; 1600: -50.5%, p 0.00016), at the falsifier's own upper boundary** ("over 50% would be surprising for an ECDSA certificate"). TLS is a much larger share of setup than estimated: broker CPU/connection fell 25% (top of the predicted 10-25%) but generator CPU/connection fell 49-50%, far past the predicted 18-36% - the client-side handshake costs more than guessed. Total fell 26-28%, less than setup, as predicted. **Falsified: auth did not stay flat, it rose** (+25.5% at 800, +10.2% at 1600). Cause, confirmed from the callout's own CPU data: the callout's per-connection cost is unchanged (TLS is not callout work) but its throttled share rose 43% to 54% in both sizes - with setup twice as fast, connections reach the already-saturated 1 CPU callout more concentrated in time, so TLS had been incidentally pacing arrival at the real bottleneck. This both answers the original question (TLS is most of setup, not a minor part) and sharpens the picture from `eks-broker-cpu` and `eks-callout-replicas`: the stack now has three stages near capacity whose order matters - speeding up any one without the others can make total latency better while making a downstream stage's latency worse. Not tried: no_tls together with more callout replicas, which the conservation argument says should now help more than it did at full TLS.
