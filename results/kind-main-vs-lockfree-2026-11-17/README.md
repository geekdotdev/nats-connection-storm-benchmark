# nats-server `main` against a lock-free `accConnsUpdate`, on one kind node

Run 2026-11-17, 21:10 to 21:36 PST, with `bench run --a 582fb8e... --b 37260075e... --a-label main --b-label lockfree --blocks 8 --sizes 800,1600`. `summary.txt` is the report; `environment.json` records the arms, the design and the node.

## Arms
- `main`: upstream `582fb8e939a7` (the `RLock` change, #8905).
- `lockfree`: commit `37260075e531` of a fork, which makes `accConnsUpdate` read the system send queue through an atomic pointer instead of taking the server lock. It does **not** include a later, uncommitted fix (a re-check under the account lock so a call that races with shutdown cannot leave a connection-count timer armed). Earlier in-process benchmarks showed that re-check costing nothing detectable, but this is not the final code.

## Result (mean of 8 block medians of the median auth time, CONNECT to PONG)
| phase | main | lockfree | change | exact permutation p |
|---|---|---|---|---|
| idle (1 connection at a time) | 3.3 ms | 3.3 ms | +1.1% | 0.81 |
| storm of 800 | 43.5 ms | 39.4 ms | -9.3% | 0.058 |
| storm of 1600 | 56.5 ms | 52.1 ms | -7.8% | 0.11 |

Every connection was admitted (51,200 and 102,400 per arm in the storm phases, 320 in idle). The whole-burst length did not change (+0.0%). **Neither storm difference reaches p < 0.05.** The direction is the same in both sizes, but the block medians of the two arms overlap.

## Conditions that limit what this shows
- One kind node on an 8 vCPU podman VM: the broker, the callout and the load generator share the same cores. The generator's own scheduling delay p99 was about 4 to 5 ms (median over bursts, max 26 ms), so the generator is not free of the measurement.
- The VM also ran 12 other containers (an unrelated demo stack) at the same time, mostly idle.
- The callout is the minimal one in `components/callout`, not an identity-provider-backed one.
- One machine, one run of 8 blocks. Nothing was repeated.
- The image IDs of both arms are in `raw/*.meta.json`; the callout and load generator image IDs are not recorded.
