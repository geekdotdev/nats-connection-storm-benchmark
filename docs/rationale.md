# Why this benchmark runs the components as separate processes

> Draft. The "What we observed" figures come from measurements on the private stack before this repository existed; they are not reproduced here yet and must be re-measured on this stack before they are cited.

nats-server's own Go benchmarks run the server, the auth-callout responder and the load generator inside one `go test` process. `BenchmarkAuthCalloutWebSocketBurst` is an example: the server is started with `RunServerWithConfig`, the callout is an in-process client with 16 queue subscriptions, and each client connection is a goroutine. That is the right tool for regression and race testing, and this repository does not replace it. For contention-sensitive changes it has limits, because the components are not independent:

1. **One runtime for everything.** The server, the callout and hundreds of client goroutines share one Go scheduler, one garbage collector and one heap. Client allocations drive GC cycles that the server pays for. When the server waits on a lock, the CPU it would have used can be picked up by client goroutines. Real components do not share a scheduler or a GC.
2. **The measurement includes the generator.** The timer is read by goroutines that must themselves be scheduled. In an 800-connection burst, part of every "auth time" is the generator waiting for a turn. The standalone generator here reports its own scheduling delay, so a limit in the generator is visible.
3. **No real process or network boundary.** A deployment puts the callout and the clients in other processes or on other hosts, with their own CPU. Separate containers, real TLS and real WebSocket connections are closer to that.

## What we observed

The same change to `accConnsUpdate` (lock-free read of the system send queue, against upstream `main`):

| Setup | Median auth time change |
|---|---|
| In-process Go benchmark, Linux VM | -6% to -14% |
| In-process Go benchmark, macOS | no difference detected |
| Separate processes, storm of 800 | -34% |
| Separate processes, storm of 1600 | -44% |

## What we have not shown

That the shared runtime is the reason for the difference. Other things changed at the same time: the storm goes to larger bursts, uses TLS, and arrives at the broker all at once. The checks that would separate them are:

- the Go benchmark at burst sizes of 1600 and 3200;
- a `GOMAXPROCS` sweep on the in-process benchmark;
- running the broker as its own process under the same benchmark.

## How the evidence is produced

- A minimal callout that makes no external calls, so the benchmark measures the server and not the callout.
- A standalone Rust load generator.
- Pinned toolchains and base images, and the environment recorded with every run.
- Interleaved A/B blocks, exact permutation tests on block medians, and raw data published.
- Within-run comparisons only. Absolute numbers from different machines are not comparable.
