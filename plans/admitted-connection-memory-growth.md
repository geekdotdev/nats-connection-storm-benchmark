# admitted-connection-memory-growth

**Status:** dropped (2026-11-22: proposed, then paused before any step was built — satisfied with the broker's performance and sound in the findings so far; revisit when there is a reason to scale). **Run:** none.

## Question

Split out of `plans/held-connection-exhaustion.md`, which found it by accident while investigating something else entirely: across two runs sharing almost nothing (one pure good-faith and unramped, one 80% bad-faith and ramped over 30s, 22x different duration), the broker's resident memory grew by about the same **27KB per *admitted* connection**, with no sign of leveling off over 24 minutes and 48,000 admitted connections. What is actually growing — which process (broker, callout, or both), which allocation site, and is it bounded by anything before it reaches the broker's 4Gi cgroup limit? Unlike every other plan here, this is not about bad faith at all: it is a question about what any sustained volume of *ordinary* connection churn costs this stack over time.

## Hypothesis

Something on the connection-admission path retains memory per connection after that connection closes, rather than freeing it. Candidates, in rough order of how cheaply each can be ruled in or out:
1. **The callout, not just the broker.** Nothing has measured the callout's own memory yet — `varz` is `nats-server`-specific, and `cpu_snapshot`'s existing cgroup read (`kube.py`, `CGROUP_FILES`) only reads `cpu.stat`/`cpu.max`, never `memory.current`, for either the broker or the callout. The broker's growth is measured; the callout's is not measured at all.
2. **A specific, identifiable allocation site in the broker**, reachable from a GC root (so it shows up in a heap profile's `inuse_space`) rather than unreachable memory the allocator just hasn't returned to the OS (which would not show growth in a heap profile, only in RSS). `CLAUDE.md`'s own pitfalls already note that a connection's write loop blocks on a `sync.Cond` "for as long as the connection lives" (`plans/*` profiling notes) — worth checking literally whether anything about a *closed* connection's goroutines, buffers, or per-client bookkeeping is kept alive past `removeClient`, not just busy while the connection is open.
3. **The auth callout round trip's own bookkeeping** (`processClientOrLeafCallout`'s correlation of a request to its reply) retaining an entry per connection past the reply (or the timeout) arriving, rather than cleaning up on both the success and failure paths.

## Prediction

1. **The callout's own memory (cgroup `memory.current`, not yet read by anything) stays flat** while the broker's climbs, once measured directly — i.e. this is a broker-specific cost, not shared by every component on the connection path. (If the callout *also* grows, that points toward something in the JWT/account machinery common to both rather than the broker's own client bookkeeping.)
2. **A heap profile (`inuse_space`) diffed between an early and a late point in a long, bad-faith-free run shows growth concentrated in one or a small number of allocation sites**, not spread diffusely across the heap. `cpuprobe` already harvests `block` and `mutex` profiles from the broker's existing `prof_port` (`components/cpuprobe/profiles.go`, `Kinds: []string{"block", "mutex"}`) — Go's pprof HTTP interface serves `heap` from the same port with no broker-config change, so this is extending an existing, working harvester by one more kind, not building new infrastructure.
3. **The ~27 KB/admitted-connection rate replicates, within the same order of magnitude, on a completely different burst shape** — many small bursts (e.g. 800 connections, a size this benchmark has run hundreds of times before, known not to throttle the generator) rather than few large ones. If it does not, burst *size* or *shape* was the real driver all along, not cumulative admitted connections, and the agreement between the two runs so far (`main` and `main_exhaustion`) was coincidence.
4. **A run long enough to approach the 4Gi limit (order of 150,000 more admitted connections than any run so far, extrapolating the observed rate) either keeps climbing toward it, or the growth rate slows as the heap approaches some internal Go GC threshold.** This prediction is the one with a real, intended-to-happen-if-true failure mode: a broker OOMKill. That is the point of running it, not an accident to avoid (see Needs approval).

## Falsifier

- If a heap profile's `inuse_space` diff is flat (no growing allocation site) while RSS (`/varz`'s `mem`, or the cgroup's own `memory.current`) keeps climbing, this is not a classic Go memory leak (reachable objects accumulating) but allocator/OS-level retention or fragmentation — a real finding, but a different one, needing a different fix (GC tuning, `debug.FreeOSMemory`, `GOMEMLIMIT`) rather than a specific structure to stop retaining.
- If the per-admitted-connection rate does not replicate on a smaller-burst, many-more-reps shape (Prediction 3), revisit whether cumulative admitted connections is really the driving variable, rather than burst size, arrival pattern, or something specific to size-6000 bursts.
- If the callout's memory also grows at a comparable rate (Prediction 1 fails), the cause is more likely something both processes share (the JWT library, the TLS stack, something in how each connection's identity is handled) than broker-specific client bookkeeping.

## Design

Three steps, increasing in cost and risk, meant to be done in order — each one either narrows where to look or answers the question outright, so a later step is only worth doing if an earlier one does not already settle things:

1. **Read the callout's cgroup memory too.** `kube.py`'s `cpu_snapshot` already execs into the broker and callout pods for `cpu.stat`/`cpu.max` (`CGROUP_FILES`); adding `memory.current` (and `memory.peak` where the kernel has it) to the same read, for both components, is the same mechanism, one more file. Bracket a long run the same way CPU already is (before/after), or extend `varz.py`'s poll pattern to the callout too (continuous, not two snapshots — this question is exactly the kind "before/after" under-samples, per the lesson `held-connection-exhaustion.md` already learned the hard way). Cheapest step; do this first regardless of what else gets approved.
2. **Add `heap` to `cpuprobe`'s harvested profile kinds** (`components/cpuprobe/profiles.go`: `Kinds: []string{"block", "mutex"}` → add `"heap"`) and a driver-side diff for it in `profiling.py` (a new sibling to the existing `_traces`/`waits`/`holders` functions, reading `inuse_space`/`inuse_objects` instead of the `delay` sample `parse_traces` currently assumes — `go tool pprof -top -inuse_space -base=<early> <late>` rather than `-traces -sample_index=delay`). Take one snapshot early in a long run (after a short warm-up) and one late, and diff them, exactly as block/mutex profiles already are. No broker config change: `prof_port` already serves every standard pprof path.
3. **A long enough run to approach the 4Gi limit.** At the observed rate (~27 KB/admitted connection), reaching it needs on the order of 150,000 further admitted connections beyond any run so far — at this benchmark's own admission rates, something on the order of an hour or more of sustained load, run with **no bad faith at all** (isolates the admission-path cost cleanly, and avoids re-raising the generator-throttling finding from the calibration run, which only showed up unramped at size 6000; smaller, repeated bursts sidestep it). This step is explicitly trying to find out whether the broker can be made to run out of memory from pure, ordinary connection churn — a different risk profile from every other run in this project, which assumed the broker stays healthy throughout.

Steps 1 and 2 need the same `STORM_RUNS`-bypass technique `eks-exhaustion-memory-probe-2026-11-22`'s one-off script used (the plan's own phases reserve `STORM_RUNS`, so a long run cannot go through a variants file).

## Needs approval

1. **Reading the callout's cgroup memory** (`kube.py`/`cpu.py`, a small extension of existing, working code) — no cluster cost to write, answers Prediction 1 cheaply. Not done.
2. **Adding `heap` to `cpuprobe`'s harvested kinds, plus a driver-side heap-profile diff** (`components/cpuprobe/profiles.go`, `profiling.py`) — a new image build and push for `cpuprobe` (small code change, same harvester pattern already running in production for block/mutex), plus new driver code. Not done.
3. **A long run large enough to approach the 4Gi limit** — real EKS time (on the order of an hour or more at ~$1.1/h, so a few dollars, not a few cents like every run so far), and a real chance of actually OOMKilling the broker pod, which this step wants to see happen, not avoid. Worth saying plainly before it is approved: this is the first run in this whole project designed to try to break the broker, not to measure it staying healthy. Not done.

## Outcome

Dropped before any step was built: see Status. The question stands, documented here for when benchmarking resumes.
