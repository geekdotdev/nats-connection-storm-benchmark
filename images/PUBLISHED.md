# Published images

The container images this project pushes to GHCR, with their digests, and which are current. The tables are reference, not history: a run's notes (`results/<run>/NOTES.md`) say which image rows a run used, and `results/INDEX.md` lists the runs. Companion to [README.md](README.md) (building and pushing) and [../CLAUDE.md](../CLAUDE.md).

**Add a row whenever an image is pushed**, with the tag, the digest the push printed and what changed, and mark the one it replaces *retired*. A test (`tests/test_published.py`) fails when a digest pinned in an `eks` overlay or a variants file is missing from this file, so the two cannot drift.

## What is published

Registry account `ghcr.io/cfedersp/`. Every image is a multi-platform list (linux/amd64 and linux/arm64) and is public, so EKS nodes pull it without credentials (`bench preflight` checks this). Deploy by **digest**, never by tag. Tags: components are `src-<12 hex>` (a hash of their source), the broker is `<12 of the commit sha>`, plus `-<patch>` when a patch is applied. kind uses locally built `localhost/<name>:dev` images that are never pushed.

### `nats-server` (the broker, built from a commit)

| Variant | Tag | Digest | Built from | Notes |
|---|---|---|---|---|
| `main` | `582fb8e939a7` | `sha256:7e89e5a9ad5adf167322479a901b13df6750f4ff7ec3e254eaffa00113aedf5d` | upstream `main` at 582fb8e: `accConnsUpdate` takes the server lock for reading (#8905) | the baseline; pinned in `deploy/stack/overlays/eks` and `lock-variants.toml` |
| `nolock` | `37260075e531` | `sha256:4214ee07b2bbf7098cbccdaccc8dc1eaf2c67d1999dfa95f1b220eff264f3186` | branch `feature/lockfree-acc-conns-update`: an atomic pointer to the system send queue, so `accConnsUpdate` takes no server-wide lock | |
| `mux_main` | `ec9244c3444d` | `sha256:0676f13113d55867c00c5a1cb76bffc6057896a9adb461892a0570354355e0aa` | branch `feature/auth-callout-mux-on-main`: auth callout reply multiplexing on `main` | experiment set aside 2026-11-16 |
| `mux_nolock` | `7ea787e2b5ee` | `sha256:d35b480b8d136f25d83e5eabba251b15147772a1672c4bd4158c766f7ae9f5cb` | branch `feature/auth-callout-mux-on-lockfree`: the multiplexing on `nolock` | |
| `main_mutex` | `582fb8e939a7-mutex-profile` | `sha256:e5ad6a92cf954403f7b1ad8117d62801e33223ba44398b68a625c4ae4ac4c976` | `main` plus `images/nats-server/patches/mutex-profile.patch` | one added line, `runtime.SetMutexProfileFraction(1)`: the mutex profile exists only here; labelled `benchmark.patch`. **Never compare with unpatched images** |
| `nolock_mutex` | `37260075e531-mutex-profile` | `sha256:cdd54d7983f1321a5d96d7f6db2493d35855175b854cc008c3046f7dc6d8e6e7` | `nolock` plus the same patch | |
| `relaxed` | `37260075e531-relaxed-acc-conns` | `sha256:01b0f78412f07175657cf37c72455445858b72c17b6a914fc651b4e067b4f001` | `nolock` plus `images/nats-server/patches/relaxed-acc-conns.patch` | `accConnsUpdate` coalesces: one update per account per 100 ms instead of an event per connect, with no account lock on the connect path. **Gives up** immediate cross-server enforcement of account connection limits (5 existing nats-server tests assert it and fail; see the run's notes). Built on `nolock` so the only difference from it is the relaxation. Not for clusters. Labelled `benchmark.patch`; never compare with unpatched images except as the experiment |

The branches are in `github.com/cfedersp/nats-server` (build with `--repo https://github.com/cfedersp/nats-server.git`). Variant files: `lock-variants.toml` (the first four), `mutex-variants.toml` (the patched pair), `profile-variants.toml` (`main` and `nolock` with the profiler on).

### The benchmark's own components

| Image | Tag | Digest | Status | What it is |
|---|---|---|---|---|
| `callout` | `src-3cff67e67dd2` | `sha256:21ca37a09635ed93f8ef7ef55c841e003dea3795ebdddfcfdf20b99a1f49ed23` | current | the auth callout responder (Go) |
| `provision` | `src-c1a3acc650f6` | `sha256:b01b5bc532eac54cd7b0e97f9fd60266527f8e54158138628d3e2f0122ae81f1` | current | the callout pod's init container: operator, keys, resolver (Go) |
| `loadgen` | `src-699acf4c7c39` | `sha256:35ad2f3aa8dfcfdef43e466a181d83ec4b4993cf2c3e06eb149c1fe236352673` | **current** | the load generator (Rust): adds bad-faith connections (`STORM_INVALID_FRACTION` etc, `plans/bad-faith-storm.md`); per-connection timings, its own cgroup CPU, and it asks the CPU probes (`STORM_CPUPROBE`) at each burst's start and end |
| `loadgen` | `src-a3d7265ebd6e` | `sha256:c82321826578f33bddd790c1ad2e297b2a8381398e884b544305ffabba3a6099` | retired | the same, before bad-faith connections existed |
| `loadgen` | `src-16759d90c2a9` | `sha256:93d0fc352a2ac997b1554de429bf29840135c7b256a86759fa3964f95db73cf9` | retired | timings and its own CPU, but **does not ask the probes**: a run with monitoring on has no `probeCpu` (found in `eks-monitoring-check`) |
| `loadgen` | `src-f92df7b220d0`, `src-d4e091f900ab` | not recorded here | retired | timings only; neither (the older ones are in the earliest runs) |
| `cpuprobe` | `src-4682f1886471` | `sha256:305ae94ea239ba26cd9f8d018e0861353f5c6f8a60b12bed195f49ce1cca242d` | **current** | the CPU probe DaemonSet (Go): cgroup counters on request and `/metrics`, and the broker's **block and mutex** pprof profiles every 2 s |
| `cpuprobe` | `src-61b99ccb898f` | `sha256:ddb8fc5214260cdd7bba82cafc571decb5a92c465ec24c288a377559d606aff1` | retired | harvested the block profile only |
| `cpuprobe` | `src-bf62150bbb28` | `sha256:f448f153d27e1348144ba03a7a5ebb4c267645dde490ed64c25dec3a1de98a4d` | retired | CPU only, no profile harvester |

Not on GHCR but pinned: Prometheus `quay.io/prometheus/prometheus@sha256:fa3302279710be2f21a8301ec17a8b4f2301fd829bd113839b3b6b02b6f191a2` (v3.15.1, in `deploy/monitoring/base/prometheus.yaml`).
