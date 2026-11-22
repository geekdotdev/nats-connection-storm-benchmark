# eks-badfaith-sanity-2026-11-22

<!-- generated from environment.json: do not edit between the markers -->

- **Run:** cluster eks · namespace storm-bench · run id cd537d1a4771 · started 2026-11-22T13:24:48-08:00 · run by cfedersp
- **Target:** eks, auth operator mode, memory resolver
- **Design:** 1 blocks, sizes [200], phases warm, idle, n200
- **Variants file:** bad-faith-variants.toml (sha256 33c9a1007f23…, copy in `variants.toml`)
- **CPU source:** probe; monitoring: probe and Prometheus, scrape every 1 s

| Variant | Commit | Image |
|---|---|---|
| `main` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |
| `main_badfaith` | 582fb8e939a7 | nats-server@sha256:7e89e5a9ad5a… |

<!-- end generated -->

## Purpose

A sanity check of the bad-faith generator built in [plans/bad-faith-storm.md](../../../plans/bad-faith-storm.md): one block, 200 connections, 60% good-faith and 10% each of `invalid`, `nonexistentAccount`, `forged`, `incompleteHandshake`, run over the deployed `wss://` configuration on the generator's `eks-restricted` `NetworkPolicy` overlay (ingress denied, egress limited to DNS, the broker's 8443, and the CPU probe). Checks the four kinds behave as traced from source before committing to the plan's full ratio sweep (0%, 25%, 50%, 100%+ across 8 blocks). This run also shook out a real bug: the Python driver's `KubeBackend.prepare()` (`driver/stormbench/kube.py`) assumed `kubectl kustomize <overlay> | kubectl create --dry-run=client -o json -f -` always renders exactly one object; the `eks-restricted` overlay renders two (the Job and the new NetworkPolicy), which `kubectl` prints as two concatenated JSON values, not a list — `json.loads()` failed with `Extra data`. Fixed by reusing `preflight.py`'s existing multi-object parser (moved to `k8s.json_stream`) and picking the `Job` object out explicitly; regression test in `tests/test_jobs.py`. No change to the Rust generator was needed.

## What it showed

`invalid` and `nonexistentAccount` behaved as predicted: fast rejections (p50 18.5ms and 21.6ms, p90 39ms and 44ms), cheaper than the good-faith auth median in the same burst (~37ms). All 120 good-faith connections in the mixed burst were admitted, 0 errors.

**`forged` did not.** All 20 `forged` attempts (a genuinely valid, unmodified ticket, but the CONNECT frame's nonce signed with an unrelated throwaway key) landed in a tight band of 10020-10047ms before being refused with `-ERR 'Authorization Violation'` — indistinguishable in cost from `incompleteHandshake` (10006-10008ms, which errors with "closed before the broker answered" since the broker never gets a CONNECT at all). This falsifies Prediction #1 in the opposite direction from the one named in the Falsifier: not "too cheap to tell apart from nonexistentAccount," but catastrophically more expensive — as expensive as the kind the plan called out as the one most worth caution over.

Traced the mechanism against `nats-server` source at the pinned commit (582fb8e939a77b6a33ef845955a208bdc222d767) and this repo's own callout (`components/callout/operator.go`), via two focused source-reading passes (not assumed):
- `processClientOrLeafAuthentication`'s local nonce check (`pub.Verify(c.nonce, sig)`) does run and does correctly fail for `forged`. But for a JWT-ticket client whose account requires external auth, that `false` does not short-circuit: a deferred closure unconditionally re-delegates to the auth callout afterward.
- The request the broker sends to the callout (`$SYS.REQ.USER.AUTH`) carries a **server-generated, one-time ephemeral key** as `UserNkey` (`nkeys.CreateUser()`, "to prevent replay attacks" per the broker's own comment) — not the client's real key. The client's actual nkey and signed nonce pass through only as unverified fields of `ConnectOptions`.
- This repo's callout (`operator.go`) never reads those fields at all — it only decodes the ticket JWT and, if it's validly issued (which an untouched `forged` ticket is), mints an accept keyed to whatever `UserNkey` it was given, which should return quickly.
- Despite that, the connection still dies at ~10s with the callout-path's own error text (`Authorization Violation`, matching the broker's `authorization.timeout`/`Options.AuthTimeout` value, not the separate "Authentication Timeout" string the no-CONNECT-at-all timer uses). The error text alone confirms this is the callout-timeout path, not the generic idle-connection timer.
- The last link — why an apparently-fast callout accept still leaves the exchange waiting the full timeout — was not pinned to a specific line: the callout pod serving this run's traffic was recycled (an unrelated diagnostic rollout restart) before its own accepted/rejected/dropped counters could be read, and a second rollout (after the run had already finished) only showed a fresh pod's zero counts. Re-running to recapture those counters was judged not worth the cluster time for a sanity check; the practical finding (the cost, not yet the full causal chain) is solid regardless.

Good-faith degradation in the mixed burst (120 good-faith of 200 total) was not cleanly tested against `main`'s 200-good-faith burst — the two have different total connection counts, which confounds queueing by itself (smaller bursts queue less). The plan's own Design (same good-faith count across ratios) is needed for a real comparison; this run was not that.

## What perturbs it

One block only — no p-values mean anything here (CLAUDE.md: "few blocks mean nothing"); read every number as a lead. The `NetworkPolicy`-restricted overlay is new and used for the first time in this run; it changes only the generator pod's networking, not the broker, callout or probes. The callout pod rolls on every variant apply (its own restart stamp), so its per-run counters cannot be read after the fact without disrupting the next run — a limitation worth fixing before relying on them again.
