# Results index

One line per run, oldest first: the log. Each run's directory has a `NOTES.md` (the facts from its `environment.json`, then why it was made, what it showed and what perturbs it) and `summary.txt`. The images the runs used are in [../images/PUBLISHED.md](../images/PUBLISHED.md). **Add a line here when a run ends** (`bench run` writes the run's `NOTES.md`; you write its three sections and this line). Compare numbers within a run only.

| Run | Date | Target | What it was | Read it as |
|---|---|---|---|---|
| [kind-main-vs-lockfree-2026-11-17](cfedersp/kind-main-vs-lockfree-2026-11-17/NOTES.md) | 2026-11-17 | kind | `main` vs the lock-free change, 8 blocks, 800 and 1600 | history: before operator mode |
| [eks-smoke-2026-11-18](cfedersp/eks-smoke-2026-11-18/NOTES.md) | 2026-11-18 | EKS | `single` vs `cluster`, 2 blocks, 200: does it run on EKS | history: before operator mode |
| [eks-800-1600-2026-11-18](cfedersp/eks-800-1600-2026-11-18/NOTES.md) | 2026-11-18 | EKS | `single` vs `cluster`, 8 blocks, 800 and 1600 | history: before operator mode |
| [eks-operator-smoke-2026-11-21](cfedersp/eks-operator-smoke-2026-11-21/NOTES.md) | 2026-11-21 | EKS | first operator-mode run on EKS, 2 blocks, 800 | a check, not a measurement |
| [lock-mux-eks-2026-11-21](cfedersp/lock-mux-eks-2026-11-21/NOTES.md) | 2026-11-21 | EKS | first four-way attempt | **partial, 8 of 32 units**: not for results |
| [lock-mux-eks-retry-2026-11-21](cfedersp/lock-mux-eks-retry-2026-11-21/NOTES.md) | 2026-11-21 | EKS | `main`, `nolock`, `mux_main`, `mux_nolock`; 8 blocks, 800 and 1600 | the four-way result; no CPU data; two units redone |
| [eks-cpu-check-2026-11-21](cfedersp/eks-cpu-check-2026-11-21/NOTES.md) | 2026-11-21 | EKS | first CPU and throttling readings, 1 block | callout and generator throttled; one block |
| [eks-monitoring-check-2026-11-21](cfedersp/eks-monitoring-check-2026-11-21/NOTES.md) | 2026-11-21 | EKS | first probe and Prometheus on EKS | **stale generator: no `probeCpu`**; use probe-check |
| [eks-probe-check-2026-11-21](cfedersp/eks-probe-check-2026-11-21/NOTES.md) | 2026-11-21 | EKS | the same with the current generator | `probeCpu` works; callout about 60% throttled |
| [eks-profile-lock-2026-11-21](cfedersp/eks-profile-lock-2026-11-21/NOTES.md) | 2026-11-21 | EKS | block profile, `main` vs `nolock` | **profiled (perturbed)**; driver-side harvest |
| [eks-profile-probe-2026-11-21](cfedersp/eks-profile-probe-2026-11-21/NOTES.md) | 2026-11-21 | EKS | the same, harvested by the probes | **profiled (perturbed)**; agrees with the one above |
| [eks-mutex-profile-2026-11-21](cfedersp/eks-mutex-profile-2026-11-21/NOTES.md) | 2026-11-21 | EKS | who holds the locks: mutex-profile images | **profiled and patched**; a lead, not a result |
| [eks-relaxed-acc-conns-2026-11-21](cfedersp/eks-relaxed-acc-conns-2026-11-21/NOTES.md) | 2026-11-21 | EKS | `main`, `nolock`, `relaxed` (patched), 8 blocks, 800 and 1600 | `nolock` vs `main` replicated (-15%, -13%); `relaxed` only marginally faster than `nolock`; callout still throttled |
