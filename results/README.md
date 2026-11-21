# Results

One directory per run, named `<description>-<date>`, holding:

- `environment.json`: both commits and the image digests, runner or node types, kernel, toolchain versions, the storm settings, and the design (blocks, order, pauses);
- `raw/`: the load generator's per-burst output for every block;
- `summary.txt`: the report, regenerated from `raw/` by `uv run bench report`;
- `queue-<phase>.svg`: for each burst phase, the queue of connections and the share admitted over time, one line per variant (only for runs made with per-connection samples; every run here predates them, including `eks-operator-smoke-2026-11-21`, so none has charts).

Nothing here may contain credentials, hostnames of private systems or personal details. Compare numbers within a run only; absolute numbers from different machines or runs are not comparable.

## What the existing results are

Every directory here was made **before operator mode replaced the earlier authentication** (a password callout, with the accounts written in the broker's configuration and one shared password): `kind-main-vs-lockfree-2026-11-17` on kind, and `eks-smoke-2026-11-18` and `eks-800-1600-2026-11-18` on EKS (Kubernetes 1.34). Their `environment.json` does not name the authentication, because there was only one then. They were produced by the code at git commit `bf93da8` and earlier; the current code cannot reproduce them (check out `bf93da8` to re-run). Numbers from the two authentication setups are not comparable: a connection does different work in each. Runs made from now on record `auth` in `environment.json`.
