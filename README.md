# nats-connection-storm-benchmark

An A/B benchmark for nats-server: a burst of authenticated WebSocket connections against a broker with an auth callout, run for two builds of nats-server (any repository and commit) under identical conditions, with the results compared statistically.

> **Unofficial.** This project is not affiliated with or endorsed by the NATS project, the CNCF or Synadia.
>
> **Status: early.** The layout exists. The Python driver, the auth-callout responder and the load generator are implemented and tested, and both images build. The load generator and callout work together against a local nats-server over `ws://` and `wss://`. The base manifests and the kind overlay run the whole stack on a local kind cluster; the secrets command creates the stack's Secrets in the cluster and `bench run` does the A/B on kind; the EKS cluster config and overlays are written but have never been run against AWS, and no benchmark has been run from this repository.

Why it exists, and why the components run as separate processes: [docs/rationale.md](docs/rationale.md).

## Layout

| Path | Contents |
|---|---|
| `components/` | source of the load generator (`loadgen/`, Rust) and the auth-callout responder (`callout/`, Go) |
| `images/` | one Dockerfile per image: `nats-server` (built from a repo and commit), `callout`, `loadgen` |
| `deploy/` | Kubernetes manifests, composed with Kustomize: the `stack` (brokers, callout) with optional components for a cluster and for leaf nodes, and the `loadgen` Job; overlays for kind (`local`, `local-cluster`, `local-cluster-leaf`) and `eks`, `eks-cluster`, `eks-cluster-leaf` (rendered and checked, never applied) |
| `infra/` | cluster definitions: the kind config, and EKS (not written) |
| `driver/` | the Python driver (`stormbench`, command `bench`): builds and loads images, runs the A/B blocks, collects and reports |
| `tests/` | unit tests for the driver |
| `pyproject.toml`, `uv.lock` | the driver's project and locked dependencies |
| `results/` | one directory per run, with `environment.json`, `raw/` and a summary |
| `docs/` | rationale and design notes |
| `.github/workflows/` | the on-demand workflow (not written yet) |

## The driver and API keys

The driver is Python (one dependency, `cryptography`, for the throwaway TLS certificates), run with [uv](https://docs.astral.sh/uv/). `uv run` creates the project's virtual environment (`.venv/`, gitignored) on first use:

```sh
uv run bench keys
uv run pytest
```

Every command that runs an external tool (`docker`, `kind`, ...) echoes the exact command first, and `--dry-run` echoes without running.

`uv run bench keys` lists which keys are set, by name only; it never prints a value. Keys come from the environment or from a gitignored `.env` in the repository root; copy `.env.example` and fill in only what the commands you run need. A command asks for a key only when it needs one, so a local run on kind needs none. In CI no key file is used: AWS access is by GitHub OIDC.

## Planned usage (not implemented)

Implemented, but not yet tried against a real container tool or registry:

```sh
uv run bench --dry-run images build <full commit sha> [--repo https://github.com/nats-io/nats-server.git]
uv run bench images load-kind localhost/nats-server:<short sha> localhost/callout:dev localhost/loadgen:dev
uv run bench cluster up|down            # local kind cluster, own kubeconfig; `--target eks` for EKS (see infra/eks)
uv run bench secrets create|delete      # the stack's Secrets, generated in memory, never written to disk (`--target local|eks`)
uv run bench preflight --a <sha> --b <sha>   # check cluster, Secrets, node placement and (eks) registry images; `run` does this first
uv run bench images push nats-server:<short sha>
```

A run compares two or more variants of the stack on the local cluster (build and load the broker images first, then create the Secrets). A variant is a configuration: a broker image (from a commit) and, optionally, a topology or size overlay, extra broker configuration, broker and callout settings and generator settings. `--target eks` runs it on the EKS cluster instead, untested (infra/eks/README.md):

```sh
uv run bench run --a <full sha> --b <full sha> --a-label main --b-label mine --blocks 8 --name my-change   # two commits
uv run bench run --variants docs/variants.example.toml --blocks 8 --name my-variants               # two or more configurations
uv run bench report results/my-change-<date>      # also written to summary.txt by `run`
uv run bench --dry-run run --a <sha> --b <sha>    # print the variants and the schedule only
```

Each block gives each variant a freshly restarted broker and callout (one `kubectl apply -k` of a kustomization generated from the variant), then runs a warm-up, an idle phase and one storm phase per size; the order rotates by block (alternating, for two variants). The first variant is the baseline: the report compares each other variant with it, without correcting for the number of comparisons. Variants of one run must have the same Kubernetes objects (preflight checks it), so they differ in settings, not in what exists. Variants have been run end to end on kind (2 variants, 2 blocks at 200 connections, 2026-11-18: one broker against three with extra config, an environment variable and different generator threads); switching variants removed the earlier variant's settings. That run is a smoke test and says nothing about performance. A run writes `environment.json`, `raw/` and `summary.txt`. It has been run end to end on kind (2 blocks at 800 connections: too few for any conclusion).

## License

Apache-2.0, see [LICENSE](LICENSE).
