# nats-connection-storm-benchmark

A framework for testing a nats-server, one broker or a cluster, in different configurations: a burst of authenticated WebSocket connections against a broker in operator mode with an auth callout, run for two or more variants (a build of nats-server from any repository and commit, extra broker settings, a topology, resources) under identical conditions, with the results compared statistically. It reports what it measured, neutrally; it is not aimed at any particular result.

> **Unofficial.** This project is not affiliated with or endorsed by the NATS project, the CNCF or Synadia.
>
> **Status: early.** The Python driver, the auth-callout responder, the key provisioning and the load generator are implemented and tested, together against a real nats-server on one machine, and on a kind cluster (2026-11-21: `bench run` for one broker and for the three-broker cluster, every connection admitted; details in [deploy/README.md](deploy/README.md)). Nothing has been run on EKS in operator mode (the images are pushed and pinned in the EKS overlays), and the results already in `results/` were made before operator mode replaced the earlier password-callout configuration (see [results/README.md](results/README.md)). No benchmark has been run from this repository since that is enough to conclude anything. The design and what remains are in [docs/operator-mode-plan.md](docs/operator-mode-plan.md).

Why it exists, and why the components run as separate processes: [docs/rationale.md](docs/rationale.md).

## What it measures, and what it does not

Each connection opens a WebSocket (TLS), receives the broker's INFO, and presents its own **ticket**: a user JWT signed by the AUTH account's scoped signing key, carrying role names as tags, with the broker's nonce signed by a user key of its own. The broker verifies the ticket and sends an authorization request to the **callout**, which maps the roles to subjects and answers with a user JWT for the tenant account. The generator times every connection from CONNECT to the broker's answer (`auth`) and from the start of the connection to the answer (`total`), and reports its own scheduling delay so a limit in the generator is visible.

- **What the timings include.** The broker's work, the callout round trip, and the network between them. `total` also includes one ed25519 signature per connection on the generator's side (the nonce is different for each connection, so it cannot be made early); `auth` does not. The tickets themselves are signed before each run's clock starts.
- **What the callout is.** A minimal responder: it decodes the request, maps role names to subjects (`app-feed-publish-<stem>` and `app-feed-subscribe-<stem>`) and signs a user JWT valid for one hour. It does not verify tokens, seal responses, check grants or call anything else, so the numbers describe the broker and the callout round trip, not a callout that does those things. The roles are self-asserted by whoever holds the scoped key.
- **The setup.** One tenant account, so every connection lands in the same account. The MEMORY resolver, so accounts are fixed while a broker runs. No leaf nodes. TLS on the client WebSocket only: the callout's connection and the routes between brokers are plain, on a private network.
- **Compare within a run.** Variants of one run are compared with each other; absolute numbers from different machines or runs are not comparable, and neither are numbers from before and after operator mode replaced the earlier authentication.

Later versions are meant to let the tester choose the resolver and whether to run leaf nodes; the plan says what is and is not designed for that.

## Layout

| Path | Contents |
|---|---|
| `components/` | source of the load generator (`loadgen/`, Rust), the auth-callout responder (`callout/`, Go) and the key provisioning that runs as the callout pod's init container (`provision/`, Go) |
| `images/` | one Dockerfile per image: `nats-server` (built from a repo and commit), `callout`, `loadgen`, `provision` |
| `deploy/` | Kubernetes manifests, composed with Kustomize: the `stack` (brokers, callout) with an optional component for a cluster, and the `loadgen` Job; overlays for kind (`local`, `local-cluster`) and `eks`, `eks-cluster` (rendered and checked, never applied) |
| `infra/` | cluster definitions: the kind config, and the EKS cluster config the driver renders for eksctl |
| `driver/` | the Python driver (`stormbench`, command `bench`): builds and loads images, runs the blocks of variants, collects and reports |
| `tests/` | tests for the driver, the rendered manifests and the documentation (commands and paths the docs name must exist) |
| `pyproject.toml`, `uv.lock` | the driver's project and locked dependencies |
| `results/` | one directory per run, with `environment.json`, `raw/` and a summary; the existing ones predate operator mode |
| `docs/` | rationale, the operator-mode design and status, and an example variants file |
| `.github/workflows/` | the on-demand workflow (not written yet) |

## The driver and API keys

The driver is Python (one dependency, `cryptography`, for the throwaway TLS certificates), run with [uv](https://docs.astral.sh/uv/). `uv run` creates the project's virtual environment (`.venv/`, gitignored) on first use:

```sh
uv run bench keys
uv run pytest
```

Every command that runs an external tool (`docker`, `kind`, ...) echoes the exact command first, and `--dry-run` echoes without running.

`uv run bench keys` lists which keys are set, by name only; it never prints a value. Keys come from the environment or from a gitignored `.env` in the repository root; copy `.env.example` and fill in only what the commands you run need. A command asks for a key only when it needs one, so a local run on kind needs none. In CI no key file is used: AWS access is by GitHub OIDC.

## Usage

These commands are implemented. The image, cluster and run commands have been tried before operator mode (kind and EKS, podman and GHCR) and not since:

```sh
uv run bench --dry-run images build <full commit sha> [--repo https://github.com/nats-io/nats-server.git]
uv run bench images load-kind localhost/nats-server:<short sha> localhost/callout:dev localhost/loadgen:dev localhost/provision:dev
uv run bench cluster up|down            # local kind cluster, own kubeconfig; `--target eks` for EKS (see infra/eks)
uv run bench secrets create|delete      # the TLS Secret, generated in memory, never written to disk; delete also removes the keys the callout pod's init container made (`--target local|eks`)
uv run bench preflight --a <sha> --b <sha>   # check cluster, Secrets, node placement and (eks) registry images; `run` does this first
uv run bench images push nats-server:<short sha>
```

A run compares two or more variants of the stack on the local cluster (build and load the images first, then create the TLS Secret; the operator keys are made by the callout pod). A variant is a configuration: a broker image (from a commit) and, optionally, a topology or size overlay, extra broker configuration, broker and callout settings and generator settings. `--target eks` runs it on the EKS cluster instead, untested (infra/eks/README.md):

```sh
uv run bench run --a <full sha> --b <full sha> --a-label main --b-label mine --blocks 8 --name my-change   # two commits
uv run bench run --variants docs/variants.example.toml --blocks 8 --name my-variants               # two or more configurations
uv run bench report results/my-change-2026-01-01  # summary.txt and queue-<phase>.svg; `run` writes them too
uv run bench --dry-run run --a <sha> --b <sha>    # print the variants and the schedule only
```

Each block gives each variant a freshly restarted broker and callout (one `kubectl apply -k` of a kustomization generated from the variant), then runs a warm-up, an idle phase and one storm phase per size; the order rotates by block (alternating, for two variants). The first variant is the baseline: the report compares each other variant with it, without correcting for the number of comparisons. Variants of one run must have the same Kubernetes objects (preflight checks it), so they differ in settings, not in what exists. Variants were run end to end on kind in the earlier password-callout configuration, not since operator mode replaced it (2 variants, 2 blocks at 200 connections, 2026-11-18: one broker against three with extra config, an environment variable and different generator threads); switching variants removed the earlier variant's settings. That run is a smoke test and says nothing about performance. A run writes `environment.json`, `raw/`, `summary.txt` and one `queue-<phase>.svg` per burst phase.

**One number per burst hides the shape of it.** A synchronized burst is a queue: the connections arrive together and are answered one after another, so the median is one point on a ramp. The generator therefore records, for every connection in the idle and burst phases, when it started, when its CONNECT went out and when it was answered (`STORM_SAMPLES=1`, set by the driver and not overridable). The report derives from them, per burst, the peak queue (CONNECT sent, no answer yet), the times by which 10, 50, 90 and 100 % of the connections were admitted, the drain rate between 10 % and 90 %, and when 90 % of the CONNECTs were out; the charts draw the queue and the admitted share over time, median across bursts with a 10th to 90th percentile band, one line per variant (`driver/stormbench/queue.py`). The queue is the client's view: it covers the broker's authorization and the callout together, and cannot say which of them the time went to. Results made before this have percentiles only and no curves. It was run end to end on kind in the earlier configuration (2 blocks at 800 connections: too few for any conclusion).

## License

Apache-2.0, see [LICENSE](LICENSE).
