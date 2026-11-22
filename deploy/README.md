# Manifests

Kubernetes manifests composed with Kustomize. Two independent kustomizations, each with a `base` and an overlay per target (`local` = kind, `eks`):

```
stack/       the brokers, the callout, the broker's config: what stays running
  base/                  one broker (a StatefulSet), the callout with its provision init container, and the RBAC that init container needs
  components/cluster/    three brokers joined by routes
  overlays/local/                 kind, one broker: small requests, no limits, no node pinning
  overlays/local-cluster/         kind, three brokers
  overlays/eks/                   EKS, one broker: node selectors, a toleration, one broker per node, requests equal to limits, GHCR images
  overlays/eks-cluster/           EKS, three brokers
loadgen/     one Job per storm run, kept out of the stack because a Job is immutable and runs once
  base/
  overlays/local/
  overlays/eks/          runs on the clients node group, requests equal to limits
```

Render with `kubectl kustomize deploy/stack/overlays/local-cluster`. Everything lives in the `storm-bench` namespace.

## How clients are authenticated: operator mode

The broker is in operator mode: it trusts an operator and finds its accounts through a resolver, and clients present user JWTs that the auth callout answers (the design and its decisions are in `docs/operator-mode-plan.md`). It is the only mode; the earlier configuration with accounts written in the broker's configuration and a password callout has been removed (it is in git history at `bf93da8`).

- A **ticket** is a user JWT signed by the AUTH account's scoped signing key, with no permissions of its own and its role names as tags. The load generator makes one per connection and signs the broker's nonce with that connection's own user key.
- The broker verifies the ticket and hands it to the **callout** (`components/callout`), which maps the roles to subjects and answers with a user JWT for the tenant account, valid for one hour.
- The keys come from the **provision init container** of the callout pod (`components/provision`). It makes the operator, the SYS, AUTH and tenant accounts, the scoped and plain signing keys and the callout's own login, once, and stores them as three objects. If they exist it does nothing, so a restart keeps the keys.

| Object | Made by | Read by |
|---|---|---|
| Secret `storm-callout-auth` (callout creds, AUTH and tenant signing seeds, public keys) | the init container | the callout only, through environment variables |
| Secret `storm-generator-auth` (the scoped seed, the AUTH account's public key) | the init container | the load generator Job only |
| ConfigMap `storm-operator` (operator, SYS, AUTH and tenant account JWTs, `operator.conf`, `resolver-memory.conf`; no seeds) | the init container | the broker, mounted at `/etc/storm/operator` |
| Secret `storm-tls` (`ca.pem`, `tls.crt`, `tls.key`: a throwaway CA and a broker certificate whose names include `broker`, `broker.storm-bench.svc` and `localhost`) | `bench secrets create` | the broker and the generator |

The callout pod gets the init container, a ServiceAccount (`storm-provision`, `base/rbac.yaml`) that may only create those three objects and look for the ConfigMap, and the ServiceAccount token **only in the init container** (the pod does not mount one by default). The callout reads its keys from environment variables (`secretKeyRef`), not a mounted Secret: a pod's volumes are set up before its init containers run, so a Secret volume for an object the init container creates would keep the pod from starting. The broker and generator pods are separate pods and wait (`ContainerCreating`) until their objects exist.

The broker's `nats-server.conf` includes `operator.conf` and `resolver-memory.conf` from the ConfigMap with a relative path, `../storm/operator/...`: nats-server joins an include path to the directory of the file that includes it, even an absolute one. The resolver is the MEMORY resolver with the three account JWTs preloaded, so accounts cannot be changed while a broker runs. Each is its own file so a later version can offer another resolver.

## What the driver adds: the run state

Not part of the manifests: `bench run` creates the ConfigMap `storm-run` in `storm-bench` and keeps a run's progress in it (the plan and the units done; which driver holds the run is a Kubernetes Lease of the same name, atomic and checked at every renewal, so a driver that was taken over stops), so that the driver can be stopped and started again with `bench run --resume`; `bench status` shows it. Its `plan.json` records the namespace the components are in (the driver defines it once, in `config.NAMESPACE`; a test checks that the manifests under `deploy/` agree), and `--resume` refuses a run recorded in another one. Applying a stack leaves it alone, `bench cluster down` takes it with the cluster, and `bench run --discard-state` removes it (the ConfigMap and the Lease).

## What the stack expects to exist first

Nothing secret is in the manifests. One Secret must exist in `storm-bench` before the pods can start: `uv run bench secrets create` (`--target eks` for the EKS cluster) makes `storm-tls`, generated in memory and sent to kubectl on stdin, so no value is written to disk or shown. It refuses to replace an existing Secret (`bench secrets delete` first, then restart the pods, since running pods keep the values they started with). `bench secrets delete` also removes the three objects the init container made: the next callout pod then makes new keys, and the broker pods must restart with it, because the broker's trusted operator and accounts are in that ConfigMap (a variant start restarts both).

## Images

The manifests name placeholder images (`nats-server`, `callout`, `provision`, `loadgen`). The local overlays map them to `localhost/<name>:dev`, which is how podman names a locally built image; the driver overrides the broker's image per variant with Kustomize's `images:`. All containers run as a non-root user with a read-only root filesystem and no capabilities.

## Monitoring: the CPU probe and Prometheus

`deploy/monitoring` is applied by `bench monitoring up`, not by a run, and stays in the `storm-bench` namespace. `base` is the DaemonSet `cpuprobe` (reads other containers' cgroups through a read-only `hostPath` of `/sys/fs/cgroup`; no CPU limit, so it cannot be throttled itself) with its headless Service, and a Prometheus StatefulSet that scrapes every probe once a second into a 2 Gi claim. `overlays/local` uses the image loaded into kind and kind's `standard` StorageClass; `overlays/eks` uses the pushed images by digest, the `gp3` StorageClass (EBS, from the CSI driver `bench cluster addons` installs) and the `infra` node group (`bench cluster infra`), so that Prometheus's disk activity is not on a broker or client node. The probe also keeps the broker's block profile snapshots in an `emptyDir` (`/profiles`, 256 Mi) until the run has completed and the driver has read them. The monitoring pods are labelled `app.kubernetes.io/part-of: storm-bench-monitoring`; the probe measures only pods labelled `app.kubernetes.io/part-of: storm-bench`.

## Running a storm

`uv run bench run` does everything below for you. By hand: apply the stack, wait for the broker and the callout to be ready, then apply `loadgen/overlays/local`. The generator's report is the last line of the Job's log, starting `STORM_REPORT_JSON`; delete the Job before applying it again.

`bench run` makes one Job per storm from that template, named `loadgen-<label>-<hash>` (the label is the storm's, for example `mux_main-b4-n800`; the hash tells apart labels that differ only in case or `_`) and labelled `storm-bench/run-id`, `storm-bench/unit`, `storm-bench/attempt` and `storm-bench/label`. A Job's pod and its log stay on the cluster until the Job is deleted, so they are kept (there is no `ttlSecondsAfterFinished`): the driver deletes a run's Jobs when the run completes, and a new run deletes any that are left. That is what lets a resumed run read a storm that finished while the driver was stopped; see the README.

```sh
uv run bench images build --only callout,loadgen,provision          # then load callout:dev, loadgen:dev and provision:dev into kind
uv run bench secrets create                                          # storm-tls; the operator keys come from the init container
uv run bench run --variants V.toml
```

If a rollout does not finish, the driver prints which pods are not ready and why, with a pointer to the init container's log when a pod waits for one of its objects.

Numbers from the local overlay are for smoke tests; they are not comparable with EKS.

**What has been verified on kind** (2026-11-21: podman on an Apple Silicon Mac, one kind node on Kubernetes v1.37.0, nats-server `582fb8e939a7`, arm64 images built by `bench images build`): the init container makes the three objects about a second after the pod starts, and the broker and callout pods are ready within about four seconds (the broker's first mount attempt fails once, then succeeds within a second); a restart of the callout keeps the keys (same ConfigMap, "already provisioned"); `bench secrets delete` and `create` followed by a run makes new keys; the callout container has no ServiceAccount token (it has no `/var/run/secrets` at all); the API server confirms the ServiceAccount may only create Secrets and ConfigMaps and get `storm-operator`; `bench preflight` passes; and `bench run` completes for two variants (one broker against one broker, and one broker against the three-broker cluster) with every connection admitted (6400 of 6400 at 400 connections, 1600 of 1600 at 200, nothing refused, timed out or failed). Those runs are smoke tests: two identical variants differed by noise (+2.1% at 400 connections with two blocks, p = 1.0), and a single block cannot support a comparison. When a rollout cannot finish, the driver's diagnosis was checked against a cluster with the init container's permission removed on purpose: it names the broker waiting for `storm-operator` and the init container's failure with its reason (the pod's status carries the init container's last log lines, `terminationMessagePolicy: FallbackToLogsOnError`).

**Not verified:** EKS and amd64 nodes (the kind run used arm64 images; the amd64 and arm64 images were built and pushed afterwards, and have not run); a run long enough or with enough blocks to say anything about performance; that a Kubernetes version other than 1.37 behaves the same (the manifests were last run on 1.34 before operator mode). The EKS overlays pin every image by digest: the amd64 and arm64 lists for `callout`, `provision` and `loadgen` pushed to GHCR on 2026-11-21 (each pin's comment names the source tag it was built from), and the broker's. A new GHCR package starts private and must be made public (GitHub package settings) before EKS nodes can pull it without credentials; `provision` was made public on 2026-11-21, and `bench preflight --target eks` checks this. Edit the registry account in each overlay's `images:` block. See `infra/eks/README.md`.

## Variants: the cluster

The broker's `nats-server.conf` includes two files, `cluster.conf` and `extra.conf`; the cluster component fills `cluster.conf` in by patching the ConfigMap, so the common configuration is written once. `server_name` is the pod name, so the servers of a cluster are distinct.

- **cluster**: `replicas: 3`, a route port, and a `cluster.conf` listing all three pods by their StatefulSet names (`broker-0.broker`, ...). Change the replica count and the route list together. There is no route authentication or TLS: the lab network is private. nats-server opens several connections per peer (a pool of three plus one for the system account), so `/routez` shows more connections than peers.

Leaf nodes were removed with the earlier authentication (a leaf connection needs credentials inside an account in operator mode); a later version is meant to bring them back as a choice.

Configuration is read when a server starts, so after applying a different overlay restart the brokers (`kubectl -n storm-bench rollout restart statefulset/broker`). `kubectl apply` does not delete resources that an overlay no longer includes; to move between variants cleanly, recreate the cluster (`bench cluster down` and `up`).

`extra.conf` in `storm-broker-config` is empty and included last. A variant of a run (`docs/variants.example.toml`) fills it with extra nats-server settings; the driver applies a generated kustomization (the variant's overlay plus patches for its image, environment, resources, callout arguments and that file) each time the variant starts. A variant's `callout_args` replaces the callout's whole argument list, so it must include `--url` too.
