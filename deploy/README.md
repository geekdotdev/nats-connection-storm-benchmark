# Manifests

Kubernetes manifests composed with Kustomize. Two independent kustomizations, each with a `base` and an overlay per target (`local` = kind, `eks`):

```
stack/       the brokers, the callout, the broker's config: what stays running
  base/                  one broker (a StatefulSet), the callout, and the leaf servers at zero replicas
  components/cluster/    three brokers joined by routes
  components/leafnodes/  the brokers accept leaf connections, and two leaf servers connect (works with or without the cluster)
  overlays/local/                 kind, one broker: small requests, no limits, no node pinning
  overlays/local-cluster/         kind, three brokers
  overlays/local-cluster-leaf/    kind, three brokers and two leaf nodes
  overlays/eks/                   EKS, one broker: node selectors, a toleration, one broker per node, requests equal to limits, GHCR images
  overlays/eks-cluster/           EKS, three brokers
  overlays/eks-cluster-leaf/      EKS, three brokers and two leaf nodes
loadgen/     one Job per storm run, kept out of the stack because a Job is immutable and runs once
  base/
  overlays/local/
  overlays/eks/          runs on the clients node group, requests equal to limits
```

Render with `kubectl kustomize deploy/stack/overlays/local-cluster`. Everything lives in the `storm-bench` namespace.

## What the stack expects to exist first

Nothing secret is in the manifests. Two Secrets in `storm-bench` must exist before the pods can start. `uv run bench secrets create` (`--target eks` for the EKS cluster) makes them: the passwords and the throwaway CA and broker certificate are generated in memory, the issuer key comes from `callout keygen --json` in the callout image, and the manifest goes to kubectl on stdin, so no value is written to disk or shown. It refuses to replace existing Secrets (`bench secrets delete` first, then restart the pods, since running pods keep the values they started with).

| Secret | Keys |
|---|---|
| `storm-secrets` | `service-password` (the callout's login to the broker), `user-password` (what every benchmark user presents), `leaf-password` (leaf nodes' login to the brokers; used only with the `leafnodes` component), `issuer-seed` (the callout's account seed), `issuer-public` (its public key, the broker's `auth_callout.issuer`) |
| `storm-tls` | `ca.pem`, `tls.crt`, `tls.key`: a throwaway CA and a broker certificate whose names include `broker`, `broker.storm-bench.svc` and `localhost` |

The seed and public key come from `callout keygen`; the broker reads the password and the public key from its environment (`$NAME` references in `stack/base/broker-config.yaml`).

## Images

The manifests name placeholder images (`nats-server`, `callout`, `loadgen`). The local overlays map them to `localhost/<name>:dev`, which is how podman names a locally built image; the driver overrides the broker's image per arm with Kustomize's `images:`. All containers run as a non-root user with a read-only root filesystem and no capabilities.

## Running a storm

`uv run bench run` does everything below for you. By hand: apply the stack, wait for the broker and the callout to be ready, then apply `loadgen/overlays/local`. The generator's report is the last line of the Job's log, starting `STORM_REPORT_JSON`. Delete the Job before the next run. The accounts the broker has (A0 to A15) must match the callout's `--accounts` and the generator's `STORM_ACCOUNTS`.

Numbers from the local overlay are for smoke tests; they are not comparable with EKS.

The EKS overlays are rendered and checked (`kubectl kustomize deploy/stack/overlays/eks-cluster-leaf`: every workload lands in `storm-bench`, with the expected images, replicas, node selectors and limits) but have never been applied to a cluster. The broker image is pinned by digest to the nats-server image already pushed to GHCR; the callout and loadgen images are not published, so those two overlays name a `dev` tag that does not exist yet. Edit the registry account in each overlay's `images:` block. See `infra/eks/README.md`.

## Variants: the cluster and the leaf nodes

The broker's `nats-server.conf` includes two files that are empty in the base, `cluster.conf` and `leafnodes.conf`; a component fills one in by patching the ConfigMap, so the common configuration is written once. `server_name` is the pod name, so the servers of a cluster are distinct.

- **cluster**: `replicas: 3`, a route port, and a `cluster.conf` listing all three pods by their StatefulSet names (`broker-0.broker`, ...). Change the replica count and the route list together. There is no route authentication or TLS: the lab network is private. nats-server opens several connections per peer (a pool of three plus one for the system account), so `/routez` shows more connections than peers.
- **leafnodes**: the brokers listen on 7422 and authenticate leaf connections with the `leaf` user (password `leaf-password`), which binds them to account `A0`; the callout is not involved. The `leaf` Deployment is in the base at zero replicas, because resources added by a component do not receive the overlay's image mapping or the namespace; the component scales it to two.

Configuration is read when a server starts, so after applying a different overlay restart the brokers (`kubectl -n storm-bench rollout restart statefulset/broker`). `kubectl apply` does not delete resources that an overlay no longer includes; to move between variants cleanly, recreate the cluster (`bench cluster down` and `up`).

What has been checked on kind: the single broker and the cluster admit a 400-connection storm (the cluster spread it across the three brokers, 262, 270 and 269 connections); each broker sees both peers; with the leafnodes component, two leaf servers connect to one of the brokers and are bound to account `A0`. Not checked: messages flowing across a leaf connection, a cluster of a size other than three, TLS on routes or leaf connections.

`extra.conf` in `storm-broker-config` is empty and included last. A variant of a run (`docs/variants.example.toml`) fills it with extra nats-server settings; the driver applies a generated kustomization (the variant's overlay plus patches for its image, environment, resources, callout arguments and that file) each time the variant starts.
