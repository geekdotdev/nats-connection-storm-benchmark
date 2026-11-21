# Infrastructure

Cluster definitions, kept apart from the manifests.

- `kind/cluster.yaml`: the local cluster (one node, named `storm-bench`). Create and delete it with `uv run bench cluster up|down`. The cluster gets its own kubeconfig file (`.kube/storm-bench`, gitignored, or `STORM_KUBECONFIG`), so the kubectl context in `~/.kube/config` is never switched. With podman, kind needs a rootful podman machine; the driver sets `KIND_EXPERIMENTAL_PROVIDER=podman`.
- `eks/`: the EKS cluster for benchmark runs, made with eksctl from a ClusterConfig the driver renders: a tainted node group for the brokers and one for everything else, one instance type, one availability zone. See `eks/README.md`.

Rules for anything here that costs money: it must have a teardown that runs on failure, and no long-lived credentials are stored in the repository. CI authenticates with GitHub OIDC.
