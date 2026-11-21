# infra/eks

The EKS cluster for runs that need real nodes. **Not yet created against AWS. The rendered config was accepted by `eksctl create cluster --dry-run` (eksctl 0.231.0, 2026-11-18), which resolved it with that eksctl's default Kubernetes version, 1.34 (the driver now pins 1.37 instead, 2026-11-20; the manifests have been run on 1.34 only); no cluster has been created from it.** The dry run itself calls AWS read-only (it needs credentials) and rejects `--kubeconfig`, so check a config with `eksctl create cluster --config-file F --dry-run`.

There is no cluster file to edit. eksctl does not expand environment variables in its config, so the driver renders the ClusterConfig from parameters (`driver/stormbench/eks.py`) and hands it to eksctl:

```bash
export AWS_REGION=us-east-1            # required; there is deliberately no default region
export AWS_PROFILE=<a profile you chose on purpose>
uv run bench cluster config --target eks                 # print the ClusterConfig; touches nothing
uv run bench --dry-run cluster up --target eks           # print the eksctl command; touches nothing
uv run bench cluster up --target eks --yes               # create it: BILLABLE until you run the next line
uv run bench cluster down --target eks
```

`up` refuses to run without `--yes`. Parameters (flag, or environment variable where there is one): `--region` (`AWS_REGION`), `--zone` (`STORM_EKS_ZONE`, default `<region>a`), `--instance-type` (`STORM_EKS_INSTANCE_TYPE`, default `c6i.xlarge`), `--broker-nodes` (default 3), `--client-nodes` (default 1), `--k8s-version` (`STORM_EKS_K8S_VERSION`, default 1.37, pinned in `eks.py`; not eksctl's own default, and `latest` is refused), and `STORM_EKS_CLUSTER` (default `storm-bench`). Node counts are capped at 10.

## Layout

| Node group | Label | Taint | Runs |
|---|---|---|---|
| `brokers` | `role=broker` | `dedicated=broker:NoSchedule` | the brokers, one per node |
| `clients` | `role=client` | none | CoreDNS and the other add-ons, the callout, the leaf servers, the load generator |

- No NAT gateway (`vpc.nat.gateway: Disable`, accepted by the dry run): the nodes are in public subnets and pull images over the internet gateway, so the default NAT gateway would only add an hourly charge.
- Both groups are fixed-size, one instance type, one availability zone (the control plane needs subnets in two, so the cluster lists two and the nodes use the first).
- The clients group is not tainted on purpose: with every node tainted, CoreDNS has nowhere to run. The overlays pin their pods to it with a `nodeSelector`.
- The cluster and its node groups are tagged `project=storm-bench`, which is how to find leftovers in the console or in Cost Explorer.
- A single-broker run only needs `--broker-nodes 1`; the cluster variants need at least 3. A broker that cannot be placed stays Pending (one per node is enforced).

## Kubeconfig

eksctl writes `.kube/storm-bench-eks` (gitignored) and nothing else: the driver passes `--kubeconfig`, so `~/.kube/config` is not modified. `up` refuses a `STORM_EKS_KUBECONFIG` that points at `~/.kube/config`, and `down` deletes only the file it made. The eks cluster has its own variable, `STORM_EKS_KUBECONFIG`, and the local one keeps `STORM_KUBECONFIG`, so neither target can act on the other's cluster.

`--target` picks the cluster for `cluster`, `secrets` and `run`, before or after the subcommand (default `local`). After `up`:

```sh
uv run bench --target eks secrets create          # keygen runs in a local callout image; the Secrets go to the EKS cluster
uv run bench --target eks run --a <sha> --b <sha> --name eks-smoke
uv run bench --target eks run ... --stack-overlay deploy/stack/overlays/eks-cluster    # the 3-broker variant
```

`bench preflight --target eks --a <sha> --b <sha>` (or `--variants FILE`; and `run`, before it applies anything; `--no-preflight` skips it) checks what the cluster and registry must have, and changes nothing: the nodes answer and have the `role=broker` / `role=client` groups the overlay asks for (enough tolerated nodes for the replicas, one per node, and the pods' CPU and memory requests fit a node), the Secrets exist, and every image (both arms' brokers and what the overlays name) can be pulled **without credentials**, which is what the nodes do, and has a build for the nodes' architecture. A private GHCR package and a missing tag both answer 401 there, so the message names both. Run it before the first real run: the registry checks have been run against the real GHCR packages and pass; the cluster checks have only been run against fakes and the repository's overlays.

`run --target eks` applies `deploy/stack/overlays/eks` and `deploy/loadgen/overlays/eks` unless told otherwise, and names each variant's broker image `$REGISTRY/nats-server:<short sha>` (so every variant's image must be pushed and public, unless a variants file gives an `image`). That is a tag; the digest the pod actually ran is in the results. `environment.json` records the target. None of this has run against a real cluster.

## Before the first real run

- Use a sandbox AWS account, or at least pick `AWS_PROFILE` deliberately: nothing here checks which account you are in.
- The images must be pullable without credentials: `nats-server`, `callout` and `loadgen` are pushed to `ghcr.io/<account>/` and pinned by digest in the overlays. All three packages start private: make each public in the GitHub package settings before EKS can pull it (done 2026-11-18; an anonymous manifest check of the pinned digests then passed).
- The overlays' CPU and memory sizes assume `c6i.xlarge` (4 vCPU, 8 GiB). Change them together with `--instance-type`.
- Always tear down, including after a failed run. `down` waits for the deletion to finish; check the CloudFormation console if it reports an error.
