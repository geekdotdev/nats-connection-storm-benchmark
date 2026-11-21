# SPDX-License-Identifier: Apache-2.0
"""The EKS cluster: render the eksctl ClusterConfig, create it, delete it. NOT RUN against AWS yet.

eksctl does not expand environment variables in its config file, so the driver renders the config
from parameters (region, zone, instance type, node counts) instead of keeping a template next to it.
`bench cluster config --target eks` prints the result without touching AWS.

Layout: no NAT gateway (the nodes are in public subnets), and two managed node groups in one availability zone each, one instance type for both.
- `brokers`: label role=broker and the taint dedicated=broker:NoSchedule, so only the brokers run there.
- `clients`: label role=client, no taint. It carries CoreDNS and the other add-ons (with every node tainted,
  CoreDNS would have nowhere to run), and the callout, the leaf servers and the load generator, which the
  overlays pin to it with a nodeSelector.
The control plane needs subnets in two zones, so the cluster lists two; the node groups use only the first.

The kubeconfig is STORM_EKS_KUBECONFIG or .kube/storm-bench-eks. It is deliberately not STORM_KUBECONFIG, which is the
local (kind) cluster's: `--target` picks the cluster, and one target can never act on the other's file.

Creating a cluster costs money. `up` refuses to run without confirm=True (the CLI's --yes). The cluster has its
own kubeconfig file, so ~/.kube/config is never written.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

from . import config
from .config import REPO_ROOT
from .runner import Runner

TAGS = {"project": "storm-bench"}
MAX_NODES = 10  # a typo should not be able to start fifty instances
# Pinned, not eksctl's default, so a newer eksctl cannot change what a run is measured on without a change here. 1.37 was the newest
# version eksctl 0.232.0-dev and EKS (us-east-1) both offered on 2026-11-20; the manifests have been run on 1.34 only.
DEFAULT_K8S_VERSION = "1.37"

_REGION = re.compile(r"^[a-z]{2}(-[a-z]+)+-\d$")
_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9-]{0,39}$")
_INSTANCE = re.compile(r"^[a-z][a-z0-9]*\.[a-z0-9]+$")
_VERSION = re.compile(r"^1\.\d{2}$")


class NotConfirmed(RuntimeError):
    pass


@dataclass(frozen=True)
class Params:
    region: str
    cluster: str = "storm-bench"
    zone: str = ""               # the node groups' zone; default <region>a
    instance_type: str = "c6i.xlarge"  # 4 vCPU, 8 GiB, x86. The overlays' resource sizes assume this size.
    broker_nodes: int = 3
    client_nodes: int = 1
    k8s_version: str = DEFAULT_K8S_VERSION  # "" would leave it to eksctl's default; the command line cannot ask for that

    def __post_init__(self) -> None:
        if not _REGION.match(self.region):
            raise ValueError(f"region must look like us-east-1, got {self.region!r}")
        if not _NAME.match(self.cluster):
            raise ValueError(f"cluster name must be letters, digits and dashes, got {self.cluster!r}")
        if self.zone and not self.zone.startswith(self.region):
            raise ValueError(f"zone {self.zone!r} is not in region {self.region!r}")
        if not _INSTANCE.match(self.instance_type):
            raise ValueError(f"instance type must look like c6i.xlarge, got {self.instance_type!r}")
        for what, n in (("broker", self.broker_nodes), ("client", self.client_nodes)):
            if not 1 <= n <= MAX_NODES:
                raise ValueError(f"{what} nodes must be 1 to {MAX_NODES}, got {n}")
        if self.k8s_version and not _VERSION.match(self.k8s_version):
            raise ValueError(f"kubernetes version must look like 1.33, got {self.k8s_version!r}")

    @property
    def node_zone(self) -> str:
        return self.zone or f"{self.region}a"

    @property
    def zones(self) -> list[str]:
        """Two zones for the control plane's subnets: the node zone first, then the next letter."""
        first = self.node_zone
        second = first[:-1] + ("b" if first[-1] != "b" else "a")
        return [first, second]


def params_from_env(**overrides: str | int | None) -> Params:
    """Params from the environment (AWS_REGION is required) with command line values on top.
    Environment: STORM_EKS_CLUSTER, STORM_EKS_ZONE, STORM_EKS_INSTANCE_TYPE, STORM_EKS_K8S_VERSION (default DEFAULT_K8S_VERSION)."""
    env = os.environ
    region = overrides.get("region") or config.require("AWS_REGION")["AWS_REGION"]
    values: dict[str, str | int] = {"region": region}
    for field, var in (("cluster", "STORM_EKS_CLUSTER"), ("zone", "STORM_EKS_ZONE"),
                       ("instance_type", "STORM_EKS_INSTANCE_TYPE"), ("k8s_version", "STORM_EKS_K8S_VERSION")):
        if env.get(var):
            values[field] = env[var]
    values.update({k: v for k, v in overrides.items() if v is not None and k != "region"})
    return Params(**values)  # type: ignore[arg-type]


DEFAULT_KUBECONFIG = REPO_ROOT / ".kube" / "storm-bench-eks"


def kubeconfig_path() -> Path:
    """The cluster's own kubeconfig. eksctl merges into an existing file, so refuse the user's default one."""
    path = Path(os.environ.get("STORM_EKS_KUBECONFIG") or DEFAULT_KUBECONFIG)
    if path.expanduser().resolve() == (Path.home() / ".kube" / "config").resolve():
        raise ValueError("STORM_EKS_KUBECONFIG must not be ~/.kube/config: the benchmark keeps its own kubeconfig file")
    return path


def _nodegroup(p: Params, name: str, role: str, count: int, taint: bool) -> dict:
    group: dict = {
        "name": name,
        "instanceType": p.instance_type,
        "desiredCapacity": count,
        "minSize": count,
        "maxSize": count,        # fixed size: nothing scales, so nothing changes under a run
        "availabilityZones": [p.node_zone],
        "volumeSize": 30,
        "volumeType": "gp3",
        "labels": {"role": role},
        "tags": dict(TAGS),
    }
    if taint:
        group["taints"] = [{"key": "dedicated", "value": role, "effect": "NoSchedule"}]
    return group


def cluster_config(p: Params) -> dict:
    metadata: dict = {"name": p.cluster, "region": p.region, "tags": dict(TAGS)}
    if p.k8s_version:
        metadata["version"] = p.k8s_version
    return {
        "apiVersion": "eksctl.io/v1alpha5",
        "kind": "ClusterConfig",
        "metadata": metadata,
        "availabilityZones": p.zones,
        # The nodes sit in public subnets (they pull images over the internet gateway), so the NAT gateway eksctl
        # makes by default would cost money and carry nothing.
        "vpc": {"nat": {"gateway": "Disable"}},
        "managedNodeGroups": [
            _nodegroup(p, "brokers", "broker", p.broker_nodes, taint=True),
            _nodegroup(p, "clients", "client", p.client_nodes, taint=False),
        ],
    }


def to_yaml(value: object, indent: int = 0) -> str:
    """YAML for dicts, lists, strings, ints and bools: the only types a ClusterConfig here uses. Strings are
    JSON-quoted, which is valid YAML, so no value can be read as something else (1.33 stays a string)."""
    pad = "  " * indent
    if isinstance(value, dict):
        lines = []
        for k, v in value.items():
            if isinstance(v, (dict, list)) and v:
                lines.append(f"{pad}{k}:\n{to_yaml(v, indent + 1)}")
            else:
                lines.append(f"{pad}{k}: {_scalar(v)}")
        return "\n".join(lines)
    if isinstance(value, list):
        lines = []
        for item in value:
            if isinstance(item, dict) and item:
                body = to_yaml(item, indent + 1).split("\n")
                lines.append(f"{pad}- {body[0].lstrip()}")
                lines.extend(body[1:])
            else:
                lines.append(f"{pad}- {_scalar(item)}")
        return "\n".join(lines)
    return f"{pad}{_scalar(value)}"


def _scalar(v: object) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, (dict, list)):
        return "{}" if isinstance(v, dict) else "[]"
    return json.dumps(str(v))


def render(p: Params) -> str:
    return to_yaml(cluster_config(p)) + "\n"


def create_command(config_file: Path, kubeconfig: Path) -> list[str]:
    # --kubeconfig makes eksctl write only that file; ~/.kube/config is left alone.
    return ["eksctl", "create", "cluster", "--config-file", str(config_file), "--kubeconfig", str(kubeconfig)]


def delete_command(p: Params) -> list[str]:
    return ["eksctl", "delete", "cluster", "--name", p.cluster, "--region", p.region, "--wait"]


def plan_text(p: Params) -> str:
    return (f"{p.cluster} in {p.region} (nodes in {p.node_zone}): {p.broker_nodes} broker node(s) and "
            f"{p.client_nodes} client node(s), {p.instance_type} each, plus the EKS control plane. "
            "All of it is billed by the hour until `bench cluster down --target eks`.")


def up(run: Runner, p: Params, confirm: bool) -> Path:
    if not confirm and not run.dry_run:
        raise NotConfirmed(f"this creates billable AWS resources: {plan_text(p)}\nRun again with --yes to create them.")
    kc = kubeconfig_path()
    if not run.dry_run:
        kc.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="storm-eks-") as tmp:
        cfg = Path(tmp) / "cluster.yaml"
        cfg.write_text(render(p))
        run(create_command(cfg, kc))
    return kc


def down(run: Runner, p: Params) -> None:
    run(delete_command(p))
    kc = kubeconfig_path()
    # Remove only the file the driver made itself, never one STORM_EKS_KUBECONFIG points at.
    if not run.dry_run and kc == DEFAULT_KUBECONFIG and kc.is_file():
        kc.unlink()
