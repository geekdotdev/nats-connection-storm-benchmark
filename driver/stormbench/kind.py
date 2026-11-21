# SPDX-License-Identifier: Apache-2.0
"""The local kind cluster: create, delete, and load locally built images into it.

The cluster gets its own kubeconfig file (STORM_KUBECONFIG, default .kube/storm-bench, gitignored), so the kubectl
context in ~/.kube/config is never switched or touched. Every kubectl call the driver makes must pass that file.

With podman, kind needs KIND_EXPERIMENTAL_PROVIDER=podman, and `kind load docker-image` cannot be used (it calls
docker): images go through `podman save` and `kind load image-archive` instead. UNTESTED with docker.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from .config import REPO_ROOT
from .images import container_tool
from .runner import Runner

KIND_CONFIG = "infra/kind/cluster.yaml"


def cluster_name() -> str:
    return os.environ.get("KIND_CLUSTER", "storm-bench")


def kubeconfig_path() -> Path:
    return Path(os.environ.get("STORM_KUBECONFIG") or REPO_ROOT / ".kube" / "storm-bench")


def provider_env(tool: str | None = None) -> dict[str, str]:
    return {"KIND_EXPERIMENTAL_PROVIDER": "podman"} if (tool or container_tool()) == "podman" else {}


def create_command(cluster: str | None = None, kubeconfig: Path | None = None) -> list[str]:
    return ["kind", "create", "cluster", "--name", cluster or cluster_name(), "--config", KIND_CONFIG,
            "--kubeconfig", str(kubeconfig or kubeconfig_path()), "--wait", "120s"]


def delete_command(cluster: str | None = None, kubeconfig: Path | None = None) -> list[str]:
    return ["kind", "delete", "cluster", "--name", cluster or cluster_name(),
            "--kubeconfig", str(kubeconfig or kubeconfig_path())]


def load_commands(images: list[str], cluster: str | None = None, tool: str | None = None,
                  archive_dir: Path | None = None) -> list[list[str]]:
    cluster = cluster or cluster_name()
    tool = tool or container_tool()
    if tool != "podman":
        return [["kind", "load", "docker-image", "--name", cluster, img] for img in images]
    if archive_dir is None:
        raise ValueError("podman needs an archive_dir for the image archives")
    cmds: list[list[str]] = []
    for i, img in enumerate(images):
        archive = str(archive_dir / f"image-{i}.tar")
        cmds.append([tool, "save", "-o", archive, img])
        cmds.append(["kind", "load", "image-archive", "--name", cluster, archive])
    return cmds


def up(run: Runner) -> None:
    if not run.dry_run:
        kubeconfig_path().parent.mkdir(parents=True, exist_ok=True)
    run(create_command(), env=provider_env())


def down(run: Runner) -> None:
    run(delete_command(), env=provider_env())


def load(run: Runner, images: list[str]) -> None:
    env = provider_env()
    with tempfile.TemporaryDirectory(prefix="storm-images-") as d:  # archives are removed when this block ends
        for cmd in load_commands(images, archive_dir=Path(d)):
            run(cmd, env=env)
