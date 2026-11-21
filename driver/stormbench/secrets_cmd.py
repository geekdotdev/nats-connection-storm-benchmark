# SPDX-License-Identifier: Apache-2.0
"""bench secrets create | delete: the Secrets the stack expects, made at deploy time and never written to disk.

create: the issuer key comes from `callout keygen --json` run in the callout image (the Go nkeys library is the
authority on that encoding); the passwords and the throwaway TLS key pair are generated in memory; the two Secrets
reach the cluster as one manifest on kubectl's stdin. It refuses to replace Secrets that exist: delete them first,
then restart the pods, because running pods keep the values they started with.
"""
from __future__ import annotations

import json

from . import k8s, secretgen
from .images import component_tag, container_tool
from .runner import Runner

DRY_RUN_KEYGEN = '{"seed": "dry-run", "public": "dry-run"}'


def keygen_command(image: str, tool: str | None = None) -> list[str]:
    return [tool or container_tool(), "run", "--rm", image, "keygen", "--json"]


def default_callout_image(run: Runner, tool: str | None = None) -> str:
    """The locally built callout image to run keygen in: the one for the current source if it exists (a
    multi-platform build), else callout:dev (a single-platform build). The key does not depend on which one."""
    tool = tool or container_tool()
    tagged = component_tag("callout")
    return tagged if run.succeeds([tool, "image", "exists", tagged]) else "callout:dev"


def create(run: Runner, callout_image: str | None = None, target: str = "local") -> list[str]:
    """Create the namespace and both Secrets in the target's cluster; returns the Secret names (never values)."""
    k8s.require_kubeconfig(target)  # fail before keygen if there is no cluster to put the Secrets in
    callout_image = callout_image or default_callout_image(run)
    out = run.output(keygen_command(callout_image), dry_run_value=DRY_RUN_KEYGEN)
    key = json.loads(out)
    docs = secretgen.manifests(key["seed"], key["public"])
    run(k8s.kubectl_command("apply", "-f", "-", target=target), input=json.dumps(secretgen.namespace_manifest()))
    run(k8s.kubectl_command("create", "-f", "-", target=target), input=json.dumps({"apiVersion": "v1", "kind": "List", "items": docs}))
    return [d["metadata"]["name"] for d in docs]


def delete(run: Runner, target: str = "local") -> None:
    run(k8s.kubectl_command("-n", secretgen.NAMESPACE, "delete", "secret", secretgen.SECRETS_NAME, secretgen.TLS_NAME,
                            "--ignore-not-found", target=target))
