# SPDX-License-Identifier: Apache-2.0
"""bench secrets create | delete: the TLS Secret the stack expects, made at deploy time and never written to disk.

create: the throwaway TLS key pair is generated in memory and the Secret reaches the cluster as one manifest on kubectl's stdin. It
refuses to replace a Secret that exists: delete it first, then restart the pods, because running pods keep the values they started with.
The operator, account and user keys are not made here: the provision init container of the callout pod makes them (components/provision).

delete also removes the objects that init container made: the callout's and the generator's Secrets and the broker's ConfigMap. The next
callout pod then makes new keys, and the broker pods must restart with it, because the broker's trusted operator and accounts are in that
ConfigMap: a variant start restarts both.
"""
from __future__ import annotations

import json

from . import k8s, secretgen
from .runner import Runner


def create(run: Runner, target: str = "local") -> list[str]:
    """Create the namespace and the TLS Secret in the target's cluster; returns the Secret names (never values)."""
    k8s.require_kubeconfig(target)  # fail before anything is made if there is no cluster to put the Secret in
    docs = secretgen.manifests()
    run(k8s.kubectl_command("apply", "-f", "-", target=target), input=json.dumps(secretgen.namespace_manifest()))
    run(k8s.kubectl_command("create", "-f", "-", target=target), input=json.dumps({"apiVersion": "v1", "kind": "List", "items": docs}))
    return [d["metadata"]["name"] for d in docs]


def delete(run: Runner, target: str = "local") -> None:
    run(k8s.kubectl_command("-n", secretgen.NAMESPACE, "delete", "secret", secretgen.TLS_NAME,
                            *secretgen.PROVISION_SECRETS, "--ignore-not-found", target=target))
    run(k8s.kubectl_command("-n", secretgen.NAMESPACE, "delete", "configmap", secretgen.PROVISION_CONFIGMAP,
                            "--ignore-not-found", target=target))
