# SPDX-License-Identifier: Apache-2.0
"""kubectl, always against the benchmark's own kubeconfig for the chosen target.

The user's default kubectl context may be a real cluster, so the driver never relies on it: every call passes
--kubeconfig explicitly, and refuses to run when that file does not exist (so a missing cluster is an error, not a
silent fallback to ~/.kube/config). Each target has its own file: local (kind) uses STORM_KUBECONFIG or
.kube/storm-bench, eks uses STORM_EKS_KUBECONFIG or .kube/storm-bench-eks, so one target can never act on the other's cluster.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import eks, kind

TARGETS = ("local", "eks")


class NoCluster(RuntimeError):
    pass


def kubeconfig_for(target: str) -> Path:
    if target == "local":
        return kind.kubeconfig_path()
    if target == "eks":
        return eks.kubeconfig_path()
    raise ValueError(f"target must be one of {', '.join(TARGETS)}, got {target!r}")


def require_kubeconfig(target: str = "local") -> str:
    path = kubeconfig_for(target)
    if not path.is_file():
        raise NoCluster(f"no kubeconfig at {path}: create the cluster first (bench cluster up --target {target})")
    return str(path)


def kubectl_command(*args: str, target: str = "local") -> list[str]:
    return ["kubectl", "--kubeconfig", require_kubeconfig(target), *args]


def json_stream(text: str) -> list[dict]:
    """The objects in kubectl's JSON output: it prints one JSON value per resource, one after another (a single value, a List
    of them, or several in a row), not one document. An overlay with more than one resource (e.g. a Job alongside a
    NetworkPolicy) renders as more than one value here, so callers that want a specific kind must pick it out themselves."""
    decoder, pos, objs = json.JSONDecoder(), 0, []
    text = text.strip()
    while pos < len(text):
        obj, end = decoder.raw_decode(text, pos)
        objs += obj["items"] if obj.get("kind") == "List" else [obj]
        pos = end
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return objs
