# SPDX-License-Identifier: Apache-2.0
"""What every output says about where and when it came from, so that nobody has to guess.

Four things qualify a result: the kind of cluster it ran on (`kind`, or `eks`), the namespace its components were in, the run's id and
when the run started. The results directory's name is only `<name>-<date>`, so they are written into the files themselves: `environment.json`,
the first line of `summary.txt`, the footer of every chart, every unit's `meta.json` and every storm's report (so a single file is
still labelled when it has been copied out of its directory). The cluster's state (`storm-run`) has them too.

`--target local` is a kind cluster, and a file that only said "local" would leave a reader to work that out: the label says `kind`. A value
that was not recorded (a result made before it was written) is shown as "not recorded", never filled in with a guess.
"""
from __future__ import annotations

CLUSTER_KIND = {"local": "kind", "eks": "eks"}
NOT_RECORDED = "not recorded"


def cluster_kind(target: str | None) -> str:
    """The kind of cluster a target is: `local` is kind, `eks` is eks."""
    return CLUSTER_KIND.get(target, target) if target else NOT_RECORDED


def from_env(env: dict) -> dict:
    """The four qualifiers of a run, from its environment.json (older ones lack some: those read "not recorded")."""
    return {"cluster": env.get("cluster_kind") or cluster_kind(env.get("target")),
            "namespace": env.get("namespace") or NOT_RECORDED,
            "run_id": env.get("run_id") or NOT_RECORDED,
            "started": env.get("date") or NOT_RECORDED}


def line(q: dict) -> str:
    return f"cluster {q['cluster']} · namespace {q['namespace']} · run id {q['run_id']} · started {q['started']}"


def header(env: dict, directory: str) -> str:
    """The first line of summary.txt."""
    return f"{directory}: {line(from_env(env))}"
