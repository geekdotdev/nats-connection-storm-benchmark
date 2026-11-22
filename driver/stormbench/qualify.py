# SPDX-License-Identifier: Apache-2.0
"""What every output says about where and when it came from, so that nobody has to guess.

Five things qualify a result: the kind of cluster it ran on (`kind`, or `eks`), the namespace its components were in, the run's id, when
the run started, and who ran it (a GitHub login). The login is also the directory: results/<login>/<name>-<date>/, so that two people's
runs of the same experiment sit side by side in one repository and neither overwrites the other. The rest are written into the files themselves: `environment.json`,
the first line of `summary.txt`, the footer of every chart, every unit's `meta.json` and every storm's report (so a single file is
still labelled when it has been copied out of its directory). The cluster's state (`storm-run`) has them too.

`--target local` is a kind cluster, and a file that only said "local" would leave a reader to work that out: the label says `kind`. A value
that was not recorded (a result made before it was written) is shown as "not recorded", never filled in with a guess.
"""
from __future__ import annotations

import re

CLUSTER_KIND = {"local": "kind", "eks": "eks"}
NOT_RECORDED = "not recorded"


def cluster_kind(target: str | None) -> str:
    """The kind of cluster a target is: `local` is kind, `eks` is eks."""
    return CLUSTER_KIND.get(target, target) if target else NOT_RECORDED


# A GitHub login: letters, digits and single hyphens, not starting or ending with one, at most 39 characters.
_LOGIN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9]|-(?=[A-Za-z0-9])){0,38}")


def github_user(value: str | None) -> str:
    """A GitHub login as the results use it: checked, and in lower case (GitHub treats logins case-insensitively, and a directory name that
    differs only in case would be two directories on one file system and one on another). Raises ValueError saying what is wrong."""
    if not value or not value.strip():
        raise ValueError("a run needs the GitHub login of whoever runs it: set GITHUB_USER (in the environment or in .env) or pass --github-user. "
                         "It qualifies the run's results: results/<login>/<run>/ and every file of it")
    login = value.strip()
    if not _LOGIN.fullmatch(login):
        raise ValueError(f"{value!r} is not a GitHub login (letters, digits and single hyphens, up to 39 characters, not starting or ending with a hyphen)")
    return login.lower()


def from_env(env: dict) -> dict:
    """The qualifiers of a run, from its environment.json (older ones lack some: those read "not recorded")."""
    return {"cluster": env.get("cluster_kind") or cluster_kind(env.get("target")),
            "namespace": env.get("namespace") or NOT_RECORDED,
            "run_id": env.get("run_id") or NOT_RECORDED,
            "started": env.get("date") or NOT_RECORDED,
            "github_user": env.get("github_user") or NOT_RECORDED}


def line(q: dict) -> str:
    return (f"cluster {q['cluster']} · namespace {q['namespace']} · run id {q['run_id']} · started {q['started']}"
            + f" · run by {q.get('github_user') or NOT_RECORDED}")


def header(env: dict, directory: str) -> str:
    """The first line of summary.txt."""
    return f"{directory}: {line(from_env(env))}"
