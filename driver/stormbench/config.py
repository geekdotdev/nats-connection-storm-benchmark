# SPDX-License-Identifier: Apache-2.0
"""Where the driver gets its API keys and tokens.

Rules:
- Secrets come from the environment, or from a .env file in the repository root that is
  gitignored. Never from the command line (shell history) and never from a committed file.
- A command asks for a secret only when it needs it (require), so `uv run bench keys` and a
  local kind run work without any of them.
- Values are never printed or logged: describe() reports only whether a name is set.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Key:
    name: str
    needed_by: str
    note: str = ""


# One entry per secret the driver can read. Keep in step with .env.example.
KEYS: tuple[Key, ...] = (
    Key("REGISTRY", "images push", "registry host and path, not a secret; listed for completeness"),
    Key("REGISTRY_USER", "images push"),
    Key("REGISTRY_TOKEN", "images push", "GHCR: classic token with write:packages"),
    Key("GITHUB_TOKEN", "ci dispatch, ci fetch", "fine-grained, this repository only"),
    Key("AWS_PROFILE", "cluster up/down --target eks", "preferred over static keys"),
    Key("AWS_REGION", "cluster up/down --target eks"),
    Key("AWS_ACCESS_KEY_ID", "cluster up/down --target eks", "discouraged; use a profile"),
    Key("AWS_SECRET_ACCESS_KEY", "cluster up/down --target eks", "discouraged; use a profile"),
)


class MissingSecret(RuntimeError):
    pass


def load_dotenv(path: Path | None = None) -> None:
    """Read KEY=VALUE lines from .env into os.environ without overriding what is already set."""
    path = path or REPO_ROOT / ".env"
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        value = value.strip().strip("'\"")
        if value:
            os.environ.setdefault(name.strip(), value)


def require(*names: str) -> dict[str, str]:
    """Return the named secrets, or raise one error that lists every name that is missing."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        raise MissingSecret(
            "missing " + ", ".join(missing) + ": set in the environment or in .env (see .env.example)"
        )
    return {n: os.environ[n] for n in names}


def describe() -> list[tuple[str, bool, str]]:
    """(name, is_set, needed_by) for every known key. Never includes a value."""
    return [(k.name, bool(os.environ.get(k.name)), k.needed_by) for k in KEYS]
