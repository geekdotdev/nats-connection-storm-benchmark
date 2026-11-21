# SPDX-License-Identifier: Apache-2.0
"""Push images to the registry named in REGISTRY.

The token is read from the environment (see .env.example) and sent on stdin, so it is
never in the process list, the shell history or the echoed command line. The login goes
into a temporary credentials file that is deleted when the push ends, so the token is not
left in the container tool's own saved logins either.

REGISTRY is the registry host plus the account or organisation, for example
ghcr.io/octocat. An image built locally as nats-server:abc becomes
ghcr.io/octocat/nats-server:abc. GHCR needs lowercase names and a classic token with the
write:packages scope (fine-grained tokens are not accepted for packages).

Only podman has been tried; the docker commands have not.
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

from . import config
from .images import container_tool
from .runner import Runner


def _is_podman(tool: str) -> bool:
    return os.path.basename(tool) == "podman"


def check_registry(registry: str) -> None:
    host, _, account = registry.partition("/")
    if not host or not account or registry.endswith("/"):
        raise ValueError(f"REGISTRY must be host/account, for example ghcr.io/octocat; got {registry!r}")
    if registry != registry.lower():
        raise ValueError(f"REGISTRY must be lowercase (registries reject uppercase names); got {registry!r}")


def target_name(registry: str, image: str) -> str:
    """The name an image is pushed as. A local `localhost/` prefix (added by podman) is dropped."""
    return f"{registry}/{image.removeprefix('localhost/')}"


def login_command(registry: str, user: str, tool: str | None = None, authfile: Path | None = None,
                  insecure: bool = False) -> list[str]:
    cmd = [tool or container_tool(), "login", registry.split("/")[0], "--username", user, "--password-stdin"]
    if insecure:
        cmd.append("--tls-verify=false")
    if authfile is not None and _is_podman(cmd[0]):
        cmd += ["--authfile", str(authfile)]
    return cmd


def push_commands(images: list[str], registry: str, tool: str | None = None,
                  authfile: Path | None = None, digestdir: Path | None = None,
                  insecure: bool = False, manifest_lists: frozenset[str] = frozenset()) -> list[list[str]]:
    """Push each image. A podman manifest list (built with several --platform values) goes through
    `manifest push --all`, which uploads every platform in it; a plain `push` would upload only one
    of them as a single image. With podman the pushed digest is written to digestdir/<n>."""
    tool = tool or container_tool()
    cmds: list[list[str]] = []
    for n, img in enumerate(images):
        dest = target_name(registry, img)
        flags: list[str] = ["--tls-verify=false"] if insecure else []
        if _is_podman(tool):
            if authfile is not None:
                flags += ["--authfile", str(authfile)]
            if digestdir is not None:
                flags += ["--digestfile", str(digestdir / str(n))]
        if img in manifest_lists:
            cmds.append([tool, "manifest", "push", "--all", *flags, img, f"docker://{dest}"])
        else:
            cmds.append([tool, "tag", img, dest])
            cmds.append([tool, "push", *flags, dest])
    return cmds


def _without_tag(name: str) -> str:
    head, sep, tail = name.rpartition(":")
    return head if sep and "/" not in tail else name


def push(run: Runner, images: list[str], insecure: bool = False) -> list[str]:
    """Log in, push, and return each pushed image by digest (empty in dry run, and with docker).
    `insecure` skips TLS verification, for a throwaway registry on localhost only."""
    if insecure and not config.require("REGISTRY")["REGISTRY"].startswith(("localhost:", "127.0.0.1:")):
        raise ValueError("--insecure is only for a registry on localhost")
    s = config.require("REGISTRY", "REGISTRY_USER", "REGISTRY_TOKEN")
    check_registry(s["REGISTRY"])
    tool = container_tool()
    pushed: list[str] = []
    with tempfile.TemporaryDirectory(prefix="storm-push-") as tmp:
        authfile = Path(tmp) / "auth.json"
        env = {} if _is_podman(tool) else {"DOCKER_CONFIG": tmp}
        run(login_command(s["REGISTRY"], s["REGISTRY_USER"], tool, authfile, insecure), input=s["REGISTRY_TOKEN"], env=env)
        lists = frozenset(i for i in images if _is_podman(tool) and run.succeeds([tool, "manifest", "exists", i]))
        for cmd in push_commands(images, s["REGISTRY"], tool, authfile, Path(tmp), insecure, lists):
            run(cmd, env=env)
        for n, img in enumerate(images):
            df = Path(tmp) / str(n)
            if df.is_file():
                pushed.append(f"{_without_tag(target_name(s['REGISTRY'], img))}@{df.read_text().strip()}")
    return pushed
