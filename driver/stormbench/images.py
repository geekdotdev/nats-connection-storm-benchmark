# SPDX-License-Identifier: Apache-2.0
"""Build the benchmark images (tested with podman; the docker commands are not).

The broker image is built from any repository and commit (build arguments REPO and REF),
tagged nats-server:<first 12 characters of the commit>. The callout and loadgen images are
built from this repository's own source, and only when that source exists (so is the provision image, the init container of the
callout pod that makes the operator-mode keys). They are tagged
<name>:src-<first 12 hex of a hash of their source files and Dockerfile>, so the tag names the
source even while it is uncommitted. A single-platform build also gets <name>:dev, which the
local (kind) flow uses. Deploy by digest: the hash does not cover the base images.
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
from pathlib import Path

from .config import REPO_ROOT
from .runner import Runner

DEFAULT_REPO = "https://github.com/nats-io/nats-server.git"
_SHA = re.compile(r"^[0-9a-f]{40}$")
_PLATFORM = re.compile(r"^linux/(amd64|arm64)$")


def container_tool() -> str:
    """CONTAINER_TOOL if set, else docker if installed, else podman."""
    if os.environ.get("CONTAINER_TOOL"):
        return os.environ["CONTAINER_TOOL"]
    return "docker" if shutil.which("docker") or not shutil.which("podman") else "podman"


def parse_platforms(text: str | None) -> tuple[str, ...]:
    """'linux/amd64,linux/arm64' -> ('linux/amd64', 'linux/arm64'). Empty or None means the host's own platform."""
    if not text:
        return ()
    out = tuple(p.strip() for p in text.split(","))
    for p in out:
        if not _PLATFORM.match(p):
            raise ValueError(f"platform must be linux/amd64 or linux/arm64, got {p!r}")
    if len(set(out)) != len(out):
        raise ValueError(f"platform listed twice in {text!r}")
    return out


def broker_tag(ref: str, patch: str | None = None) -> str:
    """nats-server:<12 of the sha>, or, with a patch applied on top, nats-server:<12 of the sha>-<patch>: the tag says what is in the image."""
    if not _SHA.match(ref):
        raise ValueError(f"ref must be a full 40 character commit sha, got {ref!r}")
    return f"nats-server:{ref[:12]}" + (f"-{patch}" if patch else "")


def patch_file(patch: str, root: Path = REPO_ROOT) -> Path:
    """The patch's file under images/nats-server/patches. Named by letters, digits and hyphens, so a name cannot reach another path."""
    if not re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", patch):
        raise ValueError(f"a patch is named in lowercase letters, digits and hyphens, got {patch!r}")
    path = root / "images/nats-server/patches" / f"{patch}.patch"
    if not path.is_file():
        have = sorted(p.stem for p in (root / "images/nats-server/patches").glob("*.patch"))
        raise ValueError(f"no patch {patch!r} in images/nats-server/patches (have: {', '.join(have) or 'none'})")
    return path


REF_NAME_LABEL = "org.opencontainers.image.ref.name"


def ref_name_tag(name: str) -> str:
    """The image tag for a branch or tag name: characters a tag cannot hold become '-', so
    feature/lock-free becomes feature-lock-free. Tags allow letters, digits, _ . - and up to 128
    characters, and cannot start with . or -."""
    if not name or any(ord(c) < 32 or ord(c) == 127 for c in name):
        raise ValueError(f"ref name must be non-empty text without control characters, got {name!r}")
    tag = re.sub(r"-{2,}", "-", re.sub(r"[^A-Za-z0-9_.-]", "-", name)).lstrip(".-")[:128].rstrip(".-")
    if not tag:
        raise ValueError(f"ref name {name!r} has no letters or digits to make a tag from")
    if re.fullmatch(r"[0-9a-f]{12}", tag):
        raise ValueError(f"ref name {name!r} would make a tag that looks like a short commit sha ({tag}); pick another")
    return tag


_SKIP_DIRS = {"target", ".git", "node_modules", "__pycache__"}


def component_tag(name: str, root: Path = REPO_ROOT) -> str:
    """name:src-<12 hex>: a hash of every file under components/<name> (not build output) and of
    images/<name>/Dockerfile, each by relative path and content. Same source, same tag."""
    files: list[Path] = []
    base = root / "components" / name
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames[:] = sorted(d for d in dirnames if d not in _SKIP_DIRS)
        files += [Path(dirpath) / f for f in sorted(filenames)]
    dockerfile = root / "images" / name / "Dockerfile"
    if dockerfile.is_file():
        files.append(dockerfile)
    h = hashlib.sha256()
    for f in files:
        h.update(f.relative_to(root).as_posix().encode() + b"\0" + f.read_bytes() + b"\0")
    return f"{name}:src-{h.hexdigest()[:12]}"


def _build(tool: str, dockerfile: str, tag: str, context: str, platforms: tuple[str, ...],
           args: tuple[str, ...] = (), extra_tags: tuple[str, ...] = ()) -> list[list[str]]:
    cmd = [tool, "build", "-f", dockerfile, *args]
    after: list[list[str]] = []
    if len(platforms) > 1:
        if tool != "podman":
            raise ValueError("more than one platform needs CONTAINER_TOOL=podman (docker would need buildx, which this driver does not use)")
        # One manifest list holding an image per platform; `images push` pushes all of them.
        # build() drops the tag first, because podman builds into a list that already exists by appending to it.
        cmd += ["--platform", ",".join(platforms), "--manifest", tag]
        after = [[tool, "tag", tag, t] for t in extra_tags]
    else:
        if platforms:
            cmd += ["--platform", platforms[0]]
        cmd += ["-t", tag]
        for t in extra_tags:
            cmd += ["-t", t]
    return [cmd + [context]] + after


IMAGE_NAMES = ("nats-server", "callout", "loadgen", "provision", "cpuprobe")


def build_commands(ref: str | None, repo: str = DEFAULT_REPO, tool: str | None = None,
                   root: Path = REPO_ROOT, platforms: tuple[str, ...] = (),
                   ref_name: str | None = None, only: tuple[str, ...] = (), patch: str | None = None) -> list[list[str]]:
    """The build commands. `only` limits them to the named images (nats-server, callout, loadgen, provision, cpuprobe); the
    commit is needed only when nats-server is among them. The broker image is always tagged with the commit (the identity). With
    ref_name (a branch or tag name) it also gets that name as a label, org.opencontainers.image.ref.name,
    and as a second tag. The name is where the branch pointed when the image was built, nothing more."""
    for name in only:
        if name not in IMAGE_NAMES:
            raise ValueError(f"--only takes {', '.join(IMAGE_NAMES)}; got {name!r}")
    if not repo.startswith("https://"):
        raise ValueError(f"repo must be an https:// URL, got {repo!r}")
    tool = tool or container_tool()
    want = lambda name: not only or name in only  # noqa: E731
    cmds: list[list[str]] = []
    args = ["--build-arg", f"REPO={repo}", "--build-arg", f"REF={ref}"]
    extra: tuple[str, ...] = ()
    if patch:
        digest = hashlib.sha256(patch_file(patch, root).read_bytes()).hexdigest()
        args += ["--build-arg", f"PATCH={patch}", "--label", f"benchmark.patch.sha256={digest}"]
    if ref_name is not None:
        extra = (f"nats-server:{ref_name_tag(ref_name)}" + (f"-{patch}" if patch else ""),)
        args += ["--label", f"{REF_NAME_LABEL}={ref_name}"]
    if want("nats-server"):
        cmds += _build(tool, "images/nats-server/Dockerfile", broker_tag(ref or "", patch), "images/nats-server", platforms,
                       tuple(args), extra)
    dev = (lambda name: (f"{name}:dev",) if len(platforms) <= 1 else ())  # the local flow builds for one platform
    if want("callout") and (root / "components/callout/go.mod").is_file():
        cmds += _build(tool, "images/callout/Dockerfile", component_tag("callout", root), ".", platforms,
                       extra_tags=dev("callout"))
    if want("loadgen") and (root / "components/loadgen/Cargo.toml").is_file():
        cmds += _build(tool, "images/loadgen/Dockerfile", component_tag("loadgen", root), ".", platforms,
                       extra_tags=dev("loadgen"))
    if want("provision") and (root / "components/provision/go.mod").is_file():
        cmds += _build(tool, "images/provision/Dockerfile", component_tag("provision", root), ".", platforms,
                       extra_tags=dev("provision"))
    if want("cpuprobe") and (root / "components/cpuprobe/go.mod").is_file():
        cmds += _build(tool, "images/cpuprobe/Dockerfile", component_tag("cpuprobe", root), ".", platforms,
                       extra_tags=dev("cpuprobe"))
    return cmds


def drop_command(run: Runner, tool: str, tag: str) -> list[str] | None:
    """The command that removes whatever `tag` holds now, or None if it holds nothing. A manifest list
    needs `manifest rm` (`rmi` fails on one with "tag not known"); a plain image needs `rmi`."""
    if run.succeeds([tool, "manifest", "exists", tag]):
        return [tool, "manifest", "rm", tag]
    if run.succeeds([tool, "image", "exists", tag]):
        return [tool, "rmi", tag]
    return None


def build(run: Runner, ref: str | None, repo: str = DEFAULT_REPO, platforms: tuple[str, ...] = (),
          ref_name: str | None = None, only: tuple[str, ...] = (), patch: str | None = None) -> None:
    tool = container_tool()
    for cmd in build_commands(ref, repo, tool, platforms=platforms, ref_name=ref_name, only=only, patch=patch):
        if "--manifest" in cmd:  # a multi-platform build: start from an empty tag, never append to an old list
            drop = drop_command(run, tool, cmd[cmd.index("--manifest") + 1])
            if drop:
                run(drop)
        run(cmd)  # the context paths in the commands are relative to the repository root
