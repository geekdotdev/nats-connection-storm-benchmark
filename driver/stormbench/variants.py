# SPDX-License-Identifier: Apache-2.0
"""Variants: the configurations a run compares.

Two ways to name them:
- `bench run --a <sha> --b <sha>`: two variants that differ only in the broker commit (what a run always was).
- `bench run --variants FILE`: two or more variants from a TOML file (docs/variants.example.toml): each has a label and a
  broker image (from a commit, or given as is), and may also set an overlay, extra broker configuration, broker and callout
  environment, broker resources, the callout's arguments, and generator settings.

Everything beyond the image reaches the cluster as a generated kustomization: `resources:` the variant's overlay plus a
few strategic-merge patches. `kubectl apply -k` of it, again each time the variant starts, changes the stack to match (and
removes a setting an earlier variant had made, which is why it is not done with `kubectl set env`).
Nothing secret belongs in a variants file: it is recorded in environment.json.
"""
from __future__ import annotations

import json
import os
import re
import tomllib
from pathlib import Path

from .ab import Variant
from .config import REPO_ROOT
from .images import broker_tag

OVERLAYS = Path("deploy/stack/overlays")
_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_STORM_NAME = re.compile(r"^STORM_[A-Z0-9_]+$")
_QUANTITY = re.compile(r"^[0-9]+(\.[0-9]+)?(m|Ki|Mi|Gi|Ti|k|M|G|T)?$")
MAX_CONF_BYTES = 64 * 1024
# Set by the plan or by the manifests' wiring, so a variant cannot override them: the phase variables (every variant runs the
# same phases), the log label and report format, and where the generator finds the broker and its credentials.
RESERVED_STORM = frozenset({"STORM_CONNECTIONS", "STORM_RUNS", "STORM_PAUSE_MS", "STORM_HOLD_MS", "STORM_LABEL", "STORM_JSON", "STORM_SAMPLES",
                            "STORM_URL", "STORM_CA_FILE", "STORM_CPUPROBE",
                            # Where the generator finds its keys (the Secret the provision init container makes). STORM_ROLES,
                            # STORM_TICKET_TTL_S and STORM_TIMEOUT_MS are not reserved: a variant may vary them.
                            "STORM_SCOPED_SEED", "STORM_SCOPED_SEED_FILE", "STORM_AUTH_ACCOUNT", "STORM_AUTH_ACCOUNT_FILE"})
# Variables the manifests fill from Secrets or the pod itself: a plain value on top of one would be an invalid container.
PROTECTED_BROKER_ENV = frozenset({"POD_NAME"})
PROTECTED_CALLOUT_ENV = frozenset({"CALLOUT_CREDS", "CALLOUT_AUTH_SIGNING_SEED", "CALLOUT_TENANT_SIGNING_SEED", "CALLOUT_TENANT_PUBLIC"})
_KEYS = {"label", "commit", "image", "stack", "conf", "conf_file", "callout_args", "broker_env", "broker_resources", "callout_resources",
         "generator_resources", "callout_env", "storm"}


def from_commits(a: str, b: str, a_label: str, b_label: str, image_prefix: str) -> tuple[Variant, Variant]:
    """The two-commit run: variants that differ only in the broker image."""
    return (Variant(a_label, a, image_prefix + broker_tag(a)), Variant(b_label, b, image_prefix + broker_tag(b)))


def load(path: Path, image_prefix: str) -> tuple[Variant, ...]:
    try:
        doc = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        raise ValueError(f"{path}: not valid TOML: {e}") from e
    return parse(doc, image_prefix, path.parent)


def parse(doc: dict, image_prefix: str, base_dir: Path = REPO_ROOT) -> tuple[Variant, ...]:
    extra = set(doc) - {"variant"}
    if extra:
        raise ValueError(f"unknown top-level key(s) {sorted(extra)}: a variants file has only [[variant]] tables")
    items = doc.get("variant")
    if not isinstance(items, list) or len(items) < 2:
        raise ValueError("a variants file needs at least two [[variant]] tables")
    return tuple(_one(i, item, image_prefix, base_dir) for i, item in enumerate(items, 1))


def _text_map(where: str, value: object, name_ok, protected: frozenset[str] = frozenset()) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a table")
    out: dict[str, str] = {}
    for k, v in value.items():
        if not name_ok(k):
            raise ValueError(f"{where}: {k!r} is not a valid name")
        if k in protected:
            raise ValueError(f"{where}: {k} is set by the manifests and cannot be overridden")
        if not isinstance(v, str):
            raise ValueError(f"{where}.{k} must be a quoted string (got {type(v).__name__}): environment values are text")
        out[k] = v
    return out


def _one(n: int, item: object, image_prefix: str, base_dir: Path) -> Variant:
    if not isinstance(item, dict):
        raise ValueError(f"variant {n} must be a table")
    label = item.get("label")
    where = f"variant {label!r}" if isinstance(label, str) else f"variant {n}"
    unknown = set(item) - _KEYS
    if unknown:
        raise ValueError(f"{where}: unknown key(s) {sorted(unknown)}; known: {sorted(_KEYS)}")
    if not isinstance(label, str) or not label:
        raise ValueError(f"variant {n} needs a label")
    commit, image = item.get("commit", ""), item.get("image", "")
    if not isinstance(commit, str) or not isinstance(image, str):
        raise ValueError(f"{where}: commit and image are text")
    if not commit and not image:
        raise ValueError(f"{where} needs a commit (the image is then {image_prefix}nats-server:<short sha>) or an image")
    if image and re.search(r"\s", image):
        raise ValueError(f"{where}: image {image!r} has whitespace")
    if commit and not image:
        image = image_prefix + broker_tag(commit)  # broker_tag insists on a full 40 character sha
    elif commit:
        broker_tag(commit)

    stack = item.get("stack", "")
    if stack:
        stack = _stack(where, stack)
    if "conf" in item and "conf_file" in item:
        raise ValueError(f"{where}: give conf or conf_file, not both")
    conf = item.get("conf", "")
    if "conf_file" in item:
        path = (base_dir / item["conf_file"]).resolve()
        if not path.is_file():
            raise ValueError(f"{where}: conf_file {item['conf_file']!r} not found next to the variants file")
        conf = path.read_text()
    if not isinstance(conf, str) or len(conf.encode()) > MAX_CONF_BYTES:
        raise ValueError(f"{where}: conf must be text of at most {MAX_CONF_BYTES} bytes")

    resources = _resources(where, "broker_resources", item)
    args = item.get("callout_args", [])
    if not isinstance(args, list) or not all(isinstance(a, str) and a for a in args):
        raise ValueError(f"{where}: callout_args must be a list of non-empty strings (it replaces the whole argument list)")
    storm = _text_map(f"{where}.storm", item.get("storm", {}), lambda k: bool(_STORM_NAME.match(k)))
    for k in storm:
        if k in RESERVED_STORM:
            raise ValueError(f"{where}.storm: {k} is set by the run's phases or the manifests and cannot be overridden")
    return Variant(
        label=label, commit=commit, image=image, stack=stack, conf=conf,
        broker_env=_text_map(f"{where}.broker_env", item.get("broker_env", {}), lambda k: bool(_ENV_NAME.match(k)), PROTECTED_BROKER_ENV),
        broker_resources=resources, callout_resources=_resources(where, "callout_resources", item),
        generator_resources=_resources(where, "generator_resources", item),
        callout_env=_text_map(f"{where}.callout_env", item.get("callout_env", {}), lambda k: bool(_ENV_NAME.match(k)), PROTECTED_CALLOUT_ENV),
        callout_args=tuple(args), storm=storm)


def _resources(where: str, key: str, item: dict) -> dict[str, str]:
    """A variant's cpu and memory for one component: Kubernetes quantities, applied as both requests and limits."""
    res = _text_map(f"{where}.{key}", item.get(key, {}), lambda k: k in ("cpu", "memory"))
    for k, v in res.items():
        if not _QUANTITY.match(v):
            raise ValueError(f"{where}.{key}.{k}: {v!r} is not a Kubernetes quantity (3, 500m, 4Gi)")
    return res


def _stack(where: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{where}: stack must be a path like {OVERLAYS}/eks-cluster")
    path = (REPO_ROOT / value).resolve()
    root = (REPO_ROOT / OVERLAYS).resolve()
    if root not in path.parents or not (path / "kustomization.yaml").is_file():
        raise ValueError(f"{where}: stack {value!r} must be an overlay directory under {OVERLAYS} (it needs a kustomization.yaml)")
    return str(path.relative_to(REPO_ROOT))


# -- the generated kustomization -----------------------------------------------------------------------------------

def _patch(kind: str, api: str, name: str, body: dict) -> dict:
    return {"patch": json.dumps({"apiVersion": api, "kind": kind, "metadata": {"name": name}, **body})}


RESTARTED_AT = "storm-bench/restarted-at"


def kustomization(v: Variant, overlay: Path, directory: Path, stamp: str = "") -> dict:
    """The kustomization that puts the stack in this variant's configuration: its overlay, then the variant's broker image and
    other settings as patches. With a stamp, the broker and callout pod templates also get it as an annotation, so applying it
    always replaces their pods (a block needs a fresh broker even when nothing else changed). `overlay` is a directory;
    `directory` is where the kustomization will be written (resources are relative to it, and kustomize refuses absolute
    directories, so both paths are resolved first: on macOS /tmp is a symlink)."""
    rel = os.path.relpath(overlay.resolve(), directory.resolve())
    patches: list[dict] = []
    broker: dict = {"name": "broker", "image": v.image}
    if v.broker_env:
        broker["env"] = [{"name": k, "value": val} for k, val in v.broker_env.items()]
    if v.broker_resources:
        broker["resources"] = {"requests": dict(v.broker_resources), "limits": dict(v.broker_resources)}  # equal: Guaranteed QoS
    template: dict = {"metadata": {"annotations": {RESTARTED_AT: stamp}}} if stamp else {}
    patches.append(_patch("StatefulSet", "apps/v1", "broker", {"spec": {"template": {**template, "spec": {"containers": [broker]}}}}))
    if v.conf:
        patches.append(_patch("ConfigMap", "v1", "storm-broker-config", {"data": {"extra.conf": v.conf}}))
    callout: dict = {"name": "callout"}
    if v.callout_env:
        callout["env"] = [{"name": k, "value": val} for k, val in v.callout_env.items()]
    if v.callout_args:
        callout["args"] = list(v.callout_args)
    if v.callout_resources:
        callout["resources"] = {"requests": dict(v.callout_resources), "limits": dict(v.callout_resources)}  # equal, like the broker's
    if len(callout) > 1 or stamp:
        patches.append(_patch("Deployment", "apps/v1", "callout",
                              {"spec": {"template": {**template, **({"spec": {"containers": [callout]}} if len(callout) > 1 else {})}}}))
    doc: dict = {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization", "resources": [rel]}
    if patches:
        doc["patches"] = patches
    return doc


def write(v: Variant, default_stack: str, root: Path, stamp: str = "") -> Path:
    """Write the variant's kustomization into root/<label>/ and return that directory (JSON is valid YAML)."""
    directory = root / v.label
    directory.mkdir(parents=True, exist_ok=True)
    doc = kustomization(v, REPO_ROOT / (v.stack or default_stack), directory, stamp)
    (directory / "kustomization.yaml").write_text(json.dumps(doc, indent=1))
    return directory


def object_ids(objs: list[dict]) -> set[tuple[str, str]]:
    return {(o["kind"], o["metadata"]["name"]) for o in objs}
