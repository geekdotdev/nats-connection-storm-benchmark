# SPDX-License-Identifier: Apache-2.0
"""Checks to make before a run starts, so a mistake shows up as one clear line instead of a rollout that times out.

What is checked (nothing is changed; every check is read-only):
- the cluster answers, and what its nodes are (architecture, labels, taints, allocatable CPU and memory);
- every variant renders (its overlay plus its settings), and all of them have the same Kubernetes objects, because applying one
  variant's overlay over another's only changes the objects both have: a workload only one has would be left behind;
- the nodes each variant's stack asks for exist: each workload's nodeSelector matches enough usable nodes (a node
  the workload cannot tolerate does not count; a required anti-affinity needs one node per replica), and each
  pod's resource requests fit on one of them (the requests alone: pods already running are not counted);
- the Secret `bench secrets create` makes exists (the operator keys are made by the callout pod's init container, so they cannot be checked
  before the stack is up);
- eks only: every image the run will use (each variant's broker, and what the overlays name) is in its registry,
  can be pulled WITHOUT credentials, which is what the nodes will do, and has a build for every node architecture.
  A private GHCR package and a missing tag both look like ImagePullBackOff otherwise.

The local target skips the image check: its images are loaded into kind, not pulled.
"""
from __future__ import annotations

import copy
import json
import re
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Callable

from pathlib import Path

from . import k8s, secretgen, variants as variants_mod
from .ab import Variant
from .runner import Runner

NAMESPACE = secretgen.NAMESPACE
_ACCEPT = ", ".join(("application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json",
                     "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"))
WORKLOADS = ("Deployment", "StatefulSet", "Job", "DaemonSet")


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


def format_checks(checks: list[Check]) -> str:
    width = max((len(c.name) for c in checks), default=0)
    return "\n".join(f"  {'ok  ' if c.ok else 'FAIL'}  {c.name:<{width}}  {c.detail}" for c in checks)


def failed(checks: list[Check]) -> bool:
    return any(not c.ok for c in checks)


# -- images -------------------------------------------------------------------------------------------------

def split_image(ref: str) -> tuple[str, str, str]:
    """'ghcr.io/o/n:tag' or 'ghcr.io/o/n@sha256:..' -> (registry host, repository, tag or digest)."""
    name, _, digest = ref.partition("@")
    head, sep, tail = name.rpartition(":")
    tag = ""
    if sep and "/" not in tail:  # 'name:tag' (a tag may accompany a digest; the digest wins)
        name, tag = head, tail
    reference = digest or tag or "latest"
    first, slash, rest = name.partition("/")
    if first in ("docker.io", "index.docker.io", "registry-1.docker.io") and slash:
        return "registry-1.docker.io", rest if "/" in rest else f"library/{rest}", reference
    if slash and ("." in first or ":" in first or first == "localhost"):
        return first, rest, reference
    return "registry-1.docker.io", name if slash else f"library/{name}", reference


def _http(url: str, headers: dict[str, str], timeout: float) -> tuple[int, dict[str, str], bytes]:
    if not url.startswith("https://"):
        raise ValueError(f"refusing a non-https registry URL: {url}")
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310 (https only, checked above)
            return r.status, {k.lower(): v for k, v in r.headers.items()}, r.read()
    except urllib.error.HTTPError as e:
        return e.code, {k.lower(): v for k, v in e.headers.items()}, e.read()


@dataclass(frozen=True)
class Manifest:
    status: int                          # HTTP status; 0 if the registry could not be reached
    platforms: tuple[str, ...] = ()      # 'linux/amd64', ... for an index; empty for a single image
    note: str = ""


Fetch = Callable[[str, str, str], Manifest]


def fetch_manifest(registry: str, repo: str, reference: str, timeout: float = 20.0) -> Manifest:
    """GET the manifest anonymously (no credentials ever sent), following the registry's token challenge."""
    url = f"https://{registry}/v2/{repo}/manifests/{urllib.parse.quote(reference, safe=':')}"
    headers = {"Accept": _ACCEPT}
    try:
        status, hdrs, body = _http(url, headers, timeout)
        if status == 401:
            challenge = {k.lower(): v for k, v in re.findall(r'(\w+)="([^"]*)"', hdrs.get("www-authenticate", ""))}
            if "realm" in challenge:
                query = urllib.parse.urlencode({"service": challenge.get("service", ""),
                                                "scope": challenge.get("scope") or f"repository:{repo}:pull"})
                t_status, _, t_body = _http(f"{challenge['realm']}?{query}", {}, timeout)
                if t_status == 200:
                    tok = json.loads(t_body)
                    token = tok.get("token") or tok.get("access_token")
                    if token:
                        status, hdrs, body = _http(url, {**headers, "Authorization": f"Bearer {token}"}, timeout)
    except (OSError, ValueError) as e:  # URLError is an OSError; so are timeouts
        return Manifest(0, note=f"could not reach {registry}: {e}")
    if status != 200:
        return Manifest(status)
    try:
        doc = json.loads(body)
    except ValueError:
        return Manifest(status, note="the registry answered with something that is not a manifest")
    platforms = tuple(f"{m['platform']['os']}/{m['platform']['architecture']}" for m in doc.get("manifests", [])
                      if m.get("platform", {}).get("os") not in (None, "unknown"))
    return Manifest(status, platforms)


def check_image(ref: str, node_arches: set[str], fetch: Fetch = fetch_manifest) -> Check:
    name = f"image {ref}"
    m = fetch(*split_image(ref))
    if m.status == 0:
        return Check(name, False, m.note)
    if m.status in (401, 403):
        # GHCR answers 401 for a private package and for one that does not exist, so the two cannot be told apart.
        return Check(name, False, "cannot be pulled without credentials: the package is private (make it public in the GitHub package settings) or this tag/digest does not exist")
    if m.status == 404:
        return Check(name, False, "not in the registry (wrong tag, never pushed, or private)")
    if m.status != 200:
        return Check(name, False, f"the registry answered {m.status}")
    if m.note:
        return Check(name, False, m.note)
    if not m.platforms:
        return Check(name, True, "pullable; single-platform image, architecture not checked")
    have = {p.split("/", 1)[1] for p in m.platforms if p.startswith("linux/")}
    missing = sorted(node_arches - have)
    if missing:
        return Check(name, False, f"no linux/{missing[0]} build, but the cluster has {missing[0]} nodes (has {', '.join(sorted(have)) or 'none'})")
    return Check(name, True, "pullable without credentials; " + ", ".join(sorted(have)))


# -- quantities and placement ---------------------------------------------------------------------------------

_SUFFIX = {"": 1, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}


def quantity(text: str | int | float) -> float:
    """A Kubernetes quantity as a float: CPU '3' or '500m' in cores, memory '4Gi' or '256Mi' in bytes."""
    m = re.fullmatch(r"([0-9.]+)(m|Ki|Mi|Gi|Ti|k|M|G|T)?", str(text))
    if not m:
        raise ValueError(f"cannot read the quantity {text!r}")
    num, suffix = float(m.group(1)), m.group(2) or ""
    return num / 1000 if suffix == "m" else num * _SUFFIX[suffix]


def _podspec(obj: dict) -> dict:
    return obj.get("spec", {}).get("template", {}).get("spec", {})


def workloads(objs: list[dict]) -> list[dict]:
    return [o for o in objs if o.get("kind") in WORKLOADS]


def images_of(objs: list[dict]) -> list[str]:
    seen: list[str] = []
    for o in workloads(objs):
        spec = _podspec(o)
        for c in [*spec.get("initContainers", []), *spec.get("containers", [])]:
            if c.get("image") and c["image"] not in seen:
                seen.append(c["image"])
    return seen


def _tolerates(tolerations: list[dict], taint: dict) -> bool:
    for t in tolerations:
        if t.get("operator") == "Exists" and not t.get("key"):
            return True
        if t.get("key") != taint.get("key") or t.get("effect") not in (None, "", taint.get("effect")):
            continue
        if t.get("operator") == "Exists" or t.get("value") == taint.get("value"):
            return True
    return False


def _usable(node: dict, spec: dict) -> bool:
    """Could the pod run on this node, as far as labels and taints go?"""
    labels = node["metadata"].get("labels", {})
    if any(labels.get(k) != v for k, v in spec.get("nodeSelector", {}).items()):
        return False
    return all(_tolerates(spec.get("tolerations", []), t) for t in node.get("spec", {}).get("taints", [])
               if t.get("effect") in ("NoSchedule", "NoExecute"))


def placement_checks(objs: list[dict], nodes: list[dict]) -> list[Check]:
    checks: list[Check] = []
    for o in workloads(objs):
        spec, name = _podspec(o), f"{o['kind'].lower()}/{o['metadata']['name']}"
        replicas = 1 if o["kind"] == "Job" else o.get("spec", {}).get("replicas", 1)
        if replicas == 0:
            continue
        usable = [n for n in nodes if _usable(n, spec)]
        selector = ",".join(f"{k}={v}" for k, v in spec.get("nodeSelector", {}).items()) or "any node"
        spread = bool(spec.get("affinity", {}).get("podAntiAffinity", {}).get("requiredDuringSchedulingIgnoredDuringExecution"))
        need = replicas if spread else 1
        if len(usable) < need:
            why = f"{replicas} replicas, one per node" if spread else "at least one"
            checks.append(Check(name, False, f"needs {why} on nodes matching {selector} (and tolerated taints), the cluster has {len(usable)}"))
            continue
        cpu = sum(quantity(c.get("resources", {}).get("requests", {}).get("cpu", 0)) for c in spec.get("containers", []))
        mem = sum(quantity(c.get("resources", {}).get("requests", {}).get("memory", 0)) for c in spec.get("containers", []))
        fits = [n for n in usable if quantity(n["status"]["allocatable"]["cpu"]) >= cpu
                and quantity(n["status"]["allocatable"]["memory"]) >= mem]
        if len(fits) < need:
            best = max(usable, key=lambda n: quantity(n["status"]["allocatable"]["cpu"]))["status"]["allocatable"]
            checks.append(Check(name, False, f"requests {cpu:g} CPU / {mem / 2**30:.1f} GiB do not fit the nodes matching {selector} "
                                             f"(a node has {best['cpu']} CPU / {quantity(best['memory']) / 2**30:.1f} GiB allocatable): "
                                             "the overlay's sizes assume a different instance type"))
            continue
        checks.append(Check(name, True, f"{len(usable)} node(s) match {selector}; requests {cpu:g} CPU / {mem / 2**30:.1f} GiB fit"
                                        + (f"; one per node for {replicas} replica(s)" if spread else "")))
    return checks


# -- do they fit at once? -------------------------------------------------------------------------------------------

# The workloads a run places itself. Every other pod on a node (the cluster's own, CoreDNS, the CSI driver, the probe...) is load the
# run has to fit around, and these are not: they are replaced when a run starts, so what is running now does not count against them.
BENCH_PODS = frozenset({"broker", "callout", "loadgen"})


def _requests(spec: dict) -> tuple[float, float]:
    cs = spec.get("containers", [])
    return (sum(quantity(c.get("resources", {}).get("requests", {}).get("cpu", 0)) for c in cs),
            sum(quantity(c.get("resources", {}).get("requests", {}).get("memory", 0)) for c in cs))


def other_pods_load(pods: list[dict]) -> dict[str, tuple[float, float]]:
    """CPU and memory the pods on each node request, not counting the benchmark's own (BENCH_PODS in its namespace) or finished pods:
    what the scheduler has already given away on that node."""
    load: dict[str, list[float]] = {}
    for p in pods:
        spec, md = p.get("spec", {}), p.get("metadata", {})
        node = spec.get("nodeName")
        if not node or p.get("status", {}).get("phase") in ("Succeeded", "Failed"):
            continue
        if md.get("namespace") == NAMESPACE and md.get("labels", {}).get("app.kubernetes.io/name") in BENCH_PODS:
            continue
        cpu, mem = _requests(spec)
        acc = load.setdefault(node, [0.0, 0.0])
        acc[0] += cpu
        acc[1] += mem
    return {n: (a, b) for n, (a, b) in load.items()}


def with_generator_resources(objs: list[dict], resources: dict) -> list[dict]:
    """The Jobs among objs with a variant's cpu and memory applied as both requests and limits, as the backend does when it makes a storm's Job."""
    if not resources:
        return objs
    out = copy.deepcopy(objs)
    for o in out:
        if o.get("kind") == "Job":
            res = _podspec(o)["containers"][0].setdefault("resources", {})
            res.setdefault("requests", {}).update(resources)
            res.setdefault("limits", {}).update(resources)
    return out


def fit_together(name: str, objs: list[dict], nodes: list[dict], load: dict[str, tuple[float, float]]) -> Check:
    """Whether every pod of these workloads can be on a node at the same time: each on a node it may run on, the pods of a workload
    that must be one per node on different nodes, and the sum of what is placed on a node within what is free there after the pods
    the run does not place. placement_checks asks this of one workload at a time, which cannot see a callout and a generator that
    each fit the client node and not both."""
    pods: list[tuple[str, float, float, list[str], str | None]] = []
    for o in workloads(objs):
        spec, title = _podspec(o), f"{o['kind'].lower()}/{o['metadata']['name']}"
        replicas = 1 if o["kind"] == "Job" else o.get("spec", {}).get("replicas", 1)
        spread = bool(spec.get("affinity", {}).get("podAntiAffinity", {}).get("requiredDuringSchedulingIgnoredDuringExecution"))
        usable = [n["metadata"]["name"] for n in nodes if _usable(n, spec)]
        cpu, mem = _requests(spec)
        pods += [(title, cpu, mem, usable, title if spread else None) for _ in range(replicas)]
    free = {n["metadata"]["name"]: [quantity(n["status"]["allocatable"]["cpu"]) - load.get(n["metadata"]["name"], (0.0, 0.0))[0],
                                    quantity(n["status"]["allocatable"]["memory"]) - load.get(n["metadata"]["name"], (0.0, 0.0))[1]] for n in nodes}
    pods.sort(key=lambda p: -p[1])
    where: list[str] = []
    used_groups: dict[str, set[str]] = {}

    def solve(i: int) -> bool:
        if i == len(pods):
            return True
        title, cpu, mem, usable, group = pods[i]
        for n in usable:
            if free[n][0] + 1e-9 < cpu or free[n][1] + 1e-9 < mem or (group and n in used_groups.get(group, set())):
                continue
            free[n][0] -= cpu
            free[n][1] -= mem
            if group:
                used_groups.setdefault(group, set()).add(n)
            where.append(n)
            if solve(i + 1):
                return True
            where.pop()
            free[n][0] += cpu
            free[n][1] += mem
            if group:
                used_groups[group].discard(n)
        return False

    if solve(0):
        shared = {n: [p[0] for p, w in zip(pods, where) if w == n] for n in set(where) if where.count(n) > 1}
        # the nodes with more than one of the run's pods: how much of what is free there they ask for
        parts = [f"{' + '.join(titles)} ask {sum(p[1] for p, w in zip(pods, where) if w == n):g} of the {free[n][0] + sum(p[1] for p, w in zip(pods, where) if w == n):.2f} "
                 "CPU free there after the cluster's other pods" for n, titles in shared.items()]
        return Check(name, True, f"all {len(pods)} pod(s) fit at once" + (": " + "; ".join(parts) if parts else ", each on a node of its own"))
    asks = ", ".join(f"{t} {c:g} CPU" for t, c, _, _, _ in pods)
    usable = sorted({n for p in pods for n in p[3]})
    room = ", ".join(f"{n} {free[n][0] + sum(p[1] for p, w in zip(pods, where) if w == n):.2f}" for n in usable)
    return Check(name, False, f"these cannot all run at once ({asks}): the nodes they may use have this much CPU free after the cluster's other pods: {room}. "
                              "Add a node (a bigger or a second client node), or lower the requests")


# -- the whole preflight ---------------------------------------------------------------------------------------

def json_stream(text: str) -> list[dict]:
    """The objects in kubectl's JSON output: it prints one JSON value per resource, one after another (a single value, a List
    of them, or several in a row), not one document."""
    decoder, pos, objs = json.JSONDecoder(), 0, []
    text = text.strip()
    while pos < len(text):
        obj, end = decoder.raw_decode(text, pos)
        objs += obj["items"] if obj.get("kind") == "List" else [obj]
        pos = end
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return objs


def _objects(run: Runner, target: str, overlay: str) -> list[dict]:
    rendered = run.output(k8s.kubectl_command("kustomize", overlay, target=target), quiet=True)
    return json_stream(run.output(k8s.kubectl_command("create", "--dry-run=client", "-o", "json", "-f", "-", target=target),
                                  input=rendered, quiet=True))


def monitoring_checks(run: Runner, target: str, overlay: str, fetch: Fetch = fetch_manifest) -> list[Check]:
    """What `bench monitoring up` needs before it applies anything: the overlay renders; the probe can run on every node (a node it
    cannot run on is a node whose pods it cannot read); Prometheus has a node it fits and a StorageClass its claim can use; and, off
    kind, the images can be pulled."""
    try:
        nodes = json.loads(run.output(k8s.kubectl_command("get", "nodes", "-o", "json", target=target), quiet=True))["items"]
    except (subprocess.CalledProcessError, ValueError, KeyError):
        return [Check("cluster", False, f"kubectl cannot read the {target} cluster: is it up, and is the kubeconfig current?")]
    checks = [Check("cluster", bool(nodes), f"{len(nodes)} node(s)")]
    try:
        objs = _objects(run, target, overlay)
    except (subprocess.CalledProcessError, ValueError, KeyError) as e:
        return [*checks, Check("monitoring", False, f"cannot render {overlay} (kubectl kustomize): {e}")]
    checks.append(Check("monitoring", True, f"{overlay} renders: " + ", ".join(sorted({f"{o['kind'].lower()}/{o['metadata']['name']}" for o in workloads(objs)}))))
    for o in workloads(objs):
        title = f"{o['kind'].lower()}/{o['metadata']['name']}"
        if o["kind"] == "DaemonSet":   # one per node by definition: what matters is that no node is left out
            missed = [n["metadata"]["name"] for n in nodes if not _usable(n, _podspec(o))]
            checks.append(Check(title, not missed, "runs on all " + f"{len(nodes)} node(s)" if not missed else
                                f"cannot run on {', '.join(missed)} (a taint it does not tolerate, or a nodeSelector): the pods there would not be measured"))
        else:
            checks += placement_checks([o], nodes)
    for o in objs:
        for claim in o.get("spec", {}).get("volumeClaimTemplates", []):
            name = claim["spec"].get("storageClassName")
            if name:
                have = run.succeeds(k8s.kubectl_command("get", "storageclass", name, target=target))
                checks.append(Check(f"storageclass {name}", have, f"exists, for the claim {claim['metadata']['name']}" if have else
                                    f"missing: the claim {claim['metadata']['name']} would never bind (on eks: `bench cluster addons --target eks`)"))
    if target == "local":
        checks.append(Check("images", True, "not checked: local images are loaded into kind (`bench images load-kind`), not pulled"))
    else:
        arches = {n["status"]["nodeInfo"]["architecture"] for n in nodes}
        checks += [check_image(ref, arches, fetch) for ref in images_of(objs)]
    return checks


def preflight(run: Runner, target: str, variants: tuple[Variant, ...], default_stack: str, loadgen: str,
              fetch: Fetch = fetch_manifest) -> list[Check]:
    """Every check, in order; a failing one does not stop the rest (unless the cluster cannot be read at all)."""
    checks: list[Check] = []
    try:
        nodes = json.loads(run.output(k8s.kubectl_command("get", "nodes", "-o", "json", target=target), quiet=True))["items"]
    except (subprocess.CalledProcessError, ValueError, KeyError):
        return [Check("cluster", False, f"kubectl cannot read the {target} cluster: is it up, and is the kubeconfig current?")]
    arches = sorted({n["status"]["nodeInfo"]["architecture"] for n in nodes})
    checks.append(Check("cluster", bool(nodes), f"{len(nodes)} node(s), architecture {', '.join(arches) or 'none'}"))
    if not nodes:
        return checks

    per_variant: dict[str, list[dict]] = {}
    with tempfile.TemporaryDirectory(prefix="storm-preflight-") as tmp:
        try:
            for v in variants:
                per_variant[v.label] = _objects(run, target, str(variants_mod.write(v, default_stack, Path(tmp))))
            loadgen_objs = _objects(run, target, loadgen)
        except (subprocess.CalledProcessError, ValueError, KeyError) as e:
            return [*checks, Check("variants", False, f"cannot render the variants and {loadgen} (kubectl kustomize): {e}")]
    first = variants[0].label
    ids = {label: variants_mod.object_ids(objs) for label, objs in per_variant.items()}
    odd = [label for label in ids if ids[label] != ids[first]]
    if odd:
        gap = sorted(ids[first] ^ ids[odd[0]])
        checks.append(Check("variants", False, f"{odd[0]} and {first} do not have the same objects (differ in {gap[0][0]}/{gap[0][1]}): "
                                               "applying one over the other would leave the extra object behind"))
    else:
        checks.append(Check("variants", True, f"{len(variants)} variants render, with the same objects in each"))
    for v in variants:
        checks += [Check(f"{v.label}: {c.name}", c.ok, c.detail) for c in placement_checks(per_variant[v.label], nodes)]
    if any(v.generator_resources for v in variants):   # the generator's size differs by variant: each is checked with its own
        for v in variants:
            checks += [Check(f"{v.label}: {c.name}", c.ok, c.detail)
                       for c in placement_checks(with_generator_resources(loadgen_objs, v.generator_resources), nodes)]
    else:
        checks += placement_checks(loadgen_objs, nodes)
    try:
        pods = json.loads(run.output(k8s.kubectl_command("get", "pods", "-A", "-o", "json", target=target), quiet=True))["items"]
    except (subprocess.CalledProcessError, ValueError, KeyError):
        checks.append(Check("fit together", True, "not checked: the cluster's pods could not be listed"))
    else:
        load = other_pods_load(pods)
        for v in variants:
            checks.append(fit_together(f"{v.label}: fit together", per_variant[v.label] + with_generator_resources(loadgen_objs, v.generator_resources),
                                       nodes, load))

    secrets = [secretgen.TLS_NAME]
    have = run.succeeds(k8s.kubectl_command("-n", NAMESPACE, "get", "secret", *secrets, target=target))
    checks.append(Check("secrets", have, ", ".join(secrets) + (" exists" if have else f" missing in {NAMESPACE}: run `bench secrets create --target {target}`")))

    if target == "local":
        checks.append(Check("images", True, "not checked: local images are loaded into kind (`bench images load-kind`), not pulled"))
    else:
        all_objs = [o for objs in per_variant.values() for o in objs] + loadgen_objs
        for ref in dict.fromkeys([*(v.image for v in variants), *images_of(all_objs)]):
            checks.append(check_image(ref, set(arches), fetch))
    return checks
