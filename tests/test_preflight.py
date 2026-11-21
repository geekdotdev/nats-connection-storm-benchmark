# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
import types

import pytest

from stormbench import cli, preflight as pf, runcmd
from stormbench.ab import Variant
from stormbench.preflight import Check, Manifest
from stormbench.runner import Runner

SHA_A, SHA_B = "a" * 40, "b" * 40


def node(name, role=None, cpu="4", mem="8Gi", arch="amd64", taints=()):
    labels = {"role": role} if role else {}
    return {"metadata": {"name": name, "labels": labels}, "spec": {"taints": list(taints)},
            "status": {"allocatable": {"cpu": cpu, "memory": mem}, "nodeInfo": {"architecture": arch}}}


BROKER_TAINT = {"key": "dedicated", "value": "broker", "effect": "NoSchedule"}


def workload(kind="StatefulSet", name="broker", replicas=3, selector=None, tolerate=True, spread=True,
             cpu="3", mem="4Gi", image="ghcr.io/o/nats-server@sha256:" + "1" * 64):
    spec = {"containers": [{"name": name, "image": image, "resources": {"requests": {"cpu": cpu, "memory": mem}}}]}
    if selector:
        spec["nodeSelector"] = selector
    if tolerate:
        spec["tolerations"] = [{"key": "dedicated", "operator": "Equal", "value": "broker", "effect": "NoSchedule"}]
    if spread:
        spec["affinity"] = {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [{"topologyKey": "kubernetes.io/hostname"}]}}
    return {"kind": kind, "metadata": {"name": name}, "spec": {"replicas": replicas, "template": {"spec": spec}}}


THREE_BROKERS = [node(f"b{i}", "broker", taints=[BROKER_TAINT]) for i in range(3)] + [node("c0", "client")]


# -- image references and quantities ------------------------------------------------------------------------------
@pytest.mark.parametrize("ref,expected", [
    ("ghcr.io/o/n:tag", ("ghcr.io", "o/n", "tag")),
    ("ghcr.io/o/n@sha256:abc", ("ghcr.io", "o/n", "sha256:abc")),
    ("alpine:3.20@sha256:abc", ("registry-1.docker.io", "library/alpine", "sha256:abc")),
    ("alpine", ("registry-1.docker.io", "library/alpine", "latest")),
    ("docker.io/library/alpine:3", ("registry-1.docker.io", "library/alpine", "3")),
    ("docker.io/someone/x:1", ("registry-1.docker.io", "someone/x", "1")),
    ("localhost:5000/x/y:1", ("localhost:5000", "x/y", "1")),
])
def test_split_image(ref, expected):
    assert pf.split_image(ref) == expected


def test_quantities():
    assert pf.quantity("3") == 3 and pf.quantity("500m") == 0.5 and pf.quantity(2) == 2
    assert pf.quantity("4Gi") == 4 * 2**30 and pf.quantity("256Mi") == 256 * 2**20 and pf.quantity("1G") == 1e9
    with pytest.raises(ValueError):
        pf.quantity("lots")


# -- one image ----------------------------------------------------------------------------------------------------
def fake(manifest):
    return lambda registry, repo, ref: manifest


@pytest.mark.parametrize("m,ok,words", [
    (Manifest(200, ("linux/amd64", "linux/arm64")), True, "amd64, arm64"),
    (Manifest(200, ("linux/arm64",)), False, "no linux/amd64 build"),
    (Manifest(200, ("unix/amd64",)), False, "no linux/amd64 build"),
    (Manifest(200), True, "architecture not checked"),
    (Manifest(401), False, "private"),
    (Manifest(403), False, "private"),
    (Manifest(404), False, "not in the registry"),
    (Manifest(500), False, "500"),
    (Manifest(0, note="could not reach x"), False, "could not reach"),
])
def test_check_image(m, ok, words):
    c = pf.check_image("ghcr.io/o/n:t", {"amd64"}, fake(m))
    assert c.ok is ok and words in c.detail


def test_fetch_follows_the_token_challenge_without_sending_credentials(monkeypatch):
    seen = []

    def http(url, headers, timeout):
        seen.append((url, headers))
        if "token" in url:
            return 200, {}, json.dumps({"token": "T"}).encode()
        if "Authorization" not in headers:
            return 401, {"www-authenticate": 'Bearer realm="https://auth.example/token",service="reg",scope="repository:o/n:pull"'}, b""
        index = {"manifests": [{"platform": {"os": "linux", "architecture": "amd64"}},
                               {"platform": {"os": "unknown", "architecture": "unknown"}}]}  # an attestation
        return 200, {}, json.dumps(index).encode()

    monkeypatch.setattr(pf, "_http", http)
    m = pf.fetch_manifest("ghcr.io", "o/n", "sha256:abc")
    assert m == Manifest(200, ("linux/amd64",))
    assert "service=reg" in seen[1][0] and "Authorization" not in seen[1][1]  # the token request carries no credentials
    assert seen[2][1]["Authorization"] == "Bearer T"


def test_fetch_reports_an_unreachable_registry_and_refuses_http(monkeypatch):
    def down(url, headers, timeout):
        raise OSError("name not known")

    with monkeypatch.context() as m:
        m.setattr(pf, "_http", down)
        assert pf.fetch_manifest("nope.example", "o/n", "t").status == 0
    with pytest.raises(ValueError):  # the real _http, which never makes a plain-http request
        pf._http("http://insecure.example/v2/", {}, 1)


# -- placement ----------------------------------------------------------------------------------------------------
def test_three_brokers_fit_three_tolerated_nodes():
    (c,) = pf.placement_checks([workload(selector={"role": "broker"})], THREE_BROKERS)
    assert c.ok and "3 node(s)" in c.detail and "one per node" in c.detail


def test_too_few_nodes_for_the_replicas():
    (c,) = pf.placement_checks([workload(selector={"role": "broker"})], THREE_BROKERS[2:])
    assert not c.ok and "3 replicas, one per node" in c.detail and "the cluster has 1" in c.detail


def test_a_taint_the_pod_does_not_tolerate_removes_the_node():
    (c,) = pf.placement_checks([workload(selector={"role": "broker"}, tolerate=False, replicas=1, spread=False)], THREE_BROKERS)
    assert not c.ok and "the cluster has 0" in c.detail


def test_requests_larger_than_a_node_are_reported_as_an_instance_type_mismatch():
    small = [node(f"b{i}", "broker", cpu="1930m", mem="3Gi", taints=[BROKER_TAINT]) for i in range(3)]
    (c,) = pf.placement_checks([workload(selector={"role": "broker"})], small)
    assert not c.ok and "do not fit" in c.detail and "instance type" in c.detail


def test_workloads_without_a_selector_or_with_zero_replicas():
    objs = [workload("Deployment", "callout", 1, spread=False, tolerate=False, cpu="500m", mem="256Mi"),
            workload("Deployment", "leaf", 0), {"kind": "Service", "metadata": {"name": "broker"}}]
    checks = pf.placement_checks(objs, [node("c0", "client")])
    assert [c.name for c in checks] == ["deployment/callout"] and checks[0].ok


def test_a_job_needs_one_node():
    (c,) = pf.placement_checks([workload("Job", "loadgen", 1, selector={"role": "client"}, spread=False, tolerate=False, cpu="2", mem="1Gi")],
                               THREE_BROKERS)
    assert c.ok


def test_images_of_lists_each_once_in_order():
    objs = [workload(image="a/x:1"), workload("Deployment", "d", image="b/y:1"), workload("Job", "j", image="a/x:1")]
    assert pf.images_of(objs) == ["a/x:1", "b/y:1"]


def test_json_stream_reads_one_object_a_list_or_several_in_a_row():
    one, two = {"kind": "Job", "metadata": {"name": "j"}}, {"kind": "Service", "metadata": {"name": "s"}}
    assert pf.json_stream(json.dumps(one)) == [one]
    assert pf.json_stream(json.dumps({"kind": "List", "items": [one, two]})) == [one, two]
    assert pf.json_stream(json.dumps(one, indent=2) + "\n" + json.dumps(two, indent=2) + "\n") == [one, two]
    assert pf.json_stream("") == []


# -- the whole preflight ------------------------------------------------------------------------------------------
class ClusterRunner(Runner):
    def __init__(self, nodes, stack_objs, loadgen_objs, secrets=True, down=False):
        super().__init__()
        self.nodes, self.secrets, self.down, self.ran = nodes, secrets, down, []
        self.rendered = {"deploy/stack": stack_objs, "deploy/loadgen": loadgen_objs}
        self.last = None

    def output(self, cmd, *, input=None, env=None, dry_run_value="", quiet=False):
        self.ran.append(cmd)
        if "nodes" in cmd:
            if self.down:
                raise subprocess.CalledProcessError(1, cmd)
            return json.dumps({"items": self.nodes})
        if "kustomize" in cmd:
            self.last = self.rendered["deploy/loadgen"] if cmd[-1].startswith("deploy/loadgen") else self.rendered["deploy/stack"]
            return "rendered"
        assert input == "rendered"
        return "\n".join(json.dumps(o, indent=4) for o in self.last)  # what kubectl prints: one JSON object per resource

    def succeeds(self, cmd):
        self.ran.append(cmd)
        return self.secrets


@pytest.fixture
def kubeconfigs(monkeypatch, tmp_path):
    for var in ("STORM_KUBECONFIG", "STORM_EKS_KUBECONFIG"):
        f = tmp_path / var
        f.write_text("x")
        monkeypatch.setenv(var, str(f))


STACK = [workload(selector={"role": "broker"}), workload("Deployment", "callout", 1, {"role": "client"}, False, False, "500m", "256Mi", "ghcr.io/o/callout@sha256:" + "2" * 64)]
LOADGEN = [workload("Job", "loadgen", 1, {"role": "client"}, False, False, "2", "1Gi", "ghcr.io/o/loadgen@sha256:" + "3" * 64)]
ARMS = tuple(Variant(l, c * 40, f"ghcr.io/o/nats-server:{c * 12}") for l, c in (("main", "a"), ("fixed", "b")))


def test_a_good_eks_cluster_passes_every_check(kubeconfigs):
    run = ClusterRunner(THREE_BROKERS, STACK, LOADGEN)
    seen = []
    checks = pf.preflight(run, "eks", ARMS, "deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks",
                          lambda *a: seen.append(a) or Manifest(200, ("linux/amd64", "linux/arm64")))
    assert not pf.failed(checks), pf.format_checks(checks)
    assert [c.name for c in checks][:4] == ["cluster", "variants", "main: statefulset/broker", "main: deployment/callout"]
    assert len([c for c in checks if c.name.startswith("image ")]) == 5  # two arms, the broker digest, callout, loadgen
    assert len(seen) == 5 and all(a[0] == "ghcr.io" for a in seen)


def test_everything_wrong_is_reported_at_once(kubeconfigs):
    run = ClusterRunner(THREE_BROKERS[:1] + THREE_BROKERS[3:], STACK, LOADGEN, secrets=False)
    checks = pf.preflight(run, "eks", ARMS, "deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks", fake(Manifest(401)))
    bad = {c.name for c in checks if not c.ok}
    assert {"main: statefulset/broker", "fixed: statefulset/broker"} <= bad and "secrets" in bad and "image ghcr.io/o/nats-server:aaaaaaaaaaaa" in bad
    assert "bench secrets create --target eks" in next(c.detail for c in checks if c.name == "secrets")


def test_an_image_without_an_arm64_build_fails_on_arm_nodes(kubeconfigs):
    nodes = [node("n", arch="arm64")]
    run = ClusterRunner(nodes, [workload("Deployment", "callout", 1, spread=False, tolerate=False, cpu="500m", mem="256Mi")], LOADGEN)
    checks = pf.preflight(run, "eks", ARMS, "deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks", fake(Manifest(200, ("linux/amd64",))))
    assert any(not c.ok and "no linux/arm64 build" in c.detail for c in checks)


def test_the_local_target_does_not_check_images(kubeconfigs):
    called = []
    run = ClusterRunner([node("kind")], [workload("Deployment", "callout", 1, spread=False, tolerate=False, cpu="500m", mem="256Mi")], LOADGEN)
    checks = pf.preflight(run, "local", ARMS, "deploy/stack/overlays/local",
                          "deploy/loadgen/overlays/local", lambda *a: called.append(a))
    assert not called and checks[-1].name == "images" and checks[-1].ok


def test_an_unreachable_cluster_is_one_failing_check(kubeconfigs):
    checks = pf.preflight(ClusterRunner([], [], [], down=True), "eks", ARMS, "s", "l", fake(Manifest(200)))
    assert len(checks) == 1 and checks[0].name == "cluster" and not checks[0].ok


def test_every_command_uses_the_targets_kubeconfig(kubeconfigs, tmp_path):
    run = ClusterRunner(THREE_BROKERS, STACK, LOADGEN)
    pf.preflight(run, "eks", ARMS, "deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks", fake(Manifest(200, ("linux/amd64",))))
    assert run.ran and all(c[:3] == ["kubectl", "--kubeconfig", str(tmp_path / "STORM_EKS_KUBECONFIG")] for c in run.ran)


# -- the commands -------------------------------------------------------------------------------------------------
def test_run_stops_before_applying_anything_when_preflight_fails(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("REGISTRY", "ghcr.io/o")
    monkeypatch.setattr(runcmd.preflight, "preflight", lambda *a, **k: [Check("secrets", False, "missing")])

    def boom(*a, **k):
        raise AssertionError("a failed preflight must not reach the cluster")

    monkeypatch.setattr(runcmd, "KubeBackend", boom)
    argv = ["--target", "eks", "run", "--a", SHA_A, "--b", SHA_B, "--out", str(tmp_path / "out")]
    assert cli.main(argv) == 1
    out = capsys.readouterr().out
    assert "FAIL  secrets" in out and "nothing was applied" in out
    assert not (tmp_path / "out").exists()


def test_no_preflight_skips_the_checks(monkeypatch, tmp_path):
    monkeypatch.setenv("REGISTRY", "ghcr.io/o")
    monkeypatch.setattr(runcmd.preflight, "preflight", lambda *a, **k: pytest.fail("checks were skipped"))
    reached = []

    class Stop(Exception):
        pass

    def backend(*a, **k):
        reached.append(k)
        raise Stop

    monkeypatch.setattr(runcmd, "KubeBackend", backend)
    with pytest.raises(Stop):
        cli.main(["--target", "eks", "run", "--a", SHA_A, "--b", SHA_B, "--out", str(tmp_path / "out"), "--no-preflight"])
    assert reached[0]["target"] == "eks"


def test_preflight_command_passes_the_arm_images_and_exits_by_the_result(monkeypatch, capsys):
    monkeypatch.setenv("REGISTRY", "ghcr.io/o/")
    got = {}

    def fake_preflight(run, target, variants, stack, loadgen):
        got.update(target=target, images=[v.image for v in variants], stack=stack, loadgen=loadgen)
        return [Check("cluster", True, "4 node(s)")]

    monkeypatch.setattr(runcmd.preflight, "preflight", fake_preflight)
    assert cli.main(["preflight", "--target", "eks", "--a", SHA_A, "--b", SHA_B, "--stack-overlay", "deploy/stack/overlays/eks-cluster"]) == 0
    assert got == {"target": "eks", "images": ["ghcr.io/o/nats-server:" + "a" * 12, "ghcr.io/o/nats-server:" + "b" * 12],
                   "stack": "deploy/stack/overlays/eks-cluster", "loadgen": "deploy/loadgen/overlays/eks"}
    assert "ok    cluster" in capsys.readouterr().out
    monkeypatch.setattr(runcmd.preflight, "preflight", lambda *a, **k: [Check("cluster", False, "down")])
    assert cli.main(["preflight", "--a", SHA_A, "--b", SHA_B]) == 1


def test_preflight_in_a_dry_run_reads_nothing(capsys):
    assert cli.main(["--dry-run", "preflight", "--a", SHA_A, "--b", SHA_B]) == 0
    assert "skipped in a dry run" in capsys.readouterr().out


# -- the repository's own overlays and Dockerfiles ----------------------------------------------------------------
def render_overlay(path):
    import shutil
    from pathlib import Path
    kubectl = shutil.which("kubectl")
    if not kubectl:
        pytest.skip("kubectl is not installed")
    root = Path(__file__).resolve().parents[1]
    rendered = subprocess.run([kubectl, "kustomize", path], cwd=root, check=True, capture_output=True, text=True).stdout
    # `label --local` converts YAML to JSON without a cluster (`create --dry-run=client` needs API discovery); it prints one
    # JSON object per resource, one after another.
    text = subprocess.run([kubectl, "label", "--local", "-f", "-", "-o", "json", "t=1"], input=rendered, check=True,
                          capture_output=True, text=True, env={"KUBECONFIG": "/dev/null"}).stdout
    decoder, pos, objs = json.JSONDecoder(), 0, []
    while pos < len(text):
        obj, end = decoder.raw_decode(text, pos)
        objs += obj["items"] if obj.get("kind") == "List" else [obj]
        pos = end
        while pos < len(text) and text[pos].isspace():
            pos += 1
    return objs


C6I_XLARGE = dict(cpu="3920m", mem="7Gi")  # roughly what a 4 vCPU / 8 GiB node offers after the kubelet's reservation


@pytest.mark.parametrize("stack,brokers", [("eks", 1), ("eks-cluster", 3), ("eks-cluster-leaf", 3)])
def test_the_eks_overlays_fit_a_c6i_xlarge_cluster_of_the_default_shape(stack, brokers):
    objs = [*render_overlay(f"deploy/stack/overlays/{stack}"), *render_overlay("deploy/loadgen/overlays/eks")]
    nodes = [node(f"b{i}", "broker", taints=[BROKER_TAINT], **C6I_XLARGE) for i in range(brokers)] + [node("c0", "client", **C6I_XLARGE)]
    checks = pf.placement_checks(objs, nodes)
    assert checks and not pf.failed(checks), pf.format_checks(checks)
    assert all("@sha256:" in i for i in pf.images_of(objs))  # every image the eks overlays name is pinned by digest


def test_the_cluster_overlay_does_not_fit_a_cluster_with_one_broker_node():
    objs = render_overlay("deploy/stack/overlays/eks-cluster")
    nodes = [node("b0", "broker", taints=[BROKER_TAINT], **C6I_XLARGE), node("c0", "client", **C6I_XLARGE)]
    assert any(not c.ok and c.name == "statefulset/broker" for c in pf.placement_checks(objs, nodes))


def test_every_dockerfile_base_image_is_pinned_by_digest():
    import re
    from pathlib import Path
    for df in sorted((Path(__file__).resolve().parents[1] / "images").glob("*/Dockerfile")):
        text = df.read_text()
        args = dict(re.findall(r"^ARG (\w+)=(\S+)$", text, re.M))
        stages = set(re.findall(r"^FROM .* AS (\w+)$", text, re.M))
        froms = re.findall(r"^FROM (?:--platform=\S+ )?(\S+)", text, re.M)
        assert froms, df
        for ref in froms:
            if ref in stages:
                continue
            resolved = re.sub(r"\$\{(\w+)\}", lambda m: args[m.group(1)], ref)
            assert re.search(r"@sha256:[0-9a-f]{64}$", resolved), f"{df}: base image {ref} -> {resolved} is not pinned by digest"
