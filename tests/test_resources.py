# SPDX-License-Identifier: Apache-2.0
"""Per-variant callout and generator resources, and the check that everything a run places fits at once."""
import json
import shutil
import subprocess

import pytest

from stormbench import ab, kube, preflight as pf, variants
from stormbench.config import REPO_ROOT
from test_jobs import Scripted, kubeconfig  # noqa: F401
from test_preflight import ClusterRunner, LOADGEN, STACK, THREE_BROKERS, kubeconfigs, node, workload  # noqa: F401

A, B = "a" * 40, "b" * 40


@pytest.fixture(autouse=True)
def a_kubeconfig_that_exists(kubeconfigs):   # noqa: F811 -- the preflight tests' fixture: the cluster itself is faked
    pass


def parse(text):
    return variants.parse(__import__("tomllib").loads(text), "localhost/", REPO_ROOT)


TWO = """
[[variant]]
label = "small"
commit = "%s"
[[variant]]
label = "big"
commit = "%s"
[variant.callout_resources]
cpu = "2"
memory = "512Mi"
[variant.generator_resources]
cpu = "3"
""" % (A, B)


# -- the settings ---------------------------------------------------------------------------------------------------------------------------
def test_a_variant_can_size_its_callout_and_its_generator():
    small, big = parse(TWO)
    assert small.callout_resources == {} and small.generator_resources == {}
    assert big.callout_resources == {"cpu": "2", "memory": "512Mi"} and big.generator_resources == {"cpu": "3"}
    assert big.settings()["callout_resources"] == {"cpu": "2", "memory": "512Mi"} and "generator_resources" in big.settings()
    assert "callout_resources" not in small.settings()


def test_the_new_settings_survive_the_recorded_form_the_state_keeps():
    big = parse(TWO)[1]
    assert ab.Variant.from_dict(json.loads(json.dumps(big.describe()))) == big


@pytest.mark.parametrize("snippet,words", [
    ('[variant.callout_resources]\ncpu = "lots"', "callout_resources.cpu"),
    ('[variant.generator_resources]\nmemory = "4 Gi"', "generator_resources.memory"),
    ('[variant.generator_resources]\ngpu = "1"', "generator_resources"),
    ('[variant.callout_resources]\ncpu = 2', "quoted string"),
])
def test_a_bad_resource_is_refused_naming_the_setting(snippet, words):
    with pytest.raises(ValueError) as e:
        parse(f'[[variant]]\nlabel = "a"\ncommit = "{A}"\n[[variant]]\nlabel = "b"\ncommit = "{B}"\n{snippet}\n')
    assert words in str(e.value)


def test_the_example_file_documents_both_settings():
    text = (REPO_ROOT / "docs" / "variants.example.toml").read_text()
    assert "callout_resources" in text and "generator_resources" in text


# -- the callout's size reaches the cluster ---------------------------------------------------------------------------------------------------
@pytest.mark.skipif(not shutil.which("kubectl"), reason="needs kubectl kustomize")
def test_the_callouts_resources_replace_the_overlays_cpu_and_keep_what_it_leaves_out(tmp_path):
    big = parse(TWO)[1]
    d = variants.write(big, "deploy/stack/overlays/eks", tmp_path)
    docs = subprocess.run(["kubectl", "kustomize", str(d)], capture_output=True, text=True, check=True, cwd=REPO_ROOT).stdout
    callout = [x for x in docs.split("\n---\n") if "kind: Deployment" in x and "name: callout" in x][0]
    # requests and limits are both 2 CPU and 512Mi: the variant's values, as both
    assert callout.count("cpu: \"2\"") + callout.count("cpu: 2") >= 2 and callout.count("memory: 512Mi") >= 2
    small = variants.write(parse(TWO)[0], "deploy/stack/overlays/eks", tmp_path)
    plain = subprocess.run(["kubectl", "kustomize", str(small)], capture_output=True, text=True, check=True, cwd=REPO_ROOT).stdout
    assert "cpu: \"1\"" in plain or "cpu: 1" in plain                        # without the setting the overlay's own size stands


# -- the generator's size reaches each storm's Job -------------------------------------------------------------------------------------------
def job_backend(run, resources):
    b = kube.KubeBackend(run, sleep=lambda s: None, read_cpu=False)
    b._job = {"metadata": {"name": "loadgen"}, "spec": {"template": {"spec": {"containers": [
        {"name": "loadgen", "resources": {"requests": {"cpu": "2", "memory": "1Gi"}, "limits": {"cpu": "2", "memory": "1Gi"}}}]}}}}
    b._generator_resources = resources
    return b


def container_of(run):
    return json.loads(run.stdin[run.cmds.index(next(c for c in run.cmds if "create" in c))])["spec"]["template"]["spec"]["containers"][0]


def test_a_storm_job_gets_the_variants_generator_resources_and_keeps_the_rest(kubeconfig):
    run = Scripted([json.dumps({"status": {"succeeded": 1}}), "LOG"])
    job_backend(run, {"cpu": "3"}).run_storm("big-b1-n800", {})
    r = container_of(run)["resources"]
    assert r["requests"] == {"cpu": "3", "memory": "1Gi"} and r["limits"] == {"cpu": "3", "memory": "1Gi"}


def test_without_a_generator_setting_the_overlays_job_is_left_as_it_is(kubeconfig):
    run = Scripted([json.dumps({"status": {"succeeded": 1}}), "LOG"])
    job_backend(run, {}).run_storm("small-b1-n800", {})
    assert container_of(run)["resources"]["requests"] == {"cpu": "2", "memory": "1Gi"}


def test_each_variant_starts_with_its_own_generator_size_and_the_next_does_not_inherit_it(kubeconfig):
    run = Scripted([])
    b = kube.KubeBackend(run, sleep=lambda s: None, read_cpu=False)
    b._apply_variant = lambda v, stamp: None
    b._wait_rollout = lambda what: None
    b._broker_facts = lambda: {}
    small, big = parse(TWO)
    b.start_variant(big)
    assert b._generator_resources == {"cpu": "3"}
    b.start_variant(small)
    assert b._generator_resources == {}


# -- preflight: do they fit at once? --------------------------------------------------------------------------------------------------------
def cluster(client_nodes=1, other_cpu=0.0, **kw):
    nodes = [n for n in THREE_BROKERS if n["metadata"]["labels"].get("role") == "broker"] + [node(f"c{i}", "client") for i in range(client_nodes)]
    pods = [{"metadata": {"namespace": "kube-system", "name": "sys"}, "spec": {"nodeName": "c0", "containers": [{"resources": {"requests": {"cpu": str(other_cpu)}}}]},
             "status": {"phase": "Running"}}] if other_cpu else []
    return nodes, pods


# The fake cluster renders one stack for every variant (the real kustomize applies each variant's patches), so a test that needs a
# bigger callout hands it the stack with the callout at that size.
def stack_with_callout(cpu):
    return [STACK[0], workload("Deployment", "callout", 1, {"role": "client"}, False, False, cpu, "512Mi", "ghcr.io/o/callout@sha256:" + "2" * 64)]


def run_preflight(vs, nodes, pods, stack=None):
    run = ClusterRunner(nodes, stack or STACK, LOADGEN, pods=pods)
    return pf.preflight(run, "eks", vs, "deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks",
                        lambda *a: pf.Manifest(200, ("linux/amd64", "linux/arm64")))


def by_name(checks):
    return {c.name: c for c in checks}


def test_a_callout_and_a_generator_that_each_fit_the_client_node_but_not_together_are_refused():
    big = ab.Variant("big", B, "ghcr.io/o/nats-server:" + "b" * 12, callout_resources={"cpu": "2"}, generator_resources={"cpu": "3"})
    nodes, pods = cluster(client_nodes=1)
    c = by_name(run_preflight((big,), nodes, pods, stack_with_callout("2")))
    assert c["big: deployment/callout"].ok and c["big: job/loadgen"].ok          # each alone fits a 4 CPU node...
    bad = c["big: fit together"]
    assert not bad.ok and "cannot all run at once" in bad.detail and "deployment/callout 2 CPU" in bad.detail and "job/loadgen 3 CPU" in bad.detail
    assert "Add a node" in bad.detail
    small = ab.Variant("small", A, "ghcr.io/o/nats-server:" + "a" * 12)
    ok = by_name(run_preflight((small,), nodes, pods))["small: fit together"]    # ...and the default sizes (0.5 + 2) fit it with room to spare
    assert ok.ok and "deployment/callout" in ok.detail and "2.5 of the 4.00" in ok.detail


def test_a_second_client_node_makes_the_same_variant_fit():
    big = ab.Variant("big", B, "ghcr.io/o/nats-server:" + "b" * 12, callout_resources={"cpu": "2"}, generator_resources={"cpu": "3"})
    nodes, pods = cluster(client_nodes=2)
    c = by_name(run_preflight((big,), nodes, pods, stack_with_callout("2")))
    assert c["big: fit together"].ok and "each on a node of its own" in c["big: fit together"].detail


def test_what_the_cluster_already_uses_on_the_node_counts_against_the_pods():
    v = ab.Variant("v", A, "ghcr.io/o/nats-server:" + "a" * 12, generator_resources={"cpu": "3"})
    w = ab.Variant("w", B, "ghcr.io/o/nats-server:" + "b" * 12)
    nodes, pods = cluster(other_cpu=0.9)                                          # 4 - 0.9 = 3.1 free: callout 0.5 + generator 3 = 3.5
    c = by_name(run_preflight((v, w), nodes, pods))
    assert not c["v: fit together"].ok and "3.10" in c["v: fit together"].detail
    nodes, pods = cluster(other_cpu=0.0)
    assert by_name(run_preflight((v, w), nodes, pods))["v: fit together"].ok


def test_the_benchmarks_own_running_pods_and_finished_pods_do_not_count_against_it():
    bench = {"metadata": {"namespace": "storm-bench", "labels": {"app.kubernetes.io/name": "callout"}},
             "spec": {"nodeName": "c0", "containers": [{"resources": {"requests": {"cpu": "3"}}}]}, "status": {"phase": "Running"}}
    done = {"metadata": {"namespace": "storm-bench", "labels": {"app.kubernetes.io/name": "other"}},
            "spec": {"nodeName": "c0", "containers": [{"resources": {"requests": {"cpu": "3"}}}]}, "status": {"phase": "Succeeded"}}
    other = {"metadata": {"namespace": "storm-bench", "labels": {"app.kubernetes.io/name": "prometheus"}},
             "spec": {"nodeName": "c0", "containers": [{"resources": {"requests": {"cpu": "0.25"}}}]}, "status": {"phase": "Running"}}
    assert pf.other_pods_load([bench, done, other]) == {"c0": (0.25, 0.0)}         # a pod of the stack that is not one the run places does count


def test_replicas_that_must_be_one_per_node_need_that_many_nodes_at_once():
    nodes = [node(f"b{i}", "broker", taints=[{"key": "dedicated", "value": "broker", "effect": "NoSchedule"}]) for i in range(2)] + [node("c0", "client")]
    on_brokers = workload(selector={"role": "broker"})                                 # three brokers, one per node, on the broker nodes only
    assert not pf.fit_together("x", [on_brokers], nodes, {}).ok                        # two broker nodes
    assert pf.fit_together("x", [workload(replicas=2, selector={"role": "broker"})], nodes, {}).ok


def test_a_pod_that_cannot_list_the_clusters_pods_is_skipped_not_failed(monkeypatch):
    class NoPods(ClusterRunner):
        def output(self, cmd, **kw):
            if "pods" in cmd:
                raise subprocess.CalledProcessError(1, cmd)
            return super().output(cmd, **kw)
    nodes, _ = cluster()
    run = NoPods(nodes, STACK, LOADGEN)
    checks = pf.preflight(run, "eks", (ab.Variant("v", A, "ghcr.io/o/nats-server:" + "a" * 12),), "deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks",
                          lambda *a: pf.Manifest(200, ("linux/amd64",)))
    assert by_name(checks)["fit together"].ok and "not checked" in by_name(checks)["fit together"].detail


def test_without_generator_settings_the_generator_is_checked_once_as_before():
    nodes, pods = cluster()
    names = [c.name for c in run_preflight((ab.Variant("a", A, "ghcr.io/o/nats-server:" + "a" * 12), ab.Variant("b", B, "ghcr.io/o/nats-server:" + "b" * 12)), nodes, pods)]
    assert names.count("job/loadgen") == 1 and "a: job/loadgen" not in names


def test_with_them_each_variant_gets_its_own_generator_line():
    nodes, pods = cluster()
    names = [c.name for c in run_preflight((ab.Variant("a", A, "ghcr.io/o/nats-server:" + "a" * 12, generator_resources={"cpu": "1"}),
                                           ab.Variant("b", B, "ghcr.io/o/nats-server:" + "b" * 12)), nodes, pods)]
    assert "a: job/loadgen" in names and "b: job/loadgen" in names and "job/loadgen" not in names
