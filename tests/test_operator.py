# SPDX-License-Identifier: Apache-2.0
"""The driver's side of operator mode, the only authentication mode (docs/operator-mode-plan.md): the provision image, the objects
`secrets delete` removes, what a variant may not override, the manifests as the driver applies them, and the diagnosis when a pod waits
for what the init container makes."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from stormbench import ab, images, kube, preflight as pf, secretgen, secrets_cmd, variants
from stormbench.config import REPO_ROOT
from stormbench.runner import Runner

A, B = "a" * 40, "b" * 40
PROVISION_GO = (REPO_ROOT / "components/provision/provision.go").read_text()


def go_const(name: str) -> str:
    return re.search(rf'\b{name}\s*=\s*"([^"]+)"', PROVISION_GO).group(1)


def render(path) -> str:
    kubectl = shutil.which("kubectl")
    if not kubectl:
        pytest.skip("kubectl is not installed")
    return subprocess.run([kubectl, "kustomize", str(path)], check=True, capture_output=True, text=True,
                          env={"KUBECONFIG": "/dev/null"}).stdout


# -- the provision image ----------------------------------------------------------------------------------------------
def test_the_provision_image_is_built_with_the_others_and_alone():
    cmds = images.build_commands(None, tool="podman", only=("provision",))
    assert len(cmds) == 1
    cmd = cmds[0]
    assert cmd[cmd.index("-f") + 1] == "images/provision/Dockerfile"
    tag = images.component_tag("provision")
    assert re.fullmatch(r"provision:src-[0-9a-f]{12}", tag) and tag in cmd
    assert "provision:dev" in cmd  # the local flow's tag, as for the callout and the generator
    assert cmd[-1] == "."  # the context is the repository root, as the Dockerfile expects

    everything = images.build_commands("c" * 40, tool="podman")
    assert [c[c.index("-f") + 1] for c in everything] == [f"images/{n}/Dockerfile" for n in ("nats-server", "callout", "loadgen", "provision")]


def test_a_multi_platform_build_makes_a_list_without_the_dev_tag():
    cmds = images.build_commands(None, tool="podman", only=("provision",), platforms=("linux/amd64", "linux/arm64"))
    assert "--manifest" in cmds[0] and "provision:dev" not in sum(cmds, [])


def test_the_tag_follows_the_source_and_the_dockerfile(tmp_path):
    (tmp_path / "components/provision").mkdir(parents=True)
    (tmp_path / "images/provision").mkdir(parents=True)
    (tmp_path / "components/provision/main.go").write_text("a")
    (tmp_path / "images/provision/Dockerfile").write_text("FROM x")
    first = images.component_tag("provision", tmp_path)
    (tmp_path / "components/provision/main.go").write_text("b")
    assert images.component_tag("provision", tmp_path) != first
    (tmp_path / "components/provision/main.go").write_text("a")
    assert images.component_tag("provision", tmp_path) == first


def test_only_still_refuses_an_unknown_image():
    with pytest.raises(ValueError, match="provision"):
        images.build_commands(None, only=("nope",))


def test_the_provision_dockerfile_is_pinned_like_the_others():
    text = (REPO_ROOT / "images/provision/Dockerfile").read_text()
    assert re.search(r"^ARG GO_IMAGE=golang:[\w.\-]+(-alpine)?@sha256:[0-9a-f]{64}$", text, re.M)
    assert re.search(r"^ARG ALPINE_IMAGE=alpine:[\w.]+@sha256:[0-9a-f]{64}$", text, re.M)


# -- the objects the init container makes ------------------------------------------------------------------------------
def test_the_object_names_match_what_provision_creates():
    assert secretgen.PROVISION_SECRETS == (go_const("CalloutSecretName"), go_const("GeneratorSecretName"))
    assert secretgen.PROVISION_CONFIGMAP == go_const("ConfigMapName")
    assert set(kube.PROVISION_OBJECTS) == {*secretgen.PROVISION_SECRETS, secretgen.PROVISION_CONFIGMAP}


class Recording(Runner):
    def __init__(self):
        super().__init__()
        self.cmds = []

    def __call__(self, cmd, *, input=None, env=None):
        self.cmds.append(cmd)


@pytest.fixture
def kubeconfig(monkeypatch, tmp_path):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(kc))
    return str(kc)


def test_secrets_delete_removes_what_the_init_container_made_too(kubeconfig):
    run = Recording()
    secrets_cmd.delete(run)
    secret, configmap = run.cmds
    assert secret[secret.index("delete"):] == ["delete", "secret", "storm-tls", "storm-callout-auth", "storm-generator-auth", "--ignore-not-found"]
    assert configmap[configmap.index("delete"):] == ["delete", "configmap", "storm-operator", "--ignore-not-found"]
    assert all(c[:3] == ["kubectl", "--kubeconfig", kubeconfig] and "storm-bench" in c for c in run.cmds)


# -- what a variant may not override -----------------------------------------------------------------------------------
def doc(**second):
    return {"variant": [{"label": "a", "commit": A}, {"label": "b", "commit": B, **second}]}


@pytest.mark.parametrize("name", ["CALLOUT_CREDS", "CALLOUT_AUTH_SIGNING_SEED", "CALLOUT_TENANT_SIGNING_SEED", "CALLOUT_TENANT_PUBLIC"])
def test_the_operator_mode_callout_keys_cannot_be_overridden(name):
    with pytest.raises(ValueError, match="set by the manifests"):
        variants.parse(doc(callout_env={name: "x"}), "ghcr.io/o/")


@pytest.mark.parametrize("name", ["STORM_SCOPED_SEED", "STORM_SCOPED_SEED_FILE", "STORM_AUTH_ACCOUNT", "STORM_AUTH_ACCOUNT_FILE"])
def test_the_generators_key_wiring_cannot_be_overridden(name):
    with pytest.raises(ValueError, match="cannot be overridden"):
        variants.parse(doc(storm={name: "x"}), "ghcr.io/o/")


def test_a_variant_may_vary_the_roles_the_tickets_ask_for():
    (_, b) = variants.parse(doc(storm={"STORM_ROLES": "app-feed-publish-a", "STORM_TICKET_TTL_S": "600", "STORM_TIMEOUT_MS": "20000"}), "ghcr.io/o/")
    assert b.storm["STORM_ROLES"] == "app-feed-publish-a"


def test_the_names_the_manifests_set_are_all_protected():
    """Every environment variable the base manifests fill from a Secret or the pod is protected from a variant."""
    callout = (REPO_ROOT / "deploy/stack/base/callout.yaml").read_text()
    assert set(re.findall(r"- name: (CALLOUT_[A-Z_]+)\n", callout)) == variants.PROTECTED_CALLOUT_ENV
    broker = (REPO_ROOT / "deploy/stack/base/broker.yaml").read_text()
    assert set(re.findall(r"- name: ([A-Z_]+)\n\s+valueFrom", broker)) == variants.PROTECTED_BROKER_ENV
    job = (REPO_ROOT / "deploy/loadgen/base/job.yaml").read_text()
    for name in re.findall(r"name: (STORM_[A-Z_]+), value: /", job):  # the ones that point at files
        assert name in variants.RESERVED_STORM, name


def test_no_config_mode_names_are_left_in_the_driver_or_the_manifests():
    leftovers = ("storm-secrets", "CALLOUT_ISSUER", "CALLOUT_USER_PASSWORD", "CALLOUT_SERVICE", "LEAF_PASSWORD", "STORM_PASSWORD",
                 "STORM_ACCOUNTS", "STORM_AUTH=", "leafnodes", "auth_callout")
    for root in ("driver/stormbench", "deploy"):
        for f in (REPO_ROOT / root).rglob("*"):
            if f.is_file() and f.suffix in (".py", ".yaml", ".md"):
                text = f.read_text()
                for word in leftovers:
                    assert word not in text, f"{f.relative_to(REPO_ROOT)} still mentions {word}"


# -- the manifests, as the driver applies them --------------------------------------------------------------------------
def test_a_variants_settings_reach_the_stack(tmp_path):
    v = ab.Variant("tuned", A, "localhost/nats-server:x", callout_env={"CALLOUT_LOG": "debug"}, broker_env={"GOGC": "50"})
    text = render(variants.write(v, "deploy/stack/overlays/local", tmp_path, "STAMP"))
    assert "name: provision" in text and "storm-provision" in text            # the init container and its ServiceAccount
    assert "name: CALLOUT_LOG" in text and "value: debug" in text              # the variant's own callout setting
    assert "name: GOGC" in text
    assert text.count("storm-bench/restarted-at: STAMP") == 2                  # broker and callout pods restart together
    assert "name: operator" in text and "storm-operator" in text               # the broker mounts the ConfigMap


def test_callout_args_replace_the_whole_list(tmp_path):
    v = ab.Variant("workers", A, "localhost/nats-server:x", callout_args=("serve", "--workers", "8"))
    text = render(variants.write(v, "deploy/stack/overlays/local", tmp_path))
    assert "- --workers" in text and "- --url" not in text   # a variant that sets callout_args gives the whole list, the broker URL too


def test_the_generator_job_presents_tickets_from_the_secret_provision_makes():
    text = render(REPO_ROOT / "deploy/loadgen/overlays/local")
    assert f"secretName: {secretgen.PROVISION_SECRETS[1]}" in text
    for key in ("scoped.seed", "auth.pub"):
        assert f"key: {key}" in text and f'"{key}"' in PROVISION_GO  # provision writes the keys the Job reads
    assert "wss://broker:8443" in text and "STORM_CA_FILE" in text            # TLS comes from storm-tls
    assert "STORM_SCOPED_SEED_FILE" in text and "STORM_AUTH_ACCOUNT_FILE" in text


def test_every_overlay_renders():
    for name in ("local", "local-cluster", "eks", "eks-cluster"):
        assert "kind: StatefulSet" in render(REPO_ROOT / "deploy/stack/overlays" / name)
    for name in ("local", "eks"):
        assert "kind: Job" in render(REPO_ROOT / "deploy/loadgen/overlays" / name)


def test_no_leaf_nodes_are_left():
    assert not (REPO_ROOT / "deploy/stack/components/leafnodes").exists()
    assert not list((REPO_ROOT / "deploy/stack/overlays").glob("*leaf*"))
    assert "leaf" not in render(REPO_ROOT / "deploy/stack/overlays/local-cluster").lower().replace("leaf-", "")


def test_the_eks_overlays_pin_every_image_by_digest_and_the_pins_name_their_source():
    """Every image the eks overlays name is pinned by digest, and each benchmark image's pin carries a comment naming the source tag it
    was built from (the digest alone does not say), which must be the tag of a component that exists."""
    text = render(REPO_ROOT / "deploy/stack/overlays/eks") + render(REPO_ROOT / "deploy/loadgen/overlays/eks")
    for name in ("callout", "provision", "loadgen", "nats-server"):
        assert re.search(rf"image: ghcr\.io/\S+/{name}@sha256:[0-9a-f]{{64}}", text), name
    for overlay in ("deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks"):
        source = (REPO_ROOT / overlay / "kustomization.yaml").read_text()
        for name, tag in re.findall(r"digest: sha256:[0-9a-f]{64}\s+# (\w+):(src-[0-9a-f]{12}|\S+)", source):
            if name in ("callout", "provision", "loadgen"):
                assert tag.startswith("src-"), f"{overlay}: {name} is pinned with a comment that does not name its source: {tag}"
                assert (REPO_ROOT / "components" / name).is_dir()


# -- the diagnosis when a pod waits for what the init container makes ---------------------------------------------------
def pod(name, ready=False, phase="Pending", init=None, main=None):
    status = {"phase": phase, "conditions": [{"type": "Ready", "status": "True" if ready else "False"}]}
    if init:
        status["initContainerStatuses"] = [{"name": "provision", "state": init}]
    if main:
        status["containerStatuses"] = [{"name": "broker", "state": main}]
    return {"metadata": {"name": name}, "status": status}


def event(pod_name, reason, message, when="2026-11-21T05:00:00Z", kind="Warning"):
    return {"involvedObject": {"name": pod_name}, "type": kind, "reason": reason, "message": message, "lastTimestamp": when}


def test_a_broker_waiting_for_the_configmap_is_named_with_the_hint():
    lines = kube.waiting_report(
        [pod("broker-0", main={"waiting": {"reason": "ContainerCreating"}}), pod("broker-1", ready=True, phase="Running")],
        [event("broker-0", "FailedMount", 'MountVolume.SetUp failed for volume "operator" : configmap "storm-operator" not found'),
         event("broker-1", "FailedMount", "unrelated")])
    text = "\n".join(lines)
    assert "pod broker-0 (Pending)" in text and "ContainerCreating" in text
    assert "FailedMount" in text and 'configmap "storm-operator" not found' in text
    assert "broker-1" not in text and "unrelated" not in text  # a ready pod, and events about it, are left out
    assert lines[-1].startswith("  hint:") and "-c provision" in lines[-1]


def test_a_failing_init_container_is_reported_with_its_exit_and_message():
    lines = kube.waiting_report(
        [pod("callout-x", phase="Pending", init={"terminated": {"exitCode": 1, "reason": "Error", "message": "creating Secret storm-callout-auth: HTTP 403 forbidden"}})], [])
    assert "init container provision exited 1: Error" in lines[0] and "HTTP 403" in lines[0]
    assert lines[-1].startswith("  hint:")


def test_a_crash_looping_init_container_is_reported():
    lines = kube.waiting_report([pod("callout-x", init={"waiting": {"reason": "CrashLoopBackOff", "message": "back-off"}})], [])
    assert "init container provision waiting: CrashLoopBackOff (back-off)" in lines[0]


def test_a_container_that_keeps_failing_shows_why_it_last_exited():
    """Seen against a live cluster: the init container waits in CrashLoopBackOff, and its last log lines are in lastState."""
    p = pod("callout-x", init={"waiting": {"reason": "CrashLoopBackOff"}})
    p["status"]["initContainerStatuses"][0]["lastState"] = {"terminated": {"exitCode": 1, "message": "provision: HTTP 403 configmaps forbidden\n"}}
    (line,) = kube.waiting_report([p], [])[:1]
    assert "CrashLoopBackOff" in line and "last exited 1: provision: HTTP 403 configmaps forbidden" in line


def test_only_the_latest_warning_of_each_reason_is_shown_and_normal_events_are_ignored():
    events = [event("broker-0", "FailedMount", "old", "2026-11-21T05:00:00Z"), event("broker-0", "FailedMount", "new", "2026-11-21T05:05:00Z"),
              event("broker-0", "Scheduled", "assigned", kind="Normal")]
    (line,) = kube.waiting_report([pod("broker-0")], events)
    assert "new" in line and "old" not in line and "assigned" not in line


def test_finished_and_terminating_pods_are_not_reported():
    """Seen against a live cluster: the previous storm Job's pod (Succeeded) and the pod being replaced (terminating) are not what is stuck."""
    done = pod("loadgen-x", phase="Succeeded")
    old = pod("broker-0")
    old["metadata"]["deletionTimestamp"] = "2026-11-21T14:00:00Z"
    stuck = pod("broker-0b", main={"waiting": {"reason": "ContainerCreating"}})
    lines = kube.waiting_report([done, old, stuck], [])
    assert len(lines) == 1 and "broker-0b" in lines[0]


def test_a_pod_with_no_reported_reason_and_no_hint_for_an_ordinary_failure():
    lines = kube.waiting_report([pod("broker-0")], [])
    assert lines == ["  pod broker-0 (Pending): not ready, no reason reported"]


class FailingRollout(Runner):
    """Answers the diagnosis reads from a script, and fails the rollout status."""
    def __init__(self, outputs):
        super().__init__()
        self.cmds, self.outputs = [], list(outputs)

    def __call__(self, cmd, *, input=None, env=None):
        self.cmds.append(cmd)
        if "rollout" in cmd:
            raise subprocess.CalledProcessError(1, cmd)

    def output(self, cmd, *, input=None, env=None, dry_run_value="", quiet=False):
        self.cmds.append(cmd)
        return self.outputs.pop(0)


def test_a_rollout_that_does_not_finish_says_which_pods_wait_and_why_then_raises(kubeconfig, capsys):
    pods = json.dumps({"items": [pod("broker-0", main={"waiting": {"reason": "ContainerCreating"}})]})
    events = json.dumps({"items": [event("broker-0", "FailedMount", 'configmap "storm-operator" not found')]})
    run = FailingRollout([pods, events])
    backend = kube.KubeBackend(run, sleep=lambda s: None)
    with pytest.raises(subprocess.CalledProcessError):
        backend._wait_rollout("statefulset/broker")
    err = capsys.readouterr().err
    assert "statefulset/broker did not become ready" in err and "pod broker-0" in err and "storm-operator" in err and "hint:" in err
    assert any("events" in c for c in run.cmds)  # the warning events are read too


def test_the_diagnosis_never_hides_the_real_failure(kubeconfig, capsys):
    run = FailingRollout(["not json", "{}"])
    with pytest.raises(subprocess.CalledProcessError):
        kube.KubeBackend(run, sleep=lambda s: None)._wait_rollout("deployment/callout")
    assert "could not read the pods" in capsys.readouterr().err


def test_a_rollout_that_succeeds_prints_nothing(kubeconfig, capsys):
    run = Recording()
    kube.KubeBackend(run, sleep=lambda s: None)._wait_rollout("statefulset/broker")
    assert capsys.readouterr().err == "" and "rollout" in run.cmds[0] and "statefulset/broker" in run.cmds[0]
