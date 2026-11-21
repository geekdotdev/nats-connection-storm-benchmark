# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
from pathlib import Path

import pytest

from stormbench import cli, eks
from stormbench.runner import Runner


def params(**kw):
    return eks.Params(region="us-east-1", **kw)


def test_defaults_make_three_node_groups_in_one_zone():
    cfg = eks.cluster_config(params())
    assert cfg["apiVersion"] == "eksctl.io/v1alpha5" and cfg["kind"] == "ClusterConfig"
    assert cfg["metadata"] == {"name": "storm-bench", "region": "us-east-1", "tags": {"project": "storm-bench"}, "version": "1.37"}
    assert cfg["availabilityZones"] == ["us-east-1a", "us-east-1b"]
    brokers, clients, infra = cfg["managedNodeGroups"]
    assert (brokers["name"], brokers["desiredCapacity"], brokers["availabilityZones"]) == ("brokers", 3, ["us-east-1a"])
    assert (clients["name"], clients["desiredCapacity"], clients["availabilityZones"]) == ("clients", 1, ["us-east-1a"])
    assert (infra["name"], infra["desiredCapacity"], infra["availabilityZones"]) == ("infra", 1, ["us-east-1a"])   # same zone: its volume is zonal


def test_no_nat_gateway_is_created():
    assert eks.cluster_config(params())["vpc"] == {"nat": {"gateway": "Disable"}}


def test_only_the_brokers_and_the_infra_nodes_are_tainted_so_coredns_has_somewhere_to_run():
    brokers, clients, infra = eks.cluster_config(params())["managedNodeGroups"]
    assert brokers["taints"] == [{"key": "dedicated", "value": "broker", "effect": "NoSchedule"}]
    assert infra["taints"] == [{"key": "dedicated", "value": "infra", "effect": "NoSchedule"}]
    assert "taints" not in clients
    assert brokers["labels"] == {"role": "broker"} and clients["labels"] == {"role": "client"} and infra["labels"] == {"role": "infra"}


def test_node_groups_have_a_fixed_size_and_the_brokers_and_clients_the_same_instance_type():
    groups = eks.cluster_config(params(instance_type="m6i.2xlarge", broker_nodes=2, client_nodes=2, infra_nodes=2))["managedNodeGroups"]
    for g in groups:
        assert g["minSize"] == g["maxSize"] == g["desiredCapacity"] == 2
    assert [g["instanceType"] for g in groups] == ["m6i.2xlarge", "m6i.2xlarge", "c6i.large"]       # the infra nodes are sized for Prometheus, not the benchmark


def test_a_second_control_plane_zone_is_always_a_different_one():
    assert params(zone="us-east-1b").zones == ["us-east-1b", "us-east-1a"]
    assert params(zone="us-east-1c").zones == ["us-east-1c", "us-east-1b"]


def test_the_kubernetes_version_is_pinned_unless_given():
    assert eks.DEFAULT_K8S_VERSION == "1.37"
    assert eks.cluster_config(params())["metadata"]["version"] == eks.DEFAULT_K8S_VERSION
    assert eks.cluster_config(params(k8s_version="1.33"))["metadata"]["version"] == "1.33"
    assert 'version: "1.37"' in eks.render(params())


@pytest.mark.parametrize("kw", [
    {"region": "useast1"}, {"region": "US-EAST-1"}, {"cluster": "bad name"}, {"cluster": "x" * 41},
    {"zone": "eu-west-1a"}, {"instance_type": "c6i"}, {"instance_type": "c6i.xlarge; rm"},
    {"broker_nodes": 0}, {"broker_nodes": eks.MAX_NODES + 1}, {"client_nodes": -1}, {"k8s_version": "latest"},
])
def test_bad_parameters_are_refused(kw):
    base = {"region": "us-east-1"}
    with pytest.raises(ValueError):
        eks.Params(**{**base, **kw})


def test_rendered_yaml_quotes_strings_and_lists_groups():
    p = params(k8s_version="1.33", zone="us-east-1c")
    text = eks.render(p)
    assert '  version: "1.33"' in text and "desiredCapacity: 3" in text
    assert '- key: "dedicated"' in text and 'effect: "NoSchedule"' in text
    assert text.count("- name:") == 5 and text.endswith("\n")          # three node groups and two add-ons
    assert json.loads(eks._scalar("a\"b")) == 'a"b'


def test_yaml_lists_of_mappings_indent_under_their_key():
    out = eks.to_yaml({"a": [{"x": 1, "y": {"z": "q"}}, {"x": 2}], "b": []})
    assert out == 'a:\n  - x: 1\n    y:\n      z: "q"\n  - x: 2\nb: []'


def test_region_comes_from_aws_region_unless_given(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-west-2")
    assert eks.params_from_env().region == "eu-west-2"
    assert eks.params_from_env(region="us-west-2", broker_nodes=2).region == "us-west-2"
    monkeypatch.delenv("AWS_REGION")
    with pytest.raises(Exception, match="AWS_REGION"):
        eks.params_from_env()
    assert eks.params_from_env(region="us-west-2").broker_nodes == 3


def test_environment_overrides(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("STORM_EKS_INSTANCE_TYPE", "c7i.xlarge")
    monkeypatch.setenv("STORM_EKS_CLUSTER", "other")
    p = eks.params_from_env(instance_type="m6i.large")
    assert (p.cluster, p.instance_type) == ("other", "m6i.large")  # the command line beats the environment


def test_create_writes_only_its_own_kubeconfig():
    cmd = eks.create_command(Path("/t/cluster.yaml"), Path("/k/eks"))
    assert cmd == ["eksctl", "create", "cluster", "--config-file", "/t/cluster.yaml", "--kubeconfig", "/k/eks"]


def test_delete_command_names_the_cluster_and_waits():
    assert eks.delete_command(params(cluster="c1")) == ["eksctl", "delete", "cluster", "--name", "c1", "--region", "us-east-1", "--wait"]


def test_up_refuses_without_confirmation_and_runs_nothing(capsys):
    with pytest.raises(eks.NotConfirmed, match="billable"):
        eks.up(Runner(), params(), confirm=False)
    assert capsys.readouterr().err == ""  # nothing was echoed because nothing ran


def test_dry_run_up_needs_no_confirmation_and_runs_nothing(capsys, tmp_path, monkeypatch):
    monkeypatch.setattr(eks, "DEFAULT_KUBECONFIG", tmp_path / ".kube" / "eks")
    eks.up(Runner(dry_run=True), params(), confirm=False)
    err = capsys.readouterr().err
    assert err.startswith("+ eksctl create cluster --config-file ") and "--kubeconfig" in err
    assert not (tmp_path / ".kube").exists()


def test_the_default_kubeconfig_cannot_be_used(monkeypatch):
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(Path.home() / ".kube" / "config"))
    with pytest.raises(ValueError, match="~/.kube/config"):
        eks.kubeconfig_path()


def test_down_never_deletes_a_kubeconfig_it_did_not_make(monkeypatch, tmp_path):
    mine = tmp_path / "theirs"
    mine.write_text("x")
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(mine))
    eks.down(type("R", (), {"dry_run": False, "__call__": lambda self, c, **k: None})(), params())
    assert mine.is_file()


def test_cli_prints_the_config_without_touching_aws(monkeypatch, capsys):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    assert cli.main(["cluster", "config", "--target", "eks", "--broker-nodes", "2"]) == 0
    out = capsys.readouterr().out
    assert 'kind: "ClusterConfig"' in out and "desiredCapacity: 2" in out


def test_cli_up_without_yes_exits_2_and_says_why(monkeypatch, capsys):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    assert cli.main(["cluster", "up", "--target", "eks"]) == 2
    assert "--yes" in capsys.readouterr().err


def test_cli_config_is_only_for_eks(capsys):
    assert cli.main(["cluster", "config"]) == 2


# -- the add-ons: the EBS CSI driver and the IAM role it needs -----------------------------------------------------------------------------
def test_the_config_installs_the_ebs_csi_driver_with_its_role_through_pod_identity():
    cfg = eks.cluster_config(params())
    assert cfg["addons"] == [{"name": "eks-pod-identity-agent"}, {"name": "aws-ebs-csi-driver", "useDefaultPodIdentityAssociations": True}]
    text = eks.render(params())
    assert '- name: "aws-ebs-csi-driver"' in text and "useDefaultPodIdentityAssociations: true" in text
    assert "withOIDC" not in text                                            # no IAM OIDC provider: an account-wide resource this does not need
    cfg["addons"][0]["name"] = "x"
    assert eks.cluster_config(params())["addons"][0]["name"] == "eks-pod-identity-agent"   # the constant is copied, not shared


def test_the_gp3_storage_class_uses_the_csi_provisioner_and_waits_for_a_pod():
    text = eks.STORAGECLASS.read_text()
    assert "provisioner: ebs.csi.aws.com" in text and "type: gp3" in text and "volumeBindingMode: WaitForFirstConsumer" in text
    assert "reclaimPolicy: Delete" in text and "kubernetes.io/aws-ebs" not in text.replace("in-tree provisioner", "")


def test_the_addons_command_uses_the_same_config_file_the_cluster_was_made_from():
    assert eks.addons_command(Path("/t/c.yaml")) == ["eksctl", "create", "addon", "--config-file", "/t/c.yaml"]


def test_adding_addons_refuses_without_confirmation_and_runs_nothing(capsys):
    with pytest.raises(eks.NotConfirmed, match="IAM role"):
        eks.add_addons(Runner(), params(), confirm=False)
    assert capsys.readouterr().err == ""


def test_a_dry_run_of_addons_shows_the_command_and_changes_nothing(capsys):
    eks.add_addons(Runner(dry_run=True), params(), confirm=False)
    err = capsys.readouterr().err
    assert err.startswith("+ eksctl create addon --config-file ") and err.count("\n") == 1     # the StorageClass is applied only for real


def test_adding_addons_for_real_installs_them_then_applies_the_storage_class(monkeypatch, tmp_path):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(kc))
    ran = []
    runner = type("R", (), {"dry_run": False, "__call__": lambda self, c, **k: ran.append(c)})()
    eks.add_addons(runner, params(), confirm=True)
    assert ran[0][:4] == ["eksctl", "create", "addon", "--config-file"]
    assert ran[1] == ["kubectl", "--kubeconfig", str(kc), "apply", "-f", str(eks.STORAGECLASS)]


def test_creating_a_cluster_applies_the_storage_class_too(monkeypatch, tmp_path):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(kc))
    ran = []
    runner = type("R", (), {"dry_run": False, "__call__": lambda self, c, **k: ran.append(c)})()
    eks.up(runner, params(), confirm=True)
    assert ran[0][:3] == ["eksctl", "create", "cluster"] and ran[1] == ["kubectl", "--kubeconfig", str(kc), "apply", "-f", str(eks.STORAGECLASS)]


def test_the_command_line_refuses_addons_for_kind_and_needs_yes_for_eks(monkeypatch, capsys):
    assert cli.main(["cluster", "addons", "--target", "local"]) == 2
    assert "only for --target eks" in capsys.readouterr().err
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    assert cli.main(["cluster", "addons", "--target", "eks"]) == 2                  # NotConfirmed
    assert "--yes" in capsys.readouterr().err


# -- the infra node group (Prometheus and its volume) ---------------------------------------------------------------------------------------
def test_no_infra_nodes_means_no_infra_group_and_the_old_two_group_cluster():
    groups = eks.cluster_config(params(infra_nodes=0))["managedNodeGroups"]
    assert [g["name"] for g in groups] == ["brokers", "clients"]
    assert "infra" not in eks.plan_text(params(infra_nodes=0))


def test_the_plan_text_names_the_infra_node_so_its_cost_is_not_a_surprise():
    assert "1 infra node(s) of c6i.large" in eks.plan_text(params())


@pytest.mark.parametrize("kw", [{"infra_nodes": -1}, {"infra_nodes": eks.MAX_NODES + 1}, {"infra_instance_type": "c6i"}, {"infra_instance_type": "c6i.large; rm"}])
def test_bad_infra_parameters_are_refused(kw):
    with pytest.raises(ValueError):
        eks.Params(**{"region": "us-east-1", **kw})


def test_the_infra_command_creates_only_that_group_from_the_same_config():
    assert eks.infra_command(Path("/t/c.yaml")) == ["eksctl", "create", "nodegroup", "--config-file", "/t/c.yaml", "--include", "infra"]


def test_adding_the_infra_group_needs_a_yes_and_a_group_to_add(capsys):
    with pytest.raises(eks.NotConfirmed, match="billed by the hour"):
        eks.add_infra(Runner(), params(), confirm=False)
    with pytest.raises(ValueError, match="no infra node group"):
        eks.add_infra(Runner(), params(infra_nodes=0), confirm=True)
    eks.add_infra(Runner(dry_run=True), params(), confirm=False)
    assert capsys.readouterr().err.startswith("+ eksctl create nodegroup --config-file ")


def test_deleting_the_cluster_first_deletes_prometheus_volume_so_ebs_is_not_left_behind(monkeypatch, tmp_path):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(kc))
    ran = []
    runner = type("R", (), {"dry_run": False, "__call__": lambda self, c, **k: ran.append(c)})()
    eks.down(runner, params())
    assert ran[0][:5] == ["kubectl", "--kubeconfig", str(kc), "delete", "-k"] and "deploy/monitoring/overlays/eks" in ran[0]
    assert ran[1][-5:-3] == ["pvc", "data-prometheus-0"]                                  # the claim, and so the EBS volume, is deleted...
    assert ran[-1][:3] == ["eksctl", "delete", "cluster"]                                  # ...before the cluster is


def test_a_cluster_that_cannot_be_reached_is_still_deleted(monkeypatch, tmp_path, capsys):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(kc))
    ran = []

    def runner(cmd, **k):
        if cmd[0] == "kubectl":
            raise subprocess.CalledProcessError(1, cmd)
        ran.append(cmd)
    r = type("R", (), {"dry_run": False, "__call__": lambda self, c, **k: runner(c)})()
    eks.down(r, params())
    assert ran and ran[-1][:3] == ["eksctl", "delete", "cluster"]
    assert "could not remove monitoring's volume first" in capsys.readouterr().err


def test_the_command_line_takes_the_infra_options_and_refuses_infra_for_kind(monkeypatch, capsys):
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    assert cli.main(["cluster", "infra", "--target", "local"]) == 2
    assert cli.main(["cluster", "config", "--target", "eks", "--infra-nodes", "0"]) == 0
    assert "name: \"infra\"" not in capsys.readouterr().out.split("only for")[-1]
    assert cli.main(["cluster", "infra", "--target", "eks"]) == 2                            # needs --yes
