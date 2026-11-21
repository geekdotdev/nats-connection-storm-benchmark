# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path

import pytest

from stormbench import cli, eks
from stormbench.runner import Runner


def params(**kw):
    return eks.Params(region="us-east-1", **kw)


def test_defaults_make_two_node_groups_in_one_zone():
    cfg = eks.cluster_config(params())
    assert cfg["apiVersion"] == "eksctl.io/v1alpha5" and cfg["kind"] == "ClusterConfig"
    assert cfg["metadata"] == {"name": "storm-bench", "region": "us-east-1", "tags": {"project": "storm-bench"}, "version": "1.37"}
    assert cfg["availabilityZones"] == ["us-east-1a", "us-east-1b"]
    brokers, clients = cfg["managedNodeGroups"]
    assert (brokers["name"], brokers["desiredCapacity"], brokers["availabilityZones"]) == ("brokers", 3, ["us-east-1a"])
    assert (clients["name"], clients["desiredCapacity"], clients["availabilityZones"]) == ("clients", 1, ["us-east-1a"])


def test_no_nat_gateway_is_created():
    assert eks.cluster_config(params())["vpc"] == {"nat": {"gateway": "Disable"}}


def test_only_the_brokers_are_tainted_so_coredns_has_somewhere_to_run():
    brokers, clients = eks.cluster_config(params())["managedNodeGroups"]
    assert brokers["taints"] == [{"key": "dedicated", "value": "broker", "effect": "NoSchedule"}]
    assert "taints" not in clients
    assert brokers["labels"] == {"role": "broker"} and clients["labels"] == {"role": "client"}


def test_node_groups_have_a_fixed_size_and_the_same_instance_type():
    for g in eks.cluster_config(params(instance_type="m6i.2xlarge", broker_nodes=2, client_nodes=2))["managedNodeGroups"]:
        assert g["instanceType"] == "m6i.2xlarge"
        assert g["minSize"] == g["maxSize"] == g["desiredCapacity"] == 2


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
    assert text.count("- name:") == 2 and text.endswith("\n")
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
