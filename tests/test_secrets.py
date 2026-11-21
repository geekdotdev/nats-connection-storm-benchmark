# SPDX-License-Identifier: Apache-2.0
import datetime as dt
import json

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.x509.oid import ExtendedKeyUsageOID

from stormbench import cli, k8s, secretgen, secrets_cmd
from stormbench.runner import Runner


def test_passwords_are_long_and_different():
    a, b = secretgen.password(), secretgen.password()
    assert a != b and len(a) == 32 and int(a, 16) >= 0


def test_the_broker_certificate_is_signed_by_the_throwaway_ca_and_names_the_broker():
    tls = secretgen.make_tls()
    ca = x509.load_pem_x509_certificate(tls["ca.pem"].encode())
    cert = x509.load_pem_x509_certificate(tls["tls.crt"].encode())
    cert.verify_directly_issued_by(ca)
    assert ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca is True
    assert cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca is False
    sans = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.DNSName)
    assert set(secretgen.BROKER_NAMES) <= set(sans)
    assert ExtendedKeyUsageOID.SERVER_AUTH in cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    now = dt.datetime.now(dt.timezone.utc)
    assert cert.not_valid_before_utc <= now <= cert.not_valid_after_utc
    assert cert.not_valid_after_utc - now < dt.timedelta(days=8)


def test_the_private_key_matches_the_certificate_and_the_ca_key_is_not_kept():
    tls = secretgen.make_tls()
    assert set(tls) == {"ca.pem", "tls.crt", "tls.key"}
    key = serialization.load_pem_private_key(tls["tls.key"].encode(), password=None)
    cert = x509.load_pem_x509_certificate(tls["tls.crt"].encode())
    assert key.public_key().public_numbers() == cert.public_key().public_numbers()
    assert "PRIVATE KEY" not in tls["ca.pem"]


def test_manifests_have_the_keys_the_stack_expects():
    docs = secretgen.manifests("SEED", "PUBLIC")
    assert [d["metadata"]["name"] for d in docs] == ["storm-secrets", "storm-tls"]
    assert all(d["kind"] == "Secret" and d["metadata"]["namespace"] == "storm-bench" for d in docs)
    assert set(docs[0]["stringData"]) == {"service-password", "user-password", "leaf-password", "issuer-seed", "issuer-public"}
    assert docs[0]["stringData"]["issuer-seed"] == "SEED" and docs[0]["stringData"]["issuer-public"] == "PUBLIC"
    sd = docs[0]["stringData"]
    assert len({sd["service-password"], sd["user-password"], sd["leaf-password"]}) == 3
    assert set(docs[1]["stringData"]) == {"ca.pem", "tls.crt", "tls.key"}


def test_kubectl_refuses_to_run_without_the_benchmarks_own_kubeconfig(monkeypatch, tmp_path):
    monkeypatch.setenv("STORM_KUBECONFIG", str(tmp_path / "missing"))
    with pytest.raises(k8s.NoCluster):
        k8s.kubectl_command("get", "pods")


def test_kubectl_always_passes_the_kubeconfig(monkeypatch, tmp_path):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(kc))
    assert k8s.kubectl_command("get", "pods") == ["kubectl", "--kubeconfig", str(kc), "get", "pods"]


def test_dry_run_create_echoes_commands_and_never_a_secret(monkeypatch, tmp_path, capsys):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(kc))
    monkeypatch.setenv("CONTAINER_TOOL", "podman")
    assert cli.main(["--dry-run", "secrets", "create", "--callout-image", "callout:dev"]) == 0
    err = capsys.readouterr()
    text = err.err + err.out
    assert "podman run --rm callout:dev keygen --json   (stdout captured)" in text
    assert f"kubectl --kubeconfig {kc} apply -f -   (stdin withheld)" in text
    assert f"kubectl --kubeconfig {kc} create -f -   (stdin withheld)" in text
    assert "PRIVATE KEY" not in text and "service-password" not in text
    assert "values not shown" in text


def test_create_without_a_cluster_is_an_error_not_a_fallback(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STORM_KUBECONFIG", str(tmp_path / "missing"))
    assert cli.main(["--dry-run", "secrets", "create"]) == 1
    assert "bench cluster up" in capsys.readouterr().err


def test_the_manifest_is_sent_on_stdin(monkeypatch, tmp_path):
    kc = tmp_path / "kc"
    kc.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(kc))
    calls = []

    class Spy(Runner):
        def __call__(self, cmd, *, input=None, env=None):
            calls.append((cmd, input))

        def output(self, cmd, *, env=None, dry_run_value=""):
            return json.dumps({"seed": "S", "public": "P"})

    names = secrets_cmd.create(Spy(), "callout:dev")
    assert names == ["storm-secrets", "storm-tls"]
    ns_cmd, ns_in = calls[0]
    create_cmd, create_in = calls[1]
    assert ns_cmd[-3:] == ["apply", "-f", "-"] and json.loads(ns_in)["kind"] == "Namespace"
    assert create_cmd[-3:] == ["create", "-f", "-"]
    items = json.loads(create_in)["items"]
    assert items[0]["stringData"]["issuer-seed"] == "S"
    assert not any("S" == a or "stringData" in a for a in create_cmd)  # nothing secret on the command line


def test_each_target_has_its_own_kubeconfig_and_neither_reads_the_others(monkeypatch, tmp_path):
    local, remote = tmp_path / "local", tmp_path / "eks"
    local.write_text("x")
    remote.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(local))
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(remote))
    assert k8s.kubectl_command("get", "pods")[2] == str(local)
    assert k8s.kubectl_command("get", "pods", target="local")[2] == str(local)
    assert k8s.kubectl_command("get", "pods", target="eks")[2] == str(remote)
    monkeypatch.delenv("STORM_EKS_KUBECONFIG")
    monkeypatch.setattr("stormbench.eks.DEFAULT_KUBECONFIG", tmp_path / "default-eks")
    with pytest.raises(k8s.NoCluster) as e:  # STORM_KUBECONFIG (local) is never used for eks
        k8s.kubectl_command("get", "pods", target="eks")
    assert "--target eks" in str(e.value)
    with pytest.raises(ValueError):
        k8s.kubectl_command("get", "pods", target="gke")


def test_target_can_come_before_or_after_the_subcommand():
    parse = cli.build_parser().parse_args
    assert parse(["secrets", "create"]).target == "local"
    assert parse(["--target", "eks", "secrets", "create"]).target == "eks"
    assert parse(["secrets", "create", "--target", "eks"]).target == "eks"
    assert parse(["--target", "eks", "run", "--a", "x", "--b", "y"]).target == "eks"
    assert parse(["--target", "eks", "cluster", "up", "--target", "local"]).target == "local"  # the later one wins
    with pytest.raises(SystemExit):
        parse(["--target", "gke", "secrets", "create"])


def test_secrets_for_eks_use_the_eks_kubeconfig(monkeypatch, tmp_path, capsys):
    local, remote = tmp_path / "local", tmp_path / "eks"
    local.write_text("x")
    remote.write_text("x")
    monkeypatch.setenv("STORM_KUBECONFIG", str(local))
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(remote))
    monkeypatch.setenv("CONTAINER_TOOL", "podman")
    assert cli.main(["--dry-run", "--target", "eks", "secrets", "create", "--callout-image", "callout:dev"]) == 0
    text = capsys.readouterr().err
    assert f"kubectl --kubeconfig {remote} apply -f -" in text and str(local) not in text
    assert cli.main(["--dry-run", "secrets", "delete", "--target", "eks"]) == 0
    assert f"kubectl --kubeconfig {remote} -n storm-bench delete secret" in capsys.readouterr().err


def test_default_callout_image_prefers_the_build_of_the_current_source():
    class Has:
        def __init__(self, *tags):
            self.tags = set(tags)

        def succeeds(self, cmd):
            return cmd[-1] in self.tags

    from stormbench.images import component_tag
    tagged = component_tag("callout")
    assert secrets_cmd.default_callout_image(Has(tagged, "callout:dev"), "podman") == tagged
    assert secrets_cmd.default_callout_image(Has("callout:dev"), "podman") == "callout:dev"
    assert secrets_cmd.default_callout_image(Has(), "podman") == "callout:dev"
