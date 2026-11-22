# SPDX-License-Identifier: Apache-2.0
"""No test may reach a real cluster.

The driver finds its cluster through a kubeconfig file in the repository (.kube/storm-bench for kind, .kube/storm-bench-eks for EKS).
On a machine with a cluster up, a test that forgot to fake the backend would run real kubectl commands against it: once, while a benchmark
was running on EKS, one did, and its `kubectl apply` replaced the broker under a storm. So every test starts with both kubeconfig variables
pointing at a file that does not exist, which makes any kubectl call fail with NoCluster before it runs. A test that needs a kubeconfig
sets its own (they all do, with monkeypatch.setenv on a file in tmp_path).
"""
import pytest


@pytest.fixture(autouse=True)
def no_real_cluster(monkeypatch, tmp_path_factory):
    nowhere = tmp_path_factory.mktemp("no-cluster") / "kubeconfig-that-does-not-exist"
    monkeypatch.setenv("STORM_KUBECONFIG", str(nowhere))
    monkeypatch.setenv("STORM_EKS_KUBECONFIG", str(nowhere))


@pytest.fixture(autouse=True)
def a_github_login(monkeypatch):
    """A run needs the GitHub login of whoever runs it (qualify.github_user); tests that check it is refused remove this."""
    monkeypatch.setenv("GITHUB_USER", "test-user")


@pytest.fixture(autouse=True)
def never_read_the_developers_dotenv(monkeypatch):
    """The CLI reads the repository's .env (config.load_dotenv() with no path), which holds a developer's real settings and keys: a test must not
    depend on them (one that removed GITHUB_USER failed the day a developer set it in .env). An explicit path, which the config tests use, still works."""
    from stormbench import config
    real = config.load_dotenv
    monkeypatch.setattr(config, "load_dotenv", lambda path=None: real(path) if path is not None else None)
