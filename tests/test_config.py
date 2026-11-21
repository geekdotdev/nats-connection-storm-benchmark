# SPDX-License-Identifier: Apache-2.0
import pytest

from stormbench import config


def test_require_lists_every_missing_name(monkeypatch):
    monkeypatch.delenv("REGISTRY_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    with pytest.raises(config.MissingSecret) as e:
        config.require("REGISTRY_TOKEN", "GITHUB_TOKEN")
    assert "REGISTRY_TOKEN" in str(e.value) and "GITHUB_TOKEN" in str(e.value)


def test_require_returns_values(monkeypatch):
    monkeypatch.setenv("REGISTRY_USER", "u")
    assert config.require("REGISTRY_USER") == {"REGISTRY_USER": "u"}


def test_dotenv_does_not_override_the_environment(monkeypatch, tmp_path):
    f = tmp_path / ".env"
    f.write_text("# c\nREGISTRY_USER=from-file\nGITHUB_TOKEN='quoted'\nEMPTY=\n")
    monkeypatch.setenv("REGISTRY_USER", "from-env")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    config.load_dotenv(f)
    import os
    assert os.environ["REGISTRY_USER"] == "from-env"
    assert os.environ["GITHUB_TOKEN"] == "quoted"


def test_describe_never_contains_a_value(monkeypatch):
    monkeypatch.setenv("REGISTRY_TOKEN", "super-secret-value")
    assert "super-secret-value" not in repr(config.describe())
