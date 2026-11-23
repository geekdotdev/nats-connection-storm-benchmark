# SPDX-License-Identifier: Apache-2.0
"""The broker's /varz: the proxy path, parsing a reading, and polling one on an interval."""
import json
import subprocess

import pytest

from stormbench import varz
from stormbench.runner import Runner

VARZ_TEXT = json.dumps({"mem": 123456, "connections": 42, "total_connections": 900, "cpu": 3.5, "max_connections": 0,
                        "server_id": "N1", "uptime": "1h2m3s"})


def test_the_broker_is_reached_through_the_api_servers_pod_proxy():
    assert varz._proxy("broker-0", "varz") == "/api/v1/namespaces/storm-bench/pods/broker-0:8222/proxy/varz"


def test_parse_keeps_only_the_known_fields():
    assert varz.parse(VARZ_TEXT) == {"mem": 123456, "connections": 42, "total_connections": 900, "cpu": 3.5, "max_connections": 0}


def test_parse_leaves_out_a_field_varz_does_not_have_rather_than_defaulting_it():
    got = varz.parse(json.dumps({"mem": 1, "connections": 2}))
    assert got == {"mem": 1, "connections": 2}
    assert "cpu" not in got and "total_connections" not in got


@pytest.mark.parametrize("text", ["", "not json", "[1, 2, 3]", "null", "\"a string\""])
def test_parse_returns_none_for_anything_that_is_not_a_json_object(text):
    assert varz.parse(text) is None


class FakeRun(Runner):
    def __init__(self, text=VARZ_TEXT, fail=False):
        super().__init__()
        self.text, self.fail, self.cmds = text, fail, []

    def output(self, cmd, **k):
        self.cmds.append(cmd)
        if self.fail:
            raise subprocess.CalledProcessError(1, cmd)
        return self.text


def test_fetch_tags_a_reading_with_when_it_was_taken(monkeypatch):
    monkeypatch.setattr(varz.time, "time", lambda: 1700000000.0)
    monkeypatch.setattr(varz.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])
    run = FakeRun()
    got = varz.fetch(run, "eks", "broker-0")
    assert got == {"mem": 123456, "connections": 42, "total_connections": 900, "cpu": 3.5, "max_connections": 0, "atMs": 1700000000000}
    assert run.cmds[0][-1].endswith("broker-0:8222/proxy/varz")


def test_fetch_returns_none_when_kubectl_fails_or_the_reply_is_not_json(monkeypatch):
    monkeypatch.setattr(varz.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])
    assert varz.fetch(FakeRun(fail=True), "eks", "broker-0") is None
    assert varz.fetch(FakeRun(text="not json"), "eks", "broker-0") is None


class FakeStop:
    """Fires `is_set()` true after `stop_after` waits, like a threading.Event a caller sets once some time has passed -- but
    deterministic and instant, so the test does not actually sleep."""
    def __init__(self, stop_after):
        self.calls, self.stop_after = 0, stop_after

    def is_set(self):
        return self.calls >= self.stop_after

    def wait(self, timeout):
        self.calls += 1


def test_poll_reads_on_an_interval_until_stopped(monkeypatch):
    monkeypatch.setattr(varz.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])
    monkeypatch.setattr(varz.time, "time", lambda: 1.0)
    out = varz.poll(FakeRun(), "eks", "broker-0", 0.01, FakeStop(3), say=lambda m: None)
    assert len(out) == 3 and all(r["connections"] == 42 for r in out)


def test_poll_keeps_going_through_a_failed_reading_and_says_so_once(monkeypatch):
    monkeypatch.setattr(varz.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])
    said = []
    out = varz.poll(FakeRun(fail=True), "eks", "broker-0", 0.01, FakeStop(3), say=said.append)
    assert out == [] and len(said) == 1
