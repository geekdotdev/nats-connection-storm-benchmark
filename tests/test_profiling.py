# SPDX-License-Identifier: Apache-2.0
"""The broker's pprof profiles: harvested by the CPU probe while the run goes on, read out of the probes when it has completed."""
import json
import shutil
import subprocess

import pytest

from stormbench import cli, profiling, runcmd
from stormbench.runner import Runner

TRACES = """File: nats-server
Type: delay
-----------+-------------------------------------------------------
   2.14hrs   sync.(*Cond).Wait
             server.(*client).writeLoop
             server.(*Server).createWSClient.func2
-----------+-------------------------------------------------------
   140.21s   sync.(*Mutex).Lock (inline)
             sync.(*RWMutex).Lock
             server.(*Server).processClientOrLeafAuthentication
             server.(*client).processConnect
-----------+-------------------------------------------------------
   60.5ms   sync.(*Mutex).Lock (inline)
            server.(*Account).addClient
            server.(*client).registerWithAccount
-----------+-------------------------------------------------------
   111.65s   runtime.selectgo
             server.(*Server).processClientOrLeafCallout
             server.(*Server).processClientOrLeafAuthentication
-----------+-------------------------------------------------------
   2mins     sync.(*RWMutex).RLock
             server.(*Server).accConnsUpdate
"""


def test_traces_are_read_with_their_units_and_frames():
    got = profiling.parse_traces(TRACES)
    assert [round(s, 4) for s, _ in got] == [7704.0, 140.21, 0.0605, 111.65, 120.0]
    assert got[1][1] == ["sync.(*Mutex).Lock (inline)", "sync.(*RWMutex).Lock", "server.(*Server).processClientOrLeafAuthentication", "server.(*client).processConnect"]


def test_only_lock_waits_count_and_the_idle_cond_wait_is_left_out():
    w = profiling.waits(profiling.parse_traces(TRACES))
    assert w["lock_s"] == pytest.approx(140.21 + 0.0605 + 120.0)                  # the 2.14 h of sync.Cond waits is idle connections, not contention
    assert w["in"]["authentication"] == pytest.approx(140.21) and w["in"]["processConnect"] == pytest.approx(140.21)
    assert w["in"]["addClient"] == pytest.approx(0.0605) and w["in"]["accConnsUpdate"] == pytest.approx(120.0)
    assert w["callout_s"] == pytest.approx(111.65)                                 # the wait for the callout's reply, apart from the locks


def test_the_probe_is_reached_through_the_api_servers_pod_proxy():
    assert profiling._proxy("cpuprobe-x", "profiles") == "/api/v1/namespaces/storm-bench/pods/cpuprobe-x:9100/proxy/profiles"
    assert profiling.is_measured("main-b1-n800") and not profiling.is_measured("main-b1-warm")


SNAPS = [{"uid": "u1", "atMs": t, "file": f"u1/{t}.block.pb.gz", "pod": "broker-0"} for t in (1000, 3000, 5000, 7000)] + \
        [{"uid": "u2", "atMs": t, "file": f"u2/{t}.block.pb.gz", "pod": "broker-0"} for t in (9000, 11000)]


def test_a_storm_is_bracketed_by_the_last_snapshot_before_it_and_the_first_after():
    b, a = profiling.pick(SNAPS, 3.5, 6.0)
    assert (b["atMs"], a["atMs"]) == (3000, 7000)
    assert profiling.pick(SNAPS, 6.0, 10.0) is None                              # the broker was replaced in between: two processes cannot be subtracted
    b, a = profiling.pick(SNAPS, 9.5, 10.5)                                      # the second broker's own pair
    assert (b["uid"], b["atMs"], a["atMs"]) == ("u2", 9000, 11000)


def test_a_storm_before_the_first_snapshot_cannot_be_bracketed():
    assert profiling.pick(SNAPS, 0.2, 2.0) is None


def test_the_storms_times_come_from_its_report(tmp_path):
    (tmp_path / "raw").mkdir()
    (tmp_path / "raw" / "a-b1-n800.json").write_text(json.dumps({"runs": [{"startedAtMs": 5000, "endedAtMs": 6000}, {"startedAtMs": 9000, "endedAtMs": 9500}, {}]}))
    assert profiling.storm_window(tmp_path, "a-b1-n800") == (5.0, 9.5)
    (tmp_path / "raw" / "b-b1-n800.json").write_text(json.dumps({"runs": [{"samples": []}]}))
    assert profiling.storm_window(tmp_path, "b-b1-n800") is None and profiling.storm_window(tmp_path, "nope") is None


class Probes(Runner):
    """kubectl as the probes' pods answer it: two probe pods, one with the broker's snapshots."""

    def __init__(self):
        super().__init__()
        self.cmds = []

    def output(self, cmd, *, input=None, env=None, dry_run_value="", quiet=False):
        self.cmds.append(cmd)
        if "pods" in cmd and "-l" in cmd:
            return json.dumps({"items": [{"metadata": {"name": "cpuprobe-a"}}, {"metadata": {"name": "cpuprobe-b"}}]})
        if cmd[-1].endswith("cpuprobe-a:9100/proxy/profiles"):
            return json.dumps({"node": "n1", "profiles": [{"pod": "broker-0", "uid": "aaaaaaaa-1", "kind": "block", "atMs": 2000, "bytes": 3, "file": "aaaaaaaa-1/2000.block.pb.gz"},
                                                           {"pod": "broker-0", "uid": "aaaaaaaa-1", "kind": "block", "atMs": 1000, "bytes": 3, "file": "aaaaaaaa-1/1000.block.pb.gz"}]})
        return json.dumps({"node": "n2", "profiles": []})

    def __call__(self, cmd, **k):
        self.cmds.append(cmd)


def test_the_run_reads_the_snapshots_out_of_the_probes_into_its_results(tmp_path, monkeypatch):
    monkeypatch.setattr(profiling.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])
    fetched = []

    def fake_run(cmd, **k):
        fetched.append(cmd[-1])
        return subprocess.CompletedProcess(cmd, 0, stdout=b"BIN" + cmd[-1].encode()[-12:-9], stderr=b"")
    monkeypatch.setattr(profiling.subprocess, "run", fake_run)
    kept = profiling.export(Probes(), "eks", tmp_path, say=lambda m: None)
    assert [s["atMs"] for s in kept] == [1000, 2000] and all(s["node"] == "n1" for s in kept)           # in time order, with the node they came from
    assert (tmp_path / "raw/profile/aaaaaaaa-1/1000.block.pb.gz").read_bytes().startswith(b"BIN")
    assert json.loads((tmp_path / "raw/profile/index.json").read_text())["snapshots"][0]["file"] == "aaaaaaaa-1/1000.block.pb.gz"
    assert any(f.endswith("cpuprobe-a:9100/proxy/profiles/aaaaaaaa-1/1000.block.pb.gz") for f in fetched)


def test_a_probe_that_has_no_profiles_says_null_and_is_not_an_error(tmp_path, monkeypatch):
    """Found on EKS: Go encodes an empty slice as null, and four of the five probes had nothing (the broker is on one node)."""
    monkeypatch.setattr(profiling.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])

    class Null(Probes):
        def output(self, cmd, **k):
            return json.dumps({"items": [{"metadata": {"name": "cpuprobe-a"}}]}) if "-l" in cmd else '{"node": "n", "profiles": null}'
    assert profiling.export(Null(), "eks", tmp_path, say=lambda m: None) == []


def test_exporting_a_finished_runs_series_by_hand_takes_its_profiles_too(tmp_path, monkeypatch):
    out = tmp_path / "run"
    (out / "raw").mkdir(parents=True)
    (out / "environment.json").write_text(json.dumps({"date": "2026-11-21T11:44:36-08:00", "target": "eks", "variants": [{"label": "a", "commit": "", "image": "x"}, {"label": "b", "commit": "", "image": "y"}], "design": {"phases": ["n800"]}}))
    (out / "raw" / "DONE").write_text("2026-11-21T12:44:36-0800\n")
    seen = []
    monkeypatch.setattr(runcmd.monitoring, "export", lambda *a, **k: {})
    monkeypatch.setattr(profiling, "export", lambda run, target, o: seen.append(o) or [])
    assert runcmd.execute_monitoring(Runner(), cli.build_parser().parse_args(["--target", "eks", "monitoring", "export", str(out)])) == 0
    assert seen == [out]


def test_a_run_without_a_profiled_broker_writes_no_profile_files(tmp_path, monkeypatch):
    monkeypatch.setattr(profiling.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])

    class Empty(Probes):
        def output(self, cmd, **k):
            return json.dumps({"items": [{"metadata": {"name": "cpuprobe-a"}}]}) if "-l" in cmd else json.dumps({"node": "n", "profiles": []})
    assert profiling.export(Empty(), "eks", tmp_path, say=lambda m: None) == []
    assert not (tmp_path / "raw").exists()


def test_clearing_asks_every_probe_to_delete_its_snapshots(monkeypatch):
    monkeypatch.setattr(profiling.k8s, "kubectl_command", lambda *a, target: ["kubectl", *a])
    run = Probes()
    profiling.clear(run, "eks")
    deletes = [c for c in run.cmds if c[:2] == ["kubectl", "delete"]]
    assert [c[-1].split("/pods/")[1].split(":")[0] for c in deletes] == ["cpuprobe-a", "cpuprobe-b"] and all("--raw" in c for c in deletes)


def test_exporting_records_that_the_broker_was_profiled_and_a_failure_does_not_fail_the_run(tmp_path, monkeypatch, capsys):
    (tmp_path / "environment.json").write_text(json.dumps({"run_id": "x"}))
    monkeypatch.setattr(profiling, "export", lambda run, target, out: [{"pod": "broker-0", "atMs": 1}])
    runcmd.export_profiles(Runner(), "eks", tmp_path)
    env = json.loads((tmp_path / "environment.json").read_text())
    assert env["profiling"]["block_profiles"] == 1 and "perturbed" in env["profiling"]["note"]
    monkeypatch.setattr(profiling, "export", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("probe gone")))
    runcmd.export_profiles(Runner(), "eks", tmp_path)
    assert "could not read the broker profiles from the probes (probe gone)" in capsys.readouterr().err


def test_a_run_that_kept_no_profiles_has_no_profile_section(tmp_path):
    (tmp_path / "raw").mkdir()
    assert profiling.summarize(tmp_path) == []


def test_without_go_the_profiles_are_kept_and_the_report_says_it_cannot_read_them(tmp_path, monkeypatch):
    folder = tmp_path / "raw" / "profile"
    folder.mkdir(parents=True)
    for n in ("before", "after"):
        (folder / f"a-b1-n800.block.{n}.pb.gz").write_bytes(b"x")
    monkeypatch.setattr(profiling.shutil, "which", lambda n: None)
    assert "`go` is not installed" in profiling.summarize(tmp_path)[0]


def test_the_summary_has_a_row_per_storm_with_the_locks_the_callout_and_each_functions_share(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    (raw / "profile").mkdir(parents=True)
    (raw / "a-b1-n800.json").write_text(json.dumps({"runs": [{"startedAtMs": 3500, "endedAtMs": 5500}]}))
    (raw / "profile" / "index.json").write_text(json.dumps({"snapshots": [s for s in SNAPS if s["uid"] == "u1"]}))
    monkeypatch.setattr(profiling.shutil, "which", lambda n: "/usr/bin/go")
    monkeypatch.setattr(profiling, "_traces", lambda b, a: profiling.parse_traces(TRACES))
    text = "\n".join(profiling.summarize(tmp_path))
    assert "perturbed" in text
    row = next(l for l in text.splitlines() if l.startswith("  a-b1-n800"))
    assert "260.3" in row and "111.7" in row and "46%" in row and "0%" in row                    # locks, callout, authentication's share and addClient's


@pytest.mark.skipif(not shutil.which("go"), reason="needs go tool pprof")
def test_real_pprof_traces_are_parsed_and_the_idle_waits_left_out(tmp_path):
    """The command line and the text format against the real tool, on a profile this test makes with Go: goroutines contend for a lock in a
    function of ours, and others wait idle on a condition variable and on a channel."""
    prog = tmp_path / "p"
    prog.mkdir()
    (prog / "go.mod").write_text("module p\ngo 1.21\n")
    (prog / "main.go").write_text('''package main
import ("os";"runtime";"runtime/pprof";"sync";"time")
type A struct{ mu sync.Mutex }
func (a *A) addClient() { a.mu.Lock(); time.Sleep(2*time.Millisecond); a.mu.Unlock() }
func main() {
  runtime.SetBlockProfileRate(1)
  w := func(n string) { f, _ := os.Create(n); pprof.Lookup("block").WriteTo(f, 0); f.Close() }
  w(os.Args[1])
  a := &A{}
  var wg sync.WaitGroup
  c := sync.NewCond(&sync.Mutex{})
  for i := 0; i < 4; i++ { go func() { c.L.Lock(); c.Wait(); c.L.Unlock() }() }
  for i := 0; i < 8; i++ { wg.Add(1); go func() { defer wg.Done(); a.addClient() }() }
  wg.Wait()
  time.Sleep(30*time.Millisecond)
  c.Broadcast()
  time.Sleep(10*time.Millisecond)
  w(os.Args[2])
}
''')
    before, after = tmp_path / "before.pb.gz", tmp_path / "after.pb.gz"
    subprocess.run(["go", "run", ".", str(before), str(after)], cwd=prog, check=True, capture_output=True, timeout=120)
    w = profiling.waits(profiling._traces(before, after))
    assert w["lock_s"] > 0.005                                                        # seven goroutines waited a few ms each
    stacks = profiling._traces(before, after)
    in_ours = sum(s for s, f in stacks if any("addClient" in x for x in f) and any(profiling.LOCK.search(x) for x in f))
    assert in_ours == pytest.approx(w["lock_s"])                                       # every lock wait here is in addClient
    assert sum(s for s, _ in stacks) > w["lock_s"]                                     # the idle Cond waits are in the profile and not in the lock time


MUTEX = """File: nats-server
Type: delay
-----------+-------------------------------------------------------
   30s   sync.(*RWMutex).Unlock
         github.com/nats-io/nats-server/v2/server.(*Server).createWSClient
         github.com/nats-io/nats-server/v2/server.(*Server).startGoRoutine.func1
-----------+-------------------------------------------------------
   10s   sync.(*Mutex).Unlock (inline)
         runtime.something
         github.com/nats-io/nats-server/v2/server.(*Server).accConnsUpdate
         github.com/nats-io/nats-server/v2/server.(*Account).addClient
-----------+-------------------------------------------------------
   5s   sync.(*RWMutex).RUnlock
        github.com/nats-io/nats-server/v2/server.(*Server).accConnsUpdate
"""


def test_a_mutex_profile_charges_the_wait_to_the_function_that_released_the_lock():
    h = profiling.holders(profiling.parse_traces(MUTEX))
    assert h["total_s"] == 45 and list(h["by"]) == ["(*Server).createWSClient", "(*Server).accConnsUpdate"]
    assert h["by"]["(*Server).accConnsUpdate"] == 15                                   # the runtime frame between is skipped, and two stacks add up
    assert h["in"]["addClient"] == 10 and h["in"]["createWSClient"] == 30


def test_the_summary_says_who_held_the_locks_when_the_broker_had_a_mutex_profile(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    (raw / "profile").mkdir(parents=True)
    (raw / "a-b1-n800.json").write_text(json.dumps({"runs": [{"startedAtMs": 3500, "endedAtMs": 5500}]}))
    snaps = [{**s, "kind": k} for s in SNAPS if s["uid"] == "u1" for k in ("block", "mutex")]
    for s in snaps:
        s["file"] = s["file"].replace("block", s["kind"])
    (raw / "profile" / "index.json").write_text(json.dumps({"snapshots": snaps}))
    monkeypatch.setattr(profiling.shutil, "which", lambda n: "/usr/bin/go")
    monkeypatch.setattr(profiling, "_traces", lambda b, a: profiling.parse_traces(MUTEX if "mutex" in str(b) else TRACES))
    text = "\n".join(profiling.summarize(tmp_path))
    held = text[text.index("held:"):]
    assert "(*Server).createWSClient 67%" in held and "(*Server).accConnsUpdate 33%" in held and "45.0" in held
    assert "perturbed" in text.split("held:")[0]


def test_a_broker_without_the_patch_has_an_empty_mutex_profile_and_no_held_section(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    (raw / "profile").mkdir(parents=True)
    (raw / "a-b1-n800.json").write_text(json.dumps({"runs": [{"startedAtMs": 3500, "endedAtMs": 5500}]}))
    snaps = [{**s, "kind": k, "file": s["file"].replace("block", k)} for s in SNAPS if s["uid"] == "u1" for k in ("block", "mutex")]
    (raw / "profile" / "index.json").write_text(json.dumps({"snapshots": snaps}))
    monkeypatch.setattr(profiling.shutil, "which", lambda n: "/usr/bin/go")
    monkeypatch.setattr(profiling, "_traces", lambda b, a: [] if "mutex" in str(b) else profiling.parse_traces(TRACES))
    assert "held:" not in "\n".join(profiling.summarize(tmp_path))


@pytest.mark.skipif(not shutil.which("go"), reason="needs go tool pprof")
def test_a_real_mutex_profile_names_the_function_that_held_the_lock(tmp_path):
    prog = tmp_path / "p"
    prog.mkdir()
    (prog / "go.mod").write_text("module p\ngo 1.21\n")
    (prog / "main.go").write_text('''package main
import ("os";"runtime";"runtime/pprof";"sync";"time")
type A struct{ mu sync.Mutex }
func (a *A) holdsTheLock() { a.mu.Lock(); time.Sleep(3*time.Millisecond); a.mu.Unlock() }
func main() {
  runtime.SetMutexProfileFraction(1)
  w := func(n string) { f, _ := os.Create(n); pprof.Lookup("mutex").WriteTo(f, 0); f.Close() }
  w(os.Args[1])
  a := &A{}
  var wg sync.WaitGroup
  for i := 0; i < 8; i++ { wg.Add(1); go func() { defer wg.Done(); a.holdsTheLock() }() }
  wg.Wait()
  w(os.Args[2])
}
''')
    before, after = tmp_path / "before.pb.gz", tmp_path / "after.pb.gz"
    subprocess.run(["go", "run", ".", str(before), str(after)], cwd=prog, check=True, capture_output=True, timeout=120)
    h = profiling.holders(profiling._traces(before, after))
    assert h["total_s"] > 0.005 and next(iter(h["by"])).endswith("holdsTheLock")
