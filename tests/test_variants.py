# SPDX-License-Identifier: Apache-2.0
import json
import re
import shutil
import subprocess
import tomllib
from pathlib import Path

import pytest

from stormbench import ab, cli, preflight as pf, report, runcmd, variants
from stormbench.config import REPO_ROOT

A, B, C = "a" * 40, "b" * 40, "c" * 40
PREFIX = "ghcr.io/o/"


def parse(text: str, base: Path = REPO_ROOT):
    return variants.parse(tomllib.loads(text), PREFIX, base)


FULL = f'''
[[variant]]
label = "main"
commit = "{A}"

[[variant]]
label = "tuned"
commit = "{B}"
stack = "deploy/stack/overlays/eks-cluster"
conf = "max_payload: 2MB\\n"
callout_args = ["serve", "--url", "nats://broker:4222", "--workers", "8"]
[variant.broker_env]
GOGC = "50"
[variant.broker_resources]
cpu = "2"
memory = "4Gi"
[variant.callout_env]
CALLOUT_LOG = "debug"
[variant.storm]
STORM_THREADS = "8"

[[variant]]
label = "other"
image = "ghcr.io/x/nats-server@sha256:{"1" * 64}"
'''


# -- reading variants ---------------------------------------------------------------------------------------------
def test_a_variants_file_is_read_with_defaults_filled_in():
    main, tuned, other = parse(FULL)
    assert main == ab.Variant("main", A, f"{PREFIX}nats-server:{'a' * 12}")  # image from the commit, nothing else changed
    assert main.settings() == {} and main.describe() == {"label": "main", "commit": A, "image": main.image}
    assert tuned.stack == "deploy/stack/overlays/eks-cluster" and tuned.conf == "max_payload: 2MB\n"
    assert tuned.broker_env == {"GOGC": "50"} and tuned.broker_resources == {"cpu": "2", "memory": "4Gi"}
    assert tuned.callout_env == {"CALLOUT_LOG": "debug"} and tuned.callout_args[-2:] == ("--workers", "8")
    assert tuned.storm == {"STORM_THREADS": "8"}
    assert other.commit == "" and other.image.endswith("1" * 64)  # an image given as is needs no commit
    assert set(tuned.settings()) == {"stack", "conf", "broker_env", "broker_resources", "callout_env", "callout_args", "storm"}
    json.dumps(tuned.describe())  # the record goes into environment.json


def test_two_commits_make_two_variants_that_differ_only_in_the_image():
    a, b = variants.from_commits(A, B, "main", "mine", "localhost/")
    assert (a.label, a.image, b.label, b.image) == ("main", "localhost/nats-server:" + "a" * 12, "mine", "localhost/nats-server:" + "b" * 12)
    assert a.settings() == b.settings() == {}


def test_conf_can_come_from_a_file_next_to_the_variants_file(tmp_path):
    (tmp_path / "extra.conf").write_text("max_connections: 100\n")
    two = f'[[variant]]\nlabel = "a"\ncommit = "{A}"\nconf_file = "extra.conf"\n[[variant]]\nlabel = "b"\ncommit = "{B}"\n'
    assert parse(two, tmp_path)[0].conf == "max_connections: 100\n"
    with pytest.raises(ValueError, match="not found"):
        parse(two.replace("extra.conf", "missing.conf"), tmp_path)
    with pytest.raises(ValueError, match="not both"):
        parse(two.replace('conf_file = "extra.conf"', 'conf_file = "extra.conf"\nconf = "x"'), tmp_path)


def v2(extra: str) -> str:
    return f'[[variant]]\nlabel = "a"\ncommit = "{A}"\n{extra}\n[[variant]]\nlabel = "b"\ncommit = "{B}"\n'


@pytest.mark.parametrize("text,message", [
    ('[[variant]]\nlabel = "a"\ncommit = "' + A + '"\n', "at least two"),
    ("x = 1\n" + v2(""), "unknown top-level"),
    (v2('colour = "red"'), "unknown key"),
    ('[[variant]]\ncommit = "' + A + '"\n[[variant]]\nlabel = "b"\ncommit = "' + B + '"\n', "needs a label"),
    ('[[variant]]\nlabel = "a"\n[[variant]]\nlabel = "b"\ncommit = "' + B + '"\n', "needs a commit"),
    ('[[variant]]\nlabel = "a"\ncommit = "main"\n[[variant]]\nlabel = "b"\ncommit = "' + B + '"\n', "40 character"),
    (v2('stack = "deploy/stack/base"'), "overlay directory"),
    (v2('stack = "../../etc"'), "overlay directory"),
    (v2('stack = "deploy/stack/overlays/nope"'), "overlay directory"),
    (v2("[variant.broker_env]\nGOGC = 50"), "quoted string"),
    (v2('[variant.broker_env]\n"bad name" = "x"'), "not a valid name"),
    (v2('[variant.broker_env]\nPOD_NAME = "x"'), "cannot be overridden"),
    (v2('[variant.callout_env]\nCALLOUT_CREDS = "x"'), "cannot be overridden"),
    (v2('[variant.storm]\nSTORM_CONNECTIONS = "5"'), "cannot be overridden"),
    (v2('[variant.storm]\nSTORM_SAMPLES = "0"'), "cannot be overridden"),   # the queue curves need every run's samples
    (v2('[variant.storm]\nthreads = "5"'), "not a valid name"),
    (v2('[variant.broker_resources]\ncpu = "lots"'), "quantity"),
    (v2('[variant.broker_resources]\ngpu = "1"'), "not a valid name"),
    (v2('callout_args = []\nconf = 5'), "conf must be text"),
    (v2('callout_args = ["serve", 5]'), "list of non-empty strings"),
])
def test_bad_variants_are_refused_with_the_reason(text, message):
    with pytest.raises(ValueError, match=message):
        parse(text)


def test_load_reports_toml_errors_with_the_file_name(tmp_path):
    f = tmp_path / "v.toml"
    f.write_text("[[variant\n")
    with pytest.raises(ValueError, match="v.toml"):
        variants.load(f, PREFIX)


def test_the_example_file_in_docs_parses():
    path = REPO_ROOT / "docs" / "variants.example.toml"
    assert len(variants.load(path, "localhost/")) >= 3


def test_the_lock_variants_file_pins_every_broker_image_by_digest():
    import re
    path = REPO_ROOT / "lock-variants.toml"
    vs = variants.load(path, "ghcr.io/cfedersp/")
    assert [v.label for v in vs] == ["main", "nolock", "mux_main", "mux_nolock"]        # the first is the baseline
    text = path.read_text()
    for v in vs:
        assert re.fullmatch(r"ghcr\.io/cfedersp/nats-server@sha256:[0-9a-f]{64}", v.image), v.image
        assert len(v.commit) == 40
        assert f"nats-server:{v.commit[:12]}" in text, f"{v.label}: the comment must name the tag the digest was pushed as"
        assert v.settings() == {}                          # they differ in the broker's source only
    assert len({v.image for v in vs}) == len(vs) and len({v.commit for v in vs}) == len(vs)


# -- the plan -----------------------------------------------------------------------------------------------------
def test_a_plan_needs_two_variants_with_different_labels():
    one = ab.Variant("a", A, "x")
    with pytest.raises(ValueError):
        ab.Plan(variants=(one,))
    with pytest.raises(ValueError):
        ab.Plan(variants=(one, one, ab.Variant("c", C, "z")))
    assert ab.Plan(variants=(one, ab.Variant("b", B, "y"))).order == "alternating by block"
    assert ab.Plan(variants=(one, ab.Variant("b", B, "y"), ab.Variant("c", C, "z"))).order == "rotating by block"


def three():
    return tuple(ab.Variant(l, c * 40, f"img-{l}") for l, c in (("a", "a"), ("b", "b"), ("c", "c")))


def test_three_variants_rotate_so_each_goes_first_in_turn():
    plan = ab.Plan(variants=three(), blocks=4, sizes=(800,))
    order = []
    for s in ab.schedule(plan):
        if (s.block, s.variant.label) not in order:
            order.append((s.block, s.variant.label))
    assert [lbl for blk, lbl in order if blk == 1] == ["a", "b", "c"]
    assert [lbl for blk, lbl in order if blk == 2] == ["b", "c", "a"]
    assert [lbl for blk, lbl in order if blk == 3] == ["c", "a", "b"]
    assert [lbl for blk, lbl in order if blk == 4] == ["a", "b", "c"]


def test_two_variants_still_alternate_exactly_as_before():
    two = three()[:2]
    assert [v.label for v in ab.order_in_block(two, 1)] == ["a", "b"]
    assert [v.label for v in ab.order_in_block(two, 2)] == ["b", "a"]
    assert [v.label for v in ab.order_in_block(two, 3)] == ["a", "b"]


def test_a_variants_own_generator_settings_go_on_top_of_the_plans():
    plan = ab.Plan(variants=three(), threads=4)
    phase = plan.phases[2]
    base = plan.storm_env(phase, "x")
    assert base["STORM_THREADS"] == "4"
    v = ab.Variant("t", B, "i", storm={"STORM_THREADS": "8", "STORM_ROLES": "app-feed-publish-a"})
    env = plan.storm_env(phase, "x", v)
    assert env["STORM_THREADS"] == "8" and env["STORM_ROLES"] == "app-feed-publish-a" and env["STORM_CONNECTIONS"] == base["STORM_CONNECTIONS"]


class FakeBackend:
    def __init__(self):
        self.started, self.envs = [], {}

    def start_variant(self, v):
        self.started.append(v.label)
        return {}

    def run_storm(self, label, env):
        self.envs[label] = env
        one = {"connections": 1, "seconds": 0.1, "outcomes": {"admitted": 1}, "authMs": {"p50": 10.0}}
        return ab.REPORT_PREFIX + json.dumps({"runs": [one]}) + "\n"


def test_every_variant_gets_a_fresh_broker_each_block_and_its_own_meta_file(tmp_path):
    fake = FakeBackend()
    plan = ab.Plan(variants=three(), blocks=2, sizes=(800,))
    ab.run_blocks(plan, fake, tmp_path / "raw", sleep=lambda s: None, now=lambda: "T", log=lambda m: None)
    assert fake.started == ["a", "b", "c", "b", "c", "a"]
    names = {p.name for p in (tmp_path / "raw").iterdir()}
    assert {f"{l}-b{b}.meta.json" for l in "abc" for b in (1, 2)} <= names
    assert json.loads((tmp_path / "raw" / "c-b1.meta.json").read_text())["variant"] == "c"


def test_the_environment_records_every_variants_settings_and_the_order():
    tuned = parse(FULL)[1]
    env = runcmd.environment(ab.Plan(variants=(parse(FULL)[0], tuned, parse(FULL)[2])), {}, 8.0)
    assert [v["label"] for v in env["variants"]] == ["main", "tuned", "other"]
    assert env["variants"][1]["broker_env"] == {"GOGC": "50"} and env["design"]["order"] == "rotating by block"
    assert "arms" not in env
    assert env["auth"]["mode"] == "operator" and env["auth"]["resolver"] == "memory" and env["auth"]["tenant_accounts"] == 1
    json.dumps(env)  # it is written to environment.json


# -- the report with several variants ----------------------------------------------------------------------------
def write_run(tmp_path, p50s, blocks=6, legacy=False):
    raw = tmp_path / "raw"
    raw.mkdir()
    labels = list(p50s)
    described = [{"label": l, "commit": "", "image": f"img-{l}", **({"stack": "deploy/stack/overlays/eks-cluster"} if l == labels[1] else {})} for l in labels]
    env = {"design": {"phases": ["warm", "n800"]}}
    env["arms" if legacy else "variants"] = described
    (tmp_path / "environment.json").write_text(json.dumps(env))
    for blk in range(1, blocks + 1):
        for label, base in p50s.items():
            (raw / f"{label}-b{blk}.meta.json").write_text("{}")
            one = {"connections": 800, "seconds": 0.2, "outcomes": {"admitted": 800}, "authMs": {"p50": base + blk * 0.1}}
            (raw / f"{label}-b{blk}-n800.json").write_text(json.dumps({"runs": [one] * 3}))
    return tmp_path


def test_the_report_compares_each_variant_with_the_first(tmp_path):
    text = report.summarize(write_run(tmp_path, {"base": 40.0, "mid": 30.0, "low": 20.0}))
    assert "baseline base; 2 comparisons with it, not corrected" in text
    assert "change mid vs base" in text and "change low vs base" in text and "change low vs mid" not in text
    assert "settings" in text and "eks-cluster" in text  # the variant's non-default settings are listed
    assert text.count("exact permutation p=") == 2


def test_two_variants_report_without_the_comparison_count_note(tmp_path):
    text = report.summarize(write_run(tmp_path, {"base": 40.0, "mid": 30.0}))
    assert "comparisons" not in text and "change mid vs base" in text


def test_older_results_with_arms_still_summarise(tmp_path):
    assert "change mid vs base" in report.summarize(write_run(tmp_path, {"base": 40.0, "mid": 30.0}, legacy=True))


# -- the commands -------------------------------------------------------------------------------------------------
def test_run_dry_run_lists_every_variant_and_its_settings(tmp_path, capsys, monkeypatch):
    monkeypatch.setenv("REGISTRY", "ghcr.io/o")
    f = tmp_path / "v.toml"
    f.write_text(FULL)
    assert cli.main(["--dry-run", "--target", "eks", "run", "--variants", str(f), "--blocks", "2", "--out", str(tmp_path / "out")]) == 0
    out = capsys.readouterr().out
    assert "2 blocks x 3 variants" in out and "variant tuned:" in out and "GOGC" in out
    assert "block 2  other" in out or "block 2  tuned" in out  # the rotation shows in the schedule


def test_variants_and_commits_cannot_be_mixed_or_both_missing(tmp_path, capsys):
    f = tmp_path / "v.toml"
    f.write_text(FULL)
    assert cli.main(["--dry-run", "run", "--variants", str(f), "--a", A, "--b", B]) == 2
    assert "not both" in capsys.readouterr().err
    assert cli.main(["--dry-run", "run"]) == 2
    assert "--variants" in capsys.readouterr().err


def test_a_bad_variants_file_is_an_error_not_a_traceback(tmp_path, capsys):
    f = tmp_path / "v.toml"
    f.write_text(v2("[variant.broker_env]\nPOD_NAME = \"x\""))
    assert cli.main(["--dry-run", "run", "--variants", str(f)]) == 2
    assert "cannot be overridden" in capsys.readouterr().err


# -- the generated kustomization ---------------------------------------------------------------------------------
def test_the_kustomization_always_sets_the_image_and_only_what_the_variant_changes(tmp_path):
    plain = ab.Variant("a", A, "ghcr.io/o/nats-server:x")
    doc = variants.kustomization(plain, REPO_ROOT / "deploy/stack/overlays/eks", tmp_path)
    assert len(doc["patches"]) == 1  # just the broker image: nothing else changed
    patch = json.loads(doc["patches"][0]["patch"])
    assert patch["spec"]["template"]["spec"]["containers"] == [{"name": "broker", "image": "ghcr.io/o/nats-server:x"}]
    assert "annotations" not in json.dumps(patch)
    stamped = variants.kustomization(plain, REPO_ROOT / "deploy/stack/overlays/eks", tmp_path, "T1")
    assert len(stamped["patches"]) == 2 and all("T1" in p["patch"] for p in stamped["patches"])  # broker and callout restart


def test_the_kustomization_path_to_the_overlay_resolves_from_where_it_is_written(tmp_path):
    d = variants.write(ab.Variant("a", A, "x"), "deploy/stack/overlays/eks", tmp_path)
    rel = json.loads((d / "kustomization.yaml").read_text())["resources"][0]
    assert (d / rel).resolve() == (REPO_ROOT / "deploy/stack/overlays/eks").resolve()


def render(directory: Path) -> str:
    kubectl = shutil.which("kubectl")
    if not kubectl:
        pytest.skip("kubectl is not installed")
    return subprocess.run([kubectl, "kustomize", str(directory)], check=True, capture_output=True, text=True,
                          env={"KUBECONFIG": "/dev/null"}).stdout


def test_a_variants_settings_reach_the_rendered_manifests(tmp_path):
    tuned = parse(FULL)[1]
    text = render(variants.write(tuned, "deploy/stack/overlays/eks", tmp_path, "STAMP"))
    assert "image: " + tuned.image in text                                     # the variant's broker image
    assert "name: GOGC" in text and 'value: "50"' in text                       # broker env
    assert "name: CALLOUT_LOG" in text and "value: debug" in text               # callout env
    assert "- --workers" in text and '- "8"' in text                            # callout args
    assert "max_payload: 2MB" in text                                           # extra broker config
    assert "replicas: 3" in text                                                # its own overlay (the cluster), not the default
    assert text.count("storm-bench/restarted-at: STAMP") == 2                   # broker and callout pod templates
    assert "name: POD_NAME" in text and "secretKeyRef" in text                  # the overlay's own wiring is untouched


def test_a_variant_with_no_settings_renders_like_its_overlay_plus_the_image(tmp_path):
    main = parse(FULL)[0]
    text = render(variants.write(main, "deploy/stack/overlays/eks", tmp_path))
    plain = render(REPO_ROOT / "deploy/stack/overlays/eks")
    assert "image: " + main.image in text and "replicas: 1" in text
    assert "extra.conf: |\n    # no extra settings" in text                    # the base's empty extra.conf is still empty
    assert text.count("kind:") == plain.count("kind:")                          # the same objects


# -- variants that cannot be applied over each other -------------------------------------------------------------
def test_preflight_refuses_variants_whose_overlays_have_different_objects(monkeypatch, tmp_path):
    from test_preflight import ClusterRunner, THREE_BROKERS, STACK, LOADGEN, fake
    for var in ("STORM_KUBECONFIG", "STORM_EKS_KUBECONFIG"):
        (tmp_path / var).write_text("x")
        monkeypatch.setenv(var, str(tmp_path / var))

    class Differing(ClusterRunner):
        n = 0

        def output(self, cmd, **kw):
            out = super().output(cmd, **kw)
            if "create" in cmd and "--dry-run=client" in cmd and self.last is STACK:
                Differing.n += 1
                if Differing.n == 2:  # the second variant renders one object more
                    self.last = STACK + [{"kind": "Service", "metadata": {"name": "extra"}}]
                    return "\n".join(json.dumps(o) for o in self.last)
            return out

    run = Differing(THREE_BROKERS, STACK, LOADGEN)
    pair = tuple(ab.Variant(l, c * 40, f"ghcr.io/o/nats-server:{c * 12}") for l, c in (("a", "a"), ("b", "b")))
    checks = pf.preflight(run, "eks", pair, "deploy/stack/overlays/eks", "deploy/loadgen/overlays/eks", fake(pf.Manifest(200, ("linux/amd64",))))
    bad = next(c for c in checks if c.name == "variants")
    assert not bad.ok and "Service/extra" in bad.detail and "leave the extra object behind" in bad.detail


# -- callout_replicas ---------------------------------------------------------------------------------------------------------------------------
REPLICA_FILE = {"variant": [{"label": "one", "image": "x"}, {"label": "three", "image": "x", "callout_replicas": 3}]}


def test_a_variant_can_say_how_many_callout_pods_run_and_the_other_keeps_the_overlays_one():
    one, three = variants.parse(REPLICA_FILE, "localhost/", REPO_ROOT)
    assert one.callout_replicas == 0 and three.callout_replicas == 3
    assert "callout_replicas" not in one.describe() and three.describe()["callout_replicas"] == 3          # recorded in environment.json when set
    assert ab.Variant.from_dict(three.describe()).callout_replicas == 3                                    # and read back by --resume


@pytest.mark.parametrize("bad", [-1, 11, 2.5, "3", True, None])
def test_a_silly_replica_count_is_refused(bad):
    with pytest.raises(ValueError, match="callout_replicas must be a whole number from 1 to 10"):
        variants.parse({"variant": [{"label": "a", "image": "x", "callout_replicas": bad}, {"label": "b", "image": "x"}]}, "localhost/", REPO_ROOT)


def test_the_replicas_become_a_patch_on_the_callout_deployment_only_for_the_variant_that_asks(tmp_path):
    one, three = variants.parse(REPLICA_FILE, "localhost/", REPO_ROOT)
    def callout_patches(v):
        doc = variants.kustomization(v, REPO_ROOT / "deploy/stack/overlays/eks", tmp_path / v.label)
        return [json.loads(p["patch"]) for p in doc.get("patches", []) if json.loads(p["patch"])["kind"] == "Deployment"]
    assert callout_patches(one) == []
    (patch,) = callout_patches(three)
    assert patch["metadata"]["name"] == "callout" and patch["spec"]["replicas"] == 3


@pytest.mark.skipif(not shutil.which("kubectl"), reason="needs kubectl kustomize")
def test_the_rendered_callout_has_the_replicas_the_variant_asked_for_and_the_other_variant_has_one(tmp_path):
    one, three = variants.parse(REPLICA_FILE, "localhost/", REPO_ROOT)
    def replicas(v):
        out = subprocess.run(["kubectl", "kustomize", str(variants.write(v, "deploy/stack/overlays/eks", tmp_path))], capture_output=True, text=True, check=True).stdout
        doc = next(d for d in out.split("\n---\n") if "kind: Deployment" in d and "name: callout" in d)
        return int(re.search(r"replicas: (\d+)", doc).group(1))
    assert (replicas(one), replicas(three)) == (1, 3)


# -- no_tls -------------------------------------------------------------------------------------------------------------------------------------
TLS_FILE = {"variant": [{"label": "tls", "image": "x"}, {"label": "plain", "image": "x", "no_tls": True}]}


def test_no_tls_is_recorded_for_the_variant_that_asks_and_read_back():
    tls, plain = variants.parse(TLS_FILE, "localhost/", REPO_ROOT)
    assert tls.no_tls is False and plain.no_tls is True
    assert "no_tls" not in tls.describe() and plain.describe()["no_tls"] is True
    assert ab.Variant.from_dict(plain.describe()).no_tls is True and ab.Variant.from_dict(tls.describe()).no_tls is False


@pytest.mark.parametrize("bad", ["yes", 1, 0, None, [True]])
def test_no_tls_must_be_a_real_boolean(bad):
    with pytest.raises(ValueError, match="no_tls must be true or false"):
        variants.parse({"variant": [{"label": "a", "image": "x", "no_tls": bad}, {"label": "b", "image": "x"}]}, "localhost/", REPO_ROOT)


def test_a_variant_that_writes_its_own_websocket_block_cannot_also_ask_for_no_tls():
    with pytest.raises(ValueError, match="no_tls writes the broker's websocket block itself"):
        variants.parse({"variant": [{"label": "a", "image": "x", "no_tls": True, "conf": "websocket { listen: 0.0.0.0:1 }"}, {"label": "b", "image": "x"}]}, "localhost/", REPO_ROOT)


def test_the_broker_and_the_generator_are_switched_together_and_only_for_that_variant(tmp_path):
    tls, plain = variants.parse(TLS_FILE, "localhost/", REPO_ROOT)
    def extra_conf(v):
        doc = variants.kustomization(v, REPO_ROOT / "deploy/stack/overlays/eks", tmp_path / v.label)
        found = [json.loads(p["patch"]) for p in doc.get("patches", []) if json.loads(p["patch"])["kind"] == "ConfigMap"]
        return found[0]["data"]["extra.conf"] if found else None
    assert extra_conf(tls) is None
    assert "no_tls: true" in extra_conf(plain) and "listen: 0.0.0.0:8443" in extra_conf(plain)             # the broker's side
    plan = ab.Plan(variants=(tls, plain), blocks=1, sizes=(800,))
    phase = plan.phases[-1]
    assert plan.storm_env(phase, "x", tls).get("STORM_URL") is None and plan.storm_env(phase, "x", plain)["STORM_URL"] == "ws://broker:8443"   # the generator's


def test_the_user_conf_and_no_tls_both_reach_extra_conf(tmp_path):
    v = variants.parse({"variant": [{"label": "a", "image": "x", "no_tls": True, "conf": "max_payload: 1048576"}, {"label": "b", "image": "x"}]}, "localhost/", REPO_ROOT)[0]
    doc = variants.kustomization(v, REPO_ROOT / "deploy/stack/overlays/eks", tmp_path / "a")
    conf = json.loads(next(p["patch"] for p in doc["patches"] if json.loads(p["patch"])["kind"] == "ConfigMap"))["data"]["extra.conf"]
    assert conf.startswith("max_payload: 1048576") and "no_tls: true" in conf


def test_a_variant_still_cannot_set_the_url_itself():
    with pytest.raises(ValueError, match="STORM_URL"):
        variants.parse({"variant": [{"label": "a", "image": "x", "storm": {"STORM_URL": "ws://elsewhere"}}, {"label": "b", "image": "x"}]}, "localhost/", REPO_ROOT)
