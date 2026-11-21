# SPDX-License-Identifier: Apache-2.0
import json
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
callout_args = ["serve", "--url", "nats://broker:4222", "--accounts", "16", "--workers", "8"]
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
    (v2('[variant.callout_env]\nCALLOUT_ISSUER_SEED = "x"'), "cannot be overridden"),
    (v2('[variant.storm]\nSTORM_CONNECTIONS = "5"'), "cannot be overridden"),
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
    v = ab.Variant("t", B, "i", storm={"STORM_THREADS": "8", "STORM_ACCOUNTS": "32"})
    env = plan.storm_env(phase, "x", v)
    assert env["STORM_THREADS"] == "8" and env["STORM_ACCOUNTS"] == "32" and env["STORM_CONNECTIONS"] == base["STORM_CONNECTIONS"]


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
