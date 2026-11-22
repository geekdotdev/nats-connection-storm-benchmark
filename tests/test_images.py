# SPDX-License-Identifier: Apache-2.0
import re

import pytest

from stormbench import images, kind, registry
from stormbench.config import REPO_ROOT
from stormbench.runner import Runner

SHA = "0123456789abcdef0123456789abcdef01234567"


def test_broker_tag_is_the_short_sha():
    assert images.broker_tag(SHA) == "nats-server:0123456789ab"


@pytest.mark.parametrize("bad", ["main", "0123", SHA.upper(), SHA + "0", "v2.12.0"])
def test_ref_must_be_a_full_lowercase_sha(bad):
    with pytest.raises(ValueError):
        images.broker_tag(bad)


def test_repo_must_be_https():
    with pytest.raises(ValueError):
        images.build_commands(SHA, repo="git@github.com:a/b.git", tool="docker")


def test_broker_build_command(tmp_path):
    (cmd,) = images.build_commands(SHA, tool="docker", root=tmp_path)
    assert cmd[:2] == ["docker", "build"]
    assert f"REF={SHA}" in cmd and f"REPO={images.DEFAULT_REPO}" in cmd
    assert cmd[-3:] == ["-t", "nats-server:0123456789ab", "images/nats-server"]


def test_other_images_only_when_their_source_exists(tmp_path):
    assert len(images.build_commands(SHA, tool="docker", root=tmp_path)) == 1
    (tmp_path / "components/callout").mkdir(parents=True)
    (tmp_path / "components/callout/go.mod").write_text("module x\n")
    (tmp_path / "components/loadgen").mkdir(parents=True)
    (tmp_path / "components/loadgen/Cargo.toml").write_text("[package]\n")
    cmds = images.build_commands(SHA, tool="docker", root=tmp_path)
    tags = [[cmd[i + 1] for i, a in enumerate(cmd) if a == "-t"] for cmd in cmds]
    assert tags == [["nats-server:0123456789ab"],
                    [images.component_tag("callout", tmp_path), "callout:dev"],  # the content tag, then the local one
                    [images.component_tag("loadgen", tmp_path), "loadgen:dev"]]


def test_component_tag_follows_the_source_and_ignores_build_output(tmp_path):
    src = tmp_path / "components/callout"
    (src / "target").mkdir(parents=True)
    (src / "main.go").write_text("package main\n")
    first = images.component_tag("callout", tmp_path)
    assert first.startswith("callout:src-") and len(first) == len("callout:src-") + 12
    (src / "target" / "junk").write_text("build output")
    assert images.component_tag("callout", tmp_path) == first
    (src / "main.go").write_text("package main // changed\n")
    assert images.component_tag("callout", tmp_path) != first
    (tmp_path / "images/callout").mkdir(parents=True)
    changed = images.component_tag("callout", tmp_path)
    (tmp_path / "images/callout/Dockerfile").write_text("FROM scratch\n")
    assert images.component_tag("callout", tmp_path) != changed  # the Dockerfile counts too


def test_a_multi_platform_build_gets_only_the_content_tag(tmp_path):
    (tmp_path / "components/callout").mkdir(parents=True)
    (tmp_path / "components/callout/go.mod").write_text("module x\n")
    _, callout = images.build_commands(SHA, tool="podman", root=tmp_path, platforms=("linux/amd64", "linux/arm64"))[:2]
    assert callout[callout.index("--manifest") + 1] == images.component_tag("callout", tmp_path)


def test_shell_metacharacters_stay_one_argument(tmp_path):
    cmds = images.build_commands(SHA, repo="https://x/a b;rm -rf ~", tool="docker", root=tmp_path)
    assert "REPO=https://x/a b;rm -rf ~" in cmds[0]


def test_registry_login_takes_the_token_on_stdin_only():
    cmd = registry.login_command("ghcr.io/me/proj", "me", tool="docker")
    assert cmd == ["docker", "login", "ghcr.io", "--username", "me", "--password-stdin"]


def test_platforms_are_parsed_and_checked():
    assert images.parse_platforms(None) == () and images.parse_platforms("") == ()
    assert images.parse_platforms("linux/amd64, linux/arm64") == ("linux/amd64", "linux/arm64")
    for bad in ["amd64", "linux/riscv64", "windows/amd64", "linux/amd64,linux/amd64"]:
        with pytest.raises(ValueError):
            images.parse_platforms(bad)


def test_one_platform_is_one_flag_and_the_same_tag(tmp_path):
    (cmd,) = images.build_commands(SHA, tool="podman", root=tmp_path, platforms=("linux/amd64",))
    assert cmd[cmd.index("--platform") + 1] == "linux/amd64"
    assert cmd[-3:] == ["-t", "nats-server:0123456789ab", "images/nats-server"]


def test_two_platforms_build_one_manifest_list(tmp_path):
    (build,) = images.build_commands(SHA, tool="podman", root=tmp_path, platforms=("linux/amd64", "linux/arm64"))
    assert build[build.index("--platform") + 1] == "linux/amd64,linux/arm64"
    assert build[build.index("--manifest") + 1] == "nats-server:0123456789ab" and "-t" not in build


def test_two_platforms_need_podman(tmp_path):
    with pytest.raises(ValueError, match="podman"):
        images.build_commands(SHA, tool="docker", root=tmp_path, platforms=("linux/amd64", "linux/arm64"))


def test_container_tool_prefers_the_variable_then_docker_then_podman(monkeypatch):
    monkeypatch.setenv("CONTAINER_TOOL", "podman")
    assert images.container_tool() == "podman"
    monkeypatch.delenv("CONTAINER_TOOL")
    monkeypatch.setattr(images.shutil, "which", lambda n: "/x/podman" if n == "podman" else None)
    assert images.container_tool() == "podman"
    monkeypatch.setattr(images.shutil, "which", lambda n: "/x/" + n)
    assert images.container_tool() == "docker"


@pytest.mark.parametrize("bad", ["ghcr.io", "ghcr.io/", "ghcr.io/Me", ""])
def test_registry_must_be_lowercase_host_and_account(bad):
    with pytest.raises(ValueError):
        registry.check_registry(bad)


def test_push_names_drop_podmans_localhost_prefix():
    assert registry.target_name("ghcr.io/me", "localhost/nats-server:abc") == "ghcr.io/me/nats-server:abc"


def test_podman_push_uses_a_temporary_authfile_and_a_digestfile(tmp_path):
    auth = tmp_path / "auth.json"
    login = registry.login_command("ghcr.io/me", "me", tool="podman", authfile=auth)
    assert login[-2:] == ["--authfile", str(auth)] and "--password-stdin" in login
    tag, push = registry.push_commands(["a:1"], "ghcr.io/me", tool="podman", authfile=auth, digestdir=tmp_path)
    assert tag == ["podman", "tag", "a:1", "ghcr.io/me/a:1"]
    assert push == ["podman", "push", "--authfile", str(auth), "--digestfile", str(tmp_path / "0"), "ghcr.io/me/a:1"]


def test_a_manifest_list_is_pushed_with_all_platforms_and_without_a_tag_step(tmp_path):
    (cmd,) = registry.push_commands(["localhost/a:1"], "ghcr.io/me", tool="podman", digestdir=tmp_path,
                                    manifest_lists=frozenset({"localhost/a:1"}))
    assert cmd == ["podman", "manifest", "push", "--all", "--digestfile", str(tmp_path / "0"),
                   "localhost/a:1", "docker://ghcr.io/me/a:1"]


def test_insecure_adds_tls_verify_false_to_login_and_push():
    assert "--tls-verify=false" in registry.login_command("127.0.0.1:5000/t", "u", tool="podman", insecure=True)
    assert "--tls-verify=false" in registry.push_commands(["a:1"], "127.0.0.1:5000/t", tool="podman", insecure=True)[1]


def test_registry_push_commands():
    cmds = registry.push_commands(["a:1"], "ghcr.io/me", tool="docker")
    assert cmds == [["docker", "tag", "a:1", "ghcr.io/me/a:1"], ["docker", "push", "ghcr.io/me/a:1"]]


def test_kind_load_with_docker_uses_docker_image():
    assert kind.load_commands(["a:1"], cluster="c", tool="docker") == [["kind", "load", "docker-image", "--name", "c", "a:1"]]


def test_kind_load_with_podman_goes_through_an_archive(tmp_path):
    cmds = kind.load_commands(["localhost/a:1"], cluster="c", tool="podman", archive_dir=tmp_path)
    archive = str(tmp_path / "image-0.tar")
    assert cmds == [["podman", "save", "-o", archive, "localhost/a:1"],
                    ["kind", "load", "image-archive", "--name", "c", archive]]


def test_kind_podman_needs_the_provider_variable_and_docker_does_not():
    assert kind.provider_env("podman") == {"KIND_EXPERIMENTAL_PROVIDER": "podman"}
    assert kind.provider_env("docker") == {}


def test_kind_cluster_commands_use_their_own_kubeconfig(tmp_path):
    kc = tmp_path / "kc"
    create = kind.create_command("c", kc)
    assert create[create.index("--kubeconfig") + 1] == str(kc)
    assert create[create.index("--config") + 1] == "infra/kind/cluster.yaml"
    assert kind.delete_command("c", kc)[-2:] == ["--kubeconfig", str(kc)]


def test_dry_run_echoes_the_environment_for_the_command(capsys):
    Runner(dry_run=True)(["kind", "x"], env={"KIND_EXPERIMENTAL_PROVIDER": "podman"})
    assert "KIND_EXPERIMENTAL_PROVIDER=podman kind x" in capsys.readouterr().err


def test_dry_run_runs_nothing_and_withholds_stdin(capsys):
    Runner(dry_run=True)(["definitely-not-a-real-binary", "x"], input="s3cret")
    err = capsys.readouterr().err
    assert "definitely-not-a-real-binary x" in err and "s3cret" not in err


class FakeRunner:
    """Pretends the container tool holds the given manifest lists and plain images."""
    def __init__(self, lists=(), plain=()):
        self.lists, self.plain, self.ran = set(lists), set(plain), []

    def succeeds(self, cmd):
        kind, tag = cmd[1:3], cmd[3]
        return tag in self.lists if kind == ["manifest", "exists"] else tag in (self.plain | self.lists)

    def __call__(self, cmd, **kw):
        self.ran.append(cmd)


TAG = "nats-server:0123456789ab"


@pytest.mark.parametrize("holds,expected", [
    ({"lists": [TAG]}, ["podman", "manifest", "rm", TAG]),
    ({"plain": [TAG]}, ["podman", "rmi", TAG]),
    ({}, None),
])
def test_old_tag_is_dropped_the_way_its_content_needs(holds, expected):
    assert images.drop_command(FakeRunner(**holds), "podman", TAG) == expected


def test_a_multi_platform_build_drops_each_old_tag_before_building_it(monkeypatch):
    monkeypatch.setattr(images, "container_tool", lambda: "podman")
    callout, loadgen = images.component_tag("callout"), images.component_tag("loadgen")
    run = FakeRunner(lists=[TAG], plain=[callout])
    images.build(run, SHA, platforms=("linux/amd64", "linux/arm64"))
    assert run.ran[0] == ["podman", "manifest", "rm", TAG]
    assert run.ran[1][:2] == ["podman", "build"]
    assert ["podman", "rmi", callout] in run.ran
    assert not any(c[1:3] == ["rmi", loadgen] for c in run.ran)  # nothing to drop: no command


def test_a_single_platform_build_drops_nothing(monkeypatch):
    monkeypatch.setattr(images, "container_tool", lambda: "podman")
    run = FakeRunner(lists=[TAG])
    images.build(run, SHA, platforms=("linux/amd64",))
    assert all(c[1] == "build" for c in run.ran)


@pytest.mark.parametrize("name,tag", [
    ("feature/lockfree-acc-conns-update", "feature-lockfree-acc-conns-update"),
    ("main", "main"), ("v2.12.0", "v2.12.0"), ("a//b  c", "a-b-c"), ("-.hidden", "hidden"),
    ("x" * 200, "x" * 128),
])
def test_ref_name_becomes_a_valid_tag(name, tag):
    assert images.ref_name_tag(name) == tag


@pytest.mark.parametrize("bad", ["", "///", "0123456789ab", "a\nb"])
def test_bad_ref_names_are_refused(bad):
    with pytest.raises(ValueError):
        images.ref_name_tag(bad)


def test_ref_name_adds_a_label_and_a_second_tag_to_the_broker_only(tmp_path):
    (cmd,) = images.build_commands(SHA, tool="docker", root=tmp_path, ref_name="feature/x")
    assert cmd[cmd.index("--label") + 1] == "org.opencontainers.image.ref.name=feature/x"
    assert [cmd[i + 1] for i, a in enumerate(cmd) if a == "-t"] == [TAG, "nats-server:feature-x"]
    (tmp_path / "components/callout").mkdir(parents=True)
    (tmp_path / "components/callout/go.mod").write_text("module x\n")
    _, callout = images.build_commands(SHA, tool="docker", root=tmp_path, ref_name="feature/x")
    assert "--label" not in callout


def test_ref_name_on_a_manifest_list_tags_the_list_afterwards(tmp_path):
    build, tag = images.build_commands(SHA, tool="podman", root=tmp_path, ref_name="main",
                                       platforms=("linux/amd64", "linux/arm64"))
    assert "--label" in build and build[build.index("--manifest") + 1] == TAG
    assert tag == ["podman", "tag", TAG, "nats-server:main"]


def test_without_ref_name_there_is_no_label_and_one_tag(tmp_path):
    (cmd,) = images.build_commands(SHA, tool="docker", root=tmp_path)
    assert "--label" not in cmd and cmd.count("-t") == 1


def test_only_limits_the_build_to_the_named_images(tmp_path):
    (tmp_path / "components/callout").mkdir(parents=True)
    (tmp_path / "components/callout/go.mod").write_text("module x\n")
    (tmp_path / "components/loadgen").mkdir(parents=True)
    (tmp_path / "components/loadgen/Cargo.toml").write_text("[package]\n")
    (cmd,) = images.build_commands(None, tool="docker", root=tmp_path, only=("callout",))  # no commit needed
    assert images.component_tag("callout", tmp_path) in cmd and "REF=None" not in " ".join(cmd)
    with pytest.raises(ValueError):
        images.build_commands(None, tool="docker", root=tmp_path, only=("nope",))
    with pytest.raises(ValueError):
        images.build_commands(None, tool="docker", root=tmp_path)  # the broker is built, so the commit is required


# -- a patch on top of the commit --------------------------------------------------------------------------------------------------------
def with_patch(tmp_path, name="mutex-profile", body="--- a/x\n+++ b/x\n"):
    d = tmp_path / "images/nats-server/patches"
    d.mkdir(parents=True)
    (d / f"{name}.patch").write_text(body)
    return tmp_path


def test_a_patched_broker_image_is_tagged_and_labelled_so_it_cannot_pass_for_the_commit(tmp_path):
    import hashlib
    root = with_patch(tmp_path)
    (cmd,) = images.build_commands(SHA, tool="docker", root=root, only=("nats-server",), patch="mutex-profile")
    assert images.broker_tag(SHA, "mutex-profile") == f"nats-server:{SHA[:12]}-mutex-profile" and f"nats-server:{SHA[:12]}-mutex-profile" in cmd
    assert f"nats-server:{SHA[:12]}" not in cmd                                      # the commit's own tag is not taken by the patched image
    assert "PATCH=mutex-profile" in cmd
    assert f"benchmark.patch.sha256={hashlib.sha256((root / 'images/nats-server/patches/mutex-profile.patch').read_bytes()).hexdigest()}" in cmd


def test_no_patch_means_the_build_is_as_it_was(tmp_path):
    (cmd,) = images.build_commands(SHA, tool="docker", root=tmp_path, only=("nats-server",))
    assert not any(a.startswith("PATCH=") for a in cmd) and f"nats-server:{SHA[:12]}" in cmd


@pytest.mark.parametrize("name", ["../x", "Mutex", "a b", "a--b", "-a", "a/b", ""])
def test_a_patch_name_cannot_reach_another_path(tmp_path, name):
    with pytest.raises(ValueError, match="lowercase letters, digits and hyphens"):
        images.patch_file(name, with_patch(tmp_path))


def test_a_patch_that_does_not_exist_lists_the_ones_that_do(tmp_path):
    with pytest.raises(ValueError, match=r"no patch 'nope'.*have: mutex-profile"):
        images.patch_file("nope", with_patch(tmp_path))


def test_the_shipped_mutex_patch_adds_only_the_mutex_profile_fraction():
    text = images.patch_file("mutex-profile").read_text()
    added = [l for l in text.splitlines() if l.startswith("+") and not l.startswith("+++") and not l.strip("+ \t").startswith("//")]
    assert added == ["+\truntime.SetMutexProfileFraction(1)"] and not [l for l in text.splitlines() if l.startswith("-") and not l.startswith("---")]


def test_the_dockerfile_applies_the_patch_only_when_asked_and_records_it():
    text = (REPO_ROOT / "images/nats-server/Dockerfile").read_text()
    assert 'git apply --verbose "/patches/$PATCH.patch"' in text and 'if [ -n "$PATCH" ]' in text and 'benchmark.patch="$PATCH"' in text
    assert (REPO_ROOT / "images/nats-server/patches/mutex-profile.patch").is_file()


def test_the_relaxed_patch_touches_only_the_two_files_it_names_and_says_what_it_gives_up():
    text = images.patch_file("relaxed-acc-conns").read_text()
    files = re.findall(r"^\+\+\+ b/(\S+)", text, re.M)
    assert files == ["server/accounts.go", "server/events.go"]                               # nothing else in nats-server changes
    assert "accConnsCoalesce" in text and "at most accConnsCoalesce late" in text and "Benchmark patch" in text
    assert "connsPending atomic.Bool" in text
