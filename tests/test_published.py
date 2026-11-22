# SPDX-License-Identifier: Apache-2.0
"""The published images, and a run's notes and the index: what keeps the context files from drifting away from the code and the data."""
import json
import re
import shutil
import subprocess

import pytest

from stormbench import ab, notes, runcmd
from stormbench.config import REPO_ROOT
from test_state import FakeCluster, go, run_args  # noqa: F401

@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(ab.time, "sleep", lambda s: None)   # run_blocks pauses 2 s after each storm


DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


# -- images/PUBLISHED.md ------------------------------------------------------------------------------------------------------------------
def pinned_digests():
    files = [*REPO_ROOT.glob("deploy/*/overlays/eks*/kustomization.yaml"), REPO_ROOT / "deploy/monitoring/base/prometheus.yaml", *REPO_ROOT.glob("*variants.toml")]
    return {f.relative_to(REPO_ROOT).as_posix(): set(DIGEST.findall(f.read_text())) for f in files}


def test_every_digest_the_overlays_and_variants_files_pin_is_in_the_published_images_file():
    published = (REPO_ROOT / "images/PUBLISHED.md").read_text()
    missing = {f: sorted(d for d in digests if d not in published) for f, digests in pinned_digests().items()}
    assert not any(missing.values()), f"pinned but not in images/PUBLISHED.md (add a row when you push an image): {missing}"
    assert sum(len(d) for d in pinned_digests().values()) >= 8                          # the scan found the pins: it is not passing for lack of looking


def test_the_published_images_file_says_which_images_are_current_and_which_retired():
    text = (REPO_ROOT / "images/PUBLISHED.md").read_text()
    assert text.count("**current**") + text.count("| current |") >= 4 and "retired" in text            # callout, provision, loadgen, cpuprobe
    for name in ("nats-server", "callout", "provision", "loadgen", "cpuprobe"):
        assert f"`{name}`" in text


# -- a run's notes ------------------------------------------------------------------------------------------------------------------------
ENV = {"target": "eks", "cluster_kind": "eks", "namespace": "storm-bench", "run_id": "abc123abc123", "date": "2026-11-21T10:00:00-08:00",
       "auth": {"mode": "operator", "resolver": "memory"}, "design": {"blocks": 2, "sizes": [800], "phases": ["warm", "idle", "n800"]},
       "cpu_source": "probe", "monitoring": {"probe": True, "prometheus": True, "scrape_interval_s": 1},
       "variants": [{"label": "main", "commit": "a" * 40, "image": "ghcr.io/o/nats-server@sha256:" + "1" * 64},
                    {"label": "b", "commit": "", "image": "localhost/x:dev"}]}


def test_the_facts_come_from_environment_json_and_say_where_the_cpu_came_from():
    text = notes.facts("r", {**ENV, "profiling": {"block_profiles": 9, "note": "perturbed"}, "resumes": [{"time": "t", "units_done": 3}],
                             "adopted": {"redo": [{"unit": "main-b1", "files": ["x"]}, {"unit": "main-b2", "files": []}]}})
    assert "cluster eks · namespace storm-bench · run id abc123abc123 · started 2026-11-21T10:00:00-08:00" in text
    assert "auth operator mode, memory resolver" in text and "2 blocks, sizes [800]" in text and "CPU source:** probe" in text
    assert "| `main` | aaaaaaaaaaaa | nats-server@sha256:111111111111… |" in text and "| `b` | - | localhost/x:dev |" in text
    assert "9 snapshots; perturbed" in text and "with 3 units done" in text
    assert "main-b1" in text and "main-b2" not in text                                  # a unit with no files had not run: it was not run again


def test_a_run_that_recorded_little_says_so_instead_of_guessing():
    text = notes.facts("old", {"date": "2026-11-17T21:11:29-08:00", "design": {"blocks": 8}})
    assert "run id not recorded" in text and "namespace not recorded" in text and "CPU source:** not recorded" in text


def test_the_skeleton_leaves_three_headings_for_what_only_a_person_knows_and_is_never_overwritten(tmp_path):
    (tmp_path / "environment.json").write_text(json.dumps(ENV))
    path = notes.ensure(tmp_path)
    assert path == tmp_path / "NOTES.md" and notes.unwritten(path) == ["Purpose", "What it showed", "What perturbs it"]
    path.write_text(path.read_text().replace(f"## Purpose\n\n{notes.TODO}", "## Purpose\n\nWhy this was made."))
    assert notes.unwritten(path) == ["What it showed", "What perturbs it"]
    assert notes.ensure(tmp_path) is None and "Why this was made." in path.read_text()  # what a person wrote is the point of the file


def test_refreshing_replaces_only_the_generated_block(tmp_path):
    (tmp_path / "environment.json").write_text(json.dumps(ENV))
    path = notes.ensure(tmp_path)
    path.write_text(path.read_text().replace(f"## Purpose\n\n{notes.TODO}", "## Purpose\n\nWhy this was made."))
    (tmp_path / "environment.json").write_text(json.dumps({**ENV, "resumes": [{"time": "later", "units_done": 1}]}))
    assert notes.refresh(tmp_path) is True
    text = path.read_text()
    assert "later with 1 units done" in text and "Why this was made." in text and text.count(notes.BEGIN) == 1
    assert notes.refresh(tmp_path / "nowhere") is False


def test_a_run_that_ends_writes_its_notes_and_says_what_is_left_to_write(tmp_path, capsys):
    assert go(run_args(tmp_path), __import__("stormbench.state", fromlist=["x"]).MemoryStore(), FakeCluster()) == 0
    out = capsys.readouterr().out
    path = tmp_path / "out" / "NOTES.md"
    assert path.is_file() and "results/INDEX.md" in out and "Purpose" in out
    assert notes.unwritten(path) == ["Purpose", "What it showed", "What perturbs it"]
    assert "run id" in path.read_text() and "kind" in path.read_text()


def test_a_failure_to_write_notes_does_not_fail_the_run(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runcmd.notes, "ensure", lambda out: (_ for _ in ()).throw(OSError("disk full")))
    runcmd.write_notes(tmp_path)
    assert "could not write" in capsys.readouterr().err


# -- the index and the notes of the runs that are here -------------------------------------------------------------------------------------
def run_dirs():
    """The runs that have ended. A run still going has no raw/DONE and its notes are written when it ends; a run that was stopped
    early is in neither place, and its notes are written by hand (CLAUDE.md), which this cannot check: say so in INDEX.md."""
    root = REPO_ROOT / "results"
    # results/<login>/<run>/ (the layout), and results/<run>/ for a run not yet moved under its login
    return sorted(p for p in [*root.glob("*/"), *root.glob("*/*/")] if (p / "environment.json").is_file() and (p / "raw" / "DONE").is_file())


def test_every_run_here_has_notes_with_nothing_left_unwritten_and_a_line_in_the_index():
    index = (REPO_ROOT / "results/INDEX.md").read_text()
    for d in run_dirs():
        assert (d / "NOTES.md").is_file(), f"{d.name} has no NOTES.md (bench run writes it; CLAUDE.md: every run gets notes)"
        assert not notes.unwritten(d / "NOTES.md"), f"{d.name}/NOTES.md still has unwritten sections: {notes.unwritten(d / 'NOTES.md')}"
        assert f"({d.relative_to(REPO_ROOT / 'results').as_posix()}/NOTES.md)" in index, f"{d.name} has no line in results/INDEX.md"


def test_the_index_names_only_runs_that_have_notes():
    for name in re.findall(r"\]\(([^)]+)/NOTES\.md\)", (REPO_ROOT / "results/INDEX.md").read_text()):
        assert (REPO_ROOT / "results" / name / "NOTES.md").is_file(), f"INDEX.md lists {name}, which has no NOTES.md"


@pytest.mark.skipif(not shutil.which("git"), reason="needs git")
def test_results_are_not_ignored_so_a_run_is_committed_whole():
    """A decision, written down so it is not undone by accident: results/ is tracked (the notes, the data and the profiles together), not ignored."""
    def ignored(path):
        return subprocess.run(["git", "check-ignore", "-q", path], cwd=REPO_ROOT).returncode == 0
    for path in ("results/INDEX.md", "results/some-run-2026-01-01/NOTES.md", "results/some-run-2026-01-01/raw/x.json", "results/some-run-2026-01-01/summary.txt",
                 "results/some-run-2026-01-01/prometheus/x.json", "results/some-run-2026-01-01/raw/profile/index.json", "results/some-run-2026-01-01/cpu.svg"):
        assert not ignored(path), f"{path} is ignored by git: results/ is meant to be committed whole (see results/README.md)"
