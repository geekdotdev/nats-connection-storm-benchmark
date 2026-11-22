# SPDX-License-Identifier: Apache-2.0
"""plans/: runs written down before the data exists. The tests keep a plan a plan: it has its prediction and its falsifier, a status, and a run once it has one."""
import re

import pytest

from stormbench.config import REPO_ROOT

PLANS = sorted(p for p in (REPO_ROOT / "plans").glob("*.md") if p.name != "README.md")
HEADINGS = ("Question", "Hypothesis", "Prediction", "Falsifier", "Design", "Needs approval", "Outcome")
STATUSES = ("planned", "running", "done", "dropped")


def test_there_are_plans_and_the_readme_lists_each_with_its_status():
    assert PLANS
    readme = (REPO_ROOT / "plans/README.md").read_text()
    for p in PLANS:
        status = re.search(r"\*\*Status:\*\* (\w+)", p.read_text()).group(1)
        assert re.search(rf"\[{p.stem}\]\({p.name}\) \| {status} \|", readme), f"plans/README.md does not list {p.stem} as {status}"


@pytest.mark.parametrize("plan", PLANS, ids=lambda p: p.stem)
def test_a_plan_has_every_heading_a_valid_status_and_says_what_would_prove_it_wrong(plan):
    text = plan.read_text()
    for h in HEADINGS:
        assert re.search(rf"^## {h}$", text, re.M), f"{plan.name} has no '## {h}'"
    m = re.search(r"\*\*Status:\*\* (\w+)", text)
    assert m and m.group(1) in STATUSES, f"{plan.name}: status must be one of {STATUSES}"
    falsifier = text.split("## Falsifier")[1].split("\n## ")[0].strip()
    assert len(falsifier) > 40, f"{plan.name}: write what would show the plan is wrong, before the run"


@pytest.mark.parametrize("plan", PLANS, ids=lambda p: p.stem)
def test_a_running_or_finished_plan_links_to_an_existing_run_and_a_planned_one_has_none(plan):
    text = plan.read_text()
    status = re.search(r"\*\*Status:\*\* (\w+)", text).group(1)
    runs = re.findall(r"\]\(\.\./results/([^)]+)/NOTES\.md\)", text.split("## Question")[0])
    if status in ("running", "done"):
        assert runs, f"{plan.name} is {status} but names no run"
        for r in runs:
            assert (REPO_ROOT / "results" / r).is_dir(), f"{plan.name} links to results/{r}, which does not exist"
    if status == "planned":
        assert not runs, f"{plan.name} is planned but already names a run"


def test_a_plan_that_is_done_says_what_happened():
    for p in PLANS:
        text = p.read_text()
        if "**Status:** done" in text:
            assert "Not yet" not in text.split("## Outcome")[1], f"{p.name} is done but its Outcome is not written"
