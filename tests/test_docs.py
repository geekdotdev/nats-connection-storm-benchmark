# SPDX-License-Identifier: Apache-2.0
"""Guards against documentation rot: the commands the docs show must parse with the real command line, and the repository paths they
name must exist. The design record docs/operator-mode-plan.md is left out (it names things that were planned, changed or removed on
purpose); the results directories are data, not documentation."""
import re
import shlex
from pathlib import Path

import pytest

from stormbench import cli
from stormbench.config import REPO_ROOT

DOCS = sorted(p for p in REPO_ROOT.rglob("*.md")
              if not any(part in (".venv", "target", ".git", ".pytest_cache", "results", "node_modules") for part in p.relative_to(REPO_ROOT).parts)
              and p.relative_to(REPO_ROOT).as_posix() != "docs/operator-mode-plan.md")
ROOTS = ("components", "deploy", "docs", "driver", "images", "infra", "tests", "results")


def code_blocks(text: str):
    return re.findall(r"```(?:sh|bash)?\n(.*?)```", text, re.S)


def bench_commands(text: str):
    """The `uv run bench ...` command lines in fenced blocks, with continuations joined and trailing comments dropped."""
    out = []
    for block in code_blocks(text):
        joined = re.sub(r"\\\n\s*", " ", block)
        for line in joined.splitlines():
            line = line.strip()
            if not line.startswith("uv run bench"):
                continue
            line = re.sub(r"\s+#.*$", "", line)          # a trailing comment
            out.append(line)
    return out


def argv_of(line: str) -> list[str]:
    line = re.sub(r"<[^>]+>", "x", line)                 # <full sha> -> x
    line = re.sub(r"\[[^\]]*\]", "", line)               # [--repo URL]: optional, left out
    argv = shlex.split(line)[3:]                         # drop `uv run bench`
    argv = [a.split("|")[0] if "|" in a else a for a in argv]   # create|delete -> create
    return [a for a in argv if a != "..."]


COMMANDS = [(p.relative_to(REPO_ROOT).as_posix(), line) for p in DOCS for line in bench_commands(p.read_text())]


def test_the_docs_show_some_commands():
    assert len(COMMANDS) >= 10  # the guard is not silently checking nothing


@pytest.mark.parametrize("doc,line", COMMANDS, ids=[f"{d}: {l[:60]}" for d, l in COMMANDS])
def test_every_documented_command_parses(doc, line):
    argv = argv_of(line)
    try:
        cli.build_parser().parse_args(argv)
    except SystemExit as e:  # argparse exits on a bad option or a missing argument
        pytest.fail(f"{doc} shows `{line}`, which the command line refuses (exit {e.code}): {argv}")


def repo_paths(text: str):
    """Backticked names that look like paths into the repository."""
    for m in re.finditer(r"`([^`\n]+)`", text):
        s = m.group(1).strip()
        if s.split("/")[0] in ROOTS and "/" in s and not re.search(r"[<>*{}|\s=]|\.\.\.|\$", s):
            yield s.rstrip("/.,:;)")


PATHS = sorted({(p.relative_to(REPO_ROOT).as_posix(), s) for p in DOCS for s in repo_paths(p.read_text())})


def test_the_docs_name_some_paths():
    assert len(PATHS) >= 20


@pytest.mark.parametrize("doc,path", PATHS, ids=[f"{d}: {p}" for d, p in PATHS])
def test_every_path_a_doc_names_exists(doc, path):
    # A path may carry a line suffix like main.go:42, or name a file by its basename inside a directory the sentence just named.
    target = path.split(":")[0]
    assert (REPO_ROOT / target).exists(), f"{doc} names {path}, which does not exist"
