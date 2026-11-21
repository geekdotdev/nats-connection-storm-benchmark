# SPDX-License-Identifier: Apache-2.0
"""Runs external tools (docker, podman, kind, kubectl, ...).

Commands are lists of arguments, never shell strings, so nothing is parsed by a shell and
a value cannot inject another command. Every command is echoed to stderr before it runs;
`dry_run` echoes without running. Standard input (`input=`) is how secrets reach a tool: it
is never echoed. `env` adds variables for one command (echoed, so never put a secret there). Commands run from the repository root, so paths in them are relative to it.
"""
from __future__ import annotations

import os
import shlex
import subprocess
import sys

from .config import REPO_ROOT


class Runner:
    def __init__(self, dry_run: bool = False) -> None:
        self.dry_run = dry_run

    def __call__(self, cmd: list[str], *, input: str | None = None, env: dict[str, str] | None = None) -> None:
        shown = " ".join(f"{k}={shlex.quote(v)}" for k, v in (env or {}).items())
        print("+ " + (shown + " " if shown else "") + shlex.join(cmd) + ("   (stdin withheld)" if input is not None else ""), file=sys.stderr)
        if self.dry_run:
            return
        subprocess.run(cmd, input=input, text=True, check=True, cwd=REPO_ROOT, env={**os.environ, **(env or {})})

    def output(self, cmd: list[str], *, input: str | None = None, env: dict[str, str] | None = None,
               dry_run_value: str = "", quiet: bool = False) -> str:
        """Run a command and return its stdout (not echoed, since it may be a secret). In dry run, return dry_run_value.
        `quiet` skips echoing the command too, for polling loops."""
        shown = " ".join(f"{k}={shlex.quote(v)}" for k, v in (env or {}).items())
        if not quiet:
            print("+ " + (shown + " " if shown else "") + shlex.join(cmd) + ("   (stdin withheld)" if input is not None else "")
                  + "   (stdout captured)", file=sys.stderr)
        if self.dry_run:
            return dry_run_value
        return subprocess.run(cmd, input=input, text=True, check=True, cwd=REPO_ROOT, env={**os.environ, **(env or {})},
                              stdout=subprocess.PIPE).stdout

    def succeeds(self, cmd: list[str]) -> bool:
        """Run a quiet, read-only yes/no probe and return whether it exited 0. Not echoed. It runs in dry run
        too, so the commands a dry run prints are the ones a real run would use."""
        return subprocess.run(cmd, cwd=REPO_ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
