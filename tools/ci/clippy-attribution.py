#!/usr/bin/env python3
"""Run scoped Clippy and fail only on compiler errors or changed-line warnings.

Historical warnings remain visible in the job summary. The classifier uses the
exact base..head Rust diff; it never suppresses a nonzero cargo exit.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


@dataclass(frozen=True)
class Interval:
    start: int
    end: int

    def overlaps(self, start: int, end: int) -> bool:
        return self.start <= end and start <= self.end


def normalize_path(value: str) -> str:
    path = value.replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    return path


def parse_changed_lines(diff_text: str) -> dict[str, list[Interval]]:
    changed: dict[str, list[Interval]] = {}
    current: str | None = None
    for raw in diff_text.splitlines():
        if raw.startswith("+++ "):
            name = raw[4:].split("\t", 1)[0]
            if name == "/dev/null":
                current = None
            elif name.startswith("b/"):
                current = normalize_path(name[2:])
            else:
                current = normalize_path(name)
            continue
        if current is None:
            continue
        match = HUNK.match(raw)
        if match is None:
            continue
        start = int(match.group(1))
        count = int(match.group(2) or "1")
        if count > 0:
            changed.setdefault(current, []).append(Interval(start, start + count - 1))
    return changed


def load_changed_lines(base: str, head: str) -> dict[str, list[Interval]]:
    command = [
        "git",
        "diff",
        "--unified=0",
        "--no-color",
        "--no-ext-diff",
        base,
        head,
        "--",
        "*.rs",
    ]
    result = subprocess.run(
        command,
        check=True,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
        encoding="utf-8",
        errors="strict",
    )
    return parse_changed_lines(result.stdout)


def primary_spans(diagnostic: dict) -> Iterable[dict]:
    for span in diagnostic.get("spans") or []:
        if span.get("is_primary"):
            yield span


def warning_is_owned(diagnostic: dict, changed: dict[str, list[Interval]]) -> bool:
    for span in primary_spans(diagnostic):
        file_name = span.get("file_name")
        line_start = span.get("line_start")
        line_end = span.get("line_end")
        if not isinstance(file_name, str) or not isinstance(line_start, int):
            continue
        if not isinstance(line_end, int):
            line_end = line_start
        intervals = changed.get(normalize_path(file_name), [])
        if any(interval.overlaps(line_start, line_end) for interval in intervals):
            return True
    return False


def rendered(diagnostic: dict) -> str:
    text = diagnostic.get("rendered")
    if isinstance(text, str) and text.strip():
        return text.rstrip()
    code = diagnostic.get("code") or {}
    code_text = code.get("code") if isinstance(code, dict) else None
    prefix = f"[{code_text}] " if code_text else ""
    return prefix + str(diagnostic.get("message", "compiler diagnostic"))


def append_summary(owned: list[dict], historical: list[dict], errors: list[dict]) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    lines = ["### Scoped Clippy attribution", ""]
    lines.append(f"- compiler errors: **{len(errors)}**")
    lines.append(f"- warnings on changed lines: **{len(owned)}**")
    lines.append(f"- historical warnings outside changed lines: **{len(historical)}**")
    if historical:
        lines.extend(["", "Historical warnings (first 50):"])
        for diagnostic in historical[:50]:
            code = diagnostic.get("code") or {}
            code_text = code.get("code") if isinstance(code, dict) else None
            message = str(diagnostic.get("message", "warning")).replace("\n", " ")
            lines.append(f"- `{code_text or 'warning'}` — {message}")
    Path(path).open("a", encoding="utf-8").write("\n".join(lines) + "\n")


def run_clippy(args: argparse.Namespace) -> int:
    packages = json.loads(args.packages_json)
    if not isinstance(packages, list) or not all(isinstance(item, str) for item in packages):
        raise ValueError("packages JSON must be an array of strings")
    if not packages:
        return 0

    changed = load_changed_lines(args.base, args.head)
    command = [
        "cargo",
        "clippy",
        "--locked",
        "--lib",
        "--bins",
        "--no-deps",
        "--target-dir",
        args.target_dir,
        "--message-format=json",
    ]
    for package in packages:
        command.extend(["--package", package])
    command.extend(["--", "-W", "warnings"])

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=None,
        text=True,
        encoding="utf-8",
        errors="strict",
    )
    assert process.stdout is not None
    diagnostics: list[dict] = []
    for line in process.stdout:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            print(line, end="")
            continue
        if message.get("reason") == "compiler-message" and isinstance(message.get("message"), dict):
            diagnostics.append(message["message"])
    return_code = process.wait()

    errors = [item for item in diagnostics if item.get("level") == "error"]
    warnings = [item for item in diagnostics if item.get("level") == "warning"]
    owned = [item for item in warnings if warning_is_owned(item, changed)]
    historical = [item for item in warnings if not warning_is_owned(item, changed)]

    for diagnostic in errors:
        print(rendered(diagnostic), file=sys.stderr)
    for diagnostic in owned:
        print(rendered(diagnostic), file=sys.stderr)
    if historical:
        print(
            f"Clippy reported {len(historical)} historical warning(s) outside changed Rust lines; "
            "see the job summary.",
            file=sys.stderr,
        )
    append_summary(owned, historical, errors)

    if return_code != 0:
        return return_code
    return 1 if owned else 0


def self_test() -> int:
    diff = """diff --git a/crates/a/src/lib.rs b/crates/a/src/lib.rs
--- a/crates/a/src/lib.rs
+++ b/crates/a/src/lib.rs
@@ -10,0 +11,2 @@
+one
+two
@@ -30,1 +32,1 @@
-old
+new
"""
    changed = parse_changed_lines(diff)
    assert changed == {
        "crates/a/src/lib.rs": [Interval(11, 12), Interval(32, 32)]
    }
    owned = {
        "spans": [
            {
                "file_name": "crates/a/src/lib.rs",
                "line_start": 12,
                "line_end": 12,
                "is_primary": True,
            }
        ]
    }
    historical = {
        "spans": [
            {
                "file_name": "crates/a/src/lib.rs",
                "line_start": 20,
                "line_end": 20,
                "is_primary": True,
            }
        ]
    }
    windows = {
        "spans": [
            {
                "file_name": "crates\\a\\src\\lib.rs",
                "line_start": 32,
                "line_end": 32,
                "is_primary": True,
            }
        ]
    }
    assert warning_is_owned(owned, changed)
    assert not warning_is_owned(historical, changed)
    assert warning_is_owned(windows, changed)

    from unittest.mock import patch

    unicode_diff = '''diff --git a/crates/a/src/lib.rs b/crates/a/src/lib.rs
--- a/crates/a/src/lib.rs
+++ b/crates/a/src/lib.rs
@@ -1,0 +2 @@
+const NAME: &str = "Ł";
'''
    try:
        unicode_diff.encode("utf-8").decode("cp1252")
    except UnicodeDecodeError:
        pass
    else:
        raise AssertionError("UTF-8 regression marker must fail under Windows CP1252")
    emit_utf8 = "import sys; sys.stdout.buffer.write(sys.argv[1].encode('utf-8'))"
    real_run = subprocess.run
    git_options: dict = {}

    def fake_git_run(command: list[str], **kwargs):
        assert command[:2] == ["git", "diff"]
        git_options.update(kwargs)
        return real_run(
            [sys.executable, "-c", emit_utf8, unicode_diff], **kwargs
        )

    with patch.object(subprocess, "run", side_effect=fake_git_run):
        assert load_changed_lines("base", "head") == {
            "crates/a/src/lib.rs": [Interval(2, 2)]
        }
    assert git_options["encoding"] == "utf-8"
    assert git_options["errors"] == "strict"
    assert git_options["stdout"] == subprocess.PIPE
    assert git_options["stderr"] is None

    clippy_payload = json.dumps(
        {
            "reason": "compiler-message",
            "message": {"level": "note", "message": "Ł", "spans": []},
        },
        ensure_ascii=False,
    ) + "\n"
    real_popen = subprocess.Popen
    clippy_options: dict = {}

    def fake_clippy_popen(command: list[str], **kwargs):
        assert command[0] == "cargo"
        clippy_options.update(kwargs)
        return real_popen(
            [sys.executable, "-c", emit_utf8, clippy_payload], **kwargs
        )

    module = sys.modules[__name__]
    clippy_args = argparse.Namespace(
        packages_json='["swarm-kernel-host"]',
        base="base",
        head="head",
        target_dir="target",
    )
    with (
        patch.object(module, "load_changed_lines", return_value={}),
        patch.object(module, "append_summary"),
        patch.object(subprocess, "Popen", side_effect=fake_clippy_popen),
    ):
        assert run_clippy(clippy_args) == 0
    assert clippy_options["encoding"] == "utf-8"
    assert clippy_options["errors"] == "strict"
    assert clippy_options["text"] is True
    assert clippy_options["stderr"] is None

    print("clippy attribution self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--base")
    parser.add_argument("--head")
    parser.add_argument("--target-dir")
    parser.add_argument("--packages-json")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    missing = [
        name
        for name in ("base", "head", "target_dir", "packages_json")
        if getattr(args, name.replace("-", "_"), None) is None
    ]
    if missing:
        parser.error("missing required run arguments: " + ", ".join(missing))
    return run_clippy(args)


if __name__ == "__main__":
    raise SystemExit(main())
