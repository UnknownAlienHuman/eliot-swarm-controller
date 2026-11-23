#!/usr/bin/env python3
"""Project check: cargo fmt --check, then locked build and strict Clippy for changed packages.

Usage: python build/check.py [base-commit]
  base-commit defaults to $ELIOT_TASK_BASE. It is resolved once to a full commit id.
  Changed files are everything differing from that commit: commits since it, staged
  and unstaged edits, and untracked files that are not ignored.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

# Inputs that affect every workspace member.
SHARED_FILES = {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "clippy.toml", "build/check.py"}
SHARED_PREFIXES = (".cargo/",)
SHARED_BUILD_SUFFIX = ".rs"


def run(args: list[str], cwd: Path, *, capture: bool = False) -> bytes:
    """Run a command, failing the check on a nonzero exit."""
    print(f"== {' '.join(args)}", flush=True)
    result = subprocess.run(
        args,
        cwd=cwd,
        check=False,
        capture_output=capture,
        # On Windows, implicit inheritance drops the empty RUSTC_WRAPPER override.
        env=os.environ.copy(),
    )
    if result.returncode != 0:
        if capture and result.stderr:
            sys.stderr.buffer.write(result.stderr)
        raise SystemExit(f"project check failed: {' '.join(args)} exited {result.returncode}")
    return result.stdout if capture else b""


def git_z(root: Path, args: list[str]) -> list[str]:
    """Return NUL-delimited git output as relative POSIX paths."""
    out = run(["git", *args], root, capture=True)
    return [os.fsdecode(item) for item in out.split(b"\0") if item]


def repo_root() -> Path:
    out = run(["git", "rev-parse", "--show-toplevel"], Path.cwd(), capture=True)
    return Path(os.fsdecode(out.removesuffix(b"\n"))).resolve()


def resolve_base(root: Path, requested: str) -> str:
    out = subprocess.run(
        ["git", "rev-parse", "--verify", "--quiet", "--end-of-options", f"{requested}^{{commit}}"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    sha = out.stdout.strip()
    if out.returncode != 0 or not sha:
        raise SystemExit(f"project check: not a commit: {requested}")
    return sha


def workspace_members(root: Path) -> list[dict]:
    """Return workspace member packages from cargo metadata, with manifest-dir paths."""
    out = run(
        ["cargo", "metadata", "--no-deps", "--locked", "--format-version", "1"],
        root,
        capture=True,
    )
    meta = json.loads(out)
    member_ids = set(meta["workspace_members"])
    packages = []
    for pkg in meta["packages"]:
        if pkg["id"] not in member_ids:
            continue
        manifest_dir = Path(pkg["manifest_path"]).resolve().parent
        packages.append({"name": pkg["name"], "dir": manifest_dir})
    return packages


def contained_in(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
        return True
    except ValueError:
        return False


def is_shared(rel: str) -> bool:
    if rel in SHARED_FILES or rel.startswith(SHARED_PREFIXES):
        return True
    return rel.startswith("build/") and rel.endswith(SHARED_BUILD_SUFFIX) and "/" not in rel[len("build/"):]


def select_packages(root: Path, changed: list[str], packages: list[dict]) -> list[str]:
    if any(is_shared(rel) for rel in changed):
        return sorted({pkg["name"] for pkg in packages})

    selected: set[str] = set()
    for rel in changed:
        relative = Path(rel)
        if relative.anchor or ".." in relative.parts:
            continue
        path = root / relative
        # Longest manifest directory wins, so nested members are not double-counted.
        best = None
        for pkg in packages:
            if contained_in(path, pkg["dir"]) and (best is None or len(pkg["dir"].parts) > len(best["dir"].parts)):
                best = pkg
        if best is not None:
            selected.add(best["name"])
    return sorted(selected)


def set_cargo_isolation(root: Path) -> None:
    """Keep Cargo outputs private to the Agent artifact root or this worktree."""
    artifact_root = os.environ.get("ELIOT_ARTIFACT_ROOT")
    target = root / Path(artifact_root) / "target" if artifact_root else root / "target" / "project-check"
    os.environ["CARGO_TARGET_DIR"] = os.path.abspath(target)
    os.environ["RUSTC_WRAPPER"] = ""


def main(argv: list[str]) -> int:
    if len(argv) > 2:
        print("usage: python build/check.py [base-commit]", file=sys.stderr)
        return 2
    requested = argv[1] if len(argv) > 1 else os.environ.get("ELIOT_TASK_BASE", "")
    if not requested:
        print("project check: no base commit; pass one or set ELIOT_TASK_BASE", file=sys.stderr)
        return 2

    root = repo_root()
    set_cargo_isolation(root)
    base = resolve_base(root, requested)
    print(f"== base: {base}", flush=True)
    run(["cargo", "fmt", "--check"], root)

    changed = git_z(root, ["diff", "--no-renames", "--name-only", "-z", base, "--"])
    changed += git_z(root, ["ls-files", "--others", "--exclude-standard", "-z"])
    changed = sorted(set(changed))

    packages = workspace_members(root)
    selected = select_packages(root, changed, packages)

    print(f"== changed Cargo packages: {' '.join(selected) or '(none)'}")

    if not selected:
        print("== no changed Cargo packages; build and clippy skipped")
        return 0

    for name in selected:
        run(["cargo", "build", "--locked", "-p", name, "--all-targets"], root)
        run(["cargo", "clippy", "--locked", "-p", name, "--all-targets", "--", "-D", "warnings"], root)

    print("== project check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
