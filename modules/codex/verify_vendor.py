#!/usr/bin/env python3
"""Verify the vendored Codex SDK unit against its SHA256SUMS manifest.

Same contract as the Atlas donor check in CI: the manifest must cover
exactly the vendored file set and every hash must match. Interpreter
bytecode caches (``__pycache__``, ``*.pyc`` — gitignored repo-wide) are
not part of the donor tree and are skipped.

    python3 verify_vendor.py
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent / "vendor_bridge"


def _skip(path: Path) -> bool:
    return "__pycache__" in path.parts or path.suffix == ".pyc"


def main() -> int:
    expected: dict[str, str] = {}
    for line in (ROOT / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        digest, name = line.split("  ", 1)
        expected[name] = digest
    actual = {
        p.relative_to(ROOT).as_posix()
        for p in ROOT.rglob("*")
        if p.is_file() and not _skip(p)
    }
    if actual != set(expected) | {"SHA256SUMS"}:
        print(f"file set drift: {sorted(actual ^ (set(expected) | {'SHA256SUMS'}))}")
        return 1
    bad = [
        name
        for name, digest in expected.items()
        if hashlib.sha256((ROOT / name).read_bytes()).hexdigest() != digest
    ]
    if bad:
        print(f"hash drift: {bad}")
        return 1
    print(f"vendor_bridge OK: {len(expected)} files match SHA256SUMS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
