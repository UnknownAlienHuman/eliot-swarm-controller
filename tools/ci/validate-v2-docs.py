#!/usr/bin/env python3
"""Validate relative links and GitHub-style heading anchors in active v2 docs."""

from __future__ import annotations

import html
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
EXCLUDED = {ROOT / "docs" / "owner-decisions.md"}
SCHEMES = {"http", "https", "mailto", "data", "javascript"}
FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
HEADING_RE = re.compile(r"^ {0,3}(#{1,6})\s+(.+?)\s*#*\s*$")
LINK_RE = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")
REFERENCE_RE = re.compile(r"^ {0,3}\[[^\]]+\]:\s*(\S+)")
HTML_TAG_RE = re.compile(r"<[^>]+>")
MARKDOWN_LINK_TEXT_RE = re.compile(r"!?\[([^\]]*)\]\([^)]*\)")
GITHUB_PUNCT_RE = re.compile(
    r"[\u2000-\u206f\u2e00-\u2e7f\\'!\"#$%&()*+,./:;<=>?@\[\]^`{|}~]"
)


def active_markdown_files() -> list[Path]:
    files = [ROOT / "README.md", ROOT / "AGENTS.md", ROOT / "CLAUDE.md"]
    files.extend(sorted((ROOT / "docs").rglob("*.md")))
    return [path for path in files if path.is_file() and path not in EXCLUDED]


def strip_fenced_lines(text: str) -> list[tuple[int, str]]:
    result: list[tuple[int, str]] = []
    fence_char: str | None = None
    fence_len = 0
    for number, line in enumerate(text.splitlines(), 1):
        match = FENCE_RE.match(line)
        if fence_char is None:
            if match:
                marker = match.group(1)
                fence_char = marker[0]
                fence_len = len(marker)
                continue
            result.append((number, line))
            continue
        if match and match.group(1)[0] == fence_char and len(match.group(1)) >= fence_len:
            fence_char = None
            fence_len = 0
    if fence_char is not None:
        raise ValueError("unclosed fenced code block")
    return result


def heading_plain_text(value: str) -> str:
    value = html.unescape(value)
    value = MARKDOWN_LINK_TEXT_RE.sub(r"\1", value)
    value = HTML_TAG_RE.sub("", value)
    value = value.replace("*", "").replace("_", "_")
    return value.strip()


def github_slug(value: str) -> str:
    value = heading_plain_text(value).lower()
    value = GITHUB_PUNCT_RE.sub("", value)
    return value.replace(" ", "-")


def anchors(path: Path, text: str) -> set[str]:
    counts: defaultdict[str, int] = defaultdict(int)
    result: set[str] = set()
    for _, line in strip_fenced_lines(text):
        match = HEADING_RE.match(line)
        if not match:
            continue
        base = github_slug(match.group(2))
        index = counts[base]
        counts[base] += 1
        result.add(base if index == 0 else f"{base}-{index}")
    return result


def destination_token(raw: str) -> str:
    raw = raw.strip()
    if raw.startswith("<"):
        end = raw.find(">")
        return raw[1:end] if end != -1 else raw[1:]
    # Optional Markdown title follows whitespace. Relative paths in this repository do
    # not contain literal spaces; encoded spaces remain intact.
    return raw.split(maxsplit=1)[0]


def resolve_target(source: Path, destination: str) -> tuple[Path, str]:
    split = urlsplit(destination)
    fragment = unquote(split.fragment)
    relative = unquote(split.path)
    target = source if not relative else (source.parent / relative)
    target = target.resolve()
    try:
        target.relative_to(ROOT.resolve())
    except ValueError as error:
        raise ValueError("relative link escapes repository") from error
    if target.is_dir():
        target = target / "README.md"
    return target, fragment


def main() -> int:
    files = active_markdown_files()
    texts: dict[Path, str] = {}
    heading_sets: dict[Path, set[str]] = {}
    errors: list[str] = []
    link_count = 0

    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
            lines = strip_fenced_lines(text)
            texts[path.resolve()] = text
            heading_sets[path.resolve()] = anchors(path, text)
        except (UnicodeError, OSError, ValueError) as error:
            errors.append(f"{path.relative_to(ROOT)}: {error}")
            continue

        for line_number, line in lines:
            destinations = [match.group(1) for match in LINK_RE.finditer(line)]
            reference = REFERENCE_RE.match(line)
            if reference:
                destinations.append(reference.group(1))
            for raw in destinations:
                destination = destination_token(raw)
                if not destination:
                    continue
                split = urlsplit(destination)
                if split.scheme.lower() in SCHEMES or destination.startswith("//"):
                    continue
                link_count += 1
                try:
                    target, fragment = resolve_target(path, destination)
                except ValueError as error:
                    errors.append(f"{path.relative_to(ROOT)}:{line_number}: {error}: {destination}")
                    continue
                if not target.is_file():
                    errors.append(
                        f"{path.relative_to(ROOT)}:{line_number}: missing target "
                        f"{target.relative_to(ROOT) if target.is_relative_to(ROOT) else target}"
                    )
                    continue
                if fragment:
                    target_key = target.resolve()
                    if target_key not in heading_sets:
                        try:
                            target_text = target.read_text(encoding="utf-8")
                            heading_sets[target_key] = anchors(target, target_text)
                        except (UnicodeError, OSError, ValueError) as error:
                            errors.append(f"{path.relative_to(ROOT)}:{line_number}: {error}")
                            continue
                    if fragment not in heading_sets[target_key]:
                        errors.append(
                            f"{path.relative_to(ROOT)}:{line_number}: missing anchor "
                            f"#{fragment} in {target.relative_to(ROOT)}"
                        )

    if errors:
        print("v2 documentation validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"Validated {len(files)} active Markdown files and {link_count} relative links/anchors.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
