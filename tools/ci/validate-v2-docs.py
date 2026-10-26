#!/usr/bin/env python3
"""Validate active v2 Markdown links and GitHub-style heading anchors."""

from __future__ import annotations

import html
import re
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]
EXCLUDED = {ROOT / "docs" / "owner-decisions.md"}
ALLOWED_EXTERNAL_SCHEMES = {"http", "https", "mailto"}
OPEN_FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
HEADING_RE = re.compile(r"^ {0,3}(#{1,6})\s+(.+?)\s*#*\s*$")
MARKDOWN_LINK_RE = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
REFERENCE_RE = re.compile(r"^ {0,3}\[[^\]]+\]:\s*(\S+)")
HTML_DEST_RE = re.compile(r"\b(?:href|src)\s*=\s*([\"'])(.*?)\1", re.IGNORECASE)
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
        if fence_char is None:
            match = OPEN_FENCE_RE.match(line)
            if match:
                marker = match.group(1)
                fence_char = marker[0]
                fence_len = len(marker)
                continue
            result.append((number, line))
            continue

        close_re = re.compile(rf"^ {{0,3}}{re.escape(fence_char)}{{{fence_len},}}[ \t]*$")
        if close_re.match(line):
            fence_char = None
            fence_len = 0

    if fence_char is not None:
        raise ValueError("unclosed fenced code block")
    return result


def strip_inline_code(line: str) -> str:
    """Replace complete backtick spans so link-looking code is ignored."""
    chars = list(line)
    index = 0
    while index < len(chars):
        if chars[index] != "`":
            index += 1
            continue
        end_run = index
        while end_run < len(chars) and chars[end_run] == "`":
            end_run += 1
        marker = "`" * (end_run - index)
        closing = line.find(marker, end_run)
        if closing == -1:
            index = end_run
            continue
        for position in range(index, closing + len(marker)):
            chars[position] = " "
        index = closing + len(marker)
    return "".join(chars)


def heading_plain_text(value: str) -> str:
    value = html.unescape(value)
    value = MARKDOWN_LINK_TEXT_RE.sub(r"\1", value)
    value = HTML_TAG_RE.sub("", value)
    return value.replace("*", "").strip()


def github_slug(value: str) -> str:
    value = GITHUB_PUNCT_RE.sub("", heading_plain_text(value).lower())
    return value.replace(" ", "-")


def anchors(path: Path, text: str) -> set[str]:
    del path
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
        if end == -1:
            raise ValueError("unterminated angle-bracket destination")
        return raw[1:end]
    return raw.split(maxsplit=1)[0]


def validate_destination_syntax(destination: str) -> None:
    if "\x00" in destination or any(ord(char) < 0x20 for char in destination):
        raise ValueError("control character in destination")
    if destination.startswith("//"):
        raise ValueError("protocol-relative URL is forbidden; use explicit https://")

    split = urlsplit(destination)
    if split.netloc and not split.scheme:
        raise ValueError("network location without an explicit allowed scheme")

    scheme = split.scheme.lower()
    if scheme and scheme not in ALLOWED_EXTERNAL_SCHEMES:
        raise ValueError(f"unsupported or unsafe URI scheme '{scheme}'")

    if not scheme:
        decoded_path = unquote(split.path)
        if "\\" in decoded_path:
            raise ValueError("repository-relative links must use '/' separators")
        if decoded_path.startswith("~"):
            raise ValueError("ambiguous home-relative destination")


def resolve_target(source: Path, destination: str) -> tuple[Path, str]:
    split = urlsplit(destination)
    fragment = unquote(split.fragment)
    relative = unquote(split.path)
    target = source if not relative else source.parent / relative
    target = target.resolve()
    try:
        target.relative_to(ROOT.resolve())
    except ValueError as error:
        raise ValueError("relative link escapes repository") from error
    if target.is_dir():
        target = target / "README.md"
    return target, fragment


def line_destinations(line: str) -> list[str]:
    visible = strip_inline_code(line)
    destinations = [match.group(1) for match in MARKDOWN_LINK_RE.finditer(visible)]
    destinations.extend(match.group(2) for match in HTML_DEST_RE.finditer(visible))
    reference = REFERENCE_RE.match(visible)
    if reference:
        destinations.append(reference.group(1))
    return destinations


def main() -> int:
    files = active_markdown_files()
    heading_sets: dict[Path, set[str]] = {}
    errors: list[str] = []
    local_link_count = 0

    for path in files:
        try:
            text = path.read_text(encoding="utf-8")
            lines = strip_fenced_lines(text)
            heading_sets[path.resolve()] = anchors(path, text)
        except (UnicodeError, OSError, ValueError) as error:
            errors.append(f"{path.relative_to(ROOT)}: {error}")
            continue

        for line_number, line in lines:
            for raw in line_destinations(line):
                try:
                    destination = destination_token(raw)
                    if not destination:
                        continue
                    validate_destination_syntax(destination)
                except ValueError as error:
                    errors.append(f"{path.relative_to(ROOT)}:{line_number}: {error}")
                    continue

                split = urlsplit(destination)
                if split.scheme:
                    continue

                local_link_count += 1
                try:
                    target, fragment = resolve_target(path, destination)
                except ValueError as error:
                    errors.append(
                        f"{path.relative_to(ROOT)}:{line_number}: {error}: {destination}"
                    )
                    continue

                if not target.is_file():
                    shown = target.relative_to(ROOT) if target.is_relative_to(ROOT) else target
                    errors.append(
                        f"{path.relative_to(ROOT)}:{line_number}: missing target {shown}"
                    )
                    continue

                if not fragment:
                    continue
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

    print(
        f"Validated {len(files)} active Markdown files and "
        f"{local_link_count} local links/anchors."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
