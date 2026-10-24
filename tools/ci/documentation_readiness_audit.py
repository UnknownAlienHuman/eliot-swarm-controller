#!/usr/bin/env python3
"""Generate a factual documentation-readiness inventory.

This does not judge source correctness. It checks whether repository instructions
honestly claim completion, whether entry-point links resolve, and whether an agent can
locate one current work order without reading the historical archive.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs/agent-operations/documentation-readiness-report.md"
INVENTORY = ROOT / "docs/agent-operations/documentation-inventory.json"

LINK_RE = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
SHA_RE = re.compile(r"(?<![0-9a-f])([0-9a-f]{7,40})(?![0-9a-f])")

ENTRYPOINTS = [
    "AGENTS.md",
    "README.md",
    "docs/agent-operations/README.md",
    "docs/agent-operations/architecture.md",
    "docs/agent-operations/implementation.md",
    "docs/agent-operations/audit-current.md",
    "docs/implementation-status.md",
]

ACTIVE_CONTRACTS = [
    "docs/agent-operations/architecture.md",
    "docs/agent-operations/implementation.md",
    "docs/agent-operations/delivery.md",
    "docs/agent-operations/configuration.md",
    "docs/agent-operations/module-api.md",
    "docs/agent-operations/module-installation.md",
    "docs/agent-operations/store-evolution.md",
    "docs/agent-operations/ipc-mcp.md",
    "docs/agent-operations/external-effects.md",
    "docs/agent-operations/github-sources.md",
    "docs/agent-operations/observability.md",
    "docs/agent-operations/modularity.md",
    "docs/agent-operations/shared-packages.md",
    "docs/agent-operations/donor-map.md",
    "docs/agent-operations/refactoring.md",
]

HISTORICAL_PATTERNS = (
    "restart-checkpoint-",
    "master-audit",
    "audit-v",
    "agent_swarm.",
    "documentation-program",
)


@dataclass(frozen=True)
class BrokenLink:
    source: str
    target: str
    resolved: str


def rel(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def markdown_files() -> list[Path]:
    excluded = {"target", ".git", "node_modules", "vendor"}
    files: list[Path] = []
    for path in ROOT.rglob("*.md"):
        if any(part in excluded for part in path.parts):
            continue
        files.append(path)
    return sorted(files)


def clean_target(raw: str) -> str | None:
    target = raw.strip().strip("<>")
    if not target or target.startswith(("#", "http://", "https://", "mailto:")):
        return None
    # Markdown titles are outside the path. The repository uses no quoted local
    # file names in active entry points, so the first whitespace-delimited token
    # is the path when a title is present.
    target = target.split()[0]
    target = unquote(target.split("#", 1)[0].split("?", 1)[0])
    return target or None


def broken_links(files: list[Path]) -> list[BrokenLink]:
    broken: list[BrokenLink] = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        for raw in LINK_RE.findall(text):
            target = clean_target(raw)
            if target is None:
                continue
            resolved = (path.parent / target).resolve()
            try:
                resolved.relative_to(ROOT.resolve())
            except ValueError:
                broken.append(BrokenLink(rel(path), raw, str(resolved)))
                continue
            if not resolved.exists():
                broken.append(BrokenLink(rel(path), raw, rel(resolved)))
    return broken


def status_markers(path: Path) -> dict[str, bool]:
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    upper = text.upper()
    return {
        "source_audit_incomplete": "SOURCE_AUDIT_INCOMPLETE" in upper,
        "working_draft": "WORKING DRAFT" in upper,
        "audit_incomplete_phrase": "AUDIT REMAINS **INCOMPLETE" in upper
        or "SOURCE AUDIT REMAINS **INCOMPLETE" in upper
        or "AUDIT REMAINS INCOMPLETE" in upper,
        "not_exhaustive": "NOT AN EXHAUSTIVE" in upper,
        "resume_here": "RESUME HERE" in upper,
    }


def classify(path: Path) -> str:
    name = rel(path)
    if name in ENTRYPOINTS or name in ACTIVE_CONTRACTS:
        return "active"
    lowered = name.lower()
    if any(pattern in lowered for pattern in HISTORICAL_PATTERNS):
        return "historical-or-snapshot"
    return "supporting-or-unclassified"


def main() -> None:
    files = markdown_files()
    records = []
    for path in files:
        text = path.read_text(encoding="utf-8")
        records.append(
            {
                "path": rel(path),
                "bytes": path.stat().st_size,
                "lines": text.count("\n") + 1,
                "headings": len(HEADING_RE.findall(text)),
                "classification": classify(path),
                "status_markers": status_markers(path),
            }
        )

    broken = broken_links(files)
    explicit_incomplete = [
        item["path"]
        for item in records
        if item["status_markers"]["source_audit_incomplete"]
        or item["status_markers"]["audit_incomplete_phrase"]
        or item["status_markers"]["not_exhaustive"]
    ]
    missing_entrypoints = [path for path in ENTRYPOINTS if not (ROOT / path).exists()]
    missing_contracts = [path for path in ACTIVE_CONTRACTS if not (ROOT / path).exists()]
    oversized = sorted(
        (item for item in records if item["bytes"] >= 25_000),
        key=lambda item: item["bytes"],
        reverse=True,
    )
    root_docs = [item for item in records if item["path"].startswith("docs/") and item["path"].count("/") == 1]
    active = [item for item in records if item["classification"] == "active"]
    historical = [item for item in records if item["classification"] == "historical-or-snapshot"]
    unclassified = [item for item in records if item["classification"] == "supporting-or-unclassified"]

    head = os.environ.get("GITHUB_SHA", "unknown")
    verdict = "NOT READY"
    reasons = []
    if explicit_incomplete:
        reasons.append("current source-of-truth documents explicitly declare the source audit incomplete or non-exhaustive")
    if broken:
        reasons.append("repository-local Markdown links are broken")
    if missing_entrypoints or missing_contracts:
        reasons.append("required current entry points/contracts are missing")
    if len(unclassified) > len(active):
        reasons.append("most Markdown files are not mechanically classified as active or historical")
    if not reasons:
        verdict = "STRUCTURALLY READY; SOURCE COMPLETENESS STILL REQUIRES HUMAN EVIDENCE REVIEW"

    inventory = {
        "schema_version": 1,
        "inspected_head": head,
        "verdict": verdict,
        "reasons": reasons,
        "counts": {
            "markdown_files": len(records),
            "docs_root_files": len(root_docs),
            "active": len(active),
            "historical_or_snapshot": len(historical),
            "supporting_or_unclassified": len(unclassified),
            "broken_local_links": len(broken),
            "oversized_25k_or_more": len(oversized),
        },
        "missing_entrypoints": missing_entrypoints,
        "missing_active_contracts": missing_contracts,
        "explicit_incomplete_documents": explicit_incomplete,
        "broken_links": [item.__dict__ for item in broken],
        "oversized_documents": oversized,
        "documents": records,
    }
    INVENTORY.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Documentation readiness audit",
        "",
        f"Inspected repository head: `{head}`.",
        "",
        f"## Verdict: {verdict}",
        "",
    ]
    if reasons:
        lines.extend(f"- {reason}." for reason in reasons)
    else:
        lines.append("- Entry points, active contracts and local links are structurally coherent.")
    lines.extend(
        [
            "",
            "This verdict concerns whether an implementation agent can safely treat the documentation as complete. It does not convert source reading, design text or a compiler pass into runtime qualification.",
            "",
            "## Inventory",
            "",
            "| Measure | Count |",
            "| --- | ---: |",
            f"| Markdown files | {len(records)} |",
            f"| Files directly under `docs/` | {len(root_docs)} |",
            f"| Explicit active entry points/contracts | {len(active)} |",
            f"| Recognizable historical/snapshot files | {len(historical)} |",
            f"| Supporting or unclassified files | {len(unclassified)} |",
            f"| Broken repository-local Markdown links | {len(broken)} |",
            f"| Documents at least 25 KiB | {len(oversized)} |",
            "",
            "## Explicit incompleteness evidence",
            "",
        ]
    )
    if explicit_incomplete:
        lines.extend(f"- `{path}`" for path in explicit_incomplete)
    else:
        lines.append("- None detected by the closed marker set.")

    lines.extend(["", "## Broken local links", ""])
    if broken:
        lines.extend(
            f"- `{item.source}` → `{item.target}` (resolved as `{item.resolved}`)"
            for item in broken
        )
    else:
        lines.append("- None detected.")

    lines.extend(["", "## Largest documentation surfaces", "", "| File | Bytes | Lines | Classification |", "| --- | ---: | ---: | --- |"])
    for item in oversized[:30]:
        lines.append(
            f"| `{item['path']}` | {item['bytes']} | {item['lines']} | {item['classification']} |"
        )

    lines.extend(
        [
            "",
            "## Required closure before a whole-project implementation claim",
            "",
            "1. Finish the source audit at a named revision and record every inspected domain, not only the latest runtime slice.",
            "2. Map every confirmed historical failure to one current invariant, source owner, implementation slice and executable acceptance case; unresolved items stay explicitly open.",
            "3. Freeze one compact agent entry path and classify every other document as active contract, evidence, historical snapshot or archive.",
            "4. Replace repository-wide A–V prose as worker input with generated narrow task packets naming exact current symbols, deletions, positive behavior, uncertainty and minimum checks.",
            "5. Reconcile current status, audit resume point, implementation order and branch/PR state in one commit; no document may call unmerged code landed.",
            "6. Run the documentation audit and link check after every source-of-truth change.",
            "",
            "## What this audit does not prove",
            "",
            "- It does not prove that all code was read.",
            "- It does not prove that historical requirements are complete.",
            "- It does not prove runtime, crash, restart, provider, migration, security or load behavior.",
            "- It does not authorize implementation from an unclassified or historical document.",
            "",
            "Machine-readable inventory: [`documentation-inventory.json`](documentation-inventory.json).",
            "",
        ]
    )
    REPORT.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
