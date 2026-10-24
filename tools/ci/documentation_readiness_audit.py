#!/usr/bin/env python3
"""Generate a reproducible documentation-readiness inventory.

The audit checks navigation, local links, authority classification and explicit
completeness claims. It does not substitute for source or runtime qualification.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs/agent-operations/documentation-readiness-report.md"
INVENTORY = ROOT / "docs/agent-operations/documentation-inventory.json"
GENERATOR = ROOT / "tools/ci/documentation_readiness_audit.py"

LINK_RE = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)

ENTRYPOINTS = {
    "AGENTS.md",
    "README.md",
    "docs/agent-operations/README.md",
}
CURRENT_STATUS = {"docs/implementation-status.md"}
CURRENT_CONTRACTS = {
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
    "docs/agent-operations/documentation-map.md",
    "docs/agent-operations/task-packet-template.md",
    "docs/agent-operations/failure-prevention-register.md",
    "docs/agent-operations/source-audit-coverage.md",
    "docs/swarm-launcher-assignment-context.md",
    "docs/check-runner.md",
    "docs/script-runner.md",
    "docs/agent-communication-program.md",
    "docs/gateway.md",
    "docs/mcp-canonical-surfaces-and-topologies.md",
    "docs/mcp-tool-catalog-and-loading.md",
    "docs/task-policy.md",
}
AUDIT_EVIDENCE_EXACT = {
    "docs/agent-operations/audit-current.md",
    "docs/agent-operations/audit-index.md",
    "docs/agent-operations/audit-register.md",
    "docs/agent-operations/audit-entrypoints-public-surfaces-20261024.md",
    "docs/lessons-learned.md",
}
GENERATED_AUDIT = {
    "docs/agent-operations/source-surface-inventory.md",
}
HISTORICAL_PREFIXES = (
    "docs/agent_swarm",
    "docs/documentation-program",
    "docs/legacy-",
)
QUALIFICATION_PREFIXES = (
    "docs/qualification-",
    "docs/restart-checkpoint-",
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
    return sorted(
        path
        for path in ROOT.rglob("*.md")
        if path != REPORT and not any(part in excluded for part in path.parts)
    )


def input_digest(files: list[Path]) -> str:
    digest = hashlib.sha256()
    for path in [*files, GENERATOR]:
        encoded = rel(path).encode()
        data = path.read_bytes()
        digest.update(len(encoded).to_bytes(4, "big"))
        digest.update(encoded)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def clean_target(raw: str) -> str | None:
    target = raw.strip().strip("<>")
    if not target or target.startswith(("#", "http://", "https://", "mailto:")):
        return None
    target = target.split()[0]
    target = unquote(target.split("#", 1)[0].split("?", 1)[0])
    return target or None


def broken_links(files: list[Path]) -> list[BrokenLink]:
    broken: list[BrokenLink] = []
    root = ROOT.resolve()
    for path in files:
        text = path.read_text(encoding="utf-8")
        for raw in LINK_RE.findall(text):
            target = clean_target(raw)
            if target is None:
                continue
            resolved = (path.parent / target).resolve()
            try:
                resolved.relative_to(root)
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
    lower = name.lower()
    if name in ENTRYPOINTS:
        return "entrypoint"
    if name in CURRENT_STATUS:
        return "current-status"
    if name in CURRENT_CONTRACTS:
        return "current-contract"
    if name in GENERATED_AUDIT:
        return "generated-audit"
    if name in AUDIT_EVIDENCE_EXACT or name.startswith("docs/remediation/"):
        return "audit-evidence"
    if (
        name.startswith(QUALIFICATION_PREFIXES)
        or "qualification" in lower
        or "-evidence" in lower
    ):
        return "qualification-evidence"
    if (
        name.startswith(HISTORICAL_PREFIXES)
        or "master-audit" in lower
        or "audit-v" in lower
    ):
        return "historical-snapshot"
    if (
        (name.startswith("modules/") or name.startswith("crates/"))
        and name.endswith("/README.md")
    ):
        return "module-guide"
    if (
        lower.endswith("-sources.md")
        or "donor" in lower
        or name.startswith("docs/local-models/")
    ):
        return "supporting-reference"
    return "supporting-reference"


def main() -> None:
    files = markdown_files()
    documentation_sha256 = input_digest(files)
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
    required = sorted(ENTRYPOINTS | CURRENT_STATUS | CURRENT_CONTRACTS)
    missing_required = [path for path in required if not (ROOT / path).exists()]
    oversized = sorted(
        (item for item in records if item["bytes"] >= 25_000),
        key=lambda item: item["bytes"],
        reverse=True,
    )
    root_docs = [
        item
        for item in records
        if item["path"].startswith("docs/") and item["path"].count("/") == 1
    ]
    counts = Counter(item["classification"] for item in records)

    reasons: list[str] = []
    if explicit_incomplete:
        reasons.append(
            "current source-of-truth documents explicitly declare the source audit incomplete or non-exhaustive"
        )
    if broken:
        reasons.append("repository-local Markdown links are broken")
    if missing_required:
        reasons.append("required entry points/contracts are missing")
    verdict = (
        "NOT READY"
        if reasons
        else "STRUCTURALLY READY; SOURCE COMPLETENESS STILL REQUIRES EVIDENCE REVIEW"
    )

    inventory = {
        "schema_version": 3,
        "documentation_sha256": documentation_sha256,
        "verdict": verdict,
        "reasons": reasons,
        "counts": {
            "markdown_inputs": len(records),
            "docs_root_files": len(root_docs),
            **dict(sorted(counts.items())),
            "broken_local_links": len(broken),
            "oversized_25k_or_more": len(oversized),
        },
        "missing_required_documents": missing_required,
        "explicit_incomplete_documents": explicit_incomplete,
        "broken_links": [item.__dict__ for item in broken],
        "oversized_documents": oversized,
        "documents": records,
    }
    INVENTORY.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")

    lines = [
        "# Documentation readiness audit",
        "",
        f"Documentation input SHA-256: `{documentation_sha256}`.",
        "",
        f"## Verdict: {verdict}",
        "",
    ]
    if reasons:
        lines.extend(f"- {reason}." for reason in reasons)
    else:
        lines.append(
            "- Entry points, authority classes and local links are structurally coherent."
        )
    lines.extend(
        [
            "",
            "This verdict concerns whether an implementation agent can safely treat the documentation as complete. It does not convert source reading, design text or a compiler pass into runtime qualification.",
            "",
            "## Inventory",
            "",
            "| Measure | Count |",
            "| --- | ---: |",
            f"| Markdown inputs (generated report excluded) | {len(records)} |",
            f"| Files directly under `docs/` | {len(root_docs)} |",
        ]
    )
    for category, count in sorted(counts.items()):
        lines.append(f"| `{category}` | {count} |")
    lines.extend(
        [
            f"| Broken repository-local Markdown links | {len(broken)} |",
            f"| Documents at least 25 KiB | {len(oversized)} |",
            "",
            "Classification follows [`documentation-map.md`](documentation-map.md). `supporting-reference` is an explicit non-authoritative class, not an unreviewed worker instruction.",
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

    lines.extend(
        [
            "",
            "## Largest documentation surfaces",
            "",
            "| File | Bytes | Lines | Classification |",
            "| --- | ---: | ---: | --- |",
        ]
    )
    for item in oversized[:30]:
        lines.append(
            f"| `{item['path']}` | {item['bytes']} | {item['lines']} | {item['classification']} |"
        )

    lines.extend(
        [
            "",
            "## Required closure before a whole-project implementation claim",
            "",
            "1. Finish the source audit at a named revision and record every package/domain and non-workspace surface, not only the latest runtime slice.",
            "2. Map every confirmed historical failure to one current invariant, source owner, implementation slice and executable acceptance case; unresolved items stay explicitly open.",
            "3. Keep one compact agent entry path; every other document remains a classified contract, status, evidence, module guide, historical snapshot or supporting reference.",
            "4. Assign work through narrow task packets naming exact current symbols, deletions/data disposition, positive behavior, uncertainty and minimum checks.",
            "5. Reconcile current status, audit resume point, implementation order and branch/PR state in one commit; no document may call unmerged code landed.",
            "6. Run this audit after every source-of-truth documentation change.",
            "",
            "## What this audit does not prove",
            "",
            "- It does not prove that all code was read.",
            "- It does not prove that historical requirements are complete.",
            "- It does not prove runtime, crash, restart, provider, migration, security or load behavior.",
            "- It does not authorize implementation from evidence, historical snapshots or supporting references.",
            "",
            "Machine-readable inventory: [`documentation-inventory.json`](documentation-inventory.json).",
            "",
        ]
    )
    REPORT.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
