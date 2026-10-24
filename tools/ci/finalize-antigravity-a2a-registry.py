from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def replace_once(path: str, old: str, new: str) -> None:
    target = ROOT / path
    text = target.read_text(encoding="utf-8")
    if text.count(old) != 1:
        raise RuntimeError(f"{path}: expected one match for {old[:60]!r}")
    target.write_text(text.replace(old, new, 1), encoding="utf-8", newline="\n")


replace_once(
    "docs/implementation-status.md",
    "A2a  IMPLEMENTED / CHECKING    private outcome/body/page retention + reconciled old boot",
    "A2a  PR READY / SCOPED PASS    private outcome/body/page retention + reconciled old boot",
)

replace_once(
    "docs/agent-operations/implementation.md",
    "**A2a — implemented in this branch, qualification pending:** retain exact",
    "**A2a — implemented in the PR branch, scoped Clippy passed:** retain exact",
)
replace_once(
    "docs/agent-operations/implementation.md",
    "intent is never persisted or replayed.\n\n**A2b:**",
    "intent is never persisted or replayed. Runs `40135475910` (job `120995970779`) "
    "and `40135726768` (job `120996711166`) passed affected rustfmt and "
    "warnings-denied Clippy. No provider, installed-harness, restart/crash or load "
    "qualification is claimed.\n\n**A2b:**",
)

replace_once(
    "docs/agent-operations/audit-current.md",
    "  delivery spool or replay input.\n- **Next exact chain — A2b:**",
    "  delivery spool or replay input. Runs `40135475910` / job `120995970779` and "
    "`40135726768` / job `120996711166` passed affected rustfmt and warnings-denied "
    "Clippy; no runtime/installed/crash qualification is claimed.\n- **Next exact chain — A2b:**",
)

replace_once(
    "modules/antigravity-rust/README.md",
    "release remain A2b. Installed/native qualification remains pending.",
    "release remain A2b. Runs `40135475910` / job `120995970779` and "
    "`40135726768` / job `120996711166` passed affected rustfmt and warnings-denied "
    "Clippy; installed/native qualification remains pending.",
)

print("A2a registry finalized")
