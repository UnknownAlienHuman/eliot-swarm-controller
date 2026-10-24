from pathlib import Path

root = Path(__file__).resolve().parents[2]
status_path = root / "docs/implementation-status.md"
status = status_path.read_text(encoding="utf-8")
old = "| Terminal receipt decoupled from Store observation | `eeea355`, recorded net −265 production Rust lines | Dispatch proof and crash-unsafe delivery remain. |"
new = "| Terminal receipt decoupled from Store observation | `eeea355`, recorded net −265 production Rust lines | Crash-unsafe delivery remains; A1b removed the dispatch proof gate. |"
if status.count(old) != 1:
    raise RuntimeError("implementation status A1a row did not match exactly once")
status = status.replace(old, new, 1)
status = status.replace(
    "A1b  LANDED / SCOPED PASS      refresh/proof gate removed; current fences retained",
    "A1b  LANDED / SCOPED PASS     refresh/proof gate removed; current fences retained",
    1,
)
status_path.write_text(status, encoding="utf-8", newline="\n")

checks = {
    "docs/implementation-status.md": [
        "A1b  NEXT / NOT IMPLEMENTED",
        "Dispatch proof and crash-unsafe delivery remain",
        "**Runtime A1b:** remove",
    ],
    "docs/agent-operations/audit-current.md": [
        "require_current_session_proof",
        "controller::{read_command,dispatch_session_proof}",
    ],
    "docs/agent-operations/implementation.md": ["**A1b:** delete"],
    "docs/agent-operations/refactoring.md": [
        "Still open: Antigravity dispatch proof/refresh gate"
    ],
    "modules/antigravity-rust/README.md": [
        "uses a v2 current-session proof",
        "current observation ID",
    ],
}
for relative, forbidden in checks.items():
    text = (root / relative).read_text(encoding="utf-8")
    stale = [phrase for phrase in forbidden if phrase in text]
    if stale:
        raise RuntimeError(f"{relative}: stale active A1b text: {stale}")

print("A1b active documentation is consistent")
