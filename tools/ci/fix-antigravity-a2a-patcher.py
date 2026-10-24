from pathlib import Path

path = Path("tools/ci/apply-antigravity-a2a.py")
text = path.read_text(encoding="utf-8")

old = '''replace_all(
    "crates/swarm-kernel-host/src/runtime/warm_stream.rs",
    \'\'\'            outcome: &outcome,
        })\'\'\',
    \'\'\'            outcome: &outcome,
            allowed_origin_boot_id: None,
        })\'\'\',
    minimum=6,
)
'''
new = '''warm_path = "crates/swarm-kernel-host/src/runtime/warm_stream.rs"
warm_content = read(warm_path)

def add_allowed_origin(match: re.Match[str]) -> str:
    indent = match.group("indent")
    return (
        f"{indent}outcome: &outcome,\\n"
        f"{indent}allowed_origin_boot_id: None,\\n"
        f"{indent}}})"
    )

warm_updated, warm_count = re.subn(
    r"(?P<indent>[ \\t]*)outcome: &outcome,\\n(?P=indent)\\}\\)",
    add_allowed_origin,
    warm_content,
)
if warm_count < 6:
    raise RuntimeError(
        f"{warm_path}: expected at least 6 OutcomeContext fixtures, found {warm_count}"
    )
write(warm_path, warm_updated)
'''
if text.count(old) != 1:
    raise RuntimeError("A2a warm-stream fixture replacement block not found exactly once")
text = text.replace(old, new, 1)

old_guard = '''# Guard the intended no-prompt persistence boundary.
for path in [
    "modules/antigravity-rust/src/store_delivery.rs",
    "modules/antigravity-rust/src/controller.rs",
]:
    content = read(path)
    if "native_payload" in content or "task_prompt" in read("modules/antigravity-rust/src/store_delivery.rs"):
        raise RuntimeError("A2 delivery spool must not persist native prompt intent")
'''
new_guard = '''# Guard the intended no-prompt persistence boundary.
spool_source = read("modules/antigravity-rust/src/store_delivery.rs")
if "native_payload" in spool_source or "task_prompt" in spool_source:
    raise RuntimeError("A2 delivery spool must not persist native prompt intent")
'''
if text.count(old_guard) != 1:
    raise RuntimeError("A2a prompt-persistence guard block not found exactly once")
text = text.replace(old_guard, new_guard, 1)
path.write_text(text, encoding="utf-8", newline="\n")
print("A2a patcher repaired")
