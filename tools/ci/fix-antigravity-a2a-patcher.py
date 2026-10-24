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

# Align the generated retained-body record with the actual stream DTO:
# RetainedResponseBody { status, response_sha256, body }.
for old_piece, new_piece in [
    (
        '''    origin_boot_id: String,
    module_receipt: ModuleReceiptIdentity,
''',
        '''    origin_boot_id: String,
    status: String,
    module_receipt: ModuleReceiptIdentity,
''',
    ),
    (
        '''            || !bounded_identity(&self.native_conversation_id, 512)
            || self.result_ordinal == 0
''',
        '''            || !bounded_identity(&self.native_conversation_id, 512)
            || !bounded_identity(&self.status, 64)
            || self.result_ordinal == 0
''',
    ),
    (
        '''        let response_bytes = u64::try_from(body.response_bytes)
            .map_err(|_| Error::invalid("retained response length is out of range"))?;
''',
        '''        let response_bytes = u64::try_from(body.body.len())
            .map_err(|_| Error::invalid("retained response length is out of range"))?;
''',
    ),
    (
        '''            origin_boot_id: origin_boot_id.to_owned(),
            module_receipt: receipt.clone(),
''',
        '''            origin_boot_id: origin_boot_id.to_owned(),
            status: body.status.clone(),
            module_receipt: receipt.clone(),
''',
    ),
    (
        '''            response: body.response.clone(),
''',
        '''            response: body.body.clone(),
''',
    ),
    (
        '''        Ok(Some(RetainedResponseBody {
            response: record.response,
            response_sha256: record.response_sha256,
            response_bytes: usize::try_from(record.response_bytes)
                .map_err(|_| Error::invalid("retained response length is out of range"))?,
        }))
''',
        '''        Ok(Some(RetainedResponseBody {
            status: record.status,
            response_sha256: record.response_sha256,
            body: record.response,
        }))
''',
    ),
]:
    if text.count(old_piece) != 1:
        raise RuntimeError(f"A2a retained-body patch fragment not found exactly once: {old_piece[:48]!r}")
    text = text.replace(old_piece, new_piece, 1)

controller_fixes = '''
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    "use serde_json::json;\\n",
    "use serde_json::{Value, json};\\n",
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    "            let pending = self.pending.as_ref()?;\\n",
    "            let Some(pending) = self.pending.as_ref() else {\\n                return Ok(None);\\n            };\\n",
)
replace_once(
    "modules/antigravity-rust/src/controller.rs",
    "        let stream_generation = self.pending.as_ref()?.stream_generation;\\n",
    "        let Some(stream_generation) = self.pending.as_ref().map(|pending| pending.stream_generation) else {\\n            return Ok(None);\\n        };\\n",
)

'''
marker = "# Main production wiring.\n"
if text.count(marker) != 1:
    raise RuntimeError("A2a main-wiring marker not found exactly once")
text = text.replace(marker, controller_fixes + marker, 1)

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
