from pathlib import Path

path = Path("crates/swarm-kernel-host/src/store/coordination.rs")
text = path.read_text(encoding="utf-8")

anchor = '''fn load_current_scope_for_client(db: &Connection, client_id: &str) -> Result<ScopeData> {
'''
helper = '''fn participant_scope_lookup_error(error: Error, missing_message: &'static str) -> Error {
    if error.code == "NOT_FOUND" {
        Error::new("STALE_PARTICIPANT", missing_message)
    } else {
        // Database, decoding and invariant failures are not evidence that the
        // participant merely became stale. Preserve the original failure.
        error
    }
}

fn load_current_scope_for_client(db: &Connection, client_id: &str) -> Result<ScopeData> {
'''
count = text.count(anchor)
if count != 1:
    raise SystemExit(f"scope helper anchor count={count}")
text = text.replace(anchor, helper)

old_task = '''    let task = tasks::get_task(db, task_id)
        .map_err(|_| Error::new("STALE_PARTICIPANT", "participant Task no longer exists"))?;
'''
new_task = '''    let task = tasks::get_task(db, task_id).map_err(|error| {
        participant_scope_lookup_error(error, "participant Task no longer exists")
    })?;
'''
count = text.count(old_task)
if count != 1:
    raise SystemExit(f"task error mapping anchor count={count}")
text = text.replace(old_task, new_task)

old_attempt = '''    let attempt = tasks::get_attempt(db, attempt_id)
        .map_err(|_| Error::new("STALE_PARTICIPANT", "participant Attempt no longer exists"))?;
'''
new_attempt = '''    let attempt = tasks::get_attempt(db, attempt_id).map_err(|error| {
        participant_scope_lookup_error(error, "participant Attempt no longer exists")
    })?;
'''
count = text.count(old_attempt)
if count != 1:
    raise SystemExit(f"attempt error mapping anchor count={count}")
text = text.replace(old_attempt, new_attempt)

path.write_text(text, encoding="utf-8")
