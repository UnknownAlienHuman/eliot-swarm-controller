from pathlib import Path

path = Path("crates/swarm-kernel-host/src/store/coordination.rs")
text = path.read_text(encoding="utf-8")

anchor = '''fn list_participant_page(
    db: &Connection,
    scope: &ScopeData,
    limit: i64,
    after_client_id: Option<&str>,
) -> Result<Value> {
'''
replacement = '''fn participant_index_client_id(prefix: &str, key: &str) -> Result<String> {
    let encoded = key
        .strip_prefix(prefix)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            Error::new(
                "STORE_INVARIANT",
                "participant index key is outside its retained scope prefix",
            )
        })?;
    if encoded.len() % 2 != 0 {
        return Err(Error::new(
            "STORE_INVARIANT",
            "participant index key has an invalid encoded client identity",
        ));
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0]).ok_or_else(|| {
            Error::new(
                "STORE_INVARIANT",
                "participant index key contains non-canonical client identity bytes",
            )
        })?;
        let low = hex_nibble(pair[1]).ok_or_else(|| {
            Error::new(
                "STORE_INVARIANT",
                "participant index key contains non-canonical client identity bytes",
            )
        })?;
        bytes.push((high << 4) | low);
    }
    let client_id = String::from_utf8(bytes).map_err(|_| {
        Error::new(
            "STORE_INVARIANT",
            "participant index key does not contain a UTF-8 client identity",
        )
    })?;
    if client_id.is_empty()
        || client_id.len() > 256
        || keys::key_component(&client_id) != encoded
    {
        return Err(Error::new(
            "STORE_INVARIANT",
            "participant index key does not round-trip to a valid page cursor",
        ));
    }
    Ok(client_id)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn participant_page_stale(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "NOT_FOUND"
            | "FORBIDDEN"
            | "UNAUTHORIZED"
            | "STALE_PARTICIPANT"
            | "PARTICIPANT_NOT_ASSIGNED"
            | "STALE_REVIEW_ASSIGNMENT"
    )
}

fn list_participant_page(
    db: &Connection,
    scope: &ScopeData,
    limit: i64,
    after_client_id: Option<&str>,
) -> Result<Value> {
'''
count = text.count(anchor)
if count != 1:
    raise SystemExit(f"participant page anchor count={count}")
text = text.replace(anchor, replacement)

old_loop = '''    for (key, raw) in rows.iter().take(scan_limit as usize) {
        let record: Value = serde_json::from_str(raw)?;
        let Some(client_id) = record.get("client_id").and_then(Value::as_str) else {
            stale = stale.saturating_add(1);
            continue;
        };
        if let Some(before) = last_scanned.as_ref() {
            cursor_before_extra = Some(before.clone());
        }
        last_scanned = Some(client_id.to_owned());
        match load_current_scope_for_client(db, client_id) {
            Ok(candidate) if candidate.scope_id == scope.scope_id => {
                let item = json!({
                    "client_id": client_id,
                    "participant": public_registration(&candidate.registration),
                });
                if items.len() < limit as usize {
                    items.push(item);
                } else {
                    more_active = true;
                    // Resume before the first unreturned active participant.
                    last_scanned = cursor_before_extra;
                    break;
                }
            }
            _ => stale = stale.saturating_add(1),
        }
        let _ = key;
    }
    let partial = more_active || has_unscanned || stale > 0;
    Ok(json!({
        "items": items,
        "task_id": scope.task["task_id"],
        "task_revision": scope.task["revision"],
        "attempt_id": scope.attempt["attempt_id"],
        "next_after": if partial { last_scanned } else { None },
        "coverage": if partial { "partial" } else { "complete" },
        "gaps": if stale > 0 { json!([{"kind":"stale_participant_index_entries","count":stale}]) } else if has_unscanned { json!([{"kind":"participant_page_scan_bound","count":null}]) } else { json!([]) },
    }))
'''
new_loop = '''    for (key, raw) in rows.iter().take(scan_limit as usize) {
        // The index key is the ordered fact. Advance the scan cursor before
        // interpreting its derived payload so one stale row cannot wedge the page.
        let client_id = participant_index_client_id(&prefix, key)?;
        if let Some(before) = last_scanned.as_ref() {
            cursor_before_extra = Some(before.clone());
        }
        last_scanned = Some(client_id.clone());

        let record: Value = match serde_json::from_str(raw) {
            Ok(record) => record,
            Err(_) => {
                stale = stale.saturating_add(1);
                continue;
            }
        };
        if record.get("client_id").and_then(Value::as_str) != Some(client_id.as_str()) {
            stale = stale.saturating_add(1);
            continue;
        }
        match load_current_scope_for_client(db, &client_id) {
            Ok(candidate) if candidate.scope_id == scope.scope_id => {
                let item = json!({
                    "client_id": client_id,
                    "participant": public_registration(&candidate.registration),
                });
                if items.len() < limit as usize {
                    items.push(item);
                } else {
                    more_active = true;
                    // Resume before the first unreturned active participant.
                    last_scanned = cursor_before_extra;
                    break;
                }
            }
            Ok(_) => stale = stale.saturating_add(1),
            Err(error) if participant_page_stale(&error) => {
                stale = stale.saturating_add(1);
            }
            Err(error) => return Err(error),
        }
    }
    let partial = more_active || has_unscanned || stale > 0;
    let mut gaps = Vec::new();
    if stale > 0 {
        gaps.push(json!({"kind":"stale_participant_index_entries","count":stale}));
    }
    if has_unscanned {
        gaps.push(json!({"kind":"participant_page_scan_bound","count":Value::Null}));
    }
    Ok(json!({
        "items": items,
        "task_id": scope.task["task_id"],
        "task_revision": scope.task["revision"],
        "attempt_id": scope.attempt["attempt_id"],
        "next_after": if partial { last_scanned } else { None },
        "coverage": if partial { "partial" } else { "complete" },
        "gaps": gaps,
    }))
'''
count = text.count(old_loop)
if count != 1:
    raise SystemExit(f"participant page loop anchor count={count}")
text = text.replace(old_loop, new_loop)
path.write_text(text, encoding="utf-8")
