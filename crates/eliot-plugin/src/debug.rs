//! Debug-mode telemetry for plugin-side native activity.
//!
//! Metadata-only records for plugin<->native frames and harness process
//! start/exit: method, direction, sizes, latency, and turn/command/agent
//! envelope IDs when the typed params carry them. Never frame content,
//! argv values, prompts, output, or environment: the type signature only
//! admits short ids and nonnegative numeric fields.
//!
//! Records are single `ELIOT_DEBUG {json}` lines on stderr. The daemon
//! strips them from retained stderr tails and routes them to the daemon
//! log in debug mode; in standard mode they are diagnostic loss only.

/// One parsed debug line, after the `ELIOT_DEBUG ` prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DebugRecord {
    pub code: String,
    pub ids: Vec<(String, String)>,
    pub fields: Vec<(String, u64)>,
}

/// Parse one `ELIOT_DEBUG {json}` payload. Returns None for malformed
/// lines: a broken telemetry line never becomes evidence.
pub fn parse_debug_line(line: &str) -> Option<DebugRecord> {
    let payload = line.strip_prefix("ELIOT_DEBUG ")?;
    if payload.len() > 4096 {
        return None;
    }
    let value: serde_json::Value = serde_json::from_str(payload.trim_end()).ok()?;
    let code = value.get("code")?.as_str()?.to_owned();
    if code.len() > 64
        || !code
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_')
    {
        return None;
    }
    let mut ids = Vec::new();
    if let Some(object) = value.get("ids").and_then(|v| v.as_object()) {
        for (key, val) in object.iter().take(8) {
            let Some(val) = val.as_str() else {
                continue;
            };
            if key.len() > 64 || val.len() > 256 {
                continue;
            }
            if !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                continue;
            }
            ids.push((key.clone(), val.to_owned()));
        }
    }
    let mut fields = Vec::new();
    if let Some(object) = value.get("fields").and_then(|v| v.as_object()) {
        for (key, val) in object.iter().take(8) {
            let (Some(val), true) = (val.as_u64(), key.len() <= 64) else {
                continue;
            };
            if !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                continue;
            }
            fields.push((key.clone(), val));
        }
    }
    Some(DebugRecord { code, ids, fields })
}

/// Bounded envelope IDs from typed JSON-RPC params: our own routing
/// metadata (`turn_id`, `command_id`, `agent_id`), never params content.
/// Values are short strings only; anything else is skipped.
pub fn envelope_ids(params: &serde_json::Value) -> Vec<(&'static str, String)> {
    let mut ids = Vec::new();
    for key in ["turn_id", "command_id", "agent_id"] {
        if let Some(value) = params.get(key).and_then(|v| v.as_str())
            && !value.is_empty()
            && value.len() <= 128
        {
            ids.push((key, value.to_owned()));
        }
    }
    ids
}

/// Emit one metadata-only debug record. Bounds ids/fields at the same
/// vocabulary the daemon log enforces; oversized values are dropped, never
/// truncated mid-secret (these fields never hold secrets).
pub fn record(code: &'static str, ids: &[(&str, &str)], fields: &[(&str, u64)]) {
    let mut map = serde_json::Map::new();
    map.insert("code".to_owned(), serde_json::Value::from(code));
    let mut id_map = serde_json::Map::new();
    for (key, value) in ids.iter().take(8) {
        if key.len() > 64
            || value.len() > 256
            || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            continue;
        }
        id_map.insert((*key).to_owned(), serde_json::Value::from(*value));
    }
    map.insert("ids".to_owned(), serde_json::Value::Object(id_map));
    let mut field_map = serde_json::Map::new();
    for (key, value) in fields.iter().take(8) {
        if key.len() > 64 || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
            continue;
        }
        field_map.insert((*key).to_owned(), serde_json::Value::from(*value));
    }
    map.insert("fields".to_owned(), serde_json::Value::Object(field_map));
    if let Ok(line) = serde_json::to_string(&serde_json::Value::Object(map))
        && line.len() <= 4096
    {
        eprintln!("ELIOT_DEBUG {line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_reject_malformed() {
        let mut line = String::from("ELIOT_DEBUG ");
        line.push_str(r#"{"code":"plugin.frame","ids":{"method":"session\/create"},"fields":{"elapsed_ms":3}}"#);
        let record = parse_debug_line(&line).expect("parses");
        assert_eq!(record.code, "plugin.frame");
        assert_eq!(
            record.ids,
            vec![("method".to_owned(), "session/create".to_owned())]
        );
        assert_eq!(record.fields, vec![("elapsed_ms".to_owned(), 3)]);
        assert!(parse_debug_line("ELIOT_DEBUG not json").is_none());
        assert!(parse_debug_line("other prefix").is_none());
        assert!(parse_debug_line(&format!("ELIOT_DEBUG {}", "x".repeat(5000))).is_none());
    }

    #[test]
    fn envelope_ids_only_takes_short_strings() {
        let params = serde_json::json!({"turn_id": "t-1", "command_id": 7, "prompt": "hi"});
        assert_eq!(envelope_ids(&params), vec![("turn_id", "t-1".to_owned())]);
    }
}
