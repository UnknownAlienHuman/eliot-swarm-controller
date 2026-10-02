//! Native patches for one isolated, closed projected turn. Not a workspace
//! capture, binary-file download, execution terminal or Task acceptance.
use super::{Data, RuntimeCommand, Service, decode, input_id, unavailable};
use crate::{error::Result, model};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(super) struct TurnDiff {
    pub document: Value,
    pub source: Value,
}

fn snapshots(interval: &[Value]) -> Result<(&str, &str)> {
    let first = interval
        .get(1)
        .filter(|item| item["type"] == "assistant")
        .ok_or_else(|| unavailable("RESULT_SNAPSHOT_UNAVAILABLE"))?;
    let last = interval
        .len()
        .checked_sub(2)
        .and_then(|index| interval.get(index))
        .filter(|item| item["type"] == "assistant")
        .ok_or_else(|| unavailable("RESULT_SNAPSHOT_UNAVAILABLE"))?;
    // Check both endpoints rather than treating native [] (also returned when
    // snapshots are missing) as proof of a turn that changed no files.
    Ok((
        snapshot_id(&first["snapshot"], "start")?,
        snapshot_id(&last["snapshot"], "end")?,
    ))
}

fn snapshot_id<'a>(snapshot: &'a Value, field: &str) -> Result<&'a str> {
    snapshot[field]
        .as_str()
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 256
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"_-".contains(&c))
        })
        .ok_or_else(|| unavailable("RESULT_SNAPSHOT_UNAVAILABLE"))
}

fn validate_files(files: &[Value]) -> Result<()> {
    let mut paths = BTreeSet::new();
    for file in files {
        if file["truncated"] == true {
            return Err(unavailable("NATIVE_DIFF_TRUNCATED"));
        }
        model::fields(file, &["file", "patch", "additions", "deletions", "status"])
            .map_err(|_| unavailable("NATIVE_DIFF_SCHEMA"))?;
        // Paths and patches stay opaque native DATA. They are never joined to
        // local paths, opened, applied, or treated as a CheckRunner candidate.
        let path = file["file"]
            .as_str()
            .filter(|s| !s.trim().is_empty() && s.len() <= 8192)
            .ok_or_else(|| unavailable("NATIVE_DIFF_SCHEMA"))?;
        if !paths.insert(path)
            || !file["patch"].is_string()
            || file["additions"].as_u64().is_none()
            || file["deletions"].as_u64().is_none()
            || !matches!(
                file["status"].as_str(),
                Some("added" | "deleted" | "modified")
            )
        {
            return Err(unavailable("NATIVE_DIFF_SCHEMA"));
        }
    }
    Ok(())
}

impl Service {
    pub(super) async fn turn_diff(
        &self,
        session: &str,
        original: &RuntimeCommand,
        interval: &[Value],
    ) -> Result<TurnDiff> {
        let (start, end) = snapshots(interval)?;
        let input = input_id(&original.operation_id);
        // Never use the vendor's default "newest user message". Both ends name
        // the exact input; input_interval proved it is the turn's only user.
        let query = [("from", input.clone()), ("to", input.clone())];
        let path = format!("/api/session/{session}/diff");
        let read = |raw: Value| -> Result<Vec<Value>> {
            if raw["truncated"] == true {
                return Err(unavailable("NATIVE_DIFF_TRUNCATED"));
            }
            model::fields(&raw, &["data"]).map_err(|_| unavailable("NATIVE_DIFF_SCHEMA"))?;
            let response: Data<Vec<Value>> = decode(raw)?;
            validate_files(&response.data)?;
            Ok(response.data)
        };
        // Completed assistants and recorded snapshot endpoints prevent the
        // documented active-step fallback to a working-copy capture. Re-reading
        // both diff and interval detects observed races, not an atomic snapshot.
        let files = read(self.get(&path, &query).await?)?;
        if read(self.get(&path, &query).await?)? != files {
            return Err(unavailable("RESULT_SOURCE_CHANGED"));
        }
        let interval_digest = format!(
            "sha256:{}",
            model::digest(model::canonical(&json!(interval))?.as_bytes())
        );
        let source = json!({
            "snapshot_start":start,"snapshot_end":end,
            "interval_digest":interval_digest,
            "file_count":files.len(),"context":"native_full_file_default",
            "turn_scope":"single_user_between_idle_boundaries",
            "format":"native_file_diff_json","binary_contents_included":false,
            "workspace_checkout_verified":false
        });
        Ok(TurnDiff {
            // Pin history and endpoints inside the body as well as provenance:
            // identical patch text after a history change is not the same source.
            document: json!({"session_id":session,"native_input_id":input,
                "snapshot_start":start,"snapshot_end":end,
                "interval_digest":interval_digest,"files":files}),
            source,
        })
    }
}
