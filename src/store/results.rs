//! File publication precedes this transaction. No filesystem or SDK calls in Store.
use super::{operations, runtime};
use crate::{
    artifacts::ArtifactRecord,
    error::{Error, Result},
    model::{self, Principal, Role},
    runtime::{EffectOutcome, RuntimeOutcome},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde_json::{Value, json};

pub(super) fn prepare(db: &Connection, p: &Principal, operation_id: &str) -> Result<Value> {
    let (id, generation, b) = runtime::scope(db, p, true)?;
    let op = operations::get_operation(db, operation_id)?;
    if op["method"] != "agent.result"
        || op["binding_id"] != id
        || op["binding_generation"] != generation
    {
        return Err(Error::new(
            "FORBIDDEN",
            "result belongs to another operation or binding",
        ));
    }
    if !matches!(
        op["state"].as_str(),
        Some("sending" | "native_accepted" | "outcome_unknown" | "settled")
    ) {
        return Err(Error::conflict(
            "result read was not admitted or has already been rejected",
        ));
    }
    let raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |r| r.get(0),
    )?;
    let request: Value = serde_json::from_str(&raw)?;
    Ok(
        json!({"operation_id":operation_id,"binding_id":id,"generation":generation,
        "native_root_id":b["native_root_id"],"native_scope_key":b["native_scope_key"],"selector":request["selector"],"requested_offset":request["offset_bytes"].as_u64().unwrap_or(0),"requested_length":request["length_bytes"].as_u64().unwrap_or(crate::artifacts::MAX_PAGE_BYTES as u64)}),
    )
}

pub(super) fn record(
    db: &mut Connection,
    p: &Principal,
    artifact: &ArtifactRecord,
) -> Result<Value> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let operation_id = model::text(&artifact.metadata, "operation_id")?;
    let context = prepare(&tx, p, operation_id)?;
    for key in [
        "binding_id",
        "generation",
        "native_root_id",
        "native_scope_key",
        "selector",
        "requested_offset",
        "requested_length",
    ] {
        if context[key] != artifact.metadata[key] {
            return Err(Error::conflict(
                "result context changed before file registration",
            ));
        }
    }
    let op = operations::get_operation(&tx, operation_id)?;
    if op["state"] == "settled" {
        let existing = get(&tx, &artifact.artifact_id)?;
        if op["result"]["details"]["artifact_ref"] != artifact.artifact_id
            || existing.content_digest != artifact.content_digest
            || existing.metadata != artifact.metadata
        {
            return Err(Error::conflict(
                "completed result cannot be replaced by different bytes or provenance",
            ));
        }
        return Ok(json!({"recorded":true,"replayed":true,"artifact_ref":artifact.artifact_id}));
    }
    let sql_length = i64::try_from(artifact.byte_length)
        .map_err(|_| Error::invalid("artifact length exceeds the SQLite integer range"))?;
    let now = model::now_ms()?;
    tx.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'native_result_page',?3,?4,?5,?6)",
        params![artifact.artifact_id,artifact.relative_path,sql_length,artifact.content_digest,now,model::canonical(&artifact.metadata)?])?;
    let details = json!({"completion_condition":"result_page_persisted","artifact_ref":artifact.artifact_id,
    "byte_length":artifact.byte_length,"page_sha256":artifact.content_digest,
    "source":artifact.metadata["source"],"offset_bytes":artifact.metadata["offset_bytes"],
    "total_bytes":artifact.metadata["total_bytes"],"eof":artifact.metadata["eof"],
    "next_offset_bytes":if artifact.metadata["eof"]==true {None} else {
        artifact.metadata["offset_bytes"].as_u64().and_then(|n| n.checked_add(artifact.byte_length))
    }});
    let outcome = RuntimeOutcome {
        native_input_id: None,
        operation_id: operation_id.to_string(),
        outcome: EffectOutcome::Applied,
        native_root_id: context["native_root_id"].as_str().map(str::to_owned),
        native_scope_key: context["native_scope_key"].as_str().map(str::to_owned),
        turn_id: None,
        details,
    };
    let encoded = model::canonical(&json!(outcome))?;
    tx.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1", params![operation_id,encoded,now])?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,'runtime.result',?6,?7)",
        params![format!("module:{}",p.client_id),format!("result:{operation_id}"),context["binding_id"].as_str(),context["generation"].as_i64(),operation_id,encoded,now])?;
    tx.commit()?;
    Ok(json!({"recorded":true,"artifact_ref":artifact.artifact_id}))
}

pub(super) fn get(db: &Connection, id: &str) -> Result<ArtifactRecord> {
    let row: Option<(String,String,i64,String,String)> = db.query_row(
        "SELECT kind,relative_path,byte_length,content_digest,metadata_json FROM artifacts WHERE artifact_id=?1 AND kind IN ('native_result_page','native_result','task_submission','source_snapshot','check_result','check_output','script_bundle','script_result','script_output')",
        [id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
    let (kind, relative_path, byte_length, content_digest, metadata) =
        row.ok_or_else(|| Error::new("NOT_FOUND", "result artifact is not registered"))?;
    Ok(ArtifactRecord {
        kind,
        artifact_id: id.to_string(),
        relative_path,
        byte_length: u64::try_from(byte_length)
            .map_err(|_| Error::new("ARTIFACT_DAMAGED", "negative stored artifact length"))?,
        content_digest,
        metadata: serde_json::from_str(&metadata)?,
    })
}
pub(super) fn describe(db: &Connection, p: &Principal, v: &Value) -> Result<Value> {
    if p.role == Role::Module {
        return Err(Error::new(
            "FORBIDDEN",
            "module reports results only for its binding",
        ));
    }
    model::fields(v, &["artifact_id"])?;
    let a = get(db, model::text(v, "artifact_id")?)?;
    if matches!(
        a.kind.as_str(),
        "script_bundle" | "script_result" | "script_output"
    ) {
        super::scripts::authorize_artifact_read(db, p, &a)?;
    }
    Ok(
        json!({"artifact_id":a.artifact_id,"kind":a.kind,"byte_length":a.byte_length,
        "content_digest":a.content_digest,"metadata":a.public_metadata()}),
    )
}
