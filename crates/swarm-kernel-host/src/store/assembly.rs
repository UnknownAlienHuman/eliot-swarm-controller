//! Durable local-only result assembly. Native delivery/retry rules are unchanged.
use super::{current_principal, operations, results};
use crate::{
    artifacts::{ArtifactRecord, AssemblyRequest},
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

fn writer(p: &Principal) -> Result<()> {
    if !matches!(p.role, Role::Operator | Role::Manager) {
        return Err(Error::new(
            "FORBIDDEN",
            "only an operator or manager can assemble retained results",
        ));
    }
    Ok(())
}
fn pages(db: &Connection, input: &AssemblyRequest) -> Result<Vec<ArtifactRecord>> {
    input
        .page_refs
        .iter()
        .map(|id| {
            let a = results::get(db, id)?;
            if a.kind != "native_result_page" {
                return Err(Error::invalid(
                    "assembly input must be a native result page",
                ));
            }
            Ok(a)
        })
        .collect()
}
pub(super) fn reserve(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    writer(p)?;
    let input: AssemblyRequest = serde_json::from_value(v.clone())?;
    input.validate()?;
    let _ = pages(tx, &input)?; // Existence before admission; bytes checked off the DB thread.
    Ok(json!({"operation_id":id,"state":"queued","admission":"durable_local","native_call":false}))
}

pub(super) fn begin(
    db: &mut Connection,
    p: Principal,
    id: &str,
) -> Result<Option<(AssemblyRequest, Vec<ArtifactRecord>)>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    writer(&p)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "artifact.assemble" || op["caller_id"] != p.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "assembly operation belongs to another caller or method",
        ));
    }
    if !matches!(op["state"].as_str(), Some("queued" | "outcome_unknown")) {
        return Ok(None);
    }
    let raw: String = tx.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let input: AssemblyRequest = serde_json::from_str(&raw)?;
    input.validate()?;
    let records = pages(&tx, &input)?;
    let now = model::now_ms()?;
    // Replay after host restart is safe ONLY for this deterministic local file
    // publication, never for vendor prompts. Publication compares existing bytes.
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1", params![id,now])?;
    tx.commit()?;
    Ok(Some((input, records)))
}

pub(super) fn finish(db: &mut Connection, id: &str, outcome: Result<ArtifactRecord>) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "artifact.assemble" || op["state"] != "sending" {
        return Err(Error::conflict("assembly operation is no longer executing"));
    }
    let now = model::now_ms()?;
    let value = match outcome {
        Ok(a) => {
            let length = i64::try_from(a.byte_length)
                .map_err(|_| Error::invalid("assembly exceeds SQLite integer range"))?;
            tx.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'native_result',?3,?4,?5,?6)",
                params![a.artifact_id,a.relative_path,length,a.content_digest,now,model::canonical(&a.metadata)?])?;
            json!({"operation_id":id,"outcome":"applied","details":{
                "completion_condition":"whole_result_persisted","artifact_ref":a.artifact_id,
                "byte_length":a.byte_length,"sha256":a.content_digest,"metadata":a.public_metadata()}})
        }
        Err(error) => json!({"operation_id":id,"outcome":"failed","error":error}),
    };
    tx.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1", params![id,model::canonical(&value)?,now])?;
    tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller',?1,?2,'artifact.assembled',?3,?4)",
        params![format!("assembly:{id}"),id,model::canonical(&value)?,now])?;
    tx.commit()?;
    Ok(())
}

pub(super) fn parts(db: &Connection, v: &Value) -> Result<Value> {
    model::fields(v, &["artifact_id", "after", "limit"])?;
    let a = results::get(db, model::text(v, "artifact_id")?)?;
    if a.kind != "native_result" {
        return Err(Error::invalid("artifact is not an assembled result"));
    }
    let list = a.metadata["parts"]
        .as_array()
        .ok_or_else(|| Error::new("ARTIFACT_DAMAGED", "missing segment manifest"))?;
    let (limit, after) = super::page(v)?;
    let after =
        usize::try_from(after).map_err(|_| Error::invalid("part offset exceeds platform range"))?;
    if after > list.len() {
        return Err(Error::invalid("part offset exceeds manifest"));
    }
    let end = list.len().min(after.saturating_add(limit as usize));
    Ok(
        json!({"artifact_id":a.artifact_id,"parts":&list[after..end],"part_count":list.len(),
        "next_after":if end<list.len(){Some(end)}else{None}}),
    )
}
