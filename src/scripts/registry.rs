use crate::{
    artifacts::ArtifactRecord,
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

pub const BUNDLE_KIND: &str = "script_bundle";

pub fn bundle_record(db: &Connection, script_id: &str, revision: i64) -> Result<ArtifactRecord> {
    let row: Option<(String, String, i64, String, String)> = db
        .query_row(
            "SELECT a.artifact_id,a.relative_path,a.byte_length,a.content_digest,a.metadata_json \
             FROM script_revisions r JOIN artifacts a ON a.artifact_id=r.bundle_ref \
             WHERE r.script_id=?1 AND r.revision=?2 AND a.kind='script_bundle'",
            params![script_id, revision],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let (artifact_id, relative_path, byte_length, content_digest, metadata) =
        row.ok_or_else(|| Error::new("NOT_FOUND", "script revision is not registered"))?;
    if byte_length < 0 || content_digest.len() != 64 {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "script bundle artifact identity is invalid",
        ));
    }
    Ok(ArtifactRecord {
        kind: BUNDLE_KIND.into(),
        artifact_id,
        relative_path,
        byte_length: byte_length as u64,
        content_digest,
        metadata: serde_json::from_str(&metadata)?,
    })
}

pub fn revision_identity(db: &Connection, script_id: &str, revision: i64) -> Result<Value> {
    let value: Option<String> = db
        .query_row(
            "SELECT json_object('script_id',r.script_id,'revision',r.revision,\
             'bundle_ref',r.bundle_ref,'bundle_sha256',r.bundle_sha256,\
             'interpreter',json(r.interpreter_json),'validated_at_ms',r.validated_at_ms,\
             'created_by',r.created_by,'created_at_ms',r.created_at_ms) \
             FROM script_revisions r WHERE r.script_id=?1 AND r.revision=?2",
            params![script_id, revision],
            |row| row.get(0),
        )
        .optional()?;
    let mut identity: Value = serde_json::from_str(
        &value.ok_or_else(|| Error::new("NOT_FOUND", "script revision is not registered"))?,
    )
    .map_err(Error::from)?;
    let bundle = bundle_record(db, script_id, revision)?;
    let effects: Vec<super::manifest::ScriptControllerEffect> = serde_json::from_value(
        bundle
            .metadata
            .get("controller_effects")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .map_err(|_| {
        Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "script bundle controller effect metadata is invalid",
        )
    })?;
    identity["controller_effects"] = json!(effects);
    Ok(identity)
}

pub fn describe(db: &Connection, script_id: &str, revision: Option<i64>) -> Result<Value> {
    let head: Option<(String, Option<i64>, i64, i64)> = db
        .query_row(
            "SELECT owner_id,active_revision,created_at_ms,updated_at_ms \
             FROM scripts WHERE script_id=?1",
            [script_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let (owner_id, active_revision, created_at_ms, updated_at_ms) =
        head.ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))?;
    let selected = revision.or(active_revision);
    let revision_info = match selected {
        Some(revision) => revision_identity(db, script_id, revision)?,
        None => Value::Null,
    };
    let runs = {
        let mut statement = db.prepare(
            "SELECT run_id,operation_id,revision,task_id,task_revision,attempt_id,state,result_ref,stdout_ref,stderr_ref,exit_code,started_at_ms,finished_at_ms,created_at_ms \
             FROM script_runs WHERE script_id=?1 ORDER BY created_at_ms DESC,run_id DESC LIMIT 20",
        )?;
        statement
            .query_map([script_id], |row| {
                Ok(json!({
                    "run_id":row.get::<_,String>(0)?,
                    "operation_id":row.get::<_,String>(1)?,
                    "revision":row.get::<_,i64>(2)?,
                    "task_id":row.get::<_,Option<String>>(3)?,
                    "task_revision":row.get::<_,Option<i64>>(4)?,
                    "attempt_id":row.get::<_,Option<String>>(5)?,
                    "state":row.get::<_,String>(6)?,
                    "result_ref":row.get::<_,Option<String>>(7)?,
                    "stdout_ref":row.get::<_,Option<String>>(8)?,
                    "stderr_ref":row.get::<_,Option<String>>(9)?,
                    "exit_code":row.get::<_,Option<i64>>(10)?,
                    "started_at_ms":row.get::<_,Option<i64>>(11)?,
                    "finished_at_ms":row.get::<_,Option<i64>>(12)?,
                    "created_at_ms":row.get::<_,i64>(13)?,
                    "current_state_read_method":"operation.get"
                }))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    Ok(json!({
        "script_id":script_id,
        "owner_id":owner_id,
        "active_revision":active_revision,
        "selected_revision":selected,
        "revision":revision_info,
        "recent_runs":runs,
        "result_read_method":"artifact.read",
        "created_at_ms":created_at_ms,
        "updated_at_ms":updated_at_ms,
    }))
}

pub fn list(db: &Connection, after: i64, limit: i64) -> Result<Value> {
    let mut statement =
        db.prepare("SELECT script_id FROM scripts ORDER BY script_id LIMIT ?1 OFFSET ?2")?;
    let ids = statement
        .query_map(params![limit, after], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let items = ids
        .iter()
        .map(|id| describe(db, id, None))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "items":items,
        "next_after":after.saturating_add(items.len() as i64),
        "pagination":"offset_snapshot_not_inventory_proof",
    }))
}

/// Paginate only a Manager's own scripts. Filtering happens before OFFSET so
/// inaccessible script IDs cannot leak through page boundaries or counts.
pub fn list_owned(db: &Connection, owner_id: &str, after: i64, limit: i64) -> Result<Value> {
    let mut statement = db.prepare(
        "SELECT script_id FROM scripts WHERE owner_id=?1 ORDER BY script_id LIMIT ?2 OFFSET ?3",
    )?;
    let ids = statement
        .query_map(params![owner_id, limit, after], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let items = ids
        .iter()
        .map(|id| describe(db, id, None))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "items":items,
        "next_after":after.saturating_add(items.len() as i64),
        "pagination":"offset_snapshot_not_inventory_proof",
    }))
}

pub fn register_artifact(tx: &Transaction<'_>, artifact: &ArtifactRecord, now: i64) -> Result<()> {
    if artifact.kind != BUNDLE_KIND
        || artifact.byte_length
            > (super::manifest::MAX_BUNDLE_REQUEST_BYTES + super::manifest::MAX_SCHEMA_BYTES) as u64
        || !artifact.artifact_id.starts_with("script-")
        || artifact.artifact_id.len() != 71
    {
        return Err(Error::invalid(
            "invalid script bundle artifact kind or size",
        ));
    }
    let length = i64::try_from(artifact.byte_length)
        .map_err(|_| Error::invalid("script bundle artifact length is out of range"))?;
    tx.execute(
        "INSERT OR IGNORE INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) \
         VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![
            artifact.artifact_id,
            artifact.relative_path,
            artifact.kind,
            length,
            artifact.content_digest,
            now,
            model::canonical(&artifact.metadata)?,
        ],
    )?;
    let actual = bundle_record_by_id(tx, &artifact.artifact_id)?;
    if actual.kind != artifact.kind
        || actual.relative_path != artifact.relative_path
        || actual.byte_length != artifact.byte_length
        || actual.content_digest != artifact.content_digest
        || actual.metadata != artifact.metadata
    {
        return Err(Error::conflict(
            "registered script bundle artifact identity changed",
        ));
    }
    Ok(())
}

pub fn bundle_record_by_id(db: &Connection, id: &str) -> Result<ArtifactRecord> {
    let row: Option<(String, String, i64, String, String)> = db
        .query_row(
            "SELECT artifact_id,relative_path,byte_length,content_digest,metadata_json \
             FROM artifacts WHERE artifact_id=?1 AND kind='script_bundle'",
            [id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let (artifact_id, relative_path, byte_length, content_digest, metadata) =
        row.ok_or_else(|| Error::new("NOT_FOUND", "script bundle artifact is not registered"))?;
    let record = ArtifactRecord {
        kind: BUNDLE_KIND.into(),
        artifact_id,
        relative_path,
        byte_length: u64::try_from(byte_length)
            .map_err(|_| Error::new("SCRIPT_REGISTRY_DAMAGED", "negative script artifact size"))?,
        content_digest,
        metadata: serde_json::from_str(&metadata)?,
    };
    if record.content_digest.len() != 64
        || !record
            .content_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || record.relative_path != format!("artifacts/{}.bin", record.artifact_id)
    {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "script artifact identity is malformed",
        ));
    }
    Ok(record)
}

pub fn parse_bundle(
    bytes: &[u8],
    record: &ArtifactRecord,
) -> Result<super::manifest::ScriptBundle> {
    if bytes.len() as u64 != record.byte_length || model::digest(bytes) != record.content_digest {
        return Err(Error::new(
            "ARTIFACT_DAMAGED",
            "script bundle bytes differ from registered identity",
        ));
    }
    let bundle: super::manifest::ScriptBundle = serde_json::from_slice(bytes).map_err(|_| {
        Error::new(
            "SCRIPT_BUNDLE_DAMAGED",
            "retained script bundle cannot be parsed",
        )
    })?;
    bundle.validate()?;
    Ok(bundle)
}
