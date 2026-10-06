//! Authenticated setup, immutable commit observations and revocation for the
//! closed `git.post_commit` hook source.

use super::{gm, meta, set_meta, workspace};
use crate::{
    automation::config,
    config::Config,
    error::{Error, Result},
    hooks::contract::{
        EVENT_NAME, EmitScope, FACT_SCHEMA_VERSION, GitCommitSnapshot, HookCommitFact,
        HookSetupRequest, HookSetupResponse, HookSourceRecord, MAX_EVENT_KEY_BYTES,
        MAX_EVENT_PAYLOAD_BYTES, MAX_SOURCE_EVENTS_PAGE, SOURCE_SCHEMA_VERSION,
        is_canonical_v4_uuid,
    },
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const SOURCE_KEY_PREFIX: &str = "hook:v1:source:";
const ACTIVE_KEY_PREFIX: &str = "hook:v1:active:";
const SETUP_REQUEST_PREFIX: &str = "hook:v1:setup-request:";
const SOURCE_STREAM: &str = "controller:hooks";
const ADMIN_STREAM: &str = "controller:hook-source";
const EVENT_KIND: &str = "git.post_commit";
const SOURCE_CLIENT_PREFIX: &str = "hook-source:";
const SETUP_REQUEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HookSourceStatus {
    Missing,
    Current(HookSourceRecord),
    Revoked(HookSourceRecord),
    Stale(HookSourceRecord),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupRequestRecord {
    schema_version: u32,
    identity_digest: String,
    project_id: String,
    source_id: String,
    credential_hash: String,
}

/// Register a caller-generated HookSource credential for the configured
/// project checkout. The client persists the credential privately before this
/// request; Store retains only its normal client-registry hash. Exact retries
/// return source metadata and never re-deliver the credential.
pub(super) fn setup_source(
    tx: &Transaction<'_>,
    principal: &Principal,
    config: &Config,
    request: &HookSetupRequest,
    now_ms: i64,
) -> Result<HookSetupResponse> {
    require_setup_authority(tx, principal)?;
    if now_ms < 0 {
        return Err(Error::invalid("hook setup time cannot be negative"));
    }
    if request.client_request_id.trim().is_empty()
        || request.client_request_id.len() > 128
        || request.client_request_id.chars().any(char::is_control)
    {
        return Err(Error::invalid("invalid hook setup request identity"));
    }

    crate::hooks::contract::validate_hook_credential(&request.source_id, &request.credential)?;
    let identity_digest =
        model::digest(format!("{}\0{}", principal.client_id, request.client_request_id).as_bytes());
    let request_digest = model::digest(request.client_request_id.as_bytes());
    let request_key = format!("{SETUP_REQUEST_PREFIX}{identity_digest}");
    let credential_hash = model::digest(request.credential.token.as_bytes());
    if let Some(previous) = config::read_record(tx, &request_key, "hook setup request")? {
        let previous: SetupRequestRecord = serde_json::from_value(previous).map_err(|_| {
            Error::new(
                "HOOK_SETUP_REQUEST_RECORD_INVALID",
                "retained hook setup request fields are invalid",
            )
        })?;
        if previous.schema_version != SETUP_REQUEST_SCHEMA_VERSION
            || !valid_sha256(&previous.identity_digest)
            || !valid_sha256(&previous.credential_hash)
            || !is_canonical_v4_uuid(&previous.source_id)
            || previous.project_id.trim().is_empty()
            || previous.project_id.len() > 128
            || previous.project_id.chars().any(char::is_control)
        {
            return Err(Error::new(
                "HOOK_SETUP_REQUEST_RECORD_INVALID",
                "retained hook setup request identity is unsupported",
            ));
        }
        if previous.identity_digest != identity_digest
            || previous.project_id != request.project_id
            || previous.source_id != request.source_id
            || previous.credential_hash != credential_hash
        {
            return Err(Error::new(
                "HOOK_SETUP_REQUEST_CONFLICT",
                "this setup request identity was already used with different source details",
            ));
        }
        let source = load_source(tx, &previous.source_id)?.ok_or_else(|| {
            Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "retained setup request points to a missing source",
            )
        })?;
        let client = meta(tx, &format!("client:{}", source.client_id))?.ok_or_else(|| {
            Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "retained source has no authenticated client registration",
            )
        })?;
        if source.setup_request_digest != request_digest
            || source.project_id != request.project_id
            || source.source_id != request.source_id
            || source.client_id != request.credential.client_id
            || client["role"] != "hook_source"
            || client["hook_source_id"] != source.source_id
            || client["token_hash"].as_str() != Some(credential_hash.as_str())
            || client["disabled"].as_bool().is_none()
            || (client["disabled"] == true) != source.revoked_at_ms.is_some()
        {
            return Err(Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "retained setup source and authenticated client disagree",
            ));
        }
        return Ok(HookSetupResponse { source });
    }

    // The stable caller-generated request identity and credential survive a
    // manager handoff. A new current manager can recover the public result of
    // the same committed request without rewriting its original provenance.
    if let Some(source) = load_source(tx, &request.source_id)? {
        if source.project_id != request.project_id
            || source.client_id != request.credential.client_id
            || source.setup_request_digest != request_digest
        {
            return Err(Error::new(
                "HOOK_SETUP_REQUEST_CONFLICT",
                "this source identity was already used with different setup details",
            ));
        }
        let client = meta(tx, &format!("client:{}", source.client_id))?.ok_or_else(|| {
            Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "retained source has no authenticated client registration",
            )
        })?;
        if client["role"] != "hook_source"
            || client["hook_source_id"] != source.source_id
            || client["disabled"].as_bool().is_none()
        {
            return Err(Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "retained source and authenticated client registration disagree",
            ));
        }
        if client["token_hash"].as_str() != Some(credential_hash.as_str()) {
            return Err(Error::new(
                "HOOK_SETUP_REQUEST_CONFLICT",
                "this source identity was already used with different setup details",
            ));
        }
        if (client["disabled"] == true) != source.revoked_at_ms.is_some() {
            return Err(Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "retained source and authenticated client revocation state disagree",
            ));
        }
        return Ok(HookSetupResponse { source });
    }

    let registration = workspace::get_registration(tx, &request.project_id, config)?;

    let active_key = active_key(&request.project_id);
    if let Some(active) = config::read_record(tx, &active_key, "active hook source")? {
        let source_id = model::text(&active, "source_id")?;
        let existing = load_source(tx, source_id)?.ok_or_else(|| {
            Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "active hook source index points to a missing source",
            )
        })?;
        if existing.revoked_at_ms.is_none() {
            return Err(Error::new(
                "HOOK_SOURCE_EXISTS",
                "this project already has an active post-commit source; read it back or revoke it before setup",
            ));
        }
        tx.execute("DELETE FROM meta WHERE key=?1", [&active_key])?;
    }

    let source_id = request.source_id.clone();
    let client_id = format!("{SOURCE_CLIENT_PREFIX}{source_id}");
    if load_source(tx, &source_id)?.is_some() || meta(tx, &format!("client:{client_id}"))?.is_some()
    {
        return Err(Error::new(
            "HOOK_SOURCE_ID_COLLISION",
            "hook source identity is already registered",
        ));
    }
    let record = HookSourceRecord {
        schema_version: SOURCE_SCHEMA_VERSION,
        source_id: source_id.clone(),
        client_id: client_id.clone(),
        project_id: request.project_id.clone(),
        event: EVENT_NAME.to_owned(),
        registration_id: registration.registration_id.clone(),
        registration_generation: registration.generation,
        registration_digest: registration.registration_digest.clone(),
        canonical_repository: registration.trusted_repository.clone(),
        setup_request_digest: request_digest,
        created_by: principal.client_id.clone(),
        created_at_ms: now_ms,
        revision: 1,
        revoked_at_ms: None,
        last_observation_id: None,
    };
    validate_source(&record)?;
    let response_source = record.clone();
    config::write_record(tx, &source_key(&source_id), &json!(record))?;
    config::write_record(
        tx,
        &active_key,
        &json!({"source_id":source_id,"event":EVENT_NAME}),
    )?;
    super::set_meta(
        tx,
        &request_key,
        &json!(SetupRequestRecord {
            schema_version: SETUP_REQUEST_SCHEMA_VERSION,
            identity_digest,
            project_id: request.project_id.clone(),
            source_id: source_id.clone(),
            credential_hash: credential_hash.clone(),
        }),
    )?;
    set_meta(
        tx,
        &format!("client:{client_id}"),
        &json!({
            "role":"hook_source",
            "token_hash":credential_hash,
            "disabled":false,
            "hook_source_id":source_id
        }),
    )?;
    insert_admin_fact(
        tx,
        &format!("setup:{source_id}"),
        "hook.source.setup",
        &json!({
            "source_id":source_id,
            "project_id":request.project_id,
            "canonical_repository":registration.trusted_repository,
            "registration_id":registration.registration_id,
            "registration_generation":registration.generation,
            "event":EVENT_NAME,
            "created_by":principal.client_id
        }),
        now_ms,
    )?;

    Ok(HookSetupResponse {
        source: response_source,
    })
}

/// Read-only preflight data for the Store's local Git verification phase.
/// This must run before entering the final write transaction so Git never runs
/// while SQLite is locked.
pub(super) fn emit_scope(
    db: &Connection,
    principal: &Principal,
    config: &Config,
    source_id: &str,
) -> Result<EmitScope> {
    if principal.role != Role::HookSource {
        return Err(Error::new(
            "FORBIDDEN",
            "only a setup-issued HookSource may emit a hook fact",
        ));
    }
    let source = load_source(db, source_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", "hook source was not found"))?;
    require_source_client(db, principal, &source)?;
    let registration = current_registration(db, config, &source)?;
    Ok(EmitScope {
        source,
        repository_path: registration.repository_path,
        git_executable: config.forge.git_executable.clone(),
    })
}

/// Commit a verified post-commit fact. `snapshot` must have been obtained
/// outside the Store transaction by checking the exact OID in the configured
/// local repository; this method rechecks source, credential and registration
/// authority before writing the immutable observation.
pub(super) fn emit(
    tx: &Transaction<'_>,
    principal: &Principal,
    config: &Config,
    scope: &EmitScope,
    snapshot: &GitCommitSnapshot,
    now_ms: i64,
) -> Result<Value> {
    if now_ms < 0 || !crate::forge::valid_object_id(&snapshot.commit_oid) {
        return Err(Error::invalid("invalid post-commit observation"));
    }
    let source = load_source(tx, &scope.source.source_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", "hook source was not found"))?;
    require_source_client(tx, principal, &source)?;
    let registration = current_registration(tx, config, &source)?;
    if !same_source_authority(&source, &scope.source)
        || registration.registration_id != scope.source.registration_id
        || registration.generation != scope.source.registration_generation
        || registration.registration_digest != scope.source.registration_digest
        || registration.trusted_repository != scope.source.canonical_repository
        || registration.repository_path != scope.repository_path
        || config.forge.git_executable != scope.git_executable
    {
        return Err(Error::new(
            "HOOK_SOURCE_SCOPE_CHANGED",
            "hook source or configured repository changed during commit verification",
        ));
    }

    let source_event_key = format!("{}:{}", source.source_id, snapshot.commit_oid);
    if source_event_key.len() > MAX_EVENT_KEY_BYTES {
        return Err(Error::invalid(
            "post-commit event identity exceeds its bound",
        ));
    }
    let payload = json!({
        "schema_version":FACT_SCHEMA_VERSION,
        "event":EVENT_NAME,
        "source_id":source.source_id,
        "project_id":source.project_id,
        "canonical_repository":source.canonical_repository,
        "registration_id":source.registration_id,
        "registration_generation":source.registration_generation,
        "commit_oid":snapshot.commit_oid,
        "readback_verified":true
    });
    let encoded = model::canonical(&payload)?;
    if encoded.len() > MAX_EVENT_PAYLOAD_BYTES {
        return Err(Error::invalid("post-commit fact exceeds its storage bound"));
    }

    let old: Option<(i64, String)> = tx
        .query_row(
            "SELECT observation_id,payload_json FROM observations WHERE source_stream_id=?1 AND source_event_key=?2",
            params![SOURCE_STREAM, source_event_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if let Some((observation_id, old_payload)) = old {
        if old_payload != encoded {
            return Err(Error::new(
                "HOOK_EVENT_IDENTITY_CONFLICT",
                "this source and commit identity already has different immutable facts",
            ));
        }
        validate_fact(&serde_json::from_str::<Value>(&old_payload)?)?;
        return Ok(json!({
            "recorded":false,
            "duplicate":true,
            "source_id":source.source_id,
            "event":EVENT_NAME,
            "commit_oid":snapshot.commit_oid,
            "readback_verified":true,
            "observation_id":observation_id
        }));
    }

    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5)",
        params![SOURCE_STREAM, source_event_key, EVENT_KIND, encoded, now_ms],
    )?;
    let observation_id = tx.last_insert_rowid();
    let mut updated = source.clone();
    updated.last_observation_id = Some(observation_id);
    config::write_record(tx, &source_key(&source.source_id), &json!(updated))?;
    Ok(json!({
        "recorded":true,
        "duplicate":false,
        "source_id":source.source_id,
        "event":EVENT_NAME,
        "commit_oid":snapshot.commit_oid,
        "readback_verified":true,
        "observation_id":observation_id
    }))
}

/// Bounded source and fact readback. HookSource credentials can select only
/// their own source; manager reads require the current GM or local Operator.
pub(super) fn get(
    db: &Connection,
    principal: &Principal,
    source_id: &str,
    after: i64,
    limit: usize,
    config: &Config,
) -> Result<Value> {
    if after < 0 || !(1..=MAX_SOURCE_EVENTS_PAGE).contains(&limit) {
        return Err(Error::invalid(
            "hook readback cursor or page size is invalid",
        ));
    }
    let source = load_source(db, source_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", "hook source was not found"))?;
    authorize_read(db, principal, &source)?;
    let registration_current = current_registration(db, config, &source)
        .is_ok_and(|registration| registration_identity_matches(&registration, &source));
    let prefix = format!("{}:", source.source_id);
    // ':' is immediately before ';' in ASCII, so this range selects only
    // event keys with this exact setup-issued source UUID prefix and can use
    // the existing unique observation identity index.
    let upper = format!("{};", source.source_id);
    let mut statement = db.prepare(
        "SELECT observation_id,source_event_key,payload_json,recorded_at_ms FROM observations \
         WHERE source_stream_id=?1 AND kind=?2 AND source_event_key>=?3 AND source_event_key<?4 \
           AND observation_id>?5 ORDER BY observation_id LIMIT ?6",
    )?;
    let rows = statement
        .query_map(
            params![
                SOURCE_STREAM,
                EVENT_KIND,
                prefix,
                upper,
                after,
                limit as i64
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut events = Vec::with_capacity(rows.len());
    for (observation_id, event_key, payload, recorded_at_ms) in rows {
        let parsed: Value = serde_json::from_str(&payload).map_err(|_| {
            Error::new(
                "HOOK_EVENT_RECORD_INVALID",
                "retained hook fact is not valid JSON",
            )
        })?;
        let fact = validate_fact(&parsed)?;
        if event_key != format!("{}:{}", source.source_id, fact.commit_oid)
            || fact.source_id != source.source_id
            || fact.project_id != source.project_id
            || fact.canonical_repository != source.canonical_repository
            || fact.registration_id != source.registration_id
            || fact.registration_generation != source.registration_generation
        {
            return Err(Error::new(
                "HOOK_EVENT_RECORD_INVALID",
                "retained hook fact does not match its source identity",
            ));
        }
        events.push(json!({
            "observation_id":observation_id,
            "source_event_key":event_key,
            "recorded_at_ms":recorded_at_ms,
            "fact":parsed
        }));
    }
    let next_after = events
        .last()
        .and_then(|event| event["observation_id"].as_i64());
    Ok(json!({
        "source":public_source(&source),
        "registration_current":registration_current,
        "events":events,
        "after":after,
        "next_after_observation_id":next_after,
        "has_more":events.len()==limit
    }))
}

/// Return only the retained public source record and a safe authority state for
/// the HookCommit consumer. Missing and stale selections are ordinary bounded
/// states; damaged durable records remain errors.
pub(super) fn source_status(
    db: &Connection,
    config: &Config,
    source_id: &str,
) -> Result<HookSourceStatus> {
    if !is_canonical_v4_uuid(source_id) {
        return Ok(HookSourceStatus::Missing);
    }
    let Some(source) = load_source(db, source_id)? else {
        return Ok(HookSourceStatus::Missing);
    };
    if source.revoked_at_ms.is_some() {
        return Ok(HookSourceStatus::Revoked(source));
    }

    let client = meta(db, &format!("client:{}", source.client_id))
        .map_err(|error| {
            if error.code == "INVALID_PARAMS" {
                Error::new(
                    "HOOK_SOURCE_RECORD_INVALID",
                    "retained hook client registration is invalid",
                )
            } else {
                error
            }
        })?
        .ok_or_else(|| {
            Error::new(
                "HOOK_SOURCE_RECORD_INVALID",
                "retained source has no authenticated client registration",
            )
        })?;
    let token_hash = client["token_hash"].as_str();
    if client["role"] != "hook_source"
        || client["hook_source_id"] != source.source_id
        || token_hash.is_none_or(|hash| !valid_sha256(hash))
        || client["disabled"].as_bool().is_none()
    {
        return Err(Error::new(
            "HOOK_SOURCE_RECORD_INVALID",
            "retained source and authenticated client registration disagree",
        ));
    }
    if client["disabled"] == true {
        return Ok(HookSourceStatus::Stale(source));
    }

    match current_registration(db, config, &source) {
        Ok(_) => Ok(HookSourceStatus::Current(source)),
        Err(error) if is_stale_scope_error(&error.code) => Ok(HookSourceStatus::Stale(source)),
        Err(error) => Err(error),
    }
}

fn is_stale_scope_error(code: &str) -> bool {
    matches!(
        code,
        "HOOK_SOURCE_SCOPE_CHANGED"
            | "WORKSPACE_UNREGISTERED"
            | "WORKSPACE_REGISTRATION_REVOKED"
            | "WORKSPACE_REGISTRATION_CHANGED"
            | "WORKSPACE_CONFIG"
            | "WORKSPACE_ROOT"
            | "WORKSPACE_REPOSITORY"
            | "WORKSPACE_REPOSITORY_IDENTITY"
    )
}

/// Revoke the transport credential and source in one transaction. File
/// restoration is a separate local CLI effect; after this commits the wrapper
/// can no longer add facts even if local removal encounters edited files.
pub(super) fn revoke(
    tx: &Transaction<'_>,
    principal: &Principal,
    source_id: &str,
    expected_revision: i64,
    now_ms: i64,
) -> Result<Value> {
    require_setup_authority(tx, principal)?;
    if expected_revision <= 0 || now_ms < 0 {
        return Err(Error::invalid(
            "hook revocation revision or time is invalid",
        ));
    }
    let mut source = load_source(tx, source_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", "hook source was not found"))?;
    if source.revision != expected_revision {
        return Err(Error::new(
            "REVISION_CONFLICT",
            "hook source revision changed; read it back before revoking",
        ));
    }
    if source.revoked_at_ms.is_some() {
        return Ok(json!({
            "source":public_source(&source),
            "already_revoked":true,
            "local_hook_restore":"readback_required"
        }));
    }
    source.revision = source
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::new("REVISION_OVERFLOW", "hook source revision overflow"))?;
    source.revoked_at_ms = Some(now_ms);
    config::write_record(tx, &source_key(source_id), &json!(source))?;

    let client_key = format!("client:{}", source.client_id);
    let mut client = meta(tx, &client_key)?.ok_or_else(|| {
        Error::new(
            "HOOK_SOURCE_RECORD_INVALID",
            "hook source has no authenticated client registration",
        )
    })?;
    if client["role"] != "hook_source" || client["hook_source_id"] != source_id {
        return Err(Error::new(
            "HOOK_SOURCE_RECORD_INVALID",
            "hook source client registration does not match its source",
        ));
    }
    client["disabled"] = json!(true);
    set_meta(tx, &client_key, &client)?;

    let active_key = active_key(&source.project_id);
    if config::read_record(tx, &active_key, "active hook source")?
        .is_some_and(|active| active["source_id"] == source_id)
    {
        tx.execute("DELETE FROM meta WHERE key=?1", [&active_key])?;
    }
    insert_admin_fact(
        tx,
        &format!("revoke:{}:{}", source.source_id, source.revision),
        "hook.source.revoke",
        &json!({
            "source_id":source.source_id,
            "project_id":source.project_id,
            "event":EVENT_NAME,
            "revision":source.revision,
            "revoked_at_ms":now_ms,
            "revoked_by":principal.client_id
        }),
        now_ms,
    )?;
    Ok(json!({
        "source":public_source(&source),
        "already_revoked":false,
        "credential_disabled":true,
        "local_hook_restore":"required"
    }))
}

fn require_setup_authority(db: &Connection, principal: &Principal) -> Result<()> {
    if !matches!(principal.role, Role::Operator | Role::Manager) {
        return Err(Error::new(
            "FORBIDDEN",
            "hook source setup and revocation require current manager or local Operator authority",
        ));
    }
    gm::require_authority(db, principal)
}

fn authorize_read(db: &Connection, principal: &Principal, source: &HookSourceRecord) -> Result<()> {
    match principal.role {
        Role::HookSource => require_source_client(db, principal, source),
        Role::Manager | Role::Operator => require_setup_authority(db, principal),
        _ => Err(Error::new(
            "FORBIDDEN",
            "hook source readback requires its HookSource or current manager/Operator authority",
        )),
    }
}

fn require_source_client(
    db: &Connection,
    principal: &Principal,
    source: &HookSourceRecord,
) -> Result<()> {
    if principal.role != Role::HookSource
        || source.revoked_at_ms.is_some()
        || principal.client_id != source.client_id
    {
        return Err(Error::new(
            "UNAUTHORIZED",
            "hook source is revoked or outside this HookSource credential",
        ));
    }
    let client = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "hook source credential is not registered"))?;
    if client["disabled"] == true
        || client["role"] != "hook_source"
        || client["hook_source_id"] != source.source_id
        || client["token_hash"].as_str().is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "UNAUTHORIZED",
            "hook source credential is disabled or outside its registered source",
        ));
    }
    Ok(())
}

fn current_registration(
    db: &Connection,
    config: &Config,
    source: &HookSourceRecord,
) -> Result<crate::workspace::WorkspaceRegistration> {
    let registration = workspace::get_registration(db, &source.project_id, config)?;
    if !registration_identity_matches(&registration, source) {
        return Err(Error::new(
            "HOOK_SOURCE_SCOPE_CHANGED",
            "the active workspace registration no longer matches this hook source",
        ));
    }
    Ok(registration)
}

fn registration_identity_matches(
    registration: &crate::workspace::WorkspaceRegistration,
    source: &HookSourceRecord,
) -> bool {
    registration.registration_id == source.registration_id
        && registration.generation == source.registration_generation
        && registration.registration_digest == source.registration_digest
        && registration.trusted_repository == source.canonical_repository
}

fn same_source_authority(left: &HookSourceRecord, right: &HookSourceRecord) -> bool {
    left.schema_version == right.schema_version
        && left.source_id == right.source_id
        && left.client_id == right.client_id
        && left.project_id == right.project_id
        && left.event == right.event
        && left.registration_id == right.registration_id
        && left.registration_generation == right.registration_generation
        && left.registration_digest == right.registration_digest
        && left.canonical_repository == right.canonical_repository
        && left.setup_request_digest == right.setup_request_digest
        && left.created_by == right.created_by
        && left.created_at_ms == right.created_at_ms
        && left.revision == right.revision
        && left.revoked_at_ms == right.revoked_at_ms
}

fn load_source(db: &Connection, source_id: &str) -> Result<Option<HookSourceRecord>> {
    if !is_canonical_v4_uuid(source_id) {
        return Ok(None);
    }
    let Some(value) = config::read_record(db, &source_key(source_id), "hook source")? else {
        return Ok(None);
    };
    let source: HookSourceRecord = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "HOOK_SOURCE_RECORD_INVALID",
            "retained hook source fields are invalid",
        )
    })?;
    validate_source(&source)?;
    if source.source_id != source_id {
        return Err(Error::new(
            "HOOK_SOURCE_RECORD_INVALID",
            "retained hook source key and identity differ",
        ));
    }
    Ok(Some(source))
}

fn validate_source(source: &HookSourceRecord) -> Result<()> {
    if source.schema_version != SOURCE_SCHEMA_VERSION
        || !is_canonical_v4_uuid(&source.source_id)
        || source.client_id != format!("{SOURCE_CLIENT_PREFIX}{}", source.source_id)
        || source.project_id.trim().is_empty()
        || source.project_id.len() > 128
        || source.project_id.chars().any(char::is_control)
        || source.event != EVENT_NAME
        || source.registration_id.trim().is_empty()
        || source.registration_generation <= 0
        || !valid_sha256(&source.registration_digest)
        || source.canonical_repository.trim().is_empty()
        || source.canonical_repository.len() > 512
        || source.canonical_repository.chars().any(char::is_control)
        || !crate::forge::canonical_repository(&source.canonical_repository)
            .is_ok_and(|canonical| canonical == source.canonical_repository)
        || !valid_sha256(&source.setup_request_digest)
        || source.created_by.trim().is_empty()
        || source.created_at_ms < 0
        || source.revision <= 0
        || source
            .revoked_at_ms
            .is_some_and(|time| time < source.created_at_ms)
        || source.last_observation_id.is_some_and(|id| id <= 0)
    {
        return Err(Error::new(
            "HOOK_SOURCE_RECORD_INVALID",
            "retained hook source identity or state is unsupported",
        ));
    }
    Ok(())
}

fn validate_fact(value: &Value) -> Result<HookCommitFact> {
    HookCommitFact::parse(value)
}

fn public_source(source: &HookSourceRecord) -> Value {
    source.public_value()
}

fn insert_admin_fact(
    tx: &Transaction<'_>,
    event_key: &str,
    kind: &str,
    payload: &Value,
    now_ms: i64,
) -> Result<()> {
    let encoded = model::canonical(payload)?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5)",
        params![ADMIN_STREAM, event_key, kind, encoded, now_ms],
    )?;
    Ok(())
}

fn source_key(source_id: &str) -> String {
    format!("{SOURCE_KEY_PREFIX}{source_id}")
}

fn active_key(project_id: &str) -> String {
    format!(
        "{ACTIVE_KEY_PREFIX}{}",
        model::digest(project_id.as_bytes())
    )
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
