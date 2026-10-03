//! Bounded host-side readback for launches awaiting native MCP evidence.
//!
//! The Store validates the retained launch and authenticates the scoped
//! Participant before the native adapter is contacted. This path only records
//! the facts exposed by OpenCode's public read API; it cannot permit dispatch.

use super::{Store, meta};
use crate::{
    config::{McpConfig, McpToolProfile},
    error::{Error, Result},
    model::{self, Credential, Role},
    participant_credentials, platform,
    runtime::opencode_v2::{Options, Service},
    store::{launcher, native_mcp, tasks, workspace},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::time::Duration;

const MAX_CANDIDATES_PER_PASS: i64 = 16;
const NATIVE_READBACK_TIMEOUT: Duration = Duration::from_secs(55);
const INFLIGHT_STALE_MS: i64 = 70_000;
const INITIAL_RETRY_MS: i64 = 15_000;
const MAX_RETRY_MS: i64 = 300_000;
const SUCCESS_REFRESH_MS: i64 = 300_000;
const MAX_PRIVATE_PROFILE_BYTES: usize = 65_536;
type BindingRouteFacts = (String, String, Option<String>, Option<i64>, String);

struct LaunchSnapshot {
    operation_id: String,
    client_id: String,
    credential_ref: String,
    profile_config_ref: String,
    registration_operation_id: String,
    grant_revision: i64,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    binding_id: String,
    binding_generation: i64,
    profile_name: String,
    surface: String,
    surface_facts: Value,
    route_json: String,
    options: Options,
}

struct ReadbackClaim {
    snapshot: LaunchSnapshot,
    attempt: i64,
    started_at_ms: i64,
    previous_observation_id: Option<i64>,
    previous_semantic_digest: Option<String>,
    first_observed_at_ms: Option<i64>,
}

struct LoadedArtifacts {
    credential: Credential,
    profile_config: McpConfig,
}

enum ClaimOutcome {
    Claimed(Box<ReadbackClaim>),
    Deferred(Value),
    Idle(Option<i64>),
}

impl Store {
    /// Advance at most one due launch readback. Repeated host ticks use the
    /// durable backoff marker and never contact OpenCode on every tick.
    pub(crate) async fn reconcile_native_mcp_once(&self) -> Result<Value> {
        let now = model::now_ms()?;
        let claim_outcome = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let outcome = claim_next_readback(&tx, now)?;
                tx.commit()?;
                Ok(outcome)
            })
            .await?;
        let claim = match claim_outcome {
            ClaimOutcome::Claimed(claim) => *claim,
            ClaimOutcome::Deferred(result) => {
                self.changed
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
                return Ok(result);
            }
            ClaimOutcome::Idle(next_retry_at_ms) => {
                return Ok(json!({
                    "state":"idle",
                    "next_retry_at_ms":next_retry_at_ms,
                    "capability_state":"unknown",
                    "dispatch_permitted":false,
                }));
            }
        };

        let loaded = match self.load_scoped_artifacts(&claim.snapshot).await {
            Ok(loaded) => loaded,
            Err(error) => return self.finish_readback_failure(&claim, &error).await,
        };
        let principal = match self.authenticate(loaded.credential).await {
            Ok(principal)
                if principal.role == Role::Participant
                    && principal.client_id == claim.snapshot.client_id =>
            {
                principal
            }
            Ok(_) => {
                let error = Error::new(
                    "NATIVE_MCP_AUTH_SCOPE",
                    "resolved credential is not the exact launch Participant",
                );
                return self.finish_readback_failure(&claim, &error).await;
            }
            Err(error) => return self.finish_readback_failure(&claim, &error).await,
        };

        let profile = loaded
            .profile_config
            .profiles
            .get(&claim.snapshot.profile_name);
        if loaded.profile_config.default_profile != claim.snapshot.profile_name
            || loaded.profile_config.profiles.len() != 1
            || profile.is_none_or(|profile| {
                profile.tool_profile != McpToolProfile::Participant
                    || profile.expected_client_id != principal.client_id
                    || profile.surface.as_deref() != Some(claim.snapshot.surface.as_str())
            })
        {
            let error = Error::new(
                "NATIVE_MCP_PROFILE_SCOPE",
                "scoped MCP profile does not match the retained launch assignment",
            );
            return self.finish_readback_failure(&claim, &error).await;
        }

        let p = principal.clone();
        let operation_id = claim.snapshot.operation_id.clone();
        let expected_snapshot = snapshot_identity(&claim.snapshot);
        let context_claim = clone_claim(&claim);
        let binding_context = self
            .run(move |db| {
                let _actor = launcher::launch_actor(db, &operation_id)?;
                let (row, manifest) = load_launch_manifest(db, &operation_id)?;
                verify_claim(&row, &manifest, &context_claim)?;
                let current = validate_launch_snapshot(db, &row, &manifest)?;
                if snapshot_identity(&current) != expected_snapshot {
                    return Err(stale_readback());
                }
                let assignment = native_mcp::current_assignment_context(db, &p)?;
                if assignment.as_value()["mcp_profile"] != "participant"
                    || assignment.participant_id() != p.client_id
                    || assignment.binding_id() != current.binding_id
                    || assignment.binding_generation() != current.binding_generation
                {
                    return Err(stale_readback());
                }
                Ok((assignment, current.options))
            })
            .await;
        let (assignment, options) = match binding_context {
            Ok(context) => context,
            Err(error) => return self.finish_readback_failure(&claim, &error).await,
        };

        let observation = tokio::time::timeout(NATIVE_READBACK_TIMEOUT, async {
            let service = Service::connect(&options).await?;
            crate::runtime::opencode_v2::observe_mcp(&service, &options, assignment).await
        })
        .await;
        let observation = match observation {
            Ok(Ok(observation)) => observation,
            Ok(Err(error)) => return self.finish_readback_failure(&claim, &error).await,
            Err(_) => {
                let error = Error::new(
                    "NATIVE_MCP_READBACK_TIMEOUT",
                    "native MCP readback exceeded its bounded time window",
                );
                return self.finish_readback_failure(&claim, &error).await;
            }
        };

        let p = principal;
        let operation_id = claim.snapshot.operation_id.clone();
        let observation_at = model::now_ms()?;
        let record_claim = clone_claim(&claim);
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let _actor = launcher::launch_actor(&tx, &operation_id)?;
                let (row, manifest) = load_launch_manifest(&tx, &operation_id)?;
                verify_claim(&row, &manifest, &record_claim)?;
                let current = validate_launch_snapshot(&tx, &row, &manifest)?;
                if snapshot_identity(&current) != snapshot_identity(&record_claim.snapshot) {
                    return Err(stale_readback());
                }
                let fresh = native_mcp::current_assignment_context(&tx, &p)?;
                if &fresh != observation.scope() {
                    return Err(stale_readback());
                }

                let semantic_digest = observation_semantic_digest(&observation.payload())?;
                let stored = native_mcp::validate_and_record(
                    &tx,
                    &p,
                    &observation,
                    observation_at,
                )?;
                let inserted_id = stored["observation_id"].as_i64().ok_or_else(|| {
                    Error::new("NATIVE_MCP_STORE", "readback observation receipt is incomplete")
                })?;
                let observation_id = match reusable_observation_id(
                    &tx,
                    inserted_id,
                    &record_claim,
                    &semantic_digest,
                )? {
                    Some(previous_id) => previous_id,
                    None => inserted_id,
                };
                if observation_id != inserted_id {
                    tx.execute(
                        "DELETE FROM observations WHERE observation_id=?1 AND kind='native.mcp.capability_readback'",
                        [inserted_id],
                    )?;
                }

                let mut manifest = manifest;
                let first_observed_at_ms = record_claim
                    .first_observed_at_ms
                    .unwrap_or(observation_at);
                manifest["native_mcp_readback"] = json!({
                    "state":"observed_partial",
                    "attempts":record_claim.attempt,
                    "first_observed_at_ms":first_observed_at_ms,
                    "last_observed_at_ms":observation_at,
                    "next_retry_at_ms":observation_at.saturating_add(SUCCESS_REFRESH_MS),
                    "observation_id":observation_id,
                    "semantic_digest":semantic_digest,
                    "dispatch_permitted":false,
                });
                persist_manifest(&tx, &row, &manifest, observation_at)?;
                tx.commit()?;
                Ok(json!({
                    "operation_id":operation_id,
                    "state":"observed_partial",
                    "attempt":record_claim.attempt,
                    "observation_id":observation_id,
                    "next_retry_at_ms":observation_at.saturating_add(SUCCESS_REFRESH_MS),
                    "capability_state":"unknown",
                    "dispatch_permitted":false,
                }))
            })
            .await;
        match result {
            Ok(result) => {
                self.changed
                    .send_modify(|revision| *revision = revision.wrapping_add(1));
                Ok(result)
            }
            Err(error) => self.finish_readback_failure(&claim, &error).await,
        }
    }

    async fn load_scoped_artifacts(&self, snapshot: &LaunchSnapshot) -> Result<LoadedArtifacts> {
        let config = self.config.clone();
        let credential_ref = snapshot.credential_ref.clone();
        let profile_config_ref = snapshot.profile_config_ref.clone();
        let client_id = snapshot.client_id.clone();
        let profile_name = snapshot.profile_name.clone();
        let surface = snapshot.surface.clone();
        let expected_surface_facts = snapshot.surface_facts.clone();
        self.file_io(move |_| {
            let resolved = participant_credentials::resolve_refs(
                &config,
                &credential_ref,
                &profile_config_ref,
            )?;
            let credential = platform::load_credential(resolved.credential_path())?;
            if credential.client_id != client_id {
                return Err(Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved Participant credential does not match the launch client",
                ));
            }
            let profile_bytes = std::fs::read(resolved.profile_config_path()).map_err(|_| {
                Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP profile could not be read",
                )
            })?;
            if profile_bytes.len() > MAX_PRIVATE_PROFILE_BYTES {
                return Err(Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP profile exceeds its size bound",
                ));
            }
            let scoped_file: toml::Value =
                toml::from_str(std::str::from_utf8(&profile_bytes).map_err(|_| {
                    Error::new(
                        "PRIVATE_ARTIFACT_REFERENCE",
                        "resolved scoped MCP profile is not UTF-8",
                    )
                })?)
                .map_err(|_| {
                    Error::new(
                        "PRIVATE_ARTIFACT_REFERENCE",
                        "resolved scoped MCP profile is invalid",
                    )
                })?;
            let mcp_value = scoped_file.get("mcp").cloned().ok_or_else(|| {
                Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP profile has no MCP section",
                )
            })?;
            let profile_config: McpConfig = mcp_value.try_into().map_err(|_| {
                Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP profile has an invalid schema",
                )
            })?;
            profile_config.validate().map_err(|_| {
                Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP profile failed validation",
                )
            })?;
            let selected = profile_config.profiles.get(&profile_name).ok_or_else(|| {
                Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP profile does not contain the launch profile",
                )
            })?;
            if profile_config.default_profile != profile_name
                || profile_config.profiles.len() != 1
                || selected.tool_profile != McpToolProfile::Participant
                || selected.expected_client_id != client_id
                || selected.surface.as_deref() != Some(surface.as_str())
            {
                return Err(Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP profile differs from the exact launch assignment",
                ));
            }
            let current_surface_facts = crate::mcp::launch_profile_surface(
                McpToolProfile::Participant,
                &surface,
                &selected.deferred_groups,
                &selected.manual_tools,
            )
            .map_err(|_| {
                Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP surface is not authorized by the static catalog",
                )
            })?;
            if model::canonical(&current_surface_facts)?
                != model::canonical(&expected_surface_facts)?
            {
                return Err(Error::new(
                    "PRIVATE_ARTIFACT_REFERENCE",
                    "resolved scoped MCP surface differs from the issued launch projection",
                ));
            }
            Ok(LoadedArtifacts {
                credential,
                profile_config,
            })
        })
        .await
    }

    async fn finish_readback_failure(&self, claim: &ReadbackClaim, error: &Error) -> Result<Value> {
        let operation_id = claim.snapshot.operation_id.clone();
        let claim = ReadbackClaim {
            snapshot: clone_snapshot(&claim.snapshot),
            attempt: claim.attempt,
            started_at_ms: claim.started_at_ms,
            previous_observation_id: claim.previous_observation_id,
            previous_semantic_digest: claim.previous_semantic_digest.clone(),
            first_observed_at_ms: claim.first_observed_at_ms,
        };
        let failure_category = failure_category(&error.code);
        let now = model::now_ms()?;
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let outcome = record_retry(&tx, &claim, now, failure_category)?;
                tx.commit()?;
                Ok(outcome)
            })
            .await;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        result
            .map_err(|_| {
                Error::new(
                    "NATIVE_MCP_STORE",
                    "native MCP readback progress could not be saved",
                )
            })
            .map(|mut value| {
                value["operation_id"] = json!(operation_id);
                value
            })
    }
}

fn claim_next_readback(tx: &Transaction<'_>, now: i64) -> Result<ClaimOutcome> {
    let cutoff = now.saturating_sub(INFLIGHT_STALE_MS);
    let ids = {
        let mut statement = tx.prepare(
            "SELECT operation_id FROM operations \
             WHERE method='swarm.launch' AND state='queued' \
               AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_native_mcp' \
               AND ( \
                 json_type(effective_request_json,'$.launch_manifest.native_mcp_readback.state') IS NULL \
                 OR (json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state') \
                       IN ('retry_wait','observed_partial') \
                     AND json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.next_retry_at_ms')<=?1) \
                 OR (json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state')='reading' \
                     AND json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.started_at_ms')<=?2) \
               ) \
             ORDER BY updated_at_ms,operation_id LIMIT ?3",
        )?;
        statement
            .query_map(params![now, cutoff, MAX_CANDIDATES_PER_PASS], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };

    if let Some(operation_id) = ids.into_iter().next() {
        let (row, manifest) = load_launch_manifest(tx, &operation_id)?;
        let previous = readback_marker(&manifest);
        let attempt = marker_attempts(previous).saturating_add(1);
        match validate_launch_snapshot(tx, &row, &manifest) {
            Ok(snapshot) => {
                let claim = ReadbackClaim {
                    snapshot,
                    attempt,
                    started_at_ms: now,
                    previous_observation_id: previous
                        .and_then(|marker| marker["observation_id"].as_i64()),
                    previous_semantic_digest: previous
                        .and_then(|marker| marker["semantic_digest"].as_str())
                        .map(str::to_owned),
                    first_observed_at_ms: previous
                        .and_then(|marker| marker["first_observed_at_ms"].as_i64()),
                };
                let mut next_manifest = manifest;
                next_manifest["native_mcp_readback"] = json!({
                    "state":"reading",
                    "attempts":attempt,
                    "started_at_ms":now,
                    "last_attempt_at_ms":now,
                    "next_retry_at_ms":Value::Null,
                    "observation_id":claim.previous_observation_id,
                    "semantic_digest":claim.previous_semantic_digest,
                    "first_observed_at_ms":claim.first_observed_at_ms,
                    "dispatch_permitted":false,
                });
                persist_manifest(tx, &row, &next_manifest, now)?;
                return Ok(ClaimOutcome::Claimed(Box::new(claim)));
            }
            Err(error) => {
                let marker = retry_marker(
                    attempt,
                    now,
                    failure_category(&error.code),
                    previous.and_then(|old| old["first_observed_at_ms"].as_i64()),
                    previous.and_then(|old| old["observation_id"].as_i64()),
                    previous
                        .and_then(|old| old["semantic_digest"].as_str())
                        .map(str::to_owned),
                );
                let mut next_manifest = manifest;
                next_manifest["native_mcp_readback"] = marker;
                let next_retry_at_ms =
                    next_manifest["native_mcp_readback"]["next_retry_at_ms"].as_i64();
                persist_manifest(tx, &row, &next_manifest, now)?;
                return Ok(ClaimOutcome::Deferred(json!({
                    "operation_id":operation_id,
                    "state":"retry_wait",
                    "attempt":attempt,
                    "failure_category":failure_category(&error.code),
                    "next_retry_at_ms":next_retry_at_ms,
                    "capability_state":"unknown",
                    "dispatch_permitted":false,
                })));
            }
        }
    }

    Ok(ClaimOutcome::Idle(next_retry_at(tx, now)?))
}

fn validate_launch_snapshot(
    db: &Connection,
    row: &LaunchRow,
    manifest: &Value,
) -> Result<LaunchSnapshot> {
    if row.method != "swarm.launch"
        || row.state != "queued"
        || manifest["state"] != "awaiting_native_mcp"
        || manifest["runtime"]["dispatch_permitted"] != false
        || manifest["progress"]["task_dispatch"] != "not_started"
    {
        return Err(stale_readback());
    }
    let _actor = launcher::launch_actor(db, &row.operation_id)?;
    let task_id = text_at(&manifest["task"], "task_id")?;
    let task_revision = positive_at(&manifest["task"], "observed_revision")?;
    let attempt_id = text_at(&manifest["task"], "attempt_id")?;
    let task = tasks::get_task(db, task_id)?;
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let binding_id = text_at(&manifest["binding"], "binding_id")?;
    let binding_generation = positive_at(&manifest["binding"], "generation")?;
    let owner_id = text_at(&attempt, "owner_id")?;
    if task["state"] != "open"
        || task["revision"] != task_revision
        || task["current_attempt_id"].as_str() != Some(attempt_id)
        || task["project_id"] != manifest["task"]["project_id"]
        || attempt["task_id"].as_str() != Some(task_id)
        || attempt["task_revision"] != task_revision
        || attempt["released_at_ms"].is_number()
        || attempt["binding_id"].as_str() != Some(binding_id)
        || attempt["binding_generation"] != binding_generation
        || row.task_id.as_deref() != Some(task_id)
        || row.attempt_id.as_deref() != Some(attempt_id)
        || row.binding_id.as_deref() != Some(binding_id)
        || row.binding_generation != Some(binding_generation)
    {
        return Err(stale_readback());
    }

    let client_id = text_at(&manifest["participant"], "client_id")?.to_owned();
    let credential_ref = text_at(&manifest["participant"], "credential_ref")?.to_owned();
    let profile_config_ref = text_at(&manifest["participant"], "profile_config_ref")?.to_owned();
    let registration_operation_id =
        text_at(&manifest["participant"], "registration_operation_id")?.to_owned();
    let grant_revision = positive_at(&manifest["participant"], "grant_revision")?;
    let profile_name = text_at(&manifest["request"], "mcp_profile")?.to_owned();
    let surface = text_at(&manifest["request"], "mcp_surface")?.to_owned();
    let registration = meta(db, &format!("client:{client_id}"))?.ok_or_else(stale_readback)?;
    let basis = json!({
        "kind":"attempt_owner",
        "assignment_id":Value::Null,
        "review_scope":Value::Null,
    });
    let participant = &manifest["participant"];
    let mcp = &manifest["mcp"];
    if row.caller_id.as_deref() != manifest["actor"]["client_id"].as_str()
        || participant["role"] != "participant"
        || participant["task_id"].as_str() != Some(task_id)
        || participant["task_revision"] != task_revision
        || participant["attempt_id"].as_str() != Some(attempt_id)
        || participant["binding_id"].as_str() != Some(binding_id)
        || participant["binding_generation"] != binding_generation
        || participant["grant_revision"] != grant_revision
        || participant["participation_basis"] != basis
        || !participant["native_session_id"].is_null()
        || registration["role"] != "participant"
        || registration["disabled"] != false
        || registration["task_id"].as_str() != Some(task_id)
        || registration["task_revision"] != task_revision
        || registration["attempt_id"].as_str() != Some(attempt_id)
        || registration["binding_id"].as_str() != Some(binding_id)
        || registration["binding_generation"] != binding_generation
        || registration["created_by"].as_str() != Some(owner_id)
        || registration["created_operation_id"].as_str() != Some(registration_operation_id.as_str())
        || registration["grant_revision"] != grant_revision
        || registration["participation_basis"] != basis
        || !registration["native_session_id"].is_null()
        || !valid_opaque_ref(&credential_ref, "participant-credential")
        || !valid_opaque_ref(&profile_config_ref, "participant-mcp-config")
        || mcp["participant_client_id"].as_str() != Some(client_id.as_str())
        || mcp["credential_ref"].as_str() != Some(credential_ref.as_str())
        || mcp["profile_config_ref"].as_str() != Some(profile_config_ref.as_str())
        || mcp["profile_name"].as_str() != Some(profile_name.as_str())
        || mcp["surface"].as_str() != Some(surface.as_str())
        || !mcp["surface_facts"].is_object()
        || mcp["hard_profile"] != "participant"
        || mcp["status"] != "validated_against_static_catalog"
        || mcp["identity"]["status"] != "registered_enabled_participant"
        || mcp["capability_state"] != "unknown"
        || mcp["runtime_loaded"] != "unknown"
    {
        return Err(stale_readback());
    }

    let binding: Option<BindingRouteFacts> = db
        .query_row(
            "SELECT state,module_artifact_id,native_root_id,released_at_ms,route_json \
             FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id, binding_generation],
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
    let Some((state, artifact_id, native_root_id, released_at_ms, route_json)) = binding else {
        return Err(stale_readback());
    };
    let route: Value = serde_json::from_str(&route_json).map_err(|_| stale_readback())?;
    if state != "ready"
        || released_at_ms.is_some()
        || artifact_id != crate::runtime::opencode_v2::ARTIFACT_ID
        || route["runtime"] != crate::runtime::opencode_v2::RUNTIME
        || route["module_artifact_id"] != crate::runtime::opencode_v2::ARTIFACT_ID
        || native_root_id.as_deref().is_none_or(|root| {
            crate::runtime::opencode_v2::valid_id(root, "ses").is_err()
                || manifest["binding"]["native_root_id"].as_str() != Some(root)
        })
    {
        return Err(stale_readback());
    }

    let lease: crate::workspace::LeaseAuthorityRef =
        serde_json::from_value(manifest["workspace"]["lease_authority"].clone())
            .map_err(|_| stale_readback())?;
    let lease_view = workspace::get_lease_view(db, &lease).map_err(|_| stale_readback())?;
    if model::canonical(&lease_view)? != model::canonical(&manifest["workspace"]["lease"])?
        || lease_view["state"] != "held"
        || lease_view["operation_id"].as_str() != Some(row.operation_id.as_str())
        || lease_view["project_id"] != task["project_id"]
        || lease_view["task_id"].as_str() != Some(task_id)
        || lease_view["task_revision"] != task_revision
        || lease_view["owner_client_id"].as_str() != Some(owner_id)
        || lease_view["attempt_id"].as_str() != Some(attempt_id)
    {
        return Err(stale_readback());
    }

    let options = Options::parse(&route["native_options"]).map_err(|_| stale_readback())?;
    Ok(LaunchSnapshot {
        operation_id: row.operation_id.clone(),
        client_id,
        credential_ref,
        profile_config_ref,
        registration_operation_id,
        grant_revision,
        task_id: task_id.to_owned(),
        task_revision,
        attempt_id: attempt_id.to_owned(),
        binding_id: binding_id.to_owned(),
        binding_generation,
        profile_name,
        surface,
        surface_facts: mcp["surface_facts"].clone(),
        route_json,
        options,
    })
}

fn load_launch_manifest(db: &Connection, operation_id: &str) -> Result<(LaunchRow, Value)> {
    let row = db.query_row(
        "SELECT operation_id,caller_id,method,state,task_id,attempt_id,binding_id,binding_generation,effective_request_json \
         FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| {
            Ok(LaunchRow {
                operation_id: row.get(0)?,
                caller_id: row.get(1)?,
                method: row.get(2)?,
                state: row.get(3)?,
                task_id: row.get(4)?,
                attempt_id: row.get(5)?,
                binding_id: row.get(6)?,
                binding_generation: row.get(7)?,
                effective_request_json: row.get(8)?,
            })
        },
    )?;
    let effective: Value = serde_json::from_str(&row.effective_request_json)?;
    let manifest = effective
        .get("launch_manifest")
        .cloned()
        .ok_or_else(stale_readback)?;
    Ok((row, manifest))
}

struct LaunchRow {
    operation_id: String,
    caller_id: Option<String>,
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    effective_request_json: String,
}

fn verify_claim(row: &LaunchRow, manifest: &Value, claim: &ReadbackClaim) -> Result<()> {
    let marker = readback_marker(manifest).ok_or_else(stale_readback)?;
    let snapshot = &claim.snapshot;
    if row.operation_id != snapshot.operation_id
        || row.method != "swarm.launch"
        || row.state != "queued"
        || manifest["state"] != "awaiting_native_mcp"
        || marker["state"] != "reading"
        || marker["attempts"].as_i64() != Some(claim.attempt)
        || marker["started_at_ms"].as_i64() != Some(claim.started_at_ms)
        || manifest["participant"]["client_id"].as_str() != Some(snapshot.client_id.as_str())
        || manifest["participant"]["credential_ref"].as_str()
            != Some(snapshot.credential_ref.as_str())
        || manifest["participant"]["profile_config_ref"].as_str()
            != Some(snapshot.profile_config_ref.as_str())
        || manifest["participant"]["registration_operation_id"].as_str()
            != Some(snapshot.registration_operation_id.as_str())
        || manifest["participant"]["grant_revision"] != snapshot.grant_revision
    {
        return Err(stale_readback());
    }
    Ok(())
}

fn persist_manifest(
    tx: &Transaction<'_>,
    row: &LaunchRow,
    manifest: &Value,
    now: i64,
) -> Result<()> {
    let mut effective: Value = serde_json::from_str(&row.effective_request_json)?;
    effective["launch_manifest"] = manifest.clone();
    let changed = tx.execute(
        "UPDATE operations SET effective_request_json=?2,updated_at_ms=?3 \
         WHERE operation_id=?1 AND method='swarm.launch' AND state='queued' \
           AND effective_request_json=?4",
        params![
            row.operation_id,
            model::canonical(&effective)?,
            now,
            row.effective_request_json,
        ],
    )?;
    if changed != 1 {
        return Err(Error::conflict(
            "launch changed while native MCP readback progress was being saved",
        ));
    }
    Ok(())
}

fn record_retry(
    tx: &Transaction<'_>,
    claim: &ReadbackClaim,
    now: i64,
    category: &'static str,
) -> Result<Value> {
    let operation_id = &claim.snapshot.operation_id;
    let (row, manifest) = match load_launch_manifest(tx, operation_id) {
        Ok(value) => value,
        Err(_) => return Ok(retry_summary(claim, now, None, "stale")),
    };
    if verify_claim(&row, &manifest, claim).is_err() {
        return Ok(retry_summary(claim, now, None, "stale"));
    }
    let marker = retry_marker(
        claim.attempt,
        now,
        category,
        claim.first_observed_at_ms,
        claim.previous_observation_id,
        claim.previous_semantic_digest.clone(),
    );
    let next_retry_at_ms = marker["next_retry_at_ms"].as_i64();
    let mut next_manifest = manifest;
    next_manifest["native_mcp_readback"] = marker;
    persist_manifest(tx, &row, &next_manifest, now)?;
    Ok(retry_summary(claim, now, next_retry_at_ms, "retry_wait"))
}

fn retry_summary(
    claim: &ReadbackClaim,
    _now: i64,
    next_retry_at_ms: Option<i64>,
    state: &str,
) -> Value {
    json!({
        "operation_id":claim.snapshot.operation_id,
        "state":state,
        "attempt":claim.attempt,
        "next_retry_at_ms":next_retry_at_ms,
        "capability_state":"unknown",
        "dispatch_permitted":false,
    })
}

fn retry_marker(
    attempts: i64,
    now: i64,
    category: &'static str,
    first_observed_at_ms: Option<i64>,
    observation_id: Option<i64>,
    semantic_digest: Option<String>,
) -> Value {
    let next_retry_at_ms = now.saturating_add(retry_delay_ms(attempts));
    json!({
        "state":"retry_wait",
        "attempts":attempts,
        "last_attempt_at_ms":now,
        "next_retry_at_ms":next_retry_at_ms,
        "last_failure_category":category,
        "first_observed_at_ms":first_observed_at_ms,
        "last_observation_id":observation_id,
        "semantic_digest":semantic_digest,
        "dispatch_permitted":false,
    })
}

fn retry_delay_ms(attempts: i64) -> i64 {
    let shift = attempts.saturating_sub(1).clamp(0, 8) as u32;
    INITIAL_RETRY_MS
        .saturating_mul(1_i64 << shift)
        .min(MAX_RETRY_MS)
}

fn next_retry_at(db: &Connection, now: i64) -> Result<Option<i64>> {
    let next: Option<i64> = db.query_row(
        "SELECT min(CASE \
           WHEN json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state')='reading' \
             THEN json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.started_at_ms')+?1 \
           ELSE json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.next_retry_at_ms') \
         END) \
         FROM operations WHERE method='swarm.launch' AND state='queued' \
           AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_native_mcp'",
        [INFLIGHT_STALE_MS],
        |row| row.get(0),
    )?;
    Ok(next.filter(|value| *value >= now))
}

fn readback_marker(manifest: &Value) -> Option<&Value> {
    manifest.get("native_mcp_readback")
}

fn marker_attempts(marker: Option<&Value>) -> i64 {
    marker
        .and_then(|marker| marker["attempts"].as_i64())
        .unwrap_or(0)
        .max(0)
}

fn observation_semantic_digest(payload: &Value) -> Result<String> {
    let mut semantic = payload.clone();
    if let Some(object) = semantic.as_object_mut() {
        object.remove("evidence_digest");
        object.remove("observed_at_ms");
    }
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(&semantic)?.as_bytes())
    ))
}

fn reusable_observation_id(
    tx: &Transaction<'_>,
    inserted_id: i64,
    claim: &ReadbackClaim,
    semantic_digest: &str,
) -> Result<Option<i64>> {
    let (Some(previous_id), Some(previous_digest)) = (
        claim.previous_observation_id,
        claim.previous_semantic_digest.as_deref(),
    ) else {
        return Ok(None);
    };
    if previous_digest != semantic_digest || previous_id == inserted_id {
        return Ok(None);
    }
    let payload: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM observations WHERE observation_id=?1 \
             AND binding_id=?2 AND binding_generation=?3 AND kind='native.mcp.capability_readback'",
            params![
                previous_id,
                claim.snapshot.binding_id,
                claim.snapshot.binding_generation,
            ],
            |row| row.get(0),
        )
        .optional()?;
    let Some(payload) = payload else {
        return Ok(None);
    };
    let payload: Value = serde_json::from_str(&payload)?;
    if observation_semantic_digest(&payload)? == semantic_digest {
        Ok(Some(previous_id))
    } else {
        Ok(None)
    }
}

fn failure_category(code: &str) -> &'static str {
    if matches!(
        code,
        "NATIVE_TRANSPORT" | "NATIVE_READ_FAILED" | "NATIVE_MCP_READBACK_TIMEOUT"
    ) {
        "native_service_unavailable"
    } else if matches!(
        code,
        "PRIVATE_ARTIFACT_REFERENCE" | "AUTH_ERROR" | "UNAUTHORIZED"
    ) {
        "scoped_artifact_or_credential_unavailable"
    } else if code.starts_with("STALE")
        || matches!(
            code,
            "FORBIDDEN" | "NATIVE_MCP_SCOPE_MISMATCH" | "NATIVE_MCP_WORKSPACE_LEASE_UNAVAILABLE"
        )
    {
        "assignment_scope_unavailable"
    } else {
        "native_readback_incomplete"
    }
}

fn valid_opaque_ref(value: &str, prefix: &str) -> bool {
    let Some((actual_prefix, id)) = value.split_once(':') else {
        return false;
    };
    actual_prefix == prefix
        && uuid::Uuid::parse_str(id).is_ok_and(|parsed| parsed.to_string() == id)
}

fn text_at<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .ok_or_else(stale_readback)
}

fn positive_at(value: &Value, field: &str) -> Result<i64> {
    value
        .get(field)
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(stale_readback)
}

fn stale_readback() -> Error {
    Error::new(
        "NATIVE_MCP_SCOPE_MISMATCH",
        "retained launch is no longer the exact current native MCP assignment",
    )
}

fn snapshot_identity(snapshot: &LaunchSnapshot) -> Value {
    json!({
        "operation_id":snapshot.operation_id,
        "client_id":snapshot.client_id,
        "credential_ref":snapshot.credential_ref,
        "profile_config_ref":snapshot.profile_config_ref,
        "registration_operation_id":snapshot.registration_operation_id,
        "grant_revision":snapshot.grant_revision,
        "task_id":snapshot.task_id,
        "task_revision":snapshot.task_revision,
        "attempt_id":snapshot.attempt_id,
        "binding_id":snapshot.binding_id,
        "binding_generation":snapshot.binding_generation,
        "profile_name":snapshot.profile_name,
        "surface":snapshot.surface,
        "surface_facts":snapshot.surface_facts,
        "route_json":snapshot.route_json,
    })
}

fn clone_snapshot(snapshot: &LaunchSnapshot) -> LaunchSnapshot {
    LaunchSnapshot {
        operation_id: snapshot.operation_id.clone(),
        client_id: snapshot.client_id.clone(),
        credential_ref: snapshot.credential_ref.clone(),
        profile_config_ref: snapshot.profile_config_ref.clone(),
        registration_operation_id: snapshot.registration_operation_id.clone(),
        grant_revision: snapshot.grant_revision,
        task_id: snapshot.task_id.clone(),
        task_revision: snapshot.task_revision,
        attempt_id: snapshot.attempt_id.clone(),
        binding_id: snapshot.binding_id.clone(),
        binding_generation: snapshot.binding_generation,
        profile_name: snapshot.profile_name.clone(),
        surface: snapshot.surface.clone(),
        surface_facts: snapshot.surface_facts.clone(),
        route_json: snapshot.route_json.clone(),
        options: snapshot.options.clone(),
    }
}

fn clone_claim(claim: &ReadbackClaim) -> ReadbackClaim {
    ReadbackClaim {
        snapshot: clone_snapshot(&claim.snapshot),
        attempt: claim.attempt,
        started_at_ms: claim.started_at_ms,
        previous_observation_id: claim.previous_observation_id,
        previous_semantic_digest: claim.previous_semantic_digest.clone(),
        first_observed_at_ms: claim.first_observed_at_ms,
    }
}
