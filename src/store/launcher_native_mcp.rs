//! Bounded host-side readback for launches awaiting native MCP evidence.
//!
//! The Store validates the retained launch and authenticates the scoped
//! Participant before the native adapter is contacted. This path only records
//! the facts exposed by OpenCode's public read API; it cannot permit dispatch.

use super::{Store, meta};
use crate::{
    config::{Config, McpConfig, McpToolProfile},
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
    current_authority_facts: Value,
    lease_facts: Value,
    route_json: String,
    options: Options,
    owned_service: Option<OwnedServiceExpectation>,
}

/// Exact process identity retained by the Store-owned startup row. The
/// connection options remain private to this in-memory consumer path.
#[derive(Clone)]
pub(crate) struct OwnedServiceExpectation {
    process_id: u32,
    process_birth_token: String,
    executable_sha256: String,
    identity_digest: String,
    proof_digest: String,
    provider_auth_required: bool,
}

impl OwnedServiceExpectation {
    pub(crate) fn process_id(&self) -> u32 {
        self.process_id
    }

    pub(crate) fn process_birth_token(&self) -> &str {
        &self.process_birth_token
    }

    pub(crate) fn executable_sha256(&self) -> &str {
        &self.executable_sha256
    }

    pub(crate) fn proof_digest(&self) -> &str {
        &self.proof_digest
    }

    pub(crate) fn provider_auth_required(&self) -> bool {
        self.provider_auth_required
    }

    fn identity_digest(&self) -> &str {
        &self.identity_digest
    }
}

/// Opaque, immutable proof that a launch is still in the exact pre-dispatch
/// native-MCP readback phase. Raw route and lease facts stay Store-private;
/// consumers receive only the bounded fields needed to contact the adapter.
pub(crate) struct NativeMcpLaunchSnapshot {
    snapshot: LaunchSnapshot,
    identity_digest: String,
}

impl NativeMcpLaunchSnapshot {
    pub(crate) fn launch_operation_id(&self) -> &str {
        &self.snapshot.operation_id
    }

    pub(crate) fn participant_id(&self) -> &str {
        &self.snapshot.client_id
    }

    pub(crate) fn binding_id(&self) -> &str {
        &self.snapshot.binding_id
    }

    pub(crate) fn binding_generation(&self) -> i64 {
        self.snapshot.binding_generation
    }

    pub(crate) fn credential_ref(&self) -> &str {
        &self.snapshot.credential_ref
    }

    pub(crate) fn profile_config_ref(&self) -> &str {
        &self.snapshot.profile_config_ref
    }

    pub(crate) fn options(&self) -> &Options {
        &self.snapshot.options
    }

    pub(crate) fn owned_service_expectation(&self) -> Option<OwnedServiceExpectation> {
        self.snapshot.owned_service.clone()
    }

    pub(crate) fn identity_digest(&self) -> Result<String> {
        Ok(self.identity_digest.clone())
    }

    /// Reconstruct the typed attempt-owner scope from this freshly validated
    /// Store snapshot. This uses the current ready binding root and grant
    /// facts already checked by `validate_launch_snapshot`; it is not a
    /// Participant Principal constructor or an external request decoder.
    pub(crate) fn assignment_context(&self) -> Result<crate::native_mcp::AssignmentContext> {
        let snapshot = &self.snapshot;
        let native_session_id = snapshot.current_authority_facts["binding"]["native_root_id"]
            .as_str()
            .filter(|value| !value.is_empty())
            .ok_or_else(stale_readback)?
            .to_owned();
        crate::native_mcp::AssignmentContext::new(crate::native_mcp::AssignmentSeed {
            task_id: snapshot.task_id.clone(),
            task_revision: snapshot.task_revision,
            attempt_id: snapshot.attempt_id.clone(),
            binding_id: snapshot.binding_id.clone(),
            binding_generation: snapshot.binding_generation,
            native_session_id,
            participant_id: snapshot.client_id.clone(),
            profile: McpToolProfile::Participant,
            grant_revision: snapshot.grant_revision,
            basis_kind: "attempt_owner".to_owned(),
            assignment_id: None,
            review_assignment_id: None,
        })
    }
}

#[derive(Clone, Copy)]
enum LaunchSnapshotPhase<'a> {
    PreDispatch,
    DispatchNotStarted,
    DispatchQueued(&'a str),
}

struct ReadbackClaim {
    snapshot: LaunchSnapshot,
    attempt: i64,
    started_at_ms: i64,
    previous_observation_id: Option<i64>,
    previous_semantic_digest: Option<String>,
    first_observed_at_ms: Option<i64>,
}

#[derive(Clone, Copy)]
enum ReadbackFailureStage {
    LaunchSnapshotValidate,
    NativeCapabilityReadback,
}

impl ReadbackFailureStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::LaunchSnapshotValidate => "launch_snapshot_validate",
            Self::NativeCapabilityReadback => "native_capability_readback",
        }
    }
}

struct ReadbackFailure {
    category: &'static str,
    code: String,
    stage: ReadbackFailureStage,
}

impl ReadbackFailure {
    fn new(code: &str, stage: ReadbackFailureStage) -> Self {
        Self {
            category: failure_category(code),
            code: safe_error_code(code),
            stage,
        }
    }
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
        let config = self.config.clone();
        let claim_outcome = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let outcome = claim_next_readback(&tx, now, &config)?;
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
        let config = self.config.clone();
        let binding_context = self
            .run(move |db| {
                let _actor = launcher::launch_actor(db, &operation_id)?;
                let (row, manifest) = load_launch_manifest(db, &operation_id)?;
                verify_claim(&row, &manifest, &context_claim)?;
                let current = validate_launch_snapshot(
                    db,
                    &row,
                    &manifest,
                    &config,
                    LaunchSnapshotPhase::PreDispatch,
                )?;
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
                Ok((assignment, current.options, current.owned_service))
            })
            .await;
        let (assignment, options, owned_service) = match binding_context {
            Ok(context) => context,
            Err(error) => return self.finish_readback_failure(&claim, &error).await,
        };

        let observation = tokio::time::timeout(NATIVE_READBACK_TIMEOUT, async {
            let service = match owned_service.as_ref() {
                Some(expected) => {
                    Service::connect_owned(
                        &options,
                        expected.process_id(),
                        expected.process_birth_token(),
                        expected.executable_sha256(),
                    )
                    .await?
                }
                None => Service::connect(&options).await?,
            };
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
        let config = self.config.clone();
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let _actor = launcher::launch_actor(&tx, &operation_id)?;
                let (row, manifest) = load_launch_manifest(&tx, &operation_id)?;
                verify_claim(&row, &manifest, &record_claim)?;
                let current = validate_launch_snapshot(
                    &tx, &row, &manifest, &config, LaunchSnapshotPhase::PreDispatch,
                )?;
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
        let failure =
            ReadbackFailure::new(&error.code, ReadbackFailureStage::NativeCapabilityReadback);
        let now = model::now_ms()?;
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let outcome = record_retry(&tx, &claim, now, &failure)?;
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

fn claim_next_readback(tx: &Transaction<'_>, now: i64, config: &Config) -> Result<ClaimOutcome> {
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
        match validate_launch_snapshot(
            tx,
            &row,
            &manifest,
            config,
            LaunchSnapshotPhase::PreDispatch,
        ) {
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
                let failure =
                    ReadbackFailure::new(&error.code, ReadbackFailureStage::LaunchSnapshotValidate);
                let marker = retry_marker(
                    attempt,
                    now,
                    &failure,
                    previous.and_then(|old| old["first_observed_at_ms"].as_i64()),
                    previous.and_then(|old| old["observation_id"].as_i64()),
                    previous
                        .and_then(|old| old["semantic_digest"].as_str())
                        .map(str::to_owned),
                );
                let mut next_manifest = manifest;
                next_manifest["native_mcp_readback"] = marker;
                next_manifest["native_mcp_latest_failure"] = latest_failure(&failure, now);
                let next_retry_at_ms =
                    next_manifest["native_mcp_readback"]["next_retry_at_ms"].as_i64();
                persist_manifest(tx, &row, &next_manifest, now)?;
                return Ok(ClaimOutcome::Deferred(json!({
                    "operation_id":operation_id,
                    "state":"retry_wait",
                    "attempt":attempt,
                    "failure_category":failure.category,
                    "last_error_code":failure.code,
                    "last_error_stage":failure.stage.as_str(),
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
    config: &Config,
    phase: LaunchSnapshotPhase<'_>,
) -> Result<LaunchSnapshot> {
    let expected_progress = match phase {
        LaunchSnapshotPhase::PreDispatch | LaunchSnapshotPhase::DispatchNotStarted => "not_started",
        LaunchSnapshotPhase::DispatchQueued(_) => "queued",
    };
    if row.method != "swarm.launch"
        || row.state != "queued"
        || manifest["state"] != "awaiting_native_mcp"
        || manifest["runtime"]["dispatch_permitted"] != false
        || manifest["progress"]["task_dispatch"] != expected_progress
    {
        return Err(stale_readback());
    }
    let actor = match phase {
        LaunchSnapshotPhase::PreDispatch => launcher::launch_actor(db, &row.operation_id)?,
        LaunchSnapshotPhase::DispatchNotStarted => {
            launcher::dispatch_launch_actor(db, &row.operation_id, None)?
        }
        LaunchSnapshotPhase::DispatchQueued(dispatch_operation_id) => {
            launcher::dispatch_launch_actor(db, &row.operation_id, Some(dispatch_operation_id))?
        }
    };
    let current_gm = meta(db, "gm")?.unwrap_or(Value::Null);
    let local_operator = meta(db, "local_operator_client_id")?.unwrap_or(Value::Null);
    let task_id = text_at(&manifest["task"], "task_id")?;
    let task_revision = positive_at(&manifest["task"], "observed_revision")?;
    let attempt_id = text_at(&manifest["task"], "attempt_id")?;
    let task = tasks::get_task(db, task_id)?;
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let binding_id = text_at(&manifest["binding"], "binding_id")?;
    let binding_generation = positive_at(&manifest["binding"], "generation")?;
    let owner_id = text_at(&attempt, "owner_id")?;
    if matches!(phase, LaunchSnapshotPhase::PreDispatch) {
        actor.require_bound_launch_attempt(
            db,
            &row.operation_id,
            task_id,
            task_revision,
            attempt_id,
            binding_id,
            binding_generation,
        )?;
    }
    if task["state"] != "open"
        || task["revision"] != task_revision
        || task["current_attempt_id"].as_str() != Some(attempt_id)
        || task["project_id"] != manifest["task"]["project_id"]
        || attempt["task_id"].as_str() != Some(task_id)
        || attempt["task_revision"] != task_revision
        || attempt["released_at_ms"].is_number()
        || attempt["binding_id"].as_str() != Some(binding_id)
        || attempt["binding_generation"] != binding_generation
        || match phase {
            LaunchSnapshotPhase::PreDispatch | LaunchSnapshotPhase::DispatchNotStarted => {
                !attempt["start_operation_id"].is_null()
            }
            LaunchSnapshotPhase::DispatchQueued(dispatch_operation_id) => {
                attempt["start_operation_id"].as_str() != Some(dispatch_operation_id)
            }
        }
        || row.task_id.as_deref() != Some(task_id)
        || row.attempt_id.as_deref() != Some(attempt_id)
        || row.binding_id.as_deref() != Some(binding_id)
        || row.binding_generation != Some(binding_generation)
        || row.caller_id.as_deref() != Some(actor.technical_requester_id())
        || owner_id != actor.effective_manager_id()
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
    if participant["role"] != "participant"
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
        || lease_view["owner_client_id"].as_str() != Some(actor.effective_manager_id())
        || lease_view["attempt_id"].as_str() != Some(attempt_id)
    {
        return Err(stale_readback());
    }

    let current_authority_facts = json!({
        "actor":launch_actor_facts(db, &actor)?,
        "local_operator_client_id":local_operator,
        "gm":{
            "client_id":current_gm["client_id"],
            "epoch":current_gm["epoch"],
        },
        "task":{
            "project_id":task["project_id"],
            "state":task["state"],
            "revision":task["revision"],
            "current_attempt_id":task["current_attempt_id"],
        },
        "attempt":{
            "task_id":attempt["task_id"],
            "task_revision":attempt["task_revision"],
            "owner_id":attempt["owner_id"],
            "start_owner":attempt["start_owner"],
            "state":attempt["state"],
            "released_at_ms":attempt["released_at_ms"],
            "binding_id":attempt["binding_id"],
            "binding_generation":attempt["binding_generation"],
        },
        "binding":{
            "state":state,
            "module_artifact_id":artifact_id,
            "native_root_id":native_root_id,
        },
        "participant_registration":{
            "role":registration["role"],
            "disabled":registration["disabled"],
            "task_id":registration["task_id"],
            "task_revision":registration["task_revision"],
            "attempt_id":registration["attempt_id"],
            "binding_id":registration["binding_id"],
            "binding_generation":registration["binding_generation"],
            "created_by":registration["created_by"],
            "created_operation_id":registration["created_operation_id"],
            "grant_revision":registration["grant_revision"],
            "participation_basis":registration["participation_basis"],
            "native_session_id":registration["native_session_id"],
        },
    });

    // An owned route must be authorized by the immutable startup row and its
    // exact process receipt. Its per-binding options come only from that
    // verified Store projection; never parse the route's external connection
    // options or treat configured ownership as a live service.
    let owned_binding =
        super::opencode::owned_service_for_binding(db, config, binding_id, binding_generation)?;
    if route
        .get("owned_service")
        .is_some_and(|value| !value.is_null() && !value.is_object())
        || route["owned_service"].is_object() != owned_binding.is_some()
    {
        return Err(stale_readback());
    }
    let (options, owned_service) = match owned_binding {
        Some(binding) => {
            let options = binding.options();
            let process_id = binding.process_id();
            let process_birth_token = binding.process_birth_token().to_owned();
            let executable_sha256 = binding.executable_sha256().to_owned();
            if options.service_id != binding.service_id()
                || options.expected_version != binding.service_version()
                || binding.service_version() != "2.0.7"
                || process_id == 0
            {
                return Err(stale_readback());
            }
            let options_digest =
                model::digest(model::canonical(&serde_json::to_value(&options)?)?.as_bytes());
            let identity_facts = json!({
                "service_id":binding.service_id(),
                "service_version":binding.service_version(),
                "owner_nonce":binding.owner_nonce(),
                "owned_binding_identity_digest":binding.identity_digest(),
                "startup_proof_digest":binding.proof_digest(),
                "process_id":process_id,
                "process_birth_token":process_birth_token,
                "executable_sha256":executable_sha256,
                "endpoint_digest":binding.endpoint_digest(),
                "connection_digest":binding.connection_digest(),
                "config_digest":binding.config_digest(),
                "plugin_module_sha256":binding.plugin_module_sha256(),
                "options_digest":options_digest,
            });
            let identity_digest = format!(
                "sha256:{}",
                model::digest(model::canonical(&identity_facts)?.as_bytes())
            );
            let expected = OwnedServiceExpectation {
                process_id,
                process_birth_token,
                executable_sha256,
                identity_digest,
                proof_digest: binding.proof_digest().to_owned(),
                provider_auth_required: binding.route().credential_ref().is_some(),
            };
            (options, Some(expected))
        }
        None => (
            Options::parse(&route["native_options"]).map_err(|_| stale_readback())?,
            None,
        ),
    };
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
        current_authority_facts,
        lease_facts: lease_view,
        route_json,
        options,
        owned_service,
    })
}

fn launch_actor_facts(db: &Connection, actor: &launcher::LaunchActor) -> Result<Value> {
    let role = match actor.role() {
        Role::Manager => "manager",
        Role::Operator => "operator",
        _ => return Err(stale_readback()),
    };
    let direct_registration = if let Some(principal) = actor.direct_principal() {
        let registration =
            meta(db, &format!("client:{}", principal.client_id))?.ok_or_else(stale_readback)?;
        let registered_role: Role =
            serde_json::from_value(registration["role"].clone()).map_err(|_| stale_readback())?;
        if registration["disabled"] == true || registered_role != principal.role {
            return Err(stale_readback());
        }
        json!({
            "client_id":principal.client_id,
            "role":registration["role"],
            "disabled":registration["disabled"],
            "link_id":actor.link_id(),
        })
    } else {
        Value::Null
    };
    let work_dispatch = actor
        .work_dispatch_context()
        .map(|context| context.linkage_value())
        .unwrap_or(Value::Null);
    Ok(json!({
        "kind":if actor.work_dispatch_context().is_some() {"work_dispatch"} else {"direct"},
        "technical_requester_id":actor.technical_requester_id(),
        "effective_manager_id":actor.effective_manager_id(),
        "role":role,
        "link_id":actor.link_id(),
        "direct_registration":direct_registration,
        "work_dispatch":work_dispatch,
    }))
}

/// Capture an opaque pre-dispatch launch snapshot. The existing validator
/// remains authoritative for the queued phase, live actor rights, current
/// Task/Attempt/Binding, Participant grant and held workspace lease.
pub(crate) fn current_mcp_launch_snapshot(
    db: &Connection,
    config: &Config,
    operation_id: &str,
) -> Result<NativeMcpLaunchSnapshot> {
    if operation_id.is_empty()
        || operation_id.len() > 256
        || operation_id.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(Error::invalid("launch Operation ID is invalid"));
    }
    let (row, manifest) = load_launch_manifest(db, operation_id)?;
    let snapshot = validate_launch_snapshot(
        db,
        &row,
        &manifest,
        config,
        LaunchSnapshotPhase::PreDispatch,
    )?;
    let identity_digest = launch_identity_digest(&snapshot)?;
    Ok(NativeMcpLaunchSnapshot {
        snapshot,
        identity_digest,
    })
}

/// Capture the launch scope at the C10 admission or pre-effect stage. `None`
/// accepts only the unstarted bound Attempt; `Some` accepts only the exact
/// queued launch-owned task.dispatch child. The legacy pre-dispatch snapshot
/// remains unchanged for C7/C8 callers.
pub(crate) fn current_mcp_launch_snapshot_for_dispatch(
    db: &Connection,
    config: &Config,
    launch_operation_id: &str,
    dispatch_operation_id: Option<&str>,
) -> Result<NativeMcpLaunchSnapshot> {
    if launch_operation_id.is_empty()
        || launch_operation_id.len() > 256
        || launch_operation_id
            .bytes()
            .any(|byte| byte.is_ascii_control())
        || dispatch_operation_id.is_some_and(|operation_id| {
            operation_id.is_empty()
                || operation_id.len() > 256
                || operation_id.bytes().any(|byte| byte.is_ascii_control())
        })
    {
        return Err(Error::invalid("launch or dispatch Operation ID is invalid"));
    }
    let (row, manifest) = load_launch_manifest(db, launch_operation_id)?;
    let phase = dispatch_operation_id
        .map(LaunchSnapshotPhase::DispatchQueued)
        .unwrap_or(LaunchSnapshotPhase::DispatchNotStarted);
    let snapshot = validate_launch_snapshot(db, &row, &manifest, config, phase)?;
    let identity_digest = launch_identity_digest(&snapshot)?;
    Ok(NativeMcpLaunchSnapshot {
        snapshot,
        identity_digest,
    })
}

/// Re-read the exact pre-dispatch scope and reject any stale snapshot. No
/// post-dispatch or settled launch state is accepted by this interface.
pub(crate) fn revalidate_mcp_launch_snapshot(
    db: &Connection,
    config: &Config,
    snapshot: &NativeMcpLaunchSnapshot,
) -> Result<()> {
    let current = current_mcp_launch_snapshot(db, config, &snapshot.snapshot.operation_id)?;
    if current.identity_digest != snapshot.identity_digest {
        return Err(stale_readback());
    }
    Ok(())
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
    failure: &ReadbackFailure,
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
        failure,
        claim.first_observed_at_ms,
        claim.previous_observation_id,
        claim.previous_semantic_digest.clone(),
    );
    let next_retry_at_ms = marker["next_retry_at_ms"].as_i64();
    let mut next_manifest = manifest;
    next_manifest["native_mcp_readback"] = marker;
    next_manifest["native_mcp_latest_failure"] = latest_failure(failure, now);
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
    failure: &ReadbackFailure,
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
        "last_failure_category":failure.category,
        "last_error_code":failure.code,
        "last_error_stage":failure.stage.as_str(),
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

fn safe_error_code(code: &str) -> String {
    if !code.is_empty()
        && code.len() <= 64
        && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    {
        // Error codes are bounded identifiers. Do not persist Error::message,
        // native response content, or any private request detail.
        code.to_owned()
    } else {
        "NATIVE_MCP_READBACK_ERROR".to_owned()
    }
}

fn latest_failure(failure: &ReadbackFailure, recorded_at_ms: i64) -> Value {
    json!({
        "schema_version":1,
        "code":failure.code,
        "stage":failure.stage.as_str(),
        "recorded_at_ms":recorded_at_ms,
        "category":failure.category,
    })
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
        "current_authority_facts":snapshot.current_authority_facts,
        "lease_facts":snapshot.lease_facts,
        "route_json":snapshot.route_json,
        "owned_service_identity":snapshot
            .owned_service
            .as_ref()
            .map(OwnedServiceExpectation::identity_digest),
    })
}

fn launch_identity_digest(snapshot: &LaunchSnapshot) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(&snapshot_identity(snapshot))?.as_bytes())
    ))
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
        current_authority_facts: snapshot.current_authority_facts.clone(),
        lease_facts: snapshot.lease_facts.clone(),
        route_json: snapshot.route_json.clone(),
        options: snapshot.options.clone(),
        owned_service: snapshot.owned_service.clone(),
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

#[cfg(test)]
#[path = "launcher_native_mcp_failure_tests.rs"]
mod failure_tests;
