//! Write-ahead admission and private readback for launch-scoped native MCP
//! installation and tools evidence.
//!
//! This module is intentionally separate from the C7 launch readback path.
//! The public entrypoint accepts only a retained launch Operation ID; the
//! opaque C7 snapshot factory revalidates the exact manager, Task/Attempt,
//! held lease, binding generation, Participant registration, and route. All
//! restart-sensitive challenge material and raw tool schemas stay in the
//! Store's private `meta` table. A native tool inventory is evidence, never a
//! dispatch or model-consumption authorization.

use super::{Store, meta, set_meta};
use crate::{
    config::Config,
    error::{Error, NativeRpcRejectionClass, Result},
    model::{self, Credential, Principal, Role},
    participant_credentials, platform,
    runtime::opencode_v2::{Options, Service},
    store::{launcher_native_mcp, native_mcp},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

const RECORD_KEY_PREFIX: &str = "launcher:native_mcp_tools:v1:";
const SUPERVISOR_KEY_PREFIX: &str = "launcher:native_mcp_tools:supervisor:v1:";
const SUPERVISOR_CURSOR_KEY: &str = "launcher:native_mcp_tools:supervisor:v1:cursor";
const OPENCODE_VERSION: &str = "2.0.7";
const NATIVE_ACTION_TIMEOUT: Duration = Duration::from_secs(55);
const MAX_PRIVATE_READBACK_BYTES: usize = 1024 * 1024;
const MAX_REPLACED_CHALLENGES: usize = 16;
const MAX_CHALLENGE_TTL_MS: i64 = 120_000;
const MAX_CANDIDATES_PER_PASS: i64 = 64;
const INITIAL_RETRY_MS: i64 = 15_000;
const MAX_RETRY_MS: i64 = 300_000;
// A claim covers at most thirteen pinned HTTP requests: install registration
// (including its exact readback) or the read-only challenge preflight plus its
// one arm and exact verification. The adapter applies no HTTP retry and each
// request has a ten-second timeout; runtime effect calls are additionally
// bounded by NATIVE_ACTION_TIMEOUT.
// Thirteen requests at the adapter's ten-second per-request ceiling is a
// conservative 130-second API bound; the additional fifty seconds keeps a
// live claim from being replaced during that bounded pass. The write-ahead
// outcome_unknown phase independently prevents an expired claim from
// replaying a PUT or challenge arm.
const CLAIM_STALE_MS: i64 = 180_000;

#[derive(Clone)]
struct ToolsClaim {
    operation_id: String,
    claim_generation: i64,
    started_at_ms: i64,
    identity_digest: String,
}

#[derive(Clone, Copy)]
enum ChallengeReplacementReason {
    Expired,
    ModuleRotated,
}

enum ToolsClaimOutcome {
    Claimed(ToolsClaim),
    Idle(Option<i64>),
}

#[derive(Clone)]
struct LaunchFacts {
    operation_id: String,
    identity_digest: String,
    assignment: crate::native_mcp::AssignmentContext,
    participant_id: String,
    credential_ref: String,
    profile_config_ref: String,
    options: Options,
    owned_service: Option<launcher_native_mcp::OwnedServiceExpectation>,
    config: Arc<Config>,
}

impl Store {
    /// Claim and advance at most one due C7 launch candidate. The Store owns a
    /// separate C8 backoff marker so the two-second host tick cannot repeat a
    /// slow native call. First C8 work is eligible as soon as C7 records an
    /// `observed_partial` readback; C7's five-minute refresh delay is not
    /// reused as an installation delay.
    pub(crate) async fn reconcile_native_mcp_tools_once(&self) -> Result<Value> {
        let now = model::now_ms()?;
        let config = self.config.clone();
        let outcome = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let outcome = claim_next_tools(&tx, now, &config)?;
                tx.commit()?;
                Ok(outcome)
            })
            .await?;
        let claim = match outcome {
            ToolsClaimOutcome::Claimed(claim) => claim,
            ToolsClaimOutcome::Idle(next_retry_at_ms) => {
                return Ok(json!({
                    "state":"idle",
                    "next_retry_at_ms":next_retry_at_ms,
                    "dispatch_permitted":false,
                }));
            }
        };
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));

        let progress = self
            .advance_native_mcp_tools_once(&claim.operation_id)
            .await;
        let now = model::now_ms()?;
        let error_code = progress.as_ref().err().map(|error| error.code.clone());
        let progress_value = progress.ok();
        let result = self
            .finish_tools_claim(&claim, error_code, progress_value, now)
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(result)
    }

    /// Advance one claimed exact launch. This inner method is not a scheduler
    /// entrypoint; all host callers must pass through the durable due claim.
    async fn advance_native_mcp_tools_once(&self, launch_operation_id: &str) -> Result<Value> {
        let operation_id = launch_operation_id.to_owned();
        let config = self.config.clone();
        let facts = self
            .run(move |db| load_launch_facts(db, &config, &operation_id))
            .await?;

        let (credential, first_principal, assignment, prepared) =
            self.prepare_install(&facts).await?;
        if credential.client_id != facts.participant_id
            || first_principal.role != Role::Participant
            || first_principal.client_id != facts.participant_id
        {
            return Err(scope_error(
                "resolved native MCP credential is not the exact launch Participant",
            ));
        }

        // Connection is read-only. It provides a process identity that is
        // retained before any per-location PUT and compared on every resume.
        let (service, _) = self
            .connect_current(&facts, &credential, &assignment, None)
            .await?;
        let install_identity = prepared.identity();
        let initial = self
            .admit_install(
                &facts,
                &credential,
                &assignment,
                &install_identity,
                service.pid,
                &service.version,
            )
            .await?;

        if let Some(module_operation_id) = initial["install"]["module_operation_id"].as_str() {
            let module_operation_phase = initial["install"]["module_operation_phase"]
                .as_str()
                .ok_or_else(|| record_error("native MCP install operation phase is missing"))?;
            if self
                .consume_native_mcp_operation(
                    &facts,
                    &credential,
                    &assignment,
                    module_operation_id,
                    module_operation_phase,
                    &install_identity,
                    service.pid,
                    &service.version,
                )
                .await?
            {
                return self.summary(&facts, &credential, &assignment).await;
            }
        }

        match initial["install"]["state"].as_str() {
            Some("prepared") => {
                let command = prepared.native_mcp_command(
                    "install",
                    assignment.binding_id(),
                    assignment.binding_generation(),
                    service.pid,
                )?;
                if self
                    .reserve_install_effect(&facts, &credential, &assignment, command)
                    .await?
                {
                    // The reservation and module Operation are committed
                    // together. The independent adapter will perform the
                    // one PUT and retain its RuntimeOutcome in that child
                    // Operation; this tick only reports the queued phase.
                    return self.summary(&facts, &credential, &assignment).await;
                }
                // The phase changed to `outcome_unknown` before any possible
                // network effect. The next call must use observe-only.
                return self.summary(&facts, &credential, &assignment).await;
            }
            Some("outcome_unknown") => {
                // A lost/uncertain PUT is never sent again. Read only the
                // uniquely derived name at its original process/location.
                let target = install_target(&initial)?;
                let command = prepared.native_mcp_command(
                    "observe",
                    assignment.binding_id(),
                    assignment.binding_generation(),
                    target.0,
                )?;
                self.queue_install_observation(
                    &facts,
                    &credential,
                    &assignment,
                    command,
                    "observe_unknown",
                )
                .await?;
                return self.summary(&facts, &credential, &assignment).await;
            }
            Some("registered") => {}
            Some("observed_after_unknown") => {
                // Name/status visibility cannot prove which command an
                // ambiguous add-or-replace request installed. Preserve that
                // limitation instead of promoting the recovered entry.
                return self.summary(&facts, &credential, &assignment).await;
            }
            _ => return Err(record_error("native MCP install phase is invalid")),
        }

        let registered_target = install_target(&initial)?;
        let (service, _) = self
            .connect_current(
                &facts,
                &credential,
                &assignment,
                Some((registered_target.0, registered_target.1.as_str())),
            )
            .await?;
        if initial["install"]["readback"]["runtime_status"] != "connected" {
            // Connection state is refreshable by GET, but it never causes a
            // second PUT. A recovered status also cannot prove the local
            // command/config because the pinned API does not expose it.
            let command = prepared.native_mcp_command(
                "observe",
                assignment.binding_id(),
                assignment.binding_generation(),
                registered_target.0,
            )?;
            self.queue_install_observation(
                &facts,
                &credential,
                &assignment,
                command,
                "observe_refresh",
            )
            .await?;
            return self.summary(&facts, &credential, &assignment).await;
        }

        let record = self.load_record(&facts, &credential, &assignment).await?;
        if let Some(module_operation_id) = record["challenge"]["module_operation_id"].as_str() {
            let module_operation_phase = record["challenge"]["module_operation_phase"]
                .as_str()
                .ok_or_else(|| record_error("native MCP challenge operation phase is missing"))?;
            if self
                .consume_native_mcp_operation(
                    &facts,
                    &credential,
                    &assignment,
                    module_operation_id,
                    module_operation_phase,
                    &install_identity,
                    service.pid,
                    &service.version,
                )
                .await?
            {
                return self.summary(&facts, &credential, &assignment).await;
            }
        }
        match record["challenge"]["state"].as_str() {
            Some("not_started") => {
                let (_, current_assignment) = self
                    .current_scope(&facts, &credential, Some(&assignment))
                    .await?;
                let challenge = crate::runtime::opencode_v2::mcp_tools::new_challenge(
                    current_assignment,
                    &service,
                    &facts.options,
                )?;
                let metadata =
                    crate::runtime::opencode_v2::mcp_tools::challenge_metadata(&challenge);
                self.record_challenge_intent(
                    &facts,
                    &credential,
                    &assignment,
                    metadata,
                    service.pid,
                    &service.version,
                )
                .await?;
                return self.summary(&facts, &credential, &assignment).await;
            }
            Some("prepared") => {
                let challenge_metadata = record["challenge"]["metadata"].clone();
                let challenge =
                    match crate::runtime::opencode_v2::mcp_tools::restore_challenge_metadata(
                        assignment.clone(),
                        &service,
                        &facts.options,
                        challenge_metadata.clone(),
                    ) {
                        Ok(challenge) => challenge,
                        Err(error) if challenge_replacement_reason(&error.code).is_some() => {
                            let reason =
                                challenge_replacement_reason(&error.code).ok_or_else(|| {
                                    record_error("challenge replacement reason disappeared")
                                })?;
                            return self
                                .replace_prepared_challenge(
                                    &facts,
                                    &credential,
                                    &assignment,
                                    &service,
                                    challenge_metadata,
                                    reason,
                                )
                                .await;
                        }
                        Err(error) => return Err(error),
                    };
                self.current_scope(&facts, &credential, Some(&assignment))
                    .await?;
                self.verify_current_service(
                    &facts,
                    &credential,
                    &assignment,
                    (service.pid, service.version.as_str()),
                    &service,
                )
                .await?;
                let prepared_arm = match crate::runtime::opencode_v2::mcp_tools::prepare_arm(
                    &service,
                    &facts.options,
                    &challenge,
                ) {
                    Ok(prepared_arm) => prepared_arm,
                    Err(error) if challenge_replacement_reason(&error.code).is_some() => {
                        let reason =
                            challenge_replacement_reason(&error.code).ok_or_else(|| {
                                record_error("challenge replacement reason disappeared")
                            })?;
                        return self
                            .replace_prepared_challenge(
                                &facts,
                                &credential,
                                &assignment,
                                &service,
                                challenge_metadata,
                                reason,
                            )
                            .await;
                    }
                    Err(error) => {
                        self.record_safe_error(
                            &facts,
                            &credential,
                            &assignment,
                            "challenge_preflight",
                            &error.code,
                        )
                        .await?;
                        return self.summary(&facts, &credential, &assignment).await;
                    }
                };
                // Preflight is read-only. Revalidate the exact assignment and
                // process after it, then persist the one-shot reservation.
                self.current_scope(&facts, &credential, Some(&assignment))
                    .await?;
                self.verify_current_service(
                    &facts,
                    &credential,
                    &assignment,
                    (service.pid, service.version.as_str()),
                    &service,
                )
                .await?;
                let command = prepared_arm.native_mcp_command(
                    "arm",
                    assignment.binding_id(),
                    assignment.binding_generation(),
                )?;
                if self
                    .reserve_challenge_effect(&facts, &credential, &assignment, command)
                    .await?
                {
                    // The arm POST is now issued only by the module worker;
                    // this tick has committed its one-shot reservation.
                    return self.summary(&facts, &credential, &assignment).await;
                }
                return self.summary(&facts, &credential, &assignment).await;
            }
            Some("armed") | Some("outcome_unknown") => {
                // For an uncertain arm, only the retained challenge may be
                // read. A new nonce or another arm is never synthesized.
                let target = challenge_target(&record)?;
                let (read_service, _) = self
                    .connect_current(
                        &facts,
                        &credential,
                        &assignment,
                        Some((target.0, target.1.as_str())),
                    )
                    .await?;
                let challenge = crate::runtime::opencode_v2::mcp_tools::restore_challenge_metadata(
                    assignment.clone(),
                    &read_service,
                    &facts.options,
                    record["challenge"]["metadata"].clone(),
                )?;
                self.current_scope(&facts, &credential, Some(&assignment))
                    .await?;
                self.verify_current_service(
                    &facts,
                    &credential,
                    &assignment,
                    (target.0, target.1.as_str()),
                    &read_service,
                )
                .await?;
                let prepared_arm = crate::runtime::opencode_v2::mcp_tools::prepare_arm(
                    &read_service,
                    &facts.options,
                    &challenge,
                )?;
                let command = prepared_arm.native_mcp_command(
                    "read",
                    assignment.binding_id(),
                    assignment.binding_generation(),
                )?;
                self.queue_challenge_read(&facts, &credential, &assignment, command)
                    .await?;
                return self.summary(&facts, &credential, &assignment).await;
            }
            Some("observed") => self.summary(&facts, &credential, &assignment).await,
            _ => Err(record_error("native MCP challenge phase is invalid")),
        }
    }

    async fn finish_tools_claim(
        &self,
        claim: &ToolsClaim,
        error_code: Option<String>,
        progress: Option<Value>,
        now: i64,
    ) -> Result<Value> {
        let claim = claim.clone();
        let config = self.config.clone();
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let key = supervisor_key(&claim.operation_id);
            let mut schedule = meta(&tx, &key)?
                .ok_or_else(|| record_error("native MCP tools claim disappeared"))?;
            if schedule["state"] != "running"
                || schedule["claim_generation"].as_i64() != Some(claim.claim_generation)
                || schedule["started_at_ms"].as_i64() != Some(claim.started_at_ms)
                || schedule["launch_identity_digest"].as_str()
                    != Some(claim.identity_digest.as_str())
            {
                tx.commit()?;
                return Ok(json!({
                    "state":"superseded",
                    "operation_id":claim.operation_id,
                    "claim_generation":claim.claim_generation,
                    "dispatch_permitted":false,
                }));
            }

            let current = match load_launch_facts(&tx, &config, &claim.operation_id) {
                Ok(current) if current.identity_digest == claim.identity_digest => true,
                Ok(_) => false,
                Err(error) if is_stale_scope_code(&error.code) => false,
                Err(error) => return Err(error),
            };
            let stale_error = error_code
                .as_ref()
                .is_some_and(|code| is_stale_scope_code(code));
            let direct_error_code = error_code.clone();
            let result = match progress {
                Some(value) => value,
                None => json!({
                    "state":"error",
                    "error_code":error_code.as_deref().map(safe_error_code),
                    "dispatch_permitted":false,
                }),
            };
            let error_code =
                error_code.or_else(|| result["last_error"]["code"].as_str().map(str::to_owned));
            let current_record_error = result["last_error"]["recorded_at_ms"]
                .as_i64()
                .is_some_and(|recorded_at_ms| recorded_at_ms >= claim.started_at_ms)
                .then(|| result["last_error"]["code"].as_str().map(str::to_owned))
                .flatten();
            let event_error_code = direct_error_code
                .or(current_record_error)
                .or_else(|| {
                    (result["install_state"] == "outcome_unknown"
                        || result["observer_state"] == "outcome_unknown")
                        .then(|| "NATIVE_OUTCOME_UNKNOWN".to_owned())
                })
                .or_else(|| (!current).then(|| "STALE_LAUNCH".to_owned()));

            if !current || stale_error {
                schedule["state"] = json!("stale");
                schedule["started_at_ms"] = Value::Null;
                schedule["next_retry_at_ms"] = Value::Null;
                schedule["last_error_code"] = error_code
                    .as_deref()
                    .filter(|code| is_stale_scope_code(code))
                    .map_or(Value::Null, |code| json!(code));
                schedule["finished_at_ms"] = json!(now);
                set_meta(&tx, &key, &schedule)?;
                if let Some(code) = event_error_code.as_deref() {
                    launcher_native_mcp::insert_safe_failure_observation(
                        &tx,
                        &claim.operation_id,
                        "native_mcp_tools",
                        "tools_retry",
                        claim.claim_generation,
                        code,
                        now,
                    )?;
                }
                tx.commit()?;
                return Ok(json!({
                    "state":"stale",
                    "operation_id":claim.operation_id,
                    "claim_generation":claim.claim_generation,
                    "dispatch_permitted":false,
                }));
            }

            let install_state = result["install_state"].as_str().unwrap_or_default();
            let runtime_status = result["runtime_status"].as_str().unwrap_or_default();
            let challenge_state = result["observer_state"].as_str().unwrap_or_default();
            let terminal =
                challenge_state == "observed" || install_state == "observed_after_unknown";
            let waiting = error_code.is_some()
                || install_state == "outcome_unknown"
                || install_state == "registered" && runtime_status != "connected"
                || challenge_state == "outcome_unknown";
            if terminal {
                schedule["state"] = json!("observed_partial");
                schedule["next_retry_at_ms"] = Value::Null;
                schedule["failure_attempts"] = json!(0);
                schedule["last_error_code"] = Value::Null;
            } else if waiting {
                let failures = schedule["failure_attempts"]
                    .as_i64()
                    .unwrap_or(0)
                    .saturating_add(1)
                    .max(1);
                let next_retry = now.saturating_add(tools_retry_delay_ms(failures));
                schedule["state"] = json!("retry_wait");
                schedule["failure_attempts"] = json!(failures);
                schedule["next_retry_at_ms"] = json!(next_retry);
                schedule["last_error_code"] = error_code.as_deref().map_or(Value::Null, |code| {
                    json!(safe_label(code).unwrap_or_else(|_| "NATIVE_MCP_ERROR".to_owned()))
                });
                if let Some(code) = event_error_code.as_deref() {
                    launcher_native_mcp::insert_safe_failure_observation(
                        &tx,
                        &claim.operation_id,
                        "native_mcp_tools",
                        "tools_retry",
                        claim.claim_generation,
                        code,
                        now,
                    )?;
                }
            } else {
                // A successful phase transition is eligible on the next host
                // tick. This advances install -> challenge -> read while each
                // individual phase is executed at most once per claim.
                schedule["state"] = json!("ready");
                schedule["failure_attempts"] = json!(0);
                schedule["next_retry_at_ms"] = json!(now);
                schedule["last_error_code"] = Value::Null;
            }
            schedule["started_at_ms"] = Value::Null;
            schedule["finished_at_ms"] = json!(now);
            let next_retry_at_ms = schedule["next_retry_at_ms"].clone();
            set_meta(&tx, &key, &schedule)?;
            tx.commit()?;
            Ok(json!({
                "state":schedule["state"],
                "operation_id":claim.operation_id,
                "claim_generation":claim.claim_generation,
                "next_retry_at_ms":next_retry_at_ms,
                "progress":result,
                "dispatch_permitted":false,
            }))
        })
        .await
    }

    async fn prepare_install(
        &self,
        facts: &LaunchFacts,
    ) -> Result<(
        Credential,
        Principal,
        crate::native_mcp::AssignmentContext,
        crate::runtime::opencode_v2::mcp_install::PreparedMcpInstall,
    )> {
        let (credential, principal, assignment) =
            self.current_scope_with_credential(facts, None).await?;
        let config = self.config.clone();
        let options = facts.options.clone();
        let assignment_for_prepare = assignment.clone();
        let credential_ref = facts.credential_ref.clone();
        let profile_ref = facts.profile_config_ref.clone();
        let prepared = self
            .file_io(move |_| {
                crate::runtime::opencode_v2::mcp_install::prepare(
                    &config,
                    &options,
                    assignment_for_prepare,
                    &credential_ref,
                    &profile_ref,
                )
            })
            .await?;
        // Re-authenticate after private artifact resolution so a disabled or
        // replaced Participant cannot carry a stale assignment into admission.
        let (principal_after, assignment_after) = self
            .current_scope_with_existing_credential(facts, &credential, Some(&assignment))
            .await?;
        if principal_after.client_id != principal.client_id || assignment_after != assignment {
            return Err(stale_scope());
        }
        Ok((credential, principal_after, assignment_after, prepared))
    }

    async fn current_scope_with_credential(
        &self,
        facts: &LaunchFacts,
        expected: Option<&crate::native_mcp::AssignmentContext>,
    ) -> Result<(Credential, Principal, crate::native_mcp::AssignmentContext)> {
        let config = self.config.clone();
        let credential_ref = facts.credential_ref.clone();
        let profile_ref = facts.profile_config_ref.clone();
        let expected_participant = facts.participant_id.clone();
        let credential = self
            .file_io(move |_| {
                let refs =
                    participant_credentials::resolve_refs(&config, &credential_ref, &profile_ref)?;
                platform::load_credential(refs.credential_path())
            })
            .await?;
        if credential.client_id != expected_participant {
            return Err(scope_error(
                "resolved credential is not the retained launch Participant",
            ));
        }
        let (principal, assignment) = self
            .current_scope_with_existing_credential(facts, &credential, expected)
            .await?;
        Ok((credential, principal, assignment))
    }

    async fn current_scope_with_existing_credential(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        expected: Option<&crate::native_mcp::AssignmentContext>,
    ) -> Result<(Principal, crate::native_mcp::AssignmentContext)> {
        let (principal, assignment) = self.current_scope(facts, credential, expected).await?;
        Ok((principal, assignment))
    }

    async fn current_scope(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        expected: Option<&crate::native_mcp::AssignmentContext>,
    ) -> Result<(Principal, crate::native_mcp::AssignmentContext)> {
        let principal = self.authenticate(credential.clone()).await?;
        if principal.role != Role::Participant || principal.client_id != facts.participant_id {
            return Err(scope_error(
                "authenticated principal is not the exact launch Participant",
            ));
        }
        let facts = facts.clone();
        let principal_for_db = principal.clone();
        let expected_assignment = expected.cloned();
        let expected_options = facts.options.clone();
        let (assignment, options) = self
            .run(move |db| {
                let snapshot = launcher_native_mcp::current_mcp_launch_snapshot(
                    db,
                    &facts.config,
                    &facts.operation_id,
                )?;
                launcher_native_mcp::revalidate_mcp_launch_snapshot(db, &facts.config, &snapshot)?;
                if snapshot.launch_operation_id() != facts.operation_id
                    || snapshot.identity_digest()? != facts.identity_digest
                    || snapshot.participant_id() != facts.participant_id
                    || snapshot.credential_ref() != facts.credential_ref
                    || snapshot.profile_config_ref() != facts.profile_config_ref
                    || model::canonical(&serde_json::to_value(snapshot.options())?)?
                        != model::canonical(&serde_json::to_value(&facts.options)?)?
                    || snapshot.owned_service_expectation().as_ref().map(|value| {
                        (
                            value.process_id(),
                            value.process_birth_token().to_owned(),
                            value.executable_sha256().to_owned(),
                        )
                    }) != facts.owned_service.as_ref().map(|value| {
                        (
                            value.process_id(),
                            value.process_birth_token().to_owned(),
                            value.executable_sha256().to_owned(),
                        )
                    })
                {
                    return Err(stale_scope());
                }
                let assignment = native_mcp::current_assignment_context(db, &principal_for_db)?;
                if assignment.participant_id() != facts.participant_id
                    || expected_assignment
                        .as_ref()
                        .is_some_and(|expected| expected != &assignment)
                {
                    return Err(stale_scope());
                }
                Ok((assignment, snapshot.options().clone()))
            })
            .await?;
        if model::canonical(&serde_json::to_value(&options)?)?
            != model::canonical(&serde_json::to_value(&expected_options)?)?
        {
            return Err(stale_scope());
        }
        Ok((principal, assignment))
    }

    async fn connect_current(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        expected_service: Option<(u32, &str)>,
    ) -> Result<(Service, Principal)> {
        let (principal, current) = self
            .current_scope(facts, credential, Some(assignment))
            .await?;
        let service = tokio::time::timeout(NATIVE_ACTION_TIMEOUT, connect_native_service(facts))
            .await
            .map_err(|_| {
                Error::new(
                    "NATIVE_MCP_CONNECT_TIMEOUT",
                    "native service check timed out",
                )
            })??;
        if service.version != OPENCODE_VERSION
            || expected_service
                .is_some_and(|(pid, version)| service.pid != pid || service.version != version)
        {
            return Err(Error::new(
                "NATIVE_INSTANCE_CHANGED",
                "native service process/version differs from the retained effect target",
            ));
        }
        self.current_scope(facts, credential, Some(&current))
            .await?;
        Ok((service, principal))
    }

    async fn verify_current_service(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        expected_service: (u32, &str),
        service: &Service,
    ) -> Result<()> {
        service.verify().await?;
        if service.pid != expected_service.0 || service.version != expected_service.1 {
            return Err(Error::new(
                "NATIVE_INSTANCE_CHANGED",
                "native service identity changed around the admitted effect",
            ));
        }
        let current = tokio::time::timeout(NATIVE_ACTION_TIMEOUT, connect_native_service(facts))
            .await
            .map_err(|_| {
                Error::new(
                    "NATIVE_MCP_CONNECT_TIMEOUT",
                    "native service check timed out",
                )
            })??;
        if current.pid != expected_service.0 || current.version != expected_service.1 {
            return Err(Error::new(
                "NATIVE_INSTANCE_CHANGED",
                "configured native service process changed around the effect",
            ));
        }
        self.current_scope(facts, credential, Some(assignment))
            .await?;
        Ok(())
    }

    async fn admit_install(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        install_identity: &Value,
        service_pid: u32,
        service_version: &str,
    ) -> Result<Value> {
        let key = record_key(&facts.operation_id);
        let facts = facts.clone();
        let credential = credential.clone();
        let assignment = assignment.clone();
        let install_identity = install_identity.clone();
        let service_version = service_version.to_owned();
        let principal = self.authenticate(credential).await?;
        let record = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                validate_current_scope(&tx, &facts, &principal, &assignment)?;
                let value = match meta(&tx, &key)? {
                    Some(value) => {
                        validate_record(&value, &facts, &assignment)?;
                        if model::canonical(&value["install"]["intent"])?
                            != model::canonical(&install_identity)?
                        {
                            return Err(stale_scope());
                        }
                        if value["service"]["pid"].as_u64() != Some(u64::from(service_pid))
                            || value["service"]["version"].as_str()
                                != Some(service_version.as_str())
                        {
                            return Err(Error::new(
                                "NATIVE_INSTANCE_CHANGED",
                                "native process differs from the retained installation target",
                            ));
                        }
                        value
                    }
                    None => {
                        let now = model::now_ms()?;
                        let value = json!({
                            "schema_version":1,
                            "kind":"launcher_native_mcp_tools",
                            "operation_id":facts.operation_id,
                            "launch_identity_digest":facts.identity_digest,
                            "assignment":assignment.as_value(),
                            "service":{
                                "id":facts.options.service_id,
                                "pid":service_pid,
                                "version":service_version,
                                "directory_sha256":install_identity["location_sha256"],
                            },
                            "install":{
                                "state":"prepared",
                                "intent":install_identity,
                                "readback":null,
                                "recovered_readback":null,
                                "effect_reserved_at_ms":null,
                                "module_operation_id":null,
                                "module_operation_phase":null,
                            },
                            "challenge":{
                                "state":"not_started",
                                "metadata":null,
                                "replaced_metadata":[],
                                "replacement_archive":{"count":0,"digest":null},
                                "effect_reserved_at_ms":null,
                                "module_operation_id":null,
                                "module_operation_phase":null,
                            },
                            "tools_readback":null,
                            "last_error":null,
                            "created_at_ms":now,
                            "updated_at_ms":now,
                            "dispatch_permitted":false,
                        });
                        set_meta(&tx, &key, &value)?;
                        value
                    }
                };
                tx.commit()?;
                Ok(value)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(record)
    }

    async fn reserve_install_effect(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        command: Value,
    ) -> Result<bool> {
        let principal = self.authenticate(credential.clone()).await?;
        let facts = facts.clone();
        let assignment = assignment.clone();
        let key = record_key(&facts.operation_id);
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                validate_current_scope(&tx, &facts, &principal, &assignment)?;
                let mut record = meta(&tx, &key)?
                    .ok_or_else(|| Error::new("NOT_FOUND", "native MCP intent is missing"))?;
                validate_record(&record, &facts, &assignment)?;
                if record["install"]["state"] != "prepared" {
                    tx.commit()?;
                    return Ok(false);
                }
                let operation_id = queue_native_mcp_operation(
                    &tx,
                    &facts.operation_id,
                    &facts.identity_digest,
                    &assignment,
                    "native.mcp.install",
                    "install",
                    command,
                )?;
                let now = model::now_ms()?;
                record["install"]["state"] = json!("outcome_unknown");
                record["install"]["effect_reserved_at_ms"] = json!(now);
                record["install"]["module_operation_id"] = json!(operation_id);
                record["install"]["module_operation_phase"] = json!("install");
                record["last_error"] = Value::Null;
                record["updated_at_ms"] = json!(now);
                set_meta(&tx, &key, &record)?;
                tx.commit()?;
                Ok(true)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(result)
    }

    /// Consume the retained RuntimeOutcome for one C8 child Operation. A
    /// queued/sending child remains pending; a rejected or unknown child is
    /// recorded as bounded failure and is never replayed by this consumer.
    /// Applied receipts are validated against the original challenge/intent
    /// before the parent C8 record advances.
    #[expect(
        clippy::too_many_arguments,
        reason = "These separate immutable launch, binding, phase, process, and service inputs jointly bind one exact C8 child receipt."
    )]
    async fn consume_native_mcp_operation(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        operation_id: &str,
        phase: &str,
        install_identity: &Value,
        service_pid: u32,
        service_version: &str,
    ) -> Result<bool> {
        let operation_id_owned = operation_id.to_owned();
        let operation = self
            .run(move |db| super::operations::get_operation(db, &operation_id_owned))
            .await?;
        let (expected_method, expected_phase) = match phase {
            "install" => ("native.mcp.install", "install"),
            "observe_unknown" => ("native.mcp.observe", "observe_unknown"),
            "observe_refresh" => ("native.mcp.observe", "observe_refresh"),
            "arm" => ("native.mcp.arm", "arm"),
            "read" => ("native.mcp.read", "read"),
            _ => return Err(Error::invalid("native MCP module phase is invalid")),
        };
        let assignment_value = assignment.as_value();
        if operation["caller_id"] != INTERNAL_NATIVE_MCP_CALLER
            || operation["method"] != expected_method
            || operation["native_mcp_parent_launch_operation_id"] != facts.operation_id
            || operation["native_mcp_phase"] != expected_phase
            || operation["binding_id"] != assignment_value["binding_id"]
            || operation["binding_generation"] != assignment_value["binding_generation"]
            || operation["task_id"] != assignment_value["task_id"]
            || operation["attempt_id"] != assignment_value["attempt_id"]
        {
            return Err(stale_scope());
        }
        let state = operation["state"].as_str().unwrap_or_default();
        if matches!(state, "queued" | "sending" | "native_accepted") {
            return Ok(true);
        }
        let outcome = operation["result"]["outcome"].as_str();
        if state == "outcome_unknown" || outcome == Some("unknown") {
            let code = operation_error_code(&operation, "NATIVE_MCP_MODULE_OUTCOME_UNKNOWN");
            self.record_safe_error(facts, credential, assignment, phase, &code)
                .await?;
            self.clear_native_mcp_operation(facts, credential, assignment, phase)
                .await?;
            return Ok(true);
        }
        if state == "rejected" || outcome == Some("rejected") {
            let code = operation_error_code(&operation, "NATIVE_MCP_MODULE_REJECTED");
            self.record_safe_error(facts, credential, assignment, phase, &code)
                .await?;
            self.clear_native_mcp_operation(facts, credential, assignment, phase)
                .await?;
            return Ok(true);
        }
        if state != "settled" || outcome != Some("applied") {
            return Err(Error::new(
                "NATIVE_MCP_OPERATION_STATE",
                "native MCP child Operation has an unrecognized terminal state",
            ));
        }
        let receipt = operation["result"]["details"]["native_mcp"].clone();
        if !receipt.is_object() || receipt["native_replay"] != false {
            return Err(Error::new(
                "NATIVE_MCP_RECEIPT_INVALID",
                "native MCP child outcome lacks its non-replayed effect receipt",
            ));
        }
        match phase {
            "install" => {
                let readback = receipt["readback"].clone();
                self.record_install_ack(
                    facts,
                    credential,
                    assignment,
                    install_identity,
                    service_pid,
                    service_version,
                    readback,
                )
                .await?;
            }
            "observe_unknown" => {
                let readback = receipt["readback"].clone();
                self.record_install_observation(facts, credential, assignment, Some(readback))
                    .await?;
            }
            "observe_refresh" => {
                let readback = receipt["readback"].clone();
                self.record_install_refresh(
                    facts,
                    credential,
                    assignment,
                    install_identity,
                    service_pid,
                    service_version,
                    readback,
                )
                .await?;
            }
            "arm" => {
                let (current_service, _) = self
                    .connect_current(
                        facts,
                        credential,
                        assignment,
                        Some((service_pid, service_version)),
                    )
                    .await?;
                let record = self.load_record(facts, credential, assignment).await?;
                let challenge = crate::runtime::opencode_v2::mcp_tools::restore_challenge_metadata(
                    assignment.clone(),
                    &current_service,
                    &facts.options,
                    record["challenge"]["metadata"].clone(),
                )?;
                let response = receipt["response"].clone();
                crate::runtime::opencode_v2::mcp_tools::validate_external_arm_response(
                    response,
                    &challenge,
                    &current_service,
                    &facts.options,
                )?;
                self.record_challenge_armed(facts, credential, assignment)
                    .await?;
            }
            "read" => {
                let (current_service, _) = self
                    .connect_current(
                        facts,
                        credential,
                        assignment,
                        Some((service_pid, service_version)),
                    )
                    .await?;
                let record = self.load_record(facts, credential, assignment).await?;
                let challenge = crate::runtime::opencode_v2::mcp_tools::restore_challenge_metadata(
                    assignment.clone(),
                    &current_service,
                    &facts.options,
                    record["challenge"]["metadata"].clone(),
                )?;
                let readback =
                    crate::runtime::opencode_v2::mcp_tools::validate_external_read_response(
                        receipt["response"].clone(),
                        &challenge,
                        &current_service,
                        &facts.options,
                    )?;
                let payload = readback.as_value();
                if model::canonical(&payload)?.len() > MAX_PRIVATE_READBACK_BYTES
                    || readback.scope() != assignment
                {
                    return Err(Error::new(
                        "NATIVE_MCP_PROOF_SCHEMA",
                        "native tools readback exceeds its bound or differs from assignment",
                    ));
                }
                self.record_tools_readback(facts, credential, assignment, payload)
                    .await?;
            }
            _ => unreachable!("native MCP phase was validated above"),
        }
        Ok(true)
    }

    async fn queue_install_observation(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        command: Value,
        phase: &str,
    ) -> Result<()> {
        let principal = self.authenticate(credential.clone()).await?;
        let facts = facts.clone();
        let assignment = assignment.clone();
        let phase = phase.to_owned();
        let key = record_key(&facts.operation_id);
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            validate_current_scope(&tx, &facts, &principal, &assignment)?;
            let mut record = meta(&tx, &key)?
                .ok_or_else(|| Error::new("NOT_FOUND", "native MCP intent is missing"))?;
            validate_record(&record, &facts, &assignment)?;
            if record["install"]["module_operation_id"].is_null() {
                let operation_id = queue_native_mcp_operation(
                    &tx,
                    &facts.operation_id,
                    &facts.identity_digest,
                    &assignment,
                    "native.mcp.observe",
                    &phase,
                    command,
                )?;
                record["install"]["module_operation_id"] = json!(operation_id);
                record["install"]["module_operation_phase"] = json!(phase);
                record["updated_at_ms"] = json!(model::now_ms()?);
                set_meta(&tx, &key, &record)?;
            }
            tx.commit()?;
            Ok(())
        })
        .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn queue_challenge_read(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        command: Value,
    ) -> Result<()> {
        let principal = self.authenticate(credential.clone()).await?;
        let facts = facts.clone();
        let assignment = assignment.clone();
        let key = record_key(&facts.operation_id);
        self.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            validate_current_scope(&tx, &facts, &principal, &assignment)?;
            let mut record = meta(&tx, &key)?
                .ok_or_else(|| Error::new("NOT_FOUND", "native MCP intent is missing"))?;
            validate_record(&record, &facts, &assignment)?;
            if record["challenge"]["module_operation_id"].is_null() {
                let operation_id = queue_native_mcp_operation(
                    &tx,
                    &facts.operation_id,
                    &facts.identity_digest,
                    &assignment,
                    "native.mcp.read",
                    "read",
                    command,
                )?;
                record["challenge"]["module_operation_id"] = json!(operation_id);
                record["challenge"]["module_operation_phase"] = json!("read");
                record["updated_at_ms"] = json!(model::now_ms()?);
                set_meta(&tx, &key, &record)?;
            }
            tx.commit()?;
            Ok(())
        })
        .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn clear_native_mcp_operation(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        phase: &str,
    ) -> Result<()> {
        let phase = phase.to_owned();
        self.transition(facts, credential, assignment, move |record, _| {
            match phase.as_str() {
                "install" | "observe_unknown" | "observe_refresh" => {
                    record["install"]["module_operation_id"] = Value::Null;
                    record["install"]["module_operation_phase"] = Value::Null;
                }
                "arm" | "read" => {
                    record["challenge"]["module_operation_id"] = Value::Null;
                    record["challenge"]["module_operation_phase"] = Value::Null;
                }
                _ => return Err(Error::invalid("native MCP phase is invalid")),
            }
            Ok(())
        })
        .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Keep exact launch, assignment, service, intent, and readback proof explicit."
    )]
    async fn record_install_ack(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        install_identity: &Value,
        service_pid: u32,
        service_version: &str,
        readback: Value,
    ) -> Result<()> {
        let install_identity = install_identity.clone();
        let service_version = service_version.to_owned();
        self.transition(facts, credential, assignment, move |record, now| {
            validate_install_readback(
                &readback,
                &record["install"]["intent"],
                &install_identity,
                service_pid,
                &service_version,
            )?;
            if record["install"]["state"] != "outcome_unknown" {
                return Err(record_error(
                    "install acknowledgement has no reserved effect",
                ));
            }
            record["install"]["state"] = json!("registered");
            record["install"]["readback"] = readback;
            record["install"]["acknowledged_at_ms"] = json!(now);
            record["install"]["module_operation_id"] = Value::Null;
            record["install"]["module_operation_phase"] = Value::Null;
            record["last_error"] = Value::Null;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn record_install_observation(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        readback: Option<Value>,
    ) -> Result<()> {
        self.transition(facts, credential, assignment, move |record, now| {
            if record["install"]["state"] != "outcome_unknown" {
                return Err(record_error(
                    "install observation is outside the unknown state",
                ));
            }
            if let Some(readback) = readback {
                validate_install_readback(
                    &readback,
                    &record["install"]["intent"],
                    &record["install"]["intent"],
                    record["service"]["pid"].as_u64().unwrap_or(0) as u32,
                    record["service"]["version"].as_str().unwrap_or_default(),
                )?;
                record["install"]["recovered_readback"] = readback;
                record["install"]["state"] = json!("observed_after_unknown");
            }
            record["install"]["module_operation_id"] = Value::Null;
            record["install"]["module_operation_phase"] = Value::Null;
            record["install"]["last_observed_at_ms"] = json!(now);
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Keep exact launch, assignment, service, intent, and readback proof explicit."
    )]
    async fn record_install_refresh(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        install_identity: &Value,
        service_pid: u32,
        service_version: &str,
        readback: Value,
    ) -> Result<()> {
        let install_identity = install_identity.clone();
        let service_version = service_version.to_owned();
        self.transition(facts, credential, assignment, move |record, now| {
            validate_install_readback(
                &readback,
                &record["install"]["intent"],
                &install_identity,
                service_pid,
                &service_version,
            )?;
            if record["install"]["state"] != "registered" {
                return Err(record_error(
                    "install refresh is outside the acknowledged state",
                ));
            }
            record["install"]["readback"] = readback;
            record["install"]["module_operation_id"] = Value::Null;
            record["install"]["module_operation_phase"] = Value::Null;
            record["install"]["last_observed_at_ms"] = json!(now);
            record["last_error"] = Value::Null;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn record_challenge_intent(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        metadata: Value,
        service_pid: u32,
        service_version: &str,
    ) -> Result<()> {
        let metadata_assignment = metadata.get("assignment").cloned().unwrap_or(Value::Null);
        let metadata_id = metadata.get("challenge_id").cloned().unwrap_or(Value::Null);
        let metadata_pid = metadata.get("service_pid").cloned().unwrap_or(Value::Null);
        let metadata_version = metadata
            .get("service_version")
            .cloned()
            .unwrap_or(Value::Null);
        let service_version = service_version.to_owned();
        self.transition(facts, credential, assignment, move |record, now| {
            if record["install"]["state"] != "registered"
                || record["install"]["readback"]["runtime_status"] != "connected"
                || record["challenge"]["state"] != "not_started"
                || metadata_assignment != record["assignment"]
                || metadata_id.as_str().is_none_or(str::is_empty)
                || metadata_pid.as_u64() != Some(u64::from(service_pid))
                || metadata_version.as_str() != Some(service_version.as_str())
                || record["service"]["pid"].as_u64() != Some(u64::from(service_pid))
                || record["service"]["version"].as_str() != Some(service_version.as_str())
            {
                return Err(stale_scope());
            }
            record["challenge"]["state"] = json!("prepared");
            record["challenge"]["metadata"] = metadata;
            record["challenge"]["prepared_at_ms"] = json!(now);
            record["last_error"] = Value::Null;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn replace_prepared_challenge(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        service: &Service,
        old_metadata: Value,
        reason: ChallengeReplacementReason,
    ) -> Result<Value> {
        let (_, current_assignment) = self
            .current_scope(facts, credential, Some(assignment))
            .await?;
        self.verify_current_service(
            facts,
            credential,
            assignment,
            (service.pid, service.version.as_str()),
            service,
        )
        .await?;
        let replacement = crate::runtime::opencode_v2::mcp_tools::new_challenge(
            current_assignment,
            service,
            &facts.options,
        )?;
        let replacement_metadata =
            crate::runtime::opencode_v2::mcp_tools::challenge_metadata(&replacement);
        self.record_challenge_replacement(
            facts,
            credential,
            assignment,
            old_metadata,
            replacement_metadata,
            service.pid,
            &service.version,
            reason,
        )
        .await?;
        self.summary(facts, credential, assignment).await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "Keep old and replacement challenge scope and service proof explicit."
    )]
    async fn record_challenge_replacement(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        old_metadata: Value,
        replacement_metadata: Value,
        service_pid: u32,
        service_version: &str,
        reason: ChallengeReplacementReason,
    ) -> Result<()> {
        let service_version = service_version.to_owned();
        self.transition(facts, credential, assignment, move |record, now| {
            let stored_old = &record["challenge"]["metadata"];
            let mut history = match record["challenge"]["replaced_metadata"].as_array() {
                Some(history) => history.clone(),
                None if record["challenge"]["replaced_metadata"].is_null() => Vec::new(),
                None => return Err(record_error("challenge provenance history is invalid")),
            };
            let old_expires_at_ms = old_metadata["expires_at_ms"].as_i64();
            let old_issued_at_ms = old_metadata["issued_at_ms"].as_i64();
            let old_challenge_id = old_metadata["challenge_id"].as_str();
            let old_nonce = old_metadata["nonce"].as_str();
            let old_module_path = old_metadata["module_path"].as_str();
            let old_module_hash = old_metadata["module_sha256"].as_str();
            let new_expires_at_ms = replacement_metadata["expires_at_ms"].as_i64();
            let new_issued_at_ms = replacement_metadata["issued_at_ms"].as_i64();
            let new_challenge_id = replacement_metadata["challenge_id"].as_str();
            let new_nonce = replacement_metadata["nonce"].as_str();
            let new_module_path = replacement_metadata["module_path"].as_str();
            let new_module_hash = replacement_metadata["module_sha256"].as_str();
            let old_time_valid = match (old_issued_at_ms, old_expires_at_ms) {
                (Some(issued), Some(expires)) => {
                    issued > 0
                        && issued <= now
                        && expires > issued
                        && expires.saturating_sub(issued) <= MAX_CHALLENGE_TTL_MS
                }
                _ => false,
            };
            let new_time_valid = match (new_issued_at_ms, new_expires_at_ms) {
                (Some(issued), Some(expires)) => {
                    issued > 0
                        && issued <= now
                        && expires > now
                        && expires.saturating_sub(issued) <= MAX_CHALLENGE_TTL_MS
                }
                _ => false,
            };
            let module_rotated =
                old_module_path != new_module_path || old_module_hash != new_module_hash;
            let replacement_reason_valid = match reason {
                // Expiry is itself a sufficient typed pre-effect reason. A
                // concurrent module rotation does not revoke that basis; the
                // replacement challenge below binds the current module.
                ChallengeReplacementReason::Expired => {
                    old_expires_at_ms.is_some_and(|expires| expires <= now)
                }
                ChallengeReplacementReason::ModuleRotated => module_rotated,
            };
            let expected_location = record["install"]["intent"]["location_sha256"].as_str();
            let old_location = old_metadata["directory"]
                .as_str()
                .map(|directory| format!("sha256:{}", model::digest(directory.as_bytes())));
            let new_location = replacement_metadata["directory"]
                .as_str()
                .map(|directory| format!("sha256:{}", model::digest(directory.as_bytes())));
            let exact_old_scope = old_metadata["schema"] == "opencode-v2-native-mcp-challenge-v1"
                && model::canonical(&old_metadata["assignment"])?
                    == model::canonical(&record["assignment"])?
                && old_metadata["service_id"] == record["service"]["id"]
                && old_metadata["service_pid"].as_u64() == Some(u64::from(service_pid))
                && old_metadata["service_version"].as_str() == Some(service_version.as_str())
                && old_metadata["model"] == replacement_metadata["model"]
                && old_location.as_deref() == expected_location;
            let exact_new_scope = replacement_metadata["schema"]
                == "opencode-v2-native-mcp-challenge-v1"
                && model::canonical(&replacement_metadata["assignment"])?
                    == model::canonical(&record["assignment"])?
                && replacement_metadata["service_id"] == record["service"]["id"]
                && replacement_metadata["service_pid"].as_u64() == Some(u64::from(service_pid))
                && replacement_metadata["service_version"].as_str()
                    == Some(service_version.as_str())
                && replacement_metadata["model"] == old_metadata["model"]
                && new_location.as_deref() == expected_location;
            if record["challenge"]["state"] != "prepared"
                || !record["challenge"]["effect_reserved_at_ms"].is_null()
                || model::canonical(stored_old)? != model::canonical(&old_metadata)?
                || !exact_old_scope
                || !exact_new_scope
                || !old_time_valid
                || !new_time_valid
                || !replacement_reason_valid
                || old_challenge_id.is_none_or(str::is_empty)
                || old_nonce.is_none_or(str::is_empty)
                || new_challenge_id.is_none_or(str::is_empty)
                || new_nonce.is_none_or(str::is_empty)
                || old_module_path.is_none_or(str::is_empty)
                || new_module_path.is_none_or(str::is_empty)
                || old_module_hash.is_none_or(|hash| !valid_sha256_hex(hash))
                || new_module_hash.is_none_or(|hash| !valid_sha256_hex(hash))
                || old_challenge_id == new_challenge_id
                || old_nonce == new_nonce
                || record["service"]["pid"].as_u64() != Some(u64::from(service_pid))
                || record["service"]["version"].as_str() != Some(service_version.as_str())
            {
                return Err(stale_scope());
            }
            let mut archive = record["challenge"]["replacement_archive"].clone();
            if archive.is_null() {
                archive = json!({"count":0,"digest":null});
            }
            let archived_count = archive["count"]
                .as_i64()
                .filter(|count| *count >= 0)
                .ok_or_else(|| record_error("challenge replacement archive is invalid"))?;
            let prior_archive_digest = archive["digest"].as_str();
            if prior_archive_digest.is_some_and(|digest| !valid_prefixed_sha256(digest))
                || archived_count == 0 && prior_archive_digest.is_some()
                || archived_count > 0 && prior_archive_digest.is_none()
            {
                return Err(record_error(
                    "challenge replacement archive digest is invalid",
                ));
            }
            if history.len() > MAX_REPLACED_CHALLENGES {
                return Err(record_error("challenge replacement ring exceeds its bound"));
            }
            if history.len() == MAX_REPLACED_CHALLENGES {
                let evicted = history.remove(0);
                let evicted_digest = model::digest(model::canonical(&evicted)?.as_bytes());
                let next_count = archived_count.saturating_add(1);
                let next_archive = json!({
                    "schema_version":1,
                    "count":next_count,
                    "prior_digest":prior_archive_digest,
                    "evicted_metadata_digest":evicted_digest,
                });
                let next_digest = format!(
                    "sha256:{}",
                    model::digest(model::canonical(&next_archive)?.as_bytes())
                );
                archive = json!({"count":next_count,"digest":next_digest});
            }
            history.push(old_metadata);
            record["challenge"]["replaced_metadata"] = json!(history);
            record["challenge"]["replacement_archive"] = archive;
            record["challenge"]["metadata"] = replacement_metadata;
            record["challenge"]["replaced_at_ms"] = json!(now);
            record["last_error"] = Value::Null;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn reserve_challenge_effect(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        command: Value,
    ) -> Result<bool> {
        let principal = self.authenticate(credential.clone()).await?;
        let facts = facts.clone();
        let assignment = assignment.clone();
        let key = record_key(&facts.operation_id);
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                validate_current_scope(&tx, &facts, &principal, &assignment)?;
                let mut record = meta(&tx, &key)?
                    .ok_or_else(|| Error::new("NOT_FOUND", "native MCP intent is missing"))?;
                validate_record(&record, &facts, &assignment)?;
                if record["challenge"]["state"] != "prepared" {
                    tx.commit()?;
                    return Ok(false);
                }
                if !record["challenge"]["effect_reserved_at_ms"].is_null() {
                    return Err(record_error(
                        "prepared challenge already has an effect reservation",
                    ));
                }
                let operation_id = queue_native_mcp_operation(
                    &tx,
                    &facts.operation_id,
                    &facts.identity_digest,
                    &assignment,
                    "native.mcp.arm",
                    "arm",
                    command,
                )?;
                let now = model::now_ms()?;
                record["challenge"]["state"] = json!("outcome_unknown");
                record["challenge"]["effect_reserved_at_ms"] = json!(now);
                record["challenge"]["module_operation_id"] = json!(operation_id);
                record["challenge"]["module_operation_phase"] = json!("arm");
                record["last_error"] = Value::Null;
                record["updated_at_ms"] = json!(now);
                set_meta(&tx, &key, &record)?;
                tx.commit()?;
                Ok(true)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(result)
    }

    async fn record_challenge_armed(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
    ) -> Result<()> {
        self.transition(facts, credential, assignment, |record, now| {
            if record["challenge"]["state"] != "outcome_unknown" {
                return Err(record_error(
                    "challenge acknowledgement has no reserved effect",
                ));
            }
            record["challenge"]["state"] = json!("armed");
            record["challenge"]["armed_at_ms"] = json!(now);
            record["challenge"]["module_operation_id"] = Value::Null;
            record["challenge"]["module_operation_phase"] = Value::Null;
            record["last_error"] = Value::Null;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn record_tools_readback(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        payload: Value,
    ) -> Result<()> {
        self.transition(facts, credential, assignment, move |record, now| {
            if !matches!(
                record["challenge"]["state"].as_str(),
                Some("armed" | "outcome_unknown")
            ) || model::canonical(&payload["assignment"])?
                != model::canonical(&record["assignment"])?
                || payload["dispatch_permitted"] != false
                || payload["model_consumed"] != "unknown"
            {
                return Err(stale_scope());
            }
            record["challenge"]["state"] = json!("observed");
            record["challenge"]["observed_at_ms"] = json!(now);
            record["challenge"]["module_operation_id"] = Value::Null;
            record["challenge"]["module_operation_phase"] = Value::Null;
            record["tools_readback"] = payload;
            record["last_error"] = Value::Null;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn record_safe_error(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        stage: &str,
        code: &str,
    ) -> Result<()> {
        self.record_safe_error_with_class(facts, credential, assignment, stage, code, None)
            .await
    }

    #[expect(
        dead_code,
        reason = "Preserve the typed RPC rejection-class recorder; the current C8 receipt consumer has only persisted error data and does not reconstruct a native RPC Error."
    )]
    async fn record_safe_rpc_error(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        stage: &str,
        error: &Error,
    ) -> Result<()> {
        self.record_safe_error_with_class(
            facts,
            credential,
            assignment,
            stage,
            &error.code,
            error.rejection_class,
        )
        .await
    }

    async fn record_safe_error_with_class(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        stage: &str,
        code: &str,
        rejection_class: Option<NativeRpcRejectionClass>,
    ) -> Result<()> {
        let stage = safe_label(stage)?;
        let code = safe_label(code)?;
        self.transition(facts, credential, assignment, move |record, now| {
            let mut failure = json!({"stage":stage,"code":code,"recorded_at_ms":now});
            if let Some(rejection_class) = rejection_class {
                failure["rejection_class"] = json!(rejection_class.as_str());
            }
            record["last_error"] = failure;
            Ok(())
        })
        .await?;
        Ok(())
    }

    async fn transition<T: Send + 'static>(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
        update: impl FnOnce(&mut Value, i64) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let principal = self.authenticate(credential.clone()).await?;
        let facts = facts.clone();
        let assignment = assignment.clone();
        let key = record_key(&facts.operation_id);
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                validate_current_scope(&tx, &facts, &principal, &assignment)?;
                let mut record = meta(&tx, &key)?
                    .ok_or_else(|| Error::new("NOT_FOUND", "native MCP intent is missing"))?;
                validate_record(&record, &facts, &assignment)?;
                let result = update(&mut record, model::now_ms()?)?;
                record["updated_at_ms"] = json!(model::now_ms()?);
                set_meta(&tx, &key, &record)?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(result)
    }

    async fn load_record(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
    ) -> Result<Value> {
        let principal = self.authenticate(credential.clone()).await?;
        let facts = facts.clone();
        let assignment = assignment.clone();
        let key = record_key(&facts.operation_id);
        self.run(move |db| {
            validate_current_scope(db, &facts, &principal, &assignment)?;
            let record = meta(db, &key)?
                .ok_or_else(|| Error::new("NOT_FOUND", "native MCP intent is missing"))?;
            validate_record(&record, &facts, &assignment)?;
            Ok(record)
        })
        .await
    }

    async fn summary(
        &self,
        facts: &LaunchFacts,
        credential: &Credential,
        assignment: &crate::native_mcp::AssignmentContext,
    ) -> Result<Value> {
        let record = self.load_record(facts, credential, assignment).await?;
        Ok(public_summary(&record))
    }
}

async fn connect_native_service(facts: &LaunchFacts) -> Result<Service> {
    match facts.owned_service.as_ref() {
        Some(expected) => {
            Service::connect_owned(
                &facts.options,
                expected.process_id(),
                expected.process_birth_token(),
                expected.executable_sha256(),
            )
            .await
        }
        None => Service::connect(&facts.options).await,
    }
}

fn load_launch_facts(db: &Connection, config: &Config, operation_id: &str) -> Result<LaunchFacts> {
    if operation_id.is_empty() || operation_id.len() > 256 {
        return Err(Error::invalid("launch Operation ID is invalid"));
    }
    let snapshot = launcher_native_mcp::current_mcp_launch_snapshot(db, config, operation_id)?;
    launcher_native_mcp::revalidate_mcp_launch_snapshot(db, config, &snapshot)?;
    Ok(LaunchFacts {
        operation_id: snapshot.launch_operation_id().to_owned(),
        identity_digest: snapshot.identity_digest()?,
        assignment: snapshot.assignment_context()?,
        participant_id: snapshot.participant_id().to_owned(),
        credential_ref: snapshot.credential_ref().to_owned(),
        profile_config_ref: snapshot.profile_config_ref().to_owned(),
        options: snapshot.options().clone(),
        owned_service: snapshot.owned_service_expectation(),
        config: Arc::new(config.clone()),
    })
}

const INTERNAL_NATIVE_MCP_CALLER: &str = "swarm.internal.c8.native_mcp";

fn require_internal_native_mcp_client(tx: &Connection) -> Result<()> {
    let registration = super::meta(tx, &format!("client:{INTERNAL_NATIVE_MCP_CALLER}"))?
        .ok_or_else(|| {
            Error::new(
                "INTERNAL_CLIENT_NOT_REGISTERED",
                "native MCP phase caller is not durably registered",
            )
        })?;
    if registration["role"] != "module"
        || registration["internal_only"] != true
        || registration["disabled"] == true
    {
        return Err(Error::new(
            "INTERNAL_CLIENT_INVALID",
            "native MCP phase caller has no internal module scope",
        ));
    }
    Ok(())
}

fn operation_error_code(operation: &Value, fallback: &str) -> String {
    operation["result"]["details"]["diagnostic_code"]
        .as_str()
        .or_else(|| operation["result"]["failure"]["code"].as_str())
        .filter(|code| !code.is_empty() && code.len() <= 128)
        .unwrap_or(fallback)
        .to_owned()
}

/// Map one exact method/phase pair to its original DTO phase and observation
/// purpose. C7 assigned-session observation and C8 installed-server
/// observations deliberately share a method while retaining distinct phases.
pub(super) fn native_mcp_phase_contract(
    method: &str,
    phase: &str,
) -> Option<(&'static str, Option<&'static str>)> {
    match (method, phase) {
        ("native.mcp.install", "install") => Some(("install", None)),
        ("native.mcp.observe", "observe_unknown" | "observe_refresh") => {
            Some(("observe", Some("installed_server")))
        }
        ("native.mcp.observe", "observe_assignment") => Some(("observe", Some("assigned_session"))),
        ("native.mcp.arm", "arm") => Some(("arm", None)),
        ("native.mcp.read", "read") => Some(("read", None)),
        _ => None,
    }
}

fn bind_native_mcp_observation_kind(command: &mut Value, expected: Option<&str>) -> Result<()> {
    let object = command
        .as_object_mut()
        .ok_or_else(|| Error::invalid("native MCP command is not an object"))?;
    match (expected, object.get("observation_kind").cloned()) {
        (Some(expected), None | Some(Value::Null)) => {
            object.insert("observation_kind".to_owned(), json!(expected));
        }
        (Some(expected), Some(Value::String(actual))) if actual.as_str() == expected => {}
        (None, None | Some(Value::Null)) => {
            object.remove("observation_kind");
        }
        _ => {
            return Err(Error::invalid(
                "native MCP observation kind differs from its exact phase",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod native_mcp_queue_contract_tests {
    use super::*;

    #[test]
    fn queue_enriches_only_the_exact_observe_purpose() {
        let mut c7_command = json!({});
        bind_native_mcp_observation_kind(&mut c7_command, Some("assigned_session")).unwrap();
        assert_eq!(c7_command["observation_kind"], "assigned_session");

        let mut c8_command = json!({});
        bind_native_mcp_observation_kind(&mut c8_command, Some("installed_server")).unwrap();
        assert_eq!(c8_command["observation_kind"], "installed_server");

        let mut wrong_purpose = json!({"observation_kind":"installed_server"});
        assert!(
            bind_native_mcp_observation_kind(&mut wrong_purpose, Some("assigned_session")).is_err()
        );
        let mut non_observe_purpose = json!({"observation_kind":"assigned_session"});
        assert!(bind_native_mcp_observation_kind(&mut non_observe_purpose, None).is_err());
    }

    #[test]
    fn queue_omits_none_observation_kind() {
        let mut command = json!({"observation_kind":null});
        bind_native_mcp_observation_kind(&mut command, None).unwrap();
        assert!(command.get("observation_kind").is_none());
    }
}

/// Insert one descriptor-bound native command in the existing Operations
/// queue. C7 and C8 callers supply their retained launch identity and share
/// this transaction with their phase reservation. The caller is an internal
/// Store linkage, never a public credential; the immutable row stores the
/// descriptor DTO template while its prepared command/challenge remains in
/// the private effective handoff.
pub(super) fn queue_native_mcp_operation(
    tx: &Transaction<'_>,
    parent_operation_id: &str,
    launch_identity_digest: &str,
    assignment: &crate::native_mcp::AssignmentContext,
    method: &str,
    phase: &str,
    mut command: Value,
) -> Result<String> {
    let (action, observation_kind) = native_mcp_phase_contract(method, phase).ok_or_else(|| {
        Error::invalid("native MCP module operation method and phase are invalid")
    })?;
    bind_native_mcp_observation_kind(&mut command, observation_kind)?;
    require_internal_native_mcp_client(tx)?;
    let scope = assignment.as_value();
    let task_id = model::text(&scope, "task_id")?;
    let attempt_id = model::text(&scope, "attempt_id")?;
    let binding_id = model::text(&scope, "binding_id")?;
    let binding_generation = model::positive(&scope, "binding_generation")?;
    let command_bytes = model::canonical(&command)?;
    if command_bytes.len() > MAX_PRIVATE_READBACK_BYTES {
        return Err(Error::new(
            "NATIVE_MCP_COMMAND_LIMIT",
            "native MCP module command exceeds its private handoff bound",
        ));
    }
    let command_scope = command
        .get("scope")
        .filter(|scope| scope.is_object())
        .ok_or_else(|| Error::invalid("native MCP command scope is missing"))?;
    if command_scope["binding_id"] != binding_id
        || command_scope["binding_generation"] != binding_generation
        || command_scope["assignment"] != scope
    {
        return Err(scope_error(
            "native MCP command is bound to a different assignment",
        ));
    }
    let native_session_id = model::text(&scope, "native_session_id")?;
    let service_id = model::text(command_scope, "service_id")?;
    let service_version = model::text(command_scope, "expected_version")?;
    let service_pid = command_scope["service_pid"]
        .as_u64()
        .filter(|pid| *pid > 0 && *pid <= u32::MAX as u64)
        .ok_or_else(|| Error::invalid("native MCP service process identity is invalid"))?
        as u32;
    let directory = model::text(command_scope, "directory")?;
    let assignment_sha256 = model::digest(model::canonical(&scope)?.as_bytes());
    let location_sha256 = model::digest(directory.as_bytes());
    let artifact_key = if matches!(action, "install" | "observe") {
        "prepared"
    } else {
        "challenge"
    };
    let artifact = command
        .get(artifact_key)
        .filter(|artifact| artifact.is_object())
        .ok_or_else(|| Error::invalid("native MCP private artifact is missing"))?;
    let artifact_sha256 = model::digest(model::canonical(artifact)?.as_bytes());
    let operation_id = model::new_id();
    let request_id = format!("native-mcp:{}", operation_id);
    let now = model::now_ms()?;
    let artifact_ref = format!(
        "store://native-mcp/{}/{}/{}",
        operation_id, action, artifact_key
    );
    let mut original = json!({
        "schema_version":2,
        "operation_id":operation_id.clone(),
        "binding_id":binding_id,
        "binding_generation":binding_generation,
        // The runtime fills this reserved field before module admission. The
        // null-bearing template itself is the immutable Operation input whose
        // digest is retained by the normal module-receipt validator.
        "input_sha256":Value::Null,
        "assignment_sha256":assignment_sha256.clone(),
        "native_session_id":native_session_id,
        "service_id":service_id,
        "service_version":service_version,
        "service_pid":service_pid,
        "location_sha256":location_sha256.clone(),
        "phase":action,
    });
    if let Some(observation_kind) = observation_kind {
        original["observation_kind"] = json!(observation_kind);
    }
    original
        .as_object_mut()
        .ok_or_else(|| Error::invalid("native MCP DTO is not an object"))?
        .insert(
            artifact_key.to_owned(),
            json!({
                "protected_ref":artifact_ref.clone(),
                "sha256":artifact_sha256,
            }),
        );
    let input_sha256 = model::digest(model::canonical(&original)?.as_bytes());
    let effective = json!({
        "native_mcp":{
            "schema_version":1,
            "child_operation_id":operation_id,
            "parent_launch_operation_id":parent_operation_id,
            "launch_identity_digest":launch_identity_digest,
            "phase":phase,
            "method":method,
            "task_id":task_id,
            "task_revision":scope["task_revision"],
            "attempt_id":attempt_id,
            "binding_id":binding_id,
            "binding_generation":binding_generation,
            "assignment_digest":assignment_sha256,
            "input_sha256":input_sha256,
            "native_session_id":native_session_id,
            "service_id":service_id,
            "service_version":service_version,
            "service_pid":service_pid,
            "location_sha256":location_sha256,
            "command_sha256":model::digest(command_bytes.as_bytes()),
            "artifact_ref":artifact_ref,
            "effect":command,
        }
    });
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'queued','{}',?11,?11,?11)",
        params![
            operation_id,
            INTERNAL_NATIVE_MCP_CALLER,
            request_id,
            method,
            model::canonical(&original)?,
            model::canonical(&effective)?,
            task_id,
            attempt_id,
            binding_id,
            binding_generation,
            now,
        ],
    )?;
    Ok(operation_id)
}

fn validate_current_scope(
    db: &Connection,
    facts: &LaunchFacts,
    principal: &Principal,
    expected_assignment: &crate::native_mcp::AssignmentContext,
) -> Result<()> {
    if principal.role != Role::Participant || principal.client_id != facts.participant_id {
        return Err(scope_error("authenticated Participant changed"));
    }
    let snapshot =
        launcher_native_mcp::current_mcp_launch_snapshot(db, &facts.config, &facts.operation_id)?;
    launcher_native_mcp::revalidate_mcp_launch_snapshot(db, &facts.config, &snapshot)?;
    if snapshot.launch_operation_id() != facts.operation_id
        || snapshot.identity_digest()? != facts.identity_digest
        || snapshot.participant_id() != facts.participant_id
        || snapshot.credential_ref() != facts.credential_ref
        || snapshot.profile_config_ref() != facts.profile_config_ref
        || model::canonical(&serde_json::to_value(snapshot.options())?)?
            != model::canonical(&serde_json::to_value(&facts.options)?)?
        || snapshot.owned_service_expectation().as_ref().map(|value| {
            (
                value.process_id(),
                value.process_birth_token().to_owned(),
                value.executable_sha256().to_owned(),
            )
        }) != facts.owned_service.as_ref().map(|value| {
            (
                value.process_id(),
                value.process_birth_token().to_owned(),
                value.executable_sha256().to_owned(),
            )
        })
    {
        return Err(stale_scope());
    }
    let current = native_mcp::current_assignment_context(db, principal)?;
    if &current != expected_assignment || current.participant_id() != facts.participant_id {
        return Err(stale_scope());
    }
    Ok(())
}

/// Return the exact acknowledged C8 native tool proof for a current launch
/// dispatch stage. This path is read-only: it never contacts OpenCode,
/// upgrades a receipt, or claims that a model consumed the schemas.
pub(crate) fn require_current_connection(
    db: &Connection,
    config: &Config,
    launch_operation_id: &str,
    dispatch_operation_id: Option<&str>,
) -> Result<Value> {
    let snapshot = launcher_native_mcp::current_mcp_launch_snapshot_for_dispatch(
        db,
        config,
        launch_operation_id,
        dispatch_operation_id,
    )?;
    let identity_digest = snapshot.identity_digest()?;
    let assignment = snapshot.assignment_context()?;
    let facts = LaunchFacts {
        operation_id: snapshot.launch_operation_id().to_owned(),
        identity_digest: identity_digest.clone(),
        assignment: assignment.clone(),
        participant_id: snapshot.participant_id().to_owned(),
        credential_ref: snapshot.credential_ref().to_owned(),
        profile_config_ref: snapshot.profile_config_ref().to_owned(),
        options: snapshot.options().clone(),
        owned_service: snapshot.owned_service_expectation(),
        config: Arc::new(config.clone()),
    };
    let owned_service = facts.owned_service.as_ref().ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "dispatch requires the retained owned-service process identity",
        )
    })?;
    let record = meta(db, &record_key(launch_operation_id))?.ok_or_else(|| {
        Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "exact acknowledged native MCP proof is not retained",
        )
    })?;
    validate_record(&record, &facts, &assignment)?;
    if record["install"]["state"] != "registered"
        || record["install"]["readback"]["runtime_status"] != "connected"
        || record["challenge"]["state"] != "observed"
        || !record["tools_readback"].is_object()
    {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "native MCP install acknowledgement or scoped tool proof is incomplete",
        ));
    }

    let install_intent = &record["install"]["intent"];
    if install_intent["schema_version"] != 1
        || install_intent["kind"] != "opencode_v2_mcp_install_intent"
        || install_intent["registration"] != "location_scoped_in_memory"
        || install_intent["expected_version"] != OPENCODE_VERSION
        || install_intent["assignment"] != record["assignment"]
        || install_intent["native_tool_set"] != "unknown"
        || install_intent["provider_request_context"] != "unknown"
        || install_intent["model_consumption"] != "unknown"
        || install_intent["dispatch_permitted"] != false
    {
        return Err(record_error("native MCP install intent is invalid"));
    }
    validate_install_readback(
        &record["install"]["readback"],
        install_intent,
        install_intent,
        record["service"]["pid"]
            .as_u64()
            .and_then(|pid| u32::try_from(pid).ok())
            .filter(|pid| *pid > 0)
            .ok_or_else(|| record_error("native MCP service PID is invalid"))?,
        record["service"]["version"]
            .as_str()
            .filter(|version| !version.is_empty())
            .ok_or_else(|| record_error("native MCP service version is missing"))?,
    )?;
    let server_name = install_intent["server_name"]
        .as_str()
        .filter(|name| {
            !name.is_empty()
                && name.len() <= 96
                && name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
        })
        .ok_or_else(|| record_error("native MCP server name is missing"))?;
    let command_sha256 = install_intent["command_sha256"]
        .as_str()
        .filter(|digest| valid_prefixed_sha256(digest))
        .ok_or_else(|| record_error("native MCP command digest is invalid"))?;
    let location_sha256 = install_intent["location_sha256"]
        .as_str()
        .filter(|digest| valid_prefixed_sha256(digest))
        .ok_or_else(|| record_error("native MCP location digest is invalid"))?;
    let tools = &record["tools_readback"];
    if model::canonical(tools)?.len() > MAX_PRIVATE_READBACK_BYTES
        || record["install"]["readback"]["runtime_config_readback"] != "not_exposed_by_pinned_api"
        || tools["contract"] != "opencode-v2-native-mcp-proof-v1"
        || tools["dispatch_permitted"] != false
        || tools["model_consumed"] != "unknown"
        || model::canonical(&tools["assignment"])? != model::canonical(&assignment.as_value())?
        || tools["service"]["id"] != record["service"]["id"]
        || tools["service"]["pid"] != record["service"]["pid"]
        || tools["service"]["version"] != record["service"]["version"]
        || tools["session"]["id"] != assignment.native_session_id()
        || model::canonical(&tools["session"]["model"])?
            != model::canonical(&serde_json::to_value(&facts.options.model)?)?
        || tools["challenge"]["id"] != record["challenge"]["metadata"]["challenge_id"]
        || tools["challenge"]["issued_at_ms"] != record["challenge"]["metadata"]["issued_at_ms"]
        || tools["challenge"]["expires_at_ms"] != record["challenge"]["metadata"]["expires_at_ms"]
        || model::canonical(&record["challenge"]["metadata"]["assignment"])?
            != model::canonical(&assignment.as_value())?
        || model::canonical(&record["challenge"]["metadata"]["model"])?
            != model::canonical(&serde_json::to_value(&facts.options.model)?)?
    {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "native MCP tool evidence does not match the acknowledged launch scope",
        ));
    }

    let directory = tools["service"]["directory"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| record_error("native MCP evidence location is missing"))?;
    let directory_digest = format!("sha256:{}", model::digest(directory.as_bytes()));
    let observer = &tools["observer"];
    let plugin_id = observer["plugin_id"]
        .as_str()
        .filter(|plugin| *plugin == "eliot.native-mcp-proof.v1")
        .ok_or_else(|| record_error("native MCP observer identity is invalid"))?;
    let module_sha256 = observer["module_sha256"]
        .as_str()
        .filter(|digest| valid_prefixed_sha256(digest))
        .ok_or_else(|| record_error("native MCP observer module digest is invalid"))?;
    let metadata = &record["challenge"]["metadata"];
    let metadata_directory = metadata["directory"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| record_error("native MCP challenge location is missing"))?;
    let metadata_directory_digest =
        format!("sha256:{}", model::digest(metadata_directory.as_bytes()));
    if metadata["schema"] != "opencode-v2-native-mcp-challenge-v1"
        || metadata["service_id"] != record["service"]["id"]
        || metadata["service_pid"] != record["service"]["pid"]
        || metadata["service_version"] != record["service"]["version"]
        || metadata["module_sha256"].as_str() != module_sha256.strip_prefix("sha256:")
        || observer["module_path"] != metadata["module_path"]
        || directory != metadata_directory
        || !valid_prefixed_sha256(&directory_digest)
        || directory_digest.as_str() != location_sha256
        || metadata_directory_digest.as_str() != location_sha256
    {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "native MCP observer evidence does not match the acknowledged install location",
        ));
    }

    let native_discovered = &tools["native_discovered"];
    let observed_at_ms = native_discovered["observed_at_ms"]
        .as_i64()
        .filter(|time| *time > 0)
        .ok_or_else(|| record_error("native MCP inventory observation time is invalid"))?;
    let inventory = native_discovered["tools"]
        .as_array()
        .filter(|tools| !tools.is_empty() && tools.len() <= 512)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
                "native MCP tool inventory is absent or exceeds its bound",
            )
        })?;
    if native_discovered["status"] != "observed"
        || observed_at_ms < metadata["issued_at_ms"].as_i64().unwrap_or(i64::MAX)
        || observed_at_ms > metadata["expires_at_ms"].as_i64().unwrap_or(0)
        || observed_at_ms > model::now_ms()?
    {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "native MCP tool inventory was not observed",
        ));
    }
    let native_tools_digest = digest_json(&native_discovered["tools"])?;
    if native_discovered["digest"].as_str() != Some(native_tools_digest.as_str()) {
        return Err(record_error(
            "native MCP tool inventory digest does not match its observed tools",
        ));
    }
    let mut scoped_tools = Vec::new();
    for tool in inventory {
        if tool["server"] != server_name {
            continue;
        }
        let name = tool["name"]
            .as_str()
            .filter(|name| !name.is_empty() && name.len() <= 256)
            .ok_or_else(|| record_error("native MCP tool name is invalid"))?;
        if !tool["input_schema"].is_object() {
            return Err(record_error("native MCP tool schema is invalid"));
        }
        scoped_tools.push(json!({
            "server":server_name,
            "name":name,
            "input_schema":tool["input_schema"],
        }));
    }
    if scoped_tools.is_empty() {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "the acknowledged server has no observed tools",
        ));
    }

    // Keep the three observation stages separate. The session-context hook
    // can show which schemas OpenCode supplied to a context build; a
    // before-transport provider hook can show schemas in a request. Neither
    // says that a model consumed a schema, and neither changes dispatch
    // authority. Project only bounded schema metadata from the authenticated
    // C8 readback; never retain request bodies or provider headers here.
    let session_context = project_session_context(&tools["session_context"], metadata)?;
    let provider_request = project_provider_request(
        &tools["provider_request"],
        metadata,
        &serde_json::to_value(&facts.options.model)?,
    )?;
    let session_context_digest = digest_json(&session_context)?;
    let provider_request_digest = digest_json(&provider_request)?;

    let assignment_value = assignment.as_value();
    let assignment_digest = digest_json(&assignment_value)?;
    let mut evidence_without_digest = (*tools).clone();
    let evidence_digest = evidence_without_digest
        .as_object_mut()
        .and_then(|evidence| evidence.remove("evidence_digest"))
        .and_then(|digest| digest.as_str().map(str::to_owned))
        .filter(|digest| valid_prefixed_sha256(digest))
        .ok_or_else(|| record_error("native MCP evidence digest is missing or invalid"))?;
    if digest_json(&evidence_without_digest)? != evidence_digest {
        return Err(record_error(
            "native MCP evidence digest does not match its observer readback",
        ));
    }
    let native_discovered_digest = digest_json(native_discovered)?;
    let service_id = record["service"]["id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| record_error("native MCP service identity is missing"))?;
    let service_version = record["service"]["version"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| record_error("native MCP service version is missing"))?;
    let service_pid = record["service"]["pid"]
        .as_u64()
        .filter(|value| *value > 0)
        .ok_or_else(|| record_error("native MCP service PID is invalid"))?;
    if service_pid != u64::from(owned_service.process_id()) {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "native MCP proof does not match the current owned-service process",
        ));
    }
    let provider_auth = validated_provider_auth_projection(db, &snapshot, owned_service)?;
    let service_process_identity_digest = digest_json(&json!({
        "process_id":owned_service.process_id(),
        "process_birth_token":owned_service.process_birth_token(),
        "executable_sha256":owned_service.executable_sha256(),
    }))?;
    let model = json!({
        "id":facts.options.model.id,
        "provider_id":facts.options.model.provider_id,
        "variant":facts.options.model.variant,
    });
    let mut projection = json!({
        "schema_version":1,
        "kind":"launcher_native_mcp_dispatch_capability",
        "launch_operation_id":facts.operation_id,
        "dispatch_operation_id":dispatch_operation_id,
        "launch_identity_digest":identity_digest,
        "assignment_digest":assignment_digest,
        "evidence_digest":evidence_digest,
        "native_discovered_digest":native_discovered_digest,
        "session_context_digest":session_context_digest,
        "provider_request_digest":provider_request_digest,
        "assignment":{
            "task_id":assignment_value["task_id"],
            "task_revision":assignment_value["task_revision"],
            "attempt_id":assignment_value["attempt_id"],
            "binding_id":assignment_value["binding_id"],
            "binding_generation":assignment_value["binding_generation"],
            "participant_id":assignment_value["participant_id"],
            "grant_revision":assignment_value["grant_revision"],
            "native_session_id":assignment_value["native_session_id"],
        },
        "service":{
            "id":service_id,
            "pid":service_pid,
            "version":service_version,
            "process_identity_digest":service_process_identity_digest,
        },
        "model":model,
        "install":{
            "server_name":server_name,
            "command_sha256":command_sha256,
            "location_sha256":location_sha256,
            "state":"registered",
            "runtime_config_readback":"not_exposed_by_pinned_api",
            "matches_prepared_command":"unknown",
        },
        "capability":{
            "identity_digest":identity_digest,
            "evidence_digest":evidence_digest,
            "native_discovered_digest":native_discovered_digest,
            "session_context_digest":session_context_digest,
            "provider_request_digest":provider_request_digest,
            "service_id":service_id,
            "service_version":service_version,
            "plugin_id":plugin_id,
            "module_sha256":module_sha256,
        },
        "native_discovered":{
            "status":"observed",
            "observed_at_ms":observed_at_ms,
            "tools":scoped_tools,
        },
        "session_context":session_context,
        "provider_request":provider_request,
        "dispatch_permitted":false,
        "model_consumed":"unknown",
    });
    if let Some(provider_auth) = provider_auth {
        projection["provider_auth"] = provider_auth;
    }
    Ok(projection)
}

fn validated_provider_auth_projection(
    db: &Connection,
    snapshot: &launcher_native_mcp::NativeMcpLaunchSnapshot,
    owned_service: &launcher_native_mcp::OwnedServiceExpectation,
) -> Result<Option<Value>> {
    let (launch_operation_id, state, proof_json): (String, String, String) = db.query_row(
        "SELECT launch_operation_id,state,proof_json FROM owned_service_starts \
         WHERE binding_id=?1 AND binding_generation=?2",
        params![snapshot.binding_id(), snapshot.binding_generation()],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if launch_operation_id != snapshot.launch_operation_id() || state != "service_observed" {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "current owned-service receipt changed during capability validation",
        ));
    }
    let proof: Value = serde_json::from_str(&proof_json)
        .map_err(|_| record_error("retained owned-service proof is invalid JSON"))?;
    let proof_digest = model::digest(model::canonical(&proof)?.as_bytes());
    if proof_digest != owned_service.proof_digest() {
        return Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "current owned-service receipt differs from its validated binding proof",
        ));
    }
    match (
        owned_service.provider_auth_required(),
        proof.get("provider_auth"),
    ) {
        (true, Some(provider_auth)) if provider_auth.is_object() => {
            if provider_auth["status"] != "stored_unverified" {
                return Err(Error::new(
                    "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
                    "owned-service provider-auth proof is not in its validated state",
                ));
            }
            Ok(Some(json!({
                "status":"stored_unverified",
                "proof_digest":digest_json(provider_auth)?,
            })))
        }
        (true, _) => Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "current owned route requires retained provider-auth proof",
        )),
        (false, None) => Ok(None),
        (false, Some(_)) => Err(Error::new(
            "NATIVE_MCP_CAPABILITY_UNAVAILABLE",
            "retained provider-auth proof is not permitted by the current owned route",
        )),
    }
}

fn digest_json(value: &Value) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        model::digest(model::canonical(value)?.as_bytes())
    ))
}

fn project_session_context(raw: &Value, challenge: &Value) -> Result<Value> {
    model::fields(
        raw,
        &[
            "status",
            "stage",
            "observed_at_ms",
            "agent",
            "tools",
            "reason_code",
        ],
    )?;
    let status = model::text(raw, "status")?;
    if status == "unknown"
        && raw["stage"].is_null()
        && raw["observed_at_ms"].is_null()
        && raw["agent"].is_null()
        && raw["reason_code"].is_null()
        && raw["tools"].as_array().is_some_and(Vec::is_empty)
    {
        return Ok(json!({
            "status":"unknown",
            "stage":Value::Null,
            "observed_at_ms":Value::Null,
            "tools":[],
        }));
    }
    let observed_at_ms = raw["observed_at_ms"]
        .as_i64()
        .filter(|time| {
            *time >= challenge["issued_at_ms"].as_i64().unwrap_or(i64::MAX)
                && *time <= challenge["expires_at_ms"].as_i64().unwrap_or(0)
        })
        .ok_or_else(|| record_error("session-context observation time is invalid"))?;
    if raw["stage"] != "session_context_hook"
        || raw["agent"]
            .as_str()
            .is_none_or(|agent| !bounded_tool_label(agent))
    {
        return Err(record_error("session-context observation stage is invalid"));
    }
    match status {
        "observed" if raw["reason_code"].is_null() => Ok(json!({
            "status":"observed",
            "stage":"session_context_hook",
            "observed_at_ms":observed_at_ms,
            "tools":project_context_tools(&raw["tools"])? ,
        })),
        "unsupported"
            if raw["reason_code"] == "context_schema_unrecognized"
                && raw["tools"].as_array().is_some_and(Vec::is_empty) =>
        {
            Ok(json!({
                "status":"unsupported",
                "stage":"session_context_hook",
                "observed_at_ms":observed_at_ms,
                "tools":[],
            }))
        }
        _ => Err(record_error(
            "session-context observation status is invalid",
        )),
    }
}

fn project_provider_request(
    raw: &Value,
    challenge: &Value,
    expected_model: &Value,
) -> Result<Value> {
    model::fields(
        raw,
        &[
            "status",
            "transport",
            "stage",
            "kind",
            "observed_at_ms",
            "agent",
            "model",
            "tools",
            "reason_code",
        ],
    )?;
    let status = model::text(raw, "status")?;
    if status == "unknown"
        && raw["transport"].is_null()
        && raw["stage"].is_null()
        && raw["kind"].is_null()
        && raw["observed_at_ms"].is_null()
        && raw["agent"].is_null()
        && raw["model"].is_null()
        && raw["reason_code"].is_null()
        && raw["tools"].as_array().is_some_and(Vec::is_empty)
    {
        return Ok(json!({
            "status":"unknown",
            "transport":Value::Null,
            "stage":Value::Null,
            "observed_at_ms":Value::Null,
            "tools":[],
        }));
    }
    let transport = model::text(raw, "transport")?;
    if !matches!(transport, "http" | "websocket")
        || raw["stage"] != "before_transport"
        || raw["kind"] != "primary"
        || model::canonical(&raw["model"])? != model::canonical(expected_model)?
        || raw["agent"]
            .as_str()
            .is_none_or(|agent| !bounded_tool_label(agent))
    {
        return Err(record_error(
            "provider-request observation scope is invalid",
        ));
    }
    let observed_at_ms = raw["observed_at_ms"]
        .as_i64()
        .filter(|time| {
            *time >= challenge["issued_at_ms"].as_i64().unwrap_or(i64::MAX)
                && *time <= challenge["expires_at_ms"].as_i64().unwrap_or(0)
        })
        .ok_or_else(|| record_error("provider-request observation time is invalid"))?;
    match status {
        "observed" if raw["reason_code"].is_null() => Ok(json!({
            "status":"observed",
            "transport":transport,
            "stage":"before_transport",
            "observed_at_ms":observed_at_ms,
            "tools":project_context_tools(&raw["tools"])? ,
        })),
        "unsupported"
            if raw["reason_code"]
                .as_str()
                .is_some_and(valid_provider_reason_code)
                && raw["tools"].as_array().is_some_and(Vec::is_empty) =>
        {
            Ok(json!({
                "status":"unsupported",
                "transport":transport,
                "stage":"before_transport",
                "observed_at_ms":observed_at_ms,
                "tools":[],
            }))
        }
        _ => Err(record_error(
            "provider-request observation status is invalid",
        )),
    }
}

fn project_context_tools(raw: &Value) -> Result<Value> {
    let tools = raw
        .as_array()
        .filter(|tools| tools.len() <= 512)
        .ok_or_else(|| record_error("observed context tool list is invalid"))?;
    let mut names = BTreeSet::new();
    let mut projected = Vec::with_capacity(tools.len());
    for tool in tools {
        model::fields(tool, &["name", "description", "input_schema"])?;
        let name = model::text(tool, "name")?;
        if !bounded_tool_label(name)
            || !names.insert(name)
            || tool["description"]
                .as_str()
                .is_none_or(|description| description.len() > 16 * 1024)
            || !tool["input_schema"].is_object()
            || model::canonical(&tool["input_schema"])?.len() > 512 * 1024
        {
            return Err(record_error("observed context tool entry is invalid"));
        }
        projected.push(json!({
            "name":name,
            "input_schema":tool["input_schema"],
        }));
    }
    Ok(Value::Array(projected))
}

fn bounded_tool_label(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn valid_provider_reason_code(value: &str) -> bool {
    matches!(
        value,
        "request_not_object"
            | "tools_field_unrecognized"
            | "tool_count_limit"
            | "tool_schema_limit"
            | "tool_schema_unrecognized"
            | "request_body_limit"
            | "request_json_invalid"
            | "request_unreadable"
            | "frame_limit"
            | "frame_json_invalid"
    )
}

fn validate_record(
    record: &Value,
    facts: &LaunchFacts,
    assignment: &crate::native_mcp::AssignmentContext,
) -> Result<()> {
    if record["schema_version"] != 1
        || record["kind"] != "launcher_native_mcp_tools"
        || record["operation_id"].as_str() != Some(facts.operation_id.as_str())
        || record["launch_identity_digest"].as_str() != Some(facts.identity_digest.as_str())
        || model::canonical(&record["assignment"])? != model::canonical(&assignment.as_value())?
        || record["service"]["id"].as_str() != Some(facts.options.service_id.as_str())
        || record["service"]["version"].as_str() != Some(OPENCODE_VERSION)
        || record["service"]["pid"].as_u64().is_none_or(|pid| pid == 0)
        || record["service"]["directory_sha256"] != record["install"]["intent"]["location_sha256"]
        || record["install"]["intent"]["service_id"] != facts.options.service_id
        || model::canonical(&record["install"]["intent"]["assignment"])?
            != model::canonical(&record["assignment"])?
        || record["challenge"]["replaced_metadata"]
            .as_array()
            .is_some_and(|history| history.len() > MAX_REPLACED_CHALLENGES)
        || !record["challenge"]["replaced_metadata"].is_null()
            && !record["challenge"]["replaced_metadata"].is_array()
        || !valid_replacement_archive(&record["challenge"]["replacement_archive"])
        || record["dispatch_permitted"] != false
    {
        return Err(stale_scope());
    }
    Ok(())
}

fn validate_install_readback(
    readback: &Value,
    stored_intent: &Value,
    expected_intent: &Value,
    expected_pid: u32,
    expected_version: &str,
) -> Result<()> {
    if model::canonical(&readback["intent"])? != model::canonical(expected_intent)?
        || model::canonical(stored_intent)? != model::canonical(expected_intent)?
        || readback["runtime_entry_present"] != true
        || readback["service_process_id"].as_u64() != Some(u64::from(expected_pid))
        || readback["service_version"].as_str() != Some(expected_version)
        || !matches!(
            readback["runtime_status"].as_str(),
            Some("connected" | "pending" | "disabled" | "failed" | "needs_auth")
        )
        || readback["dispatch_permitted"] != false
        || readback["matches_prepared_command"] != "unknown"
    {
        return Err(Error::new(
            "NATIVE_MCP_SCOPE_MISMATCH",
            "native MCP install readback does not match the exact admitted location and process",
        ));
    }
    Ok(())
}

/// Find and durably reserve no more than one due C8 launch from the exact
/// pre-dispatch C7 candidate set. The cursor makes a bounded 64-row scan fair
/// when the candidate set is larger than one page. C7's success refresh is
/// intentionally not a C8 deadline: an `observed_partial` launch is eligible
/// immediately unless its own C8 marker is waiting/running/terminal.
fn claim_next_tools(tx: &Transaction<'_>, now: i64, config: &Config) -> Result<ToolsClaimOutcome> {
    let cursor = meta(tx, SUPERVISOR_CURSOR_KEY)?
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default();
    let ids = {
        let mut statement = tx.prepare(
            "SELECT operation_id FROM operations \
             WHERE method='swarm.launch' AND state='queued' \
               AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_native_mcp' \
               AND json_extract(effective_request_json,'$.launch_manifest.runtime.dispatch_permitted')=0 \
               AND json_extract(effective_request_json,'$.launch_manifest.progress.task_dispatch')='not_started' \
               AND json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state')='observed_partial' \
               AND operation_id>?1 \
             ORDER BY operation_id LIMIT ?2",
        )?;
        statement
            .query_map(params![cursor, MAX_CANDIDATES_PER_PASS], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };

    if ids.is_empty() {
        if !cursor.is_empty() {
            // Wrap on the next host tick so this invocation remains bounded
            // to one SQL page and one possible network action.
            set_meta(tx, SUPERVISOR_CURSOR_KEY, &json!(""))?;
        }
        return Ok(ToolsClaimOutcome::Idle(None));
    }

    let mut next_retry_at_ms: Option<i64> = None;
    let mut last_scanned: Option<String> = None;
    for operation_id in ids {
        last_scanned = Some(operation_id.clone());
        let schedule_key = supervisor_key(&operation_id);
        if let Some(schedule) = meta(tx, &schedule_key)? {
            validate_tools_schedule(&schedule, &operation_id)?;
            if schedule["state"] == "retry_wait" {
                let deadline = schedule["next_retry_at_ms"]
                    .as_i64()
                    .ok_or_else(|| record_error("native MCP retry deadline is missing"))?;
                if deadline > now {
                    next_retry_at_ms =
                        Some(next_retry_at_ms.map_or(deadline, |current| current.min(deadline)));
                    continue;
                }
            }
        }
        let facts = match load_launch_facts(tx, config, &operation_id) {
            Ok(facts) => facts,
            Err(error) if error.code == "STALE_LAUNCH" => {
                if let Some(deadline) = defer_stale_launch(tx, &operation_id, now)? {
                    next_retry_at_ms =
                        Some(next_retry_at_ms.map_or(deadline, |current| current.min(deadline)));
                }
                continue;
            }
            Err(error) if is_stale_scope_code(&error.code) => {
                let key = supervisor_key(&operation_id);
                let mut marker = match meta(tx, &key)? {
                    Some(marker) => {
                        validate_tools_schedule(&marker, &operation_id)?;
                        marker
                    }
                    None => new_tools_schedule(&operation_id, "", now),
                };
                marker["state"] = json!("stale");
                marker["started_at_ms"] = Value::Null;
                marker["next_retry_at_ms"] = Value::Null;
                marker["finished_at_ms"] = json!(now);
                marker["last_error_code"] = json!(safe_error_code(&error.code));
                set_meta(tx, &key, &marker)?;
                continue;
            }
            Err(error) => return Err(error),
        };

        let schedule_key = supervisor_key(&operation_id);
        let schedule = meta(tx, &schedule_key)?;
        if let Some(record) = meta(tx, &record_key(&operation_id))? {
            if record["schema_version"] != 1
                || record["kind"] != "launcher_native_mcp_tools"
                || record["operation_id"].as_str() != Some(operation_id.as_str())
            {
                return Err(record_error(
                    "native MCP private intent identity is invalid",
                ));
            }
            if record["launch_identity_digest"].as_str() != Some(facts.identity_digest.as_str()) {
                let mut stale = schedule.unwrap_or_else(|| {
                    new_tools_schedule(&operation_id, &facts.identity_digest, now)
                });
                validate_tools_schedule(&stale, &operation_id)?;
                stale["state"] = json!("stale");
                stale["launch_identity_digest"] = json!(facts.identity_digest);
                stale["started_at_ms"] = Value::Null;
                stale["next_retry_at_ms"] = Value::Null;
                stale["finished_at_ms"] = json!(now);
                stale["last_error_code"] = json!("NATIVE_MCP_STALE_ASSIGNMENT");
                set_meta(tx, &schedule_key, &stale)?;
                continue;
            }
            validate_record(&record, &facts, &facts.assignment)?;
            if record["tools_readback"].is_object()
                || record["install"]["state"] == "observed_after_unknown"
                || record["challenge"]["state"] == "observed"
            {
                let mut terminal = schedule.unwrap_or_else(|| {
                    new_tools_schedule(&operation_id, &facts.identity_digest, now)
                });
                validate_tools_schedule(&terminal, &operation_id)?;
                if terminal["launch_identity_digest"]
                    .as_str()
                    .is_some_and(|digest| digest != facts.identity_digest)
                {
                    return Err(stale_scope());
                }
                terminal["state"] = json!("observed_partial");
                terminal["launch_identity_digest"] = json!(facts.identity_digest);
                terminal["started_at_ms"] = Value::Null;
                terminal["next_retry_at_ms"] = Value::Null;
                terminal["finished_at_ms"] = json!(now);
                terminal["failure_attempts"] = json!(0);
                terminal["last_error_code"] = Value::Null;
                set_meta(tx, &schedule_key, &terminal)?;
                continue;
            }
        }

        let mut schedule = schedule
            .unwrap_or_else(|| new_tools_schedule(&operation_id, &facts.identity_digest, now));
        validate_tools_schedule(&schedule, &operation_id)?;
        if schedule["launch_identity_digest"].as_str() != Some(facts.identity_digest.as_str()) {
            schedule["state"] = json!("stale");
            schedule["started_at_ms"] = Value::Null;
            schedule["next_retry_at_ms"] = Value::Null;
            schedule["finished_at_ms"] = json!(now);
            schedule["last_error_code"] = json!("NATIVE_MCP_STALE_ASSIGNMENT");
            set_meta(tx, &schedule_key, &schedule)?;
            continue;
        }

        let state = schedule["state"].as_str().unwrap_or_default();
        let due = match state {
            "ready" | "retry_wait" => schedule["next_retry_at_ms"]
                .as_i64()
                .is_some_and(|deadline| deadline <= now),
            "running" => schedule["started_at_ms"]
                .as_i64()
                .is_some_and(|started| started <= now.saturating_sub(CLAIM_STALE_MS)),
            "observed_partial" | "stale" => false,
            _ => return Err(record_error("native MCP tools scheduler state is invalid")),
        };
        if !due {
            let deadline = if state == "running" {
                schedule["started_at_ms"]
                    .as_i64()
                    .map(|started| started.saturating_add(CLAIM_STALE_MS))
            } else {
                schedule["next_retry_at_ms"].as_i64()
            };
            if let Some(deadline) = deadline.filter(|deadline| *deadline >= now) {
                next_retry_at_ms =
                    Some(next_retry_at_ms.map_or(deadline, |current| current.min(deadline)));
            }
            continue;
        }

        let claim_generation = schedule["claim_generation"]
            .as_i64()
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);
        schedule["state"] = json!("running");
        schedule["claim_generation"] = json!(claim_generation);
        schedule["started_at_ms"] = json!(now);
        schedule["next_retry_at_ms"] = Value::Null;
        schedule["finished_at_ms"] = Value::Null;
        schedule["last_error_code"] = Value::Null;
        set_meta(tx, &schedule_key, &schedule)?;
        set_meta(tx, SUPERVISOR_CURSOR_KEY, &json!(operation_id))?;
        return Ok(ToolsClaimOutcome::Claimed(ToolsClaim {
            operation_id,
            claim_generation,
            started_at_ms: now,
            identity_digest: facts.identity_digest,
        }));
    }

    if let Some(operation_id) = last_scanned {
        set_meta(tx, SUPERVISOR_CURSOR_KEY, &json!(operation_id))?;
    }
    Ok(ToolsClaimOutcome::Idle(next_retry_at_ms))
}

fn defer_stale_launch(tx: &Transaction<'_>, operation_id: &str, now: i64) -> Result<Option<i64>> {
    let schedule_key = supervisor_key(operation_id);
    let record = meta(tx, &record_key(operation_id))?;
    if let Some(record) = record.as_ref()
        && !valid_public_c8_record(record, operation_id)
    {
        return Err(record_error(
            "native MCP private intent identity is invalid during stale launch deferral",
        ));
    }

    let mut schedule = match meta(tx, &schedule_key)? {
        Some(schedule) => {
            validate_tools_schedule(&schedule, operation_id)?;
            schedule
        }
        None if record.is_some() => {
            return Err(record_error(
                "native MCP tools record has no matching supervisor marker",
            ));
        }
        None => new_tools_schedule(operation_id, "", now),
    };
    let already_held_for_stale_launch =
        schedule["state"] == "stale" && schedule["last_error_code"] == "STALE_LAUNCH";
    let schedule_digest = schedule["launch_identity_digest"]
        .as_str()
        .ok_or_else(|| record_error("native MCP tools launch digest is missing"))?
        .to_owned();
    if !schedule_digest.is_empty() && !valid_prefixed_sha256(&schedule_digest) {
        return Err(record_error("native MCP tools launch digest is invalid"));
    }
    let record_digest = record
        .as_ref()
        .and_then(|record| record["launch_identity_digest"].as_str())
        .map(str::to_owned);
    if record_digest
        .as_deref()
        .is_some_and(|digest| digest != schedule_digest)
    {
        return Err(record_error(
            "native MCP tools record and supervisor launch digests differ",
        ));
    }

    schedule["started_at_ms"] = Value::Null;
    schedule["finished_at_ms"] = json!(now);
    schedule["last_error_code"] = json!("STALE_LAUNCH");
    let identity_digest =
        record_digest.or_else(|| (!schedule_digest.is_empty()).then_some(schedule_digest));
    let has_identity_digest = identity_digest.is_some();
    let mut event_attempt = schedule["failure_attempts"]
        .as_i64()
        .unwrap_or(0)
        .saturating_add(1)
        .max(1);
    let next_retry_at_ms = if let Some(identity_digest) = identity_digest {
        let failures = schedule["failure_attempts"]
            .as_i64()
            .unwrap_or(0)
            .saturating_add(1)
            .max(1);
        event_attempt = failures;
        let deadline = now.saturating_add(tools_retry_delay_ms(failures));
        schedule["state"] = json!("retry_wait");
        schedule["launch_identity_digest"] = json!(identity_digest);
        schedule["failure_attempts"] = json!(failures);
        schedule["next_retry_at_ms"] = json!(deadline);
        Some(deadline)
    } else {
        // Without an established launch digest there is no safe identity to
        // retry against after startup reconciliation. Keep the hold visible.
        schedule["state"] = json!("stale");
        schedule["next_retry_at_ms"] = Value::Null;
        None
    };
    set_meta(tx, &schedule_key, &schedule)?;
    // A missing identity leaves a persistent stale hold. Emit its first
    // durable occurrence only; subsequent supervisor ticks must not append
    // another event for the same unchanged hold.
    if has_identity_digest || !already_held_for_stale_launch {
        launcher_native_mcp::insert_safe_failure_observation(
            tx,
            operation_id,
            "native_mcp_tools",
            "stale_launch_hold",
            event_attempt,
            "STALE_LAUNCH",
            now,
        )?;
    }
    Ok(next_retry_at_ms)
}

fn new_tools_schedule(operation_id: &str, identity_digest: &str, now: i64) -> Value {
    json!({
        "schema_version":1,
        "kind":"launcher_native_mcp_tools_supervisor",
        "operation_id":operation_id,
        "launch_identity_digest":identity_digest,
        "state":"ready",
        "claim_generation":0,
        "started_at_ms":null,
        "next_retry_at_ms":now,
        "finished_at_ms":null,
        "failure_attempts":0,
        "last_error_code":null,
    })
}

fn validate_tools_schedule(schedule: &Value, operation_id: &str) -> Result<()> {
    if schedule["schema_version"] != 1
        || schedule["kind"] != "launcher_native_mcp_tools_supervisor"
        || schedule["operation_id"].as_str() != Some(operation_id)
        || schedule["claim_generation"]
            .as_i64()
            .is_none_or(|value| value < 0)
        || schedule["failure_attempts"]
            .as_i64()
            .is_none_or(|value| value < 0)
        || !matches!(
            schedule["state"].as_str(),
            Some("ready" | "running" | "retry_wait" | "observed_partial" | "stale")
        )
    {
        return Err(record_error("native MCP tools scheduler marker is invalid"));
    }
    Ok(())
}

fn supervisor_key(operation_id: &str) -> String {
    format!(
        "{SUPERVISOR_KEY_PREFIX}{}",
        model::digest(operation_id.as_bytes())
    )
}

fn tools_retry_delay_ms(failures: i64) -> i64 {
    let shift = failures.saturating_sub(1).clamp(0, 8) as u32;
    INITIAL_RETRY_MS
        .saturating_mul(1_i64 << shift)
        .min(MAX_RETRY_MS)
}

fn is_stale_scope_code(code: &str) -> bool {
    matches!(
        code,
        "NATIVE_MCP_SCOPE_MISMATCH"
            | "NATIVE_MCP_STALE_ASSIGNMENT"
            | "STALE_LAUNCH"
            | "STALE_PARTICIPANT"
            | "FORBIDDEN"
            | "UNAUTHORIZED"
    )
}

fn safe_error_code(code: &str) -> String {
    safe_label(code).unwrap_or_else(|_| "NATIVE_MCP_ERROR".to_owned())
}

fn challenge_replacement_reason(code: &str) -> Option<ChallengeReplacementReason> {
    match code {
        "NATIVE_MCP_PROOF_CHALLENGE_EXPIRED" => Some(ChallengeReplacementReason::Expired),
        "NATIVE_MCP_PROOF_MODULE_ROTATED" => Some(ChallengeReplacementReason::ModuleRotated),
        _ => None,
    }
}

fn valid_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_prefixed_sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(valid_sha256_hex)
}

fn valid_replacement_archive(value: &Value) -> bool {
    if value.is_null() {
        return true;
    }
    let Some(count) = value["count"].as_i64().filter(|count| *count >= 0) else {
        return false;
    };
    match (count, value["digest"].as_str()) {
        (0, None) => true,
        (0, Some(_)) => false,
        (_, Some(digest)) => valid_prefixed_sha256(digest),
        (_, None) => false,
    }
}

fn install_target(record: &Value) -> Result<(u32, String)> {
    let pid = record["service"]["pid"]
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| record_error("native MCP target PID is missing"))?;
    let version = record["service"]["version"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| record_error("native MCP target version is missing"))?
        .to_owned();
    Ok((pid, version))
}

fn challenge_target(record: &Value) -> Result<(u32, String)> {
    let metadata = &record["challenge"]["metadata"];
    let pid = metadata["service_pid"]
        .as_u64()
        .and_then(|value| u32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| record_error("native MCP challenge PID is missing"))?;
    let version = metadata["service_version"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| record_error("native MCP challenge version is missing"))?
        .to_owned();
    let directory = metadata["directory"]
        .as_str()
        .ok_or_else(|| record_error("native MCP challenge location is missing"))?;
    let expected_location = record["install"]["intent"]["location_sha256"]
        .as_str()
        .ok_or_else(|| record_error("native MCP install location digest is missing"))?;
    let actual_location = format!("sha256:{}", model::digest(directory.as_bytes()));
    if metadata["service_id"] != record["service"]["id"]
        || actual_location != expected_location
        || record["service"]["pid"].as_u64() != Some(u64::from(pid))
        || record["service"]["version"].as_str() != Some(version.as_str())
    {
        return Err(stale_scope());
    }
    Ok((pid, version))
}

fn record_key(operation_id: &str) -> String {
    format!(
        "{RECORD_KEY_PREFIX}{}",
        model::digest(operation_id.as_bytes())
    )
}

const MAX_PUBLIC_C8_RECORD_BYTES: usize = MAX_PRIVATE_READBACK_BYTES + 65_536;
const MAX_PUBLIC_C8_SCHEDULE_BYTES: usize = 4096;
// Capability readback must not scan an unbounded set of historical launch
// Operations for one Attempt. Fetch one sentinel row beyond the accepted
// bound so overflow is reported as relay_only instead of silently truncating
// ancestry or choosing a potentially ambiguous parent.
const MAX_PARTICIPANT_LAUNCH_LINEAGE_ROWS: usize = 64;

enum DiagnosticMeta {
    Missing,
    Value(Value),
    Corrupt,
}

struct PublicC8Schedule<'a> {
    state: &'a str,
    failure_attempts: i64,
    next_retry_at_ms: Option<i64>,
    last_error_code: Option<&'a str>,
    launch_identity_digest: &'a str,
}

/// Return only the validated, bounded C8 status needed by the current-GM
/// Operation projection. This deliberately does not revalidate the historical
/// launch against today's Participant grant or assignment: the operation ID
/// binds these diagnostics, while current authority and operation visibility
/// are enforced by the caller.
pub(super) fn diagnostic_for_operation(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<Value>> {
    let method: Option<String> = db
        .query_row(
            "SELECT method FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| row.get(0),
        )
        .optional()?;
    if method.as_deref() != Some("swarm.launch") {
        return Ok(None);
    }

    let record = read_diagnostic_meta(db, &record_key(operation_id), MAX_PUBLIC_C8_RECORD_BYTES)?;
    let schedule = read_diagnostic_meta(
        db,
        &supervisor_key(operation_id),
        MAX_PUBLIC_C8_SCHEDULE_BYTES,
    )?;
    if matches!(&record, DiagnosticMeta::Missing) && matches!(&schedule, DiagnosticMeta::Missing) {
        return Ok(None);
    }
    let corrupt = || Some(native_mcp_tools_diagnostic_corrupt());
    if matches!(&record, DiagnosticMeta::Corrupt) || matches!(&schedule, DiagnosticMeta::Corrupt) {
        return Ok(corrupt());
    }

    let record = match record {
        DiagnosticMeta::Value(value) => Some(value),
        DiagnosticMeta::Missing => None,
        DiagnosticMeta::Corrupt => return Ok(corrupt()),
    };
    let schedule = match schedule {
        DiagnosticMeta::Value(value) => Some(value),
        DiagnosticMeta::Missing => None,
        DiagnosticMeta::Corrupt => return Ok(corrupt()),
    };

    let record_digest = if let Some(record) = record.as_ref() {
        if !valid_public_c8_record(record, operation_id) {
            return Ok(corrupt());
        }
        record["launch_identity_digest"].as_str()
    } else {
        None
    };
    let Some(schedule) = schedule.as_ref() else {
        // Every durable C8 record is created only after the supervisor claim.
        return Ok(corrupt());
    };
    let Some(schedule) = public_c8_schedule(schedule, operation_id) else {
        return Ok(corrupt());
    };
    if let Some(record_digest) = record_digest
        && schedule.launch_identity_digest != record_digest
    {
        return Ok(corrupt());
    }

    let latest_failure = match record.as_ref().map(|record| &record["last_error"]) {
        None | Some(Value::Null) => Value::Null,
        Some(failure) if valid_public_c8_failure(failure) => {
            let mut public = json!({
                "schema_version":1,
                "code":failure["code"],
                "stage":failure["stage"],
                "recorded_at_ms":failure["recorded_at_ms"],
            });
            if let Some(class) = failure
                .get("rejection_class")
                .and_then(Value::as_str)
                .and_then(NativeRpcRejectionClass::parse)
            {
                public["rejection_class"] = json!(class.as_str());
            }
            public
        }
        Some(_) => return Ok(corrupt()),
    };

    Ok(Some(json!({
        "schema_version":1,
        "state":schedule.state,
        "failure_attempts":schedule.failure_attempts,
        "next_retry_at_ms":schedule.next_retry_at_ms,
        "last_error_code":schedule.last_error_code,
        "latest_failure":latest_failure,
        "model_consumed":"unknown",
        "dispatch_permitted":false,
    })))
}

fn read_diagnostic_meta(db: &Connection, key: &str, max_bytes: usize) -> Result<DiagnosticMeta> {
    let bounded_max = i64::try_from(max_bytes).unwrap_or(i64::MAX);
    let raw: Option<(i64, Option<String>)> = db
        .query_row(
            "SELECT length(CAST(value_json AS BLOB)),
                    CASE WHEN length(CAST(value_json AS BLOB))<=?2 THEN value_json END
             FROM meta WHERE key=?1",
            params![key, bounded_max],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((bytes, raw)) = raw else {
        return Ok(DiagnosticMeta::Missing);
    };
    if bytes < 0 || bytes > bounded_max {
        return Ok(DiagnosticMeta::Corrupt);
    }
    let Some(raw) = raw else {
        return Ok(DiagnosticMeta::Corrupt);
    };
    match serde_json::from_str(&raw) {
        Ok(value) => Ok(DiagnosticMeta::Value(value)),
        Err(_) => Ok(DiagnosticMeta::Corrupt),
    }
}

fn valid_public_c8_record(record: &Value, operation_id: &str) -> bool {
    record["schema_version"] == 1
        && record["kind"] == "launcher_native_mcp_tools"
        && record["operation_id"].as_str() == Some(operation_id)
        && record["launch_identity_digest"]
            .as_str()
            .is_some_and(valid_prefixed_sha256)
        && record.get("last_error").is_some()
}

fn public_c8_schedule<'a>(schedule: &'a Value, operation_id: &str) -> Option<PublicC8Schedule<'a>> {
    if schedule["schema_version"] != 1
        || schedule["kind"] != "launcher_native_mcp_tools_supervisor"
        || schedule["operation_id"].as_str() != Some(operation_id)
        || schedule["claim_generation"]
            .as_i64()
            .is_none_or(|value| value < 0)
        || schedule["failure_attempts"]
            .as_i64()
            .is_none_or(|value| value < 0)
    {
        return None;
    }
    let state = schedule["state"].as_str()?;
    if !matches!(
        state,
        "ready" | "running" | "retry_wait" | "observed_partial" | "stale"
    ) {
        return None;
    }
    let digest = schedule["launch_identity_digest"].as_str()?;
    if !digest.is_empty() && !valid_prefixed_sha256(digest) {
        return None;
    }
    if digest.is_empty() && state != "stale" {
        return None;
    }
    let nonnegative_or_null = |field: &str| -> Option<Option<i64>> {
        if schedule.get(field)?.is_null() {
            Some(None)
        } else {
            schedule[field]
                .as_i64()
                .filter(|value| *value >= 0)
                .map(Some)
        }
    };
    let started_at_ms = nonnegative_or_null("started_at_ms")?;
    let next_retry_at_ms = nonnegative_or_null("next_retry_at_ms")?;
    let finished_at_ms = nonnegative_or_null("finished_at_ms")?;
    let valid_timing = match state {
        "ready" | "retry_wait" => next_retry_at_ms.is_some() && started_at_ms.is_none(),
        "running" => {
            next_retry_at_ms.is_none() && started_at_ms.is_some() && finished_at_ms.is_none()
        }
        "observed_partial" | "stale" => {
            next_retry_at_ms.is_none() && started_at_ms.is_none() && finished_at_ms.is_some()
        }
        _ => false,
    };
    if !valid_timing {
        return None;
    }
    schedule.get("last_error_code")?;
    let last_error_code = if let Some(error_code) = schedule["last_error_code"].as_str() {
        if !valid_public_c8_code(error_code) {
            return None;
        }
        Some(error_code)
    } else if !schedule["last_error_code"].is_null() {
        return None;
    } else {
        None
    };
    Some(PublicC8Schedule {
        state,
        failure_attempts: schedule["failure_attempts"].as_i64()?,
        next_retry_at_ms,
        last_error_code,
        launch_identity_digest: digest,
    })
}

fn valid_public_c8_failure(failure: &Value) -> bool {
    if !failure.is_object() {
        return false;
    }
    let stage = failure["stage"].as_str().unwrap_or_default();
    let code = failure["code"].as_str().unwrap_or_default();
    let class_valid = match failure.get("rejection_class") {
        None => true,
        Some(Value::String(value)) => valid_public_c8_rejection_class(value, stage, code),
        Some(_) => false,
    };
    // Legacy records may contain private detail. Project only the validated
    // scalar fields rather than rejecting their otherwise usable failure.
    valid_public_c8_code(code)
        && matches!(
            stage,
            "install" | "install_readback" | "challenge_preflight" | "challenge" | "tools_readback"
        )
        && failure["recorded_at_ms"]
            .as_i64()
            .is_some_and(|value| value >= 0)
        && class_valid
}

fn valid_public_c8_rejection_class(class: &str, stage: &str, code: &str) -> bool {
    if !matches!(stage, "challenge" | "tools_readback") {
        return false;
    }
    match NativeRpcRejectionClass::parse(class) {
        Some(
            NativeRpcRejectionClass::InvalidInput
            | NativeRpcRejectionClass::MethodNotFound
            | NativeRpcRejectionClass::Unavailable,
        ) => code == "NATIVE_REJECTED",
        Some(NativeRpcRejectionClass::InvalidOutput | NativeRpcRejectionClass::Internal) => {
            code == "NATIVE_OUTCOME_UNKNOWN"
        }
        Some(NativeRpcRejectionClass::Unclassified) => {
            matches!(code, "NATIVE_REJECTED" | "NATIVE_OUTCOME_UNKNOWN")
        }
        None => false,
    }
}

fn valid_public_c8_code(code: &str) -> bool {
    !code.is_empty()
        && code.len() <= 64
        && code.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}

fn native_mcp_tools_diagnostic_corrupt() -> Value {
    json!({
        "schema_version":1,
        "state":"unknown",
        "latest_failure":{"schema_version":1,"code":"NATIVE_MCP_TOOLS_DIAGNOSTIC_CORRUPT"},
        "model_consumed":"unknown",
        "dispatch_permitted":false,
    })
}

fn public_summary(record: &Value) -> Value {
    let tools = &record["tools_readback"];
    let has_readback = tools.is_object();
    let schema_digest = |evidence: &Value| {
        if evidence["status"] == "observed" {
            digest_json(&evidence["tools"])
                .map(Value::String)
                .unwrap_or(Value::Null)
        } else {
            Value::Null
        }
    };
    json!({
        "operation_id":record["operation_id"],
        "state":if has_readback {"observed_partial"} else {"pending"},
        "install_state":record["install"]["state"],
        "runtime_status":record["install"]["readback"]["runtime_status"],
        "observer_state":record["challenge"]["state"],
        "native_discovered":if has_readback {tools["native_discovered"]["status"].clone()} else {Value::String("unknown".to_owned())},
        "session_context":if has_readback {tools["session_context"]["status"].clone()} else {Value::String("unknown".to_owned())},
        "session_context_schema_digest":if has_readback {schema_digest(&tools["session_context"])} else {Value::Null},
        "provider_request":if has_readback {tools["provider_request"]["status"].clone()} else {Value::String("unknown".to_owned())},
        "provider_request_schema_digest":if has_readback {schema_digest(&tools["provider_request"])} else {Value::Null},
        "model_consumed":"unknown",
        "last_error":record["last_error"],
        "dispatch_permitted":false,
    })
}

// This is deliberately a bounded Participant read projection. It reuses the
// durable C8 record and the retained launch Operation; it never contacts the
// native service, returns private identity/configuration, or grants dispatch.
const PARTICIPANT_RUNTIME_CORE_TOOLS: &[(&str, &str)] = &[
    ("swarm.context.get", "swarm_context_get"),
    ("coordination.send", "coordination_send"),
    ("coordination.consult", "coordination_consult"),
    ("operation.get", "operation_get"),
];

pub(crate) struct ParticipantCapabilityScope<'a> {
    pub(crate) participant_id: &'a str,
    pub(crate) task_id: &'a str,
    pub(crate) task_revision: i64,
    pub(crate) attempt_id: &'a str,
    pub(crate) binding_id: Option<&'a str>,
    pub(crate) binding_generation: Option<i64>,
    pub(crate) grant_revision: Option<i64>,
    pub(crate) native_session_id: Option<&'a str>,
    pub(crate) basis_kind: Option<&'a str>,
}

pub(crate) fn participant_capability_projection(
    db: &Connection,
    scope_input: ParticipantCapabilityScope<'_>,
) -> Result<Value> {
    let ParticipantCapabilityScope {
        participant_id,
        task_id,
        task_revision,
        attempt_id,
        binding_id,
        binding_generation,
        grant_revision,
        native_session_id,
        basis_kind,
    } = scope_input;
    let scope = json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "binding_id":binding_id,
        "binding_generation":binding_generation,
    });
    let required_core_methods: Vec<&str> = PARTICIPANT_RUNTIME_CORE_TOOLS
        .iter()
        .map(|(method, _)| *method)
        .collect();
    let profile = if basis_kind == Some("sponsored_reviewer") {
        "assigned-reviewer"
    } else {
        "participant"
    };
    let surface = if basis_kind == Some("sponsored_reviewer") {
        "assigned-reviewer"
    } else {
        "participant-core"
    };
    let mut projection = json!({
        "schema_version":1,
        "profile":profile,
        "surface":surface,
        "coverage":"bounded_required_core_only",
        "presentation":"pull_only",
        "scope":scope,
        "current_task":{
            "task_id":task_id,
            "revision":task_revision,
            "attempt_id":attempt_id,
        },
        "state":"relay_only",
        "availability":{
            "configured":"unknown",
            "installed":"unknown",
            "callable":"unknown",
        },
        "core_tools":{
            "observed":[],
            "missing":required_core_methods,
            "coverage":"bounded_required_core_only",
        },
        "boot":{
            "launch_operation_id":null,
            "launch_state":null,
            "dispatch_stage":null,
            "runtime_status":"unknown",
            "scope_match":"unresolved",
        },
        "readiness":{
            "state":"relay_only",
            "dispatch_permitted":false,
            "model_consumed":"unknown",
        },
        "observations":{
            "native_discovered":"unknown",
            "session_context":"unknown",
            "provider_request":"unknown",
        },
        "gaps":[],
    });

    // A sponsored reviewer may use the same context read, but its retained
    // review slot is never a native launch or Task-submission authority.
    if basis_kind == Some("sponsored_reviewer") {
        projection["state"] = json!("relay_only");
        projection["availability"] = json!({
            "configured":"not_applicable",
            "installed":"not_applicable",
            "callable":"not_applicable",
        });
        projection["boot"]["scope_match"] = json!("exact_review_scope");
        projection["readiness"] = json!({
            "state":"relay_only",
            "dispatch_permitted":false,
            "model_consumed":"unknown",
        });
        projection["gaps"] = json!([
            "review_only_participant_scope",
            "native_dispatch_not_authorized",
        ]);
        return Ok(projection);
    }

    let Some(binding_id) = binding_id.filter(|value| !value.is_empty()) else {
        projection["gaps"] = json!(["current_binding_missing"]);
        return Ok(projection);
    };
    let Some(binding_generation) = binding_generation.filter(|value| *value > 0) else {
        projection["gaps"] = json!(["current_binding_generation_missing"]);
        return Ok(projection);
    };
    let Some(grant_revision) = grant_revision.filter(|value| *value > 0) else {
        projection["gaps"] = json!(["current_grant_revision_missing"]);
        return Ok(projection);
    };
    let Some(native_session_id) = native_session_id.filter(|value| !value.is_empty()) else {
        projection["gaps"] = json!(["current_native_session_missing"]);
        return Ok(projection);
    };

    // Retain only the exact current launch lineage. A stale or unrelated
    // launch is not a capability receipt for this Participant scope.
    let mut statement = db.prepare(
        r#"SELECT operation_id,state,task_id,attempt_id,binding_id,binding_generation,
                  effective_request_json
           FROM operations
          WHERE method='swarm.launch'
            AND (attempt_id=?1
              OR json_extract(effective_request_json,'$.launch_manifest.task.attempt_id')=?1)
          ORDER BY created_at_ms,operation_id
          LIMIT ?2"#,
    )?;
    let scan_limit = i64::try_from(MAX_PARTICIPANT_LAUNCH_LINEAGE_ROWS)
        .ok()
        .and_then(|limit| limit.checked_add(1))
        .unwrap_or(i64::MAX);
    let rows = statement.query_map(params![attempt_id, scan_limit], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<i64>>(5)?,
            row.get::<_, String>(6)?,
        ))
    })?;
    let mut parent: Option<(String, String, Value)> = None;
    let mut ancestry_corrupt = false;
    let mut scanned_lineage_rows = 0usize;
    for row in rows {
        scanned_lineage_rows = scanned_lineage_rows.saturating_add(1);
        if scanned_lineage_rows > MAX_PARTICIPANT_LAUNCH_LINEAGE_ROWS {
            projection["state"] = json!("relay_only");
            projection["readiness"] = json!({
                "state":"relay_only",
                "dispatch_permitted":false,
                "model_consumed":"unknown",
            });
            projection["gaps"] = json!(["launch_lineage_scan_bound_exceeded"]);
            return Ok(projection);
        }
        let (
            operation_id,
            state,
            row_task_id,
            row_attempt_id,
            row_binding_id,
            row_generation,
            effective_json,
        ) = row?;
        let effective: Value = match serde_json::from_str(&effective_json) {
            Ok(value) => value,
            Err(_) => {
                ancestry_corrupt = true;
                continue;
            }
        };
        let Some(manifest) = effective.get("launch_manifest") else {
            ancestry_corrupt = true;
            continue;
        };
        let row_attempt_matches = row_attempt_id.as_deref() == Some(attempt_id);
        let manifest_attempt_matches = manifest["task"]["attempt_id"].as_str() == Some(attempt_id);
        if !row_attempt_matches && !manifest_attempt_matches {
            continue;
        }
        if row_task_id.as_deref() != Some(task_id)
            || !row_attempt_matches
            || !manifest_attempt_matches
            || manifest["task"]["task_id"] != task_id
        {
            ancestry_corrupt = true;
            continue;
        }
        let row_binding_mismatch = row_binding_id
            .as_deref()
            .is_some_and(|value| value != binding_id)
            || row_generation.is_some_and(|value| value != binding_generation);
        if manifest["task"]["observed_revision"] != task_revision
            || row_binding_mismatch
            || manifest["binding"]["binding_id"] != binding_id
            || manifest["binding"]["generation"] != binding_generation
        {
            continue;
        }
        if parent.is_some() {
            projection["state"] = json!("incompatible");
            projection["readiness"] = json!({
                "state":"incompatible",
                "dispatch_permitted":false,
                "model_consumed":"unknown",
            });
            projection["gaps"] = json!(["launch_ancestry_ambiguous"]);
            return Ok(projection);
        }
        parent = Some((operation_id, state, manifest.clone()));
    }
    drop(statement);

    let Some((launch_operation_id, launch_state, manifest)) = parent else {
        if ancestry_corrupt {
            projection["state"] = json!("incompatible");
            projection["readiness"] = json!({
                "state":"incompatible",
                "dispatch_permitted":false,
                "model_consumed":"unknown",
            });
            projection["gaps"] = json!(["launch_ancestry_corrupt"]);
        } else {
            projection["gaps"] = json!(["current_launch_operation_not_found"]);
        }
        return Ok(projection);
    };

    projection["boot"] = json!({
        "launch_operation_id":launch_operation_id.clone(),
        "launch_state":public_launch_state(&launch_state),
        "dispatch_stage":public_dispatch_stage(&manifest["progress"]["task_dispatch"]),
        "runtime_status":"unknown",
        "scope_match":"exact",
    });

    let record = match read_diagnostic_meta(
        db,
        &record_key(&launch_operation_id),
        MAX_PUBLIC_C8_RECORD_BYTES,
    )? {
        DiagnosticMeta::Missing => {
            projection["gaps"] = json!(["runtime_capability_receipt_not_recorded"]);
            return Ok(projection);
        }
        DiagnosticMeta::Corrupt => {
            projection["state"] = json!("incompatible");
            projection["readiness"] = json!({
                "state":"incompatible",
                "dispatch_permitted":false,
                "model_consumed":"unknown",
            });
            projection["gaps"] = json!(["runtime_capability_receipt_corrupt"]);
            return Ok(projection);
        }
        DiagnosticMeta::Value(value) => value,
    };
    if !valid_public_c8_record(&record, &launch_operation_id) {
        projection["state"] = json!("incompatible");
        projection["readiness"] = json!({
            "state":"incompatible",
            "dispatch_permitted":false,
            "model_consumed":"unknown",
        });
        projection["gaps"] = json!(["runtime_capability_receipt_invalid"]);
        return Ok(projection);
    }

    let assignment = &record["assignment"];
    if assignment["task_id"] != task_id
        || assignment["task_revision"] != task_revision
        || assignment["attempt_id"] != attempt_id
        || assignment["binding_id"] != binding_id
        || assignment["binding_generation"] != binding_generation
        || assignment["grant_revision"] != grant_revision
        || assignment["participant_id"] != participant_id
        || assignment["native_session_id"] != native_session_id
        || record["dispatch_permitted"] != false
    {
        projection["state"] = json!("incompatible");
        projection["readiness"] = json!({
            "state":"incompatible",
            "dispatch_permitted":false,
            "model_consumed":"unknown",
        });
        projection["gaps"] = json!(["runtime_capability_scope_mismatch"]);
        return Ok(projection);
    }

    let install_state = public_c8_status(
        &record["install"]["state"],
        &[
            "prepared",
            "outcome_unknown",
            "registered",
            "observed_after_unknown",
        ],
    );
    let runtime_status = public_c8_status(
        &record["install"]["readback"]["runtime_status"],
        &["connected", "pending", "disabled", "failed", "needs_auth"],
    );
    let observer_state = public_c8_status(
        &record["challenge"]["state"],
        &[
            "not_started",
            "prepared",
            "outcome_unknown",
            "armed",
            "observed",
        ],
    );
    let tools = &record["tools_readback"];
    let native_discovered = public_c8_status(
        &tools["native_discovered"]["status"],
        &["unknown", "observed"],
    );
    let session_context = public_c8_status(
        &tools["session_context"]["status"],
        &["unknown", "observed"],
    );
    let provider_request = public_c8_status(
        &tools["provider_request"]["status"],
        &["unknown", "observed"],
    );
    let mut observed = BTreeSet::new();
    let mut inventory_shape_ok = true;
    let expected_server = record["install"]["intent"]["server_name"].as_str();
    if expected_server.is_none() {
        inventory_shape_ok = false;
    }
    if native_discovered == "observed" {
        match tools["native_discovered"]["tools"].as_array() {
            Some(items) if items.len() <= 512 => {
                for item in items {
                    if item["server"].as_str() != expected_server {
                        continue;
                    }
                    if let Some(name) = item["name"].as_str() {
                        for &(method, native_name) in PARTICIPANT_RUNTIME_CORE_TOOLS {
                            if name == native_name {
                                observed.insert(method.to_owned());
                            }
                        }
                    }
                }
            }
            _ => inventory_shape_ok = false,
        }
    }
    let observed: Vec<String> = observed.into_iter().collect();
    let missing: Vec<String> = PARTICIPANT_RUNTIME_CORE_TOOLS
        .iter()
        .filter(|entry| !observed.iter().any(|item| item.as_str() == entry.0))
        .map(|entry| entry.0.to_owned())
        .collect();
    let missing_values: Vec<Value> = missing.iter().cloned().map(Value::String).collect();
    let observed_values: Vec<Value> = observed.iter().cloned().map(Value::String).collect();
    let mut core_status = serde_json::Map::new();
    for &(method, _) in PARTICIPANT_RUNTIME_CORE_TOOLS {
        let status = if observed.iter().any(|item| item == method) {
            "available"
        } else {
            "missing"
        };
        core_status.insert(method.to_owned(), json!(status));
    }
    projection["core_tools"] = json!({
        "observed":observed_values,
        "missing":missing_values,
        "status":core_status,
        "coverage":"bounded_required_core_only",
    });
    projection["availability"] = json!({
        "configured":install_state,
        "installed":runtime_status,
        "callable":if native_discovered == "observed" && inventory_shape_ok {
            "observed"
        } else {
            "unknown"
        },
    });
    projection["observations"] = json!({
        "native_discovered":native_discovered,
        "session_context":session_context,
        "provider_request":provider_request,
    });
    projection["boot"]["runtime_status"] = json!(runtime_status);

    let mut gaps = vec!["model_consumption_unknown".to_owned()];
    if session_context == "unknown" {
        gaps.push("session_context_not_observed".to_owned());
    }
    if provider_request == "unknown" {
        gaps.push("provider_request_not_observed".to_owned());
    }
    let state = if matches!(runtime_status, "failed" | "needs_auth" | "disabled") {
        gaps.push("native_runtime_unavailable".to_owned());
        "incompatible"
    } else if runtime_status != "connected" {
        gaps.push("native_runtime_not_connected".to_owned());
        "relay_only"
    } else if install_state == "observed_after_unknown" {
        gaps.push("installation_observed_after_unknown".to_owned());
        "relay_only"
    } else if install_state != "registered" {
        gaps.push("native_install_not_registered".to_owned());
        "relay_only"
    } else if observer_state != "observed"
        || native_discovered != "observed"
        || !inventory_shape_ok
        || !missing.is_empty()
    {
        if !missing.is_empty() {
            gaps.push("required_core_tools_missing".to_owned());
        }
        if observer_state != "observed" || native_discovered != "observed" || !inventory_shape_ok {
            gaps.push("callability_not_observed".to_owned());
        }
        "relay_only"
    } else {
        gaps.push("dispatch_remains_manager_owned".to_owned());
        "ready_with_gaps"
    };
    projection["state"] = json!(state);
    projection["readiness"] = json!({
        "state":state,
        "dispatch_permitted":false,
        "model_consumed":"unknown",
    });
    projection["gaps"] = Value::Array(gaps.into_iter().map(Value::String).collect());
    Ok(projection)
}

fn public_c8_status(value: &Value, allowed: &[&'static str]) -> &'static str {
    let value = value.as_str().unwrap_or("unknown");
    allowed
        .iter()
        .copied()
        .find(|allowed| *allowed == value)
        .unwrap_or("unknown")
}

fn public_launch_state(state: &str) -> &'static str {
    match state {
        "queued" => "queued",
        "running" => "running",
        "settled" => "settled",
        "rejected" => "rejected",
        "cancelled" => "cancelled",
        "outcome_unknown" => "outcome_unknown",
        _ => "unknown",
    }
}

fn public_dispatch_stage(value: &Value) -> &'static str {
    match value.as_str() {
        Some("not_started") => "not_started",
        Some("admitted") => "admitted",
        Some("dispatched") => "dispatched",
        Some("completed") => "completed",
        Some("failed") => "failed",
        _ => "unknown",
    }
}

fn safe_label(value: &str) -> Result<String> {
    if value.is_empty()
        || value.len() > 96
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
    {
        return Err(Error::invalid("native MCP status label is invalid"));
    }
    Ok(value.to_owned())
}

fn scope_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_SCOPE_MISMATCH", message)
}

fn stale_scope() -> Error {
    Error::new(
        "NATIVE_MCP_STALE_ASSIGNMENT",
        "launch, Participant, binding, or held lease changed during native MCP admission",
    )
}

fn record_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_STORE", message)
}

#[cfg(test)]
mod failure_event_tests {
    use super::*;

    const OPERATION_ID: &str = "native-mcp-stale-event-launch";

    #[test]
    fn stale_launch_hold_is_selectable_once_without_tick_duplicates() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        db.execute(
            "INSERT INTO operations(
                 operation_id,caller_id,client_request_id,method,original_request_json,
                 effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms
             ) VALUES(?1,'manager','native-mcp-stale-event-request','swarm.launch',
                      '{}','{}','queued',1,1,1)",
            [OPERATION_ID],
        )
        .unwrap();

        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert_eq!(defer_stale_launch(&tx, OPERATION_ID, 100).unwrap(), None);
        tx.commit().unwrap();

        let (observation_id, source_id, event_kind, operation_id, recorded_at_ms): (
            i64,
            String,
            String,
            String,
            i64,
        ) = db
            .query_row(
                "SELECT observation_id,source_stream_id,kind,operation_id,recorded_at_ms \
                 FROM observations WHERE source_stream_id='controller:native-mcp' \
                   AND kind='native.mcp.failure'",
                [],
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
            .unwrap();
        let event = crate::automation::intake::ObservedEvent {
            observation_id,
            source_id: source_id.clone(),
            event_kind: event_kind.clone(),
            operation_id: Some(operation_id),
            recorded_at_ms,
        };
        let projection =
            super::super::automation_intake::safe_event_projection(&db, &event).unwrap();
        assert_eq!(
            projection.status,
            Some(crate::automation::event_rules::EventStatus::Failed)
        );
        assert_eq!(projection.error_code.as_deref(), Some("STALE_LAUNCH"));
        assert_eq!(
            projection.failure_category.as_deref(),
            Some("assignment_scope_unavailable")
        );
        let any_rule = crate::automation::event_rules::EventRule {
            source: None,
            predicate: None,
            source_id: Some(source_id.clone()),
            event_kind: Some(event_kind.clone()),
            status: None,
            action: crate::automation::event_rules::EventRuleAction::ScriptRun,
        };
        assert!(any_rule.matches_safe_event(&source_id, &event_kind, projection.status,));

        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        assert_eq!(defer_stale_launch(&tx, OPERATION_ID, 200).unwrap(), None);
        tx.commit().unwrap();
        let event_count: i64 = db
            .query_row(
                "SELECT count(*) FROM observations WHERE source_stream_id='controller:native-mcp' \
                 AND kind='native.mcp.failure' AND operation_id=?1",
                [OPERATION_ID],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(event_count, 1);
    }

    #[tokio::test]
    async fn cancelled_c8_claim_finishes_stale_and_commits_one_failure_event() {
        use crate::{
            config::Config,
            model::{self, Principal, Role},
            platform::{DataRoot, bootstrap_credential},
            store::StoreOwner,
        };
        use std::sync::Arc;

        const CANCELLED_OPERATION: &str = "native-mcp-cancelled-c8-finish";
        let directory =
            std::env::temp_dir().join(format!("swarm-native-mcp-cancel-race-{}", model::new_id()));
        std::fs::create_dir_all(&directory).expect("create temporary Store directory");
        let root = DataRoot::acquire(&directory).expect("acquire temporary Store root");
        let credential = bootstrap_credential(&root.path).expect("create local Store credential");
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        let owner = StoreOwner::start(root, Arc::new(config), credential)
            .await
            .expect("start local Store owner");

        let actor = Principal {
            link_id: "fixture-cancel-link".to_owned(),
            client_id: "fixture-cancel-operator".to_owned(),
            role: Role::Operator,
        };
        let identity_digest = format!("sha256:{}", model::digest(b"cancel-race-identity"));
        let claim = ToolsClaim {
            operation_id: CANCELLED_OPERATION.to_owned(),
            claim_generation: 3,
            started_at_ms: 10_000,
            identity_digest: identity_digest.clone(),
        };
        let seed_claim = claim.clone();
        let seed_actor = actor.clone();
        owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute(
                    "INSERT INTO operations(
                         operation_id,caller_id,client_request_id,method,original_request_json,
                         effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms
                     ) VALUES(?1,?2,'fixture-c8-cancel-request','swarm.launch',
                              '{}',
                              '{\"launch_manifest\":{\"state\":\"awaiting_native_mcp\",\"runtime\":{\"dispatch_permitted\":false},\"progress\":{\"task_dispatch\":\"not_started\"}}}',
                              'queued',?3,?3,?3)",
                    rusqlite::params![
                        CANCELLED_OPERATION,
                        seed_actor.client_id,
                        seed_claim.started_at_ms
                    ],
                )?;
                let mut schedule = new_tools_schedule(
                    &seed_claim.operation_id,
                    &seed_claim.identity_digest,
                    seed_claim.started_at_ms,
                );
                schedule["state"] = json!("running");
                schedule["claim_generation"] = json!(seed_claim.claim_generation);
                schedule["started_at_ms"] = json!(seed_claim.started_at_ms);
                schedule["next_retry_at_ms"] = Value::Null;
                set_meta(&tx, &supervisor_key(&seed_claim.operation_id), &schedule)?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect("seed queued launch and committed C8 claim");

        let cancel_actor = actor.clone();
        owner
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                super::super::operations::cancel(
                    &tx,
                    &cancel_actor,
                    &json!({
                        "client_request_id":"fixture-cancel-request",
                        "operation_id":CANCELLED_OPERATION,
                        "reason":"cancelled while C8 work was in flight",
                    }),
                    "fixture-cancel-operation",
                    15_000,
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect("commit real queued-Operation cancellation");

        let finished = owner
            .store
            .finish_tools_claim(
                &claim,
                None,
                Some(json!({
                    "state":"observed_partial",
                    "install_state":"registered",
                    "runtime_status":"connected",
                    "observer_state":"not_started",
                    "dispatch_permitted":false,
                })),
                20_000,
            )
            .await
            .expect("canceled retained launch must not fail the C8 supervisor finish");
        assert_eq!(finished["state"], "stale");
        assert_eq!(finished["dispatch_permitted"], false);

        let operation_id = CANCELLED_OPERATION.to_owned();
        let schedule_key = supervisor_key(&operation_id);
        let (operation_state, schedule_state, event_count, payload_json) = owner
            .store
            .run(move |db| {
                let operation_state = db.query_row(
                    "SELECT state FROM operations WHERE operation_id=?1",
                    [&operation_id],
                    |row| row.get::<_, String>(0),
                )?;
                let schedule_state = db.query_row(
                    "SELECT json_extract(value_json,'$.state') FROM meta WHERE key=?1",
                    [schedule_key],
                    |row| row.get::<_, String>(0),
                )?;
                let (event_count, payload_json): (i64, Option<String>) = db.query_row(
                    "SELECT count(*),min(payload_json) FROM observations \
                     WHERE source_stream_id='controller:native-mcp' \
                       AND kind='native.mcp.failure' AND operation_id=?1",
                    [&operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                Ok((operation_state, schedule_state, event_count, payload_json))
            })
            .await
            .expect("read committed C8 finish state and linked event");
        assert_eq!(operation_state, "cancelled");
        assert_eq!(schedule_state, "stale");
        assert_eq!(event_count, 1);
        let payload: Value = serde_json::from_str(
            payload_json
                .as_deref()
                .expect("one safe failure observation payload"),
        )
        .expect("valid JSON payload");
        assert_eq!(payload["phase"], "native_mcp_failure");
        assert_eq!(payload["status"], "failed");
        assert_eq!(payload["error_code"], "STALE_LAUNCH");
        assert_eq!(payload["failed_supervisor"], "native_mcp_tools");
        assert_eq!(payload["failure_kind"], "tools_retry");
        assert_eq!(payload.as_object().map(serde_json::Map::len), Some(9));

        owner.close().await.expect("close local Store owner");
        std::fs::remove_dir_all(&directory).expect("remove exact fixture Store directory");
    }
}
