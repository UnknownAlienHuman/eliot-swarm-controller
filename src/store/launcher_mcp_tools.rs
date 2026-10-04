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
    error::{Error, Result},
    model::{self, Credential, Principal, Role},
    participant_credentials, platform,
    runtime::opencode_v2::{Options, Service},
    store::{launcher_native_mcp, native_mcp},
};
use rusqlite::{Connection, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

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

        match initial["install"]["state"].as_str() {
            Some("prepared") => {
                if self
                    .reserve_install_effect(&facts, &credential, &assignment)
                    .await?
                {
                    // Recheck the Store scope and the exact process identity
                    // after the durable reservation and immediately before
                    // the one allowed PUT attempt.
                    let (effect_service, _) = self
                        .connect_current(
                            &facts,
                            &credential,
                            &assignment,
                            Some((service.pid, service.version.as_str())),
                        )
                        .await?;
                    let install = tokio::time::timeout(
                        NATIVE_ACTION_TIMEOUT,
                        crate::runtime::opencode_v2::mcp_install::register(
                            &effect_service,
                            &facts.options,
                            &prepared,
                        ),
                    )
                    .await;
                    self.current_scope(&facts, &credential, Some(&assignment))
                        .await?;
                    let after_service = self
                        .verify_current_service(
                            &facts,
                            &credential,
                            &assignment,
                            (service.pid, service.version.as_str()),
                            &effect_service,
                        )
                        .await;
                    after_service?;
                    match install {
                        Ok(Ok(readback)) => {
                            let payload = readback.as_value();
                            self.record_install_ack(
                                &facts,
                                &credential,
                                &assignment,
                                &install_identity,
                                service.pid,
                                &service.version,
                                payload,
                            )
                            .await?;
                        }
                        Ok(Err(error)) => {
                            self.record_safe_error(
                                &facts,
                                &credential,
                                &assignment,
                                "install",
                                &error.code,
                            )
                            .await?;
                            return self.summary(&facts, &credential, &assignment).await;
                        }
                        Err(_) => {
                            self.record_safe_error(
                                &facts,
                                &credential,
                                &assignment,
                                "install",
                                "NATIVE_MCP_INSTALL_TIMEOUT",
                            )
                            .await?;
                            return self.summary(&facts, &credential, &assignment).await;
                        }
                    }
                }
                // The phase changed to `outcome_unknown` before any possible
                // network effect. The next call must use observe-only.
                return self.summary(&facts, &credential, &assignment).await;
            }
            Some("outcome_unknown") => {
                // A lost/uncertain PUT is never sent again. Read only the
                // uniquely derived name at its original process/location.
                let target = install_target(&initial)?;
                let (observe_service, _) = self
                    .connect_current(
                        &facts,
                        &credential,
                        &assignment,
                        Some((target.0, target.1.as_str())),
                    )
                    .await?;
                let observed = tokio::time::timeout(
                    NATIVE_ACTION_TIMEOUT,
                    crate::runtime::opencode_v2::mcp_install::observe(
                        &observe_service,
                        &facts.options,
                        &prepared,
                    ),
                )
                .await;
                self.current_scope(&facts, &credential, Some(&assignment))
                    .await?;
                self.verify_current_service(
                    &facts,
                    &credential,
                    &assignment,
                    (target.0, target.1.as_str()),
                    &observe_service,
                )
                .await?;
                match observed {
                    Ok(Ok(readback)) => {
                        self.record_install_observation(
                            &facts,
                            &credential,
                            &assignment,
                            readback.map(|value| value.as_value()),
                        )
                        .await?;
                    }
                    Ok(Err(error)) => {
                        self.record_safe_error(
                            &facts,
                            &credential,
                            &assignment,
                            "install_readback",
                            &error.code,
                        )
                        .await?;
                    }
                    Err(_) => {
                        self.record_safe_error(
                            &facts,
                            &credential,
                            &assignment,
                            "install_readback",
                            "NATIVE_MCP_READBACK_TIMEOUT",
                        )
                        .await?;
                    }
                }
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
            let observed = tokio::time::timeout(
                NATIVE_ACTION_TIMEOUT,
                crate::runtime::opencode_v2::mcp_install::observe(
                    &service,
                    &facts.options,
                    &prepared,
                ),
            )
            .await;
            self.current_scope(&facts, &credential, Some(&assignment))
                .await?;
            self.verify_current_service(
                &facts,
                &credential,
                &assignment,
                (registered_target.0, registered_target.1.as_str()),
                &service,
            )
            .await?;
            match observed {
                Ok(Ok(Some(readback))) => {
                    self.record_install_refresh(
                        &facts,
                        &credential,
                        &assignment,
                        &prepared.identity(),
                        registered_target.0,
                        &registered_target.1,
                        readback.as_value(),
                    )
                    .await?;
                }
                Ok(Ok(None)) => {}
                Ok(Err(error)) => {
                    self.record_safe_error(
                        &facts,
                        &credential,
                        &assignment,
                        "install_readback",
                        &error.code,
                    )
                    .await?;
                }
                Err(_) => {
                    self.record_safe_error(
                        &facts,
                        &credential,
                        &assignment,
                        "install_readback",
                        "NATIVE_MCP_READBACK_TIMEOUT",
                    )
                    .await?;
                }
            }
            return self.summary(&facts, &credential, &assignment).await;
        }

        let record = self.load_record(&facts, &credential, &assignment).await?;
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
                let preflight = tokio::time::timeout(
                    NATIVE_ACTION_TIMEOUT,
                    crate::runtime::opencode_v2::mcp_tools::preflight_arm(
                        &service,
                        &facts.options,
                        &challenge,
                    ),
                )
                .await;
                let prepared_arm = match preflight {
                    Ok(Ok(prepared_arm)) => prepared_arm,
                    Ok(Err(error)) if challenge_replacement_reason(&error.code).is_some() => {
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
                    Ok(Err(error)) => {
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
                    Err(_) => {
                        self.record_safe_error(
                            &facts,
                            &credential,
                            &assignment,
                            "challenge_preflight",
                            "NATIVE_MCP_PROOF_PREFLIGHT_TIMEOUT",
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
                if self
                    .reserve_challenge_effect(&facts, &credential, &assignment)
                    .await?
                {
                    let armed = tokio::time::timeout(
                        NATIVE_ACTION_TIMEOUT,
                        crate::runtime::opencode_v2::mcp_tools::arm_prepared(
                            &service,
                            &facts.options,
                            prepared_arm,
                        ),
                    )
                    .await;
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
                    match armed {
                        Ok(Ok(())) => {
                            self.record_challenge_armed(&facts, &credential, &assignment)
                                .await?;
                        }
                        Ok(Err(error)) => {
                            self.record_safe_error(
                                &facts,
                                &credential,
                                &assignment,
                                "challenge",
                                &error.code,
                            )
                            .await?;
                        }
                        Err(_) => {
                            self.record_safe_error(
                                &facts,
                                &credential,
                                &assignment,
                                "challenge",
                                "NATIVE_MCP_PROOF_TIMEOUT",
                            )
                            .await?;
                        }
                    }
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
                let readback = tokio::time::timeout(
                    NATIVE_ACTION_TIMEOUT,
                    crate::runtime::opencode_v2::mcp_tools::read(
                        &read_service,
                        &facts.options,
                        &challenge,
                    ),
                )
                .await;
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
                match readback {
                    Ok(Ok(readback)) => {
                        if readback.scope() != &assignment {
                            return Err(scope_error(
                                "native tools readback does not match the current assignment",
                            ));
                        }
                        let payload = readback.as_value();
                        if model::canonical(&payload)?.len() > MAX_PRIVATE_READBACK_BYTES {
                            return Err(Error::new(
                                "NATIVE_MCP_PROOF_SCHEMA",
                                "native tools readback exceeds the private Store bound",
                            ));
                        }
                        self.record_tools_readback(&facts, &credential, &assignment, payload)
                            .await?;
                    }
                    Ok(Err(error)) => {
                        self.record_safe_error(
                            &facts,
                            &credential,
                            &assignment,
                            "tools_readback",
                            &error.code,
                        )
                        .await?;
                    }
                    Err(_) => {
                        self.record_safe_error(
                            &facts,
                            &credential,
                            &assignment,
                            "tools_readback",
                            "NATIVE_MCP_PROOF_TIMEOUT",
                        )
                        .await?;
                    }
                }
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
                            },
                            "challenge":{
                                "state":"not_started",
                                "metadata":null,
                                "replaced_metadata":[],
                                "replacement_archive":{"count":0,"digest":null},
                                "effect_reserved_at_ms":null,
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
    ) -> Result<bool> {
        self.transition(facts, credential, assignment, |record, now| {
            if record["install"]["state"] == "prepared" {
                record["install"]["state"] = json!("outcome_unknown");
                record["install"]["effect_reserved_at_ms"] = json!(now);
                record["last_error"] = Value::Null;
                Ok(true)
            } else {
                Ok(false)
            }
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
    ) -> Result<bool> {
        self.transition(facts, credential, assignment, |record, now| {
            if record["challenge"]["state"] == "prepared"
                && record["challenge"]["effect_reserved_at_ms"].is_null()
            {
                record["challenge"]["state"] = json!("outcome_unknown");
                record["challenge"]["effect_reserved_at_ms"] = json!(now);
                record["last_error"] = Value::Null;
                Ok(true)
            } else if record["challenge"]["state"] == "prepared" {
                Err(record_error(
                    "prepared challenge already has an effect reservation",
                ))
            } else {
                Ok(false)
            }
        })
        .await
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
        let stage = safe_label(stage)?;
        let code = safe_label(code)?;
        self.transition(facts, credential, assignment, move |record, now| {
            record["last_error"] = json!({"stage":stage,"code":code,"recorded_at_ms":now});
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
        participant_id: snapshot.participant_id().to_owned(),
        credential_ref: snapshot.credential_ref().to_owned(),
        profile_config_ref: snapshot.profile_config_ref().to_owned(),
        options: snapshot.options().clone(),
        owned_service: snapshot.owned_service_expectation(),
        config: Arc::new(config.clone()),
    })
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
        let facts = match load_launch_facts(tx, config, &operation_id) {
            Ok(facts) => facts,
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

fn public_summary(record: &Value) -> Value {
    let tools = &record["tools_readback"];
    let has_readback = tools.is_object();
    json!({
        "operation_id":record["operation_id"],
        "state":if has_readback {"observed_partial"} else {"pending"},
        "install_state":record["install"]["state"],
        "runtime_status":record["install"]["readback"]["runtime_status"],
        "observer_state":record["challenge"]["state"],
        "native_discovered":if has_readback {tools["native_discovered"]["status"].clone()} else {Value::String("unknown".to_owned())},
        "session_context":if has_readback {tools["session_context"]["status"].clone()} else {Value::String("unknown".to_owned())},
        "provider_request":if has_readback {tools["provider_request"]["status"].clone()} else {Value::String("unknown".to_owned())},
        "model_consumed":"unknown",
        "last_error":record["last_error"],
        "dispatch_permitted":false,
    })
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
