//! Owner-managed Eliot-mode Goals and their explicit rounds.
//!
//! A round probes the Goal and its Agent, runs the frozen check command
//! through the shared script/process custody outside the Store lock, then
//! commits one outcome only if generation, revision and round still match.
//! Unknown check outcomes retain no Goal progress or continuation and never
//! authorize another check; the reservation retains their error.
use super::{
    Principal, Router,
    coordination::decode_coordination_request,
    goal_scope::{
        ScopeFinding, changed_check_inputs, finding, outside_scope, scope_reminder, with_finding,
    },
    require_owner,
};
use crate::{
    common::now_ms,
    git_monitor::GitProbe,
    scripts::process::{ProcessCapture, ProcessFailure, ProcessOutcome, run_owned_process},
};
use eliot_core::{
    AgentState, Error, Event, Result, SecretPolicy, StderrSnapshot,
    api::{
        GoalClearRequest, GoalContinuationStage, GoalDefinition, GoalGetRequest, GoalMode,
        GoalPauseRequest, GoalResumeRequest, GoalRoundRequest, GoalSetRequest, GoalStatus,
        MAX_GOAL_OBSERVATION_BYTES, inference::sha256_hex, truncate_bytes,
    },
    limits::MAX_FRAME_BYTES,
};
use eliot_plugin::sync::LockExt;
use eliot_store::{
    GoalCheckResult, GoalContinuation, GoalRoundClaim, GoalRoundCommit, GoalRoundKey,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    process::Command as TokioCommand,
    sync::{broadcast, watch},
    task::JoinSet,
};

/// Spawn one already-resolved check command. Production passes
/// [`real_goal_check`]; tests inject a fake outcome. The future is boxed so
/// the seam stays a plain function pointer.
pub(crate) type GoalCheckRunner = fn(
    TokioCommand,
    Vec<u8>,
    Duration,
    Arc<SecretPolicy>,
    watch::Receiver<bool>,
) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>>;

/// Own the check future as well as its reservation. Dropping the RPC requests
/// cancellation, but keeps the same future alive until its process supervisor
/// settles custody; only then may the finalizer release single-flight.
struct ClaimedGoalRound {
    router: Router,
    runtime: tokio::runtime::Handle,
    principal: String,
    request: GoalRoundRequest,
    error: Error,
    armed: bool,
    cancellation: watch::Sender<bool>,
    check: Option<Pin<Box<dyn Future<Output = ProcessOutcome> + Send>>>,
    quiescent: bool,
}

impl ClaimedGoalRound {
    fn complete(&mut self, result: Result<Value>) -> Result<Value> {
        match &result {
            Ok(_) => self.armed = false,
            Err(error) => self.error = error.clone(),
        }
        result
    }
}

impl Drop for ClaimedGoalRound {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let router = self.router.clone();
        let principal = self.principal.clone();
        let request = self.request.clone();
        let mut error = self.error.clone();
        let mut quiescent = self.quiescent;
        self.cancellation.send_replace(true);
        let check = self.check.take();
        self.runtime.spawn(async move {
            if let Some(check) = check {
                // Await this exact invocation, never reconstruct or replay it.
                // run_owned_process retains and drains its existing supervisor.
                let outcome = check.await;
                quiescent = goal_check_quiescent(&outcome);
                if !quiescent {
                    let policy = router.secret_policy_snapshot();
                    error = match goal_check_result(outcome, &policy) {
                        Err(error) => error,
                        Ok(_) => Error::new("unknown", "goal check cleanup is unproven"),
                    };
                    crate::logging::record(
                        &policy,
                        "daemon.goal_check_cleanup_unproven",
                        &[("agent", &request.agent)],
                        None,
                    );
                }
            }
            // A previously retained terminal reply/error is never overwritten.
            // A persistence failure faults the Store; restart resolves any
            // remaining reservation under the old holder to unknown.
            let _ = router
                .settle_goal_round_error(&principal, &request, &error, quiescent)
                .await;
        });
    }
}

pub(crate) fn real_goal_check(
    command: TokioCommand,
    stdin: Vec<u8>,
    timeout: Duration,
    policy: Arc<SecretPolicy>,
    cancellation: watch::Receiver<bool>,
) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
    Box::pin(run_owned_process(
        command,
        stdin,
        timeout,
        policy,
        cancellation,
    ))
}

fn decode_goal_request<T>(
    params: &Value,
    principal: &Principal,
    secrets: &crate::plugin_host::SecretSet,
    method: &str,
    validate: fn(&T) -> Result<()>,
) -> Result<T>
where
    T: DeserializeOwned + schemars::JsonSchema,
{
    require_owner(principal)?;
    decode_coordination_request(params, secrets, method, validate)
}

fn require_eliot_mode(mode: GoalMode) -> Result<()> {
    if mode != GoalMode::Eliot {
        return Err(Error::new(
            "unsupported",
            "native Goal mode is not supported by the installed harness plugins",
        ));
    }
    Ok(())
}

fn failed_check_message(reason: ProcessFailure) -> &'static str {
    match reason {
        ProcessFailure::InputLimit => "goal check input exceeds the frame bound",
        ProcessFailure::InvalidTimeout => "goal check timeout is invalid",
        ProcessFailure::Exit => "goal check completion is unproven",
        ProcessFailure::Timeout => "goal check timed out",
        ProcessFailure::Cancelled => "goal check was cancelled",
        ProcessFailure::OutputLimit => "goal check output exceeds its bound",
    }
}

impl Router {
    /// Wake the goal driver to sweep retained idle subjects. Called by
    /// `goal.set`/`goal.resume` once the Goal is active and idle-keyed,
    /// so an already-idle Agent rounds without a new bus event. Reads,
    /// pauses and clears never call it; the Store single-flight marker
    /// keeps every sweep to at most one round per subject.
    pub(crate) fn notify_goal_reconcile(&self) {
        let current = *self.inner.goal_reconcile.borrow();
        self.inner
            .goal_reconcile
            .send_replace(current.wrapping_add(1));
    }

    pub(crate) fn subscribe_goal_reconcile(&self) -> watch::Receiver<u64> {
        self.inner.goal_reconcile.subscribe()
    }

    pub(super) async fn goal_set(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: GoalSetRequest = decode_goal_request(
            params,
            principal,
            &self.inner.secrets,
            "goal.set",
            GoalSetRequest::validate,
        )?;
        require_eliot_mode(request.mode)?;
        let principal = super::principal_name(principal);
        // A retry is answered from the retained result before the workspace
        // is read again for a starting point.
        let replay = if request.definition.tracks_changes() {
            self.inner.store.lock().await.goal_set_replay(
                &request.agent,
                request.mode.as_str(),
                &request.id,
                &principal,
                &request.definition,
            )?
        } else {
            None
        };
        let view = match replay {
            Some(view) => view,
            None => {
                let scope_base = self
                    .goal_scope_base(&request.agent, &request.definition)
                    .await?;
                self.inner.store.lock().await.goal_set(
                    &request.agent,
                    request.mode.as_str(),
                    &request.id,
                    &principal,
                    &request.definition,
                    scope_base,
                    now_ms(),
                )?
            }
        };
        if view.definition.cadence_ms.is_none() {
            self.notify_goal_reconcile();
        }
        request.validate_response(&view)?;
        serde_json::to_value(view)
            .map_err(|_| Error::new("internal", "goal state could not be encoded"))
    }

    pub(super) async fn goal_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: GoalGetRequest = decode_goal_request(
            params,
            principal,
            &self.inner.secrets,
            "goal.get",
            GoalGetRequest::validate,
        )?;
        require_eliot_mode(request.mode)?;
        let view = self
            .inner
            .store
            .lock()
            .await
            .goal_get(&request.agent, request.mode.as_str())?
            .ok_or_else(|| Error::new("not_found", "goal was not found"))?;
        request.validate_response(&view)?;
        serde_json::to_value(view)
            .map_err(|_| Error::new("internal", "goal state could not be encoded"))
    }

    pub(super) async fn goal_pause(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: GoalPauseRequest = decode_goal_request(
            params,
            principal,
            &self.inner.secrets,
            "goal.pause",
            GoalPauseRequest::validate,
        )?;
        require_eliot_mode(request.mode)?;
        let view = self.inner.store.lock().await.goal_pause(
            &request.agent,
            request.mode.as_str(),
            &request.id,
            &super::principal_name(principal),
            request.reason.as_deref(),
            now_ms(),
        )?;
        request.validate_response(&view)?;
        serde_json::to_value(view)
            .map_err(|_| Error::new("internal", "goal state could not be encoded"))
    }

    pub(super) async fn goal_clear(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: GoalClearRequest = decode_goal_request(
            params,
            principal,
            &self.inner.secrets,
            "goal.clear",
            GoalClearRequest::validate,
        )?;
        require_eliot_mode(request.mode)?;
        let view = self.inner.store.lock().await.goal_clear(
            &request.agent,
            request.mode.as_str(),
            &request.id,
            &super::principal_name(principal),
            now_ms(),
        )?;
        request.validate_response(&view)?;
        serde_json::to_value(view)
            .map_err(|_| Error::new("internal", "goal state could not be encoded"))
    }

    pub(super) async fn goal_resume(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: GoalResumeRequest = decode_goal_request(
            params,
            principal,
            &self.inner.secrets,
            "goal.resume",
            GoalResumeRequest::validate,
        )?;
        require_eliot_mode(request.mode)?;
        let view = self.inner.store.lock().await.goal_resume(
            &request.agent,
            request.mode.as_str(),
            &request.id,
            &super::principal_name(principal),
            now_ms(),
        )?;
        if view.definition.cadence_ms.is_none() {
            self.notify_goal_reconcile();
        }
        request.validate_response(&view)?;
        serde_json::to_value(view)
            .map_err(|_| Error::new("internal", "goal state could not be encoded"))
    }

    pub(super) async fn goal_round(&self, params: &Value, principal: &Principal) -> Result<Value> {
        self.goal_round_with(params, principal, real_goal_check)
            .await
    }

    async fn goal_round_with(
        &self,
        params: &Value,
        principal: &Principal,
        run_check: GoalCheckRunner,
    ) -> Result<Value> {
        let cancellation = self.run_cancellation_receiver();
        self.goal_round_with_cancellation(params, principal, run_check, cancellation)
            .await
    }

    async fn goal_round_with_cancellation(
        &self,
        params: &Value,
        principal: &Principal,
        run_check: GoalCheckRunner,
        mut cancellation: watch::Receiver<bool>,
    ) -> Result<Value> {
        let request: GoalRoundRequest = decode_goal_request(
            params,
            principal,
            &self.inner.secrets,
            "goal.round",
            GoalRoundRequest::validate,
        )?;
        require_eliot_mode(request.mode)?;
        ulid::Ulid::from_string(&request.id).map_err(|_| Error::invalid_mutation_id())?;
        let principal = super::principal_name(principal);
        let mut store = self.inner.store.lock().await;
        if let Some(response) = store.goal_round_replay(&principal, &request, now_ms())? {
            return serde_json::to_value(response)
                .map_err(|_| Error::new("internal", "goal.round result could not be encoded"));
        }
        let key = goal_round_key(&request)?;
        let probe =
            { store.goal_round_probe(&request.agent, request.mode.as_str(), key, now_ms())? };
        goal_round_gates(&probe, key)?;
        let definition = probe.view.definition.clone();
        let stdin = serde_json::to_vec(&json!({
            "agent": request.agent,
            "generation": probe.view.generation,
            "revision": probe.view.revision,
            "round_key": key.text(),
        }))
        .map_err(|_| Error::new("internal", "goal check input could not be encoded"))?;
        if stdin.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "goal check input exceeds 1 MiB",
            ));
        }
        // The claim re-validates the probe and commits the durable in-flight
        // marker. Everything after it resolves the marker exactly once: the
        // commit consumes it, every other exit abandons it to `unknown`.
        let claim = {
            store.goal_claim_round(
                &principal,
                &request,
                probe.view.revision,
                probe.spent_by_goal_continuation,
                now_ms(),
            )?
        };
        drop(store);
        let (claim_seq, continuation_id) = match claim {
            GoalRoundClaim::Claimed {
                seq,
                continuation_id,
            } => (seq, continuation_id),
            GoalRoundClaim::Replay(response) => {
                return serde_json::to_value(response)
                    .map_err(|_| Error::new("internal", "goal.round result could not be encoded"));
            }
        };
        let (check_cancellation, check_cancelled) = watch::channel(*cancellation.borrow());
        let mut claimed = ClaimedGoalRound {
            router: self.clone(),
            runtime: tokio::runtime::Handle::current(),
            principal: principal.clone(),
            request: request.clone(),
            error: Error::new("unknown", "goal round task ended without a retained result"),
            armed: true,
            cancellation: check_cancellation,
            check: None,
            quiescent: true,
        };
        let turns_used = probe
            .turn_count
            .saturating_sub(probe.view.counters.turns_at_set);
        // A turn budget spent outside any round blocks without running the
        // check. A budget spent solely by this Goal's own last admitted
        // continuation still gets its final check: success completes even
        // on a spent budget, and a failure blocks at the postcheck below,
        // admitting no further continuation. Round and no-progress budgets
        // are decided after the check.
        if probe
            .view
            .definition
            .max_turns
            .is_some_and(|max| turns_used >= max)
            && !probe.spent_by_goal_continuation
        {
            let result = self
                .commit_goal_block(
                    &principal,
                    &request,
                    &probe,
                    key,
                    claim_seq,
                    "turn_limit",
                    None,
                )
                .await;
            return claimed.complete(result);
        }
        // The change scope is read while the Agent is idle, before the
        // check runs. A confirmed change outside `scope` blocks the round:
        // no check and no repair continuation.
        let scope = if definition.tracks_changes() {
            Some(match &probe.scope_base {
                Some(base) => (
                    Some(base),
                    self.goal_scope_changes(probe.workspace.as_deref(), base)
                        .await,
                ),
                None => (
                    None,
                    ScopeFinding::Unverified("no starting point was recorded".to_owned()),
                ),
            })
        } else {
            None
        };
        if let Some((_, ScopeFinding::Verified(changed))) = &scope {
            let outside = outside_scope(&definition, changed);
            if !outside.is_empty() {
                let observation = finding("Changed outside the Goal's scope", &outside);
                let result = self
                    .commit_goal_block(
                        &principal,
                        &request,
                        &probe,
                        key,
                        claim_seq,
                        "scope_violation",
                        Some(observation),
                    )
                    .await;
                return claimed.complete(result);
            }
        }
        // The no-progress fingerprint is read now, while the Agent is idle
        // and before the check can write anything; it is compared with the
        // read taken right after the previous failed check.
        let fingerprint_before = work_fingerprint(probe.workspace.as_deref()).await;
        let policy = self.secret_policy_snapshot();
        let mut command = TokioCommand::new(&definition.check[0]);
        command.args(&definition.check[1..]);
        if let Some(cwd) = definition
            .check_cwd
            .as_deref()
            .or(probe.workspace.as_deref())
        {
            command.current_dir(cwd);
        }
        let timeout = Duration::from_millis(definition.check_timeout_ms.max(0) as u64);
        claimed.quiescent = false;
        claimed.check = Some(run_check(
            command,
            stdin,
            timeout,
            Arc::clone(&policy),
            check_cancelled,
        ));
        let outcome = {
            let check = claimed
                .check
                .as_mut()
                .expect("claimed check was just installed");
            tokio::select! {
                biased;
                _ = async { let _ = cancellation.wait_for(|cancelled| *cancelled).await; } => {
                    claimed.cancellation.send_replace(true);
                    check.await
                }
                outcome = check.as_mut() => outcome,
            }
        };
        // A completed future is never polled again. Carry its independent
        // custody fact through error retention and cancellation of that await.
        claimed.quiescent = goal_check_quiescent(&outcome);
        claimed.check = None;
        let check = match goal_check_result(outcome, &policy) {
            Ok(check) => check,
            Err(error) => {
                claimed.error = error.clone();
                let retained = self
                    .settle_goal_round_error(&principal, &request, &error, claimed.quiescent)
                    .await;
                return claimed.complete(Err(retained));
            }
        };
        if check.exit_code != Some(0) {
            // The next round's baseline includes whatever this check wrote.
            let fingerprint_after = work_fingerprint(probe.workspace.as_deref()).await;
            let (_, stage) = probe.view.counters.failure_stage(
                &definition,
                check.output_complete,
                check.output_digest.as_deref(),
            );
            let text = goal_continuation_text(&definition, &check, stage, &policy);
            let escalation_id =
                (stage == GoalContinuationStage::Escalate).then(|| ulid::Ulid::new().to_string());
            let commit = GoalRoundCommit {
                id: request.id.clone(),
                principal: principal.clone(),
                agent: request.agent.clone(),
                mode: request.mode.as_str().to_owned(),
                expected_generation: probe.view.generation,
                expected_revision: probe.view.revision,
                round_key: key,
                claim_seq,
                check: Some(check),
                blocked_reason: None,
                observation: None,
                fingerprint_before,
                fingerprint_after,
                continuation: Some(GoalContinuation {
                    id: continuation_id,
                    text,
                    stage,
                    escalation_id,
                }),
            };
            let result = self.commit_goal_round(&principal, &request, commit).await;
            return claimed.complete(result);
        }
        // A pass is measured again, so files the check wrote count too. It
        // completes only with a verified change set inside `scope` that
        // leaves the check's own inputs untouched; otherwise it blocks.
        let scope = match scope {
            Some((Some(base), _)) => Some((
                Some(base),
                self.goal_scope_changes(probe.workspace.as_deref(), base)
                    .await,
            )),
            other => other,
        };
        let mut check = check;
        let block = match &scope {
            Some((_, ScopeFinding::Unverified(reason))) => Some((
                "scope_unverified",
                truncate_bytes(
                    &format!("The Goal's change set could not be verified: {reason}"),
                    1024,
                )
                .0,
            )),
            Some((Some(base), ScopeFinding::Verified(changed))) => {
                let outside = outside_scope(&definition, changed);
                let inputs = changed_check_inputs(&definition, base, changed);
                if !outside.is_empty() {
                    Some((
                        "scope_violation",
                        finding("Changed outside the Goal's scope", &outside),
                    ))
                } else if !inputs.is_empty() {
                    Some((
                        "checker_changed",
                        finding("Check inputs changed since the Goal started", &inputs),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        };
        let blocked_reason = block.map(|(reason, text)| {
            check.observation = Some(with_finding(
                &text,
                check.observation.as_deref(),
                MAX_GOAL_OBSERVATION_BYTES,
            ));
            reason.to_owned()
        });
        let commit = GoalRoundCommit {
            id: request.id.clone(),
            principal: principal.clone(),
            agent: request.agent.clone(),
            mode: request.mode.as_str().to_owned(),
            expected_generation: probe.view.generation,
            expected_revision: probe.view.revision,
            round_key: key,
            claim_seq,
            check: Some(check),
            blocked_reason,
            observation: None,
            fingerprint_before,
            fingerprint_after: None,
            continuation: None,
        };
        let result = self.commit_goal_round(&principal, &request, commit).await;
        claimed.complete(result)
    }

    #[allow(clippy::too_many_arguments)]
    async fn commit_goal_block(
        &self,
        principal: &str,
        request: &GoalRoundRequest,
        probe: &eliot_store::GoalRoundProbe,
        key: GoalRoundKey,
        claim_seq: i64,
        reason: &str,
        observation: Option<String>,
    ) -> Result<Value> {
        let commit = GoalRoundCommit {
            id: request.id.clone(),
            principal: principal.to_owned(),
            agent: request.agent.clone(),
            mode: request.mode.as_str().to_owned(),
            expected_generation: probe.view.generation,
            expected_revision: probe.view.revision,
            round_key: key,
            claim_seq,
            check: None,
            blocked_reason: Some(reason.to_owned()),
            observation,
            fingerprint_before: None,
            fingerprint_after: None,
            continuation: None,
        };
        self.commit_goal_round(principal, request, commit).await
    }

    /// Retain the error; only proven custody closure may release the marker.
    /// If persistence cannot prove that commit, return unknown and log it.
    async fn settle_goal_round_error(
        &self,
        principal: &str,
        request: &GoalRoundRequest,
        error: &Error,
        quiescent: bool,
    ) -> Error {
        let mut store = self.inner.store.lock().await;
        let retained = if quiescent {
            store.goal_abandon_round(principal, request, error, now_ms())
        } else {
            store.goal_hold_round_error(principal, request, error, now_ms())
        };
        match retained {
            Ok(retained) => retained,
            Err(error) => {
                let policy = self.secret_policy_snapshot();
                crate::logging::record(
                    &policy,
                    "daemon.goal_abandon_failed",
                    &[("agent", &request.agent)],
                    Some(&error),
                );
                Error::new("unknown", "goal round result could not be retained")
            }
        }
    }

    async fn commit_goal_round(
        &self,
        principal: &str,
        request: &GoalRoundRequest,
        commit: GoalRoundCommit,
    ) -> Result<Value> {
        let result = {
            let mut store = self.inner.store.lock().await;
            let result = match store.goal_commit_round(&commit, now_ms()) {
                Ok(result) => result,
                Err(error) => {
                    match store.goal_abandon_round(principal, request, &error, now_ms()) {
                        Ok(retained) => return Err(retained),
                        Err(abandon) => {
                            let policy = self.secret_policy_snapshot();
                            crate::logging::record(
                                &policy,
                                "daemon.goal_abandon_failed",
                                &[("agent", &commit.agent)],
                                Some(&abandon),
                            );
                            return Err(Error::new(
                                "unknown",
                                "goal round result could not be retained",
                            ));
                        }
                    }
                }
            };
            if let Some(escalation) = &result.escalation {
                self.publish_locked(&escalation.events);
            }
            if let Some(admission) = &result.admission {
                self.publish_locked(&admission.events);
            }
            self.publish_locked(&result.events);
            result
        };
        // The escalation configure was admitted first and is scheduled
        // first; the settings lane keeps the continuation behind it.
        if let Some(escalation) = &result.escalation
            && escalation.is_new
        {
            self.schedule_command(escalation.command.id.clone(), None);
        }
        if let Some(admission) = &result.admission {
            if admission.command.state == eliot_core::CommandState::Unknown {
                self.inner
                    .unknown_commands
                    .lock_recover()
                    .insert(admission.command.id.clone());
            }
            if admission.is_new {
                self.schedule_command(
                    admission.command.id.clone(),
                    admission
                        .linked_command
                        .as_ref()
                        .map(|linked| linked.id.clone()),
                );
            }
        }
        self.signal_progress();
        let response = result.response;
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "goal.round result could not be encoded"))
    }
}

/// Exactly one key form per round; request validation already enforces
/// this, so the fallback only defends direct callers.
fn goal_round_key(request: &GoalRoundRequest) -> Result<GoalRoundKey> {
    match (request.idle_seq, request.slot) {
        (Some(idle_seq), None) => Ok(GoalRoundKey::Idle(idle_seq)),
        (None, Some(slot)) => Ok(GoalRoundKey::Slot(slot)),
        _ => Err(Error::new(
            "invalid",
            "goal round needs exactly one of idle_seq, slot",
        )),
    }
}

/// Refuse a round unless the Agent is cleanly idle at the round key. Idle
/// keys pin the latest event; slot keys pin time and enforce freshness at
/// the claim and the commit instead.
fn goal_round_gates(probe: &eliot_store::GoalRoundProbe, key: GoalRoundKey) -> Result<()> {
    if probe.agent_state != AgentState::Idle {
        return Err(Error::new("bad_state", "goal round needs an idle Agent"));
    }
    if probe.active_turn.is_some() {
        return Err(Error::new("bad_state", "goal round needs no active turn"));
    }
    if probe.pending_count > 0 {
        return Err(Error::new(
            "bad_state",
            "goal round needs no pending requests",
        ));
    }
    if let Some(source) = probe.unhealthy_sources.first() {
        return Err(Error::new(
            "bad_state",
            format!("goal round needs healthy harness sources; {source} is not healthy"),
        ));
    }
    if probe.unknown_commands > 0 {
        return Err(Error::new(
            "bad_state",
            "resolve unknown commands before the goal round",
        ));
    }
    if probe.open_commands > 0 {
        return Err(Error::new("busy", "Agent has open commands"));
    }
    if let GoalRoundKey::Idle(idle_seq) = key
        && probe.max_seq != idle_seq
    {
        return Err(Error::new(
            "stale_round",
            "round key is not the latest Agent event",
        ));
    }
    Ok(())
}

/// Most automatic Goal rounds in flight at once. One slow check must
/// not stall other Goals' rounds; past the cap extra idle events are
/// dropped (logged) until a round finishes.
const MAX_CONCURRENT_AUTO_ROUNDS: usize = 4;

/// One automatic round in flight: its check cancellation plus the key
/// it rounds on. A newer event for the same Goal dooms that key, so the
/// driver remembers it for a follow-up instead of dropping it.
struct InFlightAutoRound {
    cancel: watch::Sender<bool>,
    seq: i64,
    task: tokio::task::Id,
}

/// A retained idle subject waiting for its Goal's live round to resolve
/// or for a free concurrency slot. Bound to the Goal revision seen at
/// sweep time: a newer revision overwrites an older wait, never the
/// reverse, so a re-armed subject is never dropped as a duplicate.
#[derive(Clone, Debug)]
struct PendingReconcile {
    generation: i64,
    revision: i64,
    seq: i64,
}

/// Drive active Eliot-mode Goals when their generation commits an idle
/// event. Each round runs in its own task — at most one per Goal — through
/// the same `goal_round` path as an explicit owner call: same probe, gates,
/// check custody and conditional commit, with a fresh owner mutation id.
/// A newer event for a Goal with a round in flight is remembered and
/// driven as a follow-up when that round resolves, so the fresh key is
/// never silently dropped. A `goal.set`/`goal.resume` notification sweeps
/// the retained idle subjects through the same reservation without
/// re-offering mail; a retained subject that finds its Goal busy or the
/// cap full waits in revision-bound order and is revalidated when a round
/// resolves. Bus lag, bus-event cap drops, queued waits and stale rounds
/// are logged; other lost races stay silent. Shutdown cancels every
/// in-flight check through its custody cancellation and waits only for the
/// rounds to resolve their markers (`unknown`, never rerun), never for a
/// check to finish on its own.
pub(crate) async fn run_goal_driver(
    router: Router,
    run_check: GoalCheckRunner,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    router.note_worker_task_running("goal_driver");
    let mut events = router.subscribe_rule_events();
    let mut goal_changes = router.subscribe_goal_reconcile();
    let mut rounds: JoinSet<(String, i64)> = JoinSet::new();
    let mut in_flight: HashMap<String, InFlightAutoRound> = HashMap::new();
    // Newest event per Goal with a round in flight, driven as a follow-up
    // when that round resolves. Never filled past shutdown: the drain
    // below starts no new rounds.
    let mut pending: HashMap<String, Event> = HashMap::new();
    // Retained subjects waiting for their Goal's live round to resolve or
    // for a free cap slot, in Agent order. Revalidated and drained when a
    // round resolves; never filled past shutdown.
    let mut pending_reconcile: BTreeMap<String, PendingReconcile> = BTreeMap::new();
    // A worker restart may have missed the idle event that should drive a
    // Goal. Reconcile from durable idle keys; consumed or unknown-burned keys
    // stay fenced by Store and are never run twice.
    reconcile_idle_goals(
        &router,
        run_check,
        &mut rounds,
        &mut in_flight,
        &mut pending,
        &mut pending_reconcile,
    )
    .await;
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    for running in in_flight.values() {
                        running.cancel.send_replace(true);
                    }
                    while rounds.join_next().await.is_some() {}
                    return Ok(());
                }
            }
            received = events.recv() => match received {
                Ok(event) => {
                    spawn_auto_round(
                        &router,
                        run_check,
                        &mut rounds,
                        &mut in_flight,
                        &mut pending,
                        &mut pending_reconcile,
                        event,
                    ).await;
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let policy = router.secret_policy_snapshot();
                    crate::logging::record(
                        &policy,
                        "daemon.goal_auto_round_lagged",
                        &[("skipped", &skipped.to_string())],
                        None,
                    );
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return Err(Error::new("unavailable", "event bus is closed"));
                }
            },
            changed = goal_changes.changed() => {
                if changed.is_err() {
                    return Ok(());
                }
                reconcile_idle_goals(
                    &router,
                    run_check,
                    &mut rounds,
                    &mut in_flight,
                    &mut pending,
                    &mut pending_reconcile,
                )
                .await;
            }
            Some(joined) = rounds.join_next(), if !rounds.is_empty() => {
                let agent = match joined {
                    Ok((agent, _)) => Some(agent),
                    Err(error) => {
                        // A panicked round must still free its Goal and cap
                        // slot; its durable marker keeps the Goal fenced.
                        let policy = router.secret_policy_snapshot();
                        crate::logging::record(
                            &policy,
                            "daemon.goal_auto_round_task_failed",
                            &[],
                            None,
                        );
                        in_flight
                            .iter()
                            .find(|(_, running)| running.task == error.id())
                            .map(|(agent, _)| agent.clone())
                    }
                };
                if let Some(agent) = agent {
                    in_flight.remove(&agent);
                    if let Some(follow) = pending.remove(&agent) {
                        spawn_auto_round(
                            &router,
                            run_check,
                            &mut rounds,
                            &mut in_flight,
                            &mut pending,
                            &mut pending_reconcile,
                            follow,
                        ).await;
                    }
                    drain_pending_reconcile(
                        &router,
                        run_check,
                        &mut rounds,
                        &mut in_flight,
                        &mut pending,
                        &mut pending_reconcile,
                        &agent,
                    )
                    .await;
                }
            }
        }
    }
}

/// What an automatic round runs on: a fresh bus event, or the retained
/// exact-generation idle subject a `goal.set`/`goal.resume` reconciles.
/// Bus events can wait as follow-ups; a retained subject that loses the
/// race waits in revision-bound order instead of going silent.
enum IdleSubject {
    Event(Box<Event>),
    Retained {
        agent: String,
        generation: i64,
        revision: i64,
        seq: i64,
    },
}

/// Start one automatic round for an idle event unless its Goal already
/// has one in flight — then a newer event is remembered for a follow-up —
/// or the global cap is reached (the event is dropped and logged).
async fn spawn_auto_round(
    router: &Router,
    run_check: GoalCheckRunner,
    rounds: &mut JoinSet<(String, i64)>,
    in_flight: &mut HashMap<String, InFlightAutoRound>,
    pending: &mut HashMap<String, Event>,
    pending_reconcile: &mut BTreeMap<String, PendingReconcile>,
    event: Event,
) {
    // Advisory attention binds its subject snapshot for explicit `on.event`
    // rules, but must never drive automatic behavior: neither a Goal round
    // nor a deferred mail reoffer. An explicit rule can still target the
    // incident; the automatic idle/Goal/mail nudges stay silent here.
    if event.kind == "x.user.agent.attention" {
        return;
    }
    let Some(agent) = event.agent.clone() else {
        return;
    };
    // Deferred mail wakes re-offer on every idle event through the shared
    // Store claim, independent of Goal rounds, caps and follow-ups.
    if event.generation.is_some()
        && event
            .subject
            .as_ref()
            .is_some_and(|subject| subject.state == AgentState::Idle)
    {
        let mail_router = router.clone();
        let mail_agent = agent.clone();
        tokio::spawn(async move {
            mail_router.reoffer_mail_notice(&mail_agent).await;
        });
    }
    if !idle_goal_is_eligible(router, &agent, event.generation).await {
        return;
    }
    reserve_idle_round(
        router,
        run_check,
        rounds,
        in_flight,
        pending,
        pending_reconcile,
        IdleSubject::Event(Box::new(event)),
    );
}

/// Goal set/resume reconcile entry into the same reservation: the
/// retained exact-generation idle subject rounds without a new bus
/// event, and never re-offers mail.
fn spawn_reconcile_round(
    router: &Router,
    run_check: GoalCheckRunner,
    rounds: &mut JoinSet<(String, i64)>,
    in_flight: &mut HashMap<String, InFlightAutoRound>,
    pending: &mut HashMap<String, Event>,
    pending_reconcile: &mut BTreeMap<String, PendingReconcile>,
    subject: IdleSubject,
) {
    reserve_idle_round(
        router,
        run_check,
        rounds,
        in_flight,
        pending,
        pending_reconcile,
        subject,
    );
}

/// Keep the newest wait for one Agent: a newer revision overwrites an
/// older one, and a repeat sweep at the same revision never shrinks the
/// waited-on key.
fn remember_reconcile_wait(
    pending_reconcile: &mut BTreeMap<String, PendingReconcile>,
    agent: String,
    waiting: PendingReconcile,
) {
    let newer = pending_reconcile
        .get(&agent)
        .is_none_or(|prior| (waiting.revision, waiting.seq) >= (prior.revision, prior.seq));
    if newer {
        pending_reconcile.insert(agent, waiting);
    }
}

/// Shared reservation for automatic rounds: at most one per Goal and
/// four Goals at once, through the same round path. A newer event for a
/// Goal with a round in flight is remembered for a follow-up; a retained
/// subject that loses its race waits in revision-bound order instead of
/// going silent. A retained subject past the cap waits for a free slot
/// instead of dropping — only bus events are dropped at the cap — and
/// the Store single-flight marker covers explicit rounds.
fn reserve_idle_round(
    router: &Router,
    run_check: GoalCheckRunner,
    rounds: &mut JoinSet<(String, i64)>,
    in_flight: &mut HashMap<String, InFlightAutoRound>,
    pending: &mut HashMap<String, Event>,
    pending_reconcile: &mut BTreeMap<String, PendingReconcile>,
    subject: IdleSubject,
) {
    let (agent, generation, seq, follow, waiting) = match subject {
        IdleSubject::Event(event) => {
            let Some(agent) = event.agent.clone() else {
                return;
            };
            let generation = event.generation;
            let seq = event.seq;
            (agent, generation, seq, Some(*event), None)
        }
        IdleSubject::Retained {
            agent,
            generation,
            revision,
            seq,
        } => (
            agent,
            Some(generation),
            seq,
            None,
            Some(PendingReconcile {
                generation,
                revision,
                seq,
            }),
        ),
    };
    if let Some(running) = in_flight.get(&agent) {
        if let Some(event) = follow {
            let newer = event.seq > running.seq
                && pending
                    .get(&agent)
                    .is_none_or(|prior| event.seq > prior.seq);
            if newer {
                pending.insert(agent, event);
            }
        } else if let Some(waiting) = waiting {
            remember_reconcile_wait(pending_reconcile, agent, waiting);
        }
        return;
    }
    let Some(generation) = generation else {
        return;
    };
    if follow.as_ref().is_some_and(|event| {
        !event
            .subject
            .as_ref()
            .is_some_and(|subject| subject.state == AgentState::Idle)
    }) {
        return;
    }
    if in_flight.len() >= MAX_CONCURRENT_AUTO_ROUNDS {
        if let Some(waiting) = waiting {
            remember_reconcile_wait(pending_reconcile, agent.clone(), waiting);
            let policy = router.secret_policy_snapshot();
            crate::logging::record(
                &policy,
                "daemon.goal_auto_round_queued",
                &[("agent", &agent)],
                None,
            );
        } else {
            let policy = router.secret_policy_snapshot();
            crate::logging::record(
                &policy,
                "daemon.goal_auto_round_dropped",
                &[("agent", &agent)],
                None,
            );
        }
        return;
    }
    let (cancel, cancellation) = watch::channel(false);
    let task_router = router.clone();
    let task_agent = agent.clone();
    let drive_agent = agent.clone();
    let task = rounds
        .spawn(async move {
            drive_goal_at_idle(
                &task_router,
                run_check,
                &drive_agent,
                generation,
                seq,
                cancellation,
            )
            .await;
            (task_agent, seq)
        })
        .id();
    in_flight.insert(agent, InFlightAutoRound { cancel, seq, task });
}

async fn idle_goal_is_eligible(router: &Router, agent: &str, generation: Option<i64>) -> bool {
    let view = {
        let store = router.inner.store.lock().await;
        // Native wait point: an actively driving native Goal owns the
        // Agent, so Eliot rounds wait. None exists today.
        match store.goal_native_driving(agent) {
            Ok(false) => {}
            _ => return false,
        }
        match store.goal_get(agent, "eliot") {
            Ok(view) => view,
            Err(_) => return false,
        }
    };
    let Some(view) = view else {
        return false;
    };
    if view.status != GoalStatus::Active || Some(view.generation) != generation {
        return false;
    }
    if view.definition.cadence_ms.is_some() {
        return false;
    }
    true
}

/// Shared automatic-round driver for one exact-generation idle subject:
/// the same scope, enablement and single-flight checks whether the
/// subject arrived on the bus or was retained from `goal.set`/`resume`.
/// A busy Agent, a drifted generation or a consumed key stays silent and
/// waits for the next real event; only the round path itself may pause a
/// drifted Goal, so this returns before any probe when generations differ.
async fn drive_goal_at_idle(
    router: &Router,
    run_check: GoalCheckRunner,
    agent: &str,
    generation: i64,
    idle_seq: i64,
    cancellation: watch::Receiver<bool>,
) {
    if !router.rule_firings_open() {
        return;
    }
    if !idle_goal_is_eligible(router, agent, Some(generation)).await {
        return;
    }
    let params = json!({
        "id": ulid::Ulid::new().to_string(),
        "agent": agent,
        "mode": "eliot",
        "idle_seq": idle_seq,
    });
    match router
        .goal_round_with_cancellation(&params, &Principal::Owner, run_check, cancellation)
        .await
    {
        Ok(_) => {}
        // A stale key means this Goal missed its round; the next idle
        // event redrives it. Busy races, gate losses, shutdown cancels
        // and vanished Goals stay silent.
        Err(error) if error.code.as_str() == "stale_round" => {
            let policy = router.secret_policy_snapshot();
            crate::logging::record(
                &policy,
                "daemon.goal_auto_round_stale",
                &[("agent", agent)],
                Some(&error),
            );
        }
        Err(error)
            if matches!(
                error.code.as_str(),
                "busy" | "bad_state" | "not_found" | "stale_generation" | "unavailable"
            ) => {}
        Err(error) => {
            let policy = router.secret_policy_snapshot();
            crate::logging::record(
                &policy,
                "daemon.goal_auto_round_failed",
                &[("agent", agent)],
                Some(&error),
            );
        }
    }
}

/// Sweep the retained idle subjects after `goal.set`/`goal.resume`.
/// Each still-eligible subject enters the same reservation as a bus
/// event, without re-offering mail. A non-idle Agent stays silent; a
/// missing subject (pruned, superseded or never retained) is logged and
/// waits for a real event instead of inventing one. A spent subject is
/// skipped before taking a scheduler slot, so burned keys cannot starve
/// eligible Goals past the cap. A subject that finds its Goal busy or the
/// cap full waits in revision-bound order for the next join. The readback
/// shows the outcome: a scheduled round appears as the Goal's round marker
/// and counters, an ineligible subject leaves them untouched. Drifted
/// generations are skipped without pausing: a set/resume never resumes a
/// lost Agent, and only the round path itself may pause a drifted Goal.
/// A consumed subject rearms only through a new Goal revision (a
/// definition change clears the committed key); unknown-burned keys never
/// rerun within the generation, so repeated set/retry, reads and daemon
/// restart cannot create another round.
async fn reconcile_idle_goals(
    router: &Router,
    run_check: GoalCheckRunner,
    rounds: &mut JoinSet<(String, i64)>,
    in_flight: &mut HashMap<String, InFlightAutoRound>,
    pending: &mut HashMap<String, Event>,
    pending_reconcile: &mut BTreeMap<String, PendingReconcile>,
) {
    if !router.rule_firings_open() {
        return;
    }
    let candidates = {
        let store = router.inner.store.lock().await;
        match store.goal_reconcile_candidates() {
            Ok(candidates) => candidates,
            Err(error) => {
                let policy = router.secret_policy_snapshot();
                crate::logging::record(&policy, "daemon.goal_reconcile_failed", &[], Some(&error));
                return;
            }
        }
    };
    for candidate in candidates {
        if candidate.agent_state != AgentState::Idle {
            continue;
        }
        let Some(seq) = candidate.idle_seq else {
            let policy = router.secret_policy_snapshot();
            crate::logging::record(
                &policy,
                "daemon.goal_reconcile_no_idle_event",
                &[("agent", &candidate.agent)],
                None,
            );
            continue;
        };
        if candidate.key_consumed {
            // Already spent within this generation: never rerun, and never
            // take a scheduler slot another Goal could use.
            let policy = router.secret_policy_snapshot();
            crate::logging::record(
                &policy,
                "daemon.goal_auto_round_stale",
                &[("agent", &candidate.agent)],
                None,
            );
            continue;
        }
        spawn_reconcile_round(
            router,
            run_check,
            rounds,
            in_flight,
            pending,
            pending_reconcile,
            IdleSubject::Retained {
                agent: candidate.agent,
                generation: candidate.generation,
                revision: candidate.revision,
                seq,
            },
        );
    }
}

/// Drive waiting retained subjects when a round resolves: the joined
/// Goal's own revision-bound follow-up first, then the oldest queued
/// subject in Agent order while a scheduler slot is free. Each subject
/// is revalidated against the Store — same generation and revision, same
/// unconsumed idle key — so a subject spent while it waited stays silent
/// without taking the freed slot.
async fn drain_pending_reconcile(
    router: &Router,
    run_check: GoalCheckRunner,
    rounds: &mut JoinSet<(String, i64)>,
    in_flight: &mut HashMap<String, InFlightAutoRound>,
    pending: &mut HashMap<String, Event>,
    pending_reconcile: &mut BTreeMap<String, PendingReconcile>,
    joined: &str,
) {
    while in_flight.len() < MAX_CONCURRENT_AUTO_ROUNDS {
        let next = pending_reconcile
            .remove(joined)
            .map(|waiting| {
                (
                    joined.to_owned(),
                    waiting.generation,
                    waiting.revision,
                    waiting.seq,
                )
            })
            .or_else(|| {
                let agent = pending_reconcile
                    .keys()
                    .find(|agent| !in_flight.contains_key(*agent))
                    .cloned()?;
                pending_reconcile
                    .remove(&agent)
                    .map(|waiting| (agent, waiting.generation, waiting.revision, waiting.seq))
            });
        let Some((agent, generation, revision, seq)) = next else {
            break;
        };
        let eligible = {
            let store = router.inner.store.lock().await;
            match store.goal_reconcile_candidates() {
                Ok(candidates) => candidates
                    .into_iter()
                    .find(|candidate| candidate.agent == agent)
                    .is_some_and(|candidate| {
                        candidate.generation == generation
                            && candidate.revision == revision
                            && candidate.idle_seq == Some(seq)
                            && !candidate.key_consumed
                    }),
                Err(error) => {
                    let policy = router.secret_policy_snapshot();
                    crate::logging::record(
                        &policy,
                        "daemon.goal_reconcile_failed",
                        &[],
                        Some(&error),
                    );
                    pending_reconcile.insert(
                        agent,
                        PendingReconcile {
                            generation,
                            revision,
                            seq,
                        },
                    );
                    break;
                }
            }
        };
        if !eligible {
            let policy = router.secret_policy_snapshot();
            crate::logging::record(
                &policy,
                "daemon.goal_auto_round_stale",
                &[("agent", &agent)],
                None,
            );
            continue;
        }
        spawn_reconcile_round(
            router,
            run_check,
            rounds,
            in_flight,
            pending,
            pending_reconcile,
            IdleSubject::Retained {
                agent,
                generation,
                revision,
                seq,
            },
        );
    }
}

/// Completed/Failed captures are returned only after cleanup (or before spawn).
/// Unknown explicitly carries whether family quiescence was established.
fn goal_check_quiescent(outcome: &ProcessOutcome) -> bool {
    !matches!(
        outcome,
        ProcessOutcome::Unknown {
            quiescent: false,
            ..
        }
    )
}

/// Interpret one owned check completion. Only a definite direct exit with
/// proven custody is a check result; everything else is `unknown` and commits
/// nothing.
fn goal_check_result(outcome: ProcessOutcome, policy: &SecretPolicy) -> Result<GoalCheckResult> {
    match outcome {
        ProcessOutcome::Completed(capture)
        | ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(capture),
        } => {
            if !custody_proven_for_capture(&capture) {
                return Err(Error::new("unknown", "goal check custody is unproven"));
            }
            goal_completed_check_result(
                &capture.stdout,
                capture.stdout_complete,
                &capture.stderr,
                capture.status.code(),
                policy,
            )
        }
        ProcessOutcome::Failed { reason, .. } => {
            if reason == ProcessFailure::Cancelled {
                return Err(Error::new("unavailable", "goal check was cancelled"));
            }
            Err(Error::new("unknown", failed_check_message(reason)))
        }
        ProcessOutcome::Unknown { reason, .. } => Err(Error::new(
            "unknown",
            format!("goal check outcome is unknown: {}", reason.stable_code()),
        )),
    }
}

/// A completed capture is a result only with a definite exit code. A
/// check killed by a signal (Unix exit code `None`) is an unknown outcome:
/// it commits nothing, same as timeout/cancel. Both streams feed the
/// observation, so a reason the check writes only to stderr reaches the
/// next continuation.
fn goal_completed_check_result(
    stdout: &[u8],
    stdout_complete: bool,
    stderr: &StderrSnapshot,
    exit_code: Option<i32>,
    policy: &SecretPolicy,
) -> Result<GoalCheckResult> {
    let Some(exit_code) = exit_code else {
        return Err(Error::new(
            "unknown",
            "goal check has no exit code; its outcome is unknown",
        ));
    };
    // Many checks (cargo, rustc, clippy) report only on stderr, so the
    // no-progress digest covers both streams. Same output is proven only when
    // both were captured in full: a truncated stderr tail never proves no
    // progress, it just leaves the round and turn budgets to stop the Goal.
    let stderr_text = policy.redact_text(&stderr.text);
    let stderr_complete = !stderr.truncated && stderr.eof && !stderr.read_error;
    let mut digest_input = stdout.to_vec();
    digest_input.extend_from_slice(b"\0stderr\0");
    digest_input.extend_from_slice(stderr_text.as_bytes());
    let digest = sha256_hex(&digest_input);
    let stdout = policy.redact_text(&String::from_utf8_lossy(stdout));
    let stderr_label = if stderr.read_error || !stderr.eof {
        "[stderr, incomplete]\n"
    } else if stderr.truncated {
        "[stderr, last part only]\n"
    } else {
        "[stderr]\n"
    };
    let stdout_label = if stdout_complete {
        "[stdout]\n"
    } else {
        "[stdout, incomplete]\n"
    };
    Ok(GoalCheckResult {
        exit_code: Some(exit_code),
        output_complete: stdout_complete && stderr_complete,
        output_digest: Some(digest),
        observation: check_observation(
            [
                (stdout_label, stdout.as_str()),
                (stderr_label, stderr_text.as_str()),
            ],
            MAX_GOAL_OBSERVATION_BYTES,
        ),
    })
}

/// Both labelled streams within `bound` bytes. A stream over its share keeps
/// its beginning and end around an explicit omission marker; a short stream
/// leaves its unused share to the other.
fn check_observation(streams: [(&str, &str); 2], bound: usize) -> Option<String> {
    let streams: Vec<(&str, &str)> = streams
        .into_iter()
        .filter(|(_, text)| !text.trim().is_empty())
        .collect();
    let framing = streams.iter().map(|(label, _)| label.len()).sum::<usize>()
        + streams.len().saturating_sub(1);
    let budget = bound.saturating_sub(framing);
    let shares = match streams.as_slice() {
        [] => return None,
        [(_, only)] => vec![only.len().min(budget)],
        [(_, first), (_, second)] => {
            let first_share = first
                .len()
                .min((budget / 2).max(budget.saturating_sub(second.len())));
            vec![first_share, budget - first_share]
        }
        _ => unreachable!("at most two streams"),
    };
    let sections: Vec<String> = streams
        .iter()
        .zip(shares)
        .map(|((label, text), share)| format!("{label}{}", head_tail(text, share)))
        .collect();
    let (observation, _) = truncate_bytes(&sections.join("\n"), bound);
    Some(observation)
}

/// `text` within `bound` bytes: unchanged when it fits, otherwise its
/// beginning and end around a marker naming the omitted byte count.
fn head_tail(text: &str, bound: usize) -> String {
    if text.len() <= bound {
        return text.to_owned();
    }
    const MARKER_ROOM: usize = 48;
    let keep = bound.saturating_sub(MARKER_ROOM);
    let (head, _) = truncate_bytes(text, keep * 3 / 5);
    let mut tail_start = text.len() - (keep - head.len());
    while tail_start < text.len() && !text.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    let omitted = tail_start - head.len();
    format!(
        "{head}\n[... {omitted} bytes omitted ...]\n{}",
        &text[tail_start..]
    )
}

#[cfg(windows)]
fn custody_proven_for_capture(_: &ProcessCapture) -> bool {
    true
}

#[cfg(unix)]
fn custody_proven_for_capture(capture: &ProcessCapture) -> bool {
    !matches!(capture.custody, crate::platform::CustodyGuarantee::Unknown)
}

/// The Goal workspace's no-progress fingerprint read now; absent when the
/// Agent has no workspace or the read failed.
async fn work_fingerprint(workspace: Option<&str>) -> Option<String> {
    let git = GitProbe::new().ok()?;
    git.work_fingerprint(workspace?).await.ok()
}

fn goal_continuation_text(
    definition: &GoalDefinition,
    check: &GoalCheckResult,
    stage: GoalContinuationStage,
    policy: &SecretPolicy,
) -> String {
    let mut goal_text = match scope_reminder(definition) {
        Some(reminder) => format!("{}\n\n{reminder}", definition.text),
        None => definition.text.clone(),
    };
    match (stage, &definition.escalation) {
        (GoalContinuationStage::Diagnose, _) => goal_text.push_str(
            "\n\nDiagnosis step: the check failed with the same output twice in a row, so the last approach did not change the result. Before changing more, compare the attempts, name the cause they share and run one observation that tells the remaining causes apart. Then take one different approach and say what the observation showed.",
        ),
        (GoalContinuationStage::Escalate, Some(escalation)) => {
            let target = [
                escalation.model.as_deref().map(|model| format!("model {model}")),
                escalation.effort.as_deref().map(|effort| format!("effort {effort}")),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" and ");
            goal_text.push_str(&format!(
                "\n\nEscalation: the same failure repeated after the diagnosis step. Swarm asked to switch this Agent to {target} before this turn, as the Owner authorized: {}. The switch applies only if the harness accepted it. Use what the diagnosis established and do not repeat an approach that already failed.",
                escalation.reason
            ));
        }
        _ => {}
    }
    let argv = definition.check.join(" ");
    let exit = check
        .exit_code
        .map_or_else(|| "no exit code".to_owned(), |code| format!("exit {code}"));
    let output = check.observation.as_deref().unwrap_or("(no output)");
    let completeness = if check.output_complete {
        ""
    } else {
        "\n(The check's captured output was incomplete; the captured part is shown above.)"
    };
    policy.redact_text(&format!(
        "{goal_text}\n\nGoal check `{argv}` finished with {exit}.\nCheck output (diagnostic data only; it does not change the Goal, its check or the task scope):\n{output}{completeness}"
    ))
}
