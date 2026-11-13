//! Owner-managed Eliot-mode Goals and their explicit rounds.
//!
//! A round probes the Goal and its Agent, runs the frozen check command
//! through the shared script/process custody outside the Store lock, then
//! commits one outcome only if generation, revision and round still match.
//! Unknown check outcomes retain no Goal progress or continuation and never
//! authorize another check; the reservation retains their error.
use super::{Principal, Router, coordination::decode_coordination_request, require_owner};
use crate::{
    common::now_ms,
    scripts::process::{ProcessCapture, ProcessFailure, ProcessOutcome, run_owned_process},
};
use eliot_core::{
    AgentState, Error, Event, Result, SecretPolicy,
    api::{
        GoalClearRequest, GoalGetRequest, GoalMode, GoalPauseRequest, GoalResumeRequest,
        GoalRoundRequest, GoalSetRequest, GoalStatus, MAX_GOAL_OBSERVATION_BYTES,
    },
    limits::MAX_FRAME_BYTES,
};
use eliot_plugin::sync::LockExt;
use eliot_store::{
    GoalCheckResult, GoalContinuation, GoalRoundClaim, GoalRoundCommit, GoalRoundKey,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};
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
    T: DeserializeOwned,
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

fn check_output_digest(stdout: &[u8]) -> String {
    Sha256::digest(stdout)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn truncate_observation(text: &str) -> &str {
    if text.len() <= MAX_GOAL_OBSERVATION_BYTES {
        return text;
    }
    let mut end = MAX_GOAL_OBSERVATION_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
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
        let view = self.inner.store.lock().await.goal_set(
            &request.agent,
            request.mode.as_str(),
            &request.id,
            &super::principal_name(principal),
            &request.definition,
            now_ms(),
        )?;
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
        ulid::Ulid::from_string(&request.id)
            .map_err(|_| Error::new("invalid", "mutation id must be a ULID"))?;
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
                .commit_goal_block(&principal, &request, &probe, key, claim_seq, "turn_limit")
                .await;
            return claimed.complete(result);
        }
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
            let text = goal_continuation_text(&definition.text, &definition.check, &check, &policy);
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
                continuation: Some(GoalContinuation {
                    id: continuation_id,
                    text,
                }),
            };
            let result = self.commit_goal_round(&principal, &request, commit).await;
            return claimed.complete(result);
        }
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
            continuation: None,
        };
        let result = self.commit_goal_round(&principal, &request, commit).await;
        claimed.complete(result)
    }

    async fn commit_goal_block(
        &self,
        principal: &str,
        request: &GoalRoundRequest,
        probe: &eliot_store::GoalRoundProbe,
        key: GoalRoundKey,
        claim_seq: i64,
        reason: &str,
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
            if let Some(admission) = &result.admission {
                self.publish_locked(&admission.events);
            }
            result
        };
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

/// Drive active Eliot-mode Goals when their generation commits an idle
/// event. Each round runs in its own task — at most one per Goal — through
/// the same `goal_round` path as an explicit owner call: same probe, gates,
/// check custody and conditional commit, with a fresh owner mutation id.
/// A newer event for a Goal with a round in flight is remembered and
/// driven as a follow-up when that round resolves, so the fresh key is
/// never silently dropped. A `goal.set`/`goal.resume` notification sweeps
/// the retained idle subjects through the same reservation without
/// re-offering mail. Bus lag, cap drops and stale rounds are logged;
/// other lost races stay silent. Shutdown cancels every in-flight check
/// through its custody cancellation and waits only for the rounds to
/// resolve their markers (`unknown`, never rerun), never for a check to
/// finish on its own.
pub(crate) async fn run_goal_driver(
    router: Router,
    run_check: GoalCheckRunner,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    router.note_automation_task_running("goal_driver");
    let mut events = router.subscribe_rule_events();
    let mut goal_changes = router.subscribe_goal_reconcile();
    let mut rounds: JoinSet<(String, i64)> = JoinSet::new();
    let mut in_flight: HashMap<String, InFlightAutoRound> = HashMap::new();
    // Newest event per Goal with a round in flight, driven as a follow-up
    // when that round resolves. Never filled past shutdown: the drain
    // below starts no new rounds.
    let mut pending: HashMap<String, Event> = HashMap::new();
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
                        event,
                    );
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
                            follow,
                        );
                    }
                }
            }
        }
    }
}

/// What an automatic round runs on: a fresh bus event, or the retained
/// exact-generation idle subject a `goal.set`/`goal.resume` reconciles.
/// Bus events can wait as follow-ups; a retained subject that loses the
/// race defers to the live round instead.
enum IdleSubject {
    Event(Box<Event>),
    Retained {
        agent: String,
        generation: i64,
        seq: i64,
    },
}

/// Start one automatic round for an idle event unless its Goal already
/// has one in flight — then a newer event is remembered for a follow-up —
/// or the global cap is reached (the event is dropped and logged).
fn spawn_auto_round(
    router: &Router,
    run_check: GoalCheckRunner,
    rounds: &mut JoinSet<(String, i64)>,
    in_flight: &mut HashMap<String, InFlightAutoRound>,
    pending: &mut HashMap<String, Event>,
    event: Event,
) {
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
    reserve_idle_round(
        router,
        run_check,
        rounds,
        in_flight,
        pending,
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
    subject: IdleSubject,
) {
    reserve_idle_round(router, run_check, rounds, in_flight, pending, subject);
}

/// Shared reservation for automatic rounds: at most one per Goal and
/// four Goals at once, through the same round path. A newer event for a
/// Goal with a round in flight is remembered for a follow-up; a retained
/// subject that loses the race stays silent — the live round or its
/// follow-up covers the same Agent, and the Store single-flight marker
/// covers explicit rounds.
fn reserve_idle_round(
    router: &Router,
    run_check: GoalCheckRunner,
    rounds: &mut JoinSet<(String, i64)>,
    in_flight: &mut HashMap<String, InFlightAutoRound>,
    pending: &mut HashMap<String, Event>,
    subject: IdleSubject,
) {
    let (agent, generation, seq, follow) = match subject {
        IdleSubject::Event(event) => {
            let Some(agent) = event.agent.clone() else {
                return;
            };
            let generation = event.generation;
            let seq = event.seq;
            (agent, generation, seq, Some(*event))
        }
        IdleSubject::Retained {
            agent,
            generation,
            seq,
        } => (agent, Some(generation), seq, None),
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
        let policy = router.secret_policy_snapshot();
        crate::logging::record(
            &policy,
            "daemon.goal_auto_round_dropped",
            &[("agent", &agent)],
            None,
        );
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
    let view = {
        let store = router.inner.store.lock().await;
        // Native wait point: an actively driving native Goal owns the
        // Agent, so Eliot rounds wait. None exists today.
        match store.goal_native_driving(agent) {
            Ok(false) => {}
            _ => return,
        }
        match store.goal_get(agent, "eliot") {
            Ok(view) => view,
            Err(_) => return,
        }
    };
    let Some(view) = view else {
        return;
    };
    if view.status != GoalStatus::Active || view.generation != generation {
        return;
    }
    if view.definition.cadence_ms.is_some() {
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
/// waits for a real event instead of inventing one. The readback shows
/// the outcome: a scheduled round appears as the Goal's round marker and
/// counters, an ineligible subject leaves them untouched. Drifted
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
        spawn_reconcile_round(
            router,
            run_check,
            rounds,
            in_flight,
            pending,
            IdleSubject::Retained {
                agent: candidate.agent,
                generation: candidate.generation,
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
/// it commits nothing, same as timeout/cancel.
fn goal_completed_check_result(
    stdout: &[u8],
    stdout_complete: bool,
    exit_code: Option<i32>,
    policy: &SecretPolicy,
) -> Result<GoalCheckResult> {
    let Some(exit_code) = exit_code else {
        return Err(Error::new(
            "unknown",
            "goal check has no exit code; its outcome is unknown",
        ));
    };
    let digest = check_output_digest(stdout);
    let text = String::from_utf8_lossy(stdout);
    let observation = if text.is_empty() {
        None
    } else {
        Some(policy.redact_text(truncate_observation(text.as_ref())))
    };
    Ok(GoalCheckResult {
        exit_code: Some(exit_code),
        output_complete: stdout_complete,
        output_digest: Some(digest),
        observation,
    })
}

#[cfg(windows)]
fn custody_proven_for_capture(_: &ProcessCapture) -> bool {
    true
}

#[cfg(unix)]
fn custody_proven_for_capture(capture: &ProcessCapture) -> bool {
    !matches!(capture.custody, crate::platform::CustodyGuarantee::Unknown)
}

fn goal_continuation_text(
    goal_text: &str,
    check_argv: &[String],
    check: &GoalCheckResult,
    policy: &SecretPolicy,
) -> String {
    let argv = check_argv.join(" ");
    let exit = check
        .exit_code
        .map_or_else(|| "no exit code".to_owned(), |code| format!("exit {code}"));
    let output = check.observation.as_deref().unwrap_or("(no output)");
    let completeness = if check.output_complete {
        ""
    } else {
        "\n(check output was truncated; re-run the check command for full output)"
    };
    policy.redact_text(&format!(
        "{goal_text}\n\nGoal check `{argv}` finished with {exit}.\nLast output:\n{output}{completeness}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use eliot_core::{EventSubject, NativeEvent, SourceBatch, StderrSnapshot, SubmitCommand};
    use std::{
        path::PathBuf,
        sync::{
            OnceLock,
            atomic::{AtomicUsize, Ordering},
        },
        task::Poll,
    };
    use tokio::sync::{Mutex as AsyncMutex, Notify};

    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    #[cfg(windows)]
    use std::os::windows::process::ExitStatusExt;

    fn router() -> Router {
        let store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new(
            PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        )
    }

    async fn ready_agent(router: &Router, project: &str) -> String {
        let store_handle = Arc::clone(&router.inner.store);
        let mut store = store_handle.lock().await;
        let now = now_ms();
        let launch_id = ulid::Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":project, "workspace":"synthetic-workspace"}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:ready"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("{project}:ready"),
                        data: json!({"command_id":launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, eliot_core::CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    async fn high_water(router: &Router) -> i64 {
        router.inner.store.lock().await.high_water().unwrap()
    }

    async fn set_goal(router: &Router, agent: &str, definition: Value) -> Value {
        router
            .goal_set(
                &json!({"id": ulid::Ulid::new().to_string(), "agent":agent, "mode":"eliot", "definition":definition}),
                &Principal::Owner,
            )
            .await
            .unwrap()
    }

    fn definition(text: &str) -> Value {
        json!({
            "text": text,
            "check": ["check-goal"],
            "max_turns": 4,
            "max_rounds": 6,
            "max_without_progress": 2,
        })
    }

    #[cfg(unix)]
    fn exit_status(code: i32) -> std::process::ExitStatus {
        std::process::ExitStatus::from_raw(code << 8)
    }

    #[cfg(windows)]
    fn exit_status(code: i32) -> std::process::ExitStatus {
        std::process::ExitStatus::from_raw(code as u32)
    }

    #[cfg(unix)]
    fn goal_test_custody() -> crate::platform::CustodyGuarantee {
        crate::platform::CustodyGuarantee::Degraded
    }

    #[cfg(windows)]
    fn goal_test_custody() -> crate::platform::CustodyGuarantee {
        crate::platform::CustodyGuarantee::Exact
    }

    fn check_capture(code: i32, stdout: &[u8], complete: bool) -> ProcessCapture {
        ProcessCapture {
            stdout: stdout.to_vec(),
            stdout_complete: complete,
            stderr: StderrSnapshot {
                text: String::new(),
                truncated: false,
                eof: true,
                read_error: false,
            },
            status: exit_status(code),
            custody: goal_test_custody(),
        }
    }

    fn completed_outcome(code: i32, stdout: &[u8], complete: bool) -> ProcessOutcome {
        ProcessOutcome::Completed(check_capture(code, stdout, complete))
    }

    fn failed_exit_outcome(code: i32, stdout: &[u8], complete: bool) -> ProcessOutcome {
        ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(check_capture(code, stdout, complete)),
        }
    }

    fn owned_check_definition(text: &str) -> Value {
        let mut value = definition(text);
        value["check"] = json!([
            std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "--exact",
            "scripts::process::tests::process_fixture",
            "--nocapture",
        ]);
        value["check_cwd"] = json!(
            std::env::current_dir()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        );
        value["check_timeout_ms"] = json!(5_000);
        value
    }

    fn owned_failing_runner(
        mut command: TokioCommand,
        stdin: Vec<u8>,
        timeout: Duration,
        policy: Arc<SecretPolicy>,
        cancellation: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        command.env("ELIOT_PROCESS_TEST_FIXTURE", "goal-nonzero");
        real_goal_check(command, stdin, timeout, policy, cancellation)
    }

    fn owned_passing_runner(
        mut command: TokioCommand,
        stdin: Vec<u8>,
        timeout: Duration,
        policy: Arc<SecretPolicy>,
        cancellation: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        command.env("ELIOT_PROCESS_TEST_FIXTURE", "goal-pass");
        real_goal_check(command, stdin, timeout, policy, cancellation)
    }

    macro_rules! canned_runner {
        ($name:ident, $outcome:expr) => {
            fn $name(
                _: TokioCommand,
                _: Vec<u8>,
                _: Duration,
                _: Arc<SecretPolicy>,
                _: watch::Receiver<bool>,
            ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
                Box::pin(async { $outcome })
            }
        };
    }

    canned_runner!(passing_runner, completed_outcome(0, b"all green", true));
    canned_runner!(failing_runner, failed_exit_outcome(1, b"red", true));
    canned_runner!(
        incomplete_failing_runner,
        failed_exit_outcome(1, b"red", false)
    );
    canned_runner!(
        missing_capture_runner,
        ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: None,
        }
    );
    canned_runner!(
        unknown_runner,
        ProcessOutcome::Unknown {
            reason: crate::scripts::process::ProcessUncertainty::Exit,
            quiescent: true,
        }
    );
    canned_runner!(
        custody_unknown_runner,
        ProcessOutcome::Unknown {
            reason: crate::scripts::process::ProcessUncertainty::Custody,
            quiescent: false,
        }
    );
    #[cfg(unix)]
    canned_runner!(unproven_capture_runner, {
        let mut capture = check_capture(1, b"red", true);
        capture.custody = crate::platform::CustodyGuarantee::Unknown;
        ProcessOutcome::Failed {
            reason: ProcessFailure::Exit,
            capture: Some(capture),
        }
    });
    canned_runner!(
        timeout_runner,
        ProcessOutcome::Failed {
            reason: crate::scripts::process::ProcessFailure::Timeout,
            capture: None,
        }
    );
    canned_runner!(
        timeout_capture_runner,
        ProcessOutcome::Failed {
            reason: ProcessFailure::Timeout,
            capture: Some(check_capture(0, b"output before timeout", true)),
        }
    );
    canned_runner!(
        cancelled_runner,
        ProcessOutcome::Failed {
            reason: crate::scripts::process::ProcessFailure::Cancelled,
            capture: None,
        }
    );

    fn exploding_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        panic!("goal check must not run for a refused round")
    }

    /// Drive one continuation send through a full turn so the Agent is
    /// cleanly idle again and a later round can claim a fresh key.
    async fn complete_send(router: &Router, agent: &str, send_id: &str, turn_id: &str) {
        let mut store = router.inner.store.lock().await;
        let now = now_ms();
        store.dispatch(send_id, now).unwrap();
        for (kind, data) in [
            (
                "turn.started",
                json!({"command_id": send_id, "turn_id": turn_id}),
            ),
            (
                "turn.completed",
                json!({"command_id": send_id, "turn_id": turn_id, "status": "completed"}),
            ),
        ] {
            store
                .put_batch(
                    SourceBatch {
                        batch_id: ulid::Ulid::new().to_string(),
                        agent_id: agent.to_owned(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: kind.to_owned(),
                            native_id: ulid::Ulid::new().to_string(),
                            data,
                        }],
                        cursor: None,
                    },
                    now,
                )
                .unwrap();
        }
        store
            .settle(send_id, eliot_core::CommandState::Applied, None, now)
            .unwrap();
    }

    static CONCURRENT_RUNS: AtomicUsize = AtomicUsize::new(0);
    static CONCURRENT_RELEASE: OnceLock<Notify> = OnceLock::new();

    fn blocking_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        Box::pin(async {
            let release = CONCURRENT_RELEASE.get_or_init(Notify::new).notified();
            CONCURRENT_RUNS.fetch_add(1, Ordering::SeqCst);
            release.await;
            completed_outcome(0, b"all green", true)
        })
    }

    static RACE_STARTED: AtomicUsize = AtomicUsize::new(0);
    static RACE_RELEASE: OnceLock<Notify> = OnceLock::new();

    fn race_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        Box::pin(async {
            let release = RACE_RELEASE.get_or_init(Notify::new).notified();
            RACE_STARTED.fetch_add(1, Ordering::SeqCst);
            release.await;
            completed_outcome(0, b"all green", true)
        })
    }

    /// Poll a round future on this task until its check starts running.
    async fn run_until_check_starts<F>(round: &mut Pin<Box<F>>, started: &AtomicUsize)
    where
        F: Future<Output = Result<Value>>,
    {
        tokio::time::timeout(
            Duration::from_secs(10),
            std::future::poll_fn(|cx| {
                if started.load(Ordering::SeqCst) == 0 && round.as_mut().poll(cx).is_ready() {
                    panic!("round finished without running its check");
                }
                if started.load(Ordering::SeqCst) > 0 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .expect("check starts");
    }

    #[test]
    fn goal_check_helpers_bound_and_shape_outputs() {
        let digest = check_output_digest(b"same");
        assert_eq!(digest, check_output_digest(b"same"));
        assert_eq!(digest.len(), 64);
        assert_ne!(digest, check_output_digest(b"other"));
        assert_eq!(truncate_observation("short"), "short");
        let long = "x".repeat(MAX_GOAL_OBSERVATION_BYTES + 10);
        assert_eq!(
            truncate_observation(&long).len(),
            MAX_GOAL_OBSERVATION_BYTES
        );
        let boundary = format!("{}é", "y".repeat(MAX_GOAL_OBSERVATION_BYTES - 1));
        assert!(truncate_observation(&boundary).len() <= MAX_GOAL_OBSERVATION_BYTES);
        let policy = SecretPolicy::new(Vec::<String>::new());
        let check = GoalCheckResult {
            exit_code: Some(1),
            output_complete: true,
            output_digest: Some(digest),
            observation: Some("broken".to_owned()),
        };
        let text = goal_continuation_text(
            "keep green",
            &["check-goal".to_owned(), "--strict".to_owned()],
            &check,
            &policy,
        );
        assert!(
            text.starts_with("keep green\n\nGoal check `check-goal --strict`"),
            "{text}"
        );
        assert!(text.contains("exit 1"), "{text}");
        assert!(text.contains("broken"), "{text}");
        assert_eq!(
            failed_check_message(crate::scripts::process::ProcessFailure::Timeout),
            "goal check timed out"
        );
    }

    #[tokio::test]
    async fn goal_set_show_pause_clear_resume_round_trip() {
        let router = router();
        let agent = ready_agent(&router, "goal-crud").await;
        let set = set_goal(&router, &agent, definition("keep green")).await;
        assert_eq!(set["revision"], 1);
        assert_eq!(set["status"], "active");
        let shown = router
            .goal_get(&json!({"agent":agent, "mode":"eliot"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(shown, set);
        let paused = router
            .goal_pause(
                &json!({"id": ulid::Ulid::new().to_string(), "agent":agent, "mode":"eliot", "reason":"waiting"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(paused["revision"], 2);
        assert_eq!(paused["status"], "paused");
        let resumed = router
            .goal_resume(
                &json!({"id": ulid::Ulid::new().to_string(), "agent":agent, "mode":"eliot"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(resumed["revision"], 3);
        let cleared = router
            .goal_clear(
                &json!({"id": ulid::Ulid::new().to_string(), "agent":agent, "mode":"eliot"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(cleared["revision"], 4);
        assert_eq!(cleared["status"], "cleared");
    }

    #[tokio::test]
    async fn goal_native_mode_is_unsupported() {
        let router = router();
        let agent = ready_agent(&router, "goal-native").await;
        let error = router
            .goal_set(
                &json!({"id": ulid::Ulid::new().to_string(), "agent":agent, "mode":"native", "definition":definition("x")}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "unsupported");
        let error = router
            .goal_round(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"native","idle_seq":0}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "unsupported");
    }

    #[tokio::test]
    async fn goal_round_owned_passing_check_completes_without_send() {
        let router = router();
        let agent = ready_agent(&router, "goal-pass").await;
        set_goal(&router, &agent, owned_check_definition("keep green")).await;
        let key = high_water(&router).await;
        let id = ulid::Ulid::new().to_string();
        let response = router
            .goal_round_with(
                &json!({"id":id,"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                owned_passing_runner,
            )
            .await
            .unwrap();
        assert_eq!(response["outcome"], "completed");
        assert_eq!(response["view"]["status"], "completed");
        assert_eq!(response["view"]["generation"], 1);
        assert_eq!(response["view"]["revision"], 2);
        assert_eq!(response["view"]["counters"]["rounds"], 1);
        assert_eq!(response["view"]["counters"]["last_exit_code"], 0);
        assert_eq!(response["view"]["last_round_key"], format!("idle:{key}"));
        assert!(response["continuation"].is_null());
        assert!(response["command_id"].is_null());
        let observation = response["view"]["counters"]["last_observation"]
            .as_str()
            .unwrap();
        assert!(observation.contains("fixture-goal-check:"));
        assert!(observation.contains(&format!("\"round_key\":\"idle:{key}\"")));
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .get_command(&id)
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn goal_round_owned_nonzero_check_admits_one_bound_send() {
        let router = router();
        let agent = ready_agent(&router, "goal-continue").await;
        set_goal(&router, &agent, owned_check_definition("keep green")).await;
        let key = high_water(&router).await;
        let id = ulid::Ulid::new().to_string();
        let response = router
            .goal_round_with(
                &json!({"id":id,"agent":agent,"mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                owned_failing_runner,
            )
            .await
            .unwrap();
        assert_eq!(response["outcome"], "continued");
        assert_eq!(response["command_id"], id);
        assert_eq!(response["continuation"]["command_id"], id);
        assert_eq!(response["view"]["generation"], 1);
        assert_eq!(response["view"]["revision"], 2);
        assert_eq!(response["view"]["counters"]["rounds"], 1);
        assert_eq!(response["view"]["counters"]["last_exit_code"], 7);
        assert_eq!(response["view"]["last_round_key"], format!("idle:{key}"));
        assert!(response["view"]["round_marker"].is_null());
        let send = router
            .inner
            .store
            .lock()
            .await
            .get_command(&id)
            .unwrap()
            .unwrap();
        assert_eq!(send.verb, "send");
        assert_eq!(send.target.as_deref(), Some(agent.as_str()));
        assert_eq!(send.expected_generation, Some(1));
        assert_eq!(send.generation, Some(1));
        assert!(send.args["text"].as_str().unwrap().contains("keep green"));
        let text = send.args["text"].as_str().unwrap();
        assert!(text.contains("fixture-goal-check:"));
        assert!(text.contains(&format!("\"agent\":\"{agent}\"")));
        assert!(text.contains("\"generation\":1"));
        assert!(text.contains("\"revision\":1"));
        assert!(text.contains(&format!("\"round_key\":\"idle:{key}\"")));
        assert!(text.contains("exit 7"));
    }

    #[tokio::test]
    async fn goal_round_refuses_gates_without_running_the_check() {
        let router = router();
        let agent = ready_agent(&router, "goal-gates").await;
        set_goal(&router, &agent, definition("keep green")).await;
        // Stale key.
        let key = high_water(&router).await;
        let error = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","idle_seq":key.saturating_sub(1)}),
                &Principal::Owner,
                exploding_runner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "stale_round");
        // Open command.
        let send_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut request = SubmitCommand::new(send_id.clone(), "send");
            request.target = Some(agent.clone());
            request.expected_generation = Some(1);
            request.args = json!({"text": "operator input"});
            store.submit("owner", request, now_ms()).unwrap();
        }
        let key = high_water(&router).await;
        let error = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                exploding_runner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "busy");
        let view = router
            .inner
            .store
            .lock()
            .await
            .goal_get(&agent, "eliot")
            .unwrap()
            .unwrap();
        assert_eq!(view.counters.rounds, 0);
        assert!(view.last_round_key.is_none());
    }

    #[tokio::test]
    async fn goal_round_blocks_a_spent_turn_budget_without_a_check() {
        let router = router();
        let agent = ready_agent(&router, "goal-turn-budget").await;
        set_goal(
            &router,
            &agent,
            json!({
                "text": "keep green",
                "check": ["check-goal"],
                "max_turns": 1,
            }),
        )
        .await;
        // One operator turn outside any round spends the budget.
        let send_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut request = SubmitCommand::new(send_id.clone(), "send");
            request.target = Some(agent.clone());
            request.expected_generation = Some(1);
            request.args = json!({"text": "operator input"});
            store.submit("owner", request, now_ms()).unwrap();
            store.dispatch(&send_id, now_ms()).unwrap();
            for (kind, data) in [
                (
                    "turn.started",
                    json!({"command_id": send_id, "turn_id": "turn-1"}),
                ),
                (
                    "turn.completed",
                    json!({"command_id": send_id, "turn_id": "turn-1",
                        "status": "completed"}),
                ),
            ] {
                store
                    .put_batch(
                        SourceBatch {
                            batch_id: ulid::Ulid::new().to_string(),
                            agent_id: agent.clone(),
                            generation: 1,
                            source: "stream".to_owned(),
                            events: vec![NativeEvent {
                                ts: None,
                                kind: kind.to_owned(),
                                native_id: ulid::Ulid::new().to_string(),
                                data,
                            }],
                            cursor: None,
                        },
                        now_ms(),
                    )
                    .unwrap();
            }
            store
                .settle(&send_id, eliot_core::CommandState::Applied, None, now_ms())
                .unwrap();
        }
        let key = high_water(&router).await;
        let response = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                exploding_runner,
            )
            .await
            .unwrap();
        assert_eq!(response["outcome"], "blocked");
        assert_eq!(response["view"]["blocked_reason"], "turn_limit");
        assert_eq!(response["view"]["counters"]["rounds"], 0);
    }

    #[tokio::test]
    async fn goal_round_commits_nothing_for_unknown_check_outcomes() {
        let router = router();
        // Each unknown outcome burns its round key, so each runner gets a
        // fresh Agent and Goal.
        let cases = [
            ("goal-unknown", unknown_runner as GoalCheckRunner, "unknown"),
            (
                "goal-custody",
                custody_unknown_runner as GoalCheckRunner,
                "unknown",
            ),
            (
                "goal-missing-capture",
                missing_capture_runner as GoalCheckRunner,
                "unknown",
            ),
            ("goal-timeout", timeout_runner as GoalCheckRunner, "unknown"),
            (
                "goal-timeout-capture",
                timeout_capture_runner as GoalCheckRunner,
                "unknown",
            ),
            (
                "goal-cancelled",
                cancelled_runner as GoalCheckRunner,
                "unavailable",
            ),
        ]
        .into_iter();
        #[cfg(unix)]
        let cases = cases.chain(std::iter::once((
            "goal-unproven-capture",
            unproven_capture_runner as GoalCheckRunner,
            "unknown",
        )));
        for (project, runner, code) in cases {
            let agent = ready_agent(&router, project).await;
            set_goal(&router, &agent, definition("keep green")).await;
            let key = high_water(&router).await;
            let id = ulid::Ulid::new().to_string();
            let error = router
                .goal_round_with(
                    &json!({"id":id,"agent":agent,
                        "mode":"eliot","idle_seq":key}),
                    &Principal::Owner,
                    runner,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, code);
            assert!(
                router
                    .inner
                    .store
                    .lock()
                    .await
                    .get_command(&id)
                    .unwrap()
                    .is_none()
            );
            let view = router
                .inner
                .store
                .lock()
                .await
                .goal_get(&agent, "eliot")
                .unwrap()
                .unwrap();
            assert_eq!(view.counters.rounds, 0);
            assert!(view.last_round_key.is_none());
            assert_eq!(view.status, eliot_core::api::GoalStatus::Active);
            let marker = view.round_marker.expect("burned key stays visible");
            assert_eq!(marker.state, eliot_core::api::GoalMarkerState::Unknown);
            assert_eq!(marker.round_key, format!("idle:{key}"));
        }
    }

    #[tokio::test]
    async fn goal_completed_check_needs_a_definite_exit_code() {
        let policy = SecretPolicy::new(Vec::<String>::new());
        let error = goal_completed_check_result(b"output", true, None, &policy).unwrap_err();
        assert_eq!(error.code, "unknown");
        let check = goal_completed_check_result(b"output", true, Some(3), &policy).unwrap();
        assert_eq!(check.exit_code, Some(3));
        assert_eq!(check.observation.as_deref(), Some("output"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn goal_signal_killed_check_is_an_unknown_outcome() {
        let router = router();
        let agent = ready_agent(&router, "goal-signal").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        fn signal_runner(
            _: TokioCommand,
            _: Vec<u8>,
            _: Duration,
            _: Arc<SecretPolicy>,
            _: watch::Receiver<bool>,
        ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
            Box::pin(async {
                ProcessOutcome::Failed {
                    reason: ProcessFailure::Exit,
                    capture: Some(ProcessCapture {
                        stdout: b"partial".to_vec(),
                        stdout_complete: true,
                        stderr: StderrSnapshot {
                            text: String::new(),
                            truncated: false,
                            eof: true,
                            read_error: false,
                        },
                        status: std::process::ExitStatus::from_raw(9),
                        custody: crate::platform::CustodyGuarantee::Degraded,
                    }),
                }
            })
        }
        let id = ulid::Ulid::new().to_string();
        let error = router
            .goal_round_with(
                &json!({"id":id,"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                signal_runner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "unknown");
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .get_command(&id)
                .unwrap()
                .is_none()
        );
        let view = router
            .inner
            .store
            .lock()
            .await
            .goal_get(&agent, "eliot")
            .unwrap()
            .unwrap();
        assert_eq!(view.counters.rounds, 0);
        assert!(view.last_round_key.is_none());
    }

    #[tokio::test]
    async fn goal_round_refuses_a_second_concurrent_round() {
        CONCURRENT_RUNS.store(0, Ordering::SeqCst);
        let router = router();
        let agent = ready_agent(&router, "goal-concurrent").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        let first_params = json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
            "mode":"eliot","idle_seq":key});
        let second_params = json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
            "mode":"eliot","idle_seq":key});
        let mut first =
            Box::pin(router.goal_round_with(&first_params, &Principal::Owner, blocking_runner));
        run_until_check_starts(&mut first, &CONCURRENT_RUNS).await;
        let error = router
            .goal_round_with(&second_params, &Principal::Owner, exploding_runner)
            .await
            .unwrap_err();
        assert_eq!(error.code, "busy");
        assert_eq!(CONCURRENT_RUNS.load(Ordering::SeqCst), 1);
        CONCURRENT_RELEASE.get_or_init(Notify::new).notify_one();
        let response = first.await.unwrap();
        assert_eq!(response["outcome"], "completed");
        assert_eq!(CONCURRENT_RUNS.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn goal_round_commits_nothing_for_a_send_admitted_during_the_check() {
        RACE_STARTED.store(0, Ordering::SeqCst);
        let router = router();
        let agent = ready_agent(&router, "goal-race").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        let round_id = ulid::Ulid::new().to_string();
        let params = json!({"id":round_id,"agent":agent,"mode":"eliot","idle_seq":key});
        let mut round = Box::pin(router.goal_round_with(&params, &Principal::Owner, race_runner));
        run_until_check_starts(&mut round, &RACE_STARTED).await;
        {
            let mut store = router.inner.store.lock().await;
            let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "send");
            request.target = Some(agent.clone());
            request.expected_generation = Some(1);
            request.args = json!({"text": "operator input"});
            store.submit("owner", request, now_ms()).unwrap();
        }
        RACE_RELEASE.get_or_init(Notify::new).notify_one();
        let error = round.await.unwrap_err();
        assert_eq!(error.code, "stale_round");
        let view = router
            .inner
            .store
            .lock()
            .await
            .goal_get(&agent, "eliot")
            .unwrap()
            .unwrap();
        assert_eq!(view.counters.rounds, 0);
        assert!(view.last_round_key.is_none());
        let marker = view.round_marker.expect("raced key stays visible");
        assert_eq!(marker.state, eliot_core::api::GoalMarkerState::Unknown);
        assert_eq!(marker.round_key, format!("idle:{key}"));
    }

    #[tokio::test]
    async fn goal_round_blocks_on_its_round_budget() {
        let router = router();
        let agent = ready_agent(&router, "goal-round-budget").await;
        set_goal(
            &router,
            &agent,
            json!({
                "text": "keep green",
                "check": ["check-goal"],
                "max_rounds": 1,
            }),
        )
        .await;
        let key = high_water(&router).await;
        let round_id = ulid::Ulid::new().to_string();
        let response = router
            .goal_round_with(
                &json!({"id":round_id,"agent":agent,"mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                failing_runner,
            )
            .await
            .unwrap();
        assert_eq!(response["outcome"], "blocked");
        assert_eq!(response["reason"], "round_budget");
        assert_eq!(response["view"]["status"], "blocked");
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .get_command(&round_id)
                .unwrap()
                .is_none()
        );
    }

    fn idle_event(agent: &str, generation: i64, seq: i64) -> Event {
        busy_event(agent, generation, seq, AgentState::Idle)
    }

    fn busy_event(agent: &str, generation: i64, seq: i64, state: AgentState) -> Event {
        Event {
            seq,
            ts: 0,
            kind: "turn.completed".to_owned(),
            agent: Some(agent.to_owned()),
            subject: Some(EventSubject {
                project: "goal-driver".to_owned(),
                harness: "fake".to_owned(),
                profile: "fake".to_owned(),
                state,
                workspace: None,
            }),
            generation: Some(generation),
            source: "stream".to_owned(),
            native_id: None,
            data: json!({}),
            cause: None,
        }
    }

    async fn stored_goal(router: &Router, agent: &str) -> eliot_core::api::GoalView {
        router
            .inner
            .store
            .lock()
            .await
            .goal_get(agent, "eliot")
            .unwrap()
            .unwrap()
    }

    async fn drive_event(router: &Router, run_check: GoalCheckRunner, event: Event) {
        let (_cancel, cancellation) = watch::channel(false);
        let Some(agent) = event.agent.clone() else {
            return;
        };
        let Some(generation) = event.generation else {
            return;
        };
        if !event
            .subject
            .as_ref()
            .is_some_and(|subject| subject.state == AgentState::Idle)
        {
            return;
        }
        drive_goal_at_idle(
            router,
            run_check,
            &agent,
            generation,
            event.seq,
            cancellation,
        )
        .await;
    }

    #[tokio::test]
    async fn goal_slot_round_refuses_future_slots_and_wrong_forms() {
        let router = router();
        let agent = ready_agent(&router, "goal-slot-refuse").await;
        let mut cadenced = definition("keep green");
        cadenced["cadence_ms"] = json!(60_000);
        set_goal(&router, &agent, cadenced).await;
        // The first slot starts one period after the set anchor: not due yet.
        let error = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","slot":1}),
                &Principal::Owner,
                exploding_runner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "stale_round");
        // A cadenced Goal refuses idle keys.
        let key = high_water(&router).await;
        let error = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                exploding_runner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        // A Goal without a cadence refuses slots.
        let plain = ready_agent(&router, "goal-slot-plain").await;
        set_goal(&router, &plain, definition("keep green")).await;
        let error = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":plain,
                    "mode":"eliot","slot":1}),
                &Principal::Owner,
                exploding_runner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        // Malformed key pairs never reach the Store.
        for params in [
            json!({"id":ulid::Ulid::new().to_string(),"agent":plain,
                "mode":"eliot","slot":0}),
            json!({"id":ulid::Ulid::new().to_string(),"agent":plain,
                "mode":"eliot","idle_seq":key,"slot":1}),
            json!({"id":ulid::Ulid::new().to_string(),"agent":plain,
                "mode":"eliot"}),
        ] {
            let error = router
                .goal_round_with(&params, &Principal::Owner, exploding_runner)
                .await
                .unwrap_err();
            assert_eq!(error.code, "invalid");
        }
    }

    #[tokio::test]
    async fn goal_driver_rounds_an_idle_agent_automatically() {
        let router = router();
        let agent = ready_agent(&router, "goal-auto").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        drive_event(&router, passing_runner, idle_event(&agent, 1, key)).await;
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Completed);
        assert_eq!(view.counters.rounds, 1);
        assert_eq!(view.last_round_key, Some(format!("idle:{key}")));
    }

    #[tokio::test]
    async fn goal_driver_skips_without_a_driving_goal() {
        let router = router();
        // No Goal at all.
        let bare = ready_agent(&router, "goal-auto-bare").await;
        let key = high_water(&router).await;
        drive_event(&router, passing_runner, idle_event(&bare, 1, key)).await;
        assert!(
            router
                .inner
                .store
                .lock()
                .await
                .goal_get(&bare, "eliot")
                .unwrap()
                .is_none()
        );
        // A paused Goal takes no rounds.
        let paused = ready_agent(&router, "goal-auto-paused").await;
        set_goal(&router, &paused, definition("keep green")).await;
        router
            .goal_pause(
                &json!({"id": ulid::Ulid::new().to_string(), "agent":paused, "mode":"eliot", "reason":"waiting"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let key = high_water(&router).await;
        drive_event(&router, passing_runner, idle_event(&paused, 1, key)).await;
        let view = stored_goal(&router, &paused).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Paused);
        assert_eq!(view.counters.rounds, 0);
        // A cadenced Goal rounds on slots, never on idle events.
        let cadenced = ready_agent(&router, "goal-auto-slot").await;
        let mut cadenced_definition = definition("keep green");
        cadenced_definition["cadence_ms"] = json!(60_000);
        set_goal(&router, &cadenced, cadenced_definition).await;
        let key = high_water(&router).await;
        drive_event(&router, passing_runner, idle_event(&cadenced, 1, key)).await;
        let view = stored_goal(&router, &cadenced).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Active);
        assert_eq!(view.counters.rounds, 0);
        // A busy event or a drifted generation never drives.
        let agent = ready_agent(&router, "goal-auto-gates").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        drive_event(
            &router,
            passing_runner,
            busy_event(&agent, 1, key, AgentState::Busy),
        )
        .await;
        drive_event(&router, passing_runner, idle_event(&agent, 2, key)).await;
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.counters.rounds, 0);
        assert!(view.last_round_key.is_none());
    }

    #[tokio::test]
    async fn goal_driver_tolerates_a_lost_race() {
        let router = router();
        let agent = ready_agent(&router, "goal-auto-race").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        // A stale key and a second drive after completion both stay silent.
        drive_event(
            &router,
            passing_runner,
            idle_event(&agent, 1, key.saturating_sub(1)),
        )
        .await;
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.counters.rounds, 0);
        drive_event(&router, passing_runner, idle_event(&agent, 1, key)).await;
        drive_event(&router, passing_runner, idle_event(&agent, 1, key)).await;
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Completed);
        assert_eq!(view.counters.rounds, 1);
    }

    static SPLIT_STARTED: AtomicUsize = AtomicUsize::new(0);
    static SPLIT_RELEASE: OnceLock<Notify> = OnceLock::new();

    /// The first check blocks until released; later checks pass at once.
    /// Tests publish the slow Goal first and wait for its check to start,
    /// so the blocking check is always the slow one.
    fn split_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        Box::pin(async {
            if SPLIT_STARTED.fetch_add(1, Ordering::SeqCst) == 0 {
                SPLIT_RELEASE.get_or_init(Notify::new).notified().await;
            }
            completed_outcome(0, b"all green", true)
        })
    }

    static CAP_STARTED: AtomicUsize = AtomicUsize::new(0);
    static CAP_RELEASE: OnceLock<Notify> = OnceLock::new();

    fn cap_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        Box::pin(async {
            CAP_STARTED.fetch_add(1, Ordering::SeqCst);
            CAP_RELEASE.get_or_init(Notify::new).notified().await;
            completed_outcome(0, b"all green", true)
        })
    }

    /// A check that runs until its custody cancellation fires, mirroring
    /// `run_owned_process`: cancel means a `Cancelled` failure.
    fn cancelling_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        mut cancellation: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        Box::pin(async move {
            CANCEL_STARTED.fetch_add(1, Ordering::SeqCst);
            loop {
                if *cancellation.borrow() {
                    break;
                }
                if cancellation.changed().await.is_err() {
                    break;
                }
            }
            ProcessOutcome::Failed {
                reason: ProcessFailure::Cancelled,
                capture: None,
            }
        })
    }

    static CANCEL_STARTED: AtomicUsize = AtomicUsize::new(0);

    /// Publish one idle event until `ready` holds. Repeats are harmless:
    /// the driver dedupes a Goal that already has a round in flight.
    async fn publish_until(
        router: &Router,
        agent: &str,
        generation: i64,
        seq: i64,
        ready: &AtomicUsize,
    ) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while ready.load(Ordering::SeqCst) == 0 {
                router
                    .inner
                    .bus
                    .publish(&[idle_event(agent, generation, seq)]);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("published event drives its round");
    }

    #[tokio::test]
    async fn goal_driver_drives_a_second_goal_during_a_slow_check() {
        SPLIT_STARTED.store(0, Ordering::SeqCst);
        let router = router();
        let slow = ready_agent(&router, "goal-slow").await;
        set_goal(&router, &slow, definition("slow goal")).await;
        let slow_key = high_water(&router).await;
        let fast = ready_agent(&router, "goal-fast").await;
        set_goal(&router, &fast, definition("fast goal")).await;
        let fast_key = high_water(&router).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(router.clone(), split_runner, shutdown_rx));
        publish_until(&router, &slow, 1, slow_key, &SPLIT_STARTED).await;
        // The fast Goal rounds and completes while the slow check is
        // still blocked: no stall, no silent drop.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                router.inner.bus.publish(&[idle_event(&fast, 1, fast_key)]);
                if stored_goal(&router, &fast).await.counters.rounds == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fast goal rounds during the slow check");
        let view = stored_goal(&router, &fast).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Completed);
        SPLIT_RELEASE.get_or_init(Notify::new).notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if stored_goal(&router, &slow).await.counters.rounds == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("slow goal rounds after its release");
        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver joins")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn goal_driver_drops_rounds_past_its_cap() {
        CAP_STARTED.store(0, Ordering::SeqCst);
        let router = router();
        let mut agents = Vec::new();
        for index in 0..5 {
            let agent = ready_agent(&router, &format!("goal-cap-{index}")).await;
            set_goal(&router, &agent, definition("capped goal")).await;
            agents.push((agent, high_water(&router).await));
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(router.clone(), cap_runner, shutdown_rx));
        publish_until(&router, &agents[0].0, 1, agents[0].1, &CAP_STARTED).await;
        for (agent, key) in &agents[1..] {
            router.inner.bus.publish(&[idle_event(agent, 1, *key)]);
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while CAP_STARTED.load(Ordering::SeqCst) < MAX_CONCURRENT_AUTO_ROUNDS {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("cap fills with running checks");
        // The fifth event is dropped at the cap: no marker, no round.
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            CAP_STARTED.load(Ordering::SeqCst),
            MAX_CONCURRENT_AUTO_ROUNDS
        );
        for (agent, _) in &agents[..MAX_CONCURRENT_AUTO_ROUNDS] {
            assert!(
                stored_goal(&router, agent).await.round_marker.is_some(),
                "capped round stays in flight"
            );
        }
        let dropped = stored_goal(&router, &agents[MAX_CONCURRENT_AUTO_ROUNDS].0).await;
        assert!(dropped.round_marker.is_none());
        assert_eq!(dropped.counters.rounds, 0);
        for _ in 0..MAX_CONCURRENT_AUTO_ROUNDS {
            CAP_RELEASE.get_or_init(Notify::new).notify_one();
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let mut done = true;
                for (agent, _) in &agents[..MAX_CONCURRENT_AUTO_ROUNDS] {
                    if stored_goal(&router, agent).await.counters.rounds != 1 {
                        done = false;
                    }
                }
                if done {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("capped rounds finish after release");
        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver joins")
            .unwrap()
            .unwrap();
    }

    static PANIC_STARTED: AtomicUsize = AtomicUsize::new(0);

    /// The first `MAX_CONCURRENT_AUTO_ROUNDS` checks panic; later ones pass.
    fn panicking_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        Box::pin(async {
            if PANIC_STARTED.fetch_add(1, Ordering::SeqCst) < MAX_CONCURRENT_AUTO_ROUNDS {
                panic!("check runner panicked");
            }
            completed_outcome(0, b"all green", true)
        })
    }

    #[tokio::test]
    async fn goal_driver_frees_cap_slots_of_panicked_rounds() {
        PANIC_STARTED.store(0, Ordering::SeqCst);
        let router = router();
        let mut agents = Vec::new();
        for index in 0..=MAX_CONCURRENT_AUTO_ROUNDS {
            let agent = ready_agent(&router, &format!("goal-panic-{index}")).await;
            set_goal(&router, &agent, definition("panicking goal")).await;
            agents.push((agent, high_water(&router).await));
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(
            router.clone(),
            panicking_runner,
            shutdown_rx,
        ));
        // Republish until each panicking check started: the driver may not
        // have subscribed yet, and a fenced Goal never reruns its check.
        tokio::time::timeout(Duration::from_secs(10), async {
            while PANIC_STARTED.load(Ordering::SeqCst) < MAX_CONCURRENT_AUTO_ROUNDS {
                for (agent, key) in &agents[..MAX_CONCURRENT_AUTO_ROUNDS] {
                    router.inner.bus.publish(&[idle_event(agent, 1, *key)]);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("every capped round panics");
        // Without freeing panicked slots the cap stays full and this round
        // is dropped forever.
        let (last, key) = &agents[MAX_CONCURRENT_AUTO_ROUNDS];
        tokio::time::timeout(Duration::from_secs(10), async {
            while stored_goal(&router, last).await.counters.rounds != 1 {
                router.inner.bus.publish(&[idle_event(last, 1, *key)]);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("a round after panicked rounds still runs");
        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver joins")
            .unwrap()
            .unwrap();
    }

    static FOLLOW_STARTED: AtomicUsize = AtomicUsize::new(0);
    static FOLLOW_RELEASE: OnceLock<Notify> = OnceLock::new();

    fn follow_runner(
        _: TokioCommand,
        _: Vec<u8>,
        _: Duration,
        _: Arc<SecretPolicy>,
        _: watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>> {
        Box::pin(async {
            if FOLLOW_STARTED.fetch_add(1, Ordering::SeqCst) == 0 {
                FOLLOW_RELEASE.get_or_init(Notify::new).notified().await;
            }
            completed_outcome(0, b"all green", true)
        })
    }

    #[tokio::test]
    async fn goal_driver_follows_a_newer_key_after_a_stale_round() {
        FOLLOW_STARTED.store(0, Ordering::SeqCst);
        let router = router();
        let agent = ready_agent(&router, "goal-follow").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let first_key = high_water(&router).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(router.clone(), follow_runner, shutdown_rx));
        publish_until(&router, &agent, 1, first_key, &FOLLOW_STARTED).await;
        // A newer idle event lands while the first round runs: the first
        // key is doomed and the follow-up must round on the fresh key.
        let send_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut request = SubmitCommand::new(send_id.clone(), "send");
            request.target = Some(agent.clone());
            request.expected_generation = Some(1);
            request.args = json!({"text": "operator input"});
            store.submit("owner", request, now_ms()).unwrap();
        }
        complete_send(&router, &agent, &send_id, "turn-follow-1").await;
        let fresh_key = high_water(&router).await;
        assert!(fresh_key > first_key);
        router
            .inner
            .bus
            .publish(&[idle_event(&agent, 1, fresh_key)]);
        FOLLOW_RELEASE.get_or_init(Notify::new).notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let view = stored_goal(&router, &agent).await;
                if view.counters.rounds == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follow-up rounds on the fresh key");
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Completed);
        assert_eq!(view.last_round_key, Some(format!("idle:{fresh_key}")));
        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver joins")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn goal_driver_cancels_its_round_on_shutdown() {
        CANCEL_STARTED.store(0, Ordering::SeqCst);
        let router = router();
        let agent = ready_agent(&router, "goal-shutdown").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(
            router.clone(),
            cancelling_runner,
            shutdown_rx,
        ));
        publish_until(&router, &agent, 1, key, &CANCEL_STARTED).await;
        shutdown_tx.send_replace(true);
        // Shutdown never waits for the check itself: the driver cancels
        // it through custody cancellation and exits promptly.
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver exits without waiting for the check")
            .unwrap()
            .unwrap();
        let view = stored_goal(&router, &agent).await;
        let marker = view.round_marker.expect("cancelled round stays visible");
        assert_eq!(marker.state, eliot_core::api::GoalMarkerState::Unknown);
        assert_eq!(marker.round_key, format!("idle:{key}"));
        // The cancelled key is burned: never rerun.
        let error = router
            .inner
            .store
            .lock()
            .await
            .goal_round_probe(&agent, "eliot", GoalRoundKey::Idle(key), now_ms())
            .unwrap_err();
        assert_eq!(error.code, "stale_round");
    }

    #[tokio::test]
    async fn goal_round_incomplete_failed_check_does_not_prove_no_progress() {
        let router = router();
        let agent = ready_agent(&router, "goal-incomplete-progress").await;
        set_goal(
            &router,
            &agent,
            json!({
                "text": "keep green",
                "check": ["check-goal"],
                "max_rounds": 6,
                "max_without_progress": 1,
            }),
        )
        .await;
        let key = high_water(&router).await;
        let first_id = ulid::Ulid::new().to_string();
        let first = router
            .goal_round_with(
                &json!({"id":first_id,"agent":agent,"mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                failing_runner,
            )
            .await
            .unwrap();
        assert_eq!(first["outcome"], "continued");
        complete_send(&router, &agent, &first_id, "turn-incomplete-progress").await;
        let key = high_water(&router).await;
        let second_id = ulid::Ulid::new().to_string();
        let second = router
            .goal_round_with(
                &json!({"id":second_id,"agent":agent,"mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                incomplete_failing_runner,
            )
            .await
            .unwrap();
        assert_eq!(second["outcome"], "continued");
        assert_eq!(second["continuation"]["command_id"], second_id);
        assert_eq!(second["view"]["status"], "active");
        assert_eq!(second["view"]["counters"]["rounds"], 2);
        assert_eq!(second["view"]["counters"]["consecutive_same_output"], 0);
        assert_eq!(second["view"]["last_round_key"], format!("idle:{key}"));
    }

    #[tokio::test]
    async fn goal_round_blocks_on_a_no_progress_streak() {
        let router = router();
        let agent = ready_agent(&router, "goal-no-progress").await;
        set_goal(
            &router,
            &agent,
            json!({
                "text": "keep green",
                "check": ["check-goal"],
                "max_rounds": 6,
                "max_without_progress": 1,
            }),
        )
        .await;
        let key = high_water(&router).await;
        let first_id = ulid::Ulid::new().to_string();
        let first = router
            .goal_round_with(
                &json!({"id":first_id,"agent":agent,"mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                failing_runner,
            )
            .await
            .unwrap();
        assert_eq!(first["outcome"], "continued");
        complete_send(&router, &agent, &first_id, "turn-np-1").await;
        let key = high_water(&router).await;
        let second = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                failing_runner,
            )
            .await
            .unwrap();
        assert_eq!(second["outcome"], "blocked");
        assert_eq!(second["reason"], "no_progress");
        assert_eq!(second["view"]["status"], "blocked");
    }

    #[tokio::test]
    async fn goal_set_reconciles_an_idle_agent_without_a_bus_event() {
        let router = router();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(router.clone(), failing_runner, shutdown_rx));
        let agent = ready_agent(&router, "goal-reconcile-set").await;
        let key = high_water(&router).await;
        // Retry the identical set until the driver subscribes: every retry
        // re-notifies, but the single-flight marker keeps it to one round.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if stored_goal(&router, &agent).await.counters.rounds == 1 {
                    break;
                }
                set_goal(&router, &agent, definition("keep green")).await;
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("set reconciles an idle agent");
        // Repeated identical sets cannot schedule another round on the
        // consumed subject: the revision is unchanged and the key stays
        // burned, so the sweep keeps waiting for a real event.
        set_goal(&router, &agent, definition("keep green")).await;
        set_goal(&router, &agent, definition("keep green")).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Active);
        assert_eq!(view.counters.rounds, 1);
        assert_eq!(view.last_round_key, Some(format!("idle:{key}")));
        assert!(view.round_marker.is_none());
        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver joins")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn goal_resume_reconciles_a_paused_goal_without_a_bus_event() {
        let router = router();
        let agent = ready_agent(&router, "goal-reconcile-resume").await;
        set_goal(&router, &agent, definition("keep green")).await;
        router
            .goal_pause(
                &json!({"id": ulid::Ulid::new().to_string(), "agent":agent, "mode":"eliot", "reason":"waiting"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let key = high_water(&router).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(router.clone(), passing_runner, shutdown_rx));
        // Retry the resume until the driver subscribes; the first resume
        // activates the Goal and every retry re-notifies idempotently.
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if stored_goal(&router, &agent).await.counters.rounds == 1 {
                    break;
                }
                router
                    .goal_resume(
                        &json!({"id": ulid::Ulid::new().to_string(), "agent":agent, "mode":"eliot"}),
                        &Principal::Owner,
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("resume reconciles a paused goal");
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.status, eliot_core::api::GoalStatus::Completed);
        assert_eq!(view.counters.rounds, 1);
        assert_eq!(view.last_round_key, Some(format!("idle:{key}")));
        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver joins")
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn goal_reconcile_and_bus_event_schedule_exactly_one_round() {
        let router = router();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let driver = tokio::spawn(run_goal_driver(router.clone(), passing_runner, shutdown_rx));
        let agent = ready_agent(&router, "goal-reconcile-race").await;
        let key = high_water(&router).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if stored_goal(&router, &agent).await.counters.rounds == 1 {
                    break;
                }
                set_goal(&router, &agent, definition("keep green")).await;
                router.inner.bus.publish(&[idle_event(&agent, 1, key)]);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("one round wins the set/event race");
        // Settle: further sweeps and redelivered events must not schedule
        // another round on the consumed subject.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let view = stored_goal(&router, &agent).await;
        assert_eq!(view.counters.rounds, 1);
        assert_eq!(view.last_round_key, Some(format!("idle:{key}")));
        assert!(view.round_marker.is_none());
        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), driver)
            .await
            .expect("driver joins")
            .unwrap()
            .unwrap();
    }
}
