//! Owner-managed Eliot-mode Goals and their explicit rounds.
//!
//! A round probes the Goal and its Agent, runs the frozen check command
//! through the shared script/process custody outside the Store lock, then
//! commits one outcome only if generation, revision and round still match.
//! Unknown check outcomes commit nothing and never authorize replay.
use super::{Principal, Router, contains_known_secret_including_keys, require_owner};
use crate::{
    common::now_ms,
    scripts::process::{ProcessCapture, ProcessFailure, ProcessOutcome, run_owned_process},
};
use eliot_core::{
    AgentState, Error, Result, SecretPolicy,
    api::{
        GoalClearRequest, GoalGetRequest, GoalMode, GoalPauseRequest, GoalResumeRequest,
        GoalRoundOutcome, GoalRoundRequest, GoalRoundResponse, GoalSetRequest,
        MAX_GOAL_OBSERVATION_BYTES,
    },
    limits::MAX_FRAME_BYTES,
};
use eliot_plugin::sync::LockExt;
use eliot_store::{GoalCheckResult, GoalCommitOutcome, GoalContinuation, GoalRoundCommit};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::{process::Command as TokioCommand, sync::watch};

/// Spawn one already-resolved check command. Production passes
/// [`real_goal_check`]; tests inject a fake outcome. The future is boxed so
/// the seam stays a plain function pointer.
type GoalCheckRunner = fn(
    TokioCommand,
    Vec<u8>,
    Duration,
    Arc<SecretPolicy>,
    watch::Receiver<bool>,
) -> Pin<Box<dyn Future<Output = ProcessOutcome> + Send>>;

fn real_goal_check(
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
    if contains_known_secret_including_keys(params, secrets) {
        return Err(Error::new(
            "secret_in_payload",
            format!("{method} request contains a known credential"),
        ));
    }
    let request: T = serde_json::from_value(params.clone())
        .map_err(|_| Error::new("invalid", format!("{method} params are invalid")))?;
    validate(&request)?;
    Ok(request)
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
            &request.definition,
            now_ms(),
        )?;
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
            now_ms(),
        )?;
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
        let probe = {
            self.inner.store.lock().await.goal_round_probe(
                &request.agent,
                request.mode.as_str(),
                request.idle_seq,
                now_ms(),
            )?
        };
        goal_round_gates(&probe, request.idle_seq)?;
        let definition = probe.view.definition.clone();
        let turns_used = probe
            .turn_count
            .saturating_sub(probe.view.counters.turns_at_set);
        // A turn budget spent outside any round blocks without running the
        // check. Round and no-progress budgets are decided after the check.
        if probe
            .view
            .definition
            .max_turns
            .is_some_and(|max| turns_used >= max)
        {
            return self.commit_goal_block(&request, &probe, "turn_limit").await;
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
        let stdin = serde_json::to_vec(&json!({
            "agent": request.agent,
            "generation": probe.view.generation,
            "revision": probe.view.revision,
            "round_key": format!("idle:{}", request.idle_seq),
        }))
        .map_err(|_| Error::new("internal", "goal check input could not be encoded"))?;
        if stdin.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "goal check input exceeds 1 MiB",
            ));
        }
        let timeout = Duration::from_millis(definition.check_timeout_ms.max(0) as u64);
        let outcome = run_check(
            command,
            stdin,
            timeout,
            Arc::clone(&policy),
            self.run_cancellation_receiver(),
        )
        .await;
        let check = goal_check_result(outcome, &policy)?;
        if check.exit_code != Some(0) {
            let text = goal_continuation_text(&definition.text, &definition.check, &check, &policy);
            let commit = GoalRoundCommit {
                agent: request.agent.clone(),
                mode: request.mode.as_str().to_owned(),
                expected_generation: probe.view.generation,
                expected_revision: probe.view.revision,
                round_key: request.idle_seq,
                check: Some(check),
                blocked_reason: None,
                continuation: Some(GoalContinuation {
                    id: request.id.clone(),
                    text,
                }),
            };
            return self.commit_goal_round(&request, commit).await;
        }
        let commit = GoalRoundCommit {
            agent: request.agent.clone(),
            mode: request.mode.as_str().to_owned(),
            expected_generation: probe.view.generation,
            expected_revision: probe.view.revision,
            round_key: request.idle_seq,
            check: Some(check),
            blocked_reason: None,
            continuation: None,
        };
        self.commit_goal_round(&request, commit).await
    }

    async fn commit_goal_block(
        &self,
        request: &GoalRoundRequest,
        probe: &eliot_store::GoalRoundProbe,
        reason: &str,
    ) -> Result<Value> {
        let commit = GoalRoundCommit {
            agent: request.agent.clone(),
            mode: request.mode.as_str().to_owned(),
            expected_generation: probe.view.generation,
            expected_revision: probe.view.revision,
            round_key: request.idle_seq,
            check: None,
            blocked_reason: Some(reason.to_owned()),
            continuation: None,
        };
        self.commit_goal_round(request, commit).await
    }

    async fn commit_goal_round(
        &self,
        request: &GoalRoundRequest,
        commit: GoalRoundCommit,
    ) -> Result<Value> {
        let result = {
            let mut store = self.inner.store.lock().await;
            let result = store.goal_commit_round(&commit, now_ms())?;
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
        let (outcome, reason, command_id) = match &result.outcome {
            GoalCommitOutcome::Completed => (GoalRoundOutcome::Completed, None, None),
            GoalCommitOutcome::Blocked { reason } => {
                (GoalRoundOutcome::Blocked, Some(reason.clone()), None)
            }
            GoalCommitOutcome::Continued { command_id } => {
                (GoalRoundOutcome::Continued, None, Some(command_id.clone()))
            }
        };
        let response = GoalRoundResponse {
            outcome,
            reason,
            command_id,
            continuation: result.admission.as_ref().map(|admission| {
                super::submit_result(&admission.command, admission.linked_command.as_ref())
            }),
            view: result.view,
        };
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "goal.round result could not be encoded"))
    }
}

/// Refuse a round unless the Agent is cleanly idle at the round key.
fn goal_round_gates(probe: &eliot_store::GoalRoundProbe, idle_seq: i64) -> Result<()> {
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
    if probe.max_seq != idle_seq {
        return Err(Error::new(
            "stale_round",
            "round key is not the latest Agent event",
        ));
    }
    Ok(())
}

/// Interpret one owned check completion. Only a definite direct exit with
/// proven custody is a check result; everything else is `unknown` and commits
/// nothing.
fn goal_check_result(outcome: ProcessOutcome, policy: &SecretPolicy) -> Result<GoalCheckResult> {
    match outcome {
        ProcessOutcome::Completed(capture) => {
            if !custody_proven_for_capture(&capture) {
                return Err(Error::new("unknown", "goal check custody is unproven"));
            }
            let digest = check_output_digest(&capture.stdout);
            let text = String::from_utf8_lossy(&capture.stdout);
            let observation = if text.is_empty() {
                None
            } else {
                Some(policy.redact_text(truncate_observation(text.as_ref())))
            };
            Ok(GoalCheckResult {
                exit_code: capture.status.code(),
                output_complete: capture.stdout_complete,
                output_digest: Some(digest),
                observation,
            })
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
    use eliot_core::{NativeEvent, SourceBatch, StderrSnapshot, SubmitCommand};
    use std::path::PathBuf;
    use tokio::sync::Mutex as AsyncMutex;

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
                &json!({"agent":agent, "mode":"eliot", "definition":definition}),
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

    fn completed_outcome(code: i32, stdout: &[u8], complete: bool) -> ProcessOutcome {
        ProcessOutcome::Completed(ProcessCapture {
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
        })
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
    canned_runner!(failing_runner, completed_outcome(1, b"red", true));
    canned_runner!(
        unknown_runner,
        ProcessOutcome::Unknown {
            reason: crate::scripts::process::ProcessUncertainty::Exit,
            quiescent: true,
        }
    );
    canned_runner!(
        timeout_runner,
        ProcessOutcome::Failed {
            reason: crate::scripts::process::ProcessFailure::Timeout,
            capture: None,
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
                &json!({"agent":agent, "mode":"eliot", "reason":"waiting"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(paused["revision"], 2);
        assert_eq!(paused["status"], "paused");
        let resumed = router
            .goal_resume(&json!({"agent":agent, "mode":"eliot"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(resumed["revision"], 3);
        let cleared = router
            .goal_clear(&json!({"agent":agent, "mode":"eliot"}), &Principal::Owner)
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
                &json!({"agent":agent, "mode":"native", "definition":definition("x")}),
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
    async fn goal_round_completes_on_a_passing_check() {
        let router = router();
        let agent = ready_agent(&router, "goal-pass").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        let response = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                passing_runner,
            )
            .await
            .unwrap();
        assert_eq!(response["outcome"], "completed");
        assert_eq!(response["view"]["status"], "completed");
        assert_eq!(response["view"]["counters"]["rounds"], 1);
        assert_eq!(response["view"]["last_round_key"], format!("idle:{key}"));
    }

    #[tokio::test]
    async fn goal_round_continues_with_one_ordinary_send() {
        let router = router();
        let agent = ready_agent(&router, "goal-continue").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        let id = ulid::Ulid::new().to_string();
        let response = router
            .goal_round_with(
                &json!({"id":id,"agent":agent,"mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                failing_runner,
            )
            .await
            .unwrap();
        assert_eq!(response["outcome"], "continued");
        assert_eq!(response["continuation"]["command_id"], id);
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
        assert!(send.args["text"].as_str().unwrap().contains("keep green"));
        assert!(send.args["text"].as_str().unwrap().contains("red"));
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
        let agent = ready_agent(&router, "goal-unknown").await;
        set_goal(&router, &agent, definition("keep green")).await;
        let key = high_water(&router).await;
        for runner in [
            unknown_runner as GoalCheckRunner,
            timeout_runner as GoalCheckRunner,
        ] {
            let error = router
                .goal_round_with(
                    &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                        "mode":"eliot","idle_seq":key}),
                    &Principal::Owner,
                    runner,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code, "unknown");
        }
        let error = router
            .goal_round_with(
                &json!({"id":ulid::Ulid::new().to_string(),"agent":agent,
                    "mode":"eliot","idle_seq":key}),
                &Principal::Owner,
                cancelled_runner,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "unavailable");
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
    }
}
