//! Shared submission, wait, exit-code and output plumbing for the
//! CLI command families.
use eliot_core::api::{CommandGetRequest, CommandSubmitResponse, TurnGetRequest};
use eliot_core::{
    Command as CoreCommand, CommandState, Error as CoreError, SubmitCommand, Turn as CoreTurn,
    TurnOutcome, TurnState,
};
use serde_json::Value;
use std::{path::Path, time::Duration};
use tokio::time::{Instant, sleep, timeout_at};
use ulid::Ulid;

pub(crate) const DEFAULT_WAIT_SECONDS: u64 = 30;
pub(crate) const PAGE_SIZE: usize = 100;
pub(crate) const MAX_TEXT_BYTES: usize = 512 * 1024;
pub(crate) const MAX_ANSWERS_FILE_BYTES: usize = 512 * 1024;
pub(crate) const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(200);

pub(crate) fn parse_goal_json(
    input: Option<String>,
) -> Result<Option<serde_json::Map<String, Value>>, Failure> {
    let Some(input) = input else {
        return Ok(None);
    };
    let value: Value = serde_json::from_str(&input)
        .map_err(|_| Failure::new(2, "--goal-json must contain valid JSON"))?;
    match value {
        Value::Object(goal) => Ok(Some(goal)),
        _ => Err(Failure::new(2, "--goal-json must contain a JSON object")),
    }
}

pub(crate) fn validate_no_policy(reason: Option<String>) -> Result<Option<String>, Failure> {
    if reason
        .as_ref()
        .is_some_and(|reason| !eliot_core::api::valid_project_policy_opt_out_reason(reason))
    {
        return Err(Failure::new(
            2,
            format!(
                "--no-policy needs a nonblank, one-line reason of at most {} bytes without control, format, bidi, or invisible zero-width characters",
                eliot_core::api::PROJECT_POLICY_OPT_OUT_REASON_MAX_BYTES
            ),
        ));
    }
    Ok(reason)
}

#[derive(Debug)]
pub(crate) struct Failure {
    pub(crate) exit_code: u8,
    pub(crate) message: String,
    pub(crate) code: Option<String>,
    pub(crate) diagnostics: Vec<eliot_core::StderrDiagnostic>,
}

impl Failure {
    pub(crate) fn new(exit_code: u8, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
            code: None,
            diagnostics: Vec::new(),
        }
    }

    pub(crate) fn from_core(error: CoreError, mutation: bool) -> Self {
        let exit_code = if mutation {
            Self::mutation_exit_code(&error.code)
        } else if error.code == "invalid" {
            2
        } else {
            1
        };
        let code = error.code.clone();
        let mut failure = Self::new(exit_code, error.to_string());
        failure.code = Some(code);
        failure.diagnostics = *error.diagnostics;
        eliot_core::bound_stderr_diagnostics(
            &mut failure.diagnostics,
            eliot_core::MAX_STDERR_DIAGNOSTIC_BYTES,
        );
        failure
    }

    /// Exit code for an RPC error on a path where a write may have occurred.
    /// Invalid input exits 2. Exit 3 covers other pre-admission rejections through the
    /// one Core predicate (shared with the daemon, which marks post-write
    /// validation and framing failures `unknown` at their source);
    /// provider/domain failures exit 1; anything else is an uncertain
    /// effect (exit 4).
    pub(crate) fn mutation_exit_code(code: &str) -> u8 {
        if code == "invalid" {
            return 2;
        }
        if code == "failed" {
            return 1;
        }
        if eliot_core::is_deterministic_pre_admission_rejection(code) {
            return 3;
        }
        4
    }

    /// Phase-specific constructor for request validation and RPC errors on a
    /// path that may write: rejections exit 3, failures exit 1, and anything
    /// else is an uncertain effect (exit 4).
    pub(crate) fn for_mutation(error: CoreError) -> Self {
        Self::from_core(error, true)
    }

    /// Phase-specific constructor for request validation and RPC errors on a
    /// read-only path, where no write could have occurred.
    pub(crate) fn for_read(error: CoreError) -> Self {
        Self::from_core(error, false)
    }

    /// A local validation of an already-received response failed. The reply
    /// arrived after the request was sent, so a failed readback can never
    /// prove that no write occurred: the effect stays unknown (exit 4) even
    /// when the validator reports `invalid`, which alone only rejects a
    /// pre-effect request. The validator code and diagnostics are preserved.
    pub(crate) fn invalid_response(error: CoreError) -> Self {
        let mut failure = Self::new(4, error.to_string());
        failure.code = Some(error.code.clone());
        failure.diagnostics = *error.diagnostics;
        eliot_core::bound_stderr_diagnostics(
            &mut failure.diagnostics,
            eliot_core::MAX_STDERR_DIAGNOSTIC_BYTES,
        );
        failure
    }

    pub(crate) fn timeout() -> Self {
        Self::new(5, "wait timed out; the admitted command was not cancelled")
    }

    /// Inbox wait expiry before any read completed. No command was admitted,
    /// so the generic admitted-command timeout does not apply.
    pub(crate) fn inbox_timeout() -> Self {
        Self::new(5, "inbox wait timed out before a read completed")
    }

    pub(crate) fn new_from_message(message: String) -> Self {
        Self::new(1, message)
    }

    pub(crate) fn from_command(
        exit_code: u8,
        message: impl Into<String>,
        command: &CoreCommand,
    ) -> Self {
        let mut failure = Self::new(exit_code, message);
        if let Some(outcome) = command.outcome.as_ref() {
            let outcome = if command.verb == "worktree.remove" {
                outcome.get("result").unwrap_or(outcome)
            } else {
                outcome
            };
            let code = outcome
                .get("code")
                .or_else(|| outcome.get("reason"))
                .and_then(Value::as_str);
            let detail = outcome.get("message").and_then(Value::as_str);
            if let Some(code) = code {
                failure.code = Some(code.to_owned());
                failure.message.push_str(&format!(": {code}"));
            }
            if let Some(detail) = detail.filter(|detail| Some(*detail) != code) {
                failure.message.push_str(&format!(": {detail}"));
            }
        }
        if let Some(value) = command
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("diagnostics"))
        {
            failure.diagnostics = serde_json::from_value(value.clone()).unwrap_or_default();
            eliot_core::bound_stderr_diagnostics(
                &mut failure.diagnostics,
                eliot_core::MAX_STDERR_DIAGNOSTIC_BYTES,
            );
        }
        failure
    }

    pub(crate) fn readback(error: CoreError) -> Self {
        let mut failure = Self::for_read(error);
        failure.exit_code = 4;
        failure.message = format!("cannot establish command outcome: {}", failure.message);
        failure
    }
}

macro_rules! read_and_print {
    ($data_dir:expr, $json_output:expr, $method:literal, $params:expr, $response:ty) => {{
        let params = $params;
        params.validate().map_err(Failure::for_read)?;
        let response: $response = eliot_daemon::client_call_typed($data_dir, $method, &params)
            .await
            .map_err(Failure::for_read)?;
        params
            .validate_response(&response)
            .map_err(Failure::invalid_response)?;
        let result = serde_json::to_value(response)
            .map_err(|_| Failure::new(4, concat!($method, " response is invalid")))?;
        print_value(&result, $json_output);
        Ok(0)
    }};
}
pub(crate) use read_and_print;

pub(crate) fn mutation_id(supplied_id: Option<String>) -> Result<String, Failure> {
    let id = match supplied_id {
        Some(id) => {
            Ulid::from_string(&id).map_err(|_| Failure::new(2, "--id must be a valid ULID"))?;
            id.to_ascii_uppercase()
        }
        None => Ulid::new().to_string(),
    };
    eprintln!("id={id}");
    Ok(id)
}

pub(crate) fn submit_request(
    supplied_id: Option<String>,
    target: Option<String>,
    verb: &str,
    args: Value,
) -> Result<SubmitCommand, Failure> {
    let mut request = SubmitCommand::new(mutation_id(supplied_id)?, verb);
    request.target = target;
    request.args = args;
    Ok(request)
}

pub(crate) async fn submit_command(
    data_dir: &Path,
    params: &SubmitCommand,
) -> Result<CommandSubmitResponse, Failure> {
    params.validate().map_err(Failure::for_mutation)?;
    let response: CommandSubmitResponse =
        eliot_daemon::client_call_typed(data_dir, "command.submit", params)
            .await
            .map_err(Failure::for_mutation)?;
    params
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
    Ok(response)
}

pub(crate) async fn submit_and_print(
    data_dir: &Path,
    params: &SubmitCommand,
    json_output: bool,
) -> Result<u8, Failure> {
    let result = submit_command(data_dir, params).await?;
    print_value(&submit_value(&result)?, json_output);
    Ok(response_exit_code(result.state))
}

pub(crate) fn submit_value(response: &CommandSubmitResponse) -> Result<Value, Failure> {
    serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "command.submit response is invalid"))
}

pub(crate) fn validate_limit(limit: usize) -> Result<(), Failure> {
    if !(1..=1_000).contains(&limit) {
        return Err(Failure::new(2, "--limit must be between 1 and 1000"));
    }
    Ok(())
}

pub(crate) fn wait_deadline(seconds: u64) -> Result<Instant, Failure> {
    if seconds == 0 {
        return Err(Failure::new(
            2,
            "--timeout-seconds must be greater than zero",
        ));
    }
    Instant::now()
        .checked_add(Duration::from_secs(seconds))
        .ok_or_else(|| Failure::new(2, "--timeout-seconds is too large"))
}

pub(crate) async fn wait_call<T>(
    call: impl std::future::Future<Output = eliot_core::Result<T>>,
    deadline: Instant,
) -> Result<T, Failure> {
    match timeout_at(deadline, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Failure::readback(error)),
        Err(_) => Err(Failure::timeout()),
    }
}

/// Wait server-side until the command is terminal and return its record. A
/// command bound to a turn completes only when that turn does. The deadline
/// is a timeout; a known terminal error keeps its contract exit code.
pub(crate) async fn wait_for_command(
    data_dir: &Path,
    command_id: &str,
    deadline: Instant,
) -> Result<CoreCommand, Failure> {
    let params = CommandGetRequest::new(command_id);
    let command = wait_call(
        eliot_daemon::wait_command(data_dir, &params, deadline),
        deadline,
    )
    .await?
    .ok_or_else(Failure::timeout)?;
    command_state_result(&command)?;
    Ok(command)
}

pub(crate) async fn wait_for_turn(
    data_dir: &Path,
    command_id: &str,
    deadline: Instant,
) -> Result<CoreTurn, Failure> {
    // The wait ends when the command fails or its bound turn settles.
    let command = wait_for_command(data_dir, command_id, deadline).await?;
    let (Some(agent), Some(generation), Some(turn_id)) =
        (command.target, command.generation, command.turn_id)
    else {
        return Err(Failure::new(4, "command readback binding is invalid"));
    };
    let params = TurnGetRequest::new(agent, generation, turn_id);
    params.validate().map_err(Failure::readback)?;
    let turn: CoreTurn = wait_call(
        eliot_daemon::client_call_typed(data_dir, "turn.get", &params),
        deadline,
    )
    .await?;
    params.validate_response(&turn).map_err(Failure::readback)?;
    match turn.state {
        TurnState::Completed => Ok(turn),
        TurnState::Unknown => Err(Failure::new(4, "turn outcome is unknown")),
        TurnState::Running => Err(Failure::timeout()),
    }
}

/// Wait for a launch/resume linked SEND turn. When the linked input is
/// cancelled because its parent start failed, report the parent launch
/// failure rather than only the cancellation.
pub(crate) async fn wait_for_linked_turn(
    data_dir: &Path,
    root_id: &str,
    linked_id: &str,
    deadline: Instant,
) -> Result<CoreTurn, Failure> {
    match wait_for_turn(data_dir, linked_id, deadline).await {
        Ok(turn) => Ok(turn),
        Err(linked) if linked.exit_code == INTERRUPTED_EXIT_CODE => {
            let params = CommandGetRequest::new(root_id);
            if params.validate().is_err() {
                return Err(linked);
            }
            // Best-effort diagnostic within the remaining budget: the final
            // root readback must not outlive the wait deadline, and the
            // linked outcome stays authoritative when the budget is gone or
            // the readback fails.
            let root: CoreCommand = match timeout_at(
                deadline,
                eliot_daemon::client_call_typed(data_dir, "command.get", &params),
            )
            .await
            {
                Ok(Ok(command)) => command,
                _ => return Err(linked),
            };
            if params.validate_response(&root).is_err() {
                return Err(linked);
            }
            match command_state_result(&root) {
                Ok(_) => Err(linked),
                Err(root_failure) if root_failure.exit_code == INTERRUPTED_EXIT_CODE => Err(linked),
                Err(root_failure) => Err(root_failure),
            }
        }
        Err(other) => Err(other),
    }
}

/// True means applied; false means still open. Known terminal errors retain their
/// contract exit code and never imply that the native effect can be retried.
/// A cancelled command is an interrupted wait (exit 7), never success.
pub(crate) fn command_state_result(command: &CoreCommand) -> Result<bool, Failure> {
    match command.state {
        CommandState::Applied => Ok(true),
        CommandState::Rejected => Err(Failure::from_command(
            3,
            "command was rejected before native I/O",
            command,
        )),
        CommandState::Failed => Err(Failure::from_command(1, "command failed", command)),
        CommandState::Unknown => Err(Failure::from_command(
            4,
            "command outcome is unknown",
            command,
        )),
        CommandState::Cancelled => Err(Failure::from_command(
            INTERRUPTED_EXIT_CODE,
            "command was cancelled",
            command,
        )),
        CommandState::Accepted | CommandState::Queued | CommandState::Dispatched => Ok(false),
    }
}

/// Exit code for an interrupted or cancelled wait: the waited task ended
/// without success but not as a definite failure. A successful `interrupt`
/// command itself still exits 0; waiting on the interrupted original does not.
pub(crate) const INTERRUPTED_EXIT_CODE: u8 = 7;

pub(crate) async fn sleep_before_deadline(deadline: Instant) -> Result<(), Failure> {
    #[expect(
        clippy::disallowed_methods,
        reason = "The CLI uses a bounded wait between readback polls"
    )]
    let poll_interval = sleep(WAIT_POLL_INTERVAL);
    timeout_at(deadline, poll_interval)
        .await
        .map_err(|_| Failure::timeout())
}

pub(crate) fn response_exit_code(state: CommandState) -> u8 {
    match state {
        CommandState::Accepted | CommandState::Queued | CommandState::Applied => 0,
        CommandState::Rejected => 3,
        CommandState::Failed => 1,
        CommandState::Cancelled => INTERRUPTED_EXIT_CODE,
        CommandState::Unknown => 4,
        CommandState::Dispatched => 6,
    }
}

/// Exit code for one settled prompted wait. Lifecycle (`TurnState::Completed`)
/// and outcome stay separate facts: only a positively recognized `success`
/// exits 0. Supported native failures exit 1, interruption/cancellation
/// exits 7, and unknown or unclassified terminal statuses exit 4. The stored
/// outcome is already harness-aware for rows written before canonical
/// outcomes existed; the inline fallback classifies generically and never as
/// success.
pub(crate) fn turn_exit_code(turn: &CoreTurn) -> u8 {
    match turn.state {
        TurnState::Completed => {
            let outcome = turn
                .outcome
                .unwrap_or_else(|| TurnOutcome::classify_legacy_status(turn.status.as_deref()));
            match outcome {
                TurnOutcome::Success => 0,
                TurnOutcome::Failure => 1,
                TurnOutcome::Interrupted => INTERRUPTED_EXIT_CODE,
                TurnOutcome::Unknown => 4,
            }
        }
        TurnState::Unknown => 4,
        TurnState::Running => 6,
    }
}

pub(crate) fn command_value(command: CoreCommand) -> Result<Value, Failure> {
    serde_json::to_value(command).map_err(|_| Failure::new(4, "command readback is unavailable"))
}

pub(crate) fn turn_value(turn: CoreTurn) -> Result<Value, Failure> {
    serde_json::to_value(turn).map_err(|_| Failure::new(4, "turn readback is unavailable"))
}

pub(crate) fn print_value(value: &Value, json_output: bool) {
    if json_output {
        println!("{value}");
    } else if let Ok(rendered) = serde_json::to_string_pretty(value) {
        println!("{rendered}");
    } else {
        println!("{value}");
    }
}

pub(crate) fn print_value_or_human(value: &Value, json_output: bool, human: impl FnOnce()) {
    if json_output {
        println!("{value}");
    } else {
        human();
    }
}
