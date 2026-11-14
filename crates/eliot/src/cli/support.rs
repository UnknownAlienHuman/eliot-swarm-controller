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
        let exit_code = if !mutation {
            1
        } else {
            match error.code.as_str() {
                "rejected"
                | "not_allowed"
                | "bad_state"
                | "stale_generation"
                | "stale_round"
                | "invalid"
                | "unsupported"
                | "not_found"
                | "busy"
                | "capacity_unavailable"
                | "provider_held"
                | "frame_too_large"
                | "secret_in_payload"
                | "mailbox_full"
                | "fanout_limit"
                | "version_conflict"
                | "claim_limit"
                | "id_conflict"
                | "id_expired"
                | "profile_error" => 3,
                "failed" => 1,
                _ => 4,
            }
        };
        let code = error.code.clone();
        let mut failure = Self::new(exit_code, error.to_string());
        failure.code = Some(code);
        failure.diagnostics = error.diagnostics;
        eliot_core::bound_stderr_diagnostics(
            &mut failure.diagnostics,
            eliot_core::MAX_STDERR_DIAGNOSTIC_BYTES,
        );
        failure
    }

    pub(crate) fn timeout() -> Self {
        Self::new(5, "wait timed out; the admitted command was not cancelled")
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
        let mut failure = Self::from_core(error, false);
        failure.exit_code = 4;
        failure.message = format!("cannot establish command outcome: {}", failure.message);
        failure
    }
}

macro_rules! read_and_print {
    ($data_dir:expr, $json_output:expr, $method:literal, $params:expr, $response:ty) => {{
        let params = $params;
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: $response = eliot_daemon::client_call_typed($data_dir, $method, &params)
            .await
            .map_err(|error| Failure::from_core(error, false))?;
        params
            .validate_response(&response)
            .map_err(|error| Failure::from_core(error, false))?;
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
    params
        .validate()
        .map_err(|error| Failure::from_core(error, true))?;
    let response: CommandSubmitResponse =
        eliot_daemon::client_call_typed(data_dir, "command.submit", params)
            .await
            .map_err(|error| Failure::from_core(error, true))?;
    params
        .validate_response(&response)
        .map_err(|_| Failure::new(4, "command.submit response is invalid"))?;
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

pub(crate) async fn wait_for_command(
    data_dir: &Path,
    command_id: &str,
    deadline: Instant,
) -> Result<CoreCommand, Failure> {
    let params = CommandGetRequest::new(command_id);
    params.validate().map_err(Failure::readback)?;
    loop {
        let command: CoreCommand = wait_call(
            eliot_daemon::client_call_typed(data_dir, "command.get", &params),
            deadline,
        )
        .await?;
        params
            .validate_response(&command)
            .map_err(Failure::readback)?;
        if command_state_result(&command)? {
            return Ok(command);
        }
        sleep_before_deadline(deadline).await?;
    }
}

pub(crate) async fn wait_for_turn(
    data_dir: &Path,
    command_id: &str,
    deadline: Instant,
) -> Result<CoreTurn, Failure> {
    let command_params = CommandGetRequest::new(command_id);
    command_params.validate().map_err(Failure::readback)?;
    let (agent, generation, turn_id) = loop {
        let command: CoreCommand = wait_call(
            eliot_daemon::client_call_typed(data_dir, "command.get", &command_params),
            deadline,
        )
        .await?;
        command_params
            .validate_response(&command)
            .map_err(Failure::readback)?;
        command_state_result(&command)?;
        if let Some(turn_id) = command.turn_id {
            let agent = command
                .target
                .ok_or_else(|| Failure::new(4, "command readback binding is invalid"))?;
            let generation = command
                .generation
                .ok_or_else(|| Failure::new(4, "command readback binding is invalid"))?;
            break (agent, generation, turn_id);
        }
        sleep_before_deadline(deadline).await?;
    };

    let params = TurnGetRequest::new(agent, generation, turn_id);
    params.validate().map_err(Failure::readback)?;
    loop {
        let turn: CoreTurn = wait_call(
            eliot_daemon::client_call_typed(data_dir, "turn.get", &params),
            deadline,
        )
        .await?;
        params.validate_response(&turn).map_err(Failure::readback)?;
        match turn.state {
            TurnState::Completed => return Ok(turn),
            TurnState::Unknown => return Err(Failure::new(4, "turn outcome is unknown")),
            TurnState::Running => sleep_before_deadline(deadline).await?,
        }
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
            let root: CoreCommand =
                match eliot_daemon::client_call_typed(data_dir, "command.get", &params).await {
                    Ok(command) => command,
                    Err(_) => return Err(linked),
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
    timeout_at(deadline, sleep(WAIT_POLL_INTERVAL))
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
