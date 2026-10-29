use clap::{Args, CommandFactory, Parser, Subcommand};
use eliot_core::{
    Command as CoreCommand, CommandState, Error as CoreError, SubmitCommand, Turn as CoreTurn,
    TurnState,
    api::{
        AgentDiscoverRequest, AgentEditsRequest, AgentEditsResponse, AgentGetRequest,
        AgentGetResponse, AgentGitRequest, AgentGitResponse, AgentHistoryRequest, AgentListRequest,
        AgentListResponse, AgentProcessesRequest, AgentProcessesResponse, AgentResultRequest,
        AgentResultResponse, AgentStatsRequest, AgentStatsResponse, CommandGetRequest,
        CommandSubmitResponse, DaemonShutdownRequest, DaemonShutdownResponse, DaemonStatusRequest,
        DaemonStatusResponse, DiscoverPage, EventsFilter, EventsReadRequest, EventsReadResponse,
        EventsSubscribeRequest, EventsSubscribeResponse, ObservationPage, ProviderClearHoldRequest,
        ProviderClearHoldResponse, ProviderCorrectiveEvidence, ProviderHoldsRequest,
        ProviderHoldsResponse, ShutdownMode, TurnGetRequest,
    },
};
use serde_json::{Value, json};
#[cfg(not(windows))]
use std::process::{Command as ProcessCommand, Stdio};
use std::{
    env,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::ExitCode,
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout_at};
use ulid::Ulid;

const DEFAULT_WAIT_SECONDS: u64 = 30;
const PAGE_SIZE: usize = 100;
const MAX_TEXT_BYTES: usize = 512 * 1024;
const MAX_ANSWERS_FILE_BYTES: usize = 512 * 1024;
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Args)]
struct GenerationArgs {
    #[arg(long, value_name = "GENERATION")]
    generation: Option<i64>,
}

#[derive(Args)]
struct ReadGenerationArgs {
    #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
    generation: Option<i64>,
}

#[derive(Args)]
struct PageArgs {
    #[arg(long, default_value_t = PAGE_SIZE)]
    limit: usize,
}

#[derive(Parser)]
#[command(
    name = "eliot",
    version,
    about = "Local controller for coding-agent sessions"
)]
struct Cli {
    /// Private Eliot data directory (defaults to ELIOT_HOME or the platform data location).
    #[arg(long, global = true, value_name = "PATH")]
    data_dir: Option<PathBuf>,

    /// Print compact JSON instead of the default readable JSON output.
    #[arg(long, global = true)]
    json: bool,

    /// External ULID for a durable mutation. Eliot prints generated IDs before sending.
    #[arg(long, global = true, value_name = "ULID")]
    id: Option<String>,

    #[command(subcommand)]
    command: Option<CliCommand>,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Start the local daemon, or stop it with a drain/force request.
    Daemon(DaemonArgs),
    /// Read daemon state without starting a daemon.
    Status,
    /// Inspect provider holds or clear one with owner corrective evidence.
    Provider {
        #[command(subcommand)]
        command: ProviderCommand,
    },
    /// Discover one page of native sessions through an explicit profile.
    Discover {
        #[arg(long, value_name = "PROFILE")]
        profile: String,
        #[arg(long, value_name = "HARNESS")]
        harness: Option<String>,
        #[arg(long, value_name = "SCOPE")]
        scope: Option<String>,
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Attach to an exact native subject without taking control of it.
    Attach {
        #[arg(long, value_name = "PROFILE")]
        profile: String,
        #[arg(long, value_name = "HARNESS")]
        harness: String,
        #[arg(long, value_name = "SCOPE")]
        scope: String,
        #[arg(long = "ref", value_name = "REF")]
        native_ref: String,
        #[arg(long, value_name = "LABEL")]
        project: Option<String>,
    },
    /// Start an Agent using the named profile.
    Launch {
        #[arg(long, value_name = "PROFILE")]
        profile: String,
        #[arg(long, value_name = "PATH")]
        workspace: PathBuf,
        #[arg(long, value_name = "BRANCH")]
        worktree: Option<String>,
        #[arg(long, value_name = "REF", requires = "worktree")]
        base: Option<String>,
        #[arg(value_name = "PROMPT")]
        prompt: Option<String>,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
        timeout_seconds: u64,
    },
    /// Send text to an existing Agent.
    Send {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(value_name = "TEXT", allow_hyphen_values = true)]
        text: String,
        #[arg(long)]
        wait: bool,
        /// Explicitly claim one eligible provider-hold probe for this send.
        #[arg(long)]
        probe: bool,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
        timeout_seconds: u64,
    },
    /// Resume a retained Codex thread, optionally with new input.
    Resume {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(value_name = "PROMPT", allow_hyphen_values = true)]
        prompt: Option<String>,
        #[arg(long)]
        wait: bool,
        #[command(flatten)]
        generation: GenerationArgs,
    },
    /// Steer the exact active turn.
    Steer {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(value_name = "TEXT", allow_hyphen_values = true)]
        text: String,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, value_name = "TURN")]
        turn: Option<String>,
    },
    /// Interrupt the exact active turn. `abort` is an alias.
    #[command(alias = "abort")]
    Interrupt {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, value_name = "TURN")]
        turn: Option<String>,
    },
    /// Approve a pending native request.
    Approve {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, value_name = "REQUEST")]
        request: Option<String>,
    },
    /// Deny a pending native request.
    Deny {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, value_name = "REQUEST")]
        request: Option<String>,
    },
    /// Answer a pending native question from a bounded JSON file.
    Answer {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "PATH")]
        answers_file: PathBuf,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, value_name = "REQUEST")]
        request: Option<String>,
    },
    /// List Agents.
    Ls {
        #[arg(long, value_name = "AGENT")]
        after: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Read one page of Agents with their current cached observations.
    Top {
        #[arg(long, value_name = "AGENT")]
        after: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Show one Agent.
    Show { agent: String },
    /// Read one page of an Agent's process records.
    Processes {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: ReadGenerationArgs,
        #[arg(long, default_value = "agent", value_name = "SCOPE")]
        scope: String,
        #[arg(long, value_name = "CURSOR")]
        after: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Read an Agent's ephemeral process statistics.
    Stats {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: ReadGenerationArgs,
    },
    /// Read the current Git snapshot for an Agent's workspace.
    GitStatus {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: ReadGenerationArgs,
    },
    /// Read one page of edits attributed to an Agent by native file-change hints.
    Edits {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: ReadGenerationArgs,
        #[arg(long, value_name = "PATH")]
        after: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Follow committed events. With an Agent argument, filter to that Agent.
    Watch { agent: Option<String> },
    /// Read an Agent's retained turn results.
    Result {
        agent: String,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, value_name = "TURN")]
        turn_id: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Read a bounded, private page of native or rollout history.
    History {
        agent: String,
        #[command(flatten)]
        generation: ReadGenerationArgs,
        #[arg(long, default_value = "native", value_name = "SOURCE", value_parser = ["native", "rollout"])]
        source: String,
        #[arg(long, value_name = "VIEW", value_parser = ["subject", "turns", "items"])]
        view: String,
        #[arg(long, value_name = "TURN")]
        turn_id: Option<String>,
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Detach Eliot's observer from an Agent generation.
    Detach {
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(1..))]
        generation: Option<i64>,
    },
    /// Stop an Agent.
    Stop {
        agent: String,
        #[arg(long)]
        force: bool,
        #[command(flatten)]
        generation: GenerationArgs,
    },
    /// Manage an Agent's retained worktree.
    Worktree {
        #[command(subcommand)]
        command: WorktreeCommand,
    },
    /// Inspect a command.
    Cmd {
        #[command(subcommand)]
        command: CmdCommand,
    },
    /// Read committed events after a sequence number.
    Events {
        #[arg(long, value_name = "SEQ")]
        since: Option<i64>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Internal plugin entry point used by the daemon.
    #[command(hide = true)]
    Plugin {
        #[command(subcommand)]
        command: PluginCommand,
    },
}

#[derive(Args)]
struct DaemonArgs {
    /// Run the daemon in this process instead of detaching it.
    #[arg(long)]
    foreground: bool,
    #[command(subcommand)]
    action: Option<DaemonAction>,
}

#[derive(Subcommand)]
enum DaemonAction {
    Stop(ShutdownArgs),
}

#[derive(Args)]
struct ShutdownArgs {
    #[arg(long, conflicts_with = "force")]
    drain: bool,
    #[arg(long, conflicts_with = "drain")]
    force: bool,
}

#[derive(Subcommand)]
enum CmdCommand {
    Show { id: String },
}

#[derive(Subcommand)]
enum ProviderCommand {
    /// Read one bounded page of durable provider holds.
    Holds {
        #[arg(long, value_name = "SCOPE")]
        after: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Clear a provider hold with exact owner corrective evidence.
    ClearHold {
        #[arg(value_name = "SCOPE")]
        scope: String,
        #[arg(
            long,
            value_name = "EVIDENCE",
            value_parser = ["credentials_updated", "configuration_updated", "owner_verified"]
        )]
        corrective_evidence: String,
        #[arg(long, value_name = "ID")]
        expected_probe_command: Option<String>,
    },
}

#[derive(Subcommand)]
enum WorktreeCommand {
    /// Remove a terminal Agent's clean worktree while retaining its branch.
    Remove {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: ReadGenerationArgs,
    },
}

#[derive(Subcommand)]
enum PluginCommand {
    Fake,
    /// Internal Codex plugin entry point (hidden until L2.2 caller).
    #[command(hide = true)]
    Codex,
}

struct Failure {
    exit_code: u8,
    message: String,
    diagnostics: Vec<eliot_core::StderrDiagnostic>,
}

impl Failure {
    fn new(exit_code: u8, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
            diagnostics: Vec::new(),
        }
    }

    fn from_core(error: CoreError, mutation: bool) -> Self {
        let exit_code = if !mutation {
            1
        } else {
            match error.code.as_str() {
                "rejected"
                | "not_allowed"
                | "bad_state"
                | "stale_generation"
                | "invalid"
                | "unsupported"
                | "not_found"
                | "busy"
                | "capacity_unavailable"
                | "provider_held"
                | "frame_too_large"
                | "secret_in_payload"
                | "id_conflict"
                | "id_expired"
                | "profile_error" => 3,
                "failed" => 1,
                _ => 4,
            }
        };
        let mut failure = Self::new(exit_code, error.to_string());
        failure.diagnostics = error.diagnostics;
        eliot_core::bound_stderr_diagnostics(
            &mut failure.diagnostics,
            eliot_core::MAX_STDERR_DIAGNOSTIC_BYTES,
        );
        failure
    }

    fn timeout() -> Self {
        Self::new(5, "wait timed out; the admitted command was not cancelled")
    }

    fn from_command(exit_code: u8, message: impl Into<String>, command: &CoreCommand) -> Self {
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

    fn readback(error: CoreError) -> Self {
        let mut failure = Self::from_core(error, false);
        failure.exit_code = 4;
        failure.message = format!("cannot establish command outcome: {}", failure.message);
        failure
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let data_dir = match resolve_data_dir(cli.data_dir.as_deref()) {
        Ok(path) => path,
        Err(failure) => return report_failure(failure),
    };

    match dispatch(cli, data_dir).await {
        Ok(exit_code) => ExitCode::from(exit_code),
        Err(failure) => report_failure(failure),
    }
}

fn report_failure(failure: Failure) -> ExitCode {
    eprintln!("error: {}", failure.message);
    if !failure.diagnostics.is_empty()
        && let Ok(diagnostics) = serde_json::to_string(&failure.diagnostics)
    {
        // Escaped structured text preserves both actual stderr sources and their
        // truncation/EOF/read-error flags without terminal control sequences.
        eprintln!("stderr diagnostics: {diagnostics}");
    }
    ExitCode::from(failure.exit_code)
}

fn resolve_data_dir(argument: Option<&Path>) -> Result<PathBuf, Failure> {
    if let Some(path) = argument {
        return Ok(path.to_path_buf());
    }
    if let Some(path) = env::var_os("ELIOT_HOME").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = env::var_os("LOCALAPPDATA").filter(|value| !value.is_empty()) {
        return Ok(PathBuf::from(path).join("Eliot"));
    }
    if let Some(path) = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .filter(|value| !value.is_empty())
    {
        return Ok(PathBuf::from(path)
            .join(".local")
            .join("share")
            .join("eliot"));
    }
    Err(Failure::new(
        1,
        "cannot determine the data directory; set --data-dir or ELIOT_HOME",
    ))
}

async fn dispatch(cli: Cli, data_dir: PathBuf) -> Result<u8, Failure> {
    let Cli {
        json: json_output,
        id,
        command,
        ..
    } = cli;

    // The concrete request/response pair stays at each read caller. Binding is
    // checked before any response data is printed.
    macro_rules! read_and_print {
        ($method:literal, $params:expr, $response:ty) => {{
            let params = $params;
            params
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: $response = eliot_daemon::client_call_typed(&data_dir, $method, &params)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
            params
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, false))?;
            let result = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, concat!($method, " response is invalid")))?;
            print_value(&result, json_output);
            Ok(0)
        }};
    }

    let Some(command) = command else {
        let mut command = Cli::command();
        let _ = command.print_help();
        println!();
        return Ok(0);
    };

    match command {
        CliCommand::Daemon(args) => {
            let foreground = args.foreground;
            match args.action {
                Some(DaemonAction::Stop(args)) => {
                    if args.drain == args.force {
                        return Err(Failure::new(
                            2,
                            "daemon stop requires exactly one of --drain or --force",
                        ));
                    }
                    if foreground {
                        return Err(Failure::new(
                            2,
                            "--foreground is only valid when starting the daemon",
                        ));
                    }
                    let mode = if args.drain {
                        ShutdownMode::Drain
                    } else {
                        ShutdownMode::Force
                    };
                    let result = shutdown_daemon(
                        &data_dir,
                        &DaemonShutdownRequest::new(mutation_id(id)?, mode),
                    )
                    .await?;
                    let value = serde_json::to_value(&result)
                        .map_err(|_| Failure::new(4, "daemon.shutdown response is invalid"))?;
                    print_value(&value, json_output);
                    Ok(response_exit_code(result.state))
                }
                None if foreground => {
                    eliot_daemon::run(data_dir)
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                    Ok(0)
                }
                None => {
                    start_daemon(&data_dir, json_output)?;
                    Ok(0)
                }
            }
        }
        CliCommand::Status => {
            read_and_print!(
                "daemon.status",
                DaemonStatusRequest::default(),
                DaemonStatusResponse
            )
        }
        CliCommand::Provider { command } => match command {
            ProviderCommand::Holds { after, limit } => {
                let request = ProviderHoldsRequest { after, limit };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, false))?;
                let response: ProviderHoldsResponse =
                    eliot_daemon::client_call_typed(&data_dir, "provider.holds", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "provider.holds response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "provider.holds response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            ProviderCommand::ClearHold {
                scope,
                corrective_evidence,
                expected_probe_command,
            } => {
                let corrective_evidence: ProviderCorrectiveEvidence =
                    serde_json::from_value(Value::String(corrective_evidence))
                        .map_err(|_| Failure::new(2, "invalid provider corrective evidence"))?;
                let request = ProviderClearHoldRequest {
                    id: mutation_id(id)?,
                    scope,
                    corrective_evidence,
                    expected_probe_command,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: ProviderClearHoldResponse =
                    eliot_daemon::client_call_typed(&data_dir, "provider.clear_hold", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "provider.clear_hold response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "provider.clear_hold response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
        },
        CliCommand::Discover {
            profile,
            harness,
            scope,
            cursor,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = AgentDiscoverRequest {
                profile,
                harness,
                native_scope: scope,
                cursor,
                limit,
            };
            read_and_print!("agent.discover", params, DiscoverPage)
        }
        CliCommand::Attach {
            profile,
            harness,
            scope,
            native_ref,
            project,
        } => {
            let mut args = json!({
                "mode": "observe",
                "profile": profile,
                "harness": harness,
                "native_scope": scope,
                "native_ref": native_ref,
            });
            if let Some(project) = project {
                args["project"] = Value::String(project);
            }
            let params = submit_request(id, None, "attach", args)?;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Launch {
            profile,
            workspace,
            worktree,
            base,
            prompt,
            wait,
            timeout_seconds,
        } => {
            if profile.is_empty() {
                return Err(Failure::new(2, "--profile must not be empty"));
            }
            validate_text(prompt.as_deref())?;
            let deadline = wait_deadline(timeout_seconds)?;
            let mut args = json!({
                "profile": profile,
                "workspace": workspace.to_string_lossy(),
            });
            if let Some(worktree) = worktree {
                args["worktree"] = Value::String(worktree);
            }
            if let Some(base) = base {
                args["base"] = Value::String(base);
            }
            if let Some(prompt) = &prompt {
                args["text"] = Value::String(prompt.clone());
            }
            let params = submit_request(id, None, "launch", args)?;
            let submitted = submit_command(&data_dir, &params).await?;
            let submit_exit = response_exit_code(submitted.state);
            if matches!(submit_exit, 1 | 3 | 4) {
                print_value(&submit_value(&submitted)?, json_output);
                return Ok(submit_exit);
            }

            let root_id = &submitted.command_id;
            if !wait {
                let command = wait_for_command(&data_dir, root_id, deadline).await?;
                let exit_code = response_exit_code(command.state);
                print_value(&command_value(command)?, json_output);
                return Ok(exit_code);
            }

            let (completed, exit_code) = if prompt.is_some() {
                let linked = submitted.linked_command.as_deref().ok_or_else(|| {
                    Failure::new(4, "launch response omitted its linked input command")
                })?;
                let turn = wait_for_turn(&data_dir, linked, deadline).await?;
                let exit_code = turn_exit_code(&turn);
                (turn_value(turn)?, exit_code)
            } else {
                let command = wait_for_command(&data_dir, root_id, deadline).await?;
                let exit_code = response_exit_code(command.state);
                (command_value(command)?, exit_code)
            };
            print_value(&completed, json_output);
            Ok(exit_code)
        }
        CliCommand::Send {
            agent,
            text,
            wait,
            probe,
            generation: GenerationArgs { generation },
            timeout_seconds,
        } => {
            validate_text(Some(&text))?;
            let deadline = if wait {
                Some(wait_deadline(timeout_seconds)?)
            } else {
                None
            };
            let mut params = submit_request(id, Some(agent), "send", json!({ "text": text }))?;
            params.expected_generation = generation;
            params.probe = probe;
            let submitted = submit_command(&data_dir, &params).await?;
            let submit_exit = response_exit_code(submitted.state);
            if matches!(submit_exit, 1 | 3 | 4) {
                print_value(&submit_value(&submitted)?, json_output);
                return Ok(submit_exit);
            }
            if !wait {
                print_value(&submit_value(&submitted)?, json_output);
                return Ok(submit_exit);
            }
            let deadline = deadline.ok_or_else(|| Failure::new(2, "missing wait deadline"))?;
            let completed = wait_for_turn(&data_dir, &submitted.command_id, deadline).await?;
            let exit_code = turn_exit_code(&completed);
            print_value(&turn_value(completed)?, json_output);
            Ok(exit_code)
        }
        CliCommand::Resume {
            agent,
            prompt,
            wait,
            generation: GenerationArgs { generation },
        } => {
            validate_text(prompt.as_deref())?;
            let deadline = if wait {
                Some(wait_deadline(DEFAULT_WAIT_SECONDS)?)
            } else {
                None
            };
            let mut args = json!({});
            if let Some(prompt) = &prompt {
                args["text"] = Value::String(prompt.clone());
            }
            let mut params = submit_request(id, Some(agent), "resume", args)?;
            params.expected_generation = generation;
            let submitted = submit_command(&data_dir, &params).await?;
            let submit_exit = response_exit_code(submitted.state);
            if matches!(submit_exit, 1 | 3 | 4) {
                print_value(&submit_value(&submitted)?, json_output);
                return Ok(submit_exit);
            }
            if !wait {
                print_value(&submit_value(&submitted)?, json_output);
                return Ok(submit_exit);
            }

            let deadline = deadline.ok_or_else(|| Failure::new(2, "missing wait deadline"))?;
            let (completed, exit_code) = if prompt.is_some() {
                let linked = submitted.linked_command.as_deref().ok_or_else(|| {
                    Failure::new(4, "resume response omitted its linked input command")
                })?;
                let turn = wait_for_turn(&data_dir, linked, deadline).await?;
                let exit_code = turn_exit_code(&turn);
                (turn_value(turn)?, exit_code)
            } else {
                let command = wait_for_command(&data_dir, &submitted.command_id, deadline).await?;
                let exit_code = response_exit_code(command.state);
                (command_value(command)?, exit_code)
            };
            print_value(&completed, json_output);
            Ok(exit_code)
        }
        CliCommand::Steer {
            agent,
            text,
            generation: GenerationArgs { generation },
            turn,
        } => {
            validate_text(Some(&text))?;
            let mut params = submit_request(id, Some(agent), "steer", json!({ "text": text }))?;
            params.expected_generation = generation;
            params.expected_turn_id = turn;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Interrupt {
            agent,
            generation: GenerationArgs { generation },
            turn,
        } => {
            let mut params = submit_request(id, Some(agent), "interrupt", json!({}))?;
            params.expected_generation = generation;
            params.expected_turn_id = turn;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Approve {
            agent,
            generation: GenerationArgs { generation },
            request,
        } => {
            let mut params = submit_request(id, Some(agent), "approve", json!({}))?;
            params.expected_generation = generation;
            params.request_id = request;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Deny {
            agent,
            generation: GenerationArgs { generation },
            request,
        } => {
            let mut params = submit_request(id, Some(agent), "deny", json!({}))?;
            params.expected_generation = generation;
            params.request_id = request;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Answer {
            agent,
            answers_file,
            generation: GenerationArgs { generation },
            request,
        } => {
            let answers = read_answers_file(&answers_file)?;
            let mut params =
                submit_request(id, Some(agent), "answer", json!({ "answers": answers }))?;
            params.expected_generation = generation;
            params.request_id = request;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Ls {
            after,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = AgentListRequest {
                after,
                limit,
                observations: false,
            };
            read_and_print!("agent.list", params, AgentListResponse)
        }
        CliCommand::Top {
            after,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = AgentListRequest {
                after,
                limit,
                observations: true,
            };
            read_and_print!("agent.list", params, AgentListResponse)
        }
        CliCommand::Show { agent } => {
            read_and_print!("agent.get", AgentGetRequest::new(agent), AgentGetResponse)
        }
        CliCommand::Processes {
            agent,
            generation: ReadGenerationArgs { generation },
            scope,
            after,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = AgentProcessesRequest {
                id: agent,
                generation,
                scope: Some(scope),
                after,
                limit,
            };
            read_and_print!("agent.processes", params, AgentProcessesResponse)
        }
        CliCommand::Stats {
            agent,
            generation: ReadGenerationArgs { generation },
        } => {
            let params = AgentStatsRequest {
                id: agent,
                generation,
            };
            read_and_print!("agent.stats", params, AgentStatsResponse)
        }
        CliCommand::GitStatus {
            agent,
            generation: ReadGenerationArgs { generation },
        } => {
            let params = AgentGitRequest {
                id: agent,
                generation,
            };
            read_and_print!("agent.git", params, AgentGitResponse)
        }
        CliCommand::Edits {
            agent,
            generation: ReadGenerationArgs { generation },
            after,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = AgentEditsRequest {
                id: agent,
                generation,
                after,
                limit,
            };
            read_and_print!("agent.edits", params, AgentEditsResponse)
        }
        CliCommand::Watch { agent } => watch(&data_dir, agent.as_deref(), json_output).await,
        CliCommand::Result {
            agent,
            generation: GenerationArgs { generation },
            turn_id,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = AgentResultRequest {
                id: agent,
                generation,
                turn_id,
                limit,
            };
            read_and_print!("agent.result", params, AgentResultResponse)
        }
        CliCommand::History {
            agent,
            generation: ReadGenerationArgs { generation },
            source,
            view,
            turn_id,
            cursor,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = AgentHistoryRequest {
                id: agent,
                generation,
                source,
                view,
                turn_id,
                cursor,
                limit,
            };
            read_and_print!("agent.history", params, ObservationPage)
        }
        CliCommand::Detach { agent, generation } => {
            let mut params = submit_request(id, Some(agent), "detach", json!({}))?;
            params.expected_generation = generation;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Stop {
            agent,
            force,
            generation: GenerationArgs { generation },
        } => {
            let mut params = submit_request(id, Some(agent), "stop", json!({ "force": force }))?;
            params.expected_generation = generation;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Worktree { command } => match command {
            WorktreeCommand::Remove {
                agent,
                generation: ReadGenerationArgs { generation },
            } => {
                let deadline = wait_deadline(DEFAULT_WAIT_SECONDS)?;
                let mut params = submit_request(id, Some(agent), "worktree.remove", json!({}))?;
                params.expected_generation = generation;
                let submitted = submit_command(&data_dir, &params).await?;
                let submit_exit = response_exit_code(submitted.state);
                if matches!(submit_exit, 1 | 3 | 4) {
                    print_value(&submit_value(&submitted)?, json_output);
                    return Ok(submit_exit);
                }
                let completed =
                    wait_for_command(&data_dir, &submitted.command_id, deadline).await?;
                let exit_code = response_exit_code(completed.state);
                print_value(&command_value(completed)?, json_output);
                Ok(exit_code)
            }
        },
        CliCommand::Cmd { command } => match command {
            CmdCommand::Show { id } => {
                let params = CommandGetRequest::new(id);
                params
                    .validate()
                    .map_err(|error| Failure::from_core(error, false))?;
                let response: CoreCommand =
                    eliot_daemon::client_call_typed(&data_dir, "command.get", &params)
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                params
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, false))?;
                let result = command_value(response)?;
                print_value(&result, json_output);
                Ok(0)
            }
        },
        CliCommand::Events {
            since,
            page: PageArgs { limit },
        } => {
            validate_limit(limit)?;
            let params = EventsReadRequest {
                after: since.unwrap_or(0),
                limit,
                filter: EventsFilter::default(),
            };
            params
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: EventsReadResponse =
                eliot_daemon::client_call_typed(&data_dir, "events.read", &params)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            response
                .validate(params.after)
                .map_err(|error| Failure::from_core(error, false))?;
            let result = serde_json::to_value(response).map_err(|_| {
                Failure::new(1, "unavailable: cannot render typed events.read response")
            })?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Plugin { command } => match command {
            PluginCommand::Fake => {
                eliot_plugin::run_fake()
                    .await
                    .map_err(|error| Failure::new(1, error.to_string()))?;
                Ok(0)
            }
            PluginCommand::Codex => {
                eliot_plugin_codex::run_codex()
                    .await
                    .map_err(|error| Failure::new(1, error.to_string()))?;
                Ok(0)
            }
        },
    }
}

fn start_daemon(data_dir: &Path, json_output: bool) -> Result<(), Failure> {
    #[cfg(windows)]
    eliot_daemon::start_background_daemon(data_dir)
        .map_err(|error| Failure::new(1, format!("cannot start daemon: {error}")))?;
    #[cfg(not(windows))]
    {
        let executable = env::current_exe()
            .map_err(|error| Failure::new(1, format!("cannot locate Eliot executable: {error}")))?;
        let mut command = ProcessCommand::new(executable);
        command
            .arg("--data-dir")
            .arg(data_dir)
            .arg("daemon")
            .arg("--foreground")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            // The daemon opens its authoritative private logs/daemon.log only
            // after validating the root and holding its single-owner lock.
            .stderr(Stdio::null());
        command
            .spawn()
            .map_err(|error| Failure::new(1, format!("cannot start daemon: {error}")))?;
    }
    let log_path = data_dir.join("logs").join("daemon.log");
    if json_output {
        print_value(
            &json!({ "state": "starting", "log": log_path.to_string_lossy() }),
            true,
        );
    } else {
        println!("daemon starting; diagnostics: {}", log_path.display());
    }
    Ok(())
}

fn mutation_id(supplied_id: Option<String>) -> Result<String, Failure> {
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

fn submit_request(
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

async fn submit_command(
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

async fn submit_and_print(
    data_dir: &Path,
    params: &SubmitCommand,
    json_output: bool,
) -> Result<u8, Failure> {
    let result = submit_command(data_dir, params).await?;
    print_value(&submit_value(&result)?, json_output);
    Ok(response_exit_code(result.state))
}

async fn shutdown_daemon(
    data_dir: &Path,
    params: &DaemonShutdownRequest,
) -> Result<DaemonShutdownResponse, Failure> {
    params
        .validate()
        .map_err(|error| Failure::from_core(error, true))?;
    let response: DaemonShutdownResponse =
        eliot_daemon::client_call_typed(data_dir, "daemon.shutdown", params)
            .await
            .map_err(|error| Failure::from_core(error, true))?;
    params
        .validate_response(&response)
        .map_err(|_| Failure::new(4, "daemon.shutdown response is invalid"))?;
    Ok(response)
}

fn submit_value(response: &CommandSubmitResponse) -> Result<Value, Failure> {
    serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "command.submit response is invalid"))
}

fn read_answers_file(path: &Path) -> Result<Value, Failure> {
    let file = File::open(path).map_err(|_| Failure::new(2, "answers file could not be read"))?;
    let mut bounded = file.take(MAX_ANSWERS_FILE_BYTES as u64 + 1);
    let mut bytes = Vec::new();
    let byte_count = bounded
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::new(2, "answers file could not be read"))?;
    if byte_count > MAX_ANSWERS_FILE_BYTES {
        return Err(Failure::new(2, "answers file exceeds the 512 KiB limit"));
    }

    let answers: Value = serde_json::from_slice(&bytes)
        .map_err(|_| Failure::new(2, "answers file must contain valid answer JSON"))?;
    let Some(questions) = answers.as_object() else {
        return Err(Failure::new(
            2,
            "answers file must be an object keyed by question id",
        ));
    };
    for question in questions.values() {
        let Some(question) = question.as_object() else {
            return Err(Failure::new(
                2,
                "each answer entry must contain an answers array",
            ));
        };
        let Some(values) = question.get("answers").and_then(Value::as_array) else {
            return Err(Failure::new(
                2,
                "each answer entry must contain an answers array",
            ));
        };
        if question.len() != 1 || values.iter().any(|value| !value.is_string()) {
            return Err(Failure::new(
                2,
                "each answer entry must contain only an array of strings",
            ));
        }
    }
    Ok(answers)
}

fn validate_text(text: Option<&str>) -> Result<(), Failure> {
    if let Some(text) = text.filter(|text| text.len() > MAX_TEXT_BYTES) {
        return Err(Failure::new(
            2,
            format!(
                "input is {} bytes; the maximum is {MAX_TEXT_BYTES}",
                text.len()
            ),
        ));
    }
    Ok(())
}

fn validate_limit(limit: usize) -> Result<(), Failure> {
    if !(1..=1_000).contains(&limit) {
        return Err(Failure::new(2, "--limit must be between 1 and 1000"));
    }
    Ok(())
}

fn wait_deadline(seconds: u64) -> Result<Instant, Failure> {
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

async fn wait_call<T>(
    call: impl std::future::Future<Output = eliot_core::Result<T>>,
    deadline: Instant,
) -> Result<T, Failure> {
    match timeout_at(deadline, call).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Failure::readback(error)),
        Err(_) => Err(Failure::timeout()),
    }
}

async fn wait_for_command(
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

async fn wait_for_turn(
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

/// True means applied; false means still open. Known terminal errors retain their
/// contract exit code and never imply that the native effect can be retried.
fn command_state_result(command: &CoreCommand) -> Result<bool, Failure> {
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
        CommandState::Cancelled => Err(Failure::from_command(1, "command was cancelled", command)),
        CommandState::Accepted | CommandState::Queued | CommandState::Dispatched => Ok(false),
    }
}

async fn sleep_before_deadline(deadline: Instant) -> Result<(), Failure> {
    timeout_at(deadline, sleep(WAIT_POLL_INTERVAL))
        .await
        .map_err(|_| Failure::timeout())
}

fn response_exit_code(state: CommandState) -> u8 {
    match state {
        CommandState::Accepted | CommandState::Queued | CommandState::Applied => 0,
        CommandState::Rejected => 3,
        CommandState::Failed | CommandState::Cancelled => 1,
        CommandState::Unknown => 4,
        CommandState::Dispatched => 6,
    }
}

fn turn_exit_code(turn: &CoreTurn) -> u8 {
    match turn.state {
        TurnState::Completed => match turn.status.as_deref() {
            Some("failed" | "error") => 1,
            _ => 0,
        },
        TurnState::Unknown => 4,
        TurnState::Running => 6,
    }
}

fn command_value(command: CoreCommand) -> Result<Value, Failure> {
    serde_json::to_value(command).map_err(|_| Failure::new(4, "command readback is unavailable"))
}

fn turn_value(turn: CoreTurn) -> Result<Value, Failure> {
    serde_json::to_value(turn).map_err(|_| Failure::new(4, "turn readback is unavailable"))
}

async fn watch(data_dir: &Path, agent: Option<&str>, json_output: bool) -> Result<u8, Failure> {
    let mut after = 0_i64;
    loop {
        let params = EventsSubscribeRequest {
            after,
            limit: PAGE_SIZE,
            wait_ms: 1_000,
            filter: EventsFilter {
                agents: agent.into_iter().map(str::to_owned).collect(),
                ..EventsFilter::default()
            },
        };
        params
            .validate()
            .map_err(|error| Failure::from_core(error, false))?;
        let response: EventsSubscribeResponse = tokio::select! {
            result = eliot_daemon::client_call_typed(data_dir, "events.subscribe", &params) => {
                result.map_err(|error| Failure::from_core(error, false))?
            }
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| Failure::new(1, format!("cannot listen for Ctrl+C: {error}")))?;
                return Ok(0);
            }
        };
        response
            .validate(params.after)
            .map_err(|error| Failure::from_core(error, false))?;
        for event in &response.events {
            let value = serde_json::to_value(event).map_err(|_| {
                Failure::new(1, "unavailable: cannot render typed events.subscribe event")
            })?;
            print_value(&value, json_output);
        }
        // The server's scan cursor includes filtered-out records as well.
        after = response.next;
    }
}

fn print_value(value: &Value, json_output: bool) {
    if json_output {
        println!("{value}");
    } else if let Ok(rendered) = serde_json::to_string_pretty(value) {
        println!("{rendered}");
    } else {
        println!("{value}");
    }
}
