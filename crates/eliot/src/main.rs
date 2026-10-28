use clap::{Args, CommandFactory, Parser, Subcommand};
use eliot_core::Error as CoreError;
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
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
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
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
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
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
    },
    /// Steer the exact active turn.
    Steer {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(value_name = "TEXT", allow_hyphen_values = true)]
        text: String,
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
        #[arg(long, value_name = "TURN")]
        turn: Option<String>,
    },
    /// Interrupt the exact active turn. `abort` is an alias.
    #[command(alias = "abort")]
    Interrupt {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
        #[arg(long, value_name = "TURN")]
        turn: Option<String>,
    },
    /// Approve a pending native request.
    Approve {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
        #[arg(long, value_name = "REQUEST")]
        request: Option<String>,
    },
    /// Deny a pending native request.
    Deny {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
        #[arg(long, value_name = "REQUEST")]
        request: Option<String>,
    },
    /// Answer a pending native question from a bounded JSON file.
    Answer {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "PATH")]
        answers_file: PathBuf,
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
        #[arg(long, value_name = "REQUEST")]
        request: Option<String>,
    },
    /// List Agents.
    Ls {
        #[arg(long, value_name = "AGENT")]
        after: Option<String>,
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
    },
    /// Read one page of Agents with their current cached observations.
    Top {
        #[arg(long, value_name = "AGENT")]
        after: Option<String>,
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
    },
    /// Show one Agent.
    Show { agent: String },
    /// Read one page of an Agent's process records.
    Processes {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
        generation: Option<i64>,
        #[arg(long, default_value = "agent", value_name = "SCOPE")]
        scope: String,
        #[arg(long, value_name = "CURSOR")]
        after: Option<String>,
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
    },
    /// Read an Agent's ephemeral process statistics.
    Stats {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
        generation: Option<i64>,
    },
    /// Read the current Git snapshot for an Agent's workspace.
    GitStatus {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
        generation: Option<i64>,
    },
    /// Read one page of edits attributed to an Agent by native file-change hints.
    Edits {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
        generation: Option<i64>,
        #[arg(long, value_name = "PATH")]
        after: Option<String>,
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
    },
    /// Follow committed events. With an Agent argument, filter to that Agent.
    Watch { agent: Option<String> },
    /// Read an Agent's retained turn results.
    Result {
        agent: String,
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
        #[arg(long, value_name = "TURN")]
        turn_id: Option<String>,
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
    },
    /// Read a bounded, private page of native or rollout history.
    History {
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
        generation: Option<i64>,
        #[arg(long, default_value = "native", value_name = "SOURCE", value_parser = ["native", "rollout"])]
        source: String,
        #[arg(long, value_name = "VIEW", value_parser = ["subject", "turns", "items"])]
        view: String,
        #[arg(long, value_name = "TURN")]
        turn_id: Option<String>,
        #[arg(long, value_name = "CURSOR")]
        cursor: Option<String>,
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
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
        #[arg(long, value_name = "GENERATION")]
        generation: Option<i64>,
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
        #[arg(long, default_value_t = PAGE_SIZE)]
        limit: usize,
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
enum WorktreeCommand {
    /// Remove a terminal Agent's clean worktree while retaining its branch.
    Remove {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
        generation: Option<i64>,
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
}

impl Failure {
    fn new(exit_code: u8, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
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
                | "frame_too_large"
                | "secret_in_payload"
                | "id_conflict"
                | "id_expired"
                | "profile_error" => 3,
                "failed" => 1,
                _ => 4,
            }
        };
        Self::new(exit_code, error.to_string())
    }

    fn timeout() -> Self {
        Self::new(5, "wait timed out; the admitted command was not cancelled")
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
                    let mode = if args.drain { "drain" } else { "force" };
                    let result =
                        mutation_call(&data_dir, "daemon.shutdown", json!({ "mode": mode }), id)
                            .await?;
                    print_value(&result, json_output);
                    Ok(response_exit_code(&result))
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
            let result = read_call(&data_dir, "daemon.status", json!({})).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Discover {
            profile,
            harness,
            scope,
            cursor,
            limit,
        } => {
            validate_limit(limit)?;
            let mut params = json!({ "profile": profile, "limit": limit });
            if let Some(harness) = harness {
                params["harness"] = Value::String(harness);
            }
            if let Some(scope) = scope {
                params["native_scope"] = Value::String(scope);
            }
            if let Some(cursor) = cursor {
                params["cursor"] = Value::String(cursor);
            }
            let result = read_call(&data_dir, "agent.discover", params).await?;
            print_value(&result, json_output);
            Ok(0)
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
            let result = mutation_call(
                &data_dir,
                "command.submit",
                json!({ "verb": "attach", "args": args }),
                id,
            )
            .await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
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
            let submitted = mutation_call(
                &data_dir,
                "command.submit",
                json!({ "verb": "launch", "args": args }),
                id,
            )
            .await?;
            let submit_exit = response_exit_code(&submitted);
            if matches!(submit_exit, 1 | 3 | 4) {
                print_value(&submitted, json_output);
                return Ok(submit_exit);
            }

            let root_id = submitted
                .get("command_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Failure::new(4, "launch response omitted command_id"))?;
            if !wait {
                let command = wait_for_command(&data_dir, root_id, deadline).await?;
                let exit_code = response_exit_code(&command);
                print_value(&command, json_output);
                return Ok(exit_code);
            }

            let (completed, exit_code) = if prompt.is_some() {
                let linked = submitted
                    .get("linked_command")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Failure::new(4, "launch response omitted its linked input command")
                    })?;
                let turn = wait_for_turn(&data_dir, linked, &submitted, None, deadline).await?;
                let exit_code = turn_exit_code(&turn);
                (turn, exit_code)
            } else {
                let command = wait_for_command(&data_dir, root_id, deadline).await?;
                let exit_code = response_exit_code(&command);
                (command, exit_code)
            };
            print_value(&completed, json_output);
            Ok(exit_code)
        }
        CliCommand::Send {
            agent,
            text,
            wait,
            generation,
            timeout_seconds,
        } => {
            validate_text(Some(&text))?;
            let deadline = if wait {
                Some(wait_deadline(timeout_seconds)?)
            } else {
                None
            };
            let mut params = json!({
                "target": agent,
                "verb": "send",
                "args": { "text": text },
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            let submitted = mutation_call(&data_dir, "command.submit", params, id).await?;
            let submit_exit = response_exit_code(&submitted);
            if matches!(submit_exit, 1 | 3 | 4) {
                print_value(&submitted, json_output);
                return Ok(submit_exit);
            }
            if !wait {
                print_value(&submitted, json_output);
                return Ok(submit_exit);
            }
            let deadline = deadline.ok_or_else(|| Failure::new(2, "missing wait deadline"))?;
            let command_id = submitted
                .get("command_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Failure::new(4, "send response omitted command_id"))?;
            let completed =
                wait_for_turn(&data_dir, command_id, &submitted, generation, deadline).await?;
            print_value(&completed, json_output);
            Ok(turn_exit_code(&completed))
        }
        CliCommand::Resume {
            agent,
            prompt,
            wait,
            generation,
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
            let mut params = json!({
                "target": agent,
                "verb": "resume",
                "args": args,
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            let submitted = mutation_call(&data_dir, "command.submit", params, id).await?;
            let submit_exit = response_exit_code(&submitted);
            if matches!(submit_exit, 1 | 3 | 4) {
                print_value(&submitted, json_output);
                return Ok(submit_exit);
            }
            if !wait {
                print_value(&submitted, json_output);
                return Ok(submit_exit);
            }

            let deadline = deadline.ok_or_else(|| Failure::new(2, "missing wait deadline"))?;
            let command_id = submitted
                .get("command_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Failure::new(4, "resume response omitted command_id"))?;
            let completed = if prompt.is_some() {
                let linked = submitted
                    .get("linked_command")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Failure::new(4, "resume response omitted its linked input command")
                    })?;
                let old_generation = submitted
                    .get("generation")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| {
                        Failure::new(4, "resume response omitted its source generation")
                    })?;
                let generation = old_generation
                    .checked_add(1)
                    .ok_or_else(|| Failure::new(4, "resume generation could not be incremented"))?;
                wait_for_turn(&data_dir, linked, &submitted, Some(generation), deadline).await?
            } else {
                wait_for_command(&data_dir, command_id, deadline).await?
            };
            let exit_code = if prompt.is_some() {
                turn_exit_code(&completed)
            } else {
                response_exit_code(&completed)
            };
            print_value(&completed, json_output);
            Ok(exit_code)
        }
        CliCommand::Steer {
            agent,
            text,
            generation,
            turn,
        } => {
            validate_text(Some(&text))?;
            let mut params = json!({
                "target": agent,
                "verb": "steer",
                "args": { "text": text },
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            if let Some(turn) = turn {
                params["expected_turn_id"] = Value::String(turn);
            }
            let result = mutation_call(&data_dir, "command.submit", params, id).await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
        }
        CliCommand::Interrupt {
            agent,
            generation,
            turn,
        } => {
            let mut params = json!({
                "target": agent,
                "verb": "interrupt",
                "args": {},
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            if let Some(turn) = turn {
                params["expected_turn_id"] = Value::String(turn);
            }
            let result = mutation_call(&data_dir, "command.submit", params, id).await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
        }
        CliCommand::Approve {
            agent,
            generation,
            request,
        } => {
            let mut params = json!({
                "target": agent,
                "verb": "approve",
                "args": {},
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            if let Some(request) = request {
                params["request_id"] = Value::String(request);
            }
            let result = mutation_call(&data_dir, "command.submit", params, id).await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
        }
        CliCommand::Deny {
            agent,
            generation,
            request,
        } => {
            let mut params = json!({
                "target": agent,
                "verb": "deny",
                "args": {},
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            if let Some(request) = request {
                params["request_id"] = Value::String(request);
            }
            let result = mutation_call(&data_dir, "command.submit", params, id).await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
        }
        CliCommand::Answer {
            agent,
            answers_file,
            generation,
            request,
        } => {
            let answers = read_answers_file(&answers_file)?;
            let mut params = json!({
                "target": agent,
                "verb": "answer",
                "args": { "answers": answers },
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            if let Some(request) = request {
                params["request_id"] = Value::String(request);
            }
            let result = mutation_call(&data_dir, "command.submit", params, id).await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
        }
        CliCommand::Ls { after, limit } => {
            validate_limit(limit)?;
            let mut params = json!({ "limit": limit });
            if let Some(after) = after {
                params["after"] = Value::String(after);
            }
            let result = read_call(&data_dir, "agent.list", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Top { after, limit } => {
            validate_limit(limit)?;
            let mut params = json!({ "limit": limit, "observations": true });
            if let Some(after) = after {
                params["after"] = Value::String(after);
            }
            let result = read_call(&data_dir, "agent.list", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Show { agent } => {
            let result = read_call(&data_dir, "agent.get", json!({ "id": agent })).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Processes {
            agent,
            generation,
            scope,
            after,
            limit,
        } => {
            validate_limit(limit)?;
            let mut params = json!({ "id": agent, "scope": scope, "limit": limit });
            if let Some(generation) = generation {
                params["generation"] = json!(generation);
            }
            if let Some(after) = after {
                params["after"] = Value::String(after);
            }
            let result = read_call(&data_dir, "agent.processes", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Stats { agent, generation } => {
            let mut params = json!({ "id": agent });
            if let Some(generation) = generation {
                params["generation"] = json!(generation);
            }
            let result = read_call(&data_dir, "agent.stats", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::GitStatus { agent, generation } => {
            let mut params = json!({ "id": agent });
            if let Some(generation) = generation {
                params["generation"] = json!(generation);
            }
            let result = read_call(&data_dir, "agent.git", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Edits {
            agent,
            generation,
            after,
            limit,
        } => {
            validate_limit(limit)?;
            let mut params = json!({ "id": agent, "limit": limit });
            if let Some(generation) = generation {
                params["generation"] = json!(generation);
            }
            if let Some(after) = after {
                params["after"] = Value::String(after);
            }
            let result = read_call(&data_dir, "agent.edits", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Watch { agent } => watch(&data_dir, agent.as_deref(), json_output).await,
        CliCommand::Result {
            agent,
            generation,
            turn_id,
            limit,
        } => {
            validate_limit(limit)?;
            let mut params = json!({ "id": agent, "limit": limit });
            if let Some(generation) = generation {
                params["generation"] = json!(generation);
            }
            if let Some(turn_id) = turn_id {
                params["turn_id"] = Value::String(turn_id);
            }
            let result = read_call(&data_dir, "agent.result", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::History {
            agent,
            generation,
            source,
            view,
            turn_id,
            cursor,
            limit,
        } => {
            validate_limit(limit)?;
            let mut params = json!({
                "id": agent,
                "source": source,
                "view": view,
                "limit": limit,
            });
            if let Some(generation) = generation {
                params["generation"] = json!(generation);
            }
            if let Some(turn_id) = turn_id {
                params["turn_id"] = Value::String(turn_id);
            }
            if let Some(cursor) = cursor {
                params["cursor"] = Value::String(cursor);
            }
            let result = read_call(&data_dir, "agent.history", params).await?;
            print_value(&result, json_output);
            Ok(0)
        }
        CliCommand::Detach { agent, generation } => {
            let mut params = json!({
                "target": agent,
                "verb": "detach",
                "args": {},
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            let result = mutation_call(&data_dir, "command.submit", params, id).await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
        }
        CliCommand::Stop {
            agent,
            force,
            generation,
        } => {
            let mut params = json!({
                "target": agent,
                "verb": "stop",
                "args": { "force": force },
            });
            if let Some(generation) = generation {
                params["expected_generation"] = json!(generation);
            }
            let result = mutation_call(&data_dir, "command.submit", params, id).await?;
            print_value(&result, json_output);
            Ok(response_exit_code(&result))
        }
        CliCommand::Worktree { command } => match command {
            WorktreeCommand::Remove { agent, generation } => {
                let deadline = wait_deadline(DEFAULT_WAIT_SECONDS)?;
                let mut params = json!({
                    "target": agent,
                    "verb": "worktree.remove",
                    "args": {},
                });
                if let Some(generation) = generation {
                    params["expected_generation"] = json!(generation);
                }
                let submitted = mutation_call(&data_dir, "command.submit", params, id).await?;
                let submit_exit = response_exit_code(&submitted);
                if matches!(submit_exit, 1 | 3 | 4) {
                    print_value(&submitted, json_output);
                    return Ok(submit_exit);
                }
                let command_id = submitted
                    .get("command_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Failure::new(4, "worktree removal response omitted command_id")
                    })?;
                let completed = wait_for_command(&data_dir, command_id, deadline).await?;
                print_value(&completed, json_output);
                Ok(response_exit_code(&completed))
            }
        },
        CliCommand::Cmd { command } => match command {
            CmdCommand::Show { id } => {
                let result = read_call(&data_dir, "command.get", json!({ "id": id })).await?;
                print_value(&result, json_output);
                Ok(0)
            }
        },
        CliCommand::Events { since, limit } => {
            validate_limit(limit)?;
            let result = read_call(
                &data_dir,
                "events.read",
                json!({ "after": since.unwrap_or(0), "limit": limit }),
            )
            .await?;
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
            .stderr(Stdio::null());
        command
            .spawn()
            .map_err(|error| Failure::new(1, format!("cannot start daemon: {error}")))?;
    }
    if json_output {
        print_value(&json!({ "state": "starting" }), true);
    } else {
        println!("daemon starting");
    }
    Ok(())
}

async fn read_call(data_dir: &Path, method: &str, params: Value) -> Result<Value, Failure> {
    eliot_daemon::client_call(data_dir, method, params)
        .await
        .map_err(|error| Failure::from_core(error, false))
}

async fn mutation_call(
    data_dir: &Path,
    method: &str,
    mut params: Value,
    supplied_id: Option<String>,
) -> Result<Value, Failure> {
    let id = match supplied_id {
        Some(id) => {
            Ulid::from_string(&id).map_err(|_| Failure::new(2, "--id must be a valid ULID"))?;
            id.to_ascii_uppercase()
        }
        None => Ulid::new().to_string(),
    };
    let object = params
        .as_object_mut()
        .ok_or_else(|| Failure::new(2, "internal CLI error: mutation params must be an object"))?;
    object.insert("id".to_owned(), Value::String(id.clone()));
    eprintln!("id={id}");
    eliot_daemon::client_call(data_dir, method, params)
        .await
        .map_err(|error| Failure::from_core(error, true))
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

async fn wait_call(
    data_dir: &Path,
    method: &str,
    params: Value,
    deadline: Instant,
) -> Result<Value, Failure> {
    match timeout_at(
        deadline,
        eliot_daemon::client_call(data_dir, method, params),
    )
    .await
    {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(Failure::new(
            4,
            format!("cannot establish command outcome: {error}"),
        )),
        Err(_) => Err(Failure::timeout()),
    }
}

async fn wait_for_command(
    data_dir: &Path,
    command_id: &str,
    deadline: Instant,
) -> Result<Value, Failure> {
    loop {
        let command = wait_call(
            data_dir,
            "command.get",
            json!({ "id": command_id }),
            deadline,
        )
        .await?;
        if command_state_result(&command)? {
            return Ok(command);
        }
        sleep_before_deadline(deadline).await?;
    }
}

async fn wait_for_turn(
    data_dir: &Path,
    command_id: &str,
    submitted: &Value,
    fallback_generation: Option<i64>,
    deadline: Instant,
) -> Result<Value, Failure> {
    let (agent, generation, turn_id) = loop {
        let command = wait_call(
            data_dir,
            "command.get",
            json!({ "id": command_id }),
            deadline,
        )
        .await?;
        if let Some(turn_id) = command.get("turn_id").and_then(Value::as_str) {
            let agent = command
                .get("agent")
                .and_then(Value::as_str)
                .or_else(|| command.get("target").and_then(Value::as_str))
                .or_else(|| submitted.get("agent").and_then(Value::as_str))
                .ok_or_else(|| Failure::new(4, "command readback omitted its Agent id"))?;
            let generation = command
                .get("generation")
                .and_then(Value::as_i64)
                .or_else(|| submitted.get("generation").and_then(Value::as_i64))
                .or(fallback_generation)
                .ok_or_else(|| Failure::new(4, "command readback omitted its generation"))?;
            break (agent.to_owned(), generation, turn_id.to_owned());
        }
        command_state_result(&command)?;
        sleep_before_deadline(deadline).await?;
    };

    loop {
        let turn = wait_call(
            data_dir,
            "turn.get",
            json!({
                "agent": agent,
                "generation": generation,
                "turn_id": turn_id,
            }),
            deadline,
        )
        .await?;
        if turn
            .get("generation")
            .and_then(Value::as_i64)
            .is_some_and(|actual| actual != generation)
        {
            return Err(Failure::new(
                4,
                "turn readback belongs to a different generation",
            ));
        }
        match turn.get("state").and_then(Value::as_str) {
            Some("completed") => return Ok(turn),
            Some("unknown") => return Err(Failure::new(4, "turn outcome is unknown")),
            Some("running") => sleep_before_deadline(deadline).await?,
            Some(state) => {
                return Err(Failure::new(
                    4,
                    format!("unrecognized turn state {state:?}; outcome is unknown"),
                ));
            }
            None => return Err(Failure::new(4, "turn readback omitted state")),
        }
    }
}

/// True means applied; false means still open. Known terminal errors retain their
/// contract exit code and never imply that the native effect can be retried.
fn command_state_result(command: &Value) -> Result<bool, Failure> {
    match command.get("state").and_then(Value::as_str) {
        Some("applied") => Ok(true),
        Some("rejected") => Err(Failure::new(3, "command was rejected before native I/O")),
        Some("failed") => Err(Failure::new(1, "command failed")),
        Some("unknown") => Err(Failure::new(4, "command outcome is unknown")),
        Some("cancelled") => Err(Failure::new(1, "command was cancelled")),
        Some("accepted" | "queued" | "dispatched") => Ok(false),
        Some(state) => Err(Failure::new(
            4,
            format!("unrecognized command state {state:?}; outcome is unknown"),
        )),
        None => Err(Failure::new(4, "command readback omitted state")),
    }
}

async fn sleep_before_deadline(deadline: Instant) -> Result<(), Failure> {
    timeout_at(deadline, sleep(WAIT_POLL_INTERVAL))
        .await
        .map_err(|_| Failure::timeout())
}

fn response_exit_code(response: &Value) -> u8 {
    match response.get("state").and_then(Value::as_str) {
        None | Some("accepted" | "queued" | "applied") => 0,
        Some("rejected") => 3,
        Some("failed" | "cancelled") => 1,
        Some("unknown") => 4,
        Some("dispatched") => 6,
        Some(_) => 6,
    }
}

fn turn_exit_code(turn: &Value) -> u8 {
    match turn.get("state").and_then(Value::as_str) {
        Some("completed") => match turn.get("status").and_then(Value::as_str) {
            Some("failed" | "error") => 1,
            _ => 0,
        },
        Some("unknown") => 4,
        _ => 6,
    }
}

async fn watch(data_dir: &Path, agent: Option<&str>, json_output: bool) -> Result<u8, Failure> {
    let mut after = 0_i64;
    loop {
        let mut params = json!({
            "after": after,
            "limit": PAGE_SIZE,
            "wait_ms": 1_000,
        });
        if let Some(agent) = agent {
            params["filter"] = json!({ "agents": [agent] });
        }
        let response = tokio::select! {
            result = eliot_daemon::client_call(data_dir, "events.subscribe", params) => {
                result.map_err(|error| Failure::from_core(error, false))?
            }
            signal = tokio::signal::ctrl_c() => {
                signal.map_err(|error| Failure::new(1, format!("cannot listen for Ctrl+C: {error}")))?;
                return Ok(0);
            }
        };
        let events = response
            .get("events")
            .and_then(Value::as_array)
            .ok_or_else(|| Failure::new(1, "events.subscribe response omitted events"))?;
        for event in events {
            print_value(event, json_output);
        }
        let last_event = events
            .last()
            .and_then(|event| event.get("seq"))
            .and_then(Value::as_i64);
        after = response
            .get("next")
            .and_then(Value::as_i64)
            .or(last_event)
            .or_else(|| response.get("high_water").and_then(Value::as_i64))
            .unwrap_or(after)
            .max(after);
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
