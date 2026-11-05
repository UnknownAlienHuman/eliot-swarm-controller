use clap::{Args, CommandFactory, Parser, Subcommand};
use eliot_core::{
    Command as CoreCommand, CommandState, Error as CoreError, SubmitCommand, Turn as CoreTurn,
    TurnState,
    api::{
        AgentDiscoverRequest, AgentEditsRequest, AgentEditsResponse, AgentGetRequest,
        AgentGetResponse, AgentGitRequest, AgentGitResponse, AgentHistoryRequest, AgentListRequest,
        AgentListResponse, AgentProcessesRequest, AgentProcessesResponse, AgentResultRequest,
        AgentResultResponse, AgentStatsRequest, AgentStatsResponse, ClaimListRequest,
        ClaimListResponse, ClaimReleaseRequest, ClaimReleaseResponse, ClaimSetRequest,
        ClaimSetResponse, CommandChildrenRequest, CommandChildrenResponse, CommandGetRequest,
        CommandResolution, CommandResolveRequest, CommandResolveResponse, CommandSubmitResponse,
        DEFAULT_GOAL_CHECK_TIMEOUT_MS, DaemonShutdownRequest, DaemonShutdownResponse,
        DaemonStatusRequest, DaemonStatusResponse, DiscoverPage, EventsFilter, EventsReadRequest,
        EventsReadResponse, EventsSubscribeRequest, EventsSubscribeResponse, GoalClearRequest,
        GoalDefinition, GoalGetRequest, GoalMode, GoalPauseRequest, GoalResumeRequest,
        GoalRoundRequest, GoalRoundResponse, GoalSetRequest, GoalView, MailAckRequest,
        MailAckResponse, MailDelivery, MailInboxRequest, MailInboxResponse, MailMode,
        MailReadRequest, MailSendRequest, MailSendResponse, ObservationPage,
        ProjectRegistrationView, ProviderClearHoldRequest, ProviderClearHoldResponse,
        ProviderCorrectiveEvidence, ProviderHoldsRequest, ProviderHoldsResponse,
        RuleCatchUpRequest, RuleGetRequest, RuleStateView, ShutdownMode, TimerGetRequest,
        TimerRunRequest, TimerStateView, TurnGetRequest, TurnResolveRequest, TurnResolveResponse,
        TurnResolveStatus, VarGetRequest, VarGetResponse, VarSetRequest, VarSetResponse,
        WhoRequest, WhoResponse,
    },
};
use serde_json::{Value, json};
mod mcp;
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
    name = "swarm",
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
    /// Serve one generic { method, params } MCP tool over stdio.
    #[command(name = "mcp")]
    McpStdio,
    /// Inspect provider holds or clear one with owner corrective evidence.
    Provider {
        #[command(subcommand)]
        command: ProviderCommand,
    },
    /// Read owner-registered project authority.
    Project {
        #[command(subcommand)]
        command: ProjectCommand,
    },
    /// Read or explicitly catch up one startup-loaded rule.
    Rule {
        #[command(subcommand)]
        command: RuleCommand,
    },
    /// Read or manually run a startup-loaded timer.
    Timer {
        #[command(subcommand)]
        command: TimerCommand,
    },
    /// Set, read or drive one Agent's Eliot-mode Goal.
    Goal {
        #[command(subcommand)]
        command: GoalCommand,
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
        /// Launch as a child of this Agent; the child inherits its project.
        #[arg(long, value_name = "AGENT")]
        parent: Option<String>,
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
    /// Resume a retained native session, optionally with new input.
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
        #[arg(long, value_name = "CHOICE_ID")]
        choice: Option<String>,
        #[arg(long, value_name = "FEEDBACK")]
        feedback: Option<String>,
    },
    /// Deny a pending native request.
    Deny {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[command(flatten)]
        generation: GenerationArgs,
        #[arg(long, value_name = "REQUEST")]
        request: Option<String>,
        #[arg(long, value_name = "CHOICE_ID")]
        choice: Option<String>,
        #[arg(long, value_name = "FEEDBACK")]
        feedback: Option<String>,
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
        /// Stop the Agent's whole subtree instead of just the Agent.
        #[arg(long, conflicts_with = "generation")]
        tree: bool,
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
    /// Resolve an unknown turn with an exact owner assertion.
    Turn {
        #[command(subcommand)]
        command: TurnCommand,
    },
    /// Read committed events after a sequence number.
    Events {
        #[arg(long, value_name = "SEQ")]
        since: Option<i64>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Send, read or acknowledge addressed Agent mail.
    Msg {
        #[command(subcommand)]
        command: MsgCommand,
    },
    /// Read one page of unread mail for a mailbox.
    Inbox {
        #[arg(value_name = "AGENT")]
        agent: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long, value_name = "DELIVERY")]
        after: Option<String>,
        #[arg(long, value_name = "SECS")]
        wait: Option<u64>,
    },
    /// Read or compare-and-swap shared variables.
    Var {
        #[command(subcommand)]
        command: VarCommand,
    },
    /// Show current workers and their file areas.
    Who {
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
        /// Include bounded recent Git history per worker.
        #[arg(long)]
        history: bool,
    },
    /// Set, list or release advisory work claims.
    Claim {
        #[command(subcommand)]
        command: ClaimCommand,
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
    Show {
        id: String,
    },
    /// Read one bounded page of a retained command expansion.
    Children {
        id: String,
        #[arg(long, value_name = "ID")]
        after: Option<String>,
        #[command(flatten)]
        page: PageArgs,
    },
    /// Resolve an unknown command to its owner-asserted terminal state.
    Resolve {
        command_id: String,
        #[arg(long = "as", value_name = "STATE")]
        resolution: String,
        #[arg(long, value_name = "TEXT")]
        reason: String,
    },
}

#[derive(Subcommand)]
enum TurnCommand {
    /// Resolve an unknown turn to its owner-asserted terminal status.
    Resolve {
        agent: String,
        #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
        generation: i64,
        #[arg(long, value_name = "TURN")]
        turn_id: String,
        #[arg(long, value_name = "STATUS")]
        status: String,
        #[arg(long, value_name = "TEXT")]
        reason: String,
    },
}

#[derive(Subcommand)]
enum MsgCommand {
    /// Queue one message to the named recipients.
    Send {
        #[arg(long, value_name = "AGENT")]
        to: Vec<String>,
        #[arg(value_name = "TEXT", allow_hyphen_values = true)]
        text: String,
        #[arg(long, value_name = "LABEL")]
        thread: Option<String>,
        #[arg(long, value_name = "DELIVERY")]
        reply_to: Option<String>,
        #[arg(long, value_parser = ["queue", "send", "steer", "inject"], default_value = "queue")]
        mode: String,
    },
    /// Read one exact delivery without marking it.
    Show {
        #[arg(value_name = "DELIVERY")]
        delivery: String,
        #[arg(long, value_name = "AGENT")]
        agent: Option<String>,
    },
    /// Acknowledge exact delivery ids.
    Ack {
        #[arg(long, value_name = "DELIVERY")]
        delivery: Vec<String>,
        #[arg(long, value_name = "AGENT")]
        agent: Option<String>,
    },
}

#[derive(Subcommand)]
enum VarCommand {
    /// Write one variable, optionally guarded by its expected version.
    Set {
        #[arg(value_name = "KEY")]
        key: String,
        #[arg(value_name = "VALUE", allow_hyphen_values = true)]
        value: String,
        #[arg(long, value_name = "SCOPE")]
        scope: Option<String>,
        /// Parse VALUE as JSON instead of storing it as a string.
        #[arg(long)]
        as_json: bool,
        #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(0..))]
        expect_version: Option<i64>,
        #[arg(long, value_name = "TEXT")]
        note: Option<String>,
    },
    /// Read one variable value with its version and writer.
    Get {
        #[arg(value_name = "KEY")]
        key: String,
        #[arg(long, value_name = "SCOPE")]
        scope: Option<String>,
    },
}

#[derive(Subcommand)]
enum ClaimCommand {
    /// Claim a project-relative glob for an Agent generation.
    Set {
        #[arg(value_name = "GLOB")]
        glob: String,
        #[arg(long, value_name = "AGENT")]
        agent: Option<String>,
        #[arg(long, value_name = "TEXT")]
        note: Option<String>,
        /// Claim lifetime in seconds (1 to 86400, default 3600).
        #[arg(long, value_name = "SECS")]
        ttl_secs: Option<u64>,
    },
    /// List live claims in scope.
    List {
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
        #[arg(long, value_name = "AGENT")]
        agent: Option<String>,
    },
    /// Release one live claim.
    Release {
        #[arg(value_name = "GLOB")]
        glob: String,
        #[arg(long, value_name = "AGENT")]
        agent: Option<String>,
    },
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
enum RuleCommand {
    /// Show the loaded rule state and current/retiring cursor progress.
    Show {
        id: String,
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
    },
    /// Process one finite committed-event range through the ordinary rule path.
    CatchUp {
        #[arg(value_name = "ID")]
        rule: String,
        #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
        after: i64,
        #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
        through: Option<i64>,
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
    },
}

#[derive(Subcommand)]
enum TimerCommand {
    /// Show the current timer schedule and durable UTC slot state.
    Show {
        id: String,
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
    },
    /// Run one timer action using a durable mutation ID.
    Run {
        #[arg(value_name = "ID")]
        rule: String,
        #[arg(long, value_name = "PROJECT")]
        project: Option<String>,
    },
}

#[derive(Subcommand)]
enum GoalCommand {
    /// Set (or replace) the Agent's Eliot-mode Goal with an explicit check command.
    Set {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
        mode: String,
        #[arg(long, value_name = "TEXT")]
        text: String,
        /// Explicit check argv; pass last since it consumes the remaining arguments.
        #[arg(long, num_args = 1.., allow_hyphen_values = true, value_name = "ARG")]
        check: Vec<String>,
        #[arg(long, value_name = "PATH")]
        check_cwd: Option<String>,
        #[arg(long, value_name = "MS")]
        check_timeout_ms: Option<i64>,
        #[arg(long, value_name = "N")]
        max_turns: Option<i64>,
        #[arg(long, value_name = "N")]
        max_rounds: Option<i64>,
        #[arg(long, value_name = "N")]
        max_without_progress: Option<i64>,
        /// Cadence in milliseconds; a cadenced Goal rounds on anchored
        /// time slots instead of idle events.
        #[arg(long, value_name = "MS")]
        cadence_ms: Option<i64>,
    },
    /// Show the Goal definition, counters, budgets and limits.
    Show {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
        mode: String,
    },
    /// Pause the Goal; a paused Goal takes no rounds.
    Pause {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
        mode: String,
        #[arg(long, value_name = "REASON")]
        reason: Option<String>,
    },
    /// Clear the Goal; only `set` revives a cleared Goal.
    Clear {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
        mode: String,
    },
    /// Resume a paused Goal on its bound generation.
    Resume {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
        mode: String,
    },
    /// Run one round keyed by the exact idle event seq or cadence slot.
    Round {
        #[arg(value_name = "AGENT")]
        agent: String,
        #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
        mode: String,
        #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
        idle_seq: Option<i64>,
        #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(1..))]
        slot: Option<i64>,
    },
}

#[derive(Subcommand)]
enum ProjectCommand {
    /// List owner-registered projects and their authority bounds.
    List,
    /// Show one owner-registered project readback.
    Show { id: String },
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
    /// Internal Muse plugin entry point (hidden).
    #[command(hide = true)]
    Muse,
    /// Internal Claude plugin entry point (hidden).
    #[command(hide = true)]
    Claude,
    /// Internal OpenCode plugin entry point (hidden).
    #[command(hide = true)]
    Opencode,
    /// Internal Antigravity plugin entry point (hidden).
    #[command(hide = true)]
    Antigravity,
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

    fn new_from_message(message: String) -> Self {
        Self::new(1, message)
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
    // The derived CLI parser overflows the 1 MiB Windows main-thread stack
    // once the `who`/`claim` subcommands are added, so parse on a roomy
    // thread instead. Measured in a debug build with a direct `Cli::parse`:
    // the pre-PR parser prints `swarm --version` and exits 0, while with
    // `who`/`claim` every probe (`--version`, `status`, `who`, `claim list`,
    // `--help`) dies in `Cli::parse` with STATUS_STACK_OVERFLOW before any
    // output. Help/version still exit from the parser. A `/STACK` link arg
    // for the binary is a later follow-up.
    let cli = std::thread::Builder::new()
        .name("cli-parse".to_owned())
        .stack_size(8 * 1024 * 1024)
        .spawn(Cli::parse)
        .expect("spawn CLI parser")
        .join()
        .expect("CLI parser panicked");
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
        CliCommand::McpStdio => {
            if json_output {
                return Err(Failure::new(
                    2,
                    "--json is not valid with mcp stdio; stdout carries MCP protocol only",
                ));
            }
            mcp::run_stdio(data_dir)
                .await
                .map_err(Failure::new_from_message)?;
            Ok(0)
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
        CliCommand::Project { command } => match command {
            ProjectCommand::List => {
                let response = eliot_daemon::client_call(&data_dir, "project.list", json!({}))
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
                let Some(projects) = response.as_array() else {
                    return Err(Failure::new(4, "project.list response is invalid"));
                };
                let projects: Vec<ProjectRegistrationView> =
                    serde_json::from_value(Value::Array(projects.clone()))
                        .map_err(|_| Failure::new(4, "project.list response is invalid"))?;
                for project in &projects {
                    project
                        .validate()
                        .map_err(|_| Failure::new(4, "project.list response is invalid"))?;
                }
                print_value(&response, json_output);
                Ok(0)
            }
            ProjectCommand::Show { id } => {
                let response: ProjectRegistrationView =
                    eliot_daemon::client_call_typed(&data_dir, "project.get", &json!({"id":id}))
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                if response.id != id || response.validate().is_err() {
                    return Err(Failure::new(4, "project.get response is invalid"));
                }
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "project.get response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
        },
        CliCommand::Rule { command } => match command {
            RuleCommand::Show { id, project } => {
                let request = match project {
                    Some(project) => RuleGetRequest::new(id).in_project(project),
                    None => RuleGetRequest::new(id),
                };
                read_and_print!("rule.get", request, RuleStateView)
            }
            RuleCommand::CatchUp {
                rule,
                after,
                through,
                project,
            } => {
                let request = RuleCatchUpRequest::new(mutation_id(id)?, rule, after, through);
                let request = match project {
                    Some(project) => request.in_project(project),
                    None => request,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: CoreCommand =
                    eliot_daemon::client_call_typed(&data_dir, "rule.catch_up", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, true))?;
                let exit_code = response_exit_code(response.state);
                let value = command_value(response)?;
                print_value(&value, json_output);
                Ok(exit_code)
            }
        },
        CliCommand::Timer { command } => match command {
            TimerCommand::Show { id, project } => {
                let request = match project {
                    Some(project) => TimerGetRequest::new(id).in_project(project),
                    None => TimerGetRequest::new(id),
                };
                read_and_print!("timer.get", request, TimerStateView)
            }
            TimerCommand::Run { rule, project } => {
                let request = TimerRunRequest::new(mutation_id(id)?, rule);
                let request = match project {
                    Some(project) => request.in_project(project),
                    None => request,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: CoreCommand =
                    eliot_daemon::client_call_typed(&data_dir, "timer.run", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, true))?;
                let exit_code = response_exit_code(response.state);
                let value = command_value(response)?;
                print_value(&value, json_output);
                Ok(exit_code)
            }
        },
        CliCommand::Goal { command } => match command {
            GoalCommand::Set {
                agent,
                mode,
                text,
                check,
                check_cwd,
                check_timeout_ms,
                max_turns,
                max_rounds,
                max_without_progress,
                cadence_ms,
            } => {
                let request = GoalSetRequest {
                    agent,
                    mode: goal_mode(&mode)?,
                    definition: GoalDefinition {
                        text,
                        check,
                        check_cwd,
                        check_timeout_ms: check_timeout_ms.unwrap_or(DEFAULT_GOAL_CHECK_TIMEOUT_MS),
                        max_turns,
                        max_rounds,
                        max_without_progress,
                        cadence_ms,
                    },
                    max_cost: None,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: GoalView =
                    eliot_daemon::client_call_typed(&data_dir, "goal.set", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, true))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "goal.set response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            GoalCommand::Show { agent, mode } => {
                let request = GoalGetRequest {
                    agent,
                    mode: goal_mode(&mode)?,
                };
                read_and_print!("goal.get", request, GoalView)
            }
            GoalCommand::Pause {
                agent,
                mode,
                reason,
            } => {
                let request = GoalPauseRequest {
                    agent,
                    mode: goal_mode(&mode)?,
                    reason,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: GoalView =
                    eliot_daemon::client_call_typed(&data_dir, "goal.pause", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, true))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "goal.pause response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            GoalCommand::Clear { agent, mode } => {
                let request = GoalClearRequest {
                    agent,
                    mode: goal_mode(&mode)?,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: GoalView =
                    eliot_daemon::client_call_typed(&data_dir, "goal.clear", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, true))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "goal.clear response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            GoalCommand::Resume { agent, mode } => {
                let request = GoalResumeRequest {
                    agent,
                    mode: goal_mode(&mode)?,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: GoalView =
                    eliot_daemon::client_call_typed(&data_dir, "goal.resume", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, true))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "goal.resume response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            GoalCommand::Round {
                agent,
                mode,
                idle_seq,
                slot,
            } => {
                if idle_seq.is_none() == slot.is_none() {
                    return Err(Failure::new(
                        2,
                        "goal round needs exactly one of --idle-seq, --slot",
                    ));
                }
                let request = GoalRoundRequest {
                    id: mutation_id(id)?,
                    agent,
                    mode: goal_mode(&mode)?,
                    idle_seq,
                    slot,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: GoalRoundResponse =
                    eliot_daemon::client_call_typed(&data_dir, "goal.round", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|error| Failure::from_core(error, true))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "goal.round response is invalid"))?;
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
            parent,
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
            if let Some(parent) = parent {
                args["parent"] = Value::String(parent);
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
            choice,
            feedback,
        } => {
            let mut params = submit_request(
                id,
                Some(agent),
                "approve",
                approval_reply_args(choice, feedback),
            )?;
            params.expected_generation = generation;
            params.request_id = request;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Deny {
            agent,
            generation: GenerationArgs { generation },
            request,
            choice,
            feedback,
        } => {
            let mut params = submit_request(
                id,
                Some(agent),
                "deny",
                approval_reply_args(choice, feedback),
            )?;
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
            tree,
            generation: GenerationArgs { generation },
        } => {
            let mut params = submit_request(id, Some(agent), "stop", json!({ "force": force }))?;
            if tree {
                params.selector = params.target.take().map(|agent| format!("tree:{agent}"));
            } else {
                params.expected_generation = generation;
            }
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
            CmdCommand::Children {
                id,
                after,
                page: PageArgs { limit },
            } => {
                validate_limit(limit)?;
                let request = CommandChildrenRequest { id, after, limit };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, false))?;
                let response: CommandChildrenResponse =
                    eliot_daemon::client_call_typed(&data_dir, "command.children", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "command.children response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "command.children response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            CmdCommand::Resolve {
                command_id,
                resolution,
                reason,
            } => {
                let resolution = CommandResolution::parse(&resolution)
                    .map_err(|_| Failure::new(2, "--as must be applied or failed"))?;
                let request = CommandResolveRequest {
                    id: mutation_id(id)?,
                    command_id,
                    resolution,
                    reason,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: CommandResolveResponse =
                    eliot_daemon::client_call_typed(&data_dir, "command.resolve", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "command.resolve response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "command.resolve response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
        },
        CliCommand::Turn { command } => match command {
            TurnCommand::Resolve {
                agent,
                generation,
                turn_id,
                status,
                reason,
            } => {
                let status = TurnResolveStatus::parse(&status).map_err(|_| {
                    Failure::new(2, "--status must be completed, failed, or interrupted")
                })?;
                let request = TurnResolveRequest {
                    id: mutation_id(id)?,
                    agent,
                    generation,
                    turn_id,
                    status,
                    reason,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: TurnResolveResponse =
                    eliot_daemon::client_call_typed(&data_dir, "turn.resolve", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "turn.resolve response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "turn.resolve response is invalid"))?;
                print_value(&value, json_output);
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
        CliCommand::Msg { command } => match command {
            MsgCommand::Send {
                to,
                text,
                thread,
                reply_to,
                mode,
            } => {
                let mode: MailMode = serde_json::from_value(Value::String(mode)).map_err(|_| {
                    Failure::new(2, "mail mode must be queue, send, steer or inject")
                })?;
                let request = MailSendRequest {
                    id: mutation_id(id)?,
                    to,
                    mode,
                    text,
                    thread,
                    reply_to_delivery: reply_to,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: MailSendResponse =
                    eliot_daemon::client_call_typed(&data_dir, "mail.send", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "mail.send response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "mail.send response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            MsgCommand::Show { delivery, agent } => {
                let request = MailReadRequest {
                    agent,
                    delivery_id: delivery,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, false))?;
                let response: MailDelivery =
                    eliot_daemon::client_call_typed(&data_dir, "mail.read", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "mail.read response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "mail.read response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            MsgCommand::Ack { delivery, agent } => {
                let request = MailAckRequest {
                    id: mutation_id(id)?,
                    agent,
                    deliveries: delivery,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: MailAckResponse =
                    eliot_daemon::client_call_typed(&data_dir, "mail.ack", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "mail.ack response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "mail.ack response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
        },
        CliCommand::Inbox {
            agent,
            limit,
            after,
            wait,
        } => {
            let request = MailInboxRequest {
                agent,
                limit,
                after,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let deadline = wait.map(|secs| Instant::now() + Duration::from_secs(secs));
            loop {
                let response: MailInboxResponse =
                    eliot_daemon::client_call_typed(&data_dir, "mail.inbox", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "mail.inbox response is invalid"))?;
                let done = match deadline {
                    None => true,
                    Some(end) => !response.deliveries.is_empty() || Instant::now() >= end,
                };
                if done {
                    let value = serde_json::to_value(&response)
                        .map_err(|_| Failure::new(4, "mail.inbox response is invalid"))?;
                    print_value(&value, json_output);
                    if deadline.is_some() && response.deliveries.is_empty() {
                        return Ok(5);
                    }
                    return Ok(0);
                }
                sleep(WAIT_POLL_INTERVAL).await;
            }
        }
        CliCommand::Var { command } => match command {
            VarCommand::Set {
                key,
                value,
                scope,
                as_json,
                expect_version,
                note,
            } => {
                let value = if as_json {
                    serde_json::from_str(&value)
                        .map_err(|_| Failure::new(2, "--as-json value must be valid JSON"))?
                } else {
                    Value::String(value)
                };
                let request = VarSetRequest {
                    id: mutation_id(id)?,
                    scope,
                    key,
                    value,
                    expect_version,
                    note,
                };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, true))?;
                let response: VarSetResponse =
                    eliot_daemon::client_call_typed(&data_dir, "var.set", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, true))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "var.set response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "var.set response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
            VarCommand::Get { key, scope } => {
                let request = VarGetRequest { scope, key };
                request
                    .validate()
                    .map_err(|error| Failure::from_core(error, false))?;
                let response: VarGetResponse =
                    eliot_daemon::client_call_typed(&data_dir, "var.get", &request)
                        .await
                        .map_err(|error| Failure::from_core(error, false))?;
                request
                    .validate_response(&response)
                    .map_err(|_| Failure::new(4, "var.get response is invalid"))?;
                let value = serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "var.get response is invalid"))?;
                print_value(&value, json_output);
                Ok(0)
            }
        },
        CliCommand::Who { project, history } => {
            run_who_command(&data_dir, json_output, project, history).await
        }
        CliCommand::Claim { command } => {
            run_claim_command(&data_dir, json_output, id, command).await
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
            PluginCommand::Muse => {
                eliot_plugin_muse::run_muse()
                    .await
                    .map_err(|error| Failure::new(1, error.to_string()))?;
                Ok(0)
            }
            PluginCommand::Claude => {
                eliot_plugin_claude::run_claude()
                    .await
                    .map_err(|error| Failure::new(1, error.to_string()))?;
                Ok(0)
            }
            PluginCommand::Opencode => {
                eliot_plugin_opencode::run_opencode()
                    .await
                    .map_err(|error| Failure::new(1, error.to_string()))?;
                Ok(0)
            }
            PluginCommand::Antigravity => {
                eliot_plugin_antigravity::run()
                    .await
                    .map_err(|error| Failure::new(1, error.to_string()))?;
                Ok(0)
            }
        },
    }
}

async fn run_who_command(
    data_dir: &Path,
    json_output: bool,
    project: Option<String>,
    history: bool,
) -> Result<u8, Failure> {
    let request = WhoRequest {
        project,
        history: Some(history),
    };
    request
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: WhoResponse = eliot_daemon::client_call_typed(data_dir, "who", &request)
        .await
        .map_err(|error| Failure::from_core(error, false))?;
    request
        .validate_response(&response)
        .map_err(|_| Failure::new(4, "who response is invalid"))?;
    let value =
        serde_json::to_value(response).map_err(|_| Failure::new(4, "who response is invalid"))?;
    print_value(&value, json_output);
    Ok(0)
}

async fn run_claim_command(
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
    command: ClaimCommand,
) -> Result<u8, Failure> {
    match command {
        ClaimCommand::Set {
            glob,
            agent,
            note,
            ttl_secs,
        } => {
            let request = ClaimSetRequest {
                id: mutation_id(id)?,
                agent,
                glob,
                note,
                ttl_secs,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: ClaimSetResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.set", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "claim.set response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "claim.set response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        ClaimCommand::List { project, agent } => {
            let request = ClaimListRequest { project, agent };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: ClaimListResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.list", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "claim.list response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "claim.list response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        ClaimCommand::Release { glob, agent } => {
            let request = ClaimReleaseRequest {
                id: mutation_id(id)?,
                agent,
                glob,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: ClaimReleaseResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.release", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "claim.release response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "claim.release response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
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

fn goal_mode(mode: &str) -> Result<GoalMode, Failure> {
    GoalMode::parse(mode).map_err(|_| Failure::new(2, "--mode must be eliot or native"))
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

fn approval_reply_args(choice: Option<String>, feedback: Option<String>) -> Value {
    let mut args = serde_json::Map::new();
    if let Some(choice) = choice {
        args.insert("choice_id".to_owned(), Value::String(choice));
    }
    if let Some(feedback) = feedback {
        args.insert("feedback".to_owned(), Value::String(feedback));
    }
    Value::Object(args)
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser as _;

    #[test]
    fn approval_cli_parser_keeps_explicit_choice_and_feedback() {
        for verb in ["approve", "deny"] {
            let cli = Cli::try_parse_from([
                "swarm",
                verb,
                "a7",
                "--generation",
                "3",
                "--request",
                "request-id",
                "--choice",
                "always",
                "--feedback",
                "trust it",
            ])
            .unwrap();
            let Some(command) = cli.command else {
                panic!("approve/deny command required");
            };
            let (choice, feedback) = match command {
                CliCommand::Approve {
                    choice, feedback, ..
                } => (choice, feedback),
                CliCommand::Deny {
                    choice, feedback, ..
                } => (choice, feedback),
                _ => panic!("expected an approve or deny command"),
            };
            assert_eq!(
                approval_reply_args(choice, feedback),
                serde_json::json!({"choice_id": "always", "feedback": "trust it"})
            );
        }
    }
}
