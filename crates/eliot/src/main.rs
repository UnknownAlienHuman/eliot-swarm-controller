use clap::{Args, CommandFactory, Parser, Subcommand};
use eliot_core::{
    AgentState, Command as CoreCommand, CommandState, Error as CoreError, InjectArgs, InjectPoint,
    NativeChildActivityState, SubmitCommand, Turn as CoreTurn, TurnOutcome, TurnState,
    api::{
        AgentDiscoverRequest, AgentEditsRequest, AgentEditsResponse, AgentGetRequest,
        AgentGetResponse, AgentGitRequest, AgentGitResponse, AgentHistoryRequest, AgentListRequest,
        AgentListResponse, AgentProcessesRequest, AgentProcessesResponse, AgentResultRequest,
        AgentResultResponse, AgentStatsRequest, AgentStatsResponse, ClaimListRequest,
        ClaimListResponse, ClaimReleaseRequest, ClaimReleaseResponse, ClaimSetRequest,
        ClaimSetResponse, CommandChildrenRequest, CommandChildrenResponse, CommandGetRequest,
        CommandResolution, CommandResolveRequest, CommandResolveResponse, CommandSubmitResponse,
        DEFAULT_GOAL_CHECK_TIMEOUT_MS, DaemonShutdownRequest, DaemonShutdownResponse,
        DaemonStatusRequest, DaemonStatusResponse, DiscoverPage, EventsBoundsRequest,
        EventsBoundsResponse, EventsFilter, EventsReadRequest, EventsReadResponse,
        EventsSubscribeRequest, EventsSubscribeResponse, GoalClearRequest, GoalDefinition,
        GoalGetRequest, GoalMode, GoalPauseRequest, GoalResumeRequest, GoalRoundRequest,
        GoalRoundResponse, GoalSetRequest, GoalView, MAX_SWARM_WAIT_TARGETS, MAX_VAR_VALUE_BYTES,
        MailAckRequest, MailAckResponse, MailDelivery, MailInboxRequest, MailInboxResponse,
        MailMode, MailReadRequest, MailSendRequest, MailSendResponse, ObservationPage,
        ProjectRegistrationView, ProviderClearHoldRequest, ProviderClearHoldResponse,
        ProviderCorrectiveEvidence, ProviderHoldsRequest, ProviderHoldsResponse,
        RuleCatchUpRequest, RuleGetRequest, RuleStateView, ShutdownMode, SwarmWaitRequest,
        SwarmWaitResponse, TimerGetRequest, TimerRunRequest, TimerStateView, TurnGetRequest,
        TurnResolveRequest, TurnResolveResponse, TurnResolveStatus, VarGetRequest, VarGetResponse,
        VarSetRequest, VarSetResponse, WaitTurnRef, WhoRequest, WhoResponse,
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

    /// Print compact JSON instead of the default readable output.
    #[arg(long, global = true)]
    json: bool,

    /// External ULID for a durable mutation. Eliot prints generated IDs before sending.
    #[arg(long, global = true, value_name = "ULID")]
    id: Option<String>,

    #[command(subcommand)]
    command: Option<CliCommand>,
}

// Keep payloads in typed Args: clap builds each payload in its own generated
// function instead of accumulating every argument builder in this enum's frame.
#[derive(Subcommand)]
enum CliCommand {
    /// Start the local daemon, or stop it with a drain/force request.
    Daemon(DaemonArgs),
    /// Read daemon state without starting a daemon.
    Status,
    /// Serve one generic { method, params } MCP tool over stdio (default) or loopback StreamableHTTP.
    #[command(name = "mcp")]
    Mcp(McpArgs),
    /// Inspect provider holds or clear one with owner corrective evidence.
    Provider(ProviderArgs),
    /// Read owner-registered project authority.
    Project(ProjectArgs),
    /// Read or explicitly catch up one startup-loaded rule.
    Rule(RuleArgs),
    /// Read or manually run a startup-loaded timer.
    Timer(TimerArgs),
    /// Set, read or drive one Agent's Eliot-mode Goal.
    Goal(GoalArgs),
    /// Discover one page of native sessions through an explicit profile.
    Discover(DiscoverArgs),
    /// Attach to an exact native subject without taking control of it.
    Attach(AttachArgs),
    /// Start an Agent using the named profile.
    Launch(LaunchArgs),
    /// Send text to an existing Agent.
    Send(SendArgs),
    /// Resume a retained native session, optionally with new input.
    Resume(ResumeArgs),
    /// Steer the exact active turn.
    Steer(SteerArgs),
    /// Queue system context for the next input or the exact active turn's safe point.
    Inject(InjectCliArgs),
    /// Interrupt the exact active turn. `abort` is an alias.
    #[command(alias = "abort")]
    Interrupt(InterruptArgs),
    /// Approve a pending native request.
    Approve(ApproveArgs),
    /// Deny a pending native request.
    Deny(DenyArgs),
    /// Answer a pending native question from a bounded JSON file.
    Answer(AnswerArgs),
    /// List compact Agent summaries in your scope.
    Ls(LsArgs),
    /// List compact Agent summaries with cached observations in your scope.
    Top(TopArgs),
    /// Show one Agent.
    Show(ShowArgs),
    /// Read one page of an Agent's process records.
    Processes(ProcessesArgs),
    /// Read an Agent's ephemeral process statistics.
    Stats(StatsArgs),
    /// Read the current Git snapshot for an Agent's workspace.
    GitStatus(GitStatusArgs),
    /// Read one page of edits attributed to an Agent by native file-change hints.
    Edits(EditsArgs),
    /// Follow new committed events; use --after to replay retained history.
    Watch(WatchArgs),
    /// Read an Agent's retained turn results.
    Result(ResultArgs),
    /// Read a bounded, private page of native or rollout history.
    History(HistoryArgs),
    /// Detach Eliot's observer from an Agent generation.
    Detach(DetachArgs),
    /// Stop an Agent.
    Stop(StopArgs),
    /// Manage an Agent's retained worktree.
    Worktree(WorktreeArgs),
    /// Inspect a command.
    Cmd(CmdArgs),
    /// Resolve an unknown turn with an exact owner assertion.
    Turn(TurnArgs),
    /// Read committed events after a sequence number.
    Events(EventsArgs),
    /// Wait for a frozen set of child commands/turns with all/any semantics.
    Wait(WaitArgs),
    /// Send, read or acknowledge addressed Agent mail.
    Msg(MsgArgs),
    /// Read one page of unread mail for a mailbox.
    Inbox(InboxArgs),
    /// Read or compare-and-swap shared variables.
    Var(VarArgs),
    /// Show current workers and their file areas.
    Who(WhoArgs),
    /// Set, list or release advisory work claims.
    Claim(ClaimArgs),
    /// Internal plugin entry point used by the daemon.
    #[command(hide = true)]
    Plugin(PluginArgs),
}

#[derive(Args)]
struct McpArgs {
    /// Serve StreamableHTTP on 127.0.0.1 instead of stdio.
    #[arg(long)]
    http: bool,
    /// Require a complete inherited Agent identity for stdio calls; never use Owner or Run.
    #[arg(long)]
    require_agent: bool,
    /// Loopback port for --http; 0 selects an ephemeral port (actual address is printed in the ready frame).
    #[arg(long, default_value_t = 0)]
    port: u16,
    /// Public https tunnel URL (path /mcp) widening the --http Host/Origin checks; required with --managed-child.
    #[arg(long, value_name = "URL")]
    public_url: Option<String>,
    /// Managed-child lifecycle: also exit cleanly when stdin reaches EOF.
    #[arg(long, hide = true)]
    managed_child: bool,
}

#[derive(Args)]
struct ProviderArgs {
    #[command(subcommand)]
    command: ProviderCommand,
}

#[derive(Args)]
struct ProjectArgs {
    #[command(subcommand)]
    command: ProjectCommand,
}

#[derive(Args)]
struct RuleArgs {
    #[command(subcommand)]
    command: RuleCommand,
}

#[derive(Args)]
struct TimerArgs {
    #[command(subcommand)]
    command: TimerCommand,
}

#[derive(Args)]
struct GoalArgs {
    #[command(subcommand)]
    command: GoalCommand,
}

#[derive(Args)]
struct DiscoverArgs {
    #[arg(long, value_name = "PROFILE")]
    profile: String,
    #[arg(long, value_name = "HARNESS")]
    harness: Option<String>,
    #[arg(long, value_name = "SCOPE")]
    scope: Option<String>,
    #[arg(long, value_name = "CURSOR")]
    cursor: Option<String>,
    #[arg(long)]
    archived: bool,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
struct AttachArgs {
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
}

#[derive(Args)]
struct LaunchArgs {
    /// Named profile. Required for a parentless launch. An Agent child
    /// reuses its parent's frozen profile; the Owner may supply one.
    #[arg(long, value_name = "PROFILE")]
    profile: Option<String>,
    #[arg(long, value_name = "PATH")]
    workspace: PathBuf,
    #[arg(long, value_name = "BRANCH")]
    worktree: Option<String>,
    #[arg(long, value_name = "REF", requires = "worktree")]
    base: Option<String>,
    /// Launch as a child of this Agent; the child inherits its project
    /// and, for an Agent principal, its parent's frozen profile.
    #[arg(long, value_name = "AGENT")]
    parent: Option<String>,
    #[arg(value_name = "PROMPT")]
    prompt: Option<String>,
    #[arg(long)]
    wait: bool,
    #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
    timeout_seconds: u64,
}

#[derive(Args)]
struct SendArgs {
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
}

#[derive(Args)]
struct ResumeArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(value_name = "PROMPT", allow_hyphen_values = true)]
    prompt: Option<String>,
    #[arg(long)]
    wait: bool,
    #[command(flatten)]
    generation: GenerationArgs,
    #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
    timeout_seconds: u64,
}

#[derive(Args)]
struct SteerArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(value_name = "TEXT", allow_hyphen_values = true)]
    text: String,
    #[command(flatten)]
    generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    turn: Option<String>,
}

#[derive(Args)]
struct InjectCliArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(value_name = "TEXT", allow_hyphen_values = true)]
    text: String,
    #[arg(long, value_parser = ["next_input", "turn_safe_point", "active_turn"], default_value = "next_input")]
    point: String,
    #[command(flatten)]
    generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    turn: Option<String>,
    #[arg(long, value_name = "MS")]
    expires_at_ms: Option<i64>,
}

#[derive(Args)]
struct InterruptArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[command(flatten)]
    generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    turn: Option<String>,
}

#[derive(Args)]
struct ApproveArgs {
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
}

#[derive(Args)]
struct DenyArgs {
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
}

#[derive(Args)]
struct AnswerArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(long, value_name = "PATH")]
    answers_file: PathBuf,
    #[command(flatten)]
    generation: GenerationArgs,
    #[arg(long, value_name = "REQUEST")]
    request: Option<String>,
}

#[derive(Args)]
struct ListFilterArgs {
    /// Exact Agent state: unknown, created, starting, idle, busy, waiting,
    /// stopped, failed or lost.
    #[arg(long, value_name = "STATE")]
    state: Option<String>,
    /// Exact project label.
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
    /// Exact harness name.
    #[arg(long, value_name = "HARNESS")]
    harness: Option<String>,
    /// Direct-parent Agent ID.
    #[arg(long, value_name = "AGENT")]
    parent: Option<String>,
    /// Subtree-root Agent ID.
    #[arg(long, value_name = "AGENT")]
    root: Option<String>,
}

#[derive(Args)]
struct LsArgs {
    #[arg(long, value_name = "AGENT")]
    after: Option<String>,
    #[command(flatten)]
    page: PageArgs,
    #[command(flatten)]
    filter: ListFilterArgs,
}

#[derive(Args)]
struct TopArgs {
    #[arg(long, value_name = "AGENT")]
    after: Option<String>,
    #[command(flatten)]
    page: PageArgs,
    #[command(flatten)]
    filter: ListFilterArgs,
}

#[derive(Args)]
struct ShowArgs {
    agent: String,
}

#[derive(Args)]
struct ProcessesArgs {
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
}

#[derive(Args)]
struct StatsArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[command(flatten)]
    generation: ReadGenerationArgs,
}

#[derive(Args)]
struct GitStatusArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[command(flatten)]
    generation: ReadGenerationArgs,
}

#[derive(Args)]
struct EditsArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[command(flatten)]
    generation: ReadGenerationArgs,
    #[arg(long, value_name = "PATH")]
    after: Option<String>,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
struct WatchArgs {
    agent: Option<String>,
    /// Exclusive replay cursor. Omitted: start at the current journal tip.
    #[arg(long, value_name = "SEQ")]
    after: Option<i64>,
}

#[derive(Args)]
struct ResultArgs {
    agent: String,
    #[command(flatten)]
    generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    turn_id: Option<String>,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
struct HistoryArgs {
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
}

#[derive(Args)]
struct DetachArgs {
    agent: String,
    #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(1..))]
    generation: Option<i64>,
}

#[derive(Args)]
struct StopArgs {
    agent: String,
    #[arg(long)]
    force: bool,
    /// Stop the Agent's whole subtree instead of just the Agent.
    #[arg(long, conflicts_with = "generation")]
    tree: bool,
    #[command(flatten)]
    generation: GenerationArgs,
}

#[derive(Args)]
struct WorktreeArgs {
    #[command(subcommand)]
    command: WorktreeCommand,
}

#[derive(Args)]
struct CmdArgs {
    #[command(subcommand)]
    command: CmdCommand,
}

#[derive(Args)]
struct TurnArgs {
    #[command(subcommand)]
    command: TurnCommand,
}

#[derive(Args)]
struct EventsArgs {
    #[arg(long, value_name = "SEQ")]
    since: Option<i64>,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
struct WaitArgs {
    /// Complete when every entry is terminal.
    #[arg(long, conflicts_with = "any")]
    all: bool,
    /// Complete when any entry is terminal.
    #[arg(long, conflicts_with = "all")]
    any: bool,
    #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
    timeout_seconds: u64,
    /// Command ids, or `agent/generation/turn_id` for exact turn bindings.
    #[arg(value_name = "ID")]
    ids: Vec<String>,
}

#[derive(Args)]
struct MsgArgs {
    #[command(subcommand)]
    command: MsgCommand,
}

#[derive(Args)]
struct InboxArgs {
    #[arg(value_name = "AGENT")]
    agent: Option<String>,
    #[arg(long)]
    limit: Option<usize>,
    #[arg(long, value_name = "DELIVERY")]
    after: Option<String>,
    #[arg(long, value_name = "SECS")]
    wait: Option<u64>,
}

#[derive(Args)]
struct VarArgs {
    #[command(subcommand)]
    command: VarCommand,
}

#[derive(Args)]
struct WhoArgs {
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
    /// Include bounded recent Git history per worker.
    #[arg(long)]
    history: bool,
}

#[derive(Args)]
struct ClaimArgs {
    #[command(subcommand)]
    command: ClaimCommand,
}

#[derive(Args)]
struct PluginArgs {
    #[command(subcommand)]
    command: PluginCommand,
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
    Show(CmdShowArgs),
    /// Read one bounded page of a retained command expansion.
    Children(CmdChildrenArgs),
    /// Resolve an unknown command to its owner-asserted terminal state.
    Resolve(CmdResolveArgs),
}

#[derive(Args)]
struct CmdShowArgs {
    id: String,
}

#[derive(Args)]
struct CmdChildrenArgs {
    id: String,
    #[arg(long, value_name = "ID")]
    after: Option<String>,
    #[command(flatten)]
    page: PageArgs,
}

#[derive(Args)]
struct CmdResolveArgs {
    command_id: String,
    #[arg(long = "as", value_name = "STATE")]
    resolution: String,
    #[arg(long, value_name = "TEXT")]
    reason: String,
}

#[derive(Subcommand)]
enum TurnCommand {
    /// Resolve an unknown turn to its owner-asserted terminal status.
    Resolve(TurnResolveArgs),
}

#[derive(Args)]
struct TurnResolveArgs {
    agent: String,
    #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
    generation: i64,
    #[arg(long, value_name = "TURN")]
    turn_id: String,
    #[arg(long, value_name = "STATUS")]
    status: String,
    #[arg(long, value_name = "TEXT")]
    reason: String,
}

#[derive(Subcommand)]
enum MsgCommand {
    /// Queue one message to the named recipients.
    Send(MsgSendArgs),
    /// Read one exact delivery without marking it.
    Show(MsgShowArgs),
    /// Acknowledge exact delivery ids.
    Ack(MsgAckArgs),
}

#[derive(Args)]
struct MsgSendArgs {
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
}

#[derive(Args)]
struct MsgShowArgs {
    #[arg(value_name = "DELIVERY")]
    delivery: String,
    #[arg(long, value_name = "AGENT")]
    agent: Option<String>,
}

#[derive(Args)]
struct MsgAckArgs {
    #[arg(long, value_name = "DELIVERY")]
    delivery: Vec<String>,
    #[arg(long, value_name = "AGENT")]
    agent: Option<String>,
}

#[derive(Subcommand)]
enum VarCommand {
    /// Write one variable, optionally guarded by its expected version.
    Set(VarSetArgs),
    /// Read one variable value with its version and writer.
    Get(VarGetArgs),
}

#[derive(Args)]
struct VarSetArgs {
    #[arg(value_name = "KEY")]
    key: String,
    /// Value supplied directly on the command line.
    #[arg(
        value_name = "VALUE",
        allow_hyphen_values = true,
        required_unless_present = "value_file",
        conflicts_with = "value_file"
    )]
    value: Option<String>,
    /// Read the value from a UTF-8 file instead of the inline VALUE.
    #[arg(long, value_name = "PATH")]
    value_file: Option<PathBuf>,
    #[arg(long, value_name = "SCOPE")]
    scope: Option<String>,
    /// Parse the value (inline or from --value-file) as JSON instead of storing it as a string.
    #[arg(long)]
    as_json: bool,
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(0..))]
    expect_version: Option<i64>,
    #[arg(long, value_name = "TEXT")]
    note: Option<String>,
}

#[derive(Args)]
struct VarGetArgs {
    #[arg(value_name = "KEY")]
    key: String,
    #[arg(long, value_name = "SCOPE")]
    scope: Option<String>,
}

#[derive(Subcommand)]
enum ClaimCommand {
    /// Claim a project-relative glob for an Agent generation.
    Set(ClaimSetArgs),
    /// List live claims in scope.
    List(ClaimListArgs),
    /// Release one live claim.
    Release(ClaimReleaseArgs),
}

#[derive(Args)]
struct ClaimSetArgs {
    #[arg(value_name = "GLOB")]
    glob: String,
    #[arg(long, value_name = "AGENT")]
    agent: Option<String>,
    #[arg(long, value_name = "TEXT")]
    note: Option<String>,
    /// Claim lifetime in seconds (1 to 86400, default 3600).
    #[arg(long, value_name = "SECS")]
    ttl_secs: Option<u64>,
}

#[derive(Args)]
struct ClaimListArgs {
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
    #[arg(long, value_name = "AGENT")]
    agent: Option<String>,
}

#[derive(Args)]
struct ClaimReleaseArgs {
    #[arg(value_name = "GLOB")]
    glob: String,
    #[arg(long, value_name = "AGENT")]
    agent: Option<String>,
}

#[derive(Subcommand)]
enum ProviderCommand {
    /// Read one bounded page of durable provider holds.
    Holds(ProviderHoldsArgs),
    /// Clear a provider hold with exact owner corrective evidence.
    ClearHold(ProviderClearHoldArgs),
}

#[derive(Args)]
struct ProviderHoldsArgs {
    #[arg(long, value_name = "SCOPE")]
    after: Option<String>,
    #[arg(long)]
    limit: Option<usize>,
}

#[derive(Args)]
struct ProviderClearHoldArgs {
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
}

#[derive(Subcommand)]
enum RuleCommand {
    /// Show the loaded rule state and current/retiring cursor progress.
    Show(RuleShowArgs),
    /// Process one finite committed-event range through the ordinary rule path.
    CatchUp(RuleCatchUpArgs),
}

#[derive(Args)]
struct RuleShowArgs {
    id: String,
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
}

#[derive(Args)]
struct RuleCatchUpArgs {
    #[arg(value_name = "ID")]
    rule: String,
    #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
    after: i64,
    #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
    through: Option<i64>,
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
}

#[derive(Subcommand)]
enum TimerCommand {
    /// Show the current timer schedule and durable UTC slot state.
    Show(TimerShowArgs),
    /// Run one timer action using a durable mutation ID.
    Run(TimerRunArgs),
}

#[derive(Args)]
struct TimerShowArgs {
    id: String,
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
}

#[derive(Args)]
struct TimerRunArgs {
    #[arg(value_name = "ID")]
    rule: String,
    #[arg(long, value_name = "PROJECT")]
    project: Option<String>,
}

#[derive(Subcommand)]
enum GoalCommand {
    /// Set (or replace) the Agent's Eliot-mode Goal with an explicit check command.
    Set(GoalSetArgs),
    /// Show the Goal definition, counters, budgets and limits.
    Show(GoalShowArgs),
    /// Pause the Goal; a paused Goal takes no rounds.
    Pause(GoalPauseArgs),
    /// Clear the Goal; only `set` revives a cleared Goal.
    Clear(GoalClearArgs),
    /// Resume a paused Goal on its bound generation.
    Resume(GoalResumeArgs),
    /// Run one round keyed by the exact idle event seq or cadence slot.
    Round(GoalRoundArgs),
}

#[derive(Args)]
struct GoalSetArgs {
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
}

#[derive(Args)]
struct GoalShowArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    mode: String,
}

#[derive(Args)]
struct GoalPauseArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    mode: String,
    #[arg(long, value_name = "REASON")]
    reason: Option<String>,
}

#[derive(Args)]
struct GoalClearArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    mode: String,
}

#[derive(Args)]
struct GoalResumeArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    mode: String,
}

#[derive(Args)]
struct GoalRoundArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    mode: String,
    #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
    idle_seq: Option<i64>,
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(1..))]
    slot: Option<i64>,
}

#[derive(Subcommand)]
enum ProjectCommand {
    /// List owner-registered projects and their authority bounds.
    List,
    /// Show one owner-registered project readback.
    Show(ProjectShowArgs),
}

#[derive(Args)]
struct ProjectShowArgs {
    id: String,
}

#[derive(Subcommand)]
enum WorktreeCommand {
    /// Remove a terminal Agent's clean worktree while retaining its branch.
    Remove(WorktreeRemoveArgs),
}

#[derive(Args)]
struct WorktreeRemoveArgs {
    #[arg(value_name = "AGENT")]
    agent: String,
    #[command(flatten)]
    generation: ReadGenerationArgs,
}

#[derive(Subcommand)]
enum PluginCommand {
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
    /// Internal Antigravity command-hook entry point (hidden).
    #[command(name = "antigravity-hook", hide = true)]
    AntigravityHook,
}

#[derive(Debug)]
struct Failure {
    exit_code: u8,
    message: String,
    code: Option<String>,
    diagnostics: Vec<eliot_core::StderrDiagnostic>,
}

impl Failure {
    fn new(exit_code: u8, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
            code: None,
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    if matches!(&cli.command, Some(CliCommand::Plugin(_))) {
        match eliot_plugin::native_lifecycle::run_plugin_entry(run_inner(cli)) {
            Ok(exit_code) => exit_code,
            Err(error) => report_failure(Failure::new(1, error.to_string())),
        }
    } else {
        run(cli)
    }
}

#[tokio::main]
async fn run(cli: Cli) -> ExitCode {
    run_inner(cli).await
}

async fn run_inner(cli: Cli) -> ExitCode {
    let data_dir = match resolve_data_dir(cli.data_dir.as_deref()) {
        Ok(path) => path,
        Err(failure) => return report_failure(failure),
    };

    let Cli {
        json: json_output,
        id,
        command,
        ..
    } = cli;
    // Build top-level help before entering dispatch's large async frame.
    let Some(command) = command else {
        let mut command = Cli::command();
        let _ = command.print_help();
        println!();
        return ExitCode::SUCCESS;
    };

    let result = match command {
        CliCommand::Plugin(PluginArgs { command }) => run_plugin_command(command).await,
        command => dispatch(command, data_dir, json_output, id).await,
    };
    match result {
        Ok(exit_code) => ExitCode::from(exit_code),
        Err(failure) => report_failure(failure),
    }
}

async fn run_plugin_command(command: PluginCommand) -> Result<u8, Failure> {
    match command {
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
        PluginCommand::AntigravityHook => {
            eliot_plugin_antigravity::run_hook_helper()
                .await
                .map_err(|error| Failure::new(1, error.to_string()))?;
            Ok(0)
        }
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

async fn dispatch(
    command: CliCommand,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
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
                    let request = DaemonShutdownRequest::new(mutation_id(id)?, mode);
                    let result = shutdown_daemon(&data_dir, &request).await?;
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
                    let exit_code = start_daemon(&data_dir, json_output).await?;
                    Ok(exit_code)
                }
            }
        }
        CliCommand::Mcp(McpArgs {
            http,
            require_agent,
            port,
            public_url,
            managed_child,
        }) => {
            if require_agent && http {
                return Err(Failure::new(
                    2,
                    "--require-agent is only valid with MCP stdio",
                ));
            }
            if json_output {
                return Err(Failure::new(
                    2,
                    "--json is not valid with mcp; stdout carries protocol/ready output only",
                ));
            }
            if !http {
                if port != 0 || managed_child || public_url.is_some() {
                    return Err(Failure::new(
                        2,
                        "--port, --public-url and --managed-child require --http",
                    ));
                }
                mcp::run_stdio(data_dir, require_agent)
                    .await
                    .map_err(Failure::new_from_message)?;
                return Ok(0);
            }
            if managed_child && public_url.is_none() {
                return Err(Failure::new(2, "--managed-child requires --public-url"));
            }
            mcp::run_http(data_dir, port, managed_child, public_url)
                .await
                .map_err(Failure::new_from_message)?;
            Ok(0)
        }
        CliCommand::Status => {
            let params = DaemonStatusRequest::default();
            params
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: DaemonStatusResponse =
                eliot_daemon::client_call_typed(&data_dir, "daemon.status", &params)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            params
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, false))?;
            let mut value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "daemon.status response is invalid"))?;
            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "data_dir".to_owned(),
                    Value::String(data_dir.to_string_lossy().into_owned()),
                );
            }
            print_value(&value, json_output);
            Ok(0)
        }
        CliCommand::Provider(ProviderArgs { command }) => match command {
            ProviderCommand::Holds(ProviderHoldsArgs { after, limit }) => {
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
            ProviderCommand::ClearHold(ProviderClearHoldArgs {
                scope,
                corrective_evidence,
                expected_probe_command,
            }) => {
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
        CliCommand::Project(ProjectArgs { command }) => match command {
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
            ProjectCommand::Show(ProjectShowArgs { id }) => {
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
        CliCommand::Rule(RuleArgs { command }) => match command {
            RuleCommand::Show(RuleShowArgs { id, project }) => {
                let request = match project {
                    Some(project) => RuleGetRequest::new(id).in_project(project),
                    None => RuleGetRequest::new(id),
                };
                read_and_print!("rule.get", request, RuleStateView)
            }
            RuleCommand::CatchUp(RuleCatchUpArgs {
                rule,
                after,
                through,
                project,
            }) => {
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
        CliCommand::Timer(TimerArgs { command }) => match command {
            TimerCommand::Show(TimerShowArgs { id, project }) => {
                let request = match project {
                    Some(project) => TimerGetRequest::new(id).in_project(project),
                    None => TimerGetRequest::new(id),
                };
                read_and_print!("timer.get", request, TimerStateView)
            }
            TimerCommand::Run(TimerRunArgs { rule, project }) => {
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
        CliCommand::Goal(GoalArgs { command }) => match command {
            GoalCommand::Set(GoalSetArgs {
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
            }) => {
                let request = GoalSetRequest {
                    id: mutation_id(id)?,
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
            GoalCommand::Show(GoalShowArgs { agent, mode }) => {
                let request = GoalGetRequest {
                    agent,
                    mode: goal_mode(&mode)?,
                };
                read_and_print!("goal.get", request, GoalView)
            }
            GoalCommand::Pause(GoalPauseArgs {
                agent,
                mode,
                reason,
            }) => {
                let request = GoalPauseRequest {
                    id: mutation_id(id)?,
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
            GoalCommand::Clear(GoalClearArgs { agent, mode }) => {
                let request = GoalClearRequest {
                    id: mutation_id(id)?,
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
            GoalCommand::Resume(GoalResumeArgs { agent, mode }) => {
                let request = GoalResumeRequest {
                    id: mutation_id(id)?,
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
            GoalCommand::Round(GoalRoundArgs {
                agent,
                mode,
                idle_seq,
                slot,
            }) => {
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
        CliCommand::Discover(DiscoverArgs {
            profile,
            harness,
            scope,
            cursor,
            archived,
            page: PageArgs { limit },
        }) => {
            validate_limit(limit)?;
            let params = AgentDiscoverRequest {
                profile,
                harness,
                native_scope: scope,
                cursor,
                archived,
                limit,
            };
            read_and_print!("agent.discover", params, DiscoverPage)
        }
        CliCommand::Attach(AttachArgs {
            profile,
            harness,
            scope,
            native_ref,
            project,
        }) => {
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
        CliCommand::Launch(LaunchArgs {
            profile,
            workspace,
            worktree,
            base,
            parent,
            prompt,
            wait,
            timeout_seconds,
        }) => {
            if let Some(name) = &profile
                && name.is_empty()
            {
                return Err(Failure::new(2, "--profile must not be empty"));
            }
            if profile.is_none() && parent.is_none() {
                return Err(Failure::new(
                    2,
                    "--profile is required for a parentless launch",
                ));
            }
            validate_text(prompt.as_deref())?;
            let deadline = wait_deadline(timeout_seconds)?;
            let mut args = json!({
                "workspace": workspace.to_string_lossy(),
            });
            if let Some(profile) = profile {
                args["profile"] = Value::String(profile);
            }
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
                let turn = wait_for_linked_turn(&data_dir, root_id, linked, deadline).await?;
                let exit_code = turn_exit_code(&turn);
                (turn_value(turn)?, exit_code)
            } else {
                // Promptless launch carries no turn to judge: readiness is
                // the root command applying, not a turn outcome.
                let command = wait_for_command(&data_dir, root_id, deadline).await?;
                let exit_code = response_exit_code(command.state);
                (command_value(command)?, exit_code)
            };
            print_value(&completed, json_output);
            Ok(exit_code)
        }
        CliCommand::Send(SendArgs {
            agent,
            text,
            wait,
            probe,
            generation: GenerationArgs { generation },
            timeout_seconds,
        }) => {
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
        CliCommand::Resume(ResumeArgs {
            agent,
            prompt,
            wait,
            generation: GenerationArgs { generation },
            timeout_seconds,
        }) => {
            validate_text(prompt.as_deref())?;
            let deadline = if wait {
                Some(wait_deadline(timeout_seconds)?)
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
                let turn = wait_for_linked_turn(&data_dir, &submitted.command_id, linked, deadline)
                    .await?;
                let exit_code = turn_exit_code(&turn);
                (turn_value(turn)?, exit_code)
            } else {
                // Promptless resume carries no turn to judge: readiness is
                // the root command applying, not a turn outcome.
                let command = wait_for_command(&data_dir, &submitted.command_id, deadline).await?;
                let exit_code = response_exit_code(command.state);
                (command_value(command)?, exit_code)
            };
            print_value(&completed, json_output);
            Ok(exit_code)
        }
        CliCommand::Steer(SteerArgs {
            agent,
            text,
            generation: GenerationArgs { generation },
            turn,
        }) => {
            validate_text(Some(&text))?;
            let (generation, turn, capture) =
                resolve_exact_turn_target(&data_dir, "steer", &agent, generation, turn).await?;
            let mut params = submit_request(id, Some(agent), "steer", json!({ "text": text }))?;
            params.expected_generation = generation;
            params.expected_turn_id = turn;
            submit_exact_turn_command(&data_dir, &params, json_output, capture.as_ref()).await
        }
        CliCommand::Inject(InjectCliArgs {
            agent,
            text,
            point,
            generation: GenerationArgs { generation },
            turn,
            expires_at_ms,
        }) => {
            validate_text(Some(&text))?;
            let point = match point.as_str() {
                "next_input" => InjectPoint::NextInput,
                "turn_safe_point" => InjectPoint::TurnSafePoint,
                "active_turn" => InjectPoint::ActiveTurn,
                _ => {
                    return Err(Failure::new(
                        2,
                        "inject point must be next_input, turn_safe_point, or active_turn",
                    ));
                }
            };
            let args = serde_json::to_value(&InjectArgs {
                text,
                point,
                expires_at_ms,
            })
            .map_err(|_| Failure::new(2, "inject args could not be encoded"))?;
            let mut params = submit_request(id, Some(agent), "inject", args)?;
            params.expected_generation = generation;
            params.expected_turn_id = turn;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Interrupt(InterruptArgs {
            agent,
            generation: GenerationArgs { generation },
            turn,
        }) => {
            let (generation, turn, capture) =
                resolve_exact_turn_target(&data_dir, "interrupt", &agent, generation, turn).await?;
            let mut params = submit_request(id, Some(agent), "interrupt", json!({}))?;
            params.expected_generation = generation;
            params.expected_turn_id = turn;
            submit_exact_turn_command(&data_dir, &params, json_output, capture.as_ref()).await
        }
        CliCommand::Approve(ApproveArgs {
            agent,
            generation: GenerationArgs { generation },
            request,
            choice,
            feedback,
        }) => {
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
        CliCommand::Deny(DenyArgs {
            agent,
            generation: GenerationArgs { generation },
            request,
            choice,
            feedback,
        }) => {
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
        CliCommand::Answer(AnswerArgs {
            agent,
            answers_file,
            generation: GenerationArgs { generation },
            request,
        }) => {
            let answers = read_answers_file(&answers_file)?;
            let mut params =
                submit_request(id, Some(agent), "answer", json!({ "answers": answers }))?;
            params.expected_generation = generation;
            params.request_id = request;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Ls(LsArgs {
            after,
            page: PageArgs { limit },
            filter,
        }) => {
            validate_limit(limit)?;
            let params = list_params(after, limit, false, filter)?;
            if json_output {
                read_and_print!("agent.list", params, AgentListResponse)
            } else {
                let response = read_agent_list(&data_dir, &params).await?;
                print_agent_table(&response, false);
                Ok(0)
            }
        }
        CliCommand::Top(TopArgs {
            after,
            page: PageArgs { limit },
            filter,
        }) => {
            validate_limit(limit)?;
            let params = list_params(after, limit, true, filter)?;
            if json_output {
                read_and_print!("agent.list", params, AgentListResponse)
            } else {
                let response = read_agent_list(&data_dir, &params).await?;
                print_agent_table(&response, true);
                Ok(0)
            }
        }
        CliCommand::Show(ShowArgs { agent }) => {
            read_and_print!("agent.get", AgentGetRequest::new(agent), AgentGetResponse)
        }
        CliCommand::Processes(ProcessesArgs {
            agent,
            generation: ReadGenerationArgs { generation },
            scope,
            after,
            page: PageArgs { limit },
        }) => {
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
        CliCommand::Stats(StatsArgs {
            agent,
            generation: ReadGenerationArgs { generation },
        }) => {
            let params = AgentStatsRequest {
                id: agent,
                generation,
            };
            read_and_print!("agent.stats", params, AgentStatsResponse)
        }
        CliCommand::GitStatus(GitStatusArgs {
            agent,
            generation: ReadGenerationArgs { generation },
        }) => {
            let params = AgentGitRequest {
                id: agent,
                generation,
            };
            read_and_print!("agent.git", params, AgentGitResponse)
        }
        CliCommand::Edits(EditsArgs {
            agent,
            generation: ReadGenerationArgs { generation },
            after,
            page: PageArgs { limit },
        }) => {
            validate_limit(limit)?;
            let params = AgentEditsRequest {
                id: agent,
                generation,
                after,
                limit,
            };
            read_and_print!("agent.edits", params, AgentEditsResponse)
        }
        CliCommand::Watch(WatchArgs { agent, after }) => {
            watch(&data_dir, agent.as_deref(), after, json_output).await
        }
        CliCommand::Result(ResultArgs {
            agent,
            generation: GenerationArgs { generation },
            turn_id,
            page: PageArgs { limit },
        }) => {
            validate_limit(limit)?;
            let params = AgentResultRequest {
                id: agent,
                generation,
                turn_id,
                limit,
            };
            read_and_print!("agent.result", params, AgentResultResponse)
        }
        CliCommand::History(HistoryArgs {
            agent,
            generation: ReadGenerationArgs { generation },
            source,
            view,
            turn_id,
            cursor,
            page: PageArgs { limit },
        }) => {
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
        CliCommand::Detach(DetachArgs { agent, generation }) => {
            let mut params = submit_request(id, Some(agent), "detach", json!({}))?;
            params.expected_generation = generation;
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Stop(StopArgs {
            agent,
            force,
            tree,
            generation: GenerationArgs { generation },
        }) => {
            let mut params = submit_request(id, Some(agent), "stop", json!({ "force": force }))?;
            if tree {
                params.selector = params.target.take().map(|agent| format!("tree:{agent}"));
            } else {
                params.expected_generation = generation;
            }
            submit_and_print(&data_dir, &params, json_output).await
        }
        CliCommand::Worktree(WorktreeArgs { command }) => match command {
            WorktreeCommand::Remove(WorktreeRemoveArgs {
                agent,
                generation: ReadGenerationArgs { generation },
            }) => {
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
        CliCommand::Cmd(CmdArgs { command }) => match command {
            CmdCommand::Show(CmdShowArgs { id }) => {
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
            CmdCommand::Children(CmdChildrenArgs {
                id,
                after,
                page: PageArgs { limit },
            }) => {
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
            CmdCommand::Resolve(CmdResolveArgs {
                command_id,
                resolution,
                reason,
            }) => {
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
        CliCommand::Turn(TurnArgs { command }) => match command {
            TurnCommand::Resolve(TurnResolveArgs {
                agent,
                generation,
                turn_id,
                status,
                reason,
            }) => {
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
        CliCommand::Events(EventsArgs {
            since,
            page: PageArgs { limit },
        }) => {
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
        CliCommand::Wait(WaitArgs {
            all,
            any,
            timeout_seconds,
            ids,
        }) => swarm_wait(&data_dir, all, any, timeout_seconds, ids, json_output).await,
        CliCommand::Msg(MsgArgs { command }) => match command {
            MsgCommand::Send(MsgSendArgs {
                to,
                text,
                thread,
                reply_to,
                mode,
            }) => {
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
            MsgCommand::Show(MsgShowArgs { delivery, agent }) => {
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
            MsgCommand::Ack(MsgAckArgs { delivery, agent }) => {
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
        CliCommand::Inbox(InboxArgs {
            agent,
            limit,
            after,
            wait,
        }) => {
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
        CliCommand::Var(VarArgs { command }) => match command {
            VarCommand::Set(VarSetArgs {
                key,
                value,
                value_file,
                scope,
                as_json,
                expect_version,
                note,
            }) => {
                let raw_value = match (value, value_file) {
                    (Some(value), None) => value,
                    (None, Some(path)) => read_var_value_file(&path)?,
                    _ => {
                        return Err(Failure::new(
                            2,
                            "provide exactly one of VALUE or --value-file",
                        ));
                    }
                };
                let value = if as_json {
                    serde_json::from_str(&raw_value)
                        .map_err(|_| Failure::new(2, "--as-json value must be valid JSON"))?
                } else {
                    Value::String(raw_value)
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
            VarCommand::Get(VarGetArgs { key, scope }) => {
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
        CliCommand::Who(WhoArgs { project, history }) => {
            run_who_command(&data_dir, json_output, project, history).await
        }
        CliCommand::Claim(ClaimArgs { command }) => {
            run_claim_command(&data_dir, json_output, id, command).await
        }
        CliCommand::Plugin(_) => Err(Failure::new(
            1,
            "plugin commands must be dispatched before CLI dispatch",
        )),
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
        ClaimCommand::Set(ClaimSetArgs {
            glob,
            agent,
            note,
            ttl_secs,
        }) => {
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
        ClaimCommand::List(ClaimListArgs { project, agent }) => {
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
        ClaimCommand::Release(ClaimReleaseArgs { glob, agent }) => {
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

async fn start_daemon(data_dir: &Path, json_output: bool) -> Result<u8, Failure> {
    let start_ms = daemon_now_ms();
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
    let data_dir_value = Value::String(data_dir.to_string_lossy().into_owned());
    if json_output {
        print_value(
            &json!({
                "state": "starting",
                "data_dir": data_dir_value,
                "log": log_path.to_string_lossy(),
            }),
            true,
        );
    } else {
        println!(
            "daemon starting; data_dir: {}; diagnostics: {}",
            data_dir.display(),
            log_path.display()
        );
    }
    wait_for_daemon_ready(data_dir, &log_path, start_ms, json_output).await
}

fn daemon_now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis())
        .unwrap_or(0)
}

const STARTUP_WAIT: Duration = Duration::from_secs(30);
const STARTUP_POLL: Duration = Duration::from_millis(200);
const STARTUP_RPC_TIMEOUT: Duration = Duration::from_secs(2);

async fn wait_for_daemon_ready(
    data_dir: &Path,
    log_path: &Path,
    start_ms: u128,
    json_output: bool,
) -> Result<u8, Failure> {
    let deadline = Instant::now() + STARTUP_WAIT;
    loop {
        if let Some(failure) = eliot_daemon::daemon_failure_since(data_dir, start_ms) {
            return Err(startup_failure(failure, data_dir, log_path));
        }
        let status = tokio::time::timeout(
            STARTUP_RPC_TIMEOUT,
            eliot_daemon::client_call_typed::<DaemonStatusRequest, DaemonStatusResponse>(
                data_dir,
                "daemon.status",
                &DaemonStatusRequest::default(),
            ),
        )
        .await;
        if let Ok(Ok(_)) = status {
            let boot_id = read_daemon_boot_id(data_dir);
            if json_output {
                print_value(
                    &json!({
                        "state": "ready",
                        "data_dir": data_dir.to_string_lossy(),
                        "boot_id": boot_id.unwrap_or_default(),
                        "log": log_path.to_string_lossy(),
                    }),
                    true,
                );
            } else if let Some(boot_id) = boot_id {
                println!(
                    "daemon ready; data_dir: {}; boot_id: {}",
                    data_dir.display(),
                    boot_id
                );
            } else {
                println!("daemon ready; data_dir: {}", data_dir.display());
            }
            return Ok(0);
        }
        if Instant::now() >= deadline {
            break;
        }
        sleep(STARTUP_POLL.min(deadline.saturating_duration_since(Instant::now()))).await;
        if Instant::now() >= deadline {
            break;
        }
    }
    if let Some(failure) = eliot_daemon::daemon_failure_since(data_dir, start_ms) {
        return Err(startup_failure(failure, data_dir, log_path));
    }
    Err(Failure::new(
        1,
        format!(
            "daemon did not become ready within 30s; data_dir: {}; diagnostics: {}",
            data_dir.display(),
            log_path.display()
        ),
    ))
}

fn read_daemon_boot_id(data_dir: &Path) -> Option<String> {
    let bytes = std::fs::read(data_dir.join("daemon.json")).ok()?;
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("boot_id")
        .and_then(Value::as_str)
        .filter(|boot_id| !boot_id.is_empty() && boot_id.len() <= 256)
        .map(str::to_owned)
}

fn startup_failure(
    failure: eliot_daemon::DaemonStartupFailure,
    data_dir: &Path,
    log_path: &Path,
) -> Failure {
    let mut result = Failure::new(
        1,
        format!(
            "daemon failed during startup: {}; data_dir: {}; diagnostics: {}",
            failure.error_code,
            data_dir.display(),
            log_path.display()
        ),
    );
    result.diagnostics = failure.diagnostics;
    eliot_core::bound_stderr_diagnostics(
        &mut result.diagnostics,
        eliot_core::MAX_STDERR_DIAGNOSTIC_BYTES,
    );
    result
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

/// An exact turn binding the CLI captured from the Agent's current generation
/// and active turn for an omitted `--generation`/`--turn` flag. The captured
/// identity is submitted unchanged; a later rejection means the turn moved.
struct ExactTurnCapture {
    verb: &'static str,
    agent: String,
    generation: i64,
    turn_id: String,
}

impl ExactTurnCapture {
    /// A captured submit rejected as `stale_generation` lost a race: the
    /// generation or active turn changed between the capture read and
    /// admission. The command was rejected before any effect, so the CLI
    /// reports the race and never retries against the new turn.
    fn race(&self) -> Failure {
        Failure::new(
            3,
            format!(
                "{} race: Agent {} turn changed after capture \
                 (submitted generation={} turn={}); \
                 rejected before any effect, not retried",
                self.verb, self.agent, self.generation, self.turn_id
            ),
        )
    }
}

fn missing_turn_failure(verb: &str, agent: &str) -> Failure {
    Failure::new(
        3,
        format!("{verb} requires an active turn: Agent {agent} has no active turn"),
    )
}

/// A `stale_generation` rejection of a captured exact-target submit is a race,
/// not a generic rejection. Every other failure passes through unchanged.
fn race_or_failure(capture: Option<&ExactTurnCapture>, failure: Failure) -> Failure {
    match capture {
        Some(capture) if failure.code.as_deref() == Some("stale_generation") => {
            let mut race = capture.race();
            race.diagnostics = failure.diagnostics;
            race
        }
        _ => failure,
    }
}

/// Read the Agent's current generation and active turn for an omitted
/// `--generation`/`--turn` flag. Fails clearly when there is no active turn.
async fn capture_exact_turn(
    data_dir: &Path,
    verb: &'static str,
    agent: &str,
) -> Result<ExactTurnCapture, Failure> {
    let params = AgentGetRequest::new(agent);
    params
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: AgentGetResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.get", &params)
            .await
            .map_err(|error| Failure::from_core(error, false))?;
    params
        .validate_response(&response)
        .map_err(|error| Failure::from_core(error, false))?;
    let turn_id = response
        .agent
        .active_turn
        .clone()
        .ok_or_else(|| missing_turn_failure(verb, agent))?;
    Ok(ExactTurnCapture {
        verb,
        agent: agent.to_owned(),
        generation: response.agent.generation,
        turn_id,
    })
}

/// Resolve the exact binding a turn-control command submits. Explicit flags
/// are kept unchanged with no extra read; each omitted flag is filled from a
/// single capture read, which is printed before submitting.
async fn resolve_exact_turn_target(
    data_dir: &Path,
    verb: &'static str,
    agent: &str,
    generation: Option<i64>,
    turn: Option<String>,
) -> Result<(Option<i64>, Option<String>, Option<ExactTurnCapture>), Failure> {
    if generation.is_some() && turn.is_some() {
        return Ok((generation, turn, None));
    }
    let capture = capture_exact_turn(data_dir, verb, agent).await?;
    let resolved_generation = generation.unwrap_or(capture.generation);
    let resolved_turn = turn.unwrap_or_else(|| capture.turn_id.clone());
    eprintln!("{verb} target: agent={agent} generation={resolved_generation} turn={resolved_turn}");
    let submitted = ExactTurnCapture {
        verb,
        agent: agent.to_owned(),
        generation: resolved_generation,
        turn_id: resolved_turn.clone(),
    };
    Ok((
        Some(resolved_generation),
        Some(resolved_turn),
        Some(submitted),
    ))
}

async fn submit_exact_turn_command(
    data_dir: &Path,
    params: &SubmitCommand,
    json_output: bool,
    capture: Option<&ExactTurnCapture>,
) -> Result<u8, Failure> {
    match submit_command(data_dir, params).await {
        Ok(submitted) => {
            print_value(&submit_value(&submitted)?, json_output);
            Ok(response_exit_code(submitted.state))
        }
        Err(failure) => Err(race_or_failure(capture, failure)),
    }
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
    if !answers.is_object() {
        return Err(Failure::new(
            2,
            "answers file must be an object keyed by question id",
        ));
    }
    eliot_core::requests::parse_answers_map(&answers).map_err(|_| {
        Failure::new(
            2,
            "each answer entry must contain only an answers array of strings",
        )
    })?;
    Ok(answers)
}

fn read_var_value_file(path: &Path) -> Result<String, Failure> {
    let file = File::open(path).map_err(|_| Failure::new(2, "value file could not be read"))?;
    let mut bounded = file.take(MAX_VAR_VALUE_BYTES as u64 + 1);
    let mut bytes = Vec::new();
    let byte_count = bounded
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::new(2, "value file could not be read"))?;
    if byte_count > MAX_VAR_VALUE_BYTES {
        return Err(Failure::new(
            2,
            format!("value file exceeds the {MAX_VAR_VALUE_BYTES} byte limit"),
        ));
    }
    String::from_utf8(bytes).map_err(|_| Failure::new(2, "value file must be UTF-8"))
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

/// Wait for a launch/resume linked SEND turn. When the linked input is
/// cancelled because its parent start failed, report the parent launch
/// failure rather than only the cancellation.
async fn wait_for_linked_turn(
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
const INTERRUPTED_EXIT_CODE: u8 = 7;

async fn sleep_before_deadline(deadline: Instant) -> Result<(), Failure> {
    timeout_at(deadline, sleep(WAIT_POLL_INTERVAL))
        .await
        .map_err(|_| Failure::timeout())
}

fn response_exit_code(state: CommandState) -> u8 {
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
fn turn_exit_code(turn: &CoreTurn) -> u8 {
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

/// Bounded wait over a frozen set of child commands/turns. Positional ids are
/// command ids, or `agent/generation/turn_id` for exact turn bindings. The set
/// is frozen client-side before the RPC; the daemon never adds later turns or
/// children. Exit 0 only when every entry succeeded (`--all`) or at least one
/// succeeded (`--any`); timeout exits 5 with the honest per-entry snapshot.
async fn swarm_wait(
    data_dir: &Path,
    all: bool,
    any: bool,
    timeout_seconds: u64,
    ids: Vec<String>,
    json_output: bool,
) -> Result<u8, Failure> {
    if all == any {
        return Err(Failure::new(
            2,
            "swarm wait requires exactly one of --all or --any",
        ));
    }
    if timeout_seconds == 0 || timeout_seconds > 30 {
        return Err(Failure::new(
            2,
            "--timeout-seconds must be between 1 and 30",
        ));
    }
    if ids.is_empty() || ids.len() > MAX_SWARM_WAIT_TARGETS {
        return Err(Failure::new(2, "swarm wait requires 1..=256 captured ids"));
    }
    let mode = if all { "all" } else { "any" };
    let mut commands = Vec::new();
    let mut turns = Vec::new();
    for id in ids {
        if id.contains('/') {
            turns.push(parse_wait_turn_ref(&id)?);
        } else {
            if id.trim().is_empty() {
                return Err(Failure::new(2, "swarm wait ids must be non-empty strings"));
            }
            commands.push(id);
        }
    }
    if commands.is_empty() && turns.is_empty() {
        return Err(Failure::new(2, "swarm wait requires 1..=256 captured ids"));
    }
    let wait_ms = timeout_seconds.saturating_mul(1_000);
    let params = SwarmWaitRequest::new(commands, turns, mode, wait_ms);
    params
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: SwarmWaitResponse =
        eliot_daemon::client_call_typed(data_dir, "swarm.wait", &params)
            .await
            .map_err(|error| Failure::from_core(error, false))?;
    params
        .validate_response(&response)
        .map_err(|_| Failure::new(4, "swarm.wait response is invalid"))?;
    let value = serde_json::to_value(&response)
        .map_err(|_| Failure::new(4, "swarm.wait response is invalid"))?;
    print_value(&value, json_output);
    Ok(swarm_wait_exit_code(&response))
}

fn parse_wait_turn_ref(id: &str) -> Result<WaitTurnRef, Failure> {
    let mut parts = id.splitn(3, '/');
    let agent = parts.next().unwrap_or_default();
    let generation = parts.next().unwrap_or_default();
    let turn_id = parts.next().unwrap_or_default();
    if agent.trim().is_empty() || turn_id.trim().is_empty() || generation.trim().is_empty() {
        return Err(Failure::new(2, "turn ids must be agent/generation/turn_id"));
    }
    let generation: i64 = generation.parse().map_err(|_| {
        Failure::new(
            2,
            "turn ids must be agent/generation/turn_id with a nonnegative generation",
        )
    })?;
    if generation < 0 {
        return Err(Failure::new(
            2,
            "turn ids must be agent/generation/turn_id with a nonnegative generation",
        ));
    }
    let target = WaitTurnRef::new(agent, generation, turn_id);
    target
        .validate()
        .map_err(|_| Failure::new(2, "turn ids must be agent/generation/turn_id"))?;
    Ok(target)
}

/// Exit code for a settled `swarm wait`. Timeout (or an uncompleted snapshot)
/// exits 5; otherwise 0 only when every entry succeeded (`all`) or at least
/// one succeeded (`any`). Interrupted entries exit 7, unknown/not_found/
/// not_visible entries exit 4, and plain failures exit 1.
fn swarm_wait_exit_code(response: &SwarmWaitResponse) -> u8 {
    if response.timed_out || !response.completed {
        return 5;
    }
    let succeeded = response
        .results
        .iter()
        .filter(|entry| entry.status == "succeeded")
        .count();
    if response.mode == "any" && succeeded > 0 {
        return 0;
    }
    if response.mode == "all" && succeeded == response.results.len() && !response.results.is_empty()
    {
        return 0;
    }
    if response
        .results
        .iter()
        .any(|entry| entry.status == "interrupted")
    {
        return INTERRUPTED_EXIT_CODE;
    }
    if response.results.iter().any(|entry| {
        matches!(
            entry.status.as_str(),
            "unknown" | "not_found" | "not_visible"
        )
    }) {
        return 4;
    }
    1
}

fn command_value(command: CoreCommand) -> Result<Value, Failure> {
    serde_json::to_value(command).map_err(|_| Failure::new(4, "command readback is unavailable"))
}

fn turn_value(turn: CoreTurn) -> Result<Value, Failure> {
    serde_json::to_value(turn).map_err(|_| Failure::new(4, "turn readback is unavailable"))
}

async fn watch(
    data_dir: &Path,
    agent: Option<&str>,
    after: Option<i64>,
    json_output: bool,
) -> Result<u8, Failure> {
    let filter = EventsFilter {
        agents: agent.into_iter().map(str::to_owned).collect(),
        ..EventsFilter::default()
    };
    let mut after = match after {
        Some(after) => after,
        None => {
            let params = EventsBoundsRequest {
                filter: filter.clone(),
            };
            params
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let bounds: EventsBoundsResponse =
                eliot_daemon::client_call_typed(data_dir, "events.bounds", &params)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            bounds
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            bounds.hi
        }
    };
    loop {
        let params = EventsSubscribeRequest {
            after,
            limit: PAGE_SIZE,
            wait_ms: 1_000,
            filter: filter.clone(),
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

/// Build a compact `agent.list` request from CLI paging, scope filters and the
/// observations flag behind `swarm top`. Unknown state names reject here so a
/// typo never reads back as an empty fleet.
fn list_params(
    after: Option<String>,
    limit: usize,
    observations: bool,
    filter: ListFilterArgs,
) -> Result<AgentListRequest, Failure> {
    let state = filter
        .state
        .map(|value| {
            serde_json::from_value::<AgentState>(Value::String(value)).map_err(|_| {
                Failure::new(
                    2,
                    "state must be unknown, created, starting, idle, busy, waiting, stopped, failed or lost",
                )
            })
        })
        .transpose()?;
    Ok(AgentListRequest {
        after,
        limit,
        observations,
        state,
        project: filter.project,
        harness: filter.harness,
        parent: filter.parent,
        root: filter.root,
    })
}

async fn read_agent_list(
    data_dir: &Path,
    params: &AgentListRequest,
) -> Result<AgentListResponse, Failure> {
    params
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: AgentListResponse =
        eliot_daemon::client_call_typed(data_dir, "agent.list", params)
            .await
            .map_err(|error| Failure::from_core(error, false))?;
    params
        .validate_response(&response)
        .map_err(|error| Failure::from_core(error, false))?;
    Ok(response)
}

/// Readable fleet table for `swarm ls` (summaries) and `swarm top` (plus
/// child activity, last-terminal usage and observation data availability).
/// `ACT_TURN` is the live active turn; `LAST_OUT`/`LAST_TOK` belong to the
/// last terminal turn (`last_turn_id`), not the active one. `DATA` reports
/// observation data availability only, never freshness or liveness. `--json`
/// keeps printing the raw response; this table never replaces it.
fn print_agent_table(response: &AgentListResponse, with_observations: bool) {
    if with_observations {
        println!(
            "{:<6} {:<4} {:<9} {:<11} {:<12} {:<6} {:<11} {:<8} {:<12}",
            "ID", "GEN", "STATE", "ACTIVITY", "ACT_TURN", "AGE", "LAST_OUT", "LAST_TOK", "DATA"
        );
    } else {
        println!(
            "{:<6} {:<4} {:<9} {:<14} {:<9} {:<12} {:<6} {:<11} {:<6}",
            "ID", "GEN", "STATE", "PROJECT", "HARNESS", "ACT_TURN", "AGE", "LAST_OUT", "SEEN"
        );
    }
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0);
    for row in &response.agents {
        let summary = &row.summary;
        let turn = summary.active_turn.as_deref().unwrap_or("-");
        let age = age_cell(summary.progress_age_ms);
        let outcome = summary
            .last_outcome
            .map(|outcome| outcome.as_str())
            .unwrap_or("-");
        if with_observations {
            let activity = summary
                .activity
                .as_ref()
                .map(|activity| activity_state_name(activity.state))
                .unwrap_or("-");
            let tokens = summary
                .usage
                .as_ref()
                .and_then(|usage| usage.total_tokens)
                .map(|tokens| tokens.to_string())
                .unwrap_or_else(|| "-".to_owned());
            let observation = row
                .observation
                .as_ref()
                .map(|view| match view {
                    eliot_core::api::ObservationView::Data(_) => "data".to_owned(),
                    eliot_core::api::ObservationView::Unavailable { error_code, .. } => {
                        truncate_cell(error_code, 12)
                    }
                })
                .unwrap_or_else(|| "-".to_owned());
            println!(
                "{:<6} {:<4} {:<9} {:<11} {:<12} {:<6} {:<11} {:<8} {:<12}",
                truncate_cell(&summary.id, 6),
                summary.generation,
                state_cell(summary.state),
                activity,
                truncate_cell(turn, 12),
                age,
                outcome,
                tokens,
                observation,
            );
        } else {
            let seen = age_cell(
                now_ms
                    .checked_sub(summary.updated_at)
                    .filter(|age| *age >= 0),
            );
            println!(
                "{:<6} {:<4} {:<9} {:<14} {:<9} {:<12} {:<6} {:<11} {:<6}",
                truncate_cell(&summary.id, 6),
                summary.generation,
                state_cell(summary.state),
                truncate_cell(&summary.project, 14),
                truncate_cell(&summary.harness, 9),
                truncate_cell(turn, 12),
                age,
                outcome,
                seen,
            );
        }
    }
    if response.agents.is_empty() {
        println!("(no Agents in scope for these filters)");
    }
    println!(
        "total {} shown {} truncated {} next {} (full record: swarm show <id>)",
        response.total,
        response.agents.len(),
        yes_no(response.truncated),
        response.next.as_deref().unwrap_or("-"),
    );
}

fn state_cell(state: AgentState) -> &'static str {
    match state {
        AgentState::Unknown => "unknown",
        AgentState::Created => "created",
        AgentState::Starting => "starting",
        AgentState::Idle => "idle",
        AgentState::Busy => "busy",
        AgentState::Waiting => "waiting",
        AgentState::Stopped => "stopped",
        AgentState::Failed => "failed",
        AgentState::Lost => "lost",
    }
}

fn activity_state_name(state: NativeChildActivityState) -> &'static str {
    match state {
        NativeChildActivityState::Running => "running",
        NativeChildActivityState::Idle => "idle",
        NativeChildActivityState::Interrupted => "interrupted",
        NativeChildActivityState::Lost => "lost",
        NativeChildActivityState::Unknown => "unknown",
    }
}

fn age_cell(age_ms: Option<i64>) -> String {
    let Some(age_ms) = age_ms else {
        return "-".to_owned();
    };
    if age_ms < 0 {
        return "-".to_owned();
    }
    let seconds = age_ms / 1_000;
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}

fn truncate_cell(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_owned();
    }
    if width == 0 {
        return String::new();
    }
    value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "~"
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_params_rejects_unknown_state_before_any_call() {
        let filter = |state: Option<&str>| ListFilterArgs {
            state: state.map(str::to_owned),
            project: None,
            harness: None,
            parent: None,
            root: None,
        };
        let params = list_params(None, 10, false, filter(Some("busy"))).expect("busy parses");
        assert_eq!(params.state, Some(AgentState::Busy));
        assert!(list_params(None, 10, false, filter(Some("done"))).is_err());
        assert!(list_params(None, 10, false, filter(Some("BUSY"))).is_err());
    }

    #[test]
    fn fleet_table_cells_stay_compact() {
        assert_eq!(age_cell(None), "-");
        assert_eq!(age_cell(Some(-1)), "-");
        assert_eq!(age_cell(Some(45_000)), "45s");
        assert_eq!(age_cell(Some(180_000)), "3m");
        assert_eq!(age_cell(Some(7_200_000)), "2h");
        assert_eq!(age_cell(Some(172_800_000)), "2d");
        assert_eq!(truncate_cell("a1", 6), "a1");
        assert_eq!(truncate_cell("alongprojectname", 6), "along~");
        assert_eq!(state_cell(AgentState::Busy), "busy");
        assert_eq!(activity_state_name(NativeChildActivityState::Lost), "lost");
    }

    #[test]
    fn swarm_wait_parse_and_exit_codes() {
        use eliot_core::api::WaitEntryResult;

        let turn = parse_wait_turn_ref("a1/2/turn-9").expect("turn parses");
        assert_eq!(turn.agent, "a1");
        assert_eq!(turn.generation, 2);
        assert_eq!(turn.turn_id, "turn-9");
        // Turn ids may contain slashes after the generation.
        let slashed = parse_wait_turn_ref("a1/2/turn/with/slash").expect("slashed turn parses");
        assert_eq!(slashed.turn_id, "turn/with/slash");
        for bad in ["a1", "a1/", "/1/t", "a1/x/t", "a1/-1/t", "a1/1/", " /1/t"] {
            assert!(parse_wait_turn_ref(bad).is_err(), "id: {bad}");
        }
        let completed = |mode: &str, statuses: &[&str]| SwarmWaitResponse {
            mode: mode.to_owned(),
            completed: true,
            timed_out: false,
            results: statuses
                .iter()
                .enumerate()
                .map(|(index, status)| {
                    WaitEntryResult::new("command", format!("c{index}"), (*status).to_owned())
                })
                .collect(),
        };
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "succeeded"])),
            0
        );
        assert_eq!(
            swarm_wait_exit_code(&completed("any", &["failed", "succeeded"])),
            0
        );
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "failed"])),
            1
        );
        assert_eq!(swarm_wait_exit_code(&completed("any", &["failed"])), 1);
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "unknown"])),
            4
        );
        assert_eq!(swarm_wait_exit_code(&completed("any", &["not_found"])), 4);
        assert_eq!(
            swarm_wait_exit_code(&completed("all", &["succeeded", "interrupted"])),
            INTERRUPTED_EXIT_CODE
        );
        let timed_out = SwarmWaitResponse {
            mode: "all".to_owned(),
            completed: false,
            timed_out: true,
            results: vec![WaitEntryResult::new("command", "c0", "still_running")],
        };
        assert_eq!(swarm_wait_exit_code(&timed_out), 5);
        let cli = Cli::try_parse_from(["swarm", "wait", "--all", "--timeout-seconds", "7", "c1"])
            .unwrap();
        let Some(CliCommand::Wait(WaitArgs {
            all,
            any,
            timeout_seconds,
            ids,
        })) = cli.command
        else {
            panic!("wait command required");
        };
        assert!(all);
        assert!(!any);
        assert_eq!(timeout_seconds, 7);
        assert_eq!(ids, vec!["c1".to_owned()]);
    }

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
                CliCommand::Approve(ApproveArgs {
                    choice, feedback, ..
                }) => (choice, feedback),
                CliCommand::Deny(DenyArgs {
                    choice, feedback, ..
                }) => (choice, feedback),
                _ => panic!("expected an approve or deny command"),
            };
            assert_eq!(
                approval_reply_args(choice, feedback),
                serde_json::json!({"choice_id": "always", "feedback": "trust it"})
            );
        }
    }

    #[test]
    fn steer_and_interrupt_parse_without_exact_flags() {
        let cli = Cli::try_parse_from(["swarm", "steer", "a1", "hello"]).unwrap();
        let Some(CliCommand::Steer(SteerArgs {
            agent,
            text,
            generation: GenerationArgs { generation },
            turn,
        })) = cli.command
        else {
            panic!("steer command required");
        };
        assert_eq!(agent, "a1");
        assert_eq!(text, "hello");
        assert_eq!(generation, None);
        assert_eq!(turn, None);

        let cli = Cli::try_parse_from(["swarm", "interrupt", "a2"]).unwrap();
        let Some(CliCommand::Interrupt(InterruptArgs {
            agent,
            generation: GenerationArgs { generation },
            turn,
        })) = cli.command
        else {
            panic!("interrupt command required");
        };
        assert_eq!(agent, "a2");
        assert_eq!(generation, None);
        assert_eq!(turn, None);

        // `abort` stays an alias of `interrupt`.
        let cli = Cli::try_parse_from(["swarm", "abort", "a2"]).unwrap();
        assert!(matches!(cli.command, Some(CliCommand::Interrupt(_))));

        // Explicit bindings still parse for an exact submit with no capture.
        let cli = Cli::try_parse_from([
            "swarm",
            "steer",
            "a1",
            "hello",
            "--generation",
            "3",
            "--turn",
            "turn-9",
        ])
        .unwrap();
        let Some(CliCommand::Steer(SteerArgs {
            generation: GenerationArgs { generation },
            turn,
            ..
        })) = cli.command
        else {
            panic!("steer command required");
        };
        assert_eq!(generation, Some(3));
        assert_eq!(turn.as_deref(), Some("turn-9"));

        let cli = Cli::try_parse_from([
            "swarm",
            "interrupt",
            "a2",
            "--generation",
            "3",
            "--turn",
            "turn-9",
        ])
        .unwrap();
        let Some(CliCommand::Interrupt(InterruptArgs {
            generation: GenerationArgs { generation },
            turn,
            ..
        })) = cli.command
        else {
            panic!("interrupt command required");
        };
        assert_eq!(generation, Some(3));
        assert_eq!(turn.as_deref(), Some("turn-9"));
    }

    #[test]
    fn exact_turn_reports_missing_turn_and_race() {
        let missing = missing_turn_failure("steer", "a1");
        assert_eq!(missing.exit_code, 3);
        assert!(missing.message.contains("a1"), "{missing:?}");
        assert!(missing.message.contains("no active turn"), "{missing:?}");

        let capture = ExactTurnCapture {
            verb: "interrupt",
            agent: "a2".to_owned(),
            generation: 4,
            turn_id: "turn-9".to_owned(),
        };
        let race = capture.race();
        assert_eq!(race.exit_code, 3);
        assert!(race.message.contains("race"), "{race:?}");
        assert!(race.message.contains("a2"), "{race:?}");
        assert!(race.message.contains("generation=4"), "{race:?}");
        assert!(race.message.contains("turn-9"), "{race:?}");
        assert!(race.message.contains("not retried"), "{race:?}");
    }

    #[test]
    fn stale_rejection_of_a_capture_is_a_race() {
        let capture = ExactTurnCapture {
            verb: "steer",
            agent: "a1".to_owned(),
            generation: 2,
            turn_id: "turn-3".to_owned(),
        };
        let stale = Failure::from_core(CoreError::new("stale_generation", "turn moved"), true);
        assert_eq!(stale.exit_code, 3);
        let race = race_or_failure(Some(&capture), stale);
        assert_eq!(race.exit_code, 3);
        assert!(race.message.contains("race"), "{race:?}");
        assert!(race.message.contains("turn-3"), "{race:?}");

        // Explicit submits keep the untouched rejection: no capture, no race.
        let stale = Failure::from_core(CoreError::new("stale_generation", "turn moved"), true);
        let untouched = race_or_failure(None, stale);
        assert_eq!(untouched.exit_code, 3);
        assert!(!untouched.message.contains("race"), "{untouched:?}");

        // Non-stale failures pass through even after a capture.
        for code in ["bad_state", "not_found", "failed", "internal"] {
            let failure = Failure::from_core(CoreError::new(code, "other"), true);
            let expected_exit = failure.exit_code;
            let passed = race_or_failure(Some(&capture), failure);
            assert_eq!(passed.exit_code, expected_exit, "code={code}");
            assert!(!passed.message.contains("race"), "code={code}");
        }
    }
}
