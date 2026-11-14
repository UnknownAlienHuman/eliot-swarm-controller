//! Clap argument definitions for the `swarm` CLI, kept apart from dispatch.
use super::support::{DEFAULT_WAIT_SECONDS, PAGE_SIZE};
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Args)]
pub(crate) struct GenerationArgs {
    #[arg(long, value_name = "GENERATION")]
    pub(crate) generation: Option<i64>,
}

#[derive(Args)]
pub(crate) struct ReadGenerationArgs {
    #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) generation: Option<i64>,
}

#[derive(Args)]
pub(crate) struct PageArgs {
    #[arg(long, default_value_t = PAGE_SIZE)]
    pub(crate) limit: usize,
}

#[derive(Parser)]
#[command(
    name = "swarm",
    version,
    about = "Local controller for coding-agent sessions"
)]
pub(crate) struct Cli {
    /// Private Eliot data directory (defaults to ELIOT_HOME or the platform data location).
    #[arg(long, global = true, value_name = "PATH")]
    pub(crate) data_dir: Option<PathBuf>,

    /// Print compact JSON instead of the default readable output.
    #[arg(long, global = true)]
    pub(crate) json: bool,

    /// External ULID for a durable mutation. Eliot prints generated IDs before sending.
    #[arg(long, global = true, value_name = "ULID")]
    pub(crate) id: Option<String>,

    #[command(subcommand)]
    pub(crate) command: Option<CliCommand>,
}

// Keep payloads in typed Args: clap builds each payload in its own generated
// function instead of accumulating every argument builder in this enum's frame.
#[derive(Subcommand)]
pub(crate) enum CliCommand {
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
    /// List installed harness executables with versions. Starts no model
    /// turn and performs no inference.
    Harnesses,
    /// Read a harness native model catalogue through its own read-only
    /// method. Starts no model turn and performs no inference.
    Models(ModelsArgs),
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
    /// Read bounded usage totals with explicit coverage for one Agent, project or tree.
    Usage(UsageArgs),
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
    /// Switch a live Agent's model and/or effort for its next turn.
    Configure(ConfigureArgs),
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
pub(crate) struct McpArgs {
    /// Serve StreamableHTTP on 127.0.0.1 instead of stdio.
    #[arg(long)]
    pub(crate) http: bool,
    /// Require a complete inherited Agent identity for stdio calls; never use Owner or Run.
    #[arg(long)]
    pub(crate) require_agent: bool,
    /// Loopback port for --http; 0 selects an ephemeral port (actual address is printed in the ready frame).
    #[arg(long, default_value_t = 0)]
    pub(crate) port: u16,
    /// Public https tunnel URL (path /mcp) widening the --http Host/Origin checks; required with --managed-child.
    #[arg(long, value_name = "URL")]
    pub(crate) public_url: Option<String>,
    /// Managed-child lifecycle: also exit cleanly when stdin reaches EOF.
    #[arg(long, hide = true)]
    pub(crate) managed_child: bool,
}

#[derive(Args)]
pub(crate) struct ProviderArgs {
    #[command(subcommand)]
    pub(crate) command: ProviderCommand,
}

#[derive(Args)]
pub(crate) struct ProjectArgs {
    #[command(subcommand)]
    pub(crate) command: ProjectCommand,
}

#[derive(Args)]
pub(crate) struct RuleArgs {
    #[command(subcommand)]
    pub(crate) command: RuleCommand,
}

#[derive(Args)]
pub(crate) struct TimerArgs {
    #[command(subcommand)]
    pub(crate) command: TimerCommand,
}

#[derive(Args)]
pub(crate) struct GoalArgs {
    #[command(subcommand)]
    pub(crate) command: GoalCommand,
}

#[derive(Args)]
pub(crate) struct DiscoverArgs {
    #[arg(long, value_name = "PROFILE")]
    pub(crate) profile: String,
    #[arg(long, value_name = "HARNESS")]
    pub(crate) harness: Option<String>,
    #[arg(long, value_name = "SCOPE")]
    pub(crate) scope: Option<String>,
    #[arg(long, value_name = "CURSOR")]
    pub(crate) cursor: Option<String>,
    #[arg(long)]
    pub(crate) archived: bool,
    #[command(flatten)]
    pub(crate) page: PageArgs,
}

#[derive(Args)]
pub(crate) struct ModelsArgs {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    #[arg(long, value_name = "HARNESS")]
    pub(crate) harness: String,
}

#[derive(Args)]
pub(crate) struct AttachArgs {
    #[arg(long, value_name = "PROFILE")]
    pub(crate) profile: String,
    #[arg(long, value_name = "HARNESS")]
    pub(crate) harness: String,
    #[arg(long, value_name = "SCOPE")]
    pub(crate) scope: String,
    #[arg(long = "ref", value_name = "REF")]
    pub(crate) native_ref: String,
    #[arg(long, value_name = "LABEL")]
    pub(crate) project: Option<String>,
}

#[derive(Args)]
pub(crate) struct LaunchArgs {
    /// Named profile. Required for a parentless launch unless --harness is
    /// given for an Owner ad-hoc launch. An Agent child reuses its parent's
    /// frozen profile; the Owner may supply one.
    #[arg(long, value_name = "PROFILE", conflicts_with = "harness")]
    pub(crate) profile: Option<String>,
    /// Ad-hoc Owner launch harness (codex|claude|muse|opencode|antigravity).
    /// The daemon freezes a profile-equivalent snapshot from built-in
    /// per-harness defaults plus --model/--effort. Owner-only.
    #[arg(long, value_name = "HARNESS")]
    pub(crate) harness: Option<String>,
    /// Ad-hoc model id; omitted uses the harness native default.
    #[arg(long, value_name = "MODEL")]
    pub(crate) model: Option<String>,
    /// Ad-hoc reasoning effort; omitted uses the harness native default.
    /// Only Codex and Muse map effort; other harnesses reject it.
    #[arg(long, value_name = "EFFORT")]
    pub(crate) effort: Option<String>,
    #[arg(long, value_name = "PATH")]
    pub(crate) workspace: PathBuf,
    #[arg(long, value_name = "BRANCH")]
    pub(crate) worktree: Option<String>,
    #[arg(long, value_name = "REF", requires = "worktree")]
    pub(crate) base: Option<String>,
    /// Launch as a child of this Agent; the child inherits its project
    /// and, for an Agent principal, its parent's frozen profile.
    #[arg(long, value_name = "AGENT")]
    pub(crate) parent: Option<String>,
    #[arg(value_name = "PROMPT")]
    pub(crate) prompt: Option<String>,
    #[arg(long)]
    pub(crate) wait: bool,
    #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
    pub(crate) timeout_seconds: u64,
}

#[derive(Args)]
pub(crate) struct SendArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(value_name = "TEXT", allow_hyphen_values = true)]
    pub(crate) text: String,
    #[arg(long)]
    pub(crate) wait: bool,
    /// Explicitly claim one eligible provider-hold probe for this send.
    #[arg(long)]
    pub(crate) probe: bool,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
    pub(crate) timeout_seconds: u64,
}

#[derive(Args)]
pub(crate) struct ResumeArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(value_name = "PROMPT", allow_hyphen_values = true)]
    pub(crate) prompt: Option<String>,
    #[arg(long)]
    pub(crate) wait: bool,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
    pub(crate) timeout_seconds: u64,
}

#[derive(Args)]
pub(crate) struct SteerArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(value_name = "TEXT", allow_hyphen_values = true)]
    pub(crate) text: String,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    pub(crate) turn: Option<String>,
}

#[derive(Args)]
pub(crate) struct InjectCliArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(value_name = "TEXT", allow_hyphen_values = true)]
    pub(crate) text: String,
    #[arg(long, value_parser = ["next_input", "turn_safe_point", "active_turn"], default_value = "next_input")]
    pub(crate) point: String,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    pub(crate) turn: Option<String>,
    #[arg(long, value_name = "MS")]
    pub(crate) expires_at_ms: Option<i64>,
}

#[derive(Args)]
pub(crate) struct InterruptArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    pub(crate) turn: Option<String>,
}

#[derive(Args)]
pub(crate) struct ApproveArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, value_name = "REQUEST")]
    pub(crate) request: Option<String>,
    #[arg(long, value_name = "CHOICE_ID")]
    pub(crate) choice: Option<String>,
    #[arg(long, value_name = "FEEDBACK")]
    pub(crate) feedback: Option<String>,
}

#[derive(Args)]
pub(crate) struct DenyArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, value_name = "REQUEST")]
    pub(crate) request: Option<String>,
    #[arg(long, value_name = "CHOICE_ID")]
    pub(crate) choice: Option<String>,
    #[arg(long, value_name = "FEEDBACK")]
    pub(crate) feedback: Option<String>,
}

#[derive(Args)]
pub(crate) struct AnswerArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(long, value_name = "PATH")]
    pub(crate) answers_file: PathBuf,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, value_name = "REQUEST")]
    pub(crate) request: Option<String>,
}

#[derive(Args)]
pub(crate) struct ListFilterArgs {
    /// Exact Agent state: unknown, created, starting, idle, busy, waiting,
    /// stopped, failed or lost.
    #[arg(long, value_name = "STATE")]
    pub(crate) state: Option<String>,
    /// Exact project label.
    #[arg(long, value_name = "PROJECT")]
    pub(crate) project: Option<String>,
    /// Exact harness name.
    #[arg(long, value_name = "HARNESS")]
    pub(crate) harness: Option<String>,
    /// Direct-parent Agent ID.
    #[arg(long, value_name = "AGENT")]
    pub(crate) parent: Option<String>,
    /// Subtree-root Agent ID.
    #[arg(long, value_name = "AGENT")]
    pub(crate) root: Option<String>,
}

#[derive(Args)]
pub(crate) struct LsArgs {
    #[arg(long, value_name = "AGENT")]
    pub(crate) after: Option<String>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
    #[command(flatten)]
    pub(crate) filter: ListFilterArgs,
}

#[derive(Args)]
pub(crate) struct TopArgs {
    #[arg(long, value_name = "AGENT")]
    pub(crate) after: Option<String>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
    #[command(flatten)]
    pub(crate) filter: ListFilterArgs,
}

#[derive(Args)]
pub(crate) struct ShowArgs {
    pub(crate) agent: String,
}

#[derive(Args)]
pub(crate) struct ProcessesArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: ReadGenerationArgs,
    #[arg(long, default_value = "agent", value_name = "SCOPE")]
    pub(crate) scope: String,
    #[arg(long, value_name = "CURSOR")]
    pub(crate) after: Option<String>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
}

#[derive(Args)]
pub(crate) struct StatsArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: ReadGenerationArgs,
}

#[derive(Args)]
pub(crate) struct UsageArgs {
    #[arg(
        long,
        value_name = "AGENT",
        conflicts_with = "project",
        conflicts_with = "root"
    )]
    pub(crate) agent: Option<String>,
    #[command(flatten)]
    pub(crate) generation: ReadGenerationArgs,
    #[arg(long, value_name = "PROJECT", conflicts_with = "root")]
    pub(crate) project: Option<String>,
    /// Subtree-root Agent ID; covers the root and its descendants.
    #[arg(long, value_name = "AGENT")]
    pub(crate) root: Option<String>,
}

#[derive(Args)]
pub(crate) struct GitStatusArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: ReadGenerationArgs,
}

#[derive(Args)]
pub(crate) struct EditsArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: ReadGenerationArgs,
    #[arg(long, value_name = "PATH")]
    pub(crate) after: Option<String>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
}

#[derive(Args)]
pub(crate) struct WatchArgs {
    pub(crate) agent: Option<String>,
    /// Exclusive replay cursor. Omitted: start at the current journal tip.
    #[arg(long, value_name = "SEQ")]
    pub(crate) after: Option<i64>,
}

#[derive(Args)]
pub(crate) struct ResultArgs {
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
    #[arg(long, value_name = "TURN")]
    pub(crate) turn_id: Option<String>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
}

#[derive(Args)]
pub(crate) struct HistoryArgs {
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: ReadGenerationArgs,
    #[arg(long, default_value = "native", value_name = "SOURCE", value_parser = ["native", "rollout"])]
    pub(crate) source: String,
    #[arg(long, value_name = "VIEW", value_parser = ["subject", "turns", "items"])]
    pub(crate) view: String,
    #[arg(long, value_name = "TURN")]
    pub(crate) turn_id: Option<String>,
    #[arg(long, value_name = "CURSOR")]
    pub(crate) cursor: Option<String>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
}

#[derive(Args)]
pub(crate) struct DetachArgs {
    pub(crate) agent: String,
    #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(1..))]
    pub(crate) generation: Option<i64>,
}

#[derive(Args)]
pub(crate) struct StopArgs {
    pub(crate) agent: String,
    #[arg(long)]
    pub(crate) force: bool,
    /// Stop the Agent's whole subtree instead of just the Agent.
    #[arg(long, conflicts_with = "generation")]
    pub(crate) tree: bool,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
}

#[derive(Args)]
pub(crate) struct ConfigureArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    /// Model id for the next turn (exact vendor id; catalog failures
    /// never select another model).
    #[arg(long, value_name = "MODEL")]
    pub(crate) model: Option<String>,
    /// Reasoning effort for the next turn.
    #[arg(long, value_name = "EFFORT")]
    pub(crate) effort: Option<String>,
    #[command(flatten)]
    pub(crate) generation: GenerationArgs,
}

#[derive(Args)]
pub(crate) struct WorktreeArgs {
    #[command(subcommand)]
    pub(crate) command: WorktreeCommand,
}

#[derive(Args)]
pub(crate) struct CmdArgs {
    #[command(subcommand)]
    pub(crate) command: CmdCommand,
}

#[derive(Args)]
pub(crate) struct TurnArgs {
    #[command(subcommand)]
    pub(crate) command: TurnCommand,
}

#[derive(Args)]
pub(crate) struct EventsArgs {
    #[arg(long, value_name = "SEQ")]
    pub(crate) since: Option<i64>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
}

#[derive(Args)]
pub(crate) struct WaitArgs {
    /// Complete when every entry is terminal.
    #[arg(long, conflicts_with = "any")]
    pub(crate) all: bool,
    /// Complete when any entry is terminal.
    #[arg(long, conflicts_with = "all")]
    pub(crate) any: bool,
    #[arg(long, default_value_t = DEFAULT_WAIT_SECONDS, value_name = "SECONDS")]
    pub(crate) timeout_seconds: u64,
    /// Command ids, or `agent/generation/turn_id` for exact turn bindings.
    #[arg(value_name = "ID")]
    pub(crate) ids: Vec<String>,
}

#[derive(Args)]
pub(crate) struct MsgArgs {
    #[command(subcommand)]
    pub(crate) command: MsgCommand,
}

#[derive(Args)]
pub(crate) struct InboxArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: Option<String>,
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    #[arg(long, value_name = "DELIVERY")]
    pub(crate) after: Option<String>,
    #[arg(long, value_name = "SECS")]
    pub(crate) wait: Option<u64>,
}

#[derive(Args)]
pub(crate) struct VarArgs {
    #[command(subcommand)]
    pub(crate) command: VarCommand,
}

#[derive(Args)]
pub(crate) struct WhoArgs {
    #[arg(long, value_name = "PROJECT")]
    pub(crate) project: Option<String>,
    /// Include bounded recent Git history per worker.
    #[arg(long)]
    pub(crate) history: bool,
}

#[derive(Args)]
pub(crate) struct ClaimArgs {
    #[command(subcommand)]
    pub(crate) command: ClaimCommand,
}

#[derive(Args)]
pub(crate) struct PluginArgs {
    #[command(subcommand)]
    pub(crate) command: PluginCommand,
}

#[derive(Args)]
pub(crate) struct DaemonArgs {
    /// Run the daemon in this process instead of detaching it.
    #[arg(long)]
    pub(crate) foreground: bool,
    #[command(subcommand)]
    pub(crate) action: Option<DaemonAction>,
}

#[derive(Subcommand)]
pub(crate) enum DaemonAction {
    Stop(ShutdownArgs),
}

#[derive(Args)]
pub(crate) struct ShutdownArgs {
    #[arg(long, conflicts_with = "force")]
    pub(crate) drain: bool,
    #[arg(long, conflicts_with = "drain")]
    pub(crate) force: bool,
}

#[derive(Subcommand)]
pub(crate) enum CmdCommand {
    Show(CmdShowArgs),
    /// Read one bounded page of a retained command expansion.
    Children(CmdChildrenArgs),
    /// Resolve an unknown command to its owner-asserted terminal state.
    Resolve(CmdResolveArgs),
}

#[derive(Args)]
pub(crate) struct CmdShowArgs {
    pub(crate) id: String,
}

#[derive(Args)]
pub(crate) struct CmdChildrenArgs {
    pub(crate) id: String,
    #[arg(long, value_name = "ID")]
    pub(crate) after: Option<String>,
    #[command(flatten)]
    pub(crate) page: PageArgs,
}

#[derive(Args)]
pub(crate) struct CmdResolveArgs {
    pub(crate) command_id: String,
    #[arg(long = "as", value_name = "STATE")]
    pub(crate) resolution: String,
    #[arg(long, value_name = "TEXT")]
    pub(crate) reason: String,
}

#[derive(Subcommand)]
pub(crate) enum TurnCommand {
    /// Resolve an unknown turn to its owner-asserted terminal status.
    Resolve(TurnResolveArgs),
}

#[derive(Args)]
pub(crate) struct TurnResolveArgs {
    pub(crate) agent: String,
    #[arg(long, value_name = "GENERATION", value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) generation: i64,
    #[arg(long, value_name = "TURN")]
    pub(crate) turn_id: String,
    #[arg(long, value_name = "STATUS")]
    pub(crate) status: String,
    #[arg(long, value_name = "TEXT")]
    pub(crate) reason: String,
}

#[derive(Subcommand)]
pub(crate) enum MsgCommand {
    /// Queue one message to the named recipients.
    Send(MsgSendArgs),
    /// Read one exact delivery without marking it.
    Show(MsgShowArgs),
    /// Acknowledge exact delivery ids.
    Ack(MsgAckArgs),
}

#[derive(Args)]
pub(crate) struct MsgSendArgs {
    #[arg(long, value_name = "AGENT")]
    pub(crate) to: Vec<String>,
    #[arg(value_name = "TEXT", allow_hyphen_values = true)]
    pub(crate) text: String,
    #[arg(long, value_name = "LABEL")]
    pub(crate) thread: Option<String>,
    #[arg(long, value_name = "DELIVERY")]
    pub(crate) reply_to: Option<String>,
    #[arg(long, value_parser = ["queue", "send", "steer", "inject"], default_value = "queue")]
    pub(crate) mode: String,
}

#[derive(Args)]
pub(crate) struct MsgShowArgs {
    #[arg(value_name = "DELIVERY")]
    pub(crate) delivery: String,
    #[arg(long, value_name = "AGENT")]
    pub(crate) agent: Option<String>,
}

#[derive(Args)]
pub(crate) struct MsgAckArgs {
    #[arg(long, value_name = "DELIVERY")]
    pub(crate) delivery: Vec<String>,
    #[arg(long, value_name = "AGENT")]
    pub(crate) agent: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum VarCommand {
    /// Write one variable, optionally guarded by its expected version.
    Set(VarSetArgs),
    /// Read one variable value with its version and writer.
    Get(VarGetArgs),
}

#[derive(Args)]
pub(crate) struct VarSetArgs {
    #[arg(value_name = "KEY")]
    pub(crate) key: String,
    /// Value supplied directly on the command line.
    #[arg(
        value_name = "VALUE",
        allow_hyphen_values = true,
        required_unless_present = "value_file",
        conflicts_with = "value_file"
    )]
    pub(crate) value: Option<String>,
    /// Read the value from a UTF-8 file instead of the inline VALUE.
    #[arg(long, value_name = "PATH")]
    pub(crate) value_file: Option<PathBuf>,
    #[arg(long, value_name = "SCOPE")]
    pub(crate) scope: Option<String>,
    /// Parse the value (inline or from --value-file) as JSON instead of storing it as a string.
    #[arg(long)]
    pub(crate) as_json: bool,
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) expect_version: Option<i64>,
    #[arg(long, value_name = "TEXT")]
    pub(crate) note: Option<String>,
}

#[derive(Args)]
pub(crate) struct VarGetArgs {
    #[arg(value_name = "KEY")]
    pub(crate) key: String,
    #[arg(long, value_name = "SCOPE")]
    pub(crate) scope: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum ClaimCommand {
    /// Claim a project-relative glob for an Agent generation.
    Set(ClaimSetArgs),
    /// List live claims in scope.
    List(ClaimListArgs),
    /// Release one live claim.
    Release(ClaimReleaseArgs),
}

#[derive(Args)]
pub(crate) struct ClaimSetArgs {
    #[arg(value_name = "GLOB")]
    pub(crate) glob: String,
    #[arg(long, value_name = "AGENT")]
    pub(crate) agent: Option<String>,
    #[arg(long, value_name = "TEXT")]
    pub(crate) note: Option<String>,
    /// Claim lifetime in seconds (1 to 86400, default 3600).
    #[arg(long, value_name = "SECS")]
    pub(crate) ttl_secs: Option<u64>,
}

#[derive(Args)]
pub(crate) struct ClaimListArgs {
    #[arg(long, value_name = "PROJECT")]
    pub(crate) project: Option<String>,
    #[arg(long, value_name = "AGENT")]
    pub(crate) agent: Option<String>,
}

#[derive(Args)]
pub(crate) struct ClaimReleaseArgs {
    #[arg(value_name = "GLOB")]
    pub(crate) glob: String,
    #[arg(long, value_name = "AGENT")]
    pub(crate) agent: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum ProviderCommand {
    /// Read one bounded page of durable provider holds.
    Holds(ProviderHoldsArgs),
    /// Clear a provider hold with exact owner corrective evidence.
    ClearHold(ProviderClearHoldArgs),
}

#[derive(Args)]
pub(crate) struct ProviderHoldsArgs {
    #[arg(long, value_name = "SCOPE")]
    pub(crate) after: Option<String>,
    #[arg(long)]
    pub(crate) limit: Option<usize>,
}

#[derive(Args)]
pub(crate) struct ProviderClearHoldArgs {
    #[arg(value_name = "SCOPE")]
    pub(crate) scope: String,
    #[arg(
        long,
        value_name = "EVIDENCE",
        value_parser = ["credentials_updated", "configuration_updated", "owner_verified"]
    )]
    pub(crate) corrective_evidence: String,
    #[arg(long, value_name = "ID")]
    pub(crate) expected_probe_command: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum RuleCommand {
    /// Show the loaded rule state and current/retiring cursor progress.
    Show(RuleShowArgs),
    /// Process one finite committed-event range through the ordinary rule path.
    CatchUp(RuleCatchUpArgs),
}

#[derive(Args)]
pub(crate) struct RuleShowArgs {
    pub(crate) id: String,
    #[arg(long, value_name = "PROJECT")]
    pub(crate) project: Option<String>,
}

#[derive(Args)]
pub(crate) struct RuleCatchUpArgs {
    #[arg(value_name = "ID")]
    pub(crate) rule: String,
    #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) after: i64,
    #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) through: Option<i64>,
    #[arg(long, value_name = "PROJECT")]
    pub(crate) project: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum TimerCommand {
    /// Show the current timer schedule and durable UTC slot state.
    Show(TimerShowArgs),
    /// Run one timer action using a durable mutation ID.
    Run(TimerRunArgs),
}

#[derive(Args)]
pub(crate) struct TimerShowArgs {
    pub(crate) id: String,
    #[arg(long, value_name = "PROJECT")]
    pub(crate) project: Option<String>,
}

#[derive(Args)]
pub(crate) struct TimerRunArgs {
    #[arg(value_name = "ID")]
    pub(crate) rule: String,
    #[arg(long, value_name = "PROJECT")]
    pub(crate) project: Option<String>,
}

#[derive(Subcommand)]
pub(crate) enum GoalCommand {
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
pub(crate) struct GoalSetArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    pub(crate) mode: String,
    #[arg(long, value_name = "TEXT")]
    pub(crate) text: String,
    /// Explicit check argv; pass last since it consumes the remaining arguments.
    #[arg(long, num_args = 1.., allow_hyphen_values = true, value_name = "ARG")]
    pub(crate) check: Vec<String>,
    #[arg(long, value_name = "PATH")]
    pub(crate) check_cwd: Option<String>,
    #[arg(long, value_name = "MS")]
    pub(crate) check_timeout_ms: Option<i64>,
    #[arg(long, value_name = "N")]
    pub(crate) max_turns: Option<i64>,
    #[arg(long, value_name = "N")]
    pub(crate) max_rounds: Option<i64>,
    #[arg(long, value_name = "N")]
    pub(crate) max_without_progress: Option<i64>,
    /// Cadence in milliseconds; a cadenced Goal rounds on anchored
    /// time slots instead of idle events.
    #[arg(long, value_name = "MS")]
    pub(crate) cadence_ms: Option<i64>,
}

#[derive(Args)]
pub(crate) struct GoalShowArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    pub(crate) mode: String,
}

#[derive(Args)]
pub(crate) struct GoalPauseArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    pub(crate) mode: String,
    #[arg(long, value_name = "REASON")]
    pub(crate) reason: Option<String>,
}

#[derive(Args)]
pub(crate) struct GoalClearArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    pub(crate) mode: String,
}

#[derive(Args)]
pub(crate) struct GoalResumeArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    pub(crate) mode: String,
}

#[derive(Args)]
pub(crate) struct GoalRoundArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[arg(long, value_parser = ["eliot", "native"], default_value = "eliot")]
    pub(crate) mode: String,
    #[arg(long, value_name = "SEQ", value_parser = clap::value_parser!(i64).range(0..))]
    pub(crate) idle_seq: Option<i64>,
    #[arg(long, value_name = "N", value_parser = clap::value_parser!(i64).range(1..))]
    pub(crate) slot: Option<i64>,
}

#[derive(Subcommand)]
pub(crate) enum ProjectCommand {
    /// List owner-registered projects and their authority bounds.
    List,
    /// Show one owner-registered project readback.
    Show(ProjectShowArgs),
}

#[derive(Args)]
pub(crate) struct ProjectShowArgs {
    pub(crate) id: String,
}

#[derive(Subcommand)]
pub(crate) enum WorktreeCommand {
    /// Remove a terminal Agent's clean worktree while retaining its branch.
    Remove(WorktreeRemoveArgs),
}

#[derive(Args)]
pub(crate) struct WorktreeRemoveArgs {
    #[arg(value_name = "AGENT")]
    pub(crate) agent: String,
    #[command(flatten)]
    pub(crate) generation: ReadGenerationArgs,
}

#[derive(Subcommand)]
pub(crate) enum PluginCommand {
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

#[cfg(test)]
mod tests {
    use super::{
        ApproveArgs, Cli, CliCommand, DenyArgs, GenerationArgs, InterruptArgs, ReadGenerationArgs,
        SteerArgs, UsageArgs,
    };
    use crate::cli::agents::approval_reply_args;
    use clap::Parser;
    #[test]
    fn usage_cli_selects_one_scope() {
        let cli = Cli::try_parse_from(["swarm", "usage", "--project", "p1"]).unwrap();
        let Some(CliCommand::Usage(UsageArgs {
            agent,
            generation: ReadGenerationArgs { generation },
            project,
            root,
        })) = cli.command
        else {
            panic!("usage command required");
        };
        assert_eq!(agent, None);
        assert_eq!(generation, None);
        assert_eq!(project.as_deref(), Some("p1"));
        assert_eq!(root, None);
        let cli =
            Cli::try_parse_from(["swarm", "usage", "--agent", "a1", "--generation", "2"]).unwrap();
        let Some(CliCommand::Usage(UsageArgs {
            generation: ReadGenerationArgs { generation },
            ..
        })) = cli.command
        else {
            panic!("usage command required");
        };
        assert_eq!(generation, Some(2));
        assert!(
            Cli::try_parse_from(["swarm", "usage", "--agent", "a1", "--project", "p1"]).is_err()
        );
        assert!(Cli::try_parse_from(["swarm", "usage", "--agent", "a1", "--root", "a2"]).is_err());
        assert!(
            Cli::try_parse_from(["swarm", "usage", "--project", "p1", "--root", "a2"]).is_err()
        );
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
}
