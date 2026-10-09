use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, ExitCode, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver, RecvTimeoutError},
    thread,
    time::{Duration, Instant},
};
use swarm_cli::{ClientConfig, call, prepare_call, validate_call_method};
use swarm_contracts::{
    Credential, concilium_limits as limits, coordination_limits,
    error::{Error, Result},
};
use swarm_process::child_error::{
    BoundedStderrSink, StderrForwardResult, forward_stderr, wrapper_error_json,
};
const POST_EXIT_DRAIN_TIMEOUT: Duration = Duration::from_millis(300);
#[derive(Parser)]
#[command(
    name = "swarm",
    version,
    about = "Headless task controller with explicitly connected native modules."
)]
struct Cli {
    #[arg(long, global = true)]
    config: Option<PathBuf>,
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,
    #[arg(long, global = true)]
    credential: Option<PathBuf>,
    #[arg(long, global = true)]
    request_id: Option<String>,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Start the user host in the foreground; never launches vendor agents implicitly.
    Host {
        /// Gracefully stop when the foreground owner's stdin closes.
        #[arg(long)]
        stop_on_stdin_eof: bool,
    },
    /// Serve the application API as MCP tools over stdio for a General Manager
    /// client. A client of the running host over the same local IPC as the CLI;
    /// never opens the database or a network listener.
    Mcp {
        /// Named profile from the local [mcp.profiles] configuration table.
        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },
    /// Serve one fixed restricted MCP profile over loopback Streamable HTTP.
    Gateway,
    /// Independently run one bridge under a persistent, non-killing process owner.
    ModuleRun {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        command: PathBuf,
        #[arg(last = true, required = true)]
        args: Vec<String>,
    },
    /// Internal transient executor; never opens the controller database.
    #[command(hide = true)]
    CheckWorker {
        #[arg(long)]
        file: PathBuf,
    },
    /// Internal foreground service owner; never opens the controller database.
    #[command(hide = true)]
    OwnedOpencodeService {
        #[arg(long)]
        file: PathBuf,
    },
    /// Internal trusted-local script invocation worker; never opens the Store.
    #[command(hide = true)]
    ScriptWorker {
        #[arg(long)]
        file: PathBuf,
    },
    Source {
        #[command(subcommand)]
        command: SourceCommand,
    },
    /// Propose, accept, inspect, compare and release advisory code scopes.
    Code {
        #[command(subcommand)]
        command: CodeCommand,
    },
    Check {
        #[command(subcommand)]
        command: CheckCommand,
    },
    Status,
    /// Read controller diagnostics from recorded facts; performs no repair.
    Doctor,
    /// Call a supported application method; JSON params are read from a file.
    Call {
        method: String,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    Coordination {
        #[command(subcommand)]
        command: CoordinationCommand,
    },
    Concilium {
        #[command(subcommand)]
        command: ConciliumCommand,
    },
    Review {
        #[command(subcommand)]
        command: ReviewCommand,
    },
    Automation {
        #[command(subcommand)]
        command: AutomationCommand,
    },
    Script {
        #[command(subcommand)]
        command: ScriptCommand,
    },
    Hook {
        #[command(subcommand)]
        command: HookCommand,
    },
    Goal {
        #[command(subcommand)]
        command: GoalCommand,
    },
    GitHub {
        #[command(subcommand)]
        command: GitHubCommand,
    },
    Launcher {
        #[command(subcommand)]
        command: LauncherCommand,
    },
    /// Create a manager, observer, or module credential. Participant credentials
    /// require a scoped registration through `coordination participant register`.
    ClientCreate {
        client_id: String,
        #[arg(long,default_value="manager",value_parser=["manager","observer","module"])]
        role: String,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, requires = "generation")]
        binding_id: Option<String>,
        #[arg(long, requires = "binding_id")]
        generation: Option<i64>,
    },
    /// Designate the current GM client, optionally naming its native binding.
    /// A different client rotates the epoch; rebinding the same client preserves it.
    GmHandover {
        client_id: String,
        #[arg(long, requires = "generation")]
        binding_id: Option<String>,
        #[arg(long, requires = "binding_id")]
        generation: Option<i64>,
    },
    /// Read a retained family observation, not a live SDK query or complete inventory claim.
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    Attempt {
        #[command(subcommand)]
        command: AttemptCommand,
    },
    Family {
        binding_id: String,
        #[arg(long)]
        generation: i64,
        #[arg(long)]
        observation_id: Option<i64>,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// Request one native result page. It performs no model call or result consumption.
    Result {
        binding_id: String,
        #[arg(long)]
        generation: i64,
        #[arg(long)]
        file: PathBuf,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 65536)]
        length: u64,
    },
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
    Report {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    Observer {
        #[command(subcommand)]
        command: ObserverCommand,
    },
    /// Read the Store-backed Manager live monitor through authenticated IPC.
    Monitor {
        #[command(subcommand)]
        command: MonitorCommand,
    },
}
#[derive(Subcommand)]
enum ObserverCommand {
    /// Read the authenticated delta, attention, and capacity projections once.
    Snapshot {
        #[arg(long, default_value_t = 0)]
        after: u64,
        #[arg(long, default_value_t = 200)]
        limit: u64,
    },
    /// Follow privacy-protected local diagnostic segments from an optional
    /// `(segment, byte offset)` cursor. This is not a Store journal cursor.
    Follow {
        #[arg(long)]
        after_segment: Option<u64>,
        #[arg(long)]
        after_offset: Option<u64>,
        /// Return after the current file pass; without a cursor, begin at the newest segment's end.
        #[arg(long)]
        once: bool,
    },
    /// Sample only explicit private process receipts; performs no Store read or process control.
    Metrics {
        /// Exact four-field Windows process-image receipt; accepts no PID scalar.
        #[arg(long)]
        host_identity: Option<PathBuf>,
        /// Existing module-owner envelope; must be paired with --module-worker.
        #[arg(long)]
        module_owner: Option<PathBuf>,
        /// Existing module worker receipt from the same private state directory.
        #[arg(long)]
        module_worker: Option<PathBuf>,
        /// Descriptive child label; OS membership is verified independently.
        #[arg(long, value_parser = ["helper", "adapter"])]
        child_role: Option<String>,
        /// Optional single interval (10–2000 ms) for exactly two samples.
        #[arg(long)]
        interval_ms: Option<u64>,
    },
}
#[derive(Subcommand)]
enum MonitorCommand {
    /// Capture one current-state snapshot and an atomic observation-journal cut.
    Snapshot {
        #[arg(long, default_value_t = 5)]
        limit: i64,
    },
    /// Read one bounded retained page after a monitor journal cursor.
    Follow {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
}
#[derive(Subcommand)]
enum SourceCommand {
    Capture {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CodeCommand {
    Scope {
        #[command(subcommand)]
        command: CodeScopeCommand,
    },
}
#[derive(Subcommand)]
enum CodeScopeCommand {
    /// Propose paths, symbols or interfaces for manager review; this reserves nothing.
    Propose {
        #[arg(long)]
        file: PathBuf,
    },
    /// Accept one exact proposal digest, optionally revising its declared scope.
    Accept {
        #[arg(long)]
        file: PathBuf,
    },
    /// Inspect retained scope intents within one Task and optional selector filters.
    Inspect {
        task_id: String,
        #[arg(long)]
        task_revision: Option<i64>,
        #[arg(long)]
        attempt_id: Option<String>,
        #[arg(long)]
        scope_intent_id: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        symbol: Option<String>,
        #[arg(long)]
        interface: Option<String>,
        #[arg(long)]
        after_scope_id: Option<String>,
        #[arg(
            long,
            default_value_t = coordination_limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=coordination_limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
    /// Compare exact scoped ownership selectors; unknown coverage is not a no-conflict result.
    Conflicts {
        task_id: String,
        #[arg(long)]
        task_revision: Option<i64>,
        #[arg(long)]
        attempt_id: Option<String>,
        #[arg(long)]
        scope_intent_id: Option<String>,
        #[arg(long)]
        client_id: Option<String>,
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        symbol: Option<String>,
        #[arg(long)]
        interface: Option<String>,
        #[arg(long)]
        after_scope_id: Option<String>,
        #[arg(
            long,
            default_value_t = coordination_limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=coordination_limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
    /// Release one exact accepted scope revision with a reason.
    Release {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CheckCommand {
    Run {
        #[arg(long)]
        file: PathBuf,
    },
    Get {
        check_id: String,
    },
    Profiles,
    Cancel {
        check_id: String,
        #[arg(long)]
        reason: String,
    },
}
#[derive(Subcommand)]
enum ArtifactCommand {
    /// Assemble an ordered list of retained native pages, without calling a model.
    Assemble {
        #[arg(long)]
        file: PathBuf,
    },
    /// Page the immutable provenance manifest of a whole result.
    Parts {
        artifact_id: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// Export all bytes, check the complete SHA-256, and publish without overwriting.
    Export {
        artifact_id: String,
        #[arg(long)]
        out: PathBuf,
    },
    Get {
        artifact_id: String,
    },
    Read {
        artifact_id: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 65536)]
        length: u64,
    },
}
#[derive(Subcommand)]
enum TaskCommand {
    /// Accept the exact proposal as a separate decision owner after review.
    Accept {
        #[arg(long)]
        file: PathBuf,
    },
    /// Inspect one decision and its current/revoked status.
    Acceptance {
        acceptance_operation_id: String,
    },
    /// Revoke only the named acceptance, without restarting its producer.
    InvalidateAcceptance {
        #[arg(long)]
        file: PathBuf,
    },
    /// Submit an immutable retained candidate and a requirement report; not acceptance.
    Submit {
        #[arg(long)]
        file: PathBuf,
    },
    /// Recover the exact result of an unknown prior submission.
    /// Records a Store recovery Operation; does not replay native work or create a file.
    RecoverSubmission {
        operation_id: String,
    },
    /// Read a specific immutable submission, with paged requirement claims.
    Submission {
        submission_ref: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// Return one anchored finding as the decision owner; never starts another worker.
    RequestChanges {
        #[arg(long)]
        file: PathBuf,
    },
    Create {
        #[arg(long)]
        project: String,
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        origin_key: Option<String>,
    },
    Get {
        task_id: String,
    },
    List {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    Claim {
        task_id: String,
        #[arg(long)]
        revision: i64,
        #[arg(long)]
        owner: Option<String>,
        #[arg(long,default_value="native_manager",value_parser=["controller","native_manager"])]
        start_owner: String,
        #[arg(long, requires = "generation")]
        binding_id: Option<String>,
        #[arg(long, requires = "binding_id")]
        generation: Option<i64>,
    },
    /// Associate existing native work with an Attempt; never spawns a worker.
    Bind {
        attempt_id: String,
        #[arg(long)]
        assignment: String,
        #[arg(long)]
        session: String,
        #[arg(long)]
        turn: String,
        #[arg(long)]
        observation_id: i64,
    },
    Revise {
        task_id: String,
        #[arg(long)]
        revision: i64,
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CoordinationCommand {
    Participant {
        #[command(subcommand)]
        command: ParticipantCommand,
    },
    Peer {
        #[command(subcommand)]
        command: PeerCommand,
    },
    WorkCard {
        #[command(subcommand)]
        command: CardCommand,
    },
    ContractCard {
        #[command(subcommand)]
        command: CardCommand,
    },
    Consult {
        #[arg(long)]
        file: PathBuf,
    },
    /// Record one exact integration offer or requirement under a contract key.
    SyncIntegration {
        #[arg(long)]
        file: PathBuf,
    },
    /// Compare exact paths, symbols, contracts, or a candidate with current scoped facts.
    OverlapCheck {
        #[arg(long)]
        file: PathBuf,
    },
    Watch {
        #[command(subcommand)]
        command: CoordinationWatchCommand,
    },
    /// Open and inspect scoped, durable coordination Threads.
    Thread {
        #[command(subcommand)]
        command: CoordinationThreadCommand,
    },
    /// Send one addressed message inside an exact coordination Thread.
    Message {
        #[command(subcommand)]
        command: CoordinationMessageCommand,
    },
    /// Propose, respond to, and inspect immutable contract revisions.
    Contract {
        #[command(subcommand)]
        command: CoordinationContractCommand,
    },
    /// Record a Participant's exact agreement-cell position.
    Integration {
        #[command(subcommand)]
        command: CoordinationIntegrationCommand,
    },
    /// Read a bounded retained agreement-cell projection.
    Agreement {
        #[command(subcommand)]
        command: CoordinationAgreementCommand,
    },
    Send {
        #[arg(long)]
        file: PathBuf,
    },
    Inbox {
        #[arg(long)]
        file: PathBuf,
    },
    Context {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CoordinationThreadCommand {
    /// Open a Thread from an exact JSON request file.
    Open {
        #[arg(long)]
        file: PathBuf,
    },
    /// Read one authorized Thread and messages after an optional sequence cursor.
    Get {
        thread_id: String,
        #[arg(long)]
        after_message_seq: Option<i64>,
        #[arg(
            long,
            default_value_t = coordination_limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=coordination_limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
    /// Page Threads in one exact Task and optional Attempt scope.
    List {
        task_id: String,
        #[arg(long)]
        attempt_id: Option<String>,
        #[arg(long)]
        state: Option<String>,
        #[arg(long)]
        topic_kind: Option<String>,
        #[arg(long)]
        after_thread_id: Option<String>,
        #[arg(
            long,
            default_value_t = coordination_limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=coordination_limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
    /// Close one Thread with an exact expected state revision.
    Resolve {
        #[arg(long)]
        file: PathBuf,
    },
    /// Withdraw one Thread with an exact expected state revision.
    Withdraw {
        #[arg(long)]
        file: PathBuf,
    },
    /// Link an open successor and supersede the exact current Thread.
    Supersede {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CoordinationMessageCommand {
    /// Send to one explicit roster recipient using the existing coordination mailbox.
    Send {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CoordinationContractCommand {
    /// Propose a canonical contract revision in an exact Thread.
    Propose {
        #[arg(long)]
        file: PathBuf,
    },
    /// Record one response to an exact proposal revision and digest.
    Respond {
        #[arg(long)]
        file: PathBuf,
    },
    /// Read one immutable contract revision.
    Get {
        thread_id: String,
        proposal_id: String,
        proposal_revision_id: String,
        #[arg(long)]
        after_observation_id: Option<i64>,
        #[arg(
            long,
            default_value_t = coordination_limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=coordination_limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
    /// Page proposal metadata in one exact Thread.
    List {
        thread_id: String,
        #[arg(long)]
        after_sequence: Option<i64>,
        #[arg(
            long,
            default_value_t = coordination_limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=coordination_limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
}
#[derive(Subcommand)]
enum CoordinationIntegrationCommand {
    /// Acknowledge or dissent on one exact agreement-cell revision and comparison.
    Ack {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CoordinationAgreementCommand {
    /// Read current or exact historical agreement state; revision and digest are paired.
    Get {
        cell_id: String,
        task_id: String,
        task_revision: i64,
        attempt_id: String,
        #[arg(long)]
        state_revision: Option<i64>,
        #[arg(long)]
        material_digest: Option<String>,
        #[arg(long)]
        after_position_id: Option<String>,
        #[arg(
            long,
            default_value_t = coordination_limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=coordination_limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
}
#[derive(Subcommand)]
enum ConciliumCommand {
    /// Submit a scoped proposal for manager review; this does not invoke participants.
    Propose {
        #[arg(long)]
        file: PathBuf,
    },
    /// Preview the deterministic plan for one proposal Operation.
    Preview { proposal_operation_id: String },
    /// Commit the exact reviewed preview and its participant slots.
    Open {
        #[arg(long)]
        file: PathBuf,
    },
    /// Submit the authenticated Participant's structured response to one exact slot.
    #[command(name = "position-submit")]
    PositionSubmit {
        #[arg(long)]
        file: PathBuf,
    },
    /// Commit an explicit manager-selected next packet and round.
    #[command(name = "round-advance")]
    RoundAdvance {
        #[arg(long)]
        file: PathBuf,
    },
    /// Read one Concilium by its exact ID.
    Get { concilium_id: String },
    /// Page Concilium projections visible to the authenticated scope.
    List {
        #[arg(long)]
        task_id: String,
        #[arg(long)]
        attempt_id: Option<String>,
        #[arg(long, value_parser = ["proposed", "planned", "round_1_open", "round_1_ready", "round_2_open", "round_2_ready", "merge_available", "completed", "unresolved", "cancelled", "failed"])]
        state: Option<String>,
        #[arg(long)]
        after_concilium_id: Option<String>,
        #[arg(
            long,
            default_value_t = limits::DEFAULT_READ_PAGE_SIZE,
            value_parser = clap::value_parser!(i64).range(1..=limits::MAX_READ_PAGE_SIZE)
        )]
        limit: i64,
    },
    /// Record the manager's advisory result while preserving dissent.
    Close {
        concilium_id: String,
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum ParticipantCommand {
    Register {
        #[arg(long)]
        file: PathBuf,
    },
    Disable {
        #[arg(long)]
        file: PathBuf,
    },
    Get {
        #[arg(long)]
        file: PathBuf,
    },
    List {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum PeerCommand {
    Find {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CoordinationWatchCommand {
    Create {
        #[arg(long)]
        file: PathBuf,
    },
    List {
        #[arg(long)]
        file: PathBuf,
    },
    Cancel {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum CardCommand {
    Publish {
        #[arg(long)]
        file: PathBuf,
    },
    Withdraw {
        #[arg(long)]
        file: PathBuf,
    },
    Get {
        #[arg(long)]
        file: PathBuf,
    },
    List {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum ReviewCommand {
    Assign {
        #[arg(long)]
        file: PathBuf,
    },
    Submit {
        #[arg(long)]
        file: PathBuf,
    },
    Get {
        #[arg(long)]
        file: PathBuf,
    },
    List {
        #[arg(long)]
        file: PathBuf,
    },
    Context {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum AutomationCommand {
    Config {
        #[command(subcommand)]
        command: AutomationConfigCommand,
    },
}
#[derive(Subcommand)]
enum AutomationConfigCommand {
    Get {
        #[arg(long)]
        file: PathBuf,
    },
    Preview {
        #[arg(long)]
        file: PathBuf,
    },
    Apply {
        #[arg(long)]
        file: PathBuf,
    },
    Explain {
        #[arg(long)]
        file: PathBuf,
    },
    /// Transfer one retained automation entry to the current GM.
    Transfer {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum ScriptCommand {
    Register {
        #[arg(long)]
        file: PathBuf,
    },
    Revise {
        #[arg(long)]
        file: PathBuf,
    },
    Validate {
        script_id: String,
        revision: i64,
    },
    Activate {
        script_id: String,
        revision: i64,
    },
    Run {
        #[arg(long)]
        file: PathBuf,
    },
    Get {
        script_id: String,
        #[arg(long)]
        revision: Option<i64>,
    },
    List {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
}
#[derive(Subcommand)]
enum HookCommand {
    /// Create and install the supported repository-local post-commit observer.
    Setup { project_id: String },
    /// Emit one bounded commit fact using the setup-issued HookSource credential.
    Emit {
        #[arg(long)]
        source_id: String,
        #[arg(long)]
        commit_oid: String,
    },
    Source {
        #[command(subcommand)]
        command: HookSourceCommand,
    },
    Install {
        #[command(subcommand)]
        command: HookInstallCommand,
    },
}
#[derive(Subcommand)]
enum HookSourceCommand {
    Get {
        source_id: String,
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    Revoke {
        source_id: String,
        #[arg(long)]
        revision: i64,
    },
}
#[derive(Subcommand)]
enum HookInstallCommand {
    Preview {
        project_id: String,
    },
    Readback {
        project_id: String,
        source_id: String,
    },
    Revoke {
        project_id: String,
        source_id: String,
        #[arg(long)]
        revision: i64,
    },
}
#[derive(Subcommand)]
enum GoalCommand {
    Create {
        #[arg(long)]
        file: PathBuf,
    },
    Revise {
        #[arg(long)]
        file: PathBuf,
    },
    Enable {
        #[arg(long)]
        file: PathBuf,
    },
    Disable {
        #[arg(long)]
        file: PathBuf,
    },
    Readback {
        #[arg(long)]
        file: PathBuf,
    },
    Get {
        #[arg(long)]
        file: PathBuf,
    },
    List {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum GitHubCommand {
    Source {
        #[command(subcommand)]
        command: GitHubSourceCommand,
    },
    WorkPool {
        #[command(subcommand)]
        command: GitHubWorkPoolCommand,
    },
}
#[derive(Subcommand)]
enum GitHubSourceCommand {
    Inspect {
        host: String,
        owner: String,
        repo: String,
    },
    Setup {
        #[arg(long)]
        source_id: String,
        project_id: String,
        host: String,
        owner: String,
        repo: String,
        #[arg(long)]
        repository_id: i64,
    },
    Get {
        source_id: String,
    },
    Poll {
        source_id: String,
    },
}
#[derive(Subcommand)]
enum GitHubWorkPoolCommand {
    Preview {
        source_id: String,
        #[arg(long)]
        after: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
    },
    Apply {
        source_id: String,
        #[arg(long, num_args = 1..)]
        task_ids: Vec<String>,
    },
}
#[derive(Subcommand)]
enum LauncherCommand {
    Dashboard {
        #[arg(long)]
        file: PathBuf,
    },
    Preview {
        #[arg(long)]
        file: PathBuf,
    },
    /// Submit a caller-ID and preview-digest-bound launch intent.
    Launch {
        #[arg(long)]
        file: PathBuf,
    },
    Queue {
        #[command(subcommand)]
        command: LauncherQueueCommand,
    },
    Agent {
        #[command(subcommand)]
        command: LauncherAgentCommand,
    },
    Exceptions {
        #[command(subcommand)]
        command: LauncherExceptionsCommand,
    },
}
#[derive(Subcommand)]
enum LauncherQueueCommand {
    Get {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum LauncherAgentCommand {
    Inspect {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum LauncherExceptionsCommand {
    Get {
        #[arg(long)]
        file: PathBuf,
    },
}
#[derive(Subcommand)]
enum AgentCommand {
    /// Read retained subscription evidence for this binding; starts no native read.
    Usage {
        binding_id: String,
        #[arg(long)]
        generation: i64,
    },
    /// Page known bindings and their observed state.
    List {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// Read one binding generation's observed state.
    Get {
        binding_id: String,
        #[arg(long)]
        generation: i64,
    },
}
#[derive(Subcommand)]
enum AttemptCommand {
    /// Read one attempt and its disposition.
    Get { attempt_id: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let code = error.exit_code();
            let _ = error.print();
            return ExitCode::from(code.clamp(0, 255) as u8);
        }
    };
    if command_uses_mcp(&cli.command) {
        return delegate_to_mcp(&cli);
    }
    let raw_args: Vec<OsString> = std::env::args_os().skip(1).collect();
    if command_uses_host(&cli.command) {
        return delegate_to_host(&raw_args);
    }
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", json!({"error": error}));
            ExitCode::FAILURE
        }
    }
}

fn command_uses_mcp(command: &Command) -> bool {
    matches!(command, Command::Mcp { .. })
}

fn command_uses_host(command: &Command) -> bool {
    matches!(
        command,
        Command::Host { .. }
            | Command::Gateway
            | Command::ModuleRun { .. }
            | Command::CheckWorker { .. }
            | Command::OwnedOpencodeService { .. }
            | Command::ScriptWorker { .. }
            | Command::Observer { .. }
            | Command::ClientCreate { .. }
            | Command::Hook {
                command: HookCommand::Setup { .. } | HookCommand::Install { .. }
            }
            | Command::Artifact {
                command: ArtifactCommand::Export { .. }
            }
    )
}

fn delegate_to_host(arguments: &[OsString]) -> ExitCode {
    let sink = BoundedStderrSink::spawn().ok();
    let mut executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(_) => {
            let envelope = wrapper_error_json(
                "HOST_BINARY_PATH_FAILED",
                "could not locate the public CLI executable",
                false,
                None,
                None,
            );
            emit_host_wrapper_error(
                sink.as_ref(),
                &envelope,
                Instant::now() + POST_EXIT_DRAIN_TIMEOUT,
            );
            return ExitCode::FAILURE;
        }
    };
    executable.set_file_name(if cfg!(windows) {
        "swarm-host.exe"
    } else {
        "swarm-host"
    });
    let sibling_is_file = executable.is_file();
    let mut child = match ProcessCommand::new(&executable)
        .args(arguments)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            return report_host_start_failure(sibling_is_file, error.kind(), sink.as_ref());
        }
    };
    let Some(stderr) = child.stderr.take() else {
        let _ = child.kill();
        return report_host_start_failure(
            sibling_is_file,
            std::io::ErrorKind::Other,
            sink.as_ref(),
        );
    };
    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    let reader_sink = sink.clone();
    if thread::Builder::new()
        .name("swarm-child-stderr".to_owned())
        .spawn(move || {
            let result = forward_stderr(stderr, reader_sink);
            let _ = result_sender.send(result);
        })
        .is_err()
    {
        let _ = child.kill();
        return report_host_start_failure(
            sibling_is_file,
            std::io::ErrorKind::Other,
            sink.as_ref(),
        );
    }
    let status = match child.wait() {
        Ok(status) => status,
        Err(_) => {
            let _ = child.kill();
            return report_host_wait_failure(sibling_is_file, sink.as_ref(), &result_receiver);
        }
    };
    let deadline = Instant::now() + POST_EXIT_DRAIN_TIMEOUT;
    let stream = match result_receiver.recv_timeout(remaining(deadline)) {
        Ok(result) => result,
        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
            // A descendant may retain the pipe. Preserve the child's actual
            // exit status without an unbounded reader join or stderr write.
            return status_to_exit_code(status);
        }
    };
    if !stream.pipe_drained {
        return status_to_exit_code(status);
    }
    let Some(sink) = sink.as_ref() else {
        return status_to_exit_code(status);
    };
    if !sink.flush_until(deadline) {
        return status_to_exit_code(status);
    }
    if status.success() {
        ExitCode::SUCCESS
    } else {
        let envelope = wrapper_error_json(
            "HOST_COMMAND_FAILED",
            "the explicit host command exited unsuccessfully",
            true,
            status.code(),
            stream.child_error.as_ref(),
        );
        emit_host_wrapper_error(Some(sink), &envelope, deadline);
        // Windows exception statuses can be negative i32 values. Never
        // clamp an unsuccessful child exit to zero and report success.
        status_to_exit_code(status)
    }
}

fn remaining(deadline: Instant) -> Duration {
    deadline.saturating_duration_since(Instant::now())
}

fn emit_host_wrapper_error(sink: Option<&BoundedStderrSink>, envelope: &str, deadline: Instant) {
    if let Some(sink) = sink {
        let _ = sink.write_envelope_until(envelope, deadline);
    }
}

fn report_host_wait_failure(
    sibling_is_file: bool,
    sink: Option<&BoundedStderrSink>,
    result_receiver: &Receiver<StderrForwardResult>,
) -> ExitCode {
    let Some(sink) = sink else {
        return ExitCode::FAILURE;
    };
    let deadline = Instant::now() + POST_EXIT_DRAIN_TIMEOUT;
    let Ok(stream) = result_receiver.recv_timeout(remaining(deadline)) else {
        return ExitCode::FAILURE;
    };
    if !stream.pipe_drained || !sink.flush_until(deadline) {
        return ExitCode::FAILURE;
    }
    report_host_start_failure_until(
        sibling_is_file,
        std::io::ErrorKind::Other,
        Some(sink),
        deadline,
    )
}

fn status_to_exit_code(status: ExitStatus) -> ExitCode {
    if status.success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(
            status
                .code()
                .and_then(|code| u8::try_from(code).ok())
                .filter(|code| *code != 0)
                .unwrap_or(1),
        )
    }
}

fn report_host_start_failure(
    sibling_is_file: bool,
    error_kind: std::io::ErrorKind,
    sink: Option<&BoundedStderrSink>,
) -> ExitCode {
    report_host_start_failure_until(
        sibling_is_file,
        error_kind,
        sink,
        Instant::now() + POST_EXIT_DRAIN_TIMEOUT,
    )
}

fn report_host_start_failure_until(
    sibling_is_file: bool,
    error_kind: std::io::ErrorKind,
    sink: Option<&BoundedStderrSink>,
    deadline: Instant,
) -> ExitCode {
    let (code, message) = if error_kind == std::io::ErrorKind::NotFound && !sibling_is_file {
        (
            "HOST_BINARY_MISSING",
            "install the swarm-host sibling beside the public swarm executable and retry",
        )
    } else {
        (
            "HOST_START_FAILED",
            "check the host executable, runtime dependencies and launch permissions, then retry",
        )
    };
    let envelope = wrapper_error_json(code, message, false, None, None);
    emit_host_wrapper_error(sink, &envelope, deadline);
    ExitCode::FAILURE
}

fn delegate_to_mcp(cli: &Cli) -> ExitCode {
    let profile = match &cli.command {
        Command::Mcp { profile } => profile.as_deref(),
        _ => unreachable!("MCP delegation called for a different command"),
    };
    let mut arguments = Vec::new();
    if let Some(path) = cli.config.as_deref() {
        arguments.push(OsString::from("--config"));
        arguments.push(path.as_os_str().to_owned());
    }
    if let Some(path) = cli.data_dir.as_deref() {
        arguments.push(OsString::from("--data-dir"));
        arguments.push(path.as_os_str().to_owned());
    }
    if let Some(path) = cli.credential.as_deref() {
        arguments.push(OsString::from("--credential"));
        arguments.push(path.as_os_str().to_owned());
    }
    if let Some(profile) = profile {
        arguments.push(OsString::from("--profile"));
        arguments.push(OsString::from(profile));
    }

    let mut executable = match std::env::current_exe() {
        Ok(path) => path,
        Err(_) => {
            eprintln!(
                "{}",
                json!({"error":{"code":"MCP_BINARY_PATH_FAILED","message":"could not locate the public CLI executable"}})
            );
            return ExitCode::FAILURE;
        }
    };
    executable.set_file_name(if cfg!(windows) {
        "swarm-mcp.exe"
    } else {
        "swarm-mcp"
    });
    let sibling_is_file = executable.is_file();
    match ProcessCommand::new(&executable).args(arguments).status() {
        Ok(status) if status.success() => ExitCode::SUCCESS,
        Ok(status) => {
            eprintln!(
                "{}",
                json!({"error":{"code":"MCP_COMMAND_FAILED","message":"the explicit MCP command exited unsuccessfully","exit_code":status.code()}})
            );
            ExitCode::from(
                status
                    .code()
                    .and_then(|code| u8::try_from(code).ok())
                    .filter(|code| *code != 0)
                    .unwrap_or(1),
            )
        }
        Err(error) => {
            let (code, message) = if error.kind() == std::io::ErrorKind::NotFound
                && !sibling_is_file
            {
                (
                    "MCP_BINARY_MISSING",
                    "install the swarm-mcp sibling beside the public swarm executable and retry",
                )
            } else {
                (
                    "MCP_START_FAILED",
                    "check the MCP executable, runtime dependencies and launch permissions, then retry",
                )
            };
            eprintln!("{}", json!({"error":{"code":code,"message":message}}));
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let config = ClientConfig::load(cli.config.as_deref(), cli.data_dir.as_deref())?;
    let credential_path = cli
        .credential
        .unwrap_or_else(|| config.data_dir.join("operator.json"));
    let credential = load_credential(&credential_path)?;
    let request_id = cli.request_id;
    if let Command::Call { method, .. } = &cli.command
        && method == "hook.source.setup"
    {
        return Err(Error::invalid(
            "use `swarm hook setup` so the one-time source credential is written privately and redacted from output",
        ));
    }
    match cli.command {
        Command::Mcp { .. } => unreachable!("MCP delegation returned above"),
        command => {
            let (method, params) = map_command(command)?;
            validate_call_method(&method)?;
            let (params, prepared_id) = prepare_call(&method, params, request_id.as_deref())?;
            if let Some(request_id) = prepared_id {
                eprintln!("client_request_id={request_id}");
            }
            let result = call(&config.data_dir, &credential, &config.ipc, &method, params).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
    }
}

fn read_json(file: &PathBuf) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(file)?)?)
}

fn read_json_with_concilium_id(file: &PathBuf, concilium_id: &str) -> Result<Value> {
    let mut value = read_json(file)?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| Error::invalid("Concilium close file must contain a JSON object"))?;
    if object
        .get("concilium_id")
        .is_some_and(|existing| existing.as_str() != Some(concilium_id))
    {
        return Err(Error::invalid(
            "Concilium close file ID does not match the command's Concilium ID",
        ));
    }
    object.insert("concilium_id".into(), json!(concilium_id));
    Ok(value)
}

fn load_credential(path: &Path) -> Result<Credential> {
    let credential: Credential = serde_json::from_slice(&std::fs::read(path)?)?;
    if credential.client_id.is_empty() || credential.token.len() < 32 {
        return Err(Error::new("AUTH_ERROR", "invalid credential file"));
    }
    Ok(credential)
}

fn map_command(command: Command) -> Result<(String, Value)> {
    let (method, params): (String, Value) = match command {
        Command::Host { .. }
        | Command::Mcp { .. }
        | Command::Gateway
        | Command::CheckWorker { .. }
        | Command::OwnedOpencodeService { .. }
        | Command::ScriptWorker { .. }
        | Command::ModuleRun { .. } => {
            unreachable!("executor returned above")
        }
        Command::Observer { .. } => unreachable!("observer read command returned above"),
        Command::Source {
            command: SourceCommand::Capture { file },
        } => ("source.capture".into(), read_json(&file)?),
        Command::Code {
            command: CodeCommand::Scope { command },
        } => match command {
            CodeScopeCommand::Propose { file } => ("code.scope.propose".into(), read_json(&file)?),
            CodeScopeCommand::Accept { file } => ("code.scope.accept".into(), read_json(&file)?),
            CodeScopeCommand::Inspect {
                task_id,
                task_revision,
                attempt_id,
                scope_intent_id,
                client_id,
                path,
                symbol,
                interface,
                after_scope_id,
                limit,
            } => {
                let mut params = json!({"task_id":task_id,"limit":limit});
                if let Some(task_revision) = task_revision {
                    params["task_revision"] = json!(task_revision);
                }
                if let Some(attempt_id) = attempt_id {
                    params["attempt_id"] = json!(attempt_id);
                }
                if let Some(scope_intent_id) = scope_intent_id {
                    params["scope_intent_id"] = json!(scope_intent_id);
                }
                if let Some(client_id) = client_id {
                    params["client_id"] = json!(client_id);
                }
                if let Some(path) = path {
                    params["path"] = json!(path);
                }
                if let Some(symbol) = symbol {
                    params["symbol"] = json!(symbol);
                }
                if let Some(interface) = interface {
                    params["interface"] = json!(interface);
                }
                if let Some(after_scope_id) = after_scope_id {
                    params["after_scope_id"] = json!(after_scope_id);
                }
                ("code.scope.inspect".into(), params)
            }
            CodeScopeCommand::Conflicts {
                task_id,
                task_revision,
                attempt_id,
                scope_intent_id,
                client_id,
                path,
                symbol,
                interface,
                after_scope_id,
                limit,
            } => {
                let mut params = json!({"task_id":task_id,"limit":limit});
                if let Some(task_revision) = task_revision {
                    params["task_revision"] = json!(task_revision);
                }
                if let Some(attempt_id) = attempt_id {
                    params["attempt_id"] = json!(attempt_id);
                }
                if let Some(scope_intent_id) = scope_intent_id {
                    params["scope_intent_id"] = json!(scope_intent_id);
                }
                if let Some(client_id) = client_id {
                    params["client_id"] = json!(client_id);
                }
                if let Some(path) = path {
                    params["path"] = json!(path);
                }
                if let Some(symbol) = symbol {
                    params["symbol"] = json!(symbol);
                }
                if let Some(interface) = interface {
                    params["interface"] = json!(interface);
                }
                if let Some(after_scope_id) = after_scope_id {
                    params["after_scope_id"] = json!(after_scope_id);
                }
                ("code.scope.conflicts".into(), params)
            }
            CodeScopeCommand::Release { file } => ("code.scope.release".into(), read_json(&file)?),
        },
        Command::Check { command } => match command {
            CheckCommand::Run { file } => ("check.run".into(), read_json(&file)?),
            CheckCommand::Get { check_id } => ("check.get".into(), json!({"check_id":check_id})),
            CheckCommand::Profiles => ("check.profiles".into(), json!({})),
            CheckCommand::Cancel { check_id, reason } => (
                "check.cancel".into(),
                json!({"check_id":check_id,"reason":reason}),
            ),
        },
        Command::Status => ("host.status".to_string(), json!({})),
        Command::Doctor => ("doctor.inspect".to_string(), json!({})),
        Command::Call { method, file } => (
            method,
            if let Some(p) = file {
                read_json(&p)?
            } else {
                json!({})
            },
        ),
        Command::Agent { command } => match command {
            AgentCommand::Usage {
                binding_id,
                generation,
            } => (
                "agent.usage".into(),
                json!({"binding_id":binding_id,"generation":generation}),
            ),
            AgentCommand::List { after, limit } => {
                ("agent.list".into(), json!({"after":after,"limit":limit}))
            }
            AgentCommand::Get {
                binding_id,
                generation,
            } => (
                "agent.state".into(),
                json!({"binding_id":binding_id,"generation":generation}),
            ),
        },
        Command::Attempt { command } => match command {
            AttemptCommand::Get { attempt_id } => {
                ("attempt.get".into(), json!({"attempt_id":attempt_id}))
            }
        },
        Command::Family {
            binding_id,
            generation,
            observation_id,
            after,
            limit,
        } => {
            let mut value = json!({"binding_id":binding_id,"generation":generation,"after":after,"limit":limit});
            if let Some(id) = observation_id {
                value["observation_id"] = json!(id);
            }
            ("agent.family".into(), value)
        }
        Command::Result {
            binding_id,
            generation,
            file,
            offset,
            length,
        } => (
            "agent.result".into(),
            json!({"binding_id":binding_id,"generation":generation,
                "selector":read_json(&file)?,"offset_bytes":offset,"length_bytes":length}),
        ),
        Command::Artifact { command } => match command {
            ArtifactCommand::Assemble { file } => ("artifact.assemble".into(), read_json(&file)?),
            ArtifactCommand::Parts {
                artifact_id,
                after,
                limit,
            } => (
                "artifact.parts".into(),
                json!({"artifact_id":artifact_id,"after":after,"limit":limit}),
            ),
            ArtifactCommand::Export { .. } => unreachable!("export returned above"),
            ArtifactCommand::Get { artifact_id } => {
                ("artifact.get".into(), json!({"artifact_id":artifact_id}))
            }
            ArtifactCommand::Read {
                artifact_id,
                offset,
                length,
            } => (
                "artifact.read".into(),
                json!({"artifact_id":artifact_id,"offset_bytes":offset,"length_bytes":length}),
            ),
        },
        Command::Report { after, limit } => {
            ("report.delta".into(), json!({"after":after,"limit":limit}))
        }
        Command::Monitor { command } => match command {
            MonitorCommand::Snapshot { limit } => {
                ("monitor.snapshot".into(), json!({"limit":limit}))
            }
            MonitorCommand::Follow { after, limit } => (
                "monitor.follow".into(),
                json!({"after":after,"limit":limit}),
            ),
        },
        Command::Task { command } => match command {
            TaskCommand::Accept { file } => ("task.accept".into(), read_json(&file)?),
            TaskCommand::Acceptance {
                acceptance_operation_id,
            } => (
                "task.acceptance".into(),
                json!({"acceptance_operation_id":acceptance_operation_id}),
            ),
            TaskCommand::InvalidateAcceptance { file } => {
                ("task.invalidate_acceptance".into(), read_json(&file)?)
            }
            TaskCommand::Submit { file } => ("task.submit".into(), read_json(&file)?),
            TaskCommand::RecoverSubmission { operation_id } => (
                "task.submit.recover".into(),
                json!({"operation_id":operation_id}),
            ),
            TaskCommand::RequestChanges { file } => {
                ("task.request_changes".into(), read_json(&file)?)
            }
            TaskCommand::Submission {
                submission_ref,
                after,
                limit,
            } => (
                "task.submission".into(),
                json!({"submission_ref":submission_ref,"after":after,"limit":limit}),
            ),
            TaskCommand::Create {
                project,
                file,
                origin_key,
            } => {
                let mut value = json!({"project_id":project,"spec":read_json(&file)?});
                if let Some(origin) = origin_key {
                    value["origin_key"] = json!(origin);
                }
                ("task.create".into(), value)
            }
            TaskCommand::Get { task_id } => ("task.get".into(), json!({"task_id":task_id})),
            TaskCommand::List { after, limit } => {
                ("task.list".into(), json!({"after":after,"limit":limit}))
            }
            TaskCommand::Claim {
                task_id,
                revision,
                owner,
                start_owner,
                binding_id,
                generation,
            } => {
                let mut value = json!({"task_id":task_id,"expected_revision":revision,"start_owner":start_owner});
                if let Some(owner) = owner {
                    value["owner_id"] = json!(owner);
                }
                if let (Some(binding), Some(generation)) = (binding_id, generation) {
                    value["binding_id"] = json!(binding);
                    value["binding_generation"] = json!(generation);
                }
                ("task.claim".into(), value)
            }
            TaskCommand::Bind {
                attempt_id,
                assignment,
                session,
                turn,
                observation_id,
            } => (
                "attempt.bind_producer".into(),
                json!({"attempt_id":attempt_id,"assignment_id":assignment,
                    "native_session_id":session,"native_run_id":turn,"observation_id":observation_id}),
            ),
            TaskCommand::Revise {
                task_id,
                revision,
                file,
            } => (
                "task.revise".into(),
                json!({"task_id":task_id,"expected_revision":revision,"spec":read_json(&file)?}),
            ),
        },
        Command::Coordination { command } => match command {
            CoordinationCommand::Participant { command } => match command {
                ParticipantCommand::Register { file } => (
                    "coordination.participant.register".into(),
                    read_json(&file)?,
                ),
                ParticipantCommand::Disable { file } => {
                    ("coordination.participant.disable".into(), read_json(&file)?)
                }
                ParticipantCommand::Get { file } => {
                    ("coordination.participant.get".into(), read_json(&file)?)
                }
                ParticipantCommand::List { file } => {
                    ("coordination.participant.list".into(), read_json(&file)?)
                }
            },
            CoordinationCommand::Peer { command } => match command {
                PeerCommand::Find { file } => ("coordination.peer.find".into(), read_json(&file)?),
            },
            CoordinationCommand::WorkCard { command } => match command {
                CardCommand::Publish { file } => {
                    ("coordination.work_card.publish".into(), read_json(&file)?)
                }
                CardCommand::Withdraw { file } => {
                    ("coordination.work_card.withdraw".into(), read_json(&file)?)
                }
                CardCommand::Get { file } => {
                    ("coordination.work_card.get".into(), read_json(&file)?)
                }
                CardCommand::List { file } => {
                    ("coordination.work_card.list".into(), read_json(&file)?)
                }
            },
            CoordinationCommand::ContractCard { command } => match command {
                CardCommand::Publish { file } => (
                    "coordination.contract_card.publish".into(),
                    read_json(&file)?,
                ),
                CardCommand::Withdraw { file } => (
                    "coordination.contract_card.withdraw".into(),
                    read_json(&file)?,
                ),
                CardCommand::Get { file } => {
                    ("coordination.contract_card.get".into(), read_json(&file)?)
                }
                CardCommand::List { file } => {
                    ("coordination.contract_card.list".into(), read_json(&file)?)
                }
            },
            CoordinationCommand::Consult { file } => {
                ("coordination.consult".into(), read_json(&file)?)
            }
            CoordinationCommand::SyncIntegration { file } => {
                ("coordination.sync_integration".into(), read_json(&file)?)
            }
            CoordinationCommand::OverlapCheck { file } => {
                ("swarm.overlap.check".into(), read_json(&file)?)
            }
            CoordinationCommand::Watch { command } => match command {
                CoordinationWatchCommand::Create { file } => {
                    ("coordination.watch.create".into(), read_json(&file)?)
                }
                CoordinationWatchCommand::List { file } => {
                    ("coordination.watch.list".into(), read_json(&file)?)
                }
                CoordinationWatchCommand::Cancel { file } => {
                    ("coordination.watch.cancel".into(), read_json(&file)?)
                }
            },
            CoordinationCommand::Thread { command } => match command {
                CoordinationThreadCommand::Open { file } => {
                    ("coordination.thread.open".into(), read_json(&file)?)
                }
                CoordinationThreadCommand::Get {
                    thread_id,
                    after_message_seq,
                    limit,
                } => {
                    let mut params = json!({"thread_id":thread_id,"limit":limit});
                    if let Some(after_message_seq) = after_message_seq {
                        params["after_message_seq"] = json!(after_message_seq);
                    }
                    ("coordination.thread.get".into(), params)
                }
                CoordinationThreadCommand::List {
                    task_id,
                    attempt_id,
                    state,
                    topic_kind,
                    after_thread_id,
                    limit,
                } => {
                    let mut params = json!({"task_id":task_id,"limit":limit});
                    if let Some(attempt_id) = attempt_id {
                        params["attempt_id"] = json!(attempt_id);
                    }
                    if let Some(state) = state {
                        params["state"] = json!(state);
                    }
                    if let Some(topic_kind) = topic_kind {
                        params["topic_kind"] = json!(topic_kind);
                    }
                    if let Some(after_thread_id) = after_thread_id {
                        params["after_thread_id"] = json!(after_thread_id);
                    }
                    ("coordination.thread.list".into(), params)
                }
                CoordinationThreadCommand::Resolve { file } => {
                    ("coordination.thread.resolve".into(), read_json(&file)?)
                }
                CoordinationThreadCommand::Withdraw { file } => {
                    ("coordination.thread.withdraw".into(), read_json(&file)?)
                }
                CoordinationThreadCommand::Supersede { file } => {
                    ("coordination.thread.supersede".into(), read_json(&file)?)
                }
            },
            CoordinationCommand::Message { command } => match command {
                CoordinationMessageCommand::Send { file } => {
                    ("coordination.message.send".into(), read_json(&file)?)
                }
            },
            CoordinationCommand::Contract { command } => match command {
                CoordinationContractCommand::Propose { file } => {
                    ("coordination.contract.propose".into(), read_json(&file)?)
                }
                CoordinationContractCommand::Respond { file } => {
                    ("coordination.contract.respond".into(), read_json(&file)?)
                }
                CoordinationContractCommand::Get {
                    thread_id,
                    proposal_id,
                    proposal_revision_id,
                    after_observation_id,
                    limit,
                } => {
                    let mut params = json!({"thread_id":thread_id,"proposal_id":proposal_id,
                        "proposal_revision_id":proposal_revision_id,"limit":limit});
                    if let Some(after_observation_id) = after_observation_id {
                        params["after_observation_id"] = json!(after_observation_id);
                    }
                    ("coordination.contract.get".into(), params)
                }
                CoordinationContractCommand::List {
                    thread_id,
                    after_sequence,
                    limit,
                } => {
                    let mut params = json!({"thread_id":thread_id,"limit":limit});
                    if let Some(after_sequence) = after_sequence {
                        params["after_sequence"] = json!(after_sequence);
                    }
                    ("coordination.contract.list".into(), params)
                }
            },
            CoordinationCommand::Integration { command } => match command {
                CoordinationIntegrationCommand::Ack { file } => {
                    ("coordination.integration.ack".into(), read_json(&file)?)
                }
            },
            CoordinationCommand::Agreement { command } => match command {
                CoordinationAgreementCommand::Get {
                    cell_id,
                    task_id,
                    task_revision,
                    attempt_id,
                    state_revision,
                    material_digest,
                    after_position_id,
                    limit,
                } => {
                    let mut params = json!({"cell_id":cell_id,"task_id":task_id,
                        "task_revision":task_revision,"attempt_id":attempt_id,"limit":limit});
                    if let Some(state_revision) = state_revision {
                        params["state_revision"] = json!(state_revision);
                    }
                    if let Some(material_digest) = material_digest {
                        params["material_digest"] = json!(material_digest);
                    }
                    if let Some(after_position_id) = after_position_id {
                        params["after_position_id"] = json!(after_position_id);
                    }
                    ("coordination.agreement.get".into(), params)
                }
            },
            CoordinationCommand::Send { file } => ("coordination.send".into(), read_json(&file)?),
            CoordinationCommand::Inbox { file } => ("coordination.inbox".into(), read_json(&file)?),
            CoordinationCommand::Context { file } => {
                ("swarm.context.get".into(), read_json(&file)?)
            }
        },
        Command::Concilium { command } => match command {
            ConciliumCommand::Propose { file } => ("concilium.propose".into(), read_json(&file)?),
            ConciliumCommand::Preview {
                proposal_operation_id,
            } => (
                "concilium.preview".into(),
                json!({"proposal_operation_id":proposal_operation_id}),
            ),
            ConciliumCommand::Open { file } => ("concilium.open".into(), read_json(&file)?),
            ConciliumCommand::PositionSubmit { file } => {
                ("concilium.position.submit".into(), read_json(&file)?)
            }
            ConciliumCommand::RoundAdvance { file } => {
                ("concilium.round.advance".into(), read_json(&file)?)
            }
            ConciliumCommand::Get { concilium_id } => {
                ("concilium.get".into(), json!({"concilium_id":concilium_id}))
            }
            ConciliumCommand::List {
                task_id,
                attempt_id,
                state,
                after_concilium_id,
                limit,
            } => {
                let mut params = json!({"task_id":task_id,"limit":limit});
                if let Some(attempt_id) = attempt_id {
                    params["attempt_id"] = json!(attempt_id);
                }
                if let Some(state) = state {
                    params["state"] = json!(state);
                }
                if let Some(after_concilium_id) = after_concilium_id {
                    params["after_concilium_id"] = json!(after_concilium_id);
                }
                ("concilium.list".into(), params)
            }
            ConciliumCommand::Close { concilium_id, file } => (
                "concilium.close".into(),
                read_json_with_concilium_id(&file, &concilium_id)?,
            ),
        },
        Command::Review { command } => match command {
            ReviewCommand::Assign { file } => ("review.assign".into(), read_json(&file)?),
            ReviewCommand::Submit { file } => ("review.submit".into(), read_json(&file)?),
            ReviewCommand::Get { file } => ("review.get".into(), read_json(&file)?),
            ReviewCommand::List { file } => ("review.list".into(), read_json(&file)?),
            ReviewCommand::Context { file } => ("swarm.review.context".into(), read_json(&file)?),
        },
        Command::Automation { command } => match command {
            AutomationCommand::Config { command } => match command {
                AutomationConfigCommand::Get { file } => {
                    ("automation.config.get".into(), read_json(&file)?)
                }
                AutomationConfigCommand::Preview { file } => {
                    ("automation.config.preview".into(), read_json(&file)?)
                }
                AutomationConfigCommand::Apply { file } => {
                    ("automation.config.apply".into(), read_json(&file)?)
                }
                AutomationConfigCommand::Explain { file } => {
                    ("automation.config.explain".into(), read_json(&file)?)
                }
                AutomationConfigCommand::Transfer { file } => {
                    ("automation.config.transfer".into(), read_json(&file)?)
                }
            },
        },
        Command::Script { command } => match command {
            ScriptCommand::Register { file } => ("script.register".into(), read_json(&file)?),
            ScriptCommand::Revise { file } => ("script.revise".into(), read_json(&file)?),
            ScriptCommand::Validate {
                script_id,
                revision,
            } => (
                "script.validate".into(),
                json!({"script_id":script_id,"revision":revision}),
            ),
            ScriptCommand::Activate {
                script_id,
                revision,
            } => (
                "script.activate".into(),
                json!({"script_id":script_id,"revision":revision}),
            ),
            ScriptCommand::Run { file } => ("script.run".into(), read_json(&file)?),
            ScriptCommand::Get {
                script_id,
                revision,
            } => {
                let mut params = json!({"script_id":script_id});
                if let Some(revision) = revision {
                    params["revision"] = json!(revision);
                }
                ("script.get".into(), params)
            }
            ScriptCommand::List { after, limit } => {
                ("script.list".into(), json!({"after":after,"limit":limit}))
            }
        },
        Command::Hook { command } => match command {
            HookCommand::Setup { .. } => unreachable!("setup returned above"),
            HookCommand::Emit {
                source_id,
                commit_oid,
            } => (
                "hook.emit".into(),
                json!({"source_id":source_id,"commit_oid":commit_oid}),
            ),
            HookCommand::Source { command } => match command {
                HookSourceCommand::Get {
                    source_id,
                    after,
                    limit,
                } => (
                    "hook.source.get".into(),
                    json!({"source_id":source_id,"after":after,"limit":limit}),
                ),
                HookSourceCommand::Revoke {
                    source_id,
                    revision,
                } => (
                    "hook.source.revoke".into(),
                    json!({"source_id":source_id,"expected_revision":revision}),
                ),
            },
            HookCommand::Install { .. } => unreachable!("installer commands return above"),
        },
        Command::Goal { command } => match command {
            GoalCommand::Create { file } => ("goal.create".into(), read_json(&file)?),
            GoalCommand::Revise { file } => ("goal.revise".into(), read_json(&file)?),
            GoalCommand::Enable { file } => ("goal.enable".into(), read_json(&file)?),
            GoalCommand::Disable { file } => ("goal.disable".into(), read_json(&file)?),
            GoalCommand::Readback { file } => ("goal.readback".into(), read_json(&file)?),
            GoalCommand::Get { file } => ("goal.get".into(), read_json(&file)?),
            GoalCommand::List { file } => ("goal.list".into(), read_json(&file)?),
        },
        Command::GitHub { command } => match command {
            GitHubCommand::Source { command } => match command {
                GitHubSourceCommand::Inspect { host, owner, repo } => (
                    "github.source.inspect".into(),
                    json!({"host":host,"owner":owner,"repo":repo}),
                ),
                GitHubSourceCommand::Setup {
                    source_id,
                    project_id,
                    host,
                    owner,
                    repo,
                    repository_id,
                } => (
                    "github.source.setup".into(),
                    json!({
                        "source_id":source_id,
                        "project_id":project_id,
                        "host":host,
                        "owner":owner,
                        "repo":repo,
                        "repository_id":repository_id
                    }),
                ),
                GitHubSourceCommand::Get { source_id } => {
                    ("github.source.get".into(), json!({"source_id":source_id}))
                }
                GitHubSourceCommand::Poll { source_id } => {
                    ("github.source.poll".into(), json!({"source_id":source_id}))
                }
            },
            GitHubCommand::WorkPool { command } => match command {
                GitHubWorkPoolCommand::Preview {
                    source_id,
                    after,
                    limit,
                } => {
                    let mut params = json!({"source_id":source_id});
                    if let Some(after) = after {
                        params["after"] = json!(after);
                    }
                    if let Some(limit) = limit {
                        params["limit"] = json!(limit);
                    }
                    ("github.work_pool.preview".into(), params)
                }
                GitHubWorkPoolCommand::Apply {
                    source_id,
                    task_ids,
                } => (
                    "github.work_pool.apply".into(),
                    json!({"source_id":source_id,"task_ids":task_ids}),
                ),
            },
        },
        Command::Launcher { command } => match command {
            LauncherCommand::Dashboard { file } => ("swarm.dashboard".into(), read_json(&file)?),
            LauncherCommand::Preview { file } => ("swarm.launch.preview".into(), read_json(&file)?),
            LauncherCommand::Launch { file } => ("swarm.launch".into(), read_json(&file)?),
            LauncherCommand::Queue { command } => match command {
                LauncherQueueCommand::Get { file } => ("swarm.queue.get".into(), read_json(&file)?),
            },
            LauncherCommand::Agent { command } => match command {
                LauncherAgentCommand::Inspect { file } => {
                    ("swarm.agent.inspect".into(), read_json(&file)?)
                }
            },
            LauncherCommand::Exceptions { command } => match command {
                LauncherExceptionsCommand::Get { file } => {
                    ("swarm.exceptions.get".into(), read_json(&file)?)
                }
            },
        },
        Command::ClientCreate { .. } => unreachable!("client creation delegates to swarm-host"),
        Command::GmHandover {
            client_id,
            binding_id,
            generation,
        } => {
            let mut value = json!({"client_id":client_id});
            if let Some(id) = binding_id {
                value["binding_id"] = json!(id);
            }
            if let Some(generation) = generation {
                value["binding_generation"] = json!(generation);
            }
            ("gm.handover".into(), value)
        }
    };
    Ok((method, params))
}
