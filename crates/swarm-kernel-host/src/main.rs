use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use swarm_kernel_host::{
    config::{Config, Ipc},
    error::{Error, Result},
    host, ipc,
    model::{self, Credential},
    platform,
};

#[derive(Parser)]
#[command(
    name = "swarm-kernel-host",
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
        /// Qualification-only: propagate one hidden transport-fault flag into the
        /// Store-owned scheduler worker launch.
        ///
        /// This field exists only in a build compiled with the qualification-faults
        /// feature. A release-default build does not contain the token at all, so
        /// the accepted CLI surface is unchanged and the flag is rejected as
        /// unknown there.
        ///
        /// The field is hidden from help output, so no normal operator surface
        /// mentions it.
        ///
        /// The host reads it only to pass the same single-use flag through to the
        /// worker it launches. Admission, the schedule cursor, the durable receipt,
        /// the credential, and the owner receipt stay owned by the Store.
        #[cfg(feature = "qualification-faults")]
        #[arg(long, hide = true)]
        qualification_drop_first_admit_reply: bool,
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
    /// Internal independent owner for the explicit ModuleRun command.
    #[command(hide = true)]
    ModuleOwnerWorker {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        command: PathBuf,
        #[arg(long)]
        invocation: String,
        #[arg(last = true)]
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
    /// Internal isolated owner for one admitted Zed batch; opens no Store.
    #[command(hide = true)]
    ZedBatchWorker {
        #[arg(long)]
        file: PathBuf,
    },
    Source {
        #[command(subcommand)]
        command: SourceCommand,
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
    Family {
        binding_id: String,
        #[arg(long)]
        generation: i64,
        /// Exact Task Attempt scope, required for Manager reads.
        #[arg(long)]
        attempt_id: Option<String>,
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
    /// Read the Store-backed Manager live monitor.
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
fn read_json(file: &PathBuf) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(file)?)?)
}
fn main() {
    if let Err(e) = execute(Cli::parse()) {
        eprintln!("{}", json!({"error":e}));
        std::process::exit(1);
    }
}
fn execute(cli: Cli) -> Result<()> {
    if let Command::Mcp { profile } = &cli.command {
        let mut executable = std::env::current_exe()?;
        executable.set_file_name(if cfg!(windows) {
            "swarm-mcp.exe"
        } else {
            "swarm-mcp"
        });
        let mut child = std::process::Command::new(&executable);
        for (flag, value) in [
            ("--config", cli.config.as_ref()),
            ("--data-dir", cli.data_dir.as_ref()),
            ("--credential", cli.credential.as_ref()),
        ] {
            if let Some(value) = value {
                child.arg(flag).arg(value);
            }
        }
        if let Some(profile) = profile {
            child.arg("--profile").arg(profile);
        }
        let status = child
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .map_err(|error| {
                Error::new(
                    "MCP_FRONTEND_START_FAILED",
                    format!("could not start '{}': {error}", executable.display()),
                )
            })?;
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            std::process::exit(
                status
                    .code()
                    .unwrap_or_else(|| 128 + status.signal().unwrap_or(1)),
            );
        }
        #[cfg(not(unix))]
        std::process::exit(status.code().unwrap_or(1));
    }
    if let Command::ModuleRun {
        state_dir,
        command,
        args,
    } = &cli.command
    {
        return swarm_kernel_host::runtime::owner::run(state_dir, command, args);
    }
    if let Command::ModuleOwnerWorker {
        state_dir,
        command,
        invocation,
        args,
    } = &cli.command
    {
        swarm_kernel_host::runtime::owner::run_worker(state_dir, command, args, invocation);
        return Ok(());
    }
    if let Command::CheckWorker { file } = &cli.command {
        return swarm_kernel_host::checks::worker::run(file);
    }
    if let Command::OwnedOpencodeService { file } = &cli.command {
        return swarm_kernel_host::runtime::opencode_v2::run_owned_service_helper(file);
    }
    if let Command::ScriptWorker { file } = &cli.command {
        return swarm_kernel_host::scripts::runner::run_worker(file);
    }
    if let Command::ZedBatchWorker { file } = &cli.command {
        return swarm_kernel_host::runtime::zed::worker::run_worker(file);
    }
    // The host and gateway are long-lived. CLI calls perform one local exchange
    // and must not create a CPU-sized pool for every status request.
    let mut builder = if matches!(&cli.command, Command::Host { .. } | Command::Gateway) {
        tokio::runtime::Builder::new_multi_thread()
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    let runtime = builder.enable_all().build()?;
    runtime.block_on(run(cli))
}
async fn run(cli: Cli) -> Result<()> {
    let config = Config::load(cli.config.as_deref(), cli.data_dir.as_deref())?;
    // The pattern is deliberately open-ended with `..`, so the qualification-only
    // field below exists only in a feature build and this arm compiles in both.
    if let Command::Host {
        stop_on_stdin_eof, ..
    } = &cli.command
    {
        return if *stop_on_stdin_eof {
            host::run_on_stdin_eof(config).await
        } else {
            host::run(config).await
        };
    }
    if matches!(&cli.command, Command::Gateway) {
        if cli.credential.is_some() {
            return Err(Error::invalid(
                "gateway principal is fixed by gateway.credential_file; --credential is not accepted",
            ));
        }
        if cli.request_id.is_some() {
            return Err(Error::invalid("--request-id is not accepted for gateway"));
        }
        if !config.gateway.enabled {
            return Err(Error::invalid(
                "gateway is disabled; set gateway.enabled = true in the local configuration",
            ));
        }
        return run_gateway_binary(cli.config.as_deref(), cli.data_dir.as_deref()).await;
    }
    if let Command::Observer {
        command:
            ObserverCommand::Follow {
                after_segment,
                after_offset,
                once,
            },
    } = &cli.command
    {
        if cli.request_id.is_some() || cli.credential.is_some() {
            return Err(Error::invalid(
                "observer follow reads only the current user's private local files and accepts no request or credential override",
            ));
        }
        let cursor = match (after_segment, after_offset) {
            (Some(segment), Some(offset)) => Some(swarm_observer::follow::FileCursor {
                segment: *segment,
                offset: *offset,
            }),
            (None, None) => None,
            _ => {
                return Err(Error::invalid(
                    "--after-segment and --after-offset must be supplied together",
                ));
            }
        };
        swarm_observer::follow::follow_local(
            &config
                .observability
                .recording_directory(&config.storage.data_dir),
            cursor,
            !*once,
            &mut std::io::stdout(),
        )?;
        return Ok(());
    }
    if let Command::Observer {
        command:
            ObserverCommand::Metrics {
                host_identity,
                module_owner,
                module_worker,
                child_role,
                interval_ms,
            },
    } = &cli.command
    {
        if cli.request_id.is_some() || cli.credential.is_some() {
            return Err(Error::invalid(
                "observer metrics is a local read-only command and accepts no request or credential override",
            ));
        }
        let child_role = match child_role.as_deref() {
            Some("helper") => Some(swarm_observer::process_metrics::ProcessRole::Helper),
            Some("adapter") => Some(swarm_observer::process_metrics::ProcessRole::Adapter),
            None => None,
            _ => return Err(Error::invalid("unsupported observer child role")),
        };
        let default_host_identity = if host_identity.is_none()
            && module_owner.is_none()
            && module_worker.is_none()
            && child_role.is_none()
            && config.observability.enabled
        {
            std::fs::canonicalize(&config.storage.data_dir)
                .ok()
                .map(|root| swarm_observer::host_image_receipt::default_path(&root))
        } else {
            None
        };
        let selected_host_identity = host_identity
            .as_deref()
            .or(default_host_identity.as_deref());
        let report = swarm_observer::metrics_cli::run_explicit_metrics(
            config.observability.enabled,
            selected_host_identity,
            module_owner.as_deref(),
            module_worker.as_deref(),
            child_role,
            *interval_ms,
        )?;
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }
    let credential = platform::load_credential(
        &cli.credential
            .unwrap_or_else(|| config.storage.data_dir.join("operator.json")),
    )?;
    if let Command::Observer {
        command: ObserverCommand::Snapshot { after, limit },
    } = &cli.command
    {
        if cli.request_id.is_some() {
            return Err(Error::invalid(
                "--request-id is not used for observer snapshot reads",
            ));
        }
        let snapshot = swarm_observer::readback_snapshot(
            &config.storage.data_dir,
            &credential,
            &config.ipc,
            *after,
            *limit,
        )
        .await?;
        println!("{}", serde_json::to_string_pretty(&snapshot)?);
        return Ok(());
    }
    if let Command::Call { method, .. } = &cli.command
        && method == "hook.source.setup"
    {
        return Err(Error::invalid(
            "use `swarm hook setup` so the one-time source credential is written privately and redacted from output",
        ));
    }
    if let Command::Hook {
        command: HookCommand::Setup { project_id },
    } = &cli.command
    {
        return swarm_kernel_host::hooks::setup::hook_setup(
            &config,
            &credential,
            &cli.request_id,
            project_id,
        )
        .await;
    }
    if let Command::Hook {
        command: HookCommand::Install { command },
    } = &cli.command
    {
        return hook_install(&config, &credential, &cli.request_id, command).await;
    }
    if let Command::Artifact {
        command: ArtifactCommand::Export { artifact_id, out },
    } = &cli.command
    {
        let result = swarm_kernel_host::export::artifact(
            &config.storage.data_dir,
            &credential,
            &config.ipc,
            artifact_id,
            out,
        )
        .await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }
    let mut pending_credential = None;
    let (method, mut params) = match cli.command {
        Command::Host { .. }
        | Command::Mcp { .. }
        | Command::Gateway
        | Command::CheckWorker { .. }
        | Command::OwnedOpencodeService { .. }
        | Command::ScriptWorker { .. }
        | Command::ZedBatchWorker { .. }
        | Command::ModuleRun { .. }
        | Command::ModuleOwnerWorker { .. } => {
            unreachable!("executor returned above")
        }
        Command::Observer { .. } => unreachable!("observer read command returned above"),
        Command::Source {
            command: SourceCommand::Capture { file },
        } => ("source.capture".into(), read_json(&file)?),
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
        Command::Family {
            binding_id,
            generation,
            attempt_id,
            observation_id,
            after,
            limit,
        } => {
            let mut value = json!({"binding_id":binding_id,"generation":generation,"after":after,"limit":limit});
            if let Some(id) = attempt_id {
                value["attempt_id"] = json!(id);
            }
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
            CoordinationCommand::Send { file } => ("coordination.send".into(), read_json(&file)?),
            CoordinationCommand::Inbox { file } => ("coordination.inbox".into(), read_json(&file)?),
            CoordinationCommand::Context { file } => {
                ("swarm.context.get".into(), read_json(&file)?)
            }
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
        Command::ClientCreate {
            client_id,
            role,
            out,
            binding_id,
            generation,
        } => {
            // Save the secret first. A lost registration reply cannot strand its owner.
            // Existing files are re-used, never silently rotated on a retry.
            let new = if out.try_exists()? {
                let c = platform::load_credential(&out)?;
                if c.client_id != client_id {
                    return Err(Error::invalid(
                        "credential file belongs to a different client",
                    ));
                }
                c
            } else {
                let c = Credential {
                    client_id: client_id.clone(),
                    token: format!("{}{}", model::new_id(), model::new_id()),
                };
                platform::write_private_new(&out, &serde_json::to_vec_pretty(&c)?)?;
                c
            };
            let mut value = json!({"client_id":client_id,"role":role,"token_hash":model::digest(new.token.as_bytes())});
            if let Some(id) = binding_id {
                value["binding_id"] = json!(id);
            }
            if let Some(generation) = generation {
                value["binding_generation"] = json!(generation);
            }
            pending_credential = Some(out);
            ("client.register".into(), value)
        }
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
    let is_read = swarm_kernel_host::mcp::application_method_read_only(&method)
        .unwrap_or(method == "doctor.inspect");
    let is_hook_emit = method == "hook.emit";
    if is_hook_emit && cli.request_id.is_some() {
        return Err(Error::invalid("--request-id is not accepted for hook emit"));
    }
    if !is_read && !is_hook_emit {
        if !params.is_object() {
            return Err(Error::invalid("params file must contain an object"));
        }
        if method == "swarm.launch" {
            // Launch requests are digest-bound and carry a caller-owned request
            // ID; never manufacture one or replace the value inside the file.
            let request_id = model::text(&params, "client_request_id")?;
            model::text(&params, "plan_digest")?;
            if cli
                .request_id
                .as_deref()
                .is_some_and(|requested| requested != request_id)
            {
                return Err(Error::invalid(
                    "--request-id must match the client_request_id in the swarm.launch params file",
                ));
            }
        } else if let Some(id) = cli.request_id {
            params["client_request_id"] = json!(id);
        } else if params.get("client_request_id").is_none() {
            params["client_request_id"] = json!(model::new_id());
        }
        eprintln!(
            "client_request_id={}",
            model::text(&params, "client_request_id")?
        );
    }
    let result = if is_hook_emit {
        hook_emit_with_retry(&config.storage.data_dir, &credential, &params, &config.ipc).await?
    } else {
        ipc::call(
            &config.storage.data_dir,
            &credential,
            &method,
            params,
            &config.ipc,
        )
        .await?
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    if let Some(path) = pending_credential {
        eprintln!("credential saved: {}", path.display());
    }
    Ok(())
}

// A post-commit fact is safe to resend only because Store owns the stable
// (source_id, commit_oid) identity and verifies an identical duplicate against
// the retained observation. Keep this recovery specific to hook.emit; ordinary
// mutations continue to use ipc::call's no-retry behavior.
const HOOK_EMIT_ATTEMPTS: usize = 3;
const HOOK_EMIT_RETRY_DELAY_MS: [Option<u64>; HOOK_EMIT_ATTEMPTS] = [Some(100), Some(400), None];

async fn hook_emit_with_retry(
    data_dir: &Path,
    credential: &Credential,
    params: &Value,
    ipc_config: &Ipc,
) -> Result<Value> {
    model::fields(params, &["source_id", "commit_oid", "client_request_id"])?;
    let source_id = model::text(params, "source_id")?.to_owned();
    let commit_oid = model::text(params, "commit_oid")?.to_owned();
    let exact_params = json!({"source_id":source_id,"commit_oid":commit_oid});

    for retry_delay_ms in HOOK_EMIT_RETRY_DELAY_MS {
        match ipc::call(
            data_dir,
            credential,
            "hook.emit",
            exact_params.clone(),
            ipc_config,
        )
        .await
        {
            Ok(reply) if hook_emit_ack_matches(&reply, &source_id, &commit_oid) => {
                return Ok(reply);
            }
            Ok(_) => {
                return Err(Error::new(
                    "HOOK_EMIT_ACK_INVALID",
                    "hook acknowledgment did not identify the requested source and commit",
                ));
            }
            Err(error) if error.code == "HOOK_GIT_CLEANUP_PENDING" => {
                return Err(Error::new(
                    "HOOK_GIT_CLEANUP_PENDING",
                    "hook emit remains incomplete; Store retained the exact Git helper for cleanup and did not record this callback",
                )
                .with_secondary_error(error));
            }
            Err(error) if matches!(error.code.as_str(), "HOST_UNAVAILABLE" | "OUTCOME_UNKNOWN") => {
                let Some(delay_ms) = retry_delay_ms else {
                    return Err(Error::new(
                        error.code,
                        "post-commit fact remains unconfirmed after bounded same-identity retry; a later replay of this source and commit is deduplicated",
                    ));
                };
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Err(error) => return Err(error),
        }
    }

    Err(Error::new(
        "HOOK_EMIT_RETRY_EXHAUSTED",
        "post-commit fact did not receive a durable acknowledgment",
    ))
}

fn hook_emit_ack_matches(reply: &Value, source_id: &str, commit_oid: &str) -> bool {
    let (Some(recorded), Some(duplicate)) = (
        reply.get("recorded").and_then(Value::as_bool),
        reply.get("duplicate").and_then(Value::as_bool),
    ) else {
        return false;
    };
    reply["event"] == "git.post_commit"
        && reply["source_id"].as_str() == Some(source_id)
        && reply["commit_oid"]
            .as_str()
            .is_some_and(|oid| oid.eq_ignore_ascii_case(commit_oid))
        && reply["readback_verified"] == true
        && reply["observation_id"].as_i64().is_some_and(|id| id > 0)
        && recorded != duplicate
}
async fn hook_install(
    config: &Config,
    operator: &Credential,
    request_id: &Option<String>,
    command: &HookInstallCommand,
) -> Result<()> {
    use swarm_kernel_host::hooks::{
        git::{HookLocalOperation, HookLocalResult},
        setup::{hook_local_operation, revoke_hook_source},
    };

    match command {
        HookInstallCommand::Preview { project_id } => {
            if request_id.is_some() {
                return Err(Error::invalid(
                    "--request-id is not used for local hook preview",
                ));
            }
            let preview = match hook_local_operation(
                config,
                operator,
                project_id,
                HookLocalOperation::Preview,
            )
            .await?
            {
                HookLocalResult::Preview { preview } => preview,
                _ => {
                    return Err(Error::new(
                        "HOOK_LOCAL_RESPONSE_INVALID",
                        "Store did not return a hook preview",
                    ));
                }
            };
            println!("{}", serde_json::to_string_pretty(&preview)?);
        }
        HookInstallCommand::Readback {
            project_id,
            source_id,
        } => {
            if request_id.is_some() {
                return Err(Error::invalid(
                    "--request-id is not used for local hook readback",
                ));
            }
            let readback = match hook_local_operation(
                config,
                operator,
                project_id,
                HookLocalOperation::Readback {
                    source_id: source_id.clone(),
                },
            )
            .await?
            {
                HookLocalResult::Readback { readback } => readback,
                _ => {
                    return Err(Error::new(
                        "HOOK_LOCAL_RESPONSE_INVALID",
                        "Store did not return hook readback",
                    ));
                }
            };
            println!("{}", serde_json::to_string_pretty(&readback)?);
        }
        HookInstallCommand::Revoke {
            project_id,
            source_id,
            revision,
        } => {
            let source = revoke_hook_source(
                config,
                operator,
                source_id,
                *revision,
                request_id.as_deref(),
            )
            .await?;
            let installation = match hook_local_operation(
                config,
                operator,
                project_id,
                HookLocalOperation::Revoke {
                    source_id: source_id.clone(),
                },
            )
            .await
            {
                Ok(HookLocalResult::Revoked { installation }) => installation,
                Ok(_) => {
                    return Err(Error::new(
                        "HOOK_LOCAL_RESPONSE_INVALID",
                        "Store did not return local hook restoration readback",
                    ));
                }
                Err(error) if error.code == "HOOK_GIT_CLEANUP_PENDING" => {
                    return Err(Error::new(
                        "HOOK_GIT_CLEANUP_PENDING",
                        format!(
                            "source {source_id} is revoked; exact Git cleanup remains in Store custody and local restoration is incomplete"
                        ),
                    ));
                }
                Err(error) => {
                    return Err(Error::new(
                        "HOOK_SOURCE_REVOKED_INSTALL_RESTORE_FAILED",
                        format!(
                            "source {source_id} is revoked; local hook restoration failed with {}",
                            error.code
                        ),
                    ));
                }
            };
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &json!({"source":source,"installation":installation})
                )?
            );
        }
    }
    Ok(())
}

async fn run_gateway_binary(config_path: Option<&Path>, data_dir: Option<&Path>) -> Result<()> {
    let mut executable = std::env::current_exe().map_err(|error| {
        Error::new(
            "GATEWAY_BINARY_PATH_FAILED",
            format!("could not locate the running swarm executable: {error}"),
        )
    })?;
    executable.set_file_name(if cfg!(windows) {
        "swarm-gateway.exe"
    } else {
        "swarm-gateway"
    });
    let sibling_is_file = executable.is_file();
    let mut child = tokio::process::Command::new(&executable);
    if let Some(path) = config_path {
        child.arg("--config").arg(path);
    }
    if let Some(path) = data_dir {
        child.arg("--data-dir").arg(path);
    }
    let status = child.status().await.map_err(|error| {
        let (code, recovery) = if error.kind() == std::io::ErrorKind::NotFound && !sibling_is_file {
            (
                "GATEWAY_BINARY_MISSING",
                "Gateway is optional; install the matching swarm-gateway sibling beside this swarm executable and retry.",
            )
        } else {
            (
                "GATEWAY_START_FAILED",
                "Check the sibling executable and its dependent runtime files and permissions, then retry.",
            )
        };
        Error::new(
            code,
            format!(
                "could not start the sibling executable at '{}': {error}. {recovery}",
                executable.display()
            ),
        )
    })?;
    if status.success() {
        Ok(())
    } else {
        Err(Error::new(
            "GATEWAY_FAILED",
            format!(
                "the sibling swarm-gateway process at '{}' exited unsuccessfully ({status})",
                executable.display()
            ),
        ))
    }
}

#[cfg(test)]
mod cli_parse_tests {
    use super::Cli;
    use clap::Parser;

    /// The stock host invocation must keep parsing.
    ///
    /// This is the regression guard for the default CLI surface: the plain
    /// host command must remain accepted and must not acquire any new required
    /// behavior.
    #[test]
    fn stock_host_invocation_still_parses() {
        let parsed = Cli::try_parse_from(["swarm-kernel-host", "host", "--stop-on-stdin-eof"])
            .expect("the stock host invocation is accepted");
        assert!(
            matches!(
                &parsed.command,
                super::Command::Host {
                    stop_on_stdin_eof: true,
                    ..
                }
            ),
            "the parsed host command carries the stop-on-stdin-eof request"
        );
    }

    /// The flag must not be part of the default surface.
    ///
    /// A release-default build has no field for the token, so the parser rejects it
    /// as an unknown argument rather than silently accepting and discarding it. The
    /// default build therefore has no activation path.
    #[cfg(not(feature = "qualification-faults"))]
    #[test]
    fn default_build_rejects_the_qualification_flag() {
        let parsed = Cli::try_parse_from([
            "swarm-kernel-host",
            "host",
            "--stop-on-stdin-eof",
            "--qualification-drop-first-admit-reply",
        ]);
        let error = parsed
            .err()
            .expect("the token is unknown in a default build");
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    /// The flag must parse in a qualification build.
    #[cfg(feature = "qualification-faults")]
    #[test]
    fn qualification_build_parses_the_hidden_flag() {
        let parsed = Cli::try_parse_from([
            "swarm-kernel-host",
            "host",
            "--stop-on-stdin-eof",
            "--qualification-drop-first-admit-reply",
        ])
        .expect("a qualification build accepts the hidden flag");
        assert!(
            matches!(
                &parsed.command,
                super::Command::Host {
                    stop_on_stdin_eof: true,
                    qualification_drop_first_admit_reply: true,
                }
            ),
            "the parsed host command records the qualification request"
        );
    }

    /// The flag must default to false when it is not supplied.
    #[cfg(feature = "qualification-faults")]
    #[test]
    fn qualification_flag_defaults_to_false() {
        let parsed = Cli::try_parse_from(["swarm-kernel-host", "host"])
            .expect("the host command is accepted");
        assert!(
            matches!(
                &parsed.command,
                super::Command::Host {
                    stop_on_stdin_eof: false,
                    qualification_drop_first_admit_reply: false,
                }
            ),
            "an absent flag must read as false"
        );
    }
}
