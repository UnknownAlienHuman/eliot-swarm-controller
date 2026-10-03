use clap::{Parser, Subcommand};
use eliot_swarm_controller::{
    config::Config,
    error::{Error, Result},
    host, ipc,
    model::{self, Credential},
    platform,
};
use serde_json::{Value, json};
use std::path::PathBuf;

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
    /// Rotates the GM epoch; the previous GM keeps no GM-only rights afterwards.
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
    if let Command::ModuleRun {
        state_dir,
        command,
        args,
    } = &cli.command
    {
        return eliot_swarm_controller::runtime::owner::run(state_dir, command, args);
    }
    if let Command::CheckWorker { file } = &cli.command {
        return eliot_swarm_controller::checks::worker::run(file);
    }
    if let Command::OwnedOpencodeService { file } = &cli.command {
        return eliot_swarm_controller::runtime::opencode_v2::run_owned_service_helper(file);
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
    if let Command::Host { stop_on_stdin_eof } = &cli.command {
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
        let credential_path = config.gateway.credential_file.as_ref().ok_or_else(|| {
            Error::new("CONFIG_ERROR", "gateway credential_file is not configured")
        })?;
        let bearer_path = config.gateway.local_bearer_file.as_ref().ok_or_else(|| {
            Error::new(
                "CONFIG_ERROR",
                "gateway local_bearer_file is not configured",
            )
        })?;
        let credential = platform::load_credential(credential_path)?;
        config
            .mcp
            .selected_tool_profile(Some(&config.gateway.profile), &credential.client_id)?;
        let bearer_token = load_local_bearer(bearer_path)?;
        return eliot_swarm_controller::gateway::run(config, credential, bearer_token).await;
    }
    let credential = platform::load_credential(
        &cli.credential
            .unwrap_or_else(|| config.storage.data_dir.join("operator.json")),
    )?;
    if let Command::Mcp { profile } = &cli.command {
        return eliot_swarm_controller::mcp::run_profiled(config, credential, profile.as_deref())
            .await;
    }
    if let Command::Artifact {
        command: ArtifactCommand::Export { artifact_id, out },
    } = &cli.command
    {
        let result = eliot_swarm_controller::export::artifact(
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
        | Command::ModuleRun { .. } => {
            unreachable!("executor returned above")
        }
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
    let is_read = matches!(
        method.as_str(),
        "check.get"
            | "check.profiles"
            | "host.status"
            | "doctor.inspect"
            | "artifact.get"
            | "artifact.read"
            | "artifact.parts"
            | "task.submission"
            | "task.acceptance"
            | "task.get"
            | "task.list"
            | "attempt.get"
            | "operation.get"
            | "operation.list"
            | "agent.family"
            | "agent.state"
            | "agent.list"
            | "route.list"
            | "report.delta"
            | "message.read"
            | "client.list"
            | "swarm.context.get"
            | "coordination.participant.get"
            | "coordination.participant.list"
            | "coordination.peer.find"
            | "coordination.work_card.get"
            | "coordination.work_card.list"
            | "coordination.contract_card.get"
            | "coordination.contract_card.list"
            | "coordination.inbox"
            | "swarm.overlap.check"
            | "review.get"
            | "review.list"
            | "swarm.review.context"
            | "automation.config.get"
            | "automation.config.preview"
            | "automation.config.explain"
            | "swarm.dashboard"
            | "swarm.launch.preview"
            | "swarm.queue.get"
            | "swarm.agent.inspect"
            | "swarm.exceptions.get"
            | "coordination.watch.list"
    );
    if !is_read {
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
    let result = ipc::call(
        &config.storage.data_dir,
        &credential,
        &method,
        params,
        &config.ipc,
    )
    .await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    if let Some(path) = pending_credential {
        eprintln!("credential saved: {}", path.display());
    }
    Ok(())
}

fn load_local_bearer(path: &PathBuf) -> Result<String> {
    use std::io::Read;

    let mut contents = String::new();
    std::fs::File::open(path)?
        .take(515)
        .read_to_string(&mut contents)?;
    if contents.len() > 514 {
        return Err(Error::new(
            "AUTH_ERROR",
            "gateway bearer file exceeds 512 bytes plus a final line ending",
        ));
    }
    let token = contents.trim_end_matches(['\r', '\n']);
    if !(32..=512).contains(&token.len()) || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::new(
            "AUTH_ERROR",
            "gateway bearer file must contain one printable token of 32 to 512 bytes",
        ));
    }
    Ok(token.to_owned())
}
