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
    Host,
    Status,
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
    /// Create a scoped local client credential. Run as the local operator.
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
    // Only the long-lived host needs a worker pool. CLI calls perform one local
    // exchange and must not create a CPU-sized pool for every status request.
    let mut builder = if matches!(&cli.command, Command::Host) {
        tokio::runtime::Builder::new_multi_thread()
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    let runtime = builder.enable_all().build()?;
    runtime.block_on(run(cli))
}
async fn run(cli: Cli) -> Result<()> {
    let config = Config::load(cli.config.as_deref(), cli.data_dir.as_deref())?;
    if matches!(&cli.command, Command::Host) {
        return host::run(config).await;
    }
    let credential = platform::load_credential(
        &cli.credential
            .unwrap_or_else(|| config.storage.data_dir.join("operator.json")),
    )?;
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
        Command::Host => unreachable!("host returned above"),
        Command::Status => ("host.status".to_string(), json!({})),
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
    };
    let is_read = matches!(
        method.as_str(),
        "host.status"
            | "artifact.get"
            | "artifact.read"
            | "artifact.parts"
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
    );
    if !is_read {
        if !params.is_object() {
            return Err(Error::invalid("params file must contain an object"));
        }
        if let Some(id) = cli.request_id {
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
