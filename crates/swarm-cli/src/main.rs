use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use std::{path::PathBuf, process::ExitCode};
use swarm_cli::{call, prepare_call, validate_call_method};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};

#[derive(Debug, Parser)]
#[command(
    name = "swarm-cli",
    version,
    about = "Thin local client for an existing Eliot Swarm host"
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

#[derive(Debug, Subcommand)]
enum Command {
    /// Read the host status through the existing application method.
    Status,
    /// Call one application method with JSON params read from a file.
    Call {
        method: String,
        #[arg(long)]
        file: Option<PathBuf>,
    },
    /// Manage durable Manager tasks through the existing Store methods.
    Task {
        #[command(subcommand)]
        command: TaskCommand,
    },
    /// Read agent bindings through the existing Store methods.
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
    /// Read one attempt through the existing Store method.
    Attempt {
        #[command(subcommand)]
        command: AttemptCommand,
    },
    /// Read a retained family observation, not a live query or complete inventory.
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
    /// Serve the selected application profile over MCP stdio.
    Mcp {
        /// Named profile from the local [mcp.profiles] configuration table.
        #[arg(long, value_name = "NAME")]
        profile: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
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
    Revise {
        task_id: String,
        #[arg(long)]
        revision: i64,
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum AgentCommand {
    /// Page known bindings and their observed state.
    List {
        #[arg(long, default_value_t = 0)]
        after: i64,
        #[arg(long, default_value_t = 50)]
        limit: i64,
    },
    /// Read one binding generation's observed state (`agent.state` on the wire).
    Get {
        binding_id: String,
        #[arg(long)]
        generation: i64,
    },
}

#[derive(Debug, Subcommand)]
enum AttemptCommand {
    /// Read one attempt and its disposition.
    Get { attempt_id: String },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", json!({"error": error}));
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let config = swarm_mcp::Config::load(cli.config.as_deref(), cli.data_dir.as_deref())?;
    let credential_path = cli
        .credential
        .unwrap_or_else(|| config.storage.data_dir.join("operator.json"));
    let credential = load_credential(&credential_path)?;

    match cli.command {
        Command::Status => {
            let result = call(
                &config.storage.data_dir,
                &credential,
                &config.ipc,
                "host.status",
                json!({}),
            )
            .await?;
            print_json(&result)?;
            Ok(())
        }
        Command::Call { method, file } => {
            validate_call_method(&method)?;
            let params = match file {
                Some(path) => read_json(&path)?,
                None => json!({}),
            };
            let (params, prepared_id) = prepare_call(&method, params, cli.request_id.as_deref())?;
            if let Some(request_id) = prepared_id {
                eprintln!("client_request_id={request_id}");
            }
            let result = call(
                &config.storage.data_dir,
                &credential,
                &config.ipc,
                &method,
                params,
            )
            .await?;
            print_json(&result)?;
            Ok(())
        }
        Command::Task { command } => {
            let (method, params) = match command {
                TaskCommand::Create {
                    project,
                    file,
                    origin_key,
                } => {
                    let mut params = json!({"project_id":project,"spec":read_json(&file)?});
                    if let Some(origin_key) = origin_key {
                        params["origin_key"] = json!(origin_key);
                    }
                    ("task.create", params)
                }
                TaskCommand::Get { task_id } => ("task.get", json!({"task_id":task_id})),
                TaskCommand::List { after, limit } => {
                    ("task.list", json!({"after":after,"limit":limit}))
                }
                TaskCommand::Revise {
                    task_id,
                    revision,
                    file,
                } => (
                    "task.revise",
                    json!({"task_id":task_id,"expected_revision":revision,"spec":read_json(&file)?}),
                ),
            };
            let (params, prepared_id) = prepare_call(method, params, cli.request_id.as_deref())?;
            if let Some(request_id) = prepared_id {
                eprintln!("client_request_id={request_id}");
            }
            let result = call(
                &config.storage.data_dir,
                &credential,
                &config.ipc,
                method,
                params,
            )
            .await?;
            print_json(&result)?;
            Ok(())
        }
        Command::Agent { command } => {
            let (method, params) = match command {
                AgentCommand::List { after, limit } => {
                    ("agent.list", json!({"after":after,"limit":limit}))
                }
                AgentCommand::Get {
                    binding_id,
                    generation,
                } => (
                    "agent.state",
                    json!({"binding_id":binding_id,"generation":generation}),
                ),
            };
            run_read_call(
                &config,
                &credential,
                method,
                params,
                cli.request_id.as_deref(),
            )
            .await
        }
        Command::Attempt { command } => match command {
            AttemptCommand::Get { attempt_id } => {
                run_read_call(
                    &config,
                    &credential,
                    "attempt.get",
                    json!({"attempt_id":attempt_id}),
                    cli.request_id.as_deref(),
                )
                .await
            }
        },
        Command::Family {
            binding_id,
            generation,
            observation_id,
            after,
            limit,
        } => {
            let mut params = json!({
                "binding_id": binding_id,
                "generation": generation,
                "after": after,
                "limit": limit,
            });
            if let Some(observation_id) = observation_id {
                params["observation_id"] = json!(observation_id);
            }
            run_read_call(
                &config,
                &credential,
                "agent.family",
                params,
                cli.request_id.as_deref(),
            )
            .await
        }
        Command::Mcp { profile } => {
            swarm_mcp::run_profiled(config, credential, profile.as_deref()).await
        }
    }
}

async fn run_read_call(
    config: &swarm_mcp::Config,
    credential: &Credential,
    method: &str,
    params: Value,
    request_id: Option<&str>,
) -> Result<()> {
    if swarm_mcp::application_method_read_only(method) != Some(true) {
        return Err(Error::invalid(
            "typed read shortcut does not map to a cataloged read-only method",
        ));
    }
    let (params, prepared_id) = prepare_call(method, params, request_id)?;
    if let Some(request_id) = prepared_id {
        eprintln!("client_request_id={request_id}");
    }
    let result = call(
        &config.storage.data_dir,
        credential,
        &config.ipc,
        method,
        params,
    )
    .await?;
    print_json(&result)
}

fn read_json(path: &PathBuf) -> Result<Value> {
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn print_json(value: &Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

fn load_credential(path: &std::path::Path) -> Result<Credential> {
    let credential: Credential = serde_json::from_slice(&std::fs::read(path)?)?;
    if credential.client_id.is_empty() || credential.token.len() < 32 {
        return Err(Error::new("AUTH_ERROR", "invalid credential file"));
    }
    Ok(credential)
}
