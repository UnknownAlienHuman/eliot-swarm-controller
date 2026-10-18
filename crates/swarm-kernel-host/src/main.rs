use clap::{Parser, Subcommand};
use serde_json::json;
use std::path::{Path, PathBuf};
use swarm_kernel_host::{
    config::Config,
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
    /// Create a manager, observer, or module credential and register it with Store.
    ClientCreate {
        client_id: String,
        #[arg(long, default_value = "manager", value_parser = ["manager", "observer", "module"])]
        role: String,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, requires = "generation")]
        binding_id: Option<String>,
        #[arg(long, requires = "binding_id")]
        generation: Option<i64>,
    },
    Hook {
        #[command(subcommand)]
        command: HookCommand,
    },
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommand,
    },
    Observer {
        #[command(subcommand)]
        command: ObserverCommand,
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
enum ArtifactCommand {
    /// Export all bytes, check the complete SHA-256, and publish without overwriting.
    Export {
        artifact_id: String,
        #[arg(long)]
        out: PathBuf,
    },
}
#[derive(Subcommand)]
enum HookCommand {
    /// Create and install the supported repository-local post-commit observer.
    Setup { project_id: String },
    Install {
        #[command(subcommand)]
        command: HookInstallCommand,
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

    // Only the daemon and Gateway are long-lived. Retained local commands use
    // one current-thread runtime for their bounded Store exchanges.
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

    if let Command::Observer { command } = &cli.command {
        match command {
            ObserverCommand::Follow {
                after_segment,
                after_offset,
                once,
            } => {
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
            ObserverCommand::Metrics {
                host_identity,
                module_owner,
                module_worker,
                child_role,
                interval_ms,
            } => {
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
            ObserverCommand::Snapshot { after, limit } => {
                let credential = operator_credential(&config, cli.credential.as_deref())?;
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
        }
    }

    match cli.command {
        Command::ClientCreate {
            client_id,
            role,
            out,
            binding_id,
            generation,
        } => {
            let operator = operator_credential(&config, cli.credential.as_deref())?;
            // Persist the secret before registration, and reuse an existing
            // matching credential on retry rather than silently rotating it.
            let credential = if out.try_exists()? {
                let existing = platform::load_credential(&out)?;
                if existing.client_id != client_id {
                    return Err(Error::invalid(
                        "credential file belongs to a different client",
                    ));
                }
                existing
            } else {
                let credential = Credential {
                    client_id: client_id.clone(),
                    token: format!("{}{}", model::new_id(), model::new_id()),
                };
                platform::write_private_new(&out, &serde_json::to_vec_pretty(&credential)?)?;
                credential
            };
            let mut params = json!({
                "client_id": client_id,
                "role": role,
                "token_hash": model::digest(credential.token.as_bytes())
            });
            if let Some(binding_id) = binding_id {
                params["binding_id"] = json!(binding_id);
            }
            if let Some(generation) = generation {
                params["binding_generation"] = json!(generation);
            }
            let request_id = cli.request_id.unwrap_or_else(model::new_id);
            params["client_request_id"] = json!(request_id);
            eprintln!("client_request_id={request_id}");
            let result = ipc::call(
                &config.storage.data_dir,
                &operator,
                "client.register",
                params,
                &config.ipc,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            eprintln!("credential saved: {}", out.display());
            Ok(())
        }
        Command::Hook {
            command: HookCommand::Setup { project_id },
        } => {
            let credential = operator_credential(&config, cli.credential.as_deref())?;
            swarm_kernel_host::hooks::setup::hook_setup(
                &config,
                &credential,
                &cli.request_id,
                &project_id,
            )
            .await
        }
        Command::Hook {
            command: HookCommand::Install { command },
        } => {
            let credential = operator_credential(&config, cli.credential.as_deref())?;
            hook_install(&config, &credential, &cli.request_id, &command).await
        }
        Command::Artifact {
            command: ArtifactCommand::Export { artifact_id, out },
        } => {
            let credential = operator_credential(&config, cli.credential.as_deref())?;
            let result = swarm_kernel_host::export::artifact(
                &config.storage.data_dir,
                &credential,
                &config.ipc,
                &artifact_id,
                &out,
            )
            .await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
        Command::Host { .. }
        | Command::Gateway
        | Command::ModuleRun { .. }
        | Command::ModuleOwnerWorker { .. }
        | Command::CheckWorker { .. }
        | Command::OwnedOpencodeService { .. }
        | Command::ScriptWorker { .. }
        | Command::ZedBatchWorker { .. }
        | Command::Observer { .. } => unreachable!("command executor returned above"),
    }
}

fn operator_credential(config: &Config, path: Option<&Path>) -> Result<Credential> {
    let default_path = config.storage.data_dir.join("operator.json");
    platform::load_credential(path.unwrap_or(&default_path))
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
