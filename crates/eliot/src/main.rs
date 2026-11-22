use clap::{CommandFactory, Parser};
use std::{
    env,
    path::{Path, PathBuf},
    process::ExitCode,
};

mod cli;
mod mcp;

use cli::args::{Cli, CliCommand, McpArgs, PluginArgs, PluginCommand};
use cli::support::Failure;

fn main() -> ExitCode {
    let cli = Cli::parse();
    if cli.operator_release_info {
        println!(
            "{}",
            serde_json::json!({
                "version": format!("swarm {}", env!("ELIOT_BUILD_VERSION")),
                "git_sha": option_env!("ELIOT_BUILD_GIT_SHA").filter(|sha| !sha.is_empty()),
                "store_schema": eliot_daemon::operator::STORE_SCHEMA_VERSION,
                "backup_update": true,
            })
        );
        return ExitCode::SUCCESS;
    }
    let is_plugin = matches!(&cli.command, Some(CliCommand::Plugin(_)));
    // One hook for both binaries: the daemon and every plugin share this
    // executable, so the code is mode-aware. A WMI-started daemon has no
    // console, so its marker file is the durable path. A plugin reports only
    // on its owned stderr pipe, bound by the nonce its daemon put on argv.
    if let Some(CliCommand::Plugin(args)) = &cli.command
        && let Some(nonce) = args.crash_nonce.as_deref()
    {
        eliot_daemon::crash::set_plugin_crash_nonce(nonce);
    }
    if !matches!(&cli.command, Some(CliCommand::Stand(_))) {
        eliot_daemon::crash::install_panic_hook(is_plugin);
    }
    // Debug-only panic trigger for crash-capture tests. Compiled out of
    // release builds: release binaries cannot panic on demand.
    #[cfg(debug_assertions)]
    if is_plugin && std::env::var("ELIOT_DEBUG_PLUGIN_PANIC").is_ok_and(|value| value == "1") {
        panic!("debug plugin panic trigger");
    }
    if is_plugin {
        match eliot_plugin::native_lifecycle::run_plugin_entry(run_inner(cli)) {
            Ok(exit_code) => exit_code,
            Err(error) => report_failure(Failure::new(1, error.to_string())),
        }
    } else if matches!(
        &cli.command,
        Some(CliCommand::Mcp(McpArgs { http: false, .. }))
    ) {
        // Each native client starts its own thin stdio bridge. A host-sized
        // scheduler in each bridge duplicates idle threads across the fleet.
        match tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(run_inner(cli)),
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
    let is_plugin = matches!(&cli.command, Some(CliCommand::Plugin(_)));
    let data_dir = if matches!(&cli.command, Some(CliCommand::Stand(_))) {
        if cli.data_dir.is_some() || cli.id.is_some() {
            return report_failure(Failure::new(2, "stand uses its own path and mutation IDs"));
        }
        PathBuf::new()
    } else {
        let path = match resolve_data_dir(cli.data_dir.as_deref()) {
            Ok(path) => path,
            Err(failure) => return report_failure(failure),
        };
        // Read lazily by the panic hook for the daemon this process serves.
        // Plugins never write data-directory crash files.
        if !is_plugin {
            eliot_daemon::crash::set_crash_dir(&eliot_daemon::crash::crash_dir(&path));
        }
        path
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

    let mut doctor_acl_report = if matches!(&command, CliCommand::Doctor) {
        Some(eliot_daemon::data_dir_acl_report(&data_dir))
    } else {
        None
    };

    let result = match command {
        CliCommand::Plugin(PluginArgs { command, .. }) => run_plugin_command(command).await,
        command => {
            if !matches!(
                command,
                CliCommand::Daemon(_)
                    | CliCommand::Stand(_)
                    | CliCommand::Status
                    | CliCommand::Autonomy(_)
                    | CliCommand::Mcp(_)
                    | CliCommand::Store(_)
            ) {
                let executable = match env::current_exe() {
                    Ok(path) => path,
                    Err(error) => return report_failure(Failure::new(1, error.to_string())),
                };
                let root_acl_repaired =
                    match eliot_daemon::startup::ensure_running(&data_dir, &executable).await {
                        Ok(repaired) => repaired,
                        Err(error) => {
                            if matches!(&command, CliCommand::Doctor)
                                && eliot_daemon::startup::repairable_data_dir_acl(&error)
                                && let Some(report) = doctor_acl_report.as_ref()
                            {
                                cli::report_doctor_acl(report, json_output);
                            }
                            return report_failure(Failure::from_core(error, false));
                        }
                    };
                if root_acl_repaired {
                    eprintln!("data directory ACL repaired during startup");
                }
                if let Some(report) = doctor_acl_report.as_mut()
                    && (root_acl_repaired || report.is_err())
                {
                    *report = eliot_daemon::data_dir_acl_report(&data_dir);
                }
            }
            cli::dispatch(command, data_dir, json_output, id, doctor_acl_report).await
        }
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
    eliot_daemon::resolve_data_dir(argument).map_err(|error| Failure::new(1, *error.message))
}
