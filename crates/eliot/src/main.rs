use clap::{CommandFactory, Parser};
use std::{
    env,
    path::{Path, PathBuf},
    process::ExitCode,
};

mod cli;
mod mcp;

use cli::args::{Cli, CliCommand, PluginArgs, PluginCommand};
use cli::support::Failure;

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
        command => cli::dispatch(command, data_dir, json_output, id).await,
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
