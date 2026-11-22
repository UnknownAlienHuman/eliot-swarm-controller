//! Optional WinUI 3 desktop companion for the Eliot daemon.
//!
//! The daemon stays the sole controller: this binary performs no automation,
//! owns no database, embeds no WebView and spawns no second executor. Live
//! operations use the daemon's typed RPC client. Settings repair while the
//! daemon is offline uses the same typed settings operations in its library.
//! WinUI 3 controls come from the `windows-reactor` dependency, used only on
//! Windows; other platforms build a stub that explains the requirement so the
//! default workspace build and Ubuntu CI stay green.
//!
//! On Windows every build, debug included, is a GUI-subsystem binary: a
//! console-subsystem start makes Windows open a console window beside the
//! app, which took the foreground even when the desktop was started
//! minimized, and debug and release then behave alike in QA. A start from a
//! terminal still prints to it; without a terminal, start-up errors and
//! panics go to the data directory's `desktop.log` and a fatal start-up
//! error to a message box (`startup`).
#![cfg_attr(windows, windows_subsystem = "windows")]

#[cfg(windows)]
mod accessibility;
#[cfg(windows)]
mod app_resources;
#[cfg(windows)]
mod clipboard;
#[cfg(windows)]
mod config_view;
#[cfg(windows)]
mod configure;
#[cfg(windows)]
mod file_dialog;
#[cfg(windows)]
mod m3;
#[cfg(windows)]
mod monitor_view;
#[cfg(windows)]
mod motion;
#[cfg(windows)]
mod reparent;
#[cfg(windows)]
mod scroll_end;
#[cfg(windows)]
mod shortcuts;
#[cfg(windows)]
mod startup;
#[cfg(windows)]
mod ui;
#[cfg(windows)]
mod views;
#[cfg(windows)]
mod window_place;
#[cfg(windows)]
mod winrt;

#[cfg_attr(not(windows), allow(dead_code))]
mod attachments;
#[cfg_attr(not(windows), allow(dead_code))]
mod chat;
#[cfg_attr(not(windows), allow(dead_code))]
mod client;
#[cfg_attr(not(windows), allow(dead_code))]
mod config_client;
#[cfg_attr(not(windows), allow(dead_code))]
mod feed;
#[cfg_attr(not(windows), allow(dead_code))]
mod handoff;
#[cfg_attr(not(windows), allow(dead_code))]
mod history;
#[cfg_attr(not(windows), allow(dead_code))]
mod live;
#[cfg_attr(not(windows), allow(dead_code))]
mod markdown;
#[cfg_attr(not(windows), allow(dead_code))]
mod monitor;
#[cfg_attr(not(windows), allow(dead_code))]
mod nav;
#[cfg_attr(not(windows), allow(dead_code))]
mod picker;
#[cfg_attr(not(windows), allow(dead_code))]
mod profiles;
#[cfg_attr(not(windows), allow(dead_code))]
mod quota;
#[cfg_attr(not(windows), allow(dead_code))]
mod scripts;
#[cfg_attr(not(windows), allow(dead_code))]
mod settings;
#[cfg_attr(not(windows), allow(dead_code))]
mod settings_model;
#[cfg_attr(not(windows), allow(dead_code))]
mod state;
#[cfg_attr(not(windows), allow(dead_code))]
mod theme;
#[cfg_attr(not(windows), allow(dead_code))]
#[path = "ui/tokens.rs"]
mod tokens;
#[cfg_attr(not(windows), allow(dead_code))]
mod worktrees;

#[cfg(windows)]
#[derive(clap::Parser)]
#[command(name = "swarm-desktop", version, about = "Eliot desktop companion")]
pub(crate) struct DesktopArgs {
    /// Open and register this Git checkout. Without it nothing is
    /// registered: the folder the desktop was started from is never a
    /// project by accident.
    #[arg(long, value_name = "PATH")]
    workspace: Option<std::path::PathBuf>,
}

#[cfg(windows)]
fn main() {
    use clap::Parser;
    startup::attach_parent_console();
    startup::install_panic_hook();
    let args = match DesktopArgs::try_parse() {
        Ok(args) => args,
        // --help and --version print and exit 0.
        Err(error) if !error.use_stderr() => error.exit(),
        Err(error) => startup::fatal(error.to_string().trim_end()),
    };
    let workspace = args
        .workspace
        .map(|workspace| {
            std::fs::canonicalize(workspace).and_then(|path| {
                if path.is_dir() {
                    Ok(path)
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "workspace must be a directory",
                    ))
                }
            })
        })
        .transpose();
    let result = workspace
        .map_err(|error| error.to_string())
        .and_then(ui::run);
    if let Err(error) = result {
        startup::fatal(&error);
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("swarm-desktop requires Windows (WinUI 3); nothing to run on this platform");
    std::process::exit(2);
}
