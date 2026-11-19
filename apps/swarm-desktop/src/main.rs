//! Optional WinUI 3 desktop companion for the Eliot daemon.
//!
//! The daemon stays the sole controller: this binary performs no automation,
//! owns no database, embeds no WebView and spawns no second executor. Every
//! read and mutation goes through the existing typed RPC client
//! (`eliot_daemon::client_call_typed` and the `eliot_core::api` DTOs).
//! WinUI 3 controls come from the `windows-reactor` dependency, used only on
//! Windows; other platforms build a stub that explains the requirement so the
//! default workspace build and Ubuntu CI stay green.

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
mod m3;
#[cfg(windows)]
mod monitor_view;
#[cfg(windows)]
mod reparent;
#[cfg(windows)]
mod scroll_end;
#[cfg(windows)]
mod shortcuts;
#[cfg(windows)]
mod ui;
#[cfg(windows)]
mod views;

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
mod worktrees;

#[cfg(windows)]
#[derive(clap::Parser)]
#[command(name = "swarm-desktop", version, about = "Eliot desktop companion")]
pub(crate) struct DesktopArgs {
    /// Open and register this Git checkout; defaults to the working directory.
    #[arg(long, value_name = "PATH")]
    workspace: Option<std::path::PathBuf>,
}

#[cfg(windows)]
fn main() {
    use clap::Parser;
    let args = DesktopArgs::parse();
    let workspace = args
        .workspace
        .map_or_else(std::env::current_dir, Ok)
        .and_then(std::fs::canonicalize)
        .and_then(|path| {
            if path.is_dir() {
                Ok(path)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "workspace must be a directory",
                ))
            }
        });
    let result = workspace
        .map_err(|error| error.to_string())
        .and_then(ui::run);
    if let Err(error) = result {
        eprintln!("swarm-desktop: {error}");
        std::process::exit(1);
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("swarm-desktop requires Windows (WinUI 3); nothing to run on this platform");
    std::process::exit(2);
}
