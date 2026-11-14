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
mod ui;

#[cfg_attr(not(windows), allow(dead_code))]
mod chat;
#[cfg_attr(not(windows), allow(dead_code))]
mod client;
#[cfg_attr(not(windows), allow(dead_code))]
mod state;
#[cfg_attr(not(windows), allow(dead_code))]
mod theme;

#[cfg(windows)]
fn main() {
    if let Err(error) = ui::run() {
        eprintln!("swarm-desktop: {error}");
        std::process::exit(1);
    }
}

#[cfg(not(windows))]
fn main() {
    eprintln!("swarm-desktop requires Windows (WinUI 3); nothing to run on this platform");
    std::process::exit(2);
}
