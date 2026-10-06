//! Independent Command Code module adapter.
//!
//! This package owns native invocation and its bounded receipt. Swarm keeps
//! ownership of admission, Task policy, binding identity, and final acceptance.

#![recursion_limit = "256"]

mod adapter;
mod journal;
mod module_host;
mod native;
mod result_page;

pub use adapter::run;

pub const RUNTIME: &str = "command";
pub const ARTIFACT_ID: &str = "eliot-command.rust-headless.1";
pub const ARTIFACT_VERSION: &str = "3";
pub const CONTRACT_REVISION: &str = "command-headless-module-v3";
pub const EXECUTION_SHAPE: &str = "sessionless_batch";

/// Canonical LF digest of the existing pinned Command mod source. The `.3`
/// and `.4` JavaScript module artifacts remain separate and unchanged.
pub const MOD_SHA256: &str = "513eaa7d6034cc22b5abf14d080888cdd3e8782133e39f859b7703db123e1f80";
