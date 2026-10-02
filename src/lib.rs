//! Local task authority and IPC. Native execution is a separate integration boundary.
pub mod acceptance;
pub mod artifacts;
pub mod checks;
pub mod config;
pub mod doctor;
pub mod error;
pub mod export;
pub mod host;
pub mod ipc;
pub mod model;
pub mod platform;
mod redaction;
pub mod runtime;
pub mod store;
pub mod submission;
