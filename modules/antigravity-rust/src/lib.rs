//! Standalone Antigravity warm-stream adapter.
//!
//! All durable admission and Operation state remain in the controller Store.
//! This crate keeps only bounded same-process stream state and retry payloads.

pub mod config;
pub mod contract;
pub mod controller;
pub mod ipc;
pub mod launch;
pub mod module_receipt;
pub mod process;
pub mod stderr;
pub mod stream;
pub mod wire;
