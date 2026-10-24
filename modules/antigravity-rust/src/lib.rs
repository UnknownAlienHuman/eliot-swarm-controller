//! Standalone Antigravity warm-stream adapter.
//!
//! All durable admission and Operation state remain in the controller Store.
//! The adapter retains only exact already-formed Store deliveries and selected
//! response bodies needed to survive IPC/process failure without replaying input.

pub mod config;
pub mod contract;
pub mod controller;
pub mod ipc;
pub mod launch;
pub mod module_receipt;
pub mod process;
pub mod result_page;
pub mod stderr;
pub mod store_delivery;
pub mod stream;
pub mod wire;
