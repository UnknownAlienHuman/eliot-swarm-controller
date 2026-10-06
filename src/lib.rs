//! Compatibility facade for the relocated kernel host package.
//!
//! The durable Store, authenticated IPC, and host lifecycle live in
//! `swarm-kernel-host`; this facade retains the historical root crate path for
//! local tests and downstream tooling without retaining a second Store owner.
pub use swarm_kernel_host::*;
