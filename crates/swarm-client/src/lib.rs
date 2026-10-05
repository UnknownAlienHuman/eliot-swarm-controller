//! Authenticated local IPC client. It performs no task or role policy.

mod client;
mod config;
mod endpoint;
mod module_link;

pub use client::Client;
pub use config::IpcConfig;
pub use endpoint::ipc_endpoint;
pub use module_link::ModuleLink;

use serde_json::Value;
use std::path::Path;
use swarm_contracts::{Credential, error::Result};

/// Perform one authenticated exchange. A failed application transport is not retried.
pub async fn call(
    root: &Path,
    credential: &Credential,
    method: &str,
    params: Value,
    config: &IpcConfig,
) -> Result<Value> {
    Client::connect(root, credential, config)
        .await?
        .request(method, params)
        .await
}
