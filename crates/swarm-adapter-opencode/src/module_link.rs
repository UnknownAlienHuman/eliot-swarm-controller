//! Fixed common module RPC bridge over swarm-client::ModuleLink.
//!
//! Each call is preceded by a typed hello on the same authenticated IPC link,
//! because the host scopes a module binding to that link's principal ID. A
//! failed application exchange is never transparently replayed; only the
//! caller's saved outcome/observation acknowledgement policy may retry it.

use serde_json::Value;
use std::path::Path;
use swarm_client::{IpcConfig, ModuleLink};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_contract::ModuleContractClaim,
};

pub async fn connect(root: &Path, credential: &Credential, ipc: &IpcConfig) -> Result<ModuleLink> {
    ModuleLink::connect(root, credential, ipc).await
}

pub async fn hello(
    link: &mut ModuleLink,
    params: Value,
    claim: &ModuleContractClaim,
) -> Result<Value> {
    link.hello(params, Some(claim)).await
}

pub async fn call(link: &mut ModuleLink, method: &str, params: Value) -> Result<Value> {
    match method {
        "module.next" => link.next().await,
        "module.outcome" => link.outcome(params).await,
        "module.observe" => link.observe(params).await,
        "module.result" => link.result(params).await,
        _ => Err(Error::new(
            "ADAPTER_RPC_UNSUPPORTED",
            "adapter may call only the fixed module RPC surface",
        )),
    }
}
