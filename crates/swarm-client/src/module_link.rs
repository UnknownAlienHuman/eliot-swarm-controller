//! Thin typed hello helper for a binding-scoped module credential.
//!
//! Store remains the authorization boundary. The link never selects a route,
//! registers a descriptor, or interprets descriptor capabilities as rights.

use crate::{Client, IpcConfig};
use serde_json::{Value, json};
use std::path::Path;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_contract::ModuleContractClaim,
};

pub struct ModuleLink {
    client: Client,
}

impl ModuleLink {
    /// Connect with the exact module credential already assigned to its
    /// binding and generation. Ordinary client credentials are not accepted
    /// by the Store module methods.
    pub async fn connect(root: &Path, credential: &Credential, config: &IpcConfig) -> Result<Self> {
        Ok(Self {
            client: Client::connect(root, credential, config).await?,
        })
    }

    /// Add the shared public claim to the existing hello envelope. `None` is
    /// reserved for explicitly legacy, unversioned bindings.
    pub async fn hello(
        &mut self,
        mut params: Value,
        claim: Option<&ModuleContractClaim>,
    ) -> Result<Value> {
        let fields = params
            .as_object_mut()
            .ok_or_else(|| Error::invalid("module.hello parameters must be an object"))?;
        if fields.contains_key("module_contract") {
            return Err(Error::invalid(
                "module_contract is supplied only through the typed claim argument",
            ));
        }
        if let Some(claim) = claim {
            claim.validate().map_err(Error::invalid)?;
            fields.insert("module_contract".to_owned(), serde_json::to_value(claim)?);
        }
        self.client.request("module.hello", params).await
    }

    pub async fn next(&mut self) -> Result<Value> {
        self.client.request("module.next", json!({})).await
    }

    pub async fn outcome(&mut self, params: Value) -> Result<Value> {
        self.client.request("module.outcome", params).await
    }

    pub async fn observe(&mut self, params: Value) -> Result<Value> {
        self.client.request("module.observe", params).await
    }

    pub async fn result(&mut self, params: Value) -> Result<Value> {
        self.client.request("module.result", params).await
    }
}
