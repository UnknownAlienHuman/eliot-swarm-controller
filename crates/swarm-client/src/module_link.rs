//! Thin typed hello helper for a binding-scoped module credential.
//!
//! Store remains the authorization boundary. The link never selects a route,
//! registers a descriptor, or interprets descriptor capabilities as rights.

use crate::{Client, IpcConfig};
use serde_json::{Value, json};
use std::{path::Path,sync::{Arc,atomic::{AtomicBool,Ordering}}};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
    module_contract::ModuleContractClaim,
    module_catalog::Sha256Digest,
};

#[derive(Default)]
struct Hello { digest:Option<Sha256Digest>, reply:Option<Result<Value>> }
/// Clones share one authenticated Client and one hello barrier, hence the same
/// Store Principal.link_id. Give pending next/delivery/stream tasks clones;
/// never call connect/hello independently for those lanes.
#[derive(Clone)]
pub struct ModuleLink {
    client: Client,
    hello:Arc<tokio::sync::Mutex<Hello>>,
    ready:Arc<AtomicBool>,
}

impl ModuleLink {
    /// Connect with the exact module credential already assigned to its
    /// binding and generation. Ordinary client credentials are not accepted
    /// by the Store module methods.
    pub async fn connect(root: &Path, credential: &Credential, config: &IpcConfig) -> Result<Self> {
        Ok(Self {
            client: Client::connect(root, credential, config).await?,
            hello:Arc::new(tokio::sync::Mutex::new(Hello::default())),
            ready:Arc::new(AtomicBool::new(false)),
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
        let digest=Sha256Digest::of_canonical_json(&params).map_err(|_|Error::invalid("module hello cannot be encoded"))?;
        let mut hello=self.hello.lock().await;
        if !self.client.is_connected(){return Err(Error::new("DISCONNECTED","the physical module link failed; cached hello is not a new handshake"));}
        if let Some(prior)=&hello.digest {
            if prior!=&digest{return Err(Error::new("MODULE_HELLO_CONFLICT","this physical module link already attempted another hello"));}
            return hello.reply.clone().unwrap_or_else(||Err(Error::new("MODULE_HELLO_INCOMPLETE","prior hello was interrupted; inspect/reconnect explicitly")));
        }
        hello.digest=Some(digest);
        let result=self.client.request_shared("module.hello",params).await;
        if result.is_ok(){self.ready.store(true,Ordering::Release);}
        hello.reply=Some(result.clone());
        result
    }

    fn established(&self)->Result<()> {
        if !self.ready.load(Ordering::Acquire){return Err(Error::new("MODULE_HELLO_REQUIRED","await this physical link's module hello before application calls"));}
        Ok(())
    }
    pub fn is_connected(&self)->bool {self.client.is_connected()}
    pub async fn close(&self){self.client.close().await;}

    pub async fn next(&mut self) -> Result<Value> {
        self.established()?;
        self.client.request("module.next", json!({})).await
    }

    pub async fn outcome(&mut self, params: Value) -> Result<Value> {
        self.established()?;
        self.client.request("module.outcome", params).await
    }

    pub async fn observe(&mut self, params: Value) -> Result<Value> {
        self.established()?;
        self.client.request("module.observe", params).await
    }

    /// Ephemeral content ingress only. No Observation/event or native request
    /// is produced; Store validates current link, boot and exact Operation.
    pub async fn publish_stream(
        &mut self,
        frame: &swarm_contracts::native_stream::StreamPublish,
    ) -> Result<Value> {
        self.established()?;
        self.client
            .request(
                swarm_contracts::native_stream::PUBLISH_METHOD,
                serde_json::to_value(frame)?,
            )
            .await
    }

    pub async fn result(&mut self, params: Value) -> Result<Value> {
        self.established()?;
        self.client.request("module.result", params).await
    }
}
