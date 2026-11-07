use std::{collections::BTreeMap, path::PathBuf};

use eliot_core::{Error, Result};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::{MAX_CREDENTIAL_BYTES, ManifestFeatures},
    session::{decode, not_initialized, validate_binding, validate_identity},
};
use serde_json::{Value, json};
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

use crate::{events::FAMILIES, hooks::HookServer, session::Session};

pub(crate) const TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Plugin {
    initialized: Option<InitializeParams>,
    session: Option<Session>,
    start: Option<(Value, Result<Value>)>,
}

pub(crate) async fn run() -> Result<()> {
    let mut peer = Peer::stdio();
    let client = peer.client();
    let (tx, mut requests) = mpsc::channel(16);
    let reader = tokio::spawn(async move {
        while let Some(request) = peer.next_request().await {
            if tx.send(request).await.is_err() {
                break;
            }
        }
    });
    let mut plugin = Plugin::default();
    let mut shutdown = false;
    while let Some(request) = requests.recv().await {
        let stopping = request.method == "shutdown";
        let result = plugin.dispatch(&request, &client).await;
        let settled = result.is_ok();
        if client
            .respond(request.id, result, Instant::now() + TIMEOUT)
            .await
            .is_err()
        {
            break;
        }
        if stopping {
            shutdown = settled;
            break;
        }
    }
    if let Some(session) = &mut plugin.session {
        let _ = session.terminate(true).await;
    }
    reader.abort();
    let _ = reader.await;
    if shutdown {
        Ok(())
    } else {
        Err(Error::new(
            "transport_closed",
            "Antigravity plugin input closed before shutdown completed",
        ))
    }
}

impl Plugin {
    async fn dispatch(&mut self, request: &RpcRequest, peer: &PeerClient) -> Result<Value> {
        match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "profile.store" => Err(Error::new(
                "unsupported",
                "Antigravity exposes no verified metadata-only native-store readback",
            )),
            "agent.start" => self.start(&request.params, peer).await,
            "agent.command" => {
                let params: AgentCommandParams =
                    decode(&request.params, "Invalid Antigravity command")?;
                validate_identity(
                    &params.agent_id,
                    params.generation,
                    &params.command_id,
                    "Antigravity",
                )?;
                let session = self.bound(&params.agent_id, params.generation)?;
                session.command(params).await
            }
            "agent.snapshot" => {
                let params: AgentSnapshotParams =
                    decode(&request.params, "Invalid Antigravity snapshot")?;
                validate_identity(
                    &params.agent_id,
                    params.generation,
                    &params.command_id,
                    "Antigravity",
                )?;
                self.bound(&params.agent_id, params.generation)?
                    .snapshot(params)
                    .await
            }
            "agent.stop" => {
                let params: AgentStopParams = decode(&request.params, "Invalid Antigravity stop")?;
                validate_identity(
                    &params.agent_id,
                    params.generation,
                    &params.command_id,
                    "Antigravity",
                )?;
                self.bound(&params.agent_id, params.generation)?
                    .stop(params)
                    .await
            }
            "shutdown" => {
                if let Some(session) = &mut self.session {
                    session.terminate(true).await?;
                }
                Ok(json!({"state": "shutdown"}))
            }
            _ => Err(Error::new(
                "unsupported",
                "Antigravity method is unconnected",
            )),
        }
    }

    fn bound(&mut self, agent: &str, generation: i64) -> Result<&mut Session> {
        self.initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Antigravity"))?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Antigravity session is not retained"))?;
        validate_binding(
            &session.agent,
            session.generation,
            agent,
            generation,
            "Antigravity",
        )?;
        Ok(session)
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams = decode(raw, "Invalid Antigravity initialization")?;
        if params.protocol != "eliot-plugin/1"
            || params.instance_dir.trim().is_empty()
            || params.credential.trim().is_empty()
            || params.credential.len() > MAX_CREDENTIAL_BYTES
        {
            return Err(Error::new(
                "invalid",
                "Antigravity requires protocol, private instance directory and credential",
            ));
        }
        let instance_dir = PathBuf::from(&params.instance_dir);
        if !instance_dir.is_absolute() || !instance_dir.is_dir() {
            return Err(Error::new(
                "invalid",
                "Antigravity requires an existing absolute private instance directory",
            ));
        }
        if let Some(existing) = &self.initialized {
            if existing.instance_dir != params.instance_dir
                || existing.credential != params.credential
            {
                return Err(Error::new(
                    "bad_state",
                    "Antigravity already belongs to another instance",
                ));
            }
        } else {
            self.initialized = Some(params);
        }
        let manifest = PluginManifest {
            name: "antigravity".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            protocol: "eliot-plugin/1".into(),
            harness_version: "1.5.2".into(),
            available: true,
            mode: "per_agent".into(),
            verbs: ["launch", "resume", "send", "steer", "interrupt", "stop"]
                .map(str::to_owned)
                .to_vec(),
            child_verbs: vec![],
            proxy: "none".into(),
            features: ManifestFeatures {
                start_requires_input: false,
                native_children: false,
                discover: false,
                observe: false,
            },
            sources: BTreeMap::from([(
                "controlled".into(),
                FAMILIES
                    .iter()
                    .map(|f| (f.to_string(), "stream".into()))
                    .collect(),
            )]),
        };
        serde_json::to_value(manifest)
            .map_err(|_| Error::new("internal", "Could not encode Antigravity manifest"))
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        let params: AgentStartParams = decode(raw, "Invalid Antigravity start")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Antigravity",
        )?;
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Antigravity"))?;
        let credential = initialized.credential.clone();
        let instance_dir = PathBuf::from(&initialized.instance_dir);
        let identity = serde_json::to_value(&params)
            .map_err(|_| Error::new("internal", "Could not encode Antigravity start binding"))?;
        if let Some((previous, outcome)) = &self.start {
            if previous != &identity {
                return Err(Error::new(
                    "source_integrity",
                    "Antigravity start identity or inputs changed after dispatch",
                ));
            }
            return outcome.clone();
        }
        let hooks = HookServer::bind().await?;
        let plan = crate::profile::Plan::new(&params, &credential, hooks.token(), &instance_dir)?;
        // Latch before creating a native conversation. Any failure after spawn
        // is retained; this generation never starts a second process on retry.
        self.start = Some((
            identity.clone(),
            Err(Error::new(
                "unknown",
                "Antigravity start is unresolved; it will not be replayed",
            )),
        ));
        let result =
            match Session::spawn(&params, &plan, peer, &credential, &instance_dir, hooks).await {
                Ok(session) => {
                    self.session = Some(session);
                    let session = self
                        .session
                        .as_mut()
                        .ok_or_else(|| Error::new("internal", "Antigravity session disappeared"))?;
                    let result = session.start(&plan, peer, &credential).await;
                    if result.is_err() && session.terminate(true).await.is_err() {
                        Err(Error::new(
                            "unknown",
                            "Antigravity start failed and owned cleanup is unconfirmed",
                        ))
                    } else {
                        result
                    }
                }
                Err(error) => Err(error),
            };
        self.start = Some((identity, result.clone()));
        result
    }
}
