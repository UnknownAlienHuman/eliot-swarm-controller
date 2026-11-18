use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use eliot_core::{Error, Result};
use eliot_plugin::{
    AgentCommandParams, AgentStartParams, AgentStopParams, InitializeParams, Peer, PeerClient,
    PluginManifest, RpcRequest,
    protocol::{MAX_CREDENTIAL_BYTES, ManifestFeatures},
    session::{
        classify_start_failure, decode, failed_start_value, not_initialized, validate_binding,
        validate_identity,
    },
};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::{
    events::FAMILIES,
    profile,
    session::{self, Session},
};

pub(crate) async fn run() -> Result<()> {
    let mut peer = Peer::stdio();
    let client = peer.client();
    let mut state = State::default();
    loop {
        let request = if let Some(session) = state.session.as_mut() {
            let mut stream = session.stream();
            tokio::select! {
                biased;
                frame = session.frames.recv() => {
                    let Some(frame) = frame else {
                        session.events.loss("Zed host output closed unexpectedly").await;
                        return Err(Error::new("unknown", "Zed host output closed"));
                    };
                    if let Err(error) = session.events.consume(frame).await {
                        session.events.loss("Zed native event could not be verified or committed").await;
                        return Err(error);
                    }
                    continue;
                }
                () = session::wait_for_fault(&mut stream) => {
                    session.events.loss("Zed native output was incomplete").await;
                    return Err(Error::new("unknown", "Zed native output was incomplete"));
                }
                request = peer.next_request() => request,
            }
        } else {
            peer.next_request().await
        };
        let Some(request) = request else { break };
        let shutdown = request.method == "shutdown";
        let result = state.dispatch(&request, &client).await;
        client
            .respond(request.id, result, Instant::now() + Duration::from_secs(15))
            .await?;
        if shutdown {
            return Ok(());
        }
    }
    Err(Error::new(
        "transport_closed",
        "Zed plugin input closed before shutdown",
    ))
}

#[derive(Default)]
struct State {
    initialized: Option<InitializeParams>,
    start: Option<([u8; 32], Result<Value>)>,
    session: Option<Session>,
}

impl State {
    async fn dispatch(&mut self, request: &RpcRequest, peer: &PeerClient) -> Result<Value> {
        match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "agent.start" => self.start(&request.params, peer).await,
            "agent.command" => {
                let p: AgentCommandParams = decode(&request.params, "invalid Zed command")?;
                if p.verb != "send" {
                    return Err(unsupported());
                }
                self.bound(&p.agent_id, p.generation, &p.command_id)?
                    .send(p)
                    .await
            }
            "agent.stop" => {
                let p: AgentStopParams = decode(&request.params, "invalid Zed stop")?;
                self.bound(&p.agent_id, p.generation, &p.command_id)?
                    .stop(p.force, Some(&p.command_id))
                    .await?;
                self.session = None;
                Ok(json!({"state":"applied"}))
            }
            "shutdown" => {
                if let Some(session) = self.session.as_mut() {
                    session.stop(true, None).await?;
                }
                Ok(json!({"state":"shutdown"}))
            }
            _ => Err(unsupported()),
        }
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let p: InitializeParams = decode(raw, "invalid Zed initialization")?;
        let dir = PathBuf::from(&p.instance_dir);
        if self.initialized.is_some()
            || p.protocol != "eliot-plugin/1"
            || !dir.is_absolute()
            || !dir.is_dir()
            || p.credential.is_empty()
            || p.credential.len() > MAX_CREDENTIAL_BYTES
        {
            return Err(Error::new(
                "invalid",
                "Zed requires protocol, instance directory and credential",
            ));
        }
        self.initialized = Some(p);
        serde_json::to_value(PluginManifest {
            name: "zed".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            protocol: "eliot-plugin/1".into(),
            harness_version: "1".into(),
            available: true,
            mode: "per_agent".into(),
            verbs: ["launch", "send", "stop"].map(str::to_owned).to_vec(),
            child_verbs: vec![],
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
                    .map(|&family| (family.into(), "stream".into()))
                    .collect(),
            )]),
            proxy: "none".into(),
        })
        .map_err(|_| Error::new("internal", "Zed manifest encoding failed"))
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        let digest = eliot_core::canonical_json_sha256(raw)
            .map_err(|_| Error::new("invalid", "could not bind launch payload"))?;
        if let Some((prior, result)) = &self.start {
            if *prior == digest {
                return result.clone();
            }
            return Err(Error::new(
                "bad_state",
                "Zed plugin already started another session",
            ));
        }
        let p: AgentStartParams = decode(raw, "invalid Zed start")?;
        validate_identity(&p.agent_id, p.generation, &p.command_id, "Zed")?;
        if p.resume_from.is_some() {
            return Err(unsupported());
        }
        let init = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Zed"))?;
        let plan = profile::plan(
            p.profile.as_ref(),
            p.target_environment.as_ref(),
            p.workspace.as_deref(),
        );
        let result = match plan {
            Err(error) => Ok(failed_start_value(
                &p,
                classify_start_failure(error, "zed", "profile"),
            )),
            Ok(plan) => match Session::open(
                &p,
                plan,
                &PathBuf::from(&init.instance_dir),
                peer,
                &init.credential,
            )
            .await
            {
                Ok(session) => {
                    let response = session.start.clone();
                    self.session = Some(session);
                    Ok(response)
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "launch_mismatch"
                            | "native_model_unavailable"
                            | "native_spawn_refused"
                            | "io"
                    ) =>
                {
                    Ok(failed_start_value(
                        &p,
                        classify_start_failure(error, "zed", "session"),
                    ))
                }
                Err(error) => Err(error),
            },
        };
        // The payload contains target-environment secrets. Keep only the digest.
        self.start = Some((digest, result.clone()));
        result
    }

    fn bound(&mut self, agent: &str, generation: i64, command: &str) -> Result<&mut Session> {
        validate_identity(agent, generation, command, "Zed")?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Zed session is not running"))?;
        validate_binding(
            &session.events.agent,
            session.events.generation,
            agent,
            generation,
            "Zed",
        )?;
        Ok(session)
    }
}

fn unsupported() -> Error {
    Error::new(
        "unsupported",
        "method or verb is unsupported by Zed stage 1",
    )
}
