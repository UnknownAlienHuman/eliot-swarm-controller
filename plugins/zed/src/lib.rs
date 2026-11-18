#![forbid(unsafe_code)]

mod batch;

use std::{collections::BTreeMap, path::PathBuf, process::ExitStatus, time::Duration};

use eliot_core::{ContextItem, Error, NativeEvent, Result, SecretPolicy, SourceBatch};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, CommitSink,
    InitializeParams, Peer, PeerClient, PluginManifest, RpcRequest,
    native_transport::{NativeChild, NativeLabels},
    protocol::{MAX_CREDENTIAL_BYTES, ManifestFeatures},
    session::{
        attach_final_text, decode, failed_start_value, next_input_outcomes, optional_profile_text,
        register_native_process, split_provider_model, target_environment, unknown_snapshot,
        validate_binding, validate_identity, validate_source_plan, with_claimed_context,
    },
};
use serde_json::{Value, json};
use tokio::time::{Instant, timeout_at};

const HARNESS: &str = "Zed";
const FAMILIES: &[&str] = &["session", "turn", "message"];
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const LABELS: NativeLabels = NativeLabels {
    short: HARNESS,
    server: "Zed eval-cli",
};

#[derive(Default)]
struct Plugin {
    init: Option<InitializeParams>,
    session: Option<Session>,
}

struct Session {
    agent: String,
    generation: i64,
    start_id: String,
    program: PathBuf,
    identity: eliot_plugin::FileIdentity,
    model: String,
    workspace: PathBuf,
    root: PathBuf,
    environment: Vec<(std::ffi::OsString, std::ffi::OsString)>,
    policy: SecretPolicy,
    sink: CommitSink,
    send: Option<Sent>,
    stopped: bool,
}

struct Sent {
    command: String,
    turn: String,
    fingerprint: [u8; 32],
    associated: bool,
    interrupt: Option<(String, Result<Value>)>,
    status: Option<&'static str>,
    child: Option<NativeChild>,
    deadline: Instant,
}

enum Wake {
    Request(Option<RpcRequest>),
    Native(Result<ExitStatus>),
}

pub async fn run() -> Result<()> {
    let mut peer = Peer::stdio();
    let host = peer.client();
    let mut plugin = Plugin::default();
    loop {
        let wake = tokio::select! {
            biased;
            native = async {
                let Some(sent) = plugin.session.as_mut().and_then(|s| s.send.as_mut()) else {
                    return std::future::pending().await;
                };
                let Some(child) = sent.child.as_mut() else {
                    return std::future::pending().await;
                };
                match timeout_at(sent.deadline, child.wait_for_process_exit()).await {
                    Ok(result) => result,
                    Err(_) => Err(Error::new("unknown", "Zed batch exceeded its host deadline")),
                }
            } => Wake::Native(native),
            request = peer.next_request() => Wake::Request(request),
        };
        match wake {
            Wake::Native(exit) => {
                if let Some(session) = &mut plugin.session {
                    session.finish(exit).await?;
                }
            }
            Wake::Request(Some(request)) => {
                let shutdown = request.method == "shutdown";
                let result = plugin.dispatch(&request, &host).await;
                let done = shutdown && result.is_ok();
                host.respond(request.id, result, Instant::now() + IO_TIMEOUT)
                    .await?;
                if done {
                    return Ok(());
                }
            }
            Wake::Request(None) => {
                if let Some(session) = &mut plugin.session {
                    session.terminate().await?;
                }
                return Err(Error::new(
                    "transport_closed",
                    "Zed plugin input closed before shutdown",
                ));
            }
        }
    }
}

impl Plugin {
    async fn dispatch(&mut self, request: &RpcRequest, host: &PeerClient) -> Result<Value> {
        let raw = &request.params;
        match request.method.as_str() {
            "initialize" => self.initialize(raw),
            "agent.start" => self.start(raw, host).await,
            "agent.command" => {
                let params: AgentCommandParams = decode(raw, "invalid Zed command")?;
                let credential = self
                    .init
                    .as_ref()
                    .ok_or_else(|| Error::new("bad_state", "Zed plugin has not initialized"))?
                    .credential
                    .clone();
                let session =
                    self.bound(&params.agent_id, params.generation, &params.command_id)?;
                match params.verb.as_str() {
                    "send" => session.send(params, host, &credential).await,
                    "interrupt" => session.interrupt(&params).await,
                    _ => Err(unsupported("Zed batch does not implement this verb")),
                }
            }
            "agent.snapshot" => {
                let params: AgentSnapshotParams = decode(raw, "invalid Zed snapshot")?;
                self.bound(&params.agent_id, params.generation, &params.command_id)?
                    .snapshot(&params)
            }
            "agent.stop" => {
                let params: AgentStopParams = decode(raw, "invalid Zed stop")?;
                self.bound(&params.agent_id, params.generation, &params.command_id)?
                    .stop(&params)
                    .await
            }
            "shutdown" => {
                if let Some(session) = &mut self.session {
                    session.terminate().await?;
                }
                Ok(json!({"state": "shutdown"}))
            }
            _ => Err(unsupported("Zed eval-cli does not expose this method")),
        }
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams = decode(raw, "invalid Zed initialization")?;
        let path = PathBuf::from(&params.instance_dir);
        if params.protocol != "eliot-plugin/1"
            || params.credential.trim().is_empty()
            || params.credential.len() > MAX_CREDENTIAL_BYTES
            || !path.is_absolute()
            || !path.is_dir()
        {
            return Err(Error::new(
                "invalid",
                "Zed requires a private instance directory and credential",
            ));
        }
        if self.init.is_some() {
            return Err(Error::new("bad_state", "Zed plugin already initialized"));
        }
        self.init = Some(params);
        serde_json::to_value(PluginManifest {
            name: "zed".into(),
            version: env!("CARGO_PKG_VERSION").into(),
            protocol: "eliot-plugin/1".into(),
            harness_version: "eval-cli batch".into(),
            available: cfg!(target_os = "linux"),
            mode: "per_agent".into(),
            verbs: ["launch", "send", "interrupt", "stop"]
                .map(str::to_owned)
                .to_vec(),
            child_verbs: Vec::new(),
            features: ManifestFeatures {
                start_requires_input: false,
                native_children: false,
                discover: false,
                observe: false,
            },
            sources: BTreeMap::from([(
                "controlled".to_owned(),
                FAMILIES
                    .iter()
                    .map(|family| (family.to_string(), "stream".into()))
                    .collect(),
            )]),
            proxy: "none".into(),
        })
        .map_err(|_| Error::new("internal", "could not encode Zed manifest"))
    }

    async fn start(&mut self, raw: &Value, host: &PeerClient) -> Result<Value> {
        let params: AgentStartParams = decode(raw, "invalid Zed start")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            HARNESS,
        )?;
        if self.session.is_some() {
            return Err(Error::new("bad_state", "Zed batch slot is already bound"));
        }
        let init = self
            .init
            .as_ref()
            .ok_or_else(|| Error::new("bad_state", "Zed plugin has not initialized"))?;
        let mut session = match Session::prepare(&params, init, host) {
            Ok(session) => session,
            Err(error) => return Ok(failed_start_value(&params, error)),
        };
        let effective = json!({"model": null, "model_verified": false, "mcp_servers": null, "mcp_servers_verified": false});
        session
            .commit(
                "session.ready",
                "ready",
                json!({
                    "command_id": params.command_id, "effective": effective,
                }),
            )
            .await?;
        self.session = Some(session);
        Ok(json!({
            "agent_id": params.agent_id, "generation": params.generation,
            "command_id": params.command_id, "state": "ready", "effective": effective,
        }))
    }

    fn bound(&mut self, agent: &str, generation: i64, command: &str) -> Result<&mut Session> {
        validate_identity(agent, generation, command, HARNESS)?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Zed batch slot is unbound"))?;
        validate_binding(
            &session.agent,
            session.generation,
            agent,
            generation,
            HARNESS,
        )?;
        Ok(session)
    }
}

impl Session {
    fn prepare(
        params: &AgentStartParams,
        init: &InitializeParams,
        host: &PeerClient,
    ) -> Result<Self> {
        if !cfg!(target_os = "linux") || params.resume_from.is_some() || params.initial.is_some() {
            return Err(unsupported("Zed needs a fresh Linux eval-cli batch slot"));
        }
        validate_source_plan(HARNESS, FAMILIES, params.source_plan.as_ref())?;
        let profile = params
            .profile
            .as_ref()
            .and_then(Value::as_object)
            .ok_or_else(|| Error::new("profile_error", "Zed profile is required"))?;
        if profile["dangerously_skip_permissions"].as_bool() != Some(true)
            || profile["mcp"].as_str() != Some("inherit")
        {
            return Err(unsupported(
                "Zed eval-cli requires explicit full tool access and native MCP policy",
            ));
        }
        let cmd = optional_profile_text(profile, "cmd", HARNESS)?
            .ok_or_else(|| Error::new("profile_error", "Zed eval-cli executable is required"))?;
        let model = optional_profile_text(profile, "model", HARNESS)?
            .filter(|model| split_provider_model(model).is_some())
            .ok_or_else(|| Error::new("profile_error", "Zed requires a provider/model"))?;
        if !batch::supports_model(&model) {
            return Err(unsupported(
                "Zed eval-cli supports Anthropic, OpenAI, Google and Ollama in this adapter",
            ));
        }
        let (program, identity) = batch::executable(&cmd)?;
        let workspace = PathBuf::from(params.workspace.as_deref().unwrap_or_default());
        if !workspace.is_absolute() || !workspace.is_dir() {
            return Err(Error::new(
                "invalid",
                "Zed requires an absolute existing workspace",
            ));
        }
        let root = PathBuf::from(&init.instance_dir).join("zed-eval");
        std::fs::create_dir(&root).map_err(|_| {
            Error::new(
                "platform_io",
                "could not create private Zed batch directory",
            )
        })?;
        for dir in [
            "home", "config", "data", "state", "cache", "runtime", "tmp", "output",
        ] {
            std::fs::create_dir(root.join(dir)).map_err(|_| {
                Error::new(
                    "platform_io",
                    "could not initialize private Zed batch paths",
                )
            })?;
        }
        let env = target_environment(
            params.target_environment.as_ref(),
            profile.get("pass_env"),
            HARNESS,
        )?;
        let policy = SecretPolicy::new(
            env.secrets
                .iter()
                .map(String::as_str)
                .chain([init.credential.as_str()]),
        );
        Ok(Self {
            agent: params.agent_id.clone(),
            generation: params.generation,
            start_id: params.command_id.clone(),
            program,
            identity,
            model,
            workspace,
            root,
            environment: env.variables,
            policy,
            sink: CommitSink::new(host.clone(), &init.credential),
            send: None,
            stopped: false,
        })
    }

    async fn send(
        &mut self,
        params: AgentCommandParams,
        host: &PeerClient,
        credential: &str,
    ) -> Result<Value> {
        if self.stopped || params.command_id == self.start_id {
            return Err(Error::new("bad_state", "Zed batch slot is unavailable"));
        }
        if params.turn_id.is_some()
            || params.request_id.is_some()
            || params.config_revision.is_some()
            || params.model_override.is_some()
            || params.effort_override.is_some()
        {
            return Err(unsupported(
                "Zed accepts a fresh input against its frozen model",
            ));
        }
        let fingerprint = eliot_core::canonical_json_sha256(
            &json!({"args": params.args, "context": params.context, "seq": params.dispatch_seq}),
        )
        .map_err(|_| Error::new("internal", "could not bind Zed input"))?;
        if let Some(sent) = &self.send {
            if sent.command != params.command_id {
                return Err(unsupported("Zed eval-cli accepts one send per launch"));
            }
            if sent.fingerprint != fingerprint {
                return Err(Error::new(
                    "idempotency_mismatch",
                    "Zed send payload changed",
                ));
            }
            return sent.dispatch_outcome();
        }
        let text = params.args["text"]
            .as_str()
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| Error::new("invalid", "Zed send requires text"))?;
        let input = with_claimed_context(text, &params.context, HARNESS)?;
        let (_, current) = batch::executable(self.program.to_str().unwrap_or_default())?;
        if current != self.identity {
            return Err(Error::new(
                "native_not_dispatched",
                "Zed eval-cli changed after discovery",
            ));
        }
        let launch = batch::command(
            &self.program,
            &self.workspace,
            &self.root,
            &self.model,
            self.environment.clone(),
        );
        let (mut child, mut stdout) =
            NativeChild::spawn_with_private_environment(launch, self.policy.clone(), LABELS)
                .await
                .map_err(|error| {
                    if error.code == "native_spawn_refused" {
                        error
                    } else {
                        error.with_code("unknown")
                    }
                })?;
        child.set_output_reader(tokio::spawn(async move {
            tokio::io::copy(&mut stdout, &mut tokio::io::sink())
                .await
                .map(|_| ())
                .map_err(|_| Error::new("unknown", "Zed stdout drain failed"))
        }));
        let deadline = Instant::now() + Duration::from_secs(batch::NATIVE_TIMEOUT + 90);
        self.send = Some(Sent {
            turn: format!("zed:{}:{}", self.agent, self.generation),
            command: params.command_id,
            fingerprint,
            associated: false,
            interrupt: None,
            status: None,
            child: Some(child),
            deadline,
        });
        let result = self
            .deliver(host, credential, &input, &params.context)
            .await;
        if result.is_err() {
            self.terminate().await?;
            return Err(Error::new(
                "unknown",
                "Zed batch input or turn commit is unresolved",
            ));
        }
        self.send
            .as_ref()
            .expect("batch was retained")
            .dispatch_outcome()
    }

    async fn deliver(
        &mut self,
        host: &PeerClient,
        credential: &str,
        input: &str,
        context: &[ContextItem],
    ) -> Result<()> {
        let sent = self.send.as_ref().expect("batch was retained");
        let child = sent.child.as_ref().expect("batch owns a child");
        register_native_process(
            host,
            credential,
            &self.agent,
            self.generation,
            child.pid(),
            HARNESS,
            "batch",
            "session",
            IO_TIMEOUT,
        )
        .await?;
        child
            .writer()
            .send_batch_input(input.as_bytes(), Instant::now() + IO_TIMEOUT)
            .await?;
        let data = json!({
            "command_id": sent.command, "turn_id": sent.turn,
            "context_outcomes": next_input_outcomes(context, &sent.command),
        });
        self.commit("turn.started", "started", data).await?;
        self.send.as_mut().expect("batch was retained").associated = true;
        Ok(())
    }

    async fn finish(&mut self, exit: Result<ExitStatus>) -> Result<()> {
        let sent = self.send.as_mut().expect("only an owned batch can finish");
        let Some(mut child) = sent.child.take() else {
            return Ok(());
        };
        let clean_exit = match exit {
            Ok(status) => child
                .await_output_drained(Instant::now() + IO_TIMEOUT)
                .await
                .ok()
                .map(|()| status),
            Err(_) => {
                child.terminate(true, Instant::now() + IO_TIMEOUT).await?;
                None
            }
        };
        if !sent.associated {
            return Ok(());
        }

        let status = match clean_exit {
            Some(exit) => batch::read_terminal(&self.root, exit.code(), &self.model).await,
            None => None,
        };
        let binding = json!({"command_id": sent.command, "turn_id": sent.turn});
        if let Some(status) = status {
            let mut terminal = binding.clone();
            terminal["status"] = json!(status);
            if status == "completed" {
                if let Some(answer) = batch::read_answer(&self.root, &self.policy).await {
                    attach_final_text(&mut terminal, &answer, false, &self.policy);
                    let mut message = binding;
                    message["text"] = json!(answer);
                    self.commit("message.completed", "response", message)
                        .await?;
                } else {
                    terminal["final_text_unavailable"] =
                        json!("missing_or_unreadable_native_answer");
                }
            }
            self.commit("turn.completed", "terminal", terminal).await?;
        } else {
            self.commit("turn.unknown", "terminal", binding).await?;
        }
        self.send.as_mut().expect("batch was retained").status = Some(status.unwrap_or("unknown"));
        Ok(())
    }

    async fn interrupt(&mut self, params: &AgentCommandParams) -> Result<Value> {
        let sent = self
            .send
            .as_mut()
            .filter(|sent| sent.associated && params.turn_id.as_deref() == Some(sent.turn.as_str()))
            .ok_or_else(|| {
                Error::new("stale_turn", "Zed interrupt requires the exact batch turn")
            })?;
        if let Some((command, outcome)) = &sent.interrupt {
            return if command == &params.command_id {
                outcome.clone()
            } else {
                Err(Error::new(
                    "unknown",
                    "Zed has already attempted an interrupt for this batch",
                ))
            };
        }
        let child = sent
            .child
            .as_mut()
            .ok_or_else(|| Error::new("stale_turn", "Zed batch already exited"))?;
        sent.interrupt = Some((
            params.command_id.clone(),
            Err(Error::new(
                "unknown",
                "Zed interrupt has no committed terminal receipt",
            )),
        ));
        let deadline = Instant::now() + eliot_plugin::native_lifecycle::STOP_INTERRUPT_TIMEOUT;
        child.request_interrupt(deadline).await?;
        let exit = timeout_at(deadline, child.wait_for_process_exit())
            .await
            .map_err(|_| Error::new("unknown", "Zed did not prove an interrupted batch"))?;
        self.finish(exit).await?;
        let sent = self.send.as_mut().expect("batch was retained");
        if sent.status != Some("interrupted") {
            return Err(Error::new(
                "unknown",
                "Zed did not report an interrupted batch",
            ));
        }
        let result =
            json!({"state":"applied", "command_id":params.command_id, "turn_id":sent.turn});
        sent.interrupt = Some((params.command_id.clone(), Ok(result.clone())));
        Ok(result)
    }

    async fn stop(&mut self, params: &AgentStopParams) -> Result<Value> {
        if self.stopped {
            return Ok(json!({"state":"applied", "command_id":params.command_id}));
        }
        if let Some(interrupt) = params.interrupt()
            && self.send.as_ref().is_some_and(|sent| sent.child.is_some())
        {
            let _ = self.interrupt(&interrupt).await;
        }
        self.terminate().await?;
        self.commit(
            "session.ended",
            "stopped",
            json!({"command_id": params.command_id}),
        )
        .await?;
        self.stopped = true;
        Ok(json!({"state":"applied", "command_id":params.command_id}))
    }

    async fn terminate(&mut self) -> Result<()> {
        if let Some(sent) = &mut self.send
            && let Some(child) = &mut sent.child
        {
            child.terminate(true, Instant::now() + IO_TIMEOUT).await?;
            sent.child = None;
        }
        Ok(())
    }

    fn snapshot(&self, params: &AgentSnapshotParams) -> Result<Value> {
        if let Some(sent) = &self.send
            && sent.associated
            && sent.status != Some("unknown")
            && params.command_id == sent.command
            && params
                .turn_id
                .as_ref()
                .is_none_or(|turn| turn == &sent.turn)
        {
            return Ok(json!({
                "agent_id": self.agent, "generation": self.generation, "command_id": sent.command,
                "state": if sent.child.is_some() { "busy" } else { "idle" }, "turn_id":sent.turn,
                "terminal": sent.status.is_some(), "status":sent.status,
            }));
        }
        unknown_snapshot(params, "Zed has no matching committed batch facts")
    }

    async fn commit(&mut self, kind: &str, tag: &str, mut data: Value) -> Result<()> {
        self.policy.redact_values(&mut data);
        let id = format!("zed:{}:{}:{tag}", self.agent, self.generation);
        self.sink
            .commit(
                SourceBatch {
                    batch_id: id.clone(),
                    agent_id: self.agent.clone(),
                    generation: self.generation,
                    source: "stream".into(),
                    cursor: None,
                    events: vec![NativeEvent {
                        native_id: id,
                        kind: kind.into(),
                        ts: None,
                        data,
                    }],
                },
                Instant::now() + IO_TIMEOUT,
            )
            .await
            .map(|_| ())
    }
}

impl Sent {
    fn dispatch_outcome(&self) -> Result<Value> {
        if self.associated {
            Ok(json!({"state":"applied", "command_id": self.command, "turn_id": self.turn}))
        } else {
            Err(Error::new(
                "unknown",
                "Zed batch input settlement is unresolved",
            ))
        }
    }
}

fn unsupported(message: &str) -> Error {
    Error::new("unsupported", message)
}
