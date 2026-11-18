use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use eliot_core::{Error, Result, limits::MAX_EVENT_BYTES};
use eliot_plugin::{
    AgentCommandParams, AgentStartParams, PeerClient,
    native_stdio::{
        NativeFrame, NativeJsonRpcErrorBody, NativeLaunch, NativeStdioConfig, StdioProcess,
        StreamState,
    },
    native_transport::DEFAULT_MAX_NATIVE_FRAME_BYTES,
    session::register_native_process,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};

use crate::{
    events::{Active, Events},
    profile::{self, Plan},
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct ZedConfig;

impl NativeStdioConfig for ZedConfig {
    const SERVER: &'static str = "Zed agent host";
    const SHORT: &'static str = "Zed";
    const ID_PREFIX: &'static str = "zed";
    const MAX_FRAME_BYTES: usize = DEFAULT_MAX_NATIVE_FRAME_BYTES;
    const OVERSIZED_KNOWN_METHODS: &'static [&'static str] = &["session.event"];

    fn map_response_error(error: NativeJsonRpcErrorBody) -> Result<Value> {
        let reported = error
            .data
            .as_ref()
            .and_then(|data| data.get("code"))
            .and_then(Value::as_str);
        let (code, message) = match reported {
            Some("provider_auth_required") => (
                "provider_auth_required",
                "set OPENAI_API_KEY in the configured launch environment",
            ),
            Some("model_unavailable") => (
                "native_model_unavailable",
                "model is absent from the native OpenAI catalogue",
            ),
            Some("unsupported") => (
                "unsupported",
                "method is unsupported by the stage-1 Zed host",
            ),
            Some("invalid_params" | "stale_session" | "busy") => (
                "native_not_dispatched",
                "Zed host refused the operation before dispatch",
            ),
            _ => ("native_rejected", "Zed host rejected the request"),
        };
        Err(Error::new(code, message))
    }
}

#[derive(Deserialize)]
struct Handshake {
    agent_id: String,
    generation: i64,
    protocol_version: u32,
    host_version: String,
    zed_commit: String,
    data_dir: PathBuf,
    config_dir: PathBuf,
    state_dir: PathBuf,
    temp_dir: PathBuf,
}

#[derive(Deserialize)]
struct Opened {
    session_id: String,
    provider: String,
    model: String,
    credential_source: String,
    authenticated: bool,
}

pub(crate) struct Session {
    pub events: Events,
    pub start: Value,
    native: StdioProcess<ZedConfig>,
    pub frames: mpsc::Receiver<NativeFrame>,
    last_send: Option<(i64, String, [u8; 32], Result<Value>)>,
    authenticated: bool,
}

impl Session {
    pub async fn open(
        params: &AgentStartParams,
        plan: Plan,
        instance: &Path,
        peer: &PeerClient,
        credential: &str,
    ) -> Result<Self> {
        let root = instance.join("zed");
        let temp = root.join("temp");
        std::fs::create_dir_all(&temp)
            .map_err(|_| Error::new("io", "could not create Zed's private data directory"))?;
        let mut environment = plan.env;
        for name in ["TMP", "TEMP", "TMPDIR"] {
            environment.push((name.into(), temp.clone().into_os_string()));
        }
        let launch = NativeLaunch {
            program: plan.program.into_os_string(),
            args: vec!["--data-dir".into(), root.clone().into_os_string()],
            cwd: plan.cwd.clone(),
            environment,
        };
        let policy = profile::policy(&plan.secrets, credential);
        let mut native = StdioProcess::<ZedConfig>::spawn(launch, policy.clone()).await?;
        register_native_process(
            peer,
            credential,
            &params.agent_id,
            params.generation,
            native.pid(),
            "Zed",
            "agent host",
            "session",
            REQUEST_TIMEOUT,
        )
        .await?;
        let frames = native.take_frames()?;
        let client = native.client();
        let initialized = client
            .call(
                "initialize",
                json!({"agent_id":params.agent_id,"generation":params.generation}),
                Instant::now() + REQUEST_TIMEOUT,
            )
            .await?;
        let info = serde_json::from_value::<Handshake>(initialized)
            .ok()
            .filter(|info| {
                info.protocol_version == 1
                    && info.zed_commit == profile::ZED_COMMIT
                    && info.agent_id == params.agent_id
                    && info.generation == params.generation
                    && !info.host_version.is_empty()
                    && same_path(&info.data_dir, &root)
                    && same_path(&info.config_dir, &root.join("config"))
                    && same_path(&info.temp_dir, &temp)
                    && same_path(&info.state_dir, &root.join("state"))
            });
        let Some(info) = info else {
            native
                .terminate(true, Instant::now() + REQUEST_TIMEOUT)
                .await
                .map_err(|_| {
                    Error::new(
                        "unknown",
                        "Zed host mismatch and owned cleanup is unconfirmed",
                    )
                })?;
            return Err(handshake_mismatch());
        };

        let opened = client
            .call(
                "session.open",
                json!({"cwd":plan.cwd,"provider":"openai","model":plan.model}),
                Instant::now() + REQUEST_TIMEOUT,
            )
            .await;
        let opened = match opened {
            Err(error) if error.code == "native_model_unavailable" => {
                native
                    .terminate(true, Instant::now() + REQUEST_TIMEOUT)
                    .await
                    .map_err(|_| {
                        Error::new(
                            "unknown",
                            "Zed open was refused and owned cleanup is unconfirmed",
                        )
                    })?;
                return Err(error);
            }
            result => result?,
        };
        let opened: Opened = serde_json::from_value(opened)
            .map_err(|_| Error::new("unknown", "invalid Zed session-open readback"))?;
        if opened.session_id.is_empty()
            || opened.session_id.len() > 128
            || policy.contains_text(&opened.session_id)
            || opened.provider != "openai"
            || opened.model != plan.model
            || opened.credential_source
                != if opened.authenticated {
                    "environment:OPENAI_API_KEY"
                } else {
                    "unavailable"
                }
            || *native.stream_state().borrow() != StreamState::Open
        {
            return Err(Error::new(
                "unknown",
                "Zed session or source readback did not match",
            ));
        }

        let scope = format!("zed:{}:{}", params.agent_id, params.generation);
        let effective = json!({
            "cwd":plan.cwd,"model":format!("openai/{}", opened.model),"model_provider":"openai",
            "model_verified":true,"mcp_servers":[],"mcp_servers_verified":true,
            "native_subagents":"deny","context_points":[],
            "authenticated":opened.authenticated,"credential_source":opened.credential_source,
            "host_version":info.host_version,"zed_commit":info.zed_commit,
            "data_dir":info.data_dir,"config_dir":info.config_dir,"temp_dir":info.temp_dir,"state_dir":info.state_dir
        });
        let mut events = Events::new(
            params.agent_id.clone(),
            params.generation,
            opened.session_id,
            policy,
            peer,
            credential,
        );
        events
            .commit(
                "ready",
                "session.ready",
                json!({
                    "native_scope":scope,"native_ref":events.session,"effective":effective
                }),
            )
            .await?;
        let mut start = json!({
            "state":"ready","agent_id":events.agent,"generation":events.generation,
            "command_id":params.command_id,"native_scope":scope,"native_ref":events.session,"effective":effective
        });
        events.policy.redact_values(&mut start);
        Ok(Self {
            events,
            start,
            native,
            frames,
            last_send: None,
            authenticated: opened.authenticated,
        })
    }

    pub async fn send(&mut self, params: AgentCommandParams) -> Result<Value> {
        let seq = params.dispatch_seq.filter(|seq| *seq > 0).ok_or_else(|| {
            Error::new("invalid", "Zed send requires a positive dispatch sequence")
        })?;
        let payload = eliot_core::canonical_json_sha256(&params.args)
            .map_err(|_| Error::new("invalid", "Zed input could not be encoded"))?;
        if let Some((last_seq, last_id, last_payload, result)) = &self.last_send {
            if seq == *last_seq && params.command_id == *last_id && *last_payload == payload {
                return result.clone();
            }
            if seq <= *last_seq || params.command_id == *last_id {
                return Err(unknown_input());
            }
        }
        if !params.context.is_empty()
            || params.config_revision.is_some()
            || params.request_id.is_some()
            || params.model_override.is_some()
            || params.effort_override.is_some()
            || params.turn_id.is_some()
        {
            return Err(Error::new(
                "unsupported",
                "Zed stage 1 accepts a new text turn without context or overrides",
            ));
        }
        if self.events.active.is_some() {
            return Err(Error::new(
                "native_not_dispatched",
                "Zed still has an active turn",
            ));
        }
        if !self.authenticated {
            return Ok(json!({"state":"rejected","code":"provider_auth_required",
                "reason":"set OPENAI_API_KEY in the configured launch environment"}));
        }
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty() && text.len() <= MAX_EVENT_BYTES)
            .ok_or_else(|| Error::new("invalid", "Zed send requires bounded nonempty text"))?;
        if self.events.policy.contains_text(text) {
            return Err(Error::new(
                "secret_in_payload",
                "Zed input contains a known credential",
            ));
        }

        let turn = ulid::Ulid::new().to_string();
        let command = params.command_id;
        self.events.active = Some(Active::new(turn.clone(), command.clone()));
        let result = self.submit(&turn, &command, text).await;
        self.last_send = Some((seq, command, payload, result.clone()));
        result
    }

    async fn submit(&mut self, turn: &str, command: &str, text: &str) -> Result<Value> {
        let deadline = Instant::now() + REQUEST_TIMEOUT;
        let response = self.native.client().call("turn.send",
            json!({"session_id":self.events.session,"turn_id":turn,"command_id":command,"text":text}),
            deadline,
        ).await;
        match response {
            Ok(result) if result.get("accepted").and_then(Value::as_bool) == Some(true) => {}
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "native_not_dispatched" | "provider_auth_required"
                ) =>
            {
                self.events.active = None;
                return Err(error);
            }
            _ => return Err(unknown_input()),
        }
        let frame = timeout_at(deadline, self.frames.recv())
            .await
            .ok()
            .flatten()
            .ok_or_else(unknown_input)?;
        self.events
            .consume(frame)
            .await
            .map_err(|_| unknown_input())?;
        if !self
            .events
            .active
            .as_ref()
            .is_some_and(|active| active.started)
        {
            return Err(unknown_input());
        }
        Ok(
            json!({"state":"applied","agent_id":self.events.agent,"generation":self.events.generation,
            "turn_id":turn,"command_id":command}),
        )
    }

    pub fn stream(&self) -> watch::Receiver<StreamState> {
        self.native.stream_state()
    }

    pub async fn stop(&mut self, force: bool, command_id: Option<&str>) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(8);
        let client = self.native.client();
        let close = client.call(
            "session.close",
            json!({"session_id":self.events.session}),
            deadline,
        );
        tokio::pin!(close);
        let mut complete = true;
        if !force {
            loop {
                tokio::select! {
                    frame = self.frames.recv() => match frame {
                        Some(frame) if complete => complete = self.events.consume(frame).await.is_ok(),
                        Some(_) => {}
                        None => {
                            complete &= close.await.is_ok();
                            break;
                        }
                    },
                    result = &mut close => {
                        complete &= result.is_ok();
                        break;
                    }
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(8);
        self.native.terminate(force, deadline).await?;
        self.native.await_output_drained(deadline).await?;
        while let Ok(frame) = self.frames.try_recv() {
            if complete {
                complete = self.events.consume(frame).await.is_ok();
            }
        }
        if self.events.active.is_some() || !complete {
            self.events
                .loss("Zed stopped before the native source fully settled")
                .await;
        }
        let data = command_id.map_or_else(|| json!({}), |id| json!({"command_id":id}));
        self.events.commit("ended", "session.ended", data).await
    }
}

fn same_path(reported: &Path, expected: &Path) -> bool {
    reported.is_absolute()
        && reported.canonicalize().ok().is_some_and(|path| {
            expected
                .canonicalize()
                .is_ok_and(|expected| path == expected)
        })
}

fn handshake_mismatch() -> Error {
    Error::new(
        "launch_mismatch",
        "Zed host protocol, revision, generation or private paths did not match",
    )
}

fn unknown_input() -> Error {
    Error::new("unknown", "Zed turn delivery or settlement is unknown")
}

pub(crate) async fn wait_for_fault(stream: &mut watch::Receiver<StreamState>) {
    loop {
        if matches!(
            *stream.borrow_and_update(),
            StreamState::Overflow | StreamState::Malformed(_)
        ) {
            return;
        }
        if stream.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}
