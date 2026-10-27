use crate::{
    bus::Bus,
    plugin_host::{AgentCredentials, PluginHost, SecretSet, UnknownCommands},
};
use eliot_core::{
    Agent, AgentState, Command, CommandState, Error, Event, Result, SubmitCommand, Turn, TurnState,
};
use eliot_plugin::MAX_EVENT_BYTES;
use futures_util::future::BoxFuture;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex as AsyncMutex, broadcast, watch},
    time::{Instant, timeout_at},
};

const PAGE_MAX: usize = 1_000;
const EVENT_SCAN_MAX: usize = 1_000;
const EVENT_RESPONSE_MAX_BYTES: usize = 900 * 1024;
const EVENT_RESPONSE_WRAPPER_RESERVE_BYTES: usize = 4 * 1024;
const EVENT_RESPONSE_ENTRIES_MAX_BYTES: usize =
    EVENT_RESPONSE_MAX_BYTES - EVENT_RESPONSE_WRAPPER_RESERVE_BYTES;
const LIVE_WAIT_MAX_MS: u64 = 30_000;
const STATUS_BLOCKER_MAX: usize = 64;

type StoreHandle = Arc<AsyncMutex<eliot_store::Store>>;

const MODE_OPEN: u8 = 0;
const MODE_DRAIN: u8 = 1;
const MODE_FORCE: u8 = 2;

#[derive(Clone)]
pub(crate) struct Router {
    inner: Arc<RouterInner>,
}

struct RouterInner {
    store: StoreHandle,
    bus: Bus,
    owner_token: String,
    secrets: SecretSet,
    agent_credentials: AgentCredentials,
    unknown_commands: UnknownCommands,
    unknown_causes: Mutex<HashMap<String, String>>,
    plugin_host: PluginHost,
    mode: AtomicU8,
    shutdown_ready: AtomicBool,
    progress: watch::Sender<u64>,
}

#[derive(Clone)]
enum Principal {
    Owner,
    Agent { id: String, generation: i64 },
}

struct EventFilter {
    agents: HashSet<String>,
    kinds: HashSet<String>,
    sources: HashSet<String>,
}

impl Router {
    pub(crate) fn new(
        data_root: std::path::PathBuf,
        owner_token: String,
        store: StoreHandle,
        bus: Bus,
    ) -> Self {
        let secrets: SecretSet = Arc::new(Mutex::new(HashSet::from([owner_token.clone()])));
        let agent_credentials: AgentCredentials = Arc::new(Mutex::new(Default::default()));
        let unknown_commands: UnknownCommands = Arc::new(Mutex::new(Default::default()));
        let (progress, _) = watch::channel(0_u64);
        let plugin_host = PluginHost::new(
            data_root,
            Arc::clone(&store),
            bus.clone(),
            Arc::clone(&secrets),
            Arc::clone(&agent_credentials),
            Arc::clone(&unknown_commands),
            progress.clone(),
        );
        Self {
            inner: Arc::new(RouterInner {
                store,
                bus,
                owner_token,
                secrets,
                agent_credentials,
                unknown_commands,
                unknown_causes: Mutex::new(HashMap::new()),
                plugin_host,
                mode: AtomicU8::new(MODE_OPEN),
                shutdown_ready: AtomicBool::new(false),
                progress,
            }),
        }
    }

    pub(crate) async fn handle(&self, method: &str, wire_params: Value) -> Result<Value> {
        let (principal, params) = self.authenticate(wire_params).await?;
        match method {
            "daemon.status" => {
                require_owner(&principal)?;
                reject_unknown_keys(&params, &[], method)?;
                self.status().await
            }
            "daemon.shutdown" => {
                require_owner(&principal)?;
                self.shutdown(&params, &principal).await
            }
            "command.submit" => self.submit(&params, &principal).await,
            "command.get" => self.command_get(&params, &principal).await,
            "agent.get" => self.agent_get(&params, &principal).await,
            "agent.list" => self.agent_list(&params, &principal).await,
            "agent.result" => self.agent_result(&params, &principal).await,
            "turn.get" => self.turn_get(&params, &principal).await,
            "events.read" => self.events_read(&params, &principal).await,
            "events.subscribe" => self.events_subscribe(&params, &principal).await,
            _ => Err(Error::new(
                "unsupported",
                format!("daemon method {method} is not implemented in L1"),
            )),
        }
    }

    pub(crate) fn progress_receiver(&self) -> watch::Receiver<u64> {
        self.inner.progress.subscribe()
    }

    pub(crate) fn response_finished(&self, shutdown_requested: bool) {
        if shutdown_requested && self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            self.inner.shutdown_ready.store(true, Ordering::SeqCst);
        }
        self.signal_progress();
    }

    pub(crate) fn request_force(&self) {
        self.inner.mode.store(MODE_FORCE, Ordering::SeqCst);
        self.inner.shutdown_ready.store(true, Ordering::SeqCst);
        self.signal_progress();
    }

    pub(crate) fn is_force_requested(&self) -> bool {
        self.inner.mode.load(Ordering::SeqCst) == MODE_FORCE
    }

    pub(crate) async fn should_exit(&self) -> Result<bool> {
        match self.inner.mode.load(Ordering::SeqCst) {
            MODE_FORCE => Ok(self.inner.shutdown_ready.load(Ordering::SeqCst)),
            MODE_DRAIN if self.inner.shutdown_ready.load(Ordering::SeqCst) => {
                Ok(self.status_blockers().await?.is_empty())
            }
            MODE_DRAIN => Ok(false),
            _ => Ok(false),
        }
    }

    pub(crate) async fn force_owned_processes(&self) {
        self.inner.plugin_host.force_all().await;
    }

    pub(crate) fn note_events(&self, events: &[Event]) {
        note_unknown(&self.inner.unknown_commands, events);
        self.signal_progress();
    }

    fn signal_progress(&self) {
        self.inner.progress.send_modify(|value| {
            *value = value.wrapping_add(1);
        });
    }

    async fn authenticate(&self, wire_params: Value) -> Result<(Principal, Value)> {
        let mut envelope = wire_params.as_object().cloned().ok_or_else(|| {
            Error::new(
                "invalid",
                "daemon params must contain an authentication envelope",
            )
        })?;
        let auth = envelope
            .remove("_auth")
            .ok_or_else(|| Error::new("not_allowed", "daemon call is missing credentials"))?;
        let payload = envelope
            .remove("_payload")
            .ok_or_else(|| Error::new("invalid", "daemon call is missing its payload"))?;
        if !envelope.is_empty() || !payload.is_object() {
            return Err(Error::new("invalid", "daemon call envelope is malformed"));
        }
        let auth = auth
            .as_object()
            .ok_or_else(|| Error::new("not_allowed", "daemon credentials are malformed"))?;
        let kind = auth.get("kind").and_then(Value::as_str).unwrap_or_default();
        let credential = auth
            .get("credential")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if credential.is_empty() {
            return Err(Error::new(
                "not_allowed",
                "daemon credentials are incomplete",
            ));
        }
        match kind {
            "owner" => {
                if secure_eq(credential, &self.inner.owner_token) {
                    Ok((Principal::Owner, payload))
                } else {
                    Err(Error::new("not_allowed", "owner credential is invalid"))
                }
            }
            "agent" => {
                let id = auth
                    .get("agent_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let generation = auth
                    .get("generation")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| Error::new("not_allowed", "Agent generation is missing"))?;
                if !self.inner.plugin_host.controls_generation(id, generation) {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation has no live plugin owner",
                    ));
                }
                let stored_token = {
                    let store = self.inner.store.lock().await;
                    store.get_agent_token(id, generation)?
                };
                if !stored_token
                    .as_deref()
                    .is_some_and(|known| secure_eq(credential, known))
                {
                    return Err(Error::new(
                        "not_allowed",
                        "Agent credential is invalid or revoked",
                    ));
                }
                let agent = {
                    let store = self.inner.store.lock().await;
                    store.get_agent(id)?
                }
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
                if agent.generation != generation || agent.is_terminal() {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent credential does not match a live generation",
                    ));
                }
                if !self.inner.plugin_host.controls_generation(id, generation) {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation lost its live plugin owner during authentication",
                    ));
                }
                {
                    self.inner
                        .agent_credentials
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert((id.to_owned(), generation), credential.to_owned());
                    self.inner
                        .secrets
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(credential.to_owned());
                }
                Ok((
                    Principal::Agent {
                        id: id.to_owned(),
                        generation,
                    },
                    payload,
                ))
            }
            _ => Err(Error::new("not_allowed", "daemon principal is unsupported")),
        }
    }

    async fn submit(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &[
                "id",
                "target",
                "verb",
                "args",
                "expected_generation",
                "expected_turn_id",
            ],
            "command.submit",
        )?;
        let mut request: SubmitCommand = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "command.submit params are malformed"))?;
        authorize_command(principal, &mut request)?;
        let mode = self.inner.mode.load(Ordering::SeqCst);
        if mode == MODE_FORCE {
            return Err(Error::new("unavailable", "daemon is forcing shutdown"));
        }
        if matches!(request.verb.as_str(), "launch" | "send") && mode != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is draining and rejects new launch or send commands",
            ));
        }
        if request.verb == "launch" {
            normalize_launch(&mut request)?;
        }
        if contains_known_secret(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "durable command text contains a known credential",
            ));
        }
        if matches!(request.verb.as_str(), "launch" | "send")
            && request
                .args
                .get("text")
                .and_then(Value::as_str)
                .is_some_and(|text| text.len() > MAX_EVENT_BYTES)
        {
            return Err(Error::new(
                "frame_too_large",
                "fake echo input exceeds the 64 KiB persisted-text limit",
            ));
        }

        let admission = {
            let mut store = self.inner.store.lock().await;
            let admission = store.submit(&principal_name(principal), request, now_ms())?;
            self.publish_locked(&admission.events);
            admission
        };
        note_unknown(&self.inner.unknown_commands, &admission.events);
        if admission.command.state == CommandState::Unknown {
            self.inner
                .unknown_commands
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(admission.command.id.clone());
        }

        if admission.is_new {
            let command_id = admission.command.id.clone();
            let linked_id = admission
                .linked_command
                .as_ref()
                .map(|command| command.id.clone());
            let router = self.clone();
            tokio::spawn(router.process_command(command_id, linked_id));
        }
        self.signal_progress();
        Ok(submit_result(
            &admission.command,
            admission.linked_command.as_ref(),
        ))
    }

    async fn command_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["id"], "command.get")?;
        let id = required_text(params, "id")?;
        let mut command = {
            let store = self.inner.store.lock().await;
            store.get_command(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        authorize_command_read(principal, &command)?;
        if command.state == CommandState::Unknown {
            command.turn_id = None;
        } else if let Some(cause) = self.unknown_cause(id) {
            command.state = CommandState::Unknown;
            command.outcome = Some(json!({"code": "unknown", "cause": cause}));
            command.turn_id = None;
        }
        serde_json::to_value(command)
            .map_err(|error| Error::new("internal", format!("encode command: {error}")))
    }

    async fn agent_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["id"], "agent.get")?;
        let id = required_text(params, "id")?;
        authorize_agent_read(principal, id, None)?;
        let agent = {
            let store = self.inner.store.lock().await;
            store.get_agent(id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
        if matches!(
            agent.state,
            AgentState::Idle | AgentState::Busy | AgentState::Waiting
        ) && !self
            .inner
            .plugin_host
            .controls_generation(&agent.id, agent.generation)
        {
            return Err(Error::new(
                "unknown",
                "persisted Agent state has no live source owner",
            ));
        }
        serde_json::to_value(agent)
            .map_err(|error| Error::new("internal", format!("encode Agent: {error}")))
    }

    async fn agent_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["after", "limit"], "agent.list")?;
        let after = optional_text(params, "after")?;
        let limit = page_limit(params)?;
        let agents = {
            let store = self.inner.store.lock().await;
            match principal {
                Principal::Owner => store.list_agents(after, limit)?,
                Principal::Agent { id, .. } => {
                    store.get_agent(id)?.into_iter().collect::<Vec<Agent>>()
                }
            }
        };
        let next = match principal {
            Principal::Owner => agents.last().map(|agent| Value::String(agent.id.clone())),
            Principal::Agent { .. } => None,
        };
        Ok(json!({
            "agents": agents,
            "next": next,
        }))
    }

    async fn agent_result(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &["id", "generation", "turn_id", "limit"],
            "agent.result",
        )?;
        let id = required_text(params, "id")?;
        authorize_agent_read(
            principal,
            id,
            params.get("generation").and_then(Value::as_i64),
        )?;
        let (generation, mut turns) = {
            let store = self.inner.store.lock().await;
            let agent = store
                .get_agent(id)?
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
            let generation = params
                .get("generation")
                .and_then(Value::as_i64)
                .unwrap_or(agent.generation);
            let turns = if let Some(turn_id) = params.get("turn_id").and_then(Value::as_str) {
                vec![store.get_turn(id, generation, turn_id)?.ok_or_else(|| {
                    Error::new("not_found", format!("turn {turn_id} was not found"))
                })?]
            } else {
                store.list_turns(id, generation, page_limit(params)?)?
            };
            (generation, turns)
        };
        self.mask_unknown_turns(id, generation, &mut turns).await?;
        Ok(json!({
            "agent": id,
            "generation": generation,
            "turns": turns,
        }))
    }

    async fn turn_get(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["agent", "generation", "turn_id"], "turn.get")?;
        let agent = required_text(params, "agent")?;
        let generation = required_i64(params, "generation")?;
        authorize_agent_read(principal, agent, Some(generation))?;
        let turn_id = required_text(params, "turn_id")?;
        let mut turn = {
            let store = self.inner.store.lock().await;
            store.get_turn(agent, generation, turn_id)?
        }
        .ok_or_else(|| Error::new("not_found", format!("turn {turn_id} was not found")))?;
        self.mask_unknown_turns(agent, generation, std::slice::from_mut(&mut turn))
            .await?;
        serde_json::to_value(turn)
            .map_err(|error| Error::new("internal", format!("encode turn: {error}")))
    }

    async fn events_read(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(params, &["after", "limit", "filter"], "events.read")?;
        let after = params.get("after").and_then(Value::as_i64).unwrap_or(0);
        let limit = page_limit(params)?;
        let filter = parse_filter(params.get("filter"), principal)?;
        let (events, next, high_water, truncated, _) = self
            .read_filtered(
                after,
                limit,
                None,
                EVENT_RESPONSE_ENTRIES_MAX_BYTES,
                &filter,
            )
            .await?;
        Ok(json!({
            "events": events,
            "next": next,
            "high_water": high_water,
            "truncated": truncated,
        }))
    }

    async fn events_subscribe(&self, params: &Value, principal: &Principal) -> Result<Value> {
        reject_unknown_keys(
            params,
            &["after", "limit", "wait_ms", "filter"],
            "events.subscribe",
        )?;
        let after = params.get("after").and_then(Value::as_i64).unwrap_or(0);
        if after < 0 {
            return Err(Error::new("invalid", "event cursor must be nonnegative"));
        }
        let limit = page_limit(params)?;
        let wait_ms = match params.get("wait_ms") {
            None => 0,
            Some(value) => value
                .as_u64()
                .ok_or_else(|| Error::new("invalid", "wait_ms must be a nonnegative integer"))?,
        }
        .min(LIVE_WAIT_MAX_MS);
        let filter = parse_filter(params.get("filter"), principal)?;

        // Subscribe before reading high-water so commits are either replayed or buffered.
        let mut receiver = self.inner.bus.subscribe();
        let (mut events, mut cursor, mut high_water, mut truncated, mut encoded_bytes) = self
            .read_filtered(
                after,
                limit,
                None,
                EVENT_RESPONSE_ENTRIES_MAX_BYTES,
                &filter,
            )
            .await?;
        let mut lagged = false;

        if wait_ms > 0 && events.len() < limit && !truncated {
            let deadline = Instant::now() + Duration::from_millis(wait_ms);
            while events.len() < limit && !truncated {
                match timeout_at(deadline, receiver.recv()).await {
                    Ok(Ok(event)) => {
                        if event.seq <= cursor {
                            continue;
                        }
                        let remaining = limit - events.len();
                        let remaining_bytes =
                            EVENT_RESPONSE_ENTRIES_MAX_BYTES.saturating_sub(encoded_bytes);
                        let replay = self
                            .read_filtered(
                                cursor,
                                remaining,
                                Some(event.seq),
                                remaining_bytes,
                                &filter,
                            )
                            .await?;
                        events.extend(replay.0);
                        cursor = replay.1;
                        high_water = high_water.max(event.seq).max(replay.2);
                        encoded_bytes = encoded_bytes.saturating_add(replay.4);
                        truncated |= replay.3;
                    }
                    Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                        lagged = true;
                        let remaining = limit - events.len();
                        let remaining_bytes =
                            EVENT_RESPONSE_ENTRIES_MAX_BYTES.saturating_sub(encoded_bytes);
                        let replay = self
                            .read_filtered(cursor, remaining, None, remaining_bytes, &filter)
                            .await?;
                        events.extend(replay.0);
                        cursor = replay.1;
                        high_water = high_water.max(replay.2);
                        truncated |= replay.3;
                        break;
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => {
                        return Err(Error::new("unavailable", "event bus is closed"));
                    }
                    Err(_) => break,
                }
            }
        }
        Ok(json!({
            "events": events,
            "next": cursor,
            "high_water": high_water,
            "lagged": lagged,
            "truncated": truncated,
        }))
    }

    async fn read_filtered(
        &self,
        after: i64,
        limit: usize,
        through: Option<i64>,
        byte_budget: usize,
        filter: &EventFilter,
    ) -> Result<(Vec<Event>, i64, i64, bool, usize)> {
        if after < 0 || limit == 0 || limit > PAGE_MAX {
            return Err(Error::new("invalid", "invalid event query bounds"));
        }
        let byte_budget = byte_budget.min(EVENT_RESPONSE_ENTRIES_MAX_BYTES);
        let (events, cursor, high_water, encoded_bytes) = {
            let store = self.inner.store.lock().await;
            let high_water = match through {
                Some(bound) => bound,
                None => store.high_water()?,
            };
            if high_water < after {
                return Err(Error::new(
                    "invalid",
                    "event cursor is above the high-water mark",
                ));
            }
            let mut cursor = after;
            let mut scanned = 0;
            let mut events = Vec::new();
            let mut encoded_bytes = 0_usize;
            let mut byte_limited = false;
            while cursor < high_water && events.len() < limit && scanned < EVENT_SCAN_MAX {
                let page_limit = (EVENT_SCAN_MAX - scanned).clamp(1, 256);
                let page = store.read_events(cursor, page_limit, Some(high_water))?;
                if page.is_empty() {
                    cursor = high_water;
                    break;
                }
                for event in &page {
                    scanned += 1;
                    if filter.matches(event) {
                        let cost = encoded_event_cost(event)?;
                        if encoded_bytes.saturating_add(cost) > byte_budget {
                            byte_limited = true;
                            break;
                        }
                        encoded_bytes += cost;
                        events.push(event.clone());
                    }
                    cursor = event.seq;
                    if events.len() == limit {
                        break;
                    }
                }
                if byte_limited || events.len() == limit {
                    break;
                }
            }
            (events, cursor, high_water, encoded_bytes)
        };
        let truncated = cursor < high_water;
        Ok((events, cursor, high_water, truncated, encoded_bytes))
    }

    async fn status(&self) -> Result<Value> {
        let blockers = self.status_blockers().await?;
        let mode = self.mode_name();
        let (agent_count, write_faulted, unsettled_commands) = {
            let store = self.inner.store.lock().await;
            (
                store.agent_count()?,
                store.is_write_faulted(),
                store.list_unsettled_commands(STATUS_BLOCKER_MAX)?,
            )
        };
        let mut unknown_commands = self.unknown_ids();
        let unknown_commands_count = unknown_commands.len();
        let unknown_commands_truncated = unknown_commands.len() > STATUS_BLOCKER_MAX;
        unknown_commands.truncate(STATUS_BLOCKER_MAX);
        let unsettled_commands_truncated = unsettled_commands.len() == STATUS_BLOCKER_MAX;
        let unsettled_command_details = unsettled_commands
            .iter()
            .map(|command| {
                json!({
                    "id": command.id,
                    "agent": command.target,
                    "generation": command.generation,
                    "verb": command.verb,
                    "state": command.state,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "state": if mode == "open" { "running" } else { mode },
            "admission": mode,
            "agents": agent_count,
            "store_write_faulted": write_faulted,
            "owned_plugins": self.inner.plugin_host.live_count(),
            "unknown_commands_count": unknown_commands_count,
            "unknown_commands": unknown_commands,
            "unknown_commands_truncated": unknown_commands_truncated,
            "unsettled_commands_count": unsettled_commands.len(),
            "unsettled_commands": unsettled_command_details,
            "unsettled_commands_truncated": unsettled_commands_truncated,
            "blockers": blockers,
        }))
    }

    async fn status_blockers(&self) -> Result<Vec<Value>> {
        let (agents, unsettled_commands, write_faulted) = {
            let store = self.inner.store.lock().await;
            (
                store.list_nonterminal_agents(STATUS_BLOCKER_MAX)?,
                store.list_unsettled_commands(STATUS_BLOCKER_MAX)?,
                store.is_write_faulted(),
            )
        };
        let mut blockers = Vec::new();
        for agent in agents {
            blockers.push(json!({
                "kind": "agent_live",
                "agent": agent.id,
                "generation": agent.generation,
                "state": agent.state,
            }));
        }
        let mut unsettled_ids = HashSet::new();
        for command in unsettled_commands {
            unsettled_ids.insert(command.id.clone());
            blockers.push(json!({
                "kind": "command_unsettled",
                "command_id": command.id,
                "state": command.state,
            }));
        }
        if write_faulted {
            blockers.push(json!({"kind": "store_write_faulted"}));
        }
        let unknown_ids = self.unknown_ids();
        for id in unknown_ids.iter().take(STATUS_BLOCKER_MAX) {
            if unsettled_ids.contains(id) {
                continue;
            }
            blockers.push(json!({"kind": "command_unknown", "command_id": id}));
        }
        if unknown_ids.len() > STATUS_BLOCKER_MAX {
            blockers.push(json!({
                "kind": "command_unknown_more",
                "count": unknown_ids.len() - STATUS_BLOCKER_MAX,
            }));
        }
        Ok(blockers)
    }

    async fn shutdown(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        reject_unknown_keys(params, &["id", "mode"], "daemon.shutdown")?;
        let mode = required_text(params, "mode")?;
        if mode != "drain" && mode != "force" {
            return Err(Error::new(
                "invalid",
                "daemon.shutdown mode must be drain or force",
            ));
        }
        let id = required_text(params, "id")?;
        let result = if mode == "force" {
            json!({"state": "dispatched", "mode": "force"})
        } else {
            let blockers = self.status_blockers().await?;
            json!({
                "state": if blockers.is_empty() { "applied" } else { "dispatched" },
                "mode": "drain",
                "blockers": blockers,
            })
        };
        let write = {
            let mut store = self.inner.store.lock().await;
            match store.record_write(
                id,
                &principal_name(principal),
                "daemon.shutdown",
                &json!({"mode": mode}),
                &result,
                now_ms(),
            ) {
                Ok(write) => Some(write.result),
                Err(_error) if mode == "force" && store.is_write_faulted() => None,
                Err(error) => return Err(error),
            }
        };
        let response = write.unwrap_or_else(|| {
            json!({
                "state": "dispatched",
                "mode": "force",
                "store_write_faulted": true,
            })
        });
        self.inner.mode.store(
            if mode == "force" {
                MODE_FORCE
            } else {
                MODE_DRAIN
            },
            Ordering::SeqCst,
        );
        self.inner.shutdown_ready.store(false, Ordering::SeqCst);
        self.signal_progress();
        Ok(response)
    }

    fn process_command(self, id: String, linked_id: Option<String>) -> BoxFuture<'static, ()> {
        Box::pin(self.process_command_inner(id, linked_id))
    }

    async fn process_command_inner(self, id: String, linked_id: Option<String>) {
        let mut receiver = self.inner.bus.subscribe();
        let dispatched = loop {
            let change = {
                let mut store = self.inner.store.lock().await;
                let result = match store.dispatch(&id, now_ms()) {
                    Ok(change) => {
                        self.publish_locked(&change.events);
                        Ok(change)
                    }
                    Err(error) => Err(error),
                };
                (result, store.is_write_faulted())
            };
            match change.0 {
                Ok(change) if change.value.state == CommandState::Queued => {
                    let target = change.value.target.clone();
                    let generation = change.value.generation;
                    loop {
                        match receiver.recv().await {
                            Ok(event)
                                if event.agent == target
                                    && event.generation == generation
                                    && (event.kind.starts_with("turn.")
                                        || event.kind.starts_with("session.")
                                        || event.kind.starts_with("request.")) =>
                            {
                                break;
                            }
                            Ok(_) => continue,
                            Err(broadcast::error::RecvError::Lagged(_)) => break,
                            Err(broadcast::error::RecvError::Closed) => {
                                self.reject_undispatched(&id, "event bus closed").await;
                                return;
                            }
                        }
                    }
                }
                Ok(change) => break change.value,
                Err(error) => {
                    if change.1 {
                        self.note_unknown_id(&id, "store_dispatch_outcome_unknown");
                    } else if matches!(
                        error.code.as_str(),
                        "bad_state" | "stale_generation" | "not_found"
                    ) {
                        self.reject_undispatched(&id, &error.code).await;
                    }
                    self.signal_progress();
                    return;
                }
            }
        };

        let target = match dispatched.target.as_deref() {
            Some(target) => target.to_owned(),
            None => return,
        };
        let agent = {
            let store = self.inner.store.lock().await;
            match store.get_agent(&target) {
                Ok(Some(agent)) => agent,
                _ => return,
            }
        };

        let _result = match dispatched.verb.as_str() {
            "launch" => {
                let start = self
                    .inner
                    .plugin_host
                    .start(agent, dispatched.clone())
                    .await;
                match start {
                    Ok(value) => {
                        let _ = self
                            .settle(&id, CommandState::Applied, Some(json!({"state": "ready"})))
                            .await;
                        if let Some(linked_id) = linked_id {
                            tokio::spawn(self.clone().process_command(linked_id, None));
                        }
                        Ok(value)
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            "send" => {
                let response = self
                    .inner
                    .plugin_host
                    .send(&agent, dispatched.clone())
                    .await;
                match response {
                    Ok(value) => {
                        let expected_turn = value
                            .get("turn_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        let current = {
                            let store = self.inner.store.lock().await;
                            store.get_command(&id)
                        };
                        match current {
                            Ok(Some(command))
                                if command.state == CommandState::Applied
                                    && command.turn_id.as_deref() == Some(expected_turn) =>
                            {
                                Ok(value)
                            }
                            Ok(Some(command)) if command.state == CommandState::Unknown => {
                                Err(Error::new(
                                    "unknown",
                                    "plugin event batch did not establish a committed terminal turn",
                                ))
                            }
                            Ok(Some(_)) => {
                                let error = Error::new(
                                    "unknown",
                                    "plugin returned without committed turn association",
                                );
                                self.settle_plugin_error(&id, &error).await;
                                Err(error)
                            }
                            Ok(None) => Err(Error::new("not_found", "command disappeared")),
                            Err(error) => Err(error),
                        }
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            "stop" => {
                let force = dispatched
                    .args
                    .get("force")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                match self
                    .inner
                    .plugin_host
                    .stop(&agent, dispatched.clone(), force)
                    .await
                {
                    Ok(value) => {
                        let summary = json!({
                            "state": "applied",
                            "custody": value.get("custody").cloned().unwrap_or(Value::Null),
                            "graceful": value.get("graceful").cloned().unwrap_or(Value::Bool(false)),
                        });
                        let _ = self.settle(&id, CommandState::Applied, Some(summary)).await;
                        Ok(value)
                    }
                    Err(error) => {
                        self.settle_plugin_error(&id, &error).await;
                        Err(error)
                    }
                }
            }
            _ => Err(Error::new("unsupported", "command verb is not implemented")),
        };
        self.signal_progress();
    }

    async fn reject_undispatched(&self, id: &str, reason: &str) {
        let outcome = json!({"reason": reason});
        let _ = self.settle(id, CommandState::Rejected, Some(outcome)).await;
    }

    async fn settle_plugin_error(&self, id: &str, error: &Error) {
        let current = {
            let store = self.inner.store.lock().await;
            store.get_command(id)
        };
        let (current_state, command_target) = match current {
            Ok(Some(command)) => (Some(command.state), command.target.zip(command.generation)),
            _ => (None, None),
        };
        let late_batch_cause = command_target.as_ref().and_then(|(agent, generation)| {
            self.inner
                .plugin_host
                .take_late_batch_cause(agent, *generation)
        });
        if current_state.is_some_and(|state| state.is_terminal() || state == CommandState::Unknown)
        {
            return;
        }
        let state = match error.code.as_str() {
            "unknown" => CommandState::Unknown,
            "invalid" | "unsupported" | "not_allowed" | "not_found" | "bad_state"
            | "stale_generation" | "source_integrity" | "frame_too_large" | "secret_in_payload" => {
                CommandState::Rejected
            }
            _ => CommandState::Failed,
        };
        let cause = if state == CommandState::Unknown {
            late_batch_cause.unwrap_or_else(|| unknown_cause(error))
        } else {
            String::new()
        };
        let outcome = if state == CommandState::Unknown {
            json!({"code": error.code, "cause": cause})
        } else {
            json!({"code": error.code})
        };
        let _ = self.settle(id, state, Some(outcome)).await;
    }

    async fn settle(
        &self,
        id: &str,
        state: CommandState,
        outcome: Option<Value>,
    ) -> Option<Command> {
        let persistence_cause = outcome
            .as_ref()
            .and_then(|value| value.get("cause"))
            .and_then(Value::as_str)
            .unwrap_or("store_settlement_failed")
            .to_owned();
        let (result, write_faulted) = {
            let mut store = self.inner.store.lock().await;
            let result = store.settle(id, state, outcome, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            (result, store.is_write_faulted())
        };
        match result {
            Ok(change) => {
                self.note_unknown(&change.events);
                if change.value.state.is_terminal()
                    && let (Some(agent), Some(generation)) =
                        (change.value.target.as_deref(), change.value.generation)
                {
                    self.forget_token_if_revoked(agent, generation).await;
                }
                let command = change.value;
                self.signal_progress();
                Some(command)
            }
            Err(_) => {
                if write_faulted {
                    self.note_unknown_id(id, &persistence_cause);
                }
                self.signal_progress();
                None
            }
        }
    }

    async fn forget_token_if_revoked(&self, agent: &str, generation: i64) {
        let token = {
            let store = self.inner.store.lock().await;
            store.get_agent_token(agent, generation)
        };
        if matches!(token, Ok(None)) {
            crate::plugin_host::forget_revoked_agent_token(
                &self.inner.agent_credentials,
                &self.inner.secrets,
                &(agent.to_owned(), generation),
            );
        }
    }

    fn publish_locked(&self, events: &[Event]) {
        self.inner.bus.publish(events);
        self.note_unknown(events);
    }

    fn note_unknown(&self, events: &[Event]) {
        note_unknown(&self.inner.unknown_commands, events);
    }

    fn note_unknown_id(&self, id: &str, cause: &str) {
        self.inner
            .unknown_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_owned());
        self.inner
            .unknown_causes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id.to_owned(), cause.to_owned());
    }

    fn unknown_cause(&self, id: &str) -> Option<String> {
        self.inner
            .unknown_causes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    async fn mask_unknown_turns(
        &self,
        agent: &str,
        generation: i64,
        turns: &mut [Turn],
    ) -> Result<()> {
        let memory_unknown = self.unknown_ids().into_iter().collect::<HashSet<String>>();
        let (associated, maybe_more) = {
            let store = self.inner.store.lock().await;
            let commands = store.list_unsettled_commands(STATUS_BLOCKER_MAX)?;
            let maybe_more = commands.len() == STATUS_BLOCKER_MAX;
            let associated = commands
                .into_iter()
                .filter(|command| {
                    command.state == CommandState::Unknown || memory_unknown.contains(&command.id)
                })
                .filter(|command| {
                    command.target.as_deref() == Some(agent)
                        && command.generation == Some(generation)
                })
                .filter_map(|command| command.turn_id)
                .collect::<HashSet<_>>();
            (associated, maybe_more)
        };
        let found_exact_turn = turns.iter().any(|turn| associated.contains(&turn.turn_id));
        if maybe_more && !found_exact_turn {
            return Err(Error::new(
                "unavailable",
                "unsettled-command view is truncated; turn completion cannot be confirmed",
            ));
        }
        for turn in turns {
            if associated.contains(&turn.turn_id) {
                turn.state = TurnState::Unknown;
                turn.status = None;
            }
        }
        Ok(())
    }

    fn unknown_ids(&self) -> Vec<String> {
        let mut ids: Vec<_> = self
            .inner
            .unknown_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .cloned()
            .collect();
        ids.sort();
        ids
    }

    fn mode_name(&self) -> &'static str {
        match self.inner.mode.load(Ordering::SeqCst) {
            MODE_DRAIN => "draining",
            MODE_FORCE => "forcing",
            _ => "open",
        }
    }
}

impl EventFilter {
    fn matches(&self, event: &Event) -> bool {
        (self.agents.is_empty()
            || event
                .agent
                .as_ref()
                .is_some_and(|agent| self.agents.contains(agent)))
            && (self.kinds.is_empty() || self.kinds.contains(&event.kind))
            && (self.sources.is_empty() || self.sources.contains(&event.source))
    }
}

fn encoded_event_cost(event: &Event) -> Result<usize> {
    let encoded = serde_json::to_vec(event)
        .map_err(|error| Error::new("internal", format!("encode event page entry: {error}")))?;
    Ok(encoded.len().saturating_add(1))
}

fn normalize_launch(request: &mut SubmitCommand) -> Result<()> {
    if request.verb != "launch" {
        return Ok(());
    }
    let workspace = request
        .args
        .get("workspace")
        .and_then(Value::as_str)
        .filter(|workspace| !workspace.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", "launch requires a workspace path"))?;
    let workspace = std::fs::canonicalize(workspace)
        .map_err(|_| Error::new("invalid", "launch workspace must be an existing directory"))?;
    if !workspace.is_dir() {
        return Err(Error::new(
            "invalid",
            "launch workspace must be a directory",
        ));
    }
    let workspace = workspace
        .to_str()
        .ok_or_else(|| Error::new("invalid", "workspace path is not valid UTF-8"))?
        .to_owned();
    let args = request
        .args
        .as_object_mut()
        .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?;
    args.insert("workspace".to_owned(), Value::String(workspace.clone()));
    args.insert("project".to_owned(), Value::String(workspace));
    Ok(())
}

fn authorize_command(principal: &Principal, request: &mut SubmitCommand) -> Result<()> {
    let Principal::Agent { id, generation } = principal else {
        return Ok(());
    };
    if !matches!(request.verb.as_str(), "send" | "stop") {
        return Err(Error::new(
            "not_allowed",
            "managed Agent credentials cannot launch another Agent",
        ));
    }
    if request.target.as_deref().is_some_and(|target| target != id) {
        return Err(Error::new(
            "not_allowed",
            "managed Agent credentials are scoped to their own Agent",
        ));
    }
    request.target = Some(id.clone());
    if request
        .expected_generation
        .is_some_and(|expected| expected != *generation)
    {
        return Err(Error::new(
            "stale_generation",
            "command generation does not match the caller",
        ));
    }
    request.expected_generation = Some(*generation);
    Ok(())
}

fn authorize_command_read(principal: &Principal, command: &Command) -> Result<()> {
    if let Principal::Agent { id, generation } = principal
        && (command.target.as_deref() != Some(id.as_str())
            || command
                .generation
                .is_some_and(|actual| actual != *generation))
    {
        return Err(Error::new(
            "not_allowed",
            "command is outside the caller's scope",
        ));
    }
    Ok(())
}

fn authorize_agent_read(principal: &Principal, id: &str, generation: Option<i64>) -> Result<()> {
    if let Principal::Agent {
        id: own_id,
        generation: own_generation,
    } = principal
        && (id != own_id || generation.is_some_and(|actual| actual != *own_generation))
    {
        return Err(Error::new(
            "not_allowed",
            "Agent read is outside the caller's scope",
        ));
    }
    Ok(())
}

fn require_owner(principal: &Principal) -> Result<()> {
    if matches!(principal, Principal::Owner) {
        Ok(())
    } else {
        Err(Error::new(
            "not_allowed",
            "method requires the owner principal",
        ))
    }
}

fn principal_name(principal: &Principal) -> String {
    match principal {
        Principal::Owner => "owner".to_owned(),
        Principal::Agent { id, generation } => format!("agent:{id}:{generation}"),
    }
}

fn submit_result(command: &Command, linked: Option<&Command>) -> Value {
    json!({
        "command_id": command.id,
        "agent": command.target,
        "generation": command.generation,
        "state": command.state,
        "outcome": command.outcome,
        "linked_command": linked.map(|command| command.id.as_str()),
    })
}

fn parse_filter(raw: Option<&Value>, principal: &Principal) -> Result<EventFilter> {
    let mut filter = EventFilter {
        agents: HashSet::new(),
        kinds: HashSet::new(),
        sources: HashSet::new(),
    };
    if let Some(raw) = raw {
        let object = raw
            .as_object()
            .ok_or_else(|| Error::new("invalid", "event filter must be an object"))?;
        reject_unknown_map_keys(object, &["agents", "kinds", "sources"], "event filter")?;
        filter.agents = string_set(object.get("agents"), "agents")?;
        filter.kinds = string_set(object.get("kinds"), "kinds")?;
        filter.sources = string_set(object.get("sources"), "sources")?;
    }
    if let Principal::Agent { id, .. } = principal
        && !filter.agents.is_empty()
        && !filter.agents.contains(id)
    {
        return Err(Error::new(
            "not_allowed",
            "event filter is outside the caller's scope",
        ));
    }
    if let Principal::Agent { id, .. } = principal {
        filter.agents.clear();
        filter.agents.insert(id.clone());
    }
    Ok(filter)
}

fn string_set(value: Option<&Value>, field: &str) -> Result<HashSet<String>> {
    let Some(value) = value else {
        return Ok(HashSet::new());
    };
    let array = value
        .as_array()
        .ok_or_else(|| Error::new("invalid", format!("event filter {field} must be an array")))?;
    let mut result = HashSet::new();
    if array.len() > PAGE_MAX {
        return Err(Error::new(
            "frame_too_large",
            format!("event filter {field} has too many values"),
        ));
    }
    for item in array {
        let text = item
            .as_str()
            .filter(|text| !text.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    format!("event filter {field} values must be non-empty strings"),
                )
            })?;
        result.insert(text.to_owned());
    }
    Ok(result)
}

fn contains_known_secret(value: &Value, secrets: &SecretSet) -> bool {
    let secrets = secrets
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    contains_secret_value(value, &secrets)
}

fn contains_secret_value(value: &Value, secrets: &HashSet<String>) -> bool {
    match value {
        Value::String(text) => secrets
            .iter()
            .any(|secret| !secret.is_empty() && text.contains(secret)),
        Value::Array(items) => items
            .iter()
            .any(|item| contains_secret_value(item, secrets)),
        Value::Object(object) => object
            .values()
            .any(|item| contains_secret_value(item, secrets)),
        _ => false,
    }
}

fn note_unknown(unknown: &UnknownCommands, events: &[Event]) {
    let mut unknown = unknown
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for event in events
        .iter()
        .filter(|event| event.kind == "command.unknown")
    {
        if let Some(id) = event.data.get("command_id").and_then(Value::as_str) {
            unknown.insert(id.to_owned());
        }
    }
}

fn required_text<'a>(params: &'a Value, field: &str) -> Result<&'a str> {
    params
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", format!("{field} must be a non-empty string")))
}

fn optional_text<'a>(params: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match params.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) if !text.trim().is_empty() => Ok(Some(text)),
        _ => Err(Error::new(
            "invalid",
            format!("{field} must be a non-empty string"),
        )),
    }
}

fn required_i64(params: &Value, field: &str) -> Result<i64> {
    params
        .get(field)
        .and_then(Value::as_i64)
        .filter(|value| *value >= 0)
        .ok_or_else(|| Error::new("invalid", format!("{field} must be a nonnegative integer")))
}

fn page_limit(params: &Value) -> Result<usize> {
    let limit = match params.get("limit") {
        None => 100,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| Error::new("invalid", "limit must be a positive integer"))?,
    };
    if limit == 0 || limit > PAGE_MAX as u64 {
        return Err(Error::new("invalid", "limit must be between 1 and 1000"));
    }
    Ok(limit as usize)
}

fn reject_unknown_keys(params: &Value, allowed: &[&str], method: &str) -> Result<()> {
    let object = params
        .as_object()
        .ok_or_else(|| Error::new("invalid", format!("{method} params must be an object")))?;
    reject_unknown_map_keys(object, allowed, method)
}

fn reject_unknown_map_keys(
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
    method: &str,
) -> Result<()> {
    if let Some(key) = object.keys().find(|key| !allowed.contains(&key.as_str())) {
        Err(Error::new(
            "unsupported",
            format!("{method} field {key} is not implemented in L1"),
        ))
    } else {
        Ok(())
    }
}

fn secure_eq(left: &str, right: &str) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.bytes()
        .zip(right.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn unknown_cause(error: &Error) -> String {
    if let Some(cause) = error.message.strip_prefix("events.put outcome unknown: ")
        && !cause.is_empty()
        && cause
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return cause.chars().take(64).collect();
    }
    if error.message.contains("timed out") {
        "plugin_rpc_timeout".to_owned()
    } else if error.message.contains("closed") {
        "plugin_connection_lost".to_owned()
    } else {
        "plugin_outcome_unknown".to_owned()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
