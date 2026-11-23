//! Owner resolution of unknown commands and turns.
//!
//! Both RPCs validate once through a shared request gate, then resolve
//! through the one Store writer. A replayed turn-resolution write id returns
//! its stored response before any plugin readback. Otherwise turn resolution
//! on a live generation first reads the exact native turn back through the
//! owned plugin; only a readback proving the turn inactive becomes a
//! resolution proof. Native I/O runs outside the Store lock; the resolving
//! transaction re-checks the stored terminality and the proof binding.
use super::{Principal, Router, contains_known_secret_including_keys, require_owner};
use crate::{common::now_ms, router::SecretSet};
use eliot_core::{
    Agent, Error, Result,
    api::{CommandResolveRequest, SettleStoppedRequest, TurnResolveRequest},
};
use eliot_store::TurnInactiveProof;
use serde::de::DeserializeOwned;
use serde_json::Value;

/// Authorize, secret-scan, parse, and validate one resolution request: the
/// single validation gate for both resolution RPCs.
fn decode_resolve_request<T>(
    params: &Value,
    principal: &Principal,
    secrets: &SecretSet,
    _method: &str,
    noun: &str,
    validate: fn(&T) -> Result<()>,
) -> Result<T>
where
    T: DeserializeOwned + schemars::JsonSchema,
{
    require_owner(principal)?;
    if contains_known_secret_including_keys(params, secrets) {
        return Err(Error::new(
            "secret_in_payload",
            format!("{noun} contains a known credential"),
        ));
    }
    let request: T = eliot_core::api::discovery::decode_params(params.clone())?;
    validate(&request)?;
    Ok(request)
}

/// Interpret one `agent.snapshot` response as proof that the exact requested
/// turn is inactive: the snapshot must bind this Agent generation, name the
/// turn as its last turn with `terminal: true` and a terminal status, and not
/// report it as still active. Anything else — including a contradictory
/// readback, a failed readback, or an unknown snapshot — proves nothing.
fn turn_inactive_proof(
    snapshot: &Value,
    agent_id: &str,
    generation: i64,
    turn_id: &str,
) -> Option<TurnInactiveProof> {
    if snapshot.get("agent_id").and_then(Value::as_str) != Some(agent_id) {
        return None;
    }
    if snapshot.get("generation").and_then(Value::as_i64) != Some(generation) {
        return None;
    }
    let last_turn = snapshot.get("last_turn")?;
    if last_turn.get("turn_id").and_then(Value::as_str) != Some(turn_id) {
        return None;
    }
    // Explicit terminality only: the flag and the status must agree that
    // the turn is terminal. A contradictory readback — terminal:false with a
    // terminal status, or terminal:true with a running status — proves
    // nothing, and neither does a terminal status without the explicit flag.
    let status_terminal = matches!(
        last_turn.get("status").and_then(Value::as_str),
        Some("completed" | "failed" | "interrupted")
    );
    let flag_terminal = last_turn.get("terminal").and_then(Value::as_bool) == Some(true);
    if !(status_terminal && flag_terminal) {
        return None;
    }
    if snapshot.get("active_turn").and_then(Value::as_str) == Some(turn_id) {
        return None;
    }
    Some(TurnInactiveProof {
        turn_id: turn_id.to_owned(),
    })
}

impl Router {
    pub(super) async fn command_settle_stopped(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request = decode_resolve_request(
            params,
            principal,
            &self.inner.secrets,
            "command.settle_stopped",
            "stop settlement",
            SettleStoppedRequest::validate,
        )?;
        let change = {
            let mut store = self.inner.store.lock().await;
            super::owner_inbox::check_binding(&store)?;
            let change = store.settle_stopped(&request, now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        if change.value.applied {
            self.signal_progress();
        }
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "stop settlement could not be encoded"))
    }

    pub(super) async fn command_resolve(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: CommandResolveRequest = decode_resolve_request(
            params,
            principal,
            &self.inner.secrets,
            "command.resolve",
            "command resolution",
            CommandResolveRequest::validate,
        )?;
        let command = {
            let store = self.inner.store.lock().await;
            if let Some(value) = store.stored_command_resolution("owner", &request)? {
                return serde_json::to_value(value).map_err(|_| {
                    Error::new("internal", "command resolution could not be encoded")
                });
            }
            store.get_command(&request.command_id)?
        };
        if request.resolution == eliot_core::api::CommandResolution::Failed
            && let Some(command) = command.as_ref().filter(|command| {
                command.verb == "launch"
                    && command.generation == Some(0)
                    && command.state == eliot_core::CommandState::Unknown
                    && command
                        .outcome
                        .as_ref()
                        .is_some_and(|outcome| outcome["stage"] == "workspace")
            })
        {
            let target = command
                .target
                .as_deref()
                .ok_or_else(|| Error::new("bad_state", "managed launch has no Agent"))?;
            let preparation = self
                .inner
                .store
                .lock()
                .await
                .get_workspace(target)?
                .ok_or_else(|| {
                    Error::new("bad_state", "managed launch has no workspace to read back")
                })?;
            let value = self
                .inner
                .workspaces
                .resolve_failed_launch(&request, &preparation)
                .await?;
            self.signal_progress();
            return serde_json::to_value(value)
                .map_err(|_| Error::new("internal", "command resolution could not be encoded"));
        }
        let live_agent = if request.resolution == eliot_core::api::CommandResolution::Applied {
            let store = self.inner.store.lock().await;
            match command {
                Some(command)
                    if command.verb == "configure"
                        && command.state == eliot_core::CommandState::Unknown =>
                {
                    match command.target.as_deref() {
                        Some(target) => store.get_agent(target)?.filter(|agent| {
                            agent.harness == "muse"
                                && Some(agent.generation) == command.generation
                                && agent.state == eliot_core::AgentState::Idle
                                && eliot_core::api::configure_uses_native_session(
                                    &agent.harness,
                                    &command.args,
                                )
                        }),
                        None => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        };
        let readback = match live_agent {
            Some(agent) => self.configure_readback(&agent).await,
            None => None,
        };
        let change = {
            let mut store = self.inner.store.lock().await;
            super::owner_inbox::check_binding(&store)?;
            let change = store.resolve_command_with_readback(
                "owner",
                &request,
                readback.as_ref(),
                now_ms(),
            )?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "command resolution could not be encoded"))
    }

    async fn configure_readback(
        &self,
        agent: &eliot_core::Agent,
    ) -> Option<eliot_store::ConfigureReadback> {
        let snapshot = self
            .inner
            .plugin_host
            .probe_snapshot(agent, None)
            .await
            .ok()?;
        if snapshot["agent_id"].as_str() != Some(agent.id.as_str())
            || snapshot["generation"].as_i64() != Some(agent.generation)
            || snapshot["state"].as_str() != Some("idle")
            || !snapshot.get("active_turn").is_some_and(Value::is_null)
        {
            return None;
        }
        Some(eliot_store::ConfigureReadback {
            agent: agent.id.clone(),
            generation: agent.generation,
            native_ref: snapshot["native_ref"].as_str()?.to_owned(),
            model: snapshot["native_model"].as_str()?.to_owned(),
        })
    }

    pub(super) async fn turn_resolve(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: TurnResolveRequest = decode_resolve_request(
            params,
            principal,
            &self.inner.secrets,
            "turn.resolve",
            "turn resolution",
            TurnResolveRequest::validate,
        )?;
        // Replay first: a write id already bound to this exact assertion
        // returns its stored response without any plugin readback. A
        // conflicting binding fails here exactly as the resolving
        // transaction would report it.
        let replay = {
            let store = self.inner.store.lock().await;
            store.stored_turn_resolution("owner", &request)?
        };
        if let Some(value) = replay {
            self.signal_progress();
            return serde_json::to_value(value)
                .map_err(|_| Error::new("internal", "turn resolution could not be encoded"));
        }
        // Read the stored generation and the readback binding first; the
        // resolving transaction below re-checks terminality and the proof.
        let (agent, readback) = {
            let store = self.inner.store.lock().await;
            let agent = store.get_agent(&request.agent)?;
            let readback = match &agent {
                Some(agent)
                    if !agent.state.is_terminal() && agent.generation == request.generation =>
                {
                    let bound = store.turn_readback_command(
                        &request.agent,
                        request.generation,
                        &request.turn_id,
                    )?;
                    if let Some(command_id) = bound {
                        Some((command_id, false))
                    } else if store
                        .get_turn(&request.agent, request.generation, &request.turn_id)?
                        .is_some_and(|turn| turn.native_trigger.is_some())
                    {
                        self.inner
                            .plugin_host
                            .session_start_command(agent)
                            .map(|id| (id, true))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            (agent, readback)
        };
        let proof = match (agent, readback) {
            (Some(agent), Some((command_id, self_started))) => {
                self.snapshot_turn_proof(&agent, &command_id, &request.turn_id, self_started)
                    .await
            }
            _ => None,
        };
        let change = {
            let mut store = self.inner.store.lock().await;
            super::owner_inbox::check_binding(&store)?;
            let change = store.resolve_turn("owner", &request, proof.as_ref(), now_ms())?;
            self.publish_locked(&change.events);
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "turn resolution could not be encoded"))
    }

    /// Prove one turn inactive through the owned plugin. Any readback
    /// failure — no runtime, timeout, unknown snapshot — yields no proof,
    /// and the Store then refuses the live generation with `bad_state`.
    async fn snapshot_turn_proof(
        &self,
        agent: &Agent,
        command_id: &str,
        turn_id: &str,
        self_started: bool,
    ) -> Option<TurnInactiveProof> {
        let snapshot = self
            .inner
            .plugin_host
            .snapshot_turn(agent, command_id, turn_id)
            .await
            .ok()?;
        if snapshot["command_id"].as_str() != Some(command_id)
            || (self_started && !snapshot["last_turn"]["command_id"].is_null())
        {
            return None;
        }
        turn_inactive_proof(&snapshot, &agent.id, agent.generation, turn_id)
    }
}
