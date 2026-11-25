use super::{Principal, Router, require_owner};
use eliot_core::{
    Agent, Command, CommandState, Error, FailureOrigin, Result,
    api::{
        UPDATE_HEALTH_MAX_SUBJECTS, UpdateHealthRequest, UpdateHealthResponse, UpdateProbeKind,
        UpdateStartEvidence, UpdateStartState, UpdateStartSubject, UpdateStartSummary,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct BootHealth {
    finished: Option<bool>,
    requested: usize,
    attempts: Vec<UpdateStartEvidence>,
    summary: UpdateStartSummary,
    pending: HashMap<(String, i64), UpdateStartState>,
}

fn adjust(summary: &mut UpdateStartSummary, state: UpdateStartState, delta: i8) {
    let count = match state {
        UpdateStartState::Ready => &mut summary.ready,
        UpdateStartState::NotDispatched => &mut summary.not_dispatched,
        UpdateStartState::Refused => &mut summary.refused,
        UpdateStartState::Unknown => &mut summary.unknown,
    };
    if delta.is_positive() {
        *count += delta as usize;
    } else {
        *count = count.saturating_sub((-delta) as usize);
    }
}

fn subject(agent: &Agent, kind: UpdateProbeKind) -> Result<UpdateStartSubject> {
    let digest = eliot_core::canonical_json_sha256(&json!({
        "agent":agent.id, "generation":agent.generation, "harness": agent.harness,
        "kind":kind, "profile":agent.profile,
        "workspace":agent.workspace.as_deref().map(eliot_core::workspace_path_key), "native_scope":agent.native_scope, "native_ref":agent.native_ref,
    })).map_err(|_| Error::new("unknown", "update readiness subject cannot be bound"))?;
    Ok(UpdateStartSubject {
        agent: agent.id.clone(),
        generation: agent.generation,
        harness: agent.harness.clone(),
        kind,
        binding: eliot_core::api::inference::sha256_hex(&digest),
    })
}

fn sample_kind(agent: &Agent) -> UpdateProbeKind {
    if agent.native_ref.is_some() && agent.native_scope.is_some() {
        UpdateProbeKind::Resume
    } else {
        UpdateProbeKind::Launch
    }
}

impl Router {
    pub(super) async fn update_health(
        &self,
        params: Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: UpdateHealthRequest = eliot_core::api::discovery::decode_params(params)?;
        request.validate()?;
        let response = match request {
            UpdateHealthRequest::Inventory => {
                let (live, samples) = self.inner.store.lock().await.update_readiness_agents()?;
                UpdateHealthResponse::Inventory {
                    truncated: live.len() > UPDATE_HEALTH_MAX_SUBJECTS,
                    live: live
                        .iter()
                        .take(UPDATE_HEALTH_MAX_SUBJECTS)
                        .map(|agent| subject(agent, sample_kind(agent)))
                        .collect::<Result<_>>()?,
                    samples: samples
                        .iter()
                        .map(|agent| subject(agent, sample_kind(agent)))
                        .collect::<Result<_>>()?,
                }
            }
            UpdateHealthRequest::Boot => {
                let boot = self.inner.update_boot.lock_recover();
                UpdateHealthResponse::Boot {
                    finished: boot.finished.is_some(),
                    complete: boot.finished == Some(true),
                    requested: boot.requested,
                    attempts: boot.attempts.clone(),
                    summary: boot.summary.clone(),
                }
            }
            UpdateHealthRequest::AdapterProbe { subject: expected } => {
                let agent = self.inner.store.lock().await.get_agent(&expected.agent)?;
                let Some(agent) = agent else {
                    return Err(Error::new(
                        "stale_generation",
                        "update readiness Agent disappeared",
                    ));
                };
                if agent.observe_only
                    || agent.archived
                    || subject(&agent, expected.kind)? != expected
                {
                    return Err(Error::new(
                        "stale_generation",
                        "update readiness subject changed",
                    ));
                }
                let (state, code, origin) =
                    match self.probe_retained_start(agent, expected.kind).await {
                        Ok(()) => (UpdateStartState::Ready, None, None),
                        Err(error) => {
                            let state = if matches!(
                                error.code.as_str(),
                                "unknown" | "unavailable" | "timeout" | "unsupported"
                            ) {
                                UpdateStartState::Unknown
                            } else {
                                UpdateStartState::Refused
                            };
                            (state, Some(error.code), error.origin)
                        }
                    };
                UpdateHealthResponse::AdapterProbe {
                    evidence: UpdateStartEvidence {
                        subject: expected,
                        state,
                        code,
                        origin,
                        command_id: None,
                    },
                }
            }
        };
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "update health cannot be encoded"))
    }

    pub(super) async fn note_update_boot_request(&self, id: &str, generation: i64) {
        let subject = self
            .inner
            .store
            .lock()
            .await
            .get_agent(id)
            .ok()
            .flatten()
            .filter(|agent| agent.generation == generation)
            .and_then(|agent| subject(&agent, sample_kind(&agent)).ok());
        let mut boot = self.inner.update_boot.lock_recover();
        if boot.finished.is_some() {
            return;
        }
        boot.requested += 1;
        let key = (id.to_owned(), generation);
        boot.pending.insert(key, UpdateStartState::Unknown);
        adjust(&mut boot.summary, UpdateStartState::Unknown, 1);
        if let Some(subject) = subject
            && boot.attempts.len() < UPDATE_HEALTH_MAX_SUBJECTS
        {
            boot.attempts.push(UpdateStartEvidence {
                subject,
                state: UpdateStartState::Unknown,
                code: None,
                origin: None,
                command_id: None,
            });
        }
    }

    pub(super) fn note_update_boot_refusal(&self, agent: &str, generation: i64, error: &Error) {
        let mut boot = self.inner.update_boot.lock_recover();
        if boot.finished.is_some()
            || boot.pending.get(&(agent.to_owned(), generation)).copied()
                != Some(UpdateStartState::Unknown)
        {
            return;
        }
        let state = if error.origin == Some(FailureOrigin::Swarm)
            && matches!(error.code.as_str(), "profile_error" | "invalid_settings")
        {
            UpdateStartState::Refused
        } else {
            UpdateStartState::NotDispatched
        };
        set_state(
            &mut boot,
            agent,
            generation,
            state,
            Some(error.code.clone()),
            error.origin,
            None,
        );
    }

    pub(super) fn note_update_boot_not_dispatched(&self, agent: &str, generation: i64) {
        let mut boot = self.inner.update_boot.lock_recover();
        if boot.finished.is_none()
            && boot.pending.get(&(agent.to_owned(), generation)).copied()
                == Some(UpdateStartState::Unknown)
        {
            set_state(
                &mut boot,
                agent,
                generation,
                UpdateStartState::NotDispatched,
                None,
                None,
                None,
            );
        }
    }

    pub(super) fn note_update_boot_command(&self, agent: &str, generation: i64, command: &Command) {
        let mut boot = self.inner.update_boot.lock_recover();
        if boot.finished.is_some() {
            return;
        }
        if !boot.pending.contains_key(&(agent.to_owned(), generation)) {
            return;
        }
        let state = match command.state {
            CommandState::Applied => UpdateStartState::Ready,
            CommandState::Rejected | CommandState::Failed => UpdateStartState::Refused,
            CommandState::Cancelled => UpdateStartState::NotDispatched,
            _ => UpdateStartState::Unknown,
        };
        let (mut code, origin) = command.outcome.as_ref().map_or((None, None), |outcome| {
            let code = outcome
                .get("code")
                .or_else(|| outcome.get("reason"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    (command.state == CommandState::Cancelled).then(|| "cancelled".to_owned())
                });
            let origin = outcome
                .get("origin")
                .cloned()
                .and_then(|value| serde_json::from_value::<FailureOrigin>(value).ok());
            (code, origin)
        });
        if code.is_none() && command.state == CommandState::Cancelled {
            code = Some("cancelled".to_owned());
        }
        set_state(
            &mut boot,
            agent,
            generation,
            state,
            code,
            origin,
            Some(command.id.clone()),
        );
    }

    pub(crate) fn finish_update_boot(&self, complete: bool) {
        let mut boot = self.inner.update_boot.lock_recover();
        boot.finished = Some(complete);
        boot.pending.clear();
    }
}

fn set_state(
    boot: &mut BootHealth,
    agent: &str,
    generation: i64,
    state: UpdateStartState,
    code: Option<String>,
    origin: Option<FailureOrigin>,
    command_id: Option<String>,
) {
    let key = (agent.to_owned(), generation);
    let Some(previous) = boot.pending.get_mut(&key) else {
        return;
    };
    adjust(&mut boot.summary, *previous, -1);
    adjust(&mut boot.summary, state, 1);
    *previous = state;
    if let Some(entry) = boot
        .attempts
        .iter_mut()
        .find(|entry| entry.subject.agent == agent && entry.subject.generation == generation)
    {
        entry.state = state;
        entry.code = code;
        entry.origin = origin;
        if command_id.is_some() {
            entry.command_id = command_id;
        }
    }
}
