use super::{Principal, Router, require_owner};
use eliot_core::{
    Agent, Command, CommandState, Error, FailureOrigin, Result,
    api::{
        UPDATE_HEALTH_MAX_SUBJECTS, UpdateHealthRequest, UpdateHealthResponse, UpdateProbeKind,
        UpdateStartEvidence, UpdateStartState, UpdateStartSubject,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};

#[derive(Default)]
pub(super) struct BootHealth {
    finished: Option<bool>,
    requested: usize,
    attempts: Vec<UpdateStartEvidence>,
}

fn subject(agent: &Agent, kind: UpdateProbeKind) -> Result<UpdateStartSubject> {
    let digest = eliot_core::canonical_json_sha256(&json!({
        "agent":agent.id, "generation":agent.generation, "harness": agent.harness,
        "kind":kind, "profile":agent.profile_snapshot, "effective":agent.effective,
        "workspace":agent.workspace, "native_scope":agent.native_scope, "native_ref":agent.native_ref,
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
                    complete: boot.finished == Some(true) && boot.requested == boot.attempts.len(),
                    requested: boot.requested,
                    attempts: boot.attempts.clone(),
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
            .and_then(|agent| subject(&agent, UpdateProbeKind::Resume).ok());
        let mut boot = self.inner.update_boot.lock_recover();
        if boot.finished.is_some() {
            return;
        }
        boot.requested += 1;
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
        if boot.finished.is_none()
            && let Some(entry) = boot.attempts.iter_mut().find(|e| {
                e.subject.agent == agent
                    && e.subject.generation == generation
                    && e.command_id.is_none()
            })
        {
            entry.state = UpdateStartState::Refused;
            entry.code = Some(error.code.clone());
            entry.origin = error.origin;
        }
    }

    pub(super) fn note_update_boot_command(&self, agent: &str, generation: i64, command: &Command) {
        let mut boot = self.inner.update_boot.lock_recover();
        if boot.finished.is_some() {
            return;
        }
        let Some(entry) = boot
            .attempts
            .iter_mut()
            .find(|e| e.subject.agent == agent && e.subject.generation == generation)
        else {
            return;
        };
        entry.command_id = Some(command.id.clone());
        entry.state = match command.state {
            CommandState::Applied => UpdateStartState::Ready,
            CommandState::Rejected | CommandState::Failed => UpdateStartState::Refused,
            _ => UpdateStartState::Unknown,
        };
        if let Some(outcome) = &command.outcome {
            entry.code = outcome
                .get("code")
                .and_then(Value::as_str)
                .map(str::to_owned);
            entry.origin = outcome
                .get("origin")
                .cloned()
                .and_then(|value| serde_json::from_value::<FailureOrigin>(value).ok());
        }
    }

    pub(crate) fn finish_update_boot(&self, complete: bool) {
        self.inner.update_boot.lock_recover().finished = Some(complete);
    }
}
