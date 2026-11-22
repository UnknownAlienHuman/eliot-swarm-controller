//! One-call Agent child launch, first input, durable wait and compact result.

use super::{Principal, Router, hierarchy, principal_name};
use eliot_core::{
    Command, CommandState, Error, Result, SubmitCommand,
    api::{
        AgentEditsResponse, ChildRunChangedFile, ChildRunChangedFiles, ChildRunFact, ChildRunFacts,
        ChildRunGoalCheck, ChildRunResponse, ChildRunStatus, ChildRunWake, CommandSubmitResponse,
        SwarmDigestRequest, SwarmDigestResponse, SwarmWaitRequest, SwarmWaitResponse,
        WaitEntryResult,
    },
};
use serde_json::{Value, json};

const CHILD_RUN_CHANGED_FILES_MAX: usize = 64;
const CHILD_RUN_CHANGED_FILES_READ: usize = CHILD_RUN_CHANGED_FILES_MAX + 1;

impl Router {
    pub(super) async fn child_run(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: eliot_core::api::ChildRunRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        if request.is_continuation() {
            self.continue_child_run(&request, principal).await
        } else {
            self.start_child_run(&request, principal).await
        }
    }

    async fn start_child_run(
        &self,
        request: &eliot_core::api::ChildRunRequest,
        principal: &Principal,
    ) -> Result<Value> {
        let (parent, _) = require_agent(principal)?;
        let text = request
            .text
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "child.run start requires text"))?;

        let mut args = json!({
            "parent": parent,
            "text": text,
        });
        if let Some(profile) = request.profile.as_deref() {
            args["profile"] = json!(profile);
        }
        if let Some(sources) = request.sources.as_ref() {
            args["sources"] = json!(sources);
        }

        // The caller's mutation id is the ordinary launch id. Existing command
        // admission therefore owns idempotency and retains the linked first
        // SEND in the same Store transaction as child creation.
        let mut launch = SubmitCommand::new(request.id.clone(), "launch");
        launch.args = args;
        let params = serde_json::to_value(launch)
            .map_err(|_| Error::new("internal", "child.run launch could not be encoded"))?;
        let submitted: CommandSubmitResponse =
            serde_json::from_value(self.submit_bound(&params, principal, None).await?)
                .map_err(|_| Error::new("internal", "child.run launch reply was invalid"))?;
        if submitted.command_id != request.id {
            return Err(Error::new(
                "unknown",
                "child.run launch reply did not match its retained id",
            ));
        }

        let (launch, linked) = self.load_child_run_commands(&request.id, principal).await?;
        if linked.is_none() && launch.state == CommandState::Applied {
            return Err(Error::new(
                "source_integrity",
                "child.run launch applied without its linked first input",
            ));
        }
        if linked.is_none() && !launch.state.is_terminal() && launch.state != CommandState::Unknown
        {
            return Err(Error::new(
                "unknown",
                "child.run launch has no retained first input; retry with the same id",
            ));
        }
        self.child_run_response(
            &request.id,
            &launch,
            linked.as_ref(),
            principal,
            request.effective_wait_ms(),
            request.wait_ms == Some(0),
        )
        .await
    }

    async fn continue_child_run(
        &self,
        request: &eliot_core::api::ChildRunRequest,
        principal: &Principal,
    ) -> Result<Value> {
        let (launch, linked) = self.load_child_run_commands(&request.id, principal).await?;
        if linked.is_none() && launch.state == CommandState::Applied {
            return Err(Error::new(
                "source_integrity",
                "child.run launch applied without its linked first input",
            ));
        }
        if linked.is_none() && !launch.state.is_terminal() && launch.state != CommandState::Unknown
        {
            return Err(Error::new(
                "unknown",
                "child.run first input is not yet retained; retry with the same id",
            ));
        }
        self.child_run_response(
            &request.id,
            &launch,
            linked.as_ref(),
            principal,
            request.effective_wait_ms(),
            false,
        )
        .await
    }

    /// Read only a launch created by this exact Agent principal generation.
    /// The Store owns the linked SEND identity and its generation binding.
    async fn load_child_run_commands(
        &self,
        id: &str,
        principal: &Principal,
    ) -> Result<(Command, Option<Command>)> {
        let (caller, _) = require_agent(principal)?;
        let expected_principal = principal_name(principal);
        let store = self.inner.store.lock().await;
        store.ensure_committed_reads()?;
        let launch = store
            .get_command(id)?
            .ok_or_else(|| Error::new("not_found", "child.run operation was not found"))?;
        if launch.verb != "launch"
            || launch.target.is_none()
            || launch.principal != expected_principal
            || launch.args.get("parent").and_then(Value::as_str) != Some(caller)
        {
            return Err(Error::new("not_found", "child.run operation was not found"));
        }

        let linked = store.get_linked_send(id)?;
        if let Some(send) = &linked
            && (send.id != format!("{id}/send")
                || send.verb != "send"
                || send.principal != launch.principal
                || send.target.is_none()
                || send.target != launch.target
                || send.generation.or(send.expected_generation) != Some(1))
        {
            return Err(Error::new(
                "source_integrity",
                "child.run linked first input does not match its launch",
            ));
        }
        Ok((launch, linked))
    }

    async fn child_run_response(
        &self,
        id: &str,
        launch: &Command,
        linked: Option<&Command>,
        principal: &Principal,
        wait_ms: u64,
        register_zero_wait: bool,
    ) -> Result<Value> {
        let Some(linked) = linked else {
            let response = ChildRunResponse {
                id: id.to_owned(),
                status: status_from_command(launch.state),
                agent: None,
                generation: None,
                turn_id: None,
                final_text: None,
                truncated: false,
                completion_wake: ChildRunWake::NotNeeded,
                facts: ChildRunFacts::unobserved(),
            };
            response.validate()?;
            return encode_child_run(response);
        };

        let wait_request =
            SwarmWaitRequest::new(vec![linked.id.clone()], Vec::new(), "all", wait_ms);
        let wait_params = serde_json::to_value(&wait_request)
            .map_err(|_| Error::new("internal", "child.run wait could not be encoded"))?;
        let wait_response: SwarmWaitResponse =
            serde_json::from_value(self.swarm_wait(&wait_params, principal).await?)
                .map_err(|_| Error::new("internal", "child.run wait reply was invalid"))?;
        wait_request
            .validate_response(&wait_response)
            .map_err(|_| Error::new("internal", "child.run wait reply did not match its input"))?;
        let entry = wait_response
            .results
            .first()
            .ok_or_else(|| Error::new("internal", "child.run wait returned no input result"))?;

        let mut status = status_from_wait(entry)?;
        if launch.state == CommandState::Unknown && status == ChildRunStatus::StillRunning {
            status = ChildRunStatus::Unknown;
        }
        let hidden = matches!(
            status,
            ChildRunStatus::NotFound | ChildRunStatus::NotVisible
        );
        let (agent, generation) = if hidden {
            (None, None)
        } else {
            let agent = linked.target.clone();
            let generation = linked.generation.or(linked.expected_generation);
            (agent, generation)
        };
        if let (Some(agent), Some(generation), Some(bound_agent), Some(bound_generation)) = (
            agent.as_deref(),
            generation,
            entry.agent.as_deref(),
            entry.generation,
        ) && (agent != bound_agent || generation != bound_generation)
        {
            return Err(Error::new(
                "source_integrity",
                "child.run wait resolved a different Agent generation",
            ));
        }
        let turn_id = if hidden { None } else { entry.turn_id.clone() };
        let final_text = if hidden {
            None
        } else {
            entry.final_text.clone()
        };
        let truncated = !hidden && entry.truncated;

        let facts = match (agent.as_deref(), generation) {
            (Some(agent), Some(generation)) if !hidden => {
                self.child_run_facts(agent, generation, principal).await
            }
            _ => ChildRunFacts::unobserved(),
        };

        let register_digest = register_zero_wait
            || matches!(
                status,
                ChildRunStatus::StillRunning | ChildRunStatus::Unknown
            );
        let completion_wake = self
            .child_run_completion_wake(id, linked, principal, register_digest)
            .await?;

        let response = ChildRunResponse {
            id: id.to_owned(),
            status,
            agent,
            generation,
            turn_id,
            final_text,
            truncated,
            completion_wake,
            facts,
        };
        response.validate()?;
        encode_child_run(response)
    }

    async fn child_run_completion_wake(
        &self,
        id: &str,
        linked: &Command,
        principal: &Principal,
        register: bool,
    ) -> Result<ChildRunWake> {
        let digest_id = child_run_digest_id(id)?;
        let exists = self
            .inner
            .store
            .lock()
            .await
            .get_completion_digest(&digest_id)?
            .is_some();
        if !register && !exists {
            return Ok(ChildRunWake::NotNeeded);
        }

        let (parent, _) = require_agent(principal)?;
        let request = SwarmDigestRequest::new(
            digest_id,
            parent,
            "all",
            vec![linked.id.clone()],
            Vec::new(),
        );
        let params = serde_json::to_value(&request)
            .map_err(|_| Error::new("internal", "child.run digest could not be encoded"))?;
        let response: SwarmDigestResponse =
            serde_json::from_value(self.swarm_digest(&params, principal).await?)
                .map_err(|_| Error::new("internal", "child.run digest reply was invalid"))?;
        request.validate_response(&response).map_err(|_| {
            Error::new("internal", "child.run digest reply did not match its input")
        })?;
        Ok(if response.state == "delivered" {
            ChildRunWake::Delivered
        } else {
            ChildRunWake::Pending
        })
    }

    /// Use exact-generation observed edits and Goal counters only. Git
    /// counters, commits and claim coverage describe the shared workspace or
    /// live-only claim rows, so those remain explicitly unobserved here.
    async fn child_run_facts(
        &self,
        agent: &str,
        generation: i64,
        principal: &Principal,
    ) -> ChildRunFacts {
        let mut facts = ChildRunFacts::unobserved();
        let edits_params = json!({
            "id": agent,
            "generation": generation,
            "limit": CHILD_RUN_CHANGED_FILES_READ,
        });
        if let Ok(value) = self.agent_edits(&edits_params, principal).await
            && let Ok(response) = serde_json::from_value::<AgentEditsResponse>(value)
            && response.agent == agent
            && response.generation == generation
            && !response.edits.is_empty()
        {
            let truncated = response.edits.len() > CHILD_RUN_CHANGED_FILES_MAX;
            let files = response
                .edits
                .into_iter()
                .take(CHILD_RUN_CHANGED_FILES_MAX)
                .map(|edit| ChildRunChangedFile {
                    path: edit.path,
                    observed_at: edit.updated_at,
                })
                .collect();
            facts.changed_files = ChildRunFact::Observed(ChildRunChangedFiles { files, truncated });
        }

        let goal = {
            let store = self.inner.store.lock().await;
            if hierarchy::authorize_agent_read(&store, principal, agent, Some(generation)).is_ok() {
                store.goal_get(agent, "eliot").ok().flatten()
            } else {
                None
            }
        };
        if let Some(goal) = goal
            && goal.agent == agent
            && goal.generation == generation
            && let (Some(exit_code), Some(checked_at)) =
                (goal.counters.last_exit_code, goal.counters.last_check_at)
        {
            facts.last_goal_check = ChildRunFact::Observed(ChildRunGoalCheck {
                exit_code,
                checked_at,
            });
        }
        facts
    }
}

fn require_agent(principal: &Principal) -> Result<(&str, i64)> {
    match principal {
        Principal::Agent { id, generation } => Ok((id.as_str(), *generation)),
        _ => Err(Error::new(
            "not_allowed",
            "child.run requires a current Agent principal",
        )),
    }
}

fn status_from_command(state: CommandState) -> ChildRunStatus {
    match state {
        CommandState::Applied => ChildRunStatus::Succeeded,
        CommandState::Rejected => ChildRunStatus::Rejected,
        CommandState::Failed => ChildRunStatus::Failed,
        CommandState::Unknown => ChildRunStatus::Unknown,
        CommandState::Cancelled => ChildRunStatus::Cancelled,
        CommandState::Accepted | CommandState::Queued | CommandState::Dispatched => {
            ChildRunStatus::StillRunning
        }
    }
}

fn status_from_wait(entry: &WaitEntryResult) -> Result<ChildRunStatus> {
    match entry.status.as_str() {
        "succeeded" => Ok(ChildRunStatus::Succeeded),
        "failed" => Ok(ChildRunStatus::Failed),
        "interrupted" => Ok(ChildRunStatus::Interrupted),
        "unknown" => Ok(ChildRunStatus::Unknown),
        "still_running" => Ok(ChildRunStatus::StillRunning),
        "not_found" => Ok(ChildRunStatus::NotFound),
        "not_visible" => Ok(ChildRunStatus::NotVisible),
        _ => Err(Error::new(
            "internal",
            "child.run received an unknown wait status",
        )),
    }
}

fn child_run_digest_id(id: &str) -> Result<String> {
    let value = ulid::Ulid::from_string(id).map_err(|_| Error::invalid_mutation_id())?;
    let original = value.to_bytes();
    let hash = eliot_core::canonical::digest(&json!({"child.run.digest": value.to_string()}))?;
    // Keep the launch's admission timestamp and derive a separate, stable digest id.
    let mut bytes = original;
    bytes[6..].copy_from_slice(&hash[..10]);
    if bytes == original {
        bytes[15] ^= 1;
    }
    Ok(ulid::Ulid::from_bytes(bytes).to_string())
}

fn encode_child_run(response: ChildRunResponse) -> Result<Value> {
    serde_json::to_value(response)
        .map_err(|_| Error::new("internal", "child.run response could not be encoded"))
}
