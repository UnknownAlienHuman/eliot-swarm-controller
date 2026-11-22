//! One-call Agent child launch, first input, durable wait and compact result.

use super::{Principal, Router, hierarchy, principal_name};
use eliot_core::{
    Command, CommandState, Error, Result, SubmitCommand,
    api::{
        ChildRunChangedFile, ChildRunChangedFiles, ChildRunFact, ChildRunFacts, ChildRunGoalCheck,
        ChildRunResponse, ChildRunStatus, ChildRunWake, ClaimListRequest, CommandSubmitResponse,
        GitView, MAX_CHILD_RUN_CHANGED_FILES, MAX_CHILD_RUN_COMMITS, MAX_CHILD_RUN_FILE_PATH_BYTES,
        SwarmDigestRequest, SwarmDigestResponse, SwarmWaitRequest, SwarmWaitResponse,
        WaitEntryResult, advisory_claim_matches,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, HashMap};

const CHILD_RUN_CHANGED_FILES_READ: usize = MAX_CHILD_RUN_CHANGED_FILES + 1;

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
            request.wait_ms == Some(0),
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

        let facts = if hidden {
            ChildRunFacts::unobserved()
        } else {
            entry.facts.clone()
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

    /// Attach one bounded fact snapshot per visible settled Agent turn.
    /// Called only after a wait's final Store snapshot, never inside its poll
    /// loop; repeated targets for the same turn share the read.
    pub(super) async fn attach_wait_facts(
        &self,
        results: &mut [WaitEntryResult],
        principal: &Principal,
    ) {
        let mut by_turn: HashMap<(String, i64, Option<String>), ChildRunFacts> = HashMap::new();
        for entry in results {
            if !matches!(
                entry.status.as_str(),
                "succeeded" | "failed" | "interrupted"
            ) {
                continue;
            }
            let (Some(agent), Some(generation)) = (entry.agent.as_deref(), entry.generation) else {
                continue;
            };
            let key = (agent.to_owned(), generation, entry.turn_id.clone());
            let facts = if let Some(facts) = by_turn.get(&key) {
                facts.clone()
            } else {
                let facts = self
                    .child_run_facts(agent, generation, entry.turn_id.as_deref(), principal)
                    .await;
                by_turn.insert(key, facts.clone());
                facts
            };
            entry.facts = facts;
        }
    }

    /// Read generation-bound edit and Goal facts alongside one cached Git
    /// observation and its live claims. Missing, stale or partial evidence is
    /// left explicitly unobserved.
    async fn child_run_facts(
        &self,
        agent: &str,
        generation: i64,
        turn_id: Option<&str>,
        principal: &Principal,
    ) -> ChildRunFacts {
        let mut facts = ChildRunFacts::unobserved();
        let caller = principal_name(principal);
        let captured = {
            let store = self.inner.store.lock().await;
            if hierarchy::authorize_agent_read(&store, principal, agent, Some(generation)).is_err()
            {
                return facts;
            }
            let Ok(Some(target)) = store.get_agent(agent) else {
                return facts;
            };
            if target.generation != generation {
                return facts;
            }
            if target.parent.is_none() {
                return facts;
            }
            let edits = store
                .list_agent_edits(agent, generation, None, CHILD_RUN_CHANGED_FILES_READ)
                .ok();
            let goal = store.goal_get(agent, "eliot").ok().flatten();
            let claims = store
                .claim_list(
                    &caller,
                    &ClaimListRequest {
                        project: None,
                        agent: Some(agent.to_owned()),
                    },
                    super::now_ms(),
                )
                .ok();
            let exclusively_bound = !target.state.is_terminal()
                && target.workspace.as_deref().is_some_and(|workspace| {
                    store
                        .workspace_owner_counts()
                        .ok()
                        .and_then(|counts| counts.get(workspace).copied())
                        == Some(1)
                });
            let commits = exclusively_bound
                .then(|| store.observed_git_commits(agent, generation, MAX_CHILD_RUN_COMMITS + 1))
                .and_then(Result::ok);
            let settled_at = turn_id
                .and_then(|turn| store.get_turn(agent, generation, turn).ok().flatten())
                .and_then(|turn| turn.completed_at);
            (
                target,
                edits,
                goal,
                claims,
                exclusively_bound,
                commits,
                settled_at,
            )
        };
        let (target, edits, goal, claims, exclusively_bound, commits, settled_at) = captured;
        if let Some(edits) = edits.as_ref().filter(|edits| !edits.is_empty()) {
            facts.changed_files = observed_changes(
                edits
                    .iter()
                    .map(|edit| (edit.path.as_str(), edit.updated_at)),
            );
        }
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

        let active_claims = claims
            .as_ref()
            .filter(|claims| !claims.truncated && !claims.claims.is_empty());
        if let Some(claims) = active_claims
            && edits.as_ref().is_some_and(|edits| {
                edits.iter().any(|edit| {
                    claims
                        .claims
                        .iter()
                        .all(|claim| !advisory_claim_matches(&claim.glob, &edit.path))
                })
            })
        {
            // An attributed mismatch proves an outside edit even when the
            // shared workspace prevents a complete false/true workspace claim.
            facts.outside_claims = ChildRunFact::Observed(true);
        }

        if exclusively_bound
            && let Some(settled_at) = settled_at
            && let Ok(GitView::Data(view)) = self.inner.monitoring.git_view(&target).await
            && view.available
            && view
                .observed_at
                .is_some_and(|observed_at| observed_at >= settled_at)
        {
            if let Some(counts) = view.line_counts {
                facts.line_counts = ChildRunFact::Observed(counts);
            }
            if view.history_complete == Some(true)
                && let Some(commits) = commits
                && commits.len() <= MAX_CHILD_RUN_COMMITS
            {
                facts.commits = ChildRunFact::Observed(commits);
            }
            if view.complete == Some(true)
                && view.base_complete == Some(true)
                && view.base_oid.is_some()
                && let (Some(dirty), Some(committed_paths), Some(observed_at)) = (
                    view.dirty.as_ref(),
                    view.committed_paths.as_ref(),
                    view.observed_at,
                )
            {
                let mut paths: BTreeSet<&str> =
                    dirty.iter().map(|file| file.path.as_str()).collect();
                paths.extend(committed_paths.iter().map(String::as_str));
                facts.changed_files =
                    observed_changes(paths.iter().map(|path| (*path, observed_at)));
                if let Some(claims) = active_claims {
                    let outside = facts.outside_claims == ChildRunFact::Observed(true)
                        || paths.iter().any(|path| {
                            claims
                                .claims
                                .iter()
                                .all(|claim| !advisory_claim_matches(&claim.glob, path))
                        });
                    facts.outside_claims = ChildRunFact::Observed(outside);
                }
            }
        }
        facts
    }
}

fn observed_changes<'a>(
    mut paths: impl Iterator<Item = (&'a str, i64)>,
) -> ChildRunFact<ChildRunChangedFiles> {
    let mut files = Vec::new();
    let mut truncated = false;
    for (path, observed_at) in paths.by_ref().take(MAX_CHILD_RUN_CHANGED_FILES) {
        if path.trim().is_empty()
            || path.len() > MAX_CHILD_RUN_FILE_PATH_BYTES
            || path.chars().any(char::is_control)
        {
            truncated = true;
            continue;
        }
        files.push(ChildRunChangedFile {
            path: path.to_owned(),
            observed_at,
        });
    }
    truncated |= paths.next().is_some();
    ChildRunFact::Observed(ChildRunChangedFiles { files, truncated })
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
    let mut hash = Sha256::new();
    hash.update(b"child.run.digest");
    hash.update(original);
    // Keep the launch's admission timestamp and derive a separate, stable digest id.
    let mut bytes = original;
    bytes[6..].copy_from_slice(&hash.finalize()[..10]);
    if bytes == original {
        bytes[15] ^= 1;
    }
    Ok(ulid::Ulid::from_bytes(bytes).to_string())
}

fn encode_child_run(response: ChildRunResponse) -> Result<Value> {
    serde_json::to_value(response)
        .map_err(|_| Error::new("internal", "child.run response could not be encoded"))
}
