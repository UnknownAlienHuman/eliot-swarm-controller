//! One durable switch worker composes normal launch and H2 transfer.
use super::*;
use eliot_core::api::{SWITCH_BLANK_SCOPE_ARG, SWITCH_PROJECT_REVISION_ARG, SwitchRequest};

fn log_refusal(policy: &eliot_core::SecretPolicy, id: &str, code: &str, reason: &str) {
    let error = Error::new(code, reason).with_origin(eliot_core::FailureOrigin::Swarm, reason);
    crate::logging::record(policy, "switch.refused", &[("command", id)], Some(&error));
}

/// Whether a switch keeps its source checkout: it requests neither a
/// workspace override nor a new branch. Reuse is selected before any
/// preparation so the automatic worktree policy never mints (or fails to
/// mint) a fresh worktree the replacement would drop anyway.
fn reuse_source_checkout(launch: &eliot_core::api::SwitchLaunch) -> bool {
    launch.workspace.is_none() && launch.worktree.is_none()
}

/// Whether a switch with no workspace override reuses the retained
/// checkout: a freshly prepared worktree would leave the source's
/// uncommitted edits behind in the old directory. Only an automatically
/// minted worktree is dropped; an explicitly requested branch stays
/// prepared from the retained checkout (its `generated.branch` is set).
fn reuse_retained_checkout(
    requested_workspace: Option<&str>,
    prepared: &Option<WorkspacePreparationInput>,
) -> bool {
    requested_workspace.is_none()
        && prepared.as_ref().is_some_and(|workspace| {
            workspace
                .generated
                .as_ref()
                .is_some_and(|naming| naming.branch.is_none())
        })
}

impl Router {
    pub(crate) async fn run_provider_switches(
        &self,
        mut receiver: broadcast::Receiver<Event>,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                event = receiver.recv() => {
                    match event {
                        Ok(event) if event.kind == eliot_store::PROVIDER_SWITCH_QUEUED => {
                            if let Some(id) = event.data["switch_command"].as_str() {
                                self.schedule_command(id.to_owned(), None);
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            let mut after = String::new();
                            loop {
                                let switches = self.inner.store.lock().await.accepted_provider_switches(&after)?;
                                let Some(last) = switches.last() else {
                                    break;
                                };
                                after.clone_from(last);
                                for id in switches {
                                    self.schedule_command(id, None);
                                }
                                tokio::task::yield_now().await;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                        Ok(_) => {}
                    }
                }
            }
        }
    }

    pub(super) async fn swarm_switch(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: SwitchRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let change = {
            let mut store = self.inner.store.lock().await;
            if let Some(value) = store.replay_switch(&principal_name(principal), &request)? {
                return serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode switch replay"));
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(Error::new("unavailable", "drain prevents new switches"));
            }
            let change = store
                .reserve_switch(&principal_name(principal), &request, &policy, now_ms())
                .inspect_err(|error| {
                    log_refusal(&policy, &request.id, &error.code, &error.message);
                })?;
            self.publish_locked(&change.events);
            // A disconnected caller cannot cancel the independently scheduled operation.
            if !change.events.is_empty() {
                self.schedule_command(request.id.clone(), None);
            }
            change
        };
        self.signal_progress();
        self.signal_launch_arbitration();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "cannot encode switch"))
    }

    pub(super) async fn process_switch(&self, id: &str) {
        let work = {
            let mut store = self.inner.store.lock().await;
            match store.start_switch(id, now_ms()) {
                Ok(change) => {
                    self.publish_locked(&change.events);
                    change.value
                }
                Err(_) => None,
            }
        };
        let Some(eliot_store::SwitchWork {
            request,
            plan,
            provider_failure,
        }) = work
        else {
            return;
        };
        let prepared = async {
            self.check_command_admission_mode("launch")?;
            let source = {
                let store = self.inner.store.lock().await;
                store
                    .get_agent(&request.from)?
                    .ok_or_else(|| Error::new("not_found", "source Agent missing"))?
            };
            // Continue-in-project resolves the target checkout before any
            // preparation: an omitted workspace uses the target
            // registration's checkout, never the source repo. The
            // registration the worker verifies here is rechecked inside the
            // admission transaction, so a revocation between preparation and
            // admission refuses instead of dispatching under stale authority.
            let target = match request.launch.project.as_deref() {
                Some(project) => Some({
                    let store = self.inner.store.lock().await;
                    store
                        .get_project_registration("owner", project)?
                        .ok_or_else(|| {
                            Error::new(
                                "not_found",
                                format!("switch target project {project} was not found"),
                            )
                        })?
                }),
                None => None,
            };
            let mut launch = SubmitCommand::new(&plan.launch_command, "launch");
            let workspace = match (
                &target,
                request.launch.workspace.as_deref(),
                source.workspace.as_deref(),
            ) {
                (Some(_), Some(workspace), _) => workspace.to_owned(),
                (Some(registration), None, _) => registration.checkout.clone(),
                (None, workspace, retained) => workspace
                    .or(retained)
                    .ok_or_else(|| {
                        Error::new(
                            "invalid",
                            "switch requires a workspace when A has no retained workspace",
                        )
                    })?
                    .to_owned(),
            };
            launch.args = request.launch.args(&workspace)?;
            let provider_route = if let Some(failure) = provider_failure {
                let authority = failure.require_authority(&source.profile_snapshot)?;
                let (name, identity) = self
                    .route_provider_switch(&mut launch, &source, failure.kind, authority)
                    .await?;
                launch.args["profile"] = json!(name);
                Some((launch.args["profile_choice"].take(), identity))
            } else {
                None
            };
            if provider_route.is_some() {
                launch
                    .args
                    .as_object_mut()
                    .unwrap()
                    .remove("profile_choice");
            }
            // A targeted continuation explicitly requests standard worktree
            // preparation, even in a `shared` project: B never starts in the
            // checkout itself.
            if target.is_some() && request.launch.worktree.is_none() {
                launch.args["worktree"] = json!("");
            }
            let scope = source.project_scope().to_owned();
            // A switch that requests neither a workspace nor a branch keeps
            // the source checkout: reuse is selected before any preparation.
            // A targeted continue never reuses the source checkout: B always
            // runs from the target project through ordinary preparation.
            if target.is_none() && reuse_source_checkout(&request.launch) {
                launch.args["switch_reuse_source_checkout"] = json!(true);
            }
            if let Some(registration) = &target {
                let git = crate::workspace_git::WorkspaceGit::new()?;
                let (checkout, common_dir) = git.repository_identity(workspace.as_str()).await?;
                if checkout != registration.checkout || common_dir != registration.repo_common_dir {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace is outside the target project repository",
                    ));
                }
            }
            let mut prepared = self
                .prepare_submission_with_routing(
                    launch,
                    &Principal::Owner,
                    false,
                    false,
                    false,
                    true,
                )
                .await?;
            if let Some((choice, identity)) = provider_route {
                if routing::switch_account_identity(&prepared.request.args["profile_snapshot"])
                    != identity
                {
                    return Err(Error::new(
                        "bad_state",
                        "provider switch account changed during launch preparation",
                    ));
                }
                prepared.request.args["profile_choice"] = choice;
            }
            if target.is_none()
                && reuse_retained_checkout(request.launch.workspace.as_deref(), &prepared.workspace)
            {
                prepared.workspace = None;
                if let Some(args) = prepared.request.args.as_object_mut() {
                    args.remove("workspace_preparation");
                }
            }
            // The replacement retains the source's authorization namespace for handoff.
            prepared.request.args["project"] = json!(scope);
            if let Some(target) = &target {
                // The prepared launch must carry the resolved target
                // registration; an explicit workspace in another repository
                // resolves elsewhere and is refused here.
                if prepared
                    .request
                    .args
                    .get("registered_project")
                    .and_then(Value::as_str)
                    != Some(target.id.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch target project differs from prepared registration",
                    ));
                }
                if prepared
                    .request
                    .args
                    .get("workspace")
                    .and_then(Value::as_str)
                    != Some(target.checkout.as_str())
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace differs from its target registration",
                    ));
                }
                if let Some(workspace) = prepared.workspace.as_ref()
                    && (workspace.repo_path != target.checkout
                        || workspace.repo_common_dir != target.repo_common_dir
                        || workspace.project != target.checkout)
                {
                    return Err(Error::new(
                        "not_allowed",
                        "switch workspace preparation differs from its target registration",
                    ));
                }
                let current = {
                    let store = self.inner.store.lock().await;
                    store
                        .get_project_registration("owner", &target.id)?
                        .ok_or_else(|| {
                            Error::new(
                                "not_found",
                                format!("switch target project {} was not found", target.id),
                            )
                        })?
                };
                if current.revision != target.revision {
                    return Err(Error::new(
                        "stale_assignment",
                        "switch target registration changed",
                    ));
                }
                prepared.request.args["registered_project"] = json!(target.id);
                prepared.request.args[SWITCH_PROJECT_REVISION_ARG] = json!(target.revision);
            } else {
                prepared.request.args["registered_project"] = json!(source.project);
            }
            if target
                .as_ref()
                .is_none_or(|target| target.id == source.project)
                && let Some(reason) = &source.policy_opt_out_reason
            {
                prepared.request.args["no_policy"] = json!(reason);
            }
            if scope.is_empty() {
                prepared.request.args[SWITCH_BLANK_SCOPE_ARG] = json!(true);
            }
            // Continuation retains the source registration unless the Owner
            // named and verified a target project above.
            super::project_checks::freeze_launch_goal(
                self,
                &mut prepared.request,
                &Principal::Owner,
            )
            .await?;
            let policy = self.secret_policy_snapshot();
            if policy.contains_value(&prepared.request.args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "switch launch contains a known secret",
                ));
            }
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            let admission = store.admit_switch_launch(
                id,
                prepared.request,
                prepared.workspace,
                &policy,
                now_ms(),
            )?;
            self.publish_locked(&admission.events);
            Ok::<_, Error>(admission)
        }
        .await;
        let admission = match prepared {
            Ok(admission) => admission,
            Err(error) => {
                let policy = self.secret_policy_snapshot();
                let mut store = self.inner.store.lock().await;
                if let Ok(change) = store.fail_switch_preparation(id, &error, &policy, now_ms()) {
                    self.publish_locked(&change.events);
                    log_refusal(&policy, id, &error.code, &error.message);
                }
                self.signal_progress();
                self.signal_launch_arbitration();
                return;
            }
        };
        // The same ordinary worker owns native dispatch and settlement. No linked launch
        // prompt is admitted: only H2 can create B's first SEND.
        let mut progress = self.inner.progress.subscribe();
        self.clone()
            .process_command(admission.command.id, None)
            .await;
        // Native ready batches can race the start reply. Wait on committed progress,
        // never infer readiness from Applied alone or from an Owner assertion.
        let _ = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                {
                    let store = self.inner.store.lock().await;
                    let value = store.switch_result(id)?;
                    if value.launch_state != Some(CommandState::Applied)
                        || store.switch_launch_ready(id)?
                    {
                        return Ok::<_, Error>(());
                    }
                }
                if progress.changed().await.is_err() {
                    return Ok(());
                }
            }
        })
        .await;
        let policy = self.secret_policy_snapshot();
        let result = {
            let mut store = self.inner.store.lock().await;
            let result = store.finish_switch(id, &policy, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
                if let Some(handoff) = &change.value.handoff {
                    for child in &handoff.children {
                        self.schedule_command(child.clone(), None);
                    }
                }
                if let Some(stop) = &change.value.disposal_command
                    && change.value.disposal_state == Some(CommandState::Accepted)
                {
                    self.schedule_command(stop.clone(), None);
                }
            }
            result
        };
        self.signal_progress();
        self.signal_launch_arbitration();
        if let Ok(change) = &result
            && let (Some(code), Some(reason)) = (&change.value.error, &change.value.error_reason)
        {
            log_refusal(&policy, id, code, reason);
        }
        if let Ok(change) = result
            && let Some(handoff) = change.value.handoff
        {
            self.notify_claim_conflicts(
                &handoff.to,
                &handoff.claims,
                &handoff.claim_conflicts,
                None,
            )
            .await;
        }
    }
}
