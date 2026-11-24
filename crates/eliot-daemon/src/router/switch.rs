//! One durable switch worker composes normal launch and H2 transfer.
use super::routing::HandoffRoute;
use super::*;
use eliot_core::api::{
    HandoffReason, SWITCH_BLANK_SCOPE_ARG, SWITCH_PROJECT_REVISION_ARG, SwitchRequest,
};
use eliot_store::{SwitchLaunchCheckout, SwitchLiveCheckout, SwitchReservation};

async fn canonical_checkout_identity(workspace: &str) -> (String, String) {
    let root = match crate::workspace_git::WorkspaceGit::new() {
        Ok(git) => match git.discover_repository_identity(workspace).await {
            Ok((root, _, _)) => root,
            Err(_) => canonical_directory_or_input(workspace),
        },
        Err(_) => canonical_directory_or_input(workspace),
    };
    let key = eliot_core::workspace_path_key(&root).into_owned();
    (root, key)
}

fn canonical_directory_or_input(path: &str) -> String {
    std::fs::canonicalize(path)
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_owned())
}

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
    async fn reject_switch_input(&self, id: &str, error: &Error) {
        let policy = self.secret_policy_snapshot();
        let mut store = self.inner.store.lock().await;
        if let Ok(change) = store.fail_switch_preparation(id, error, &policy, now_ms()) {
            self.publish_locked(&change.events);
            log_refusal(&policy, id, &error.code, &error.message);
        }
        self.signal_progress();
        self.signal_launch_arbitration();
    }

    async fn switch_live_checkout_snapshot(&self) -> Result<Vec<SwitchLiveCheckout>> {
        let inputs = {
            let store = self.inner.store.lock().await;
            store.switch_live_workspace_inputs()?
        };
        let mut snapshot = Vec::with_capacity(inputs.len());
        for (agent, generation, workspace) in inputs {
            let (_, key) = canonical_checkout_identity(&workspace).await;
            snapshot.push((agent, generation, workspace, key));
        }
        Ok(snapshot)
    }

    /// The profiles a manager may place a worker on: the worker's own and the
    /// manager's frozen child profiles.
    async fn manager_successors(&self, manager: &str, source: &Agent) -> Result<Vec<String>> {
        let id = manager
            .strip_prefix("agent:")
            .and_then(|rest| rest.split(':').next())
            .ok_or_else(|| Error::new("not_allowed", "switch caller is not a manager"))?;
        let row = {
            let store = self.inner.store.lock().await;
            store.get_agent(id)?
        }
        .ok_or_else(|| Error::new("stale_generation", "switch caller is gone"))?;
        let mut allowed = vec![source.profile.clone()];
        allowed.extend(
            row.profile_snapshot["child_profiles"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .map(str::to_owned),
        );
        Ok(allowed)
    }

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
                        Ok(event)
                            if event.kind == eliot_store::PROVIDER_SWITCH_QUEUED
                                || event.kind == eliot_store::HANDOFF_QUEUED =>
                        {
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
        owner_process: Option<&OwnerProcess>,
        profile_authority: Option<[u8; 32]>,
    ) -> Result<Value> {
        if !matches!(principal, Principal::Owner | Principal::Agent { .. }) {
            return Err(Error::new(
                "not_allowed",
                "only the Owner or a manager may switch Agents",
            ));
        }
        let request: SwitchRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let replay = {
            let store = self.inner.store.lock().await;
            store.replay_switch(&principal_name(principal), &request)?
        };
        if let Some(value) = replay {
            return serde_json::to_value(value)
                .map_err(|_| Error::new("internal", "cannot encode switch replay"));
        }
        let source = {
            let store = self.inner.store.lock().await;
            store
                .get_agent(&request.from)?
                .ok_or_else(|| Error::new("not_found", "source Agent missing"))?
        };
        if source.generation != request.from_generation {
            return Err(Error::new("stale_generation", "source generation moved"));
        }
        let source_identity = match source.workspace.as_deref() {
            Some(workspace) => Some(canonical_checkout_identity(workspace).await),
            None => None,
        };
        let live_checkout_snapshot = self.switch_live_checkout_snapshot().await?;
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
                .reserve_switch(
                    &principal_name(principal),
                    &request,
                    &policy,
                    SwitchReservation {
                        owner_process,
                        profile_authority,
                        source_checkout: source_identity
                            .as_ref()
                            .map(|(root, key)| (root.as_str(), key.as_str())),
                        live_checkouts: &live_checkout_snapshot,
                    },
                    now_ms(),
                )
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
            source_stop_command,
            source_checkout,
            source_checkout_root,
            source_checkout_key,
            provider_failure,
            profile_authority,
            owner_automation,
            continuation,
            requested_by,
        }) = work
        else {
            return;
        };
        let source = {
            let store = self.inner.store.lock().await;
            store.get_agent(&request.from).ok().flatten()
        };
        let Some(source) = source.filter(|source| {
            source.generation == request.from_generation
                && source.workspace == source_checkout
                && match (
                    source_checkout_root.as_deref(),
                    source_checkout_key.as_deref(),
                ) {
                    (Some(root), Some(key)) => eliot_core::workspace_path_key(root) == key,
                    (None, None) => true,
                    _ => false,
                }
        }) else {
            let error = Error::new(
                "source_integrity",
                "source generation or canonical checkout changed after reservation",
            );
            let policy = self.secret_policy_snapshot();
            let mut store = self.inner.store.lock().await;
            if let Ok(change) = store.fail_switch_preparation(id, &error, &policy, now_ms()) {
                self.publish_locked(&change.events);
            }
            self.signal_progress();
            self.signal_launch_arbitration();
            return;
        };
        let Some(source_stop_command) = source_stop_command else {
            let error = Error::new(
                "source_integrity",
                "switch has no reserved source stop command",
            );
            let policy = self.secret_policy_snapshot();
            let mut store = self.inner.store.lock().await;
            if let Ok(change) = store.fail_switch_preparation(id, &error, &policy, now_ms()) {
                self.publish_locked(&change.events);
            }
            self.signal_progress();
            self.signal_launch_arbitration();
            return;
        };
        // Resolve automatic or manager-routed targets before issuing the
        // source stop. An unavailable route must leave its source running.
        let routed = async {
            let mut launch = SubmitCommand::new(&plan.launch_command, "launch");
            let mut owner_automation = owner_automation;
            let handoff_route = if let Some(value) = continuation {
                Some(HandoffRoute {
                    reason: value.reason,
                    kind: value.kind,
                    role: None,
                    allowed: None,
                })
            } else if let (Some(manager), Some(role)) =
                (requested_by.as_deref(), request.launch.role.as_deref())
            {
                Some(HandoffRoute {
                    reason: HandoffReason::Routing,
                    kind: None,
                    role: Some(role.to_owned()),
                    allowed: Some(self.manager_successors(manager, &source).await?),
                })
            } else {
                None
            };
            let provider_route = if let Some(route) = &handoff_route {
                let (name, identity) = self.route_handoff(&mut launch, &source, route).await?;
                Some((name, launch.args["profile_choice"].take(), identity))
            } else if let Some(failure) = provider_failure {
                let authority = failure.require_authority(&source.profile_snapshot)?;
                owner_automation |= source
                    .profile_choice
                    .as_ref()
                    .is_some_and(|choice| choice.owner_automation);
                let (name, identity) = self
                    .route_provider_switch(
                        &mut launch,
                        &source,
                        failure.kind,
                        authority,
                        owner_automation,
                    )
                    .await?;
                Some((name, launch.args["profile_choice"].take(), identity))
            } else {
                None
            };
            Ok::<_, Error>((provider_route, owner_automation))
        }
        .await;
        let (provider_route, owner_automation) = match routed {
            Ok(result) => result,
            Err(error) => {
                let reason_kind = continuation
                    .map(|value| (value.reason, value.kind))
                    .or_else(|| {
                        provider_failure
                            .map(|value| (HandoffReason::ProviderHold, Some(value.kind)))
                    });
                let policy = self.secret_policy_snapshot();
                let mut store = self.inner.store.lock().await;
                if let Ok(change) = store.fail_switch_preparation(id, &error, &policy, now_ms()) {
                    self.publish_locked(&change.events);
                }
                if let Some((reason, kind)) = reason_kind
                    && let Some(message) = routing::handoff_waiting_message(&error)
                {
                    let candidate = eliot_store::AutomaticHandoff {
                        agent: source,
                        reason,
                        kind,
                        held_since: None,
                    };
                    if let Ok(events) = store.record_handoff_waiting(&candidate, &message, now_ms())
                    {
                        self.publish_locked(&events);
                    }
                }
                self.signal_progress();
                self.signal_launch_arbitration();
                return;
            }
        };
        let prepared_input = async {
            let facts = self
                .prepare_handoff_facts(&source, Some(&request.task_command))
                .await?;
            let brief = self.inner.store.lock().await.prepare_switch_brief(
                id,
                &self.secret_policy_snapshot(),
                &facts,
                now_ms(),
            )?;
            Ok::<_, Error>((facts, brief))
        }
        .await;
        let (facts, brief) = match prepared_input {
            Ok(input) => input,
            Err(error) => {
                self.reject_switch_input(id, &error).await;
                return;
            }
        };
        self.clone()
            .process_command(source_stop_command, None)
            .await;
        let source_exit_proven = {
            let store = self.inner.store.lock().await;
            store.switch_source_exit_proven(id)
        };
        let source_exit_error = match source_exit_proven {
            Ok(true) => None,
            Ok(false) => Some(Error::new(
                "process_custody_unproven",
                "source process-family exit and native source drain are unproven",
            )),
            Err(error) => Some(error),
        };
        if let Some(error) = source_exit_error {
            let policy = self.secret_policy_snapshot();
            let mut store = self.inner.store.lock().await;
            if let Ok(change) = store.fail_switch_preparation(id, &error, &policy, now_ms()) {
                self.publish_locked(&change.events);
            }
            self.signal_progress();
            self.signal_launch_arbitration();
            return;
        }
        if let Err(error) = self.check_command_admission_mode("launch") {
            let policy = self.secret_policy_snapshot();
            let mut store = self.inner.store.lock().await;
            if let Ok(change) = store.fail_switch_preparation(id, &error, &policy, now_ms()) {
                self.publish_locked(&change.events);
            }
            self.signal_progress();
            self.signal_launch_arbitration();
            return;
        }
        // Capture the source only after Store and native custody prove the
        // exact process family is gone, and before any successor launch.
        self.checkpoint_handoff_source(&source).await;
        let source_clean = match source.workspace.as_deref() {
            None => Some(true),
            Some(workspace) => match crate::git_monitor::GitProbe::new() {
                Ok(git) => match git
                    .probe(workspace, None, source.base_oid.as_deref(), &[], now_ms())
                    .await
                {
                    Ok(snapshot) if snapshot.complete => Some(snapshot.dirty.is_empty()),
                    _ => None,
                },
                Err(_) => None,
            },
        };
        let prepared = async {
            self.check_command_admission_mode("launch")?;
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
            if let Some((name, _, _)) = &provider_route {
                if let Some(args) = launch.args.as_object_mut() {
                    args.remove("role");
                }
                launch.args["profile"] = json!(name);
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
                    routing::LaunchRouting {
                        owner_automation,
                        provider_source: (provider_failure.is_some()
                            || profile_authority.is_some())
                        .then_some(&source.profile_snapshot),
                        ..Default::default()
                    },
                    true,
                )
                .await?;
            if let Some(authority) = profile_authority
                && eliot_core::provider_switch_authority(
                    &prepared.request.args["profile_snapshot"],
                )? != authority
            {
                return Err(Error::new(
                    "not_allowed",
                    "session growth switch authority differs from the source generation",
                ));
            }
            if let Some((_, choice, identity)) = provider_route {
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
            let (target_checkout_root, target_checkout_key) = match prepared.workspace.as_ref() {
                Some(workspace) => {
                    let root = workspace.worktree_path.clone();
                    let key = eliot_core::workspace_path_key(&root).into_owned();
                    (root, key)
                }
                None => {
                    let workspace = prepared
                        .request
                        .args
                        .get("workspace")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::new("invalid", "switch launch has no checkout"))?;
                    canonical_checkout_identity(workspace).await
                }
            };
            let same_checkout = source_checkout_key
                .as_deref()
                .is_some_and(|key| key == target_checkout_key);
            if !same_checkout && source_clean != Some(true) {
                return Err(Error::new(
                    "dirty",
                    "switch to a different checkout requires a fully verified clean source worktree",
                ));
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
            let live_checkout_snapshot = self.switch_live_checkout_snapshot().await?;
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            let admission = store.admit_switch_launch(
                &brief,
                prepared.request,
                prepared.workspace,
                SwitchLaunchCheckout {
                    root: &target_checkout_root,
                    key: &target_checkout_key,
                    live_checkouts: &live_checkout_snapshot,
                    source_clean,
                },
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
            let result = store.finish_switch(id, &policy, &brief, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
                if let Some(handoff) = &change.value.handoff {
                    // A provider retry must leave the successor with one
                    // input. The frozen brief already carries its task.
                    if provider_failure.is_none() {
                        self.queue_handoff_task_card(
                            &mut store,
                            handoff,
                            &change.events,
                            facts.applied_task_text(),
                        );
                    }
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
