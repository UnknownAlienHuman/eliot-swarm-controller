use super::*;
use eliot_core::api::{
    ProjectAssignmentClaimRequest, ProjectAssignmentGetRequest, ProjectAssignmentLaunchRequest,
    ProjectAssignmentReleaseRequest, ProjectTransferAction, ProjectTransferOutcome,
    ProjectTransferRequest, ProjectTransferResult, discovery::decode_params,
};
use eliot_store::Store;

impl Router {
    pub(super) async fn project_transfer(
        &self,
        params: &Value,
        principal: &Principal,
        owner_process: Option<&OwnerProcess>,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ProjectTransferRequest = decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let source = {
            let store = self.inner.store.lock().await;
            if let Some(retained) = self.replay_project_transfer(&store, &request)? {
                return encode_assignment(retained);
            }
            if let ProjectTransferAction::Launch { intent, .. } = &request.action {
                Some(store.project_transfer_launch_source(&request, intent)?)
            } else {
                None
            }
        };
        let prepared = match (&request.action, source) {
            (ProjectTransferAction::Launch { profile, .. }, Some(source)) => Some(
                async {
                    self.check_command_admission_mode("launch")?;
                    let workspace = source
                        .workspace
                        .as_deref()
                        .ok_or_else(|| Error::new("bad_state", "source workspace is missing"))?;
                    let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
                    launch.args = json!({"workspace": workspace, "profile": profile});
                    normalize_launch(&mut launch, principal)?;
                    launch.args["project"] = json!(source.project_scope());
                    launch.args["registered_project"] = json!(source.project);
                    self.freeze_launch_profile(
                        principal,
                        &mut launch,
                        routing::LaunchRouting::default(),
                    )
                    .await?;
                    super::project_checks::freeze_launch_goal(self, &mut launch, principal).await?;
                    if policy.contains_value(&launch.args) {
                        return Err(Error::new(
                            "secret_in_payload",
                            "replacement launch contains a known credential",
                        ));
                    }
                    Ok(launch)
                }
                .await,
            ),
            _ => None,
        };
        let mut store = self.inner.store.lock().await;
        if let Some(retained) = self.replay_project_transfer(&store, &request)? {
            return encode_assignment(retained);
        }
        if !matches!(request.action, ProjectTransferAction::Cancel { .. })
            && self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN
        {
            return Err(Error::new(
                "unavailable",
                "drain prevents new project transfer actions",
            ));
        }
        let change =
            store.project_transfer(&request, prepared, &policy, owner_process, now_ms())?;
        self.publish_locked(&change.events);
        if !change.events.is_empty()
            && change.value.outcome == ProjectTransferOutcome::LaunchAdmitted
            && let Some(id) = &change.value.launch_command
        {
            self.schedule_command(id.clone(), None);
        }
        if let Some(handoff) = &change.value.handoff {
            self.queue_handoff_task_card(&mut store, handoff, &change.events, None);
            for child in &handoff.children {
                self.schedule_command(child.clone(), None);
            }
        }
        self.signal_progress();
        self.signal_launch_arbitration();
        encode_assignment(change.value)
    }

    fn replay_project_transfer(
        &self,
        store: &Store,
        request: &ProjectTransferRequest,
    ) -> Result<Option<ProjectTransferResult>> {
        let retained = store.replay_project_transfer(request, now_ms())?;
        if self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN
            && !store.is_write_faulted()
            && let Some(handoff) = retained.as_ref().and_then(|value| value.handoff.as_ref())
        {
            // Use the same accepted/queued filter as startup recovery. The
            // Store still gates dispatch by command state and generation.
            for id in &handoff.children {
                if let Some(admission) = store.recover_accepted_admission(id)? {
                    self.schedule_admission(&admission);
                }
            }
        }
        Ok(retained)
    }

    pub(super) async fn project_assignment_get(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectAssignmentGetRequest = decode_params(params.clone())?;
        request.validate()?;
        let store = self.inner.store.lock().await;
        encode_assignment(store.project_assignment(&principal_name(principal), &request.project)?)
    }

    pub(super) async fn project_assignment_claim(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectAssignmentClaimRequest = decode_params(params.clone())?;
        let mut store = self.inner.store.lock().await;
        let change = store.claim_project_manager(&principal_name(principal), &request, now_ms())?;
        self.publish_locked(&change.events);
        self.signal_progress();
        self.signal_launch_arbitration();
        encode_assignment(change.value)
    }

    pub(super) async fn project_assignment_release(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectAssignmentReleaseRequest = decode_params(params.clone())?;
        let mut store = self.inner.store.lock().await;
        let change =
            store.release_project_manager(&principal_name(principal), &request, now_ms())?;
        self.publish_locked(&change.events);
        self.signal_progress();
        self.signal_launch_arbitration();
        encode_assignment(change.value)
    }

    pub(super) async fn project_assignment_launch(
        &self,
        params: &Value,
        principal: &Principal,
        owner_process: Option<&OwnerProcess>,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ProjectAssignmentLaunchRequest = decode_params(params.clone())?;
        request.validate()?;
        let registration = {
            let store = self.inner.store.lock().await;
            if let Some(result) = store.replay_project_manager_launch(&request, now_ms())? {
                return encode_assignment(result);
            }
            store
                .get_project_registration("owner", &request.project)?
                .ok_or_else(|| Error::new("not_found", "project registration was not found"))?
        };
        let prepared = async {
            self.check_command_admission_mode("launch")?;
            if !registration.profiles.contains(&request.profile) {
                return Err(Error::new(
                    "not_allowed",
                    "manager launch profile is not approved for this project",
                ));
            }
            let git = crate::workspace_git::WorkspaceGit::new()?;
            let (checkout, common_dir) = git.repository_identity(&registration.checkout).await?;
            if checkout != registration.checkout || common_dir != registration.repo_common_dir {
                return Err(Error::new(
                    "not_allowed",
                    "registered repository identity changed",
                ));
            }
            let mut launch = SubmitCommand::new(request.id.clone(), "launch");
            // A manager launch always runs in a project worktree. Without an
            // explicit adoption the daemon prepares a fresh standard worktree
            // (`<project>/<agent>-manager`) from the frozen base OID and runs
            // the existing preparation and setup chain before native dispatch.
            // The branch is finalized with the admitted Agent ID, never from
            // the launch operation ID. An explicit adoption names an existing
            // checkout of the same repository; the daemon never adopts
            // automatically, and an adopted checkout keeps its environment
            // with no setup side effects.
            let preparation = if let Some(adopt) = request.adopt_worktree.as_deref() {
                launch.args = json!({"workspace": adopt, "profile": request.profile});
                normalize_launch(&mut launch, principal)?;
                launch.args["registered_project"] = json!(request.project);
                let canonical = required_text(&launch.args, "workspace")?;
                let (adopt_checkout, adopt_common) = git.repository_identity(canonical).await?;
                if adopt_checkout != canonical || adopt_common != registration.repo_common_dir {
                    return Err(Error::new(
                        "not_allowed",
                        "adopted checkout is outside this project repository",
                    ));
                }
                self.freeze_launch_profile(principal, &mut launch, routing::LaunchRouting::default()).await?;
                self.freeze_project_command_guards(&mut launch).await?;
                None
            } else {
                launch.args = json!({"workspace": registration.checkout, "profile": request.profile, "worktree": ""});
                normalize_launch(&mut launch, principal)?;
                launch.args["registered_project"] = json!(request.project);
                // The hint carries the resolved logical name (display name,
                // else origin, else main checkout folder), never the
                // registration ID.
                let logical = super::project_logical_name(
                    &self.inner.data_root,
                    Some(request.project.as_str()),
                    registration.checkout.as_str(),
                )
                .await?;
                launch.args["auto_worktree"] =
                    json!({"project": logical, "task": "manager"});
                let input = self
                    .freeze_launch_admission(principal, &mut launch, false, false, routing::LaunchRouting::default())
                    .await?;
                Some(input.ok_or_else(|| {
                    Error::new("internal", "manager worktree was not frozen")
                })?)
            };
            launch.args["registered_project"] = json!(registration.id);
            if let Some(reason) = &request.no_policy {
                launch.args["no_policy"] = json!(reason);
            }
            if let Some(goal) = &request.goal {
                launch.args["goal"] = json!(goal);
            }
            super::project_checks::freeze_launch_goal(self, &mut launch, principal).await?;
            if contains_known_secret(&launch.args, &self.inner.secrets) {
                return Err(Error::new(
                    "secret_in_payload",
                    "manager launch contains a known credential",
                ));
            }
            Ok((launch, preparation))
        }
        .await;
        let mut store = self.inner.store.lock().await;
        let prepared = prepared.and_then(|(launch, preparation)| {
            self.check_command_admission_mode("launch")?;
            if !store
                .get_project_registration("owner", &request.project)?
                .is_some_and(|current| current.revision == registration.revision)
            {
                return Err(Error::new(
                    "stale_assignment",
                    "manager launch registration changed",
                ));
            }
            Ok((launch, preparation))
        });
        let change = store.launch_project_manager(&request, prepared, owner_process, now_ms())?;
        if !change.events.is_empty() {
            self.publish_locked(&change.events);
            if let Some(id) = &change.value.command_id {
                self.schedule_command(id.clone(), None);
            }
        }
        self.signal_progress();
        self.signal_launch_arbitration();
        encode_assignment(change.value)
    }
}

fn encode_assignment(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "assignment result could not be encoded"))
}
