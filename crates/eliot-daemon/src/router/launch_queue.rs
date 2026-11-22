use super::*;
use eliot_core::api::{
    LaunchIntentGetRequest, LaunchQueueListRequest, LaunchQueueRequest, LaunchWaitReason,
    ManagerBudgetRequest, discovery::decode_params,
};
use eliot_store::{PendingLaunch, TimerWait};

impl Router {
    pub(super) async fn enqueue_launch(
        &self,
        params: &Value,
        principal: &Principal,
        context: SubmissionContext<'_>,
    ) -> Result<Value> {
        let original: LaunchQueueRequest = decode_params(params.clone())?;
        original.validate()?;
        let mut request = original.command();
        let caller = principal_name(principal);
        {
            let store = self.inner.store.lock().await;
            if let Some(view) = store.replay_launch_intent(&caller, &original)? {
                return encode(view);
            }
            hierarchy::authorize_agent_submit(&store, principal)?;
        }
        authorize_command(principal, &mut request)?;
        self.check_command_admission_mode("launch")?;
        reject_launch_derived_keys(&request.args)?;
        super::project_checks::validate_launch_inputs(
            &request.args,
            matches!(principal, Principal::Owner),
        )?;
        if contains_known_secret_including_keys(&request.args, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "queued launch contains a known credential",
            ));
        }
        // Use the same workspace and authority lookup as a regular launch.
        // Profile selection is deferred until this project is visited.
        if request.args.get("chat") != Some(&Value::Bool(true)) {
            let store = self.inner.store.lock().await;
            hierarchy::default_child_workspace(&store, &mut request)?;
            normalize_launch(&mut request, principal)?;
        } else {
            require_owner(principal)?;
        }
        let project = self.queued_launch_project(&request).await?;
        let view = {
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            store.enqueue_launch(&caller, &original, request, &project, context, now_ms())?
        };
        self.signal_progress();
        self.signal_launch_arbitration();
        encode(view)
    }

    pub(super) async fn read_launch_intent(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: LaunchIntentGetRequest = decode_params(params.clone())?;
        request.validate()?;
        encode(
            self.inner
                .store
                .lock()
                .await
                .launch_intent(&principal_name(principal), &request.id)?,
        )
    }

    pub(super) async fn read_launch_queue(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: LaunchQueueListRequest = decode_params(params.clone())?;
        request.validate()?;
        encode(
            self.inner
                .store
                .lock()
                .await
                .launch_queue(&principal_name(principal), &request)?,
        )
    }

    pub(super) async fn set_manager_budget(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ManagerBudgetRequest = decode_params(params.clone())?;
        request.validate()?;
        let view = {
            let mut store = self.inner.store.lock().await;
            let limits = crate::config::load_scope_limits(&self.inner.data_root)?;
            store.set_manager_budget(&principal_name(principal), &request, &limits, now_ms())?
        };
        self.signal_progress();
        self.signal_launch_arbitration();
        encode(view)
    }

    pub(crate) async fn queued_launch_project(&self, request: &SubmitCommand) -> Result<String> {
        if let Some(parent) = request.args.get("parent").and_then(Value::as_str) {
            return self
                .inner
                .store
                .lock()
                .await
                .get_agent(parent)?
                .map(|agent| agent.project)
                .ok_or_else(|| Error::new("stale_generation", "queued launch parent disappeared"));
        }
        if request.args.get("chat") == Some(&Value::Bool(true)) {
            return Ok(String::new());
        }
        let workspace = std::path::Path::new(required_text(&request.args, "workspace")?);
        let workspace = std::fs::canonicalize(workspace).map_err(|_| {
            Error::new(
                "workspace_unavailable",
                "queued launch workspace is unavailable",
            )
        })?;
        let resolved = crate::bootstrap::discover_project(
            &self.inner.data_root,
            &self.inner.store,
            &workspace,
            &self.secret_policy_snapshot(),
        )
        .await?;
        // Match the normal launch identity for unregistered workspaces.
        Ok(resolved
            .project
            .map(|p| p.id)
            .unwrap_or_else(|| workspace.to_string_lossy().into_owned()))
    }

    pub(crate) async fn launch_arbitration(
        &self,
    ) -> Result<(Option<String>, HashMap<String, u8>, Vec<PendingLaunch>)> {
        let store = self.inner.store.lock().await;
        let priorities = match crate::config::load_scope_limits(&self.inner.data_root) {
            Ok(limits) => store.arbitration_priorities(&limits)?,
            // Invalid source is reported by settings reload. Keep the timer
            // worker alive so later corrected settings can be applied.
            Err(_) => HashMap::new(),
        };
        Ok((
            store.arbitration_cursor()?,
            priorities,
            store.pending_launches()?,
        ))
    }

    pub(crate) async fn rule_has_open_actions(&self, principal: &str, rule: &str) -> Result<bool> {
        self.inner
            .store
            .lock()
            .await
            .rule_has_open_actions(principal, rule)
    }

    pub(crate) async fn arbitration_timer_wait(
        &self,
        state: &TimerStateView,
    ) -> Result<Option<(String, TimerWait)>> {
        self.inner.store.lock().await.timer_wait(state)
    }

    pub(crate) async fn prepare_assignment_launch(
        &self,
        request: SubmitCommand,
        route: Option<&str>,
    ) -> Result<PreparedSubmission> {
        validate_rule_launch_action(&request, None)?;
        let prepared = self
            .prepare_arbitrated_launch(request, &Principal::Owner, true, route)
            .await?;
        validate_rule_launch_preparation(&prepared)?;
        Ok(prepared)
    }

    async fn prepare_arbitrated_launch(
        &self,
        request: SubmitCommand,
        principal: &Principal,
        owner_automation: bool,
        route: Option<&str>,
    ) -> Result<PreparedSubmission> {
        if request
            .args
            .get("workspace")
            .and_then(Value::as_str)
            .is_some_and(|path| !std::path::Path::new(path).is_dir())
        {
            return Err(Error::new(
                "workspace_unavailable",
                "queued launch workspace is unavailable",
            ));
        }
        let bound_profile =
            route.and_then(|route| route.rsplit_once(':').map(|(profile, _)| profile));
        self.prepare_submission_with_routing(
            request,
            principal,
            false,
            false,
            routing::LaunchRouting {
                arbitrated: true,
                owner_automation,
                bound_profile,
            },
            false,
        )
        .await
    }

    pub(crate) async fn try_launch_intent_in_pass(
        &self,
        intent: &PendingLaunch,
        blocked_scopes: &HashSet<String>,
    ) -> Result<LaunchArbitrationAttempt> {
        let principal = if intent.principal == "owner" {
            Principal::Owner
        } else {
            let (id, generation) = intent
                .principal
                .strip_prefix("agent:")
                .and_then(|value| value.split_once(':'))
                .and_then(|(id, generation)| Some((id, generation.parse().ok()?)))
                .ok_or_else(|| {
                    Error::new("source_integrity", "queued launch principal is malformed")
                })?;
            Principal::Agent {
                id: id.to_owned(),
                generation,
            }
        };
        let mut route = intent.route.clone();
        let outcome = async {
            let prepared = self
                .prepare_arbitrated_launch(
                    intent.request.clone(),
                    &principal,
                    false,
                    route.as_deref(),
                )
                .await?;
            let project = if prepared.request.args.get("parent").is_some() {
                self.queued_launch_project(&prepared.request).await?
            } else {
                prepared.request.args["registered_project"]
                    .as_str()
                    .or_else(|| prepared.request.args["project"].as_str())
                    .unwrap_or_default()
                    .to_owned()
            };
            if project != intent.view.project {
                return Err(Error::new(
                    "bad_state",
                    "queued launch project identity changed",
                ));
            }
            route = Some(checked_route(&prepared.request, route.as_deref())?);
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            let admission = store.admit_launch_intent(
                intent,
                prepared.request,
                prepared.workspace,
                blocked_scopes,
                now_ms(),
            )?;
            self.publish_locked(&admission.events);
            Ok(admission)
        }
        .await;
        match outcome {
            Ok(admission) => {
                self.schedule_admission(&admission);
                self.signal_progress();
                self.signal_launch_arbitration();
                Ok(LaunchArbitrationAttempt::Admitted)
            }
            Err(error) if error.code == "arbitration_retry" => {
                Ok(LaunchArbitrationAttempt::RetryPass)
            }
            Err(error) if error.code == "unavailable" && !self.rule_firings_open() => {
                Ok(LaunchArbitrationAttempt::Other)
            }
            Err(error) if let Some(wait) = TimerWait::from_error(&error, route.clone()) => {
                let capacity_scope =
                    (wait.reason.code == "capacity_unavailable").then(|| wait.reason.scope.clone());
                if intent.view.reason.as_ref() != Some(&wait.reason) || intent.route != route {
                    self.inner.store.lock().await.note_launch_wait(
                        intent,
                        wait.reason,
                        route.as_deref(),
                        false,
                    )?;
                }
                Ok(match capacity_scope {
                    Some(scope) => LaunchArbitrationAttempt::CapacityWait(scope),
                    None => LaunchArbitrationAttempt::Other,
                })
            }
            Err(error) if eliot_core::is_deterministic_pre_admission_rejection(&error.code) => {
                self.inner.store.lock().await.note_launch_wait(
                    intent,
                    LaunchWaitReason {
                        code: error.code,
                        scope: "launch".into(),
                        changed_by: "caller".into(),
                    },
                    None,
                    true,
                )?;
                self.signal_progress();
                Ok(LaunchArbitrationAttempt::Other)
            }
            Err(error) => {
                let mut store = self.inner.store.lock().await;
                if store.is_write_faulted() {
                    return Err(error);
                }
                let reason = LaunchWaitReason {
                    code: error.code.clone(),
                    scope: "launch".into(),
                    changed_by: "owner".into(),
                };
                if intent.view.reason.as_ref() != Some(&reason) || intent.route != route {
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "launch.intent_wait",
                        &[("intent", &intent.view.id)],
                        Some(&error),
                    );
                    store.note_launch_wait(intent, reason, route.as_deref(), false)?;
                }
                Ok(LaunchArbitrationAttempt::Other)
            }
        }
    }
}

pub(crate) enum LaunchArbitrationAttempt {
    Admitted,
    CapacityWait(String),
    RetryPass,
    Other,
}

pub(crate) fn checked_route(request: &SubmitCommand, previous: Option<&str>) -> Result<String> {
    let mut snapshot = request.args["profile_snapshot"].clone();
    if let Some(snapshot) = snapshot.as_object_mut() {
        snapshot.remove("quota_reserve_percent");
        snapshot.remove("quota_unknown");
    }
    if let Some(scopes) = snapshot["capacity_scopes"].as_array_mut() {
        for scope in scopes {
            if let Some(object) = scope.as_object_mut() {
                object.remove("max_live");
            }
        }
    }
    let mut workspace = request.args.get("workspace_preparation").cloned();
    if let Some(object) = workspace.as_mut().and_then(Value::as_object_mut) {
        // Preparation chooses a fresh unused destination on each probe.
        // The repository, frozen base, naming inputs and setup must still match.
        object.remove("worktree_path");
        if object
            .get("generated")
            .and_then(|generated| generated.get("branch"))
            .and_then(Value::as_str)
            .is_none()
        {
            object.remove("requested_branch");
        }
    }
    let digest = eliot_core::canonical_json_sha256(&json!({
        "profile":request.args["profile"],"snapshot":snapshot,"workspace":workspace,
    }))
    .map_err(|_| Error::new("internal", "cannot bind queued launch route"))?;
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let profile = request
        .args
        .get("profile_choice")
        .and_then(|_| request.args["profile"].as_str());
    let route = profile.map_or_else(|| digest.clone(), |profile| format!("{profile}:{digest}"));
    if previous.is_some_and(|previous| previous != route && previous != digest) {
        return Err(Error::new(
            "bad_state",
            "queued launch route changed; submit a new intent",
        ));
    }
    Ok(route)
}

fn encode(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "cannot encode launch arbitration readback"))
}
