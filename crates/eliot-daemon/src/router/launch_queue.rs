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
        remote: Option<&eliot_core::RemoteCallContext>,
    ) -> Result<Value> {
        let original: LaunchQueueRequest = decode_params(params.clone())?;
        original.validate()?;
        let mut request = original.command();
        let caller = principal_name(principal);
        if let Some(view) = self
            .inner
            .store
            .lock()
            .await
            .replay_launch_intent(&caller, &original)?
        {
            return encode(view);
        }
        authorize_command(principal, &mut request)?;
        self.check_command_admission_mode("launch")?;
        reject_launch_derived_keys(&request.args)?;
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
            store.enqueue_launch(&caller, &original, request, &project, remote, now_ms())?
        };
        self.signal_progress();
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

    pub(crate) async fn arbitration_timer_wait(
        &self,
        state: &TimerStateView,
    ) -> Result<Option<(String, TimerWait)>> {
        self.inner.store.lock().await.timer_wait(state)
    }

    pub(crate) async fn prepare_assignment_launch(
        &self,
        request: SubmitCommand,
    ) -> Result<PreparedSubmission> {
        validate_rule_launch_action(&request, None)?;
        let prepared = self
            .prepare_arbitrated_launch(request, &Principal::Owner)
            .await?;
        validate_rule_launch_preparation(&prepared)?;
        Ok(prepared)
    }

    async fn prepare_arbitrated_launch(
        &self,
        request: SubmitCommand,
        principal: &Principal,
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
        self.prepare_submission_with_routing(request, principal, false, false, true)
            .await
    }

    pub(crate) async fn try_launch_intent(&self, intent: &PendingLaunch) -> Result<()> {
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
                .prepare_arbitrated_launch(intent.request.clone(), &principal)
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
            }
            Err(error) if error.code == "unavailable" && !self.rule_firings_open() => {}
            Err(error) if let Some(wait) = TimerWait::from_error(&error, route.clone()) => {
                if intent.view.reason.as_ref() != Some(&wait.reason) || intent.route != route {
                    self.inner.store.lock().await.note_launch_wait(
                        intent,
                        wait.reason,
                        route.as_deref(),
                        false,
                    )?;
                }
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
            }
        }
        Ok(())
    }
}

pub(crate) fn checked_route(request: &SubmitCommand, previous: Option<&str>) -> Result<String> {
    let mut snapshot = request.args["profile_snapshot"].clone();
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
        // The repository, frozen base and setup must still match.
        object.remove("worktree_path");
    }
    let digest = eliot_core::canonical_json_sha256(&json!({
        "profile":request.args["profile"],"snapshot":snapshot,"workspace":workspace,
    }))
    .map_err(|_| Error::new("internal", "cannot bind queued launch route"))?;
    let digest = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if previous.is_some_and(|previous| previous != digest) {
        return Err(Error::new(
            "bad_state",
            "queued launch route changed; submit a new intent",
        ));
    }
    Ok(digest)
}

fn encode(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "cannot encode launch arbitration readback"))
}
