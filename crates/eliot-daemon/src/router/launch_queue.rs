use super::*;
use eliot_core::api::{
    LaunchIntentGetRequest, LaunchQueueListRequest, LaunchQueueRequest, LaunchWaitReason,
    ManagerBudgetRequest, discovery::decode_params,
};
use eliot_store::{DirectLaunchArbitration, PendingLaunch, TimerWait};
use std::collections::VecDeque;
use tokio::sync::oneshot;

const DIRECT_LAUNCH_LIMIT: usize = 64;
const DIRECT_LAUNCH_WAIT_MS: u64 = 20_000;

#[derive(Default)]
pub(super) struct DirectLaunches {
    projects: HashMap<String, VecDeque<DirectLaunch>>,
    next: u64,
}

struct DirectLaunch {
    sequence: u64,
    created_at: i64,
    deadline: Instant,
    principal: String,
    request: SubmitCommand,
    workspace: Option<WorkspacePreparationInput>,
    remote: Option<eliot_core::RemoteCallContext>,
    owner_process: Option<OwnerProcess>,
    settled: oneshot::Sender<Result<Admission>>,
}

impl Router {
    // Direct RPCs retain their ordinary command ID and response. The same
    // scheduler that visits retained intent and timer heads puts this prepared
    // request through one fair admission decision.
    pub(super) async fn arbitrate_direct_launch(
        &self,
        principal: &Principal,
        submission: CommandSubmission,
        workspace: Option<WorkspacePreparationInput>,
        context: SubmissionContext<'_>,
    ) -> Result<Admission> {
        if self.inner.launch_arbitration.is_closed() {
            return Err(Error::new(
                "unavailable",
                "the daemon's launch arbitration worker is unavailable",
            ));
        }
        let request = submission.request();
        let project = request.args["registered_project"]
            .as_str()
            .or_else(|| request.args["project"].as_str())
            .unwrap_or_default()
            .to_owned();
        let deadline = Instant::now() + Duration::from_millis(DIRECT_LAUNCH_WAIT_MS);
        let (settled, mut reply) = oneshot::channel();
        let sequence = {
            let mut queue = self.inner.direct_launches.lock_recover();
            queue.prune();
            if queue.projects.values().map(VecDeque::len).sum::<usize>() >= DIRECT_LAUNCH_LIMIT {
                return Err(Error::new(
                    "capacity_unavailable",
                    "direct launch arbitration is full",
                ));
            }
            queue.next = queue.next.wrapping_add(1);
            let sequence = queue.next;
            queue
                .projects
                .entry(project.clone())
                .or_default()
                .push_back(DirectLaunch {
                    sequence,
                    created_at: now_ms(),
                    deadline,
                    principal: principal_name(principal),
                    request: request.clone(),
                    workspace,
                    remote: context.remote.cloned(),
                    owner_process: context.owner_process.cloned(),
                    settled,
                });
            sequence
        };
        self.signal_launch_arbitration();
        let result = match timeout_at(deadline, &mut reply).await {
            Ok(result) => result,
            Err(_) => {
                let cancelled = self
                    .inner
                    .direct_launches
                    .lock_recover()
                    .cancel(&project, sequence);
                if cancelled {
                    self.signal_launch_arbitration();
                    return Err(direct_capacity_deadline());
                }
                // The worker claimed the request under the Store lock. Its
                // admission decision must settle before we can answer.
                reply.await
            }
        };
        result.unwrap_or_else(|_| {
            Err(Error::new(
                "unknown",
                "direct launch arbitration reply was lost; retry with the same id",
            ))
        })
    }

    pub(crate) fn direct_launch_heads(&self) -> Vec<(String, i64, u64)> {
        let mut queue = self.inner.direct_launches.lock_recover();
        queue.prune();
        queue
            .projects
            .iter()
            .filter_map(|(project, jobs)| {
                jobs.front()
                    .map(|job| (project.clone(), job.created_at, job.sequence))
            })
            .collect()
    }

    pub(crate) fn refuse_direct_behind_queued(&self, project: &str, sequence: u64) {
        let job = self
            .inner
            .direct_launches
            .lock_recover()
            .take_head(project, sequence);
        if let Some(job) = job {
            let _ = job.settled.send(Err(Error::new(
                "capacity_unavailable",
                "earlier queued work has priority in this project's launch arbitration",
            )));
            self.signal_launch_arbitration();
        }
    }

    pub(crate) async fn try_direct_launch_in_pass(
        &self,
        project: &str,
        sequence: u64,
        blocked_scopes: &HashSet<String>,
    ) -> LaunchArbitrationAttempt {
        let mut store = self.inner.store.lock().await;
        let job = self
            .inner
            .direct_launches
            .lock_recover()
            .take_head(project, sequence);
        let Some(job) = job else {
            return LaunchArbitrationAttempt::Other;
        };
        if job.settled.is_closed() {
            self.signal_launch_arbitration();
            return LaunchArbitrationAttempt::Other;
        }
        let result = if Instant::now() >= job.deadline {
            Err(direct_capacity_deadline())
        } else {
            let context = SubmissionContext {
                remote: job.remote.as_ref(),
                owner_process: job.owner_process.as_ref(),
                direct_launch: Some(DirectLaunchArbitration {
                    project,
                    blocked_scopes,
                }),
                ..Default::default()
            };
            let result = self.admit_submission(
                &mut store,
                &job.principal,
                job.request.clone().into(),
                job.workspace.clone(),
                context,
            );
            if let Ok(admission) = &result
                && !admission.events.is_empty()
            {
                self.publish_locked(&admission.events);
            }
            result
        };
        drop(store);
        if result
            .as_ref()
            .is_err_and(|error| error.code == "arbitration_retry")
        {
            let mut queue = self.inner.direct_launches.lock_recover();
            if Instant::now() < job.deadline {
                queue
                    .projects
                    .entry(project.to_owned())
                    .or_default()
                    .push_front(job);
                return LaunchArbitrationAttempt::RetryPass;
            }
            drop(queue);
            let _ = job.settled.send(Err(direct_capacity_deadline()));
            self.signal_launch_arbitration();
            return LaunchArbitrationAttempt::Other;
        }
        if let Ok(admission) = &result
            && admission.is_new
        {
            self.schedule_admission(admission);
            self.signal_progress();
        }
        let attempt = match &result {
            Ok(_) => LaunchArbitrationAttempt::Admitted,
            Err(error) if error.code == "capacity_unavailable" => {
                let wait = TimerWait::from_error(error, None, false);
                LaunchArbitrationAttempt::CapacityWait(
                    wait.map_or_else(|| "reservation".into(), |wait| wait.reason.scope),
                )
            }
            Err(_) => LaunchArbitrationAttempt::Other,
        };
        let _ = job.settled.send(result);
        self.signal_launch_arbitration();
        attempt
    }

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
        owner_automation: bool,
    ) -> Result<PreparedSubmission> {
        validate_rule_launch_action(&request, None)?;
        let prepared = self
            .prepare_arbitrated_launch(request, &Principal::Owner, owner_automation)
            .await?;
        validate_rule_launch_preparation(&prepared)?;
        Ok(prepared)
    }

    async fn prepare_arbitrated_launch(
        &self,
        request: SubmitCommand,
        principal: &Principal,
        owner_automation: bool,
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
        self.prepare_submission_with_routing(
            request,
            principal,
            false,
            super::SubmissionScope::Verify,
            routing::LaunchRouting {
                arbitrated: true,
                owner_automation,
                ..Default::default()
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
        let automatic = intent.owner_automation || matches!(principal, Principal::Agent { .. });
        let outcome = async {
            let mut frozen_request = intent.request.clone();
            if frozen_request.args.get("sources").is_some()
                && frozen_request
                    .args
                    .get(super::sources::RETAINED_SOURCE_SNAPSHOT)
                    .is_none()
            {
                let candidate = self
                    .capture_queued_source_snapshot(&frozen_request)
                    .await?
                    .ok_or_else(|| Error::new("source_integrity", "source snapshot is missing"))?
                    .to_value()?;
                if self.secret_policy_snapshot().contains_value(&candidate) {
                    return Err(Error::new(
                        "secret_in_payload",
                        "queued source contains a known credential",
                    ));
                }
                let retained = self
                    .inner
                    .store
                    .lock()
                    .await
                    .retain_launch_source_snapshot(&intent.view.id, &candidate)?;
                frozen_request.args[super::sources::RETAINED_SOURCE_SNAPSHOT] = retained;
            }
            let prepared = self
                .prepare_arbitrated_launch(frozen_request, &principal, intent.owner_automation)
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
            route = checked_route(&prepared.request, route.as_deref(), automatic)?;
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            let snapshot = &prepared.request.args["profile_snapshot"];
            if let Some(harness) = snapshot["harness"].as_str()
                && self.harness_auth_applies(harness, snapshot)
                && store.harness_auth_held(harness)?
            {
                return Err(Error::new("auth_required", "vendor sign-in is pending"));
            }
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
            Err(error)
                if error.code == super::DAEMON_UPDATE_IN_PROGRESS
                    || (error.code == "unavailable" && !self.rule_firings_open()) =>
            {
                Ok(LaunchArbitrationAttempt::Other)
            }
            Err(error)
                if let Some(wait) =
                    TimerWait::from_error(&error, route.clone(), intent.owner_automation) =>
            {
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

impl DirectLaunches {
    fn take_head(&mut self, project: &str, sequence: u64) -> Option<DirectLaunch> {
        let jobs = self.projects.get_mut(project)?;
        if jobs.front()?.sequence != sequence {
            return None;
        }
        jobs.pop_front()
    }

    fn cancel(&mut self, project: &str, sequence: u64) -> bool {
        let Some(jobs) = self.projects.get_mut(project) else {
            return false;
        };
        let Some(index) = jobs.iter().position(|job| job.sequence == sequence) else {
            return false;
        };
        jobs.remove(index).is_some()
    }

    fn prune(&mut self) {
        self.projects.retain(|_, jobs| {
            jobs.retain(|job| !job.settled.is_closed());
            !jobs.is_empty()
        });
    }
}

fn direct_capacity_deadline() -> Error {
    Error::new(
        "capacity_unavailable",
        "direct launch was still waiting for capacity when its arbitration deadline expired",
    )
}

pub(crate) enum LaunchArbitrationAttempt {
    Admitted,
    CapacityWait(String),
    RetryPass,
    Other,
}

pub(crate) fn checked_route(
    request: &SubmitCommand,
    previous: Option<&str>,
    automatic: bool,
) -> Result<Option<String>> {
    if automatic && request.args.get("profile_choice").is_some() {
        return Ok(None);
    }
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
    Ok(Some(route))
}

fn encode(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "cannot encode launch arbitration readback"))
}
