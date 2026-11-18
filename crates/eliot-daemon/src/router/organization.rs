//! Headless chat and work organization routing (step 4a).
//!
//! Placement is presentation, never authority: attach/detach/tag only
//! write the organizational overlay beside the Agent id through Owner-only
//! Store transactions. `chat.continue` composes the normal launch pipeline
//! (profile freeze, workspace preparation, admission, dispatch) with a
//! source reference brief; the source conversation is never edited.
use super::*;
use eliot_core::SubmitCommand;
use eliot_core::api::{
    ChatAttachRequest, ChatContinueRequest, ChatDetachRequest, ChatOrganizationRequest,
    ChatTagRequest,
};

impl Router {
    pub(super) async fn chat_organization(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ChatOrganizationRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let store = self.inner.store.lock().await;
        let response = store.chat_organization(&principal_name(principal), &request, now_ms())?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "organization could not be encoded"))
    }

    async fn organization_mutation(
        &self,
        params: &Value,
        principal: &Principal,
        method: &str,
    ) -> Result<Value> {
        require_owner(principal)?;
        let change = {
            let mut store = self.inner.store.lock().await;
            match method {
                "chat.attach" => {
                    let request: ChatAttachRequest =
                        eliot_core::api::discovery::decode_params(params.clone())?;
                    request.validate()?;
                    let change =
                        store.attach_chats(&principal_name(principal), &request, now_ms())?;
                    serde_json::to_value(change.value).map_err(|_| {
                        Error::new("internal", "organization mutation could not be encoded")
                    })?
                }
                "chat.detach" => {
                    let request: ChatDetachRequest =
                        eliot_core::api::discovery::decode_params(params.clone())?;
                    request.validate()?;
                    let change =
                        store.detach_chats(&principal_name(principal), &request, now_ms())?;
                    serde_json::to_value(change.value).map_err(|_| {
                        Error::new("internal", "organization mutation could not be encoded")
                    })?
                }
                _ => {
                    let request: ChatTagRequest =
                        eliot_core::api::discovery::decode_params(params.clone())?;
                    request.validate()?;
                    let change =
                        store.tag_agents(&principal_name(principal), &request, now_ms())?;
                    serde_json::to_value(change.value).map_err(|_| {
                        Error::new("internal", "organization mutation could not be encoded")
                    })?
                }
            }
        };
        Ok(change)
    }

    pub(super) async fn chat_attach(&self, params: &Value, principal: &Principal) -> Result<Value> {
        self.organization_mutation(params, principal, "chat.attach")
            .await
    }

    pub(super) async fn chat_detach(&self, params: &Value, principal: &Principal) -> Result<Value> {
        self.organization_mutation(params, principal, "chat.detach")
            .await
    }

    pub(super) async fn chat_tag(&self, params: &Value, principal: &Principal) -> Result<Value> {
        self.organization_mutation(params, principal, "chat.tag")
            .await
    }

    pub(super) async fn chat_continue(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ChatContinueRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let change = {
            let mut store = self.inner.store.lock().await;
            if let Some(value) = store.replay_chat_continue(&principal_name(principal), &request)? {
                return serde_json::to_value(value)
                    .map_err(|_| Error::new("internal", "cannot encode continue replay"));
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(Error::new(
                    "unavailable",
                    "drain prevents new continuations",
                ));
            }
            let change = store.reserve_chat_continue(
                &principal_name(principal),
                &request,
                &policy,
                now_ms(),
            )?;
            self.publish_locked(&change.events);
            // A disconnected caller cannot cancel the independently scheduled operation.
            if !change.events.is_empty() {
                self.schedule_command(request.id.clone(), None);
            }
            change
        };
        self.signal_progress();
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "cannot encode continue"))
    }

    pub(super) async fn process_continue(&self, id: &str) {
        let work = {
            let mut store = self.inner.store.lock().await;
            match store.start_chat_continue(id, now_ms()) {
                Ok(change) => {
                    self.publish_locked(&change.events);
                    change.value
                }
                Err(_) => None,
            }
        };
        let Some((request, plan)) = work else {
            return;
        };
        let prepared = async {
            // Resolve the selected project's canonical checkout and Git
            // common directory BEFORE preparation: a workspace in another
            // repository is refused here and never launches.
            let registration = {
                let store = self.inner.store.lock().await;
                store
                    .get_project_registration("owner", &request.project)?
                    .ok_or_else(|| {
                        Error::new(
                            "not_found",
                            "continue project is not a retained registration",
                        )
                    })?
            };
            let git = crate::workspace_git::WorkspaceGit::new()?;
            let (checkout, common_dir) = git.repository_identity(&registration.checkout).await?;
            if checkout != registration.checkout || common_dir != registration.repo_common_dir {
                return Err(Error::new(
                    "not_allowed",
                    "registered repository identity changed",
                ));
            }
            let mut launch = SubmitCommand::new(&plan.launch_command, "launch");
            let workspace = request.launch.workspace.as_deref().unwrap_or(&checkout);
            let (_, workspace_common) = git.repository_identity(workspace).await.map_err(|_| {
                Error::new(
                    "not_allowed",
                    "continue workspace is outside the project repository",
                )
            })?;
            if workspace_common != common_dir {
                return Err(Error::new(
                    "not_allowed",
                    "continue workspace is outside the project repository",
                ));
            }
            launch.args = request.launch.args(workspace)?;
            launch.args["text"] = json!(continue_brief(&request, &plan)?);
            let mut prepared = self
                .prepare_submission_after_authorization(launch, &Principal::Owner, false, false)
                .await?;
            // The replacement joins the reserved authorization namespace.
            prepared.request.args["project"] = json!(request.project);
            prepared.request.args["registered_project"] = json!(request.project);
            // The admitted preparation must still belong to the selected
            // project's repository; anything else fails before dispatch.
            if let Some(workspace) = &prepared.workspace
                && (workspace.project != request.project || workspace.repo_common_dir != common_dir)
            {
                return Err(Error::new(
                    "not_allowed",
                    "continue preparation left the project repository",
                ));
            }
            let policy = self.secret_policy_snapshot();
            if policy.contains_value(&prepared.request.args) {
                return Err(Error::new(
                    "secret_in_payload",
                    "continue launch contains a known secret",
                ));
            }
            let mut store = self.inner.store.lock().await;
            self.check_command_admission_mode("launch")?;
            let admission =
                store.admit_continue_launch(id, prepared.request, prepared.workspace, now_ms())?;
            self.publish_locked(&admission.events);
            Ok::<_, Error>(admission)
        }
        .await;
        let admission = match prepared {
            Ok(admission) => admission,
            Err(error) => {
                let mut store = self.inner.store.lock().await;
                if let Ok(change) = store.fail_chat_continue_preparation(id, &error.code, now_ms())
                {
                    self.publish_locked(&change.events);
                }
                self.signal_progress();
                return;
            }
        };
        // The launch's linked first input is the source reference brief; no
        // other input is admitted for the new session here. The linked
        // brief rides ordinary launch scheduling, so its actual outcome is
        // visible in the readback instead of staying accepted.
        let brief = admission
            .linked_command
            .as_ref()
            .map(|command| command.id.clone());
        let mut progress = self.inner.progress.subscribe();
        self.clone()
            .process_command(admission.command.id, brief)
            .await;
        // Native ready batches can race the start reply. Wait on committed progress,
        // never infer readiness from Applied alone or from an Owner assertion.
        let _ = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                {
                    let store = self.inner.store.lock().await;
                    if store.continue_launch_ready(id)? {
                        return Ok::<_, Error>(());
                    }
                }
                if progress.changed().await.is_err() {
                    return Ok(());
                }
            }
        })
        .await;
        let result = {
            let mut store = self.inner.store.lock().await;
            let result = store.finish_chat_continue(id, now_ms());
            if let Ok(change) = &result {
                self.publish_locked(&change.events);
            }
            result
        };
        let _ = result;
        self.signal_progress();
    }
}

/// The new session's first input: a reference to the retained source
/// conversation, never a transcript copy. The reader follows the pointers
/// with `agent.result`, `command.get` and `agent.history`.
fn continue_brief(
    request: &ChatContinueRequest,
    plan: &eliot_core::api::ChatContinueResponse,
) -> Result<String> {
    let mut brief = format!(
        "Continue in project {}: this session starts from chat {} generation {}.\nThe source conversation stays where it is; nothing was copied. Read it back with agent.result id={} generation={} or agent.history id={}.",
        request.project,
        plan.source.agent,
        plan.source.generation,
        plan.source.agent,
        plan.source.generation,
        plan.source.agent,
    );
    if let Some(text) = request.text.as_deref() {
        brief.push_str("\n\nOpening request:\n");
        brief.push_str(text);
    }
    if brief.len() > eliot_core::limits::MAX_TEXT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "continue brief exceeds input budget",
        ));
    }
    Ok(brief)
}
