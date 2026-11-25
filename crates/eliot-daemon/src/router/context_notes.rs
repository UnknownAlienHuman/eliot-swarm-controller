use super::{MODE_OPEN, Principal, Router, insertion_facts::InsertionFact};
use crate::common::now_ms;
use eliot_core::{
    CommandState, ContextWaitRequest, Error, Event, GitUpstreamMoved, Result,
    api::{HandoffResponse, MailMode, MailSendRequest, SWARM_MAIL_SENDER},
};
use eliot_store::Store;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::atomic::Ordering;
use tokio::sync::{broadcast, watch};

fn matches_git_notice_binding(
    store: &Store,
    agent: &str,
    generation: i64,
    moved: &GitUpstreamMoved,
) -> Result<bool> {
    Ok(store.get_agent(agent)?.is_some_and(|current| {
        current.generation == generation
            && current.receives_git_base_notices()
            && current.workspace.as_deref().is_some_and(|path| {
                eliot_core::workspace_path_key(path)
                    == eliot_core::workspace_path_key(&moved.workspace)
            })
    }))
}

fn git_notice_refs_current(
    checkout: &crate::workspace_git::DiffCheckout,
    moved: &GitUpstreamMoved,
) -> bool {
    checkout.on_branch(&moved.branch, &moved.head)
        && checkout
            .origin_default()
            .is_ok_and(|tip| tip == moved.upstream)
}

async fn retired_git_base(
    git: &crate::git_monitor::GitProbe,
    checkout: &crate::workspace_git::DiffCheckout,
    workspace: &eliot_core::WorkspacePreparation,
    moved: &GitUpstreamMoved,
    deadline: tokio::time::Instant,
) -> Result<Option<String>> {
    use crate::workspace_git::containment::prove_containment;
    let binding = checkout.binding();
    let base = workspace.base_oid.as_str();
    if !eliot_core::api::is_git_oid(base) {
        return Ok(None);
    }
    let ancestor = git
        .workspace_command_bound(
            &checkout.root.path,
            &["merge-base", "--is-ancestor", base, &moved.head],
            binding,
            deadline,
        )
        .await?;
    if ancestor.code != 0 {
        return Ok(None);
    }
    if let Some(deleted) = moved.base_deleted.as_ref()
        && deleted.valid()
        && deleted.verified
        && deleted.base_oid == base
        && deleted.tip.is_none()
        && let Some(reference) = workspace.base_ref.as_deref()
        && checkout
            .base_dependency(reference, base)
            .is_some_and(|now| now.reference == deleted.reference && now.tip.is_none())
    {
        return Ok(Some(format!(
            "Recorded base branch {} at {base} was deleted.",
            deleted.reference
        )));
    }
    if moved.previous_oid != moved.upstream.oid
        && prove_containment(git, binding.common_dir, base, &moved.previous_oid, deadline)
            .await
            .is_err_and(|error| error.code == "content_not_at_tip")
        && prove_containment(git, binding.common_dir, base, &moved.upstream.oid, deadline)
            .await
            .is_ok()
    {
        return Ok(Some(format!(
            "Recorded base {} at {base} is now contained in upstream.",
            workspace.base_ref.as_deref().unwrap_or("commit")
        )));
    }
    Ok(None)
}

fn append_git_conflicts(text: &mut String, conflicts: &[String]) {
    if !conflicts.is_empty() {
        text.push_str("\nConflicting files from Git merge-tree:");
        for (index, path) in conflicts.iter().enumerate() {
            let path = json!(path).to_string();
            if text.len() + path.len() > 24 * 1024 {
                text.push_str(&format!(
                    "\n{} additional conflicting files.",
                    conflicts.len() - index
                ));
                break;
            }
            text.push_str("\n- ");
            text.push_str(&path);
        }
    }
}

pub(crate) async fn run_context_notes(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut events = router.inner.bus.subscribe();
    let mut cursor = router.inner.store.lock().await.context_notes_cursor()?;
    router.note_worker_task_running("context_notes");
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let page = {
            let mut store = router.inner.store.lock().await;
            let bounds = store.event_bounds()?;
            if cursor < bounds.pruned_through {
                crate::logging::record(
                    &router.secret_policy_snapshot(),
                    "daemon.context_notes_retention_gap",
                    &[],
                    Some(&Error::new(
                        "history_gap",
                        "context notes cursor fell behind event retention",
                    )),
                );
                cursor = bounds.pruned_through;
                store.advance_context_notes_cursor(cursor)?;
            }
            let page = store.read_events(cursor, 128, Some(bounds.hi))?;
            if page.is_empty() && cursor < bounds.hi {
                cursor = bounds.hi;
                store.advance_context_notes_cursor(cursor)?;
            }
            page
        };
        if !page.is_empty() {
            for event in page {
                if let Err(error) = router.note_committed_fact(&event).await {
                    crate::logging::record(
                        &router.secret_policy_snapshot(),
                        "daemon.context_note_fact_failed",
                        &[("seq", &event.seq.to_string()), ("event_kind", &event.kind)],
                        Some(&error),
                    );
                }
                router
                    .inner
                    .store
                    .lock()
                    .await
                    .advance_context_notes_cursor(event.seq)?;
                cursor = event.seq;
            }
            continue;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            event = events.recv() => {
                if matches!(event, Err(broadcast::error::RecvError::Closed)) {
                    return Err(Error::new("unavailable", "context note event bus is closed"));
                }
            }
        }
    }
}

impl Router {
    pub(crate) async fn expire_next_input_contexts(&self) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = store.expire_next_input_contexts(now_ms())?;
        if !events.is_empty() {
            self.publish_locked(&events);
            self.signal_progress();
        }
        Ok(())
    }

    async fn note_committed_fact(&self, event: &Event) -> Result<()> {
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Ok(());
        }
        if event.kind == "git.upstream_moved" && event.source == "git" {
            return self.deliver_git_base_notice(event).await;
        }
        self.note_native_auth_hold(event).await?;
        self.reconcile_launch_refusal(event).await?;
        if matches!(
            event.kind.as_str(),
            "turn.completed" | "tool.background_ended"
        ) && event.source == "stream"
        {
            // Before the fact-note early return, and without the Store lock held.
            if let Err(error) = self.maybe_arm_background_wake(event).await {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "daemon.background_wake_arm_failed",
                    &[("seq", &event.seq.to_string())],
                    Some(&error),
                );
            }
        }
        if matches!(
            event.kind.as_str(),
            "session.context.compacted" | "session.ready"
        ) && event.source == "stream"
            && let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation)
        {
            let queue_task_card = {
                let store = self.inner.store.lock().await;
                event.kind == "session.context.compacted"
                    || store
                        .generation_start_command(agent, generation)?
                        .is_some_and(|command| command.verb == "resume")
            };
            if queue_task_card {
                let selected_task = self.selected_task_text_for_card(agent, generation).await?;
                let mut store = self.inner.store.lock().await;
                self.queue_task_card_locked(&mut store, event, None, selected_task.as_deref())?;
            }
        }
        let facts = self.insertion_facts(event).await?;
        if facts.is_empty() {
            return Ok(());
        }
        let mut captured = event.clone();
        if event.source == "git"
            && captured.data["turn_id"].is_null()
            && let (Some(id), Some(generation)) = (event.agent.as_deref(), event.generation)
        {
            let store = self.inner.store.lock().await;
            if let Some(agent) = store
                .get_agent(id)?
                .filter(|agent| agent.generation == generation)
                && let Some(turn) = agent.active_turn.as_deref()
                && store
                    .get_turn(id, generation, turn)?
                    .and_then(|record| record.started_at)
                    .is_some_and(|started_at| started_at <= event.ts)
            {
                captured.data["turn_id"] = json!(turn);
            }
        }
        if event.source == "git" && captured.data["turn_id"].is_null() {
            return Ok(());
        }
        let mut store = self.inner.store.lock().await;
        for fact in facts {
            match fact {
                InsertionFact::Edits { kind, paths } => {
                    let change = store.queue_edit_note(&captured, kind, &paths, now_ms());
                    self.publish_fact_note(change)?;
                }
                InsertionFact::NoCheckBeforeEnd => self.queue_fact_note_locked(
                    &mut store, &captured, "no_check_before_end",
                    "The turn changed files and Swarm has no passing configured check covering the latest observed edit.", None,
                )?,
            }
        }
        Ok(())
    }

    async fn deliver_git_base_notice(&self, event: &Event) -> Result<()> {
        let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation) else {
            return Ok(());
        };
        let moved: GitUpstreamMoved = serde_json::from_value(event.data.clone())
            .map_err(|_| Error::new("source_integrity", "invalid retained Git ref movement"))?;
        let Some(upstream_branch) = moved.upstream.branch() else {
            return Err(Error::new(
                "source_integrity",
                "invalid retained Git upstream tip",
            ));
        };
        if !eliot_core::api::is_git_oid(&moved.previous_oid)
            || !eliot_core::api::is_git_oid(&moved.head)
            || moved.branch.len() > 256
            || !eliot_core::api::valid_diff_path(&moved.branch)
        {
            return Err(Error::new(
                "source_integrity",
                "invalid retained Git ref movement",
            ));
        }
        let workspace = {
            let store = self.inner.store.lock().await;
            if !self.inner.monitoring.git_base_notice_config().enabled
                || !matches_git_notice_binding(&store, agent, generation, &moved)?
            {
                return Ok(());
            }
            store.get_workspace(agent)?.filter(|workspace| {
                workspace.requested_branch == moved.branch
                    && eliot_core::workspace_path_key(&workspace.worktree_path)
                        == eliot_core::workspace_path_key(&moved.workspace)
            })
        };
        let checkout = crate::workspace_git::DiffCheckout::open(
            &moved.workspace,
            workspace
                .as_ref()
                .map(|workspace| workspace.repo_common_dir.as_str()),
        )?;
        if !git_notice_refs_current(&checkout, &moved) {
            return Ok(());
        }
        let binding = checkout.binding();
        let scope =
            eliot_core::workspace_path_key(&binding.common_dir.to_string_lossy()).into_owned();
        let key = eliot_core::api::inference::sha256_hex(
            &serde_json::to_vec(&[scope.as_str(), &moved.branch, &moved.upstream.oid])
                .map_err(|_| Error::new("internal", "Git notice identity could not be encoded"))?,
        );
        let thread = format!("git-base:{key}");
        if self
            .inner
            .store
            .lock()
            .await
            .git_base_notice_sent(&thread)?
        {
            return Ok(());
        }
        let git = crate::git_monitor::GitProbe::new()?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(12);
        let base = match workspace.as_ref() {
            Some(workspace) => {
                retired_git_base(&git, &checkout, workspace, &moved, deadline).await?
            }
            None => None,
        };
        let conflicts = if moved.previous_oid != moved.upstream.oid {
            match crate::git_actions::preview_conflicting_paths(
                &self.inner.data_root,
                &git,
                binding.common_dir,
                &moved.upstream.oid,
                &moved.head,
                deadline,
            )
            .await
            {
                Ok(paths) => paths,
                Err(_) if base.is_some() => Vec::new(),
                Err(error) => return Err(error),
            }
        } else {
            Vec::new()
        };
        if base.is_none() && conflicts.is_empty() {
            return Ok(());
        }
        let rechecked = crate::workspace_git::DiffCheckout::open(
            &moved.workspace,
            binding.common_dir.to_str(),
        )?;
        if !checkout.same_identity(&rechecked) || !git_notice_refs_current(&rechecked, &moved) {
            return Ok(());
        }
        let mut store = self.inner.store.lock().await;
        let config = self.inner.monitoring.git_base_notice_config();
        if !config.enabled
            || self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN
            || !matches_git_notice_binding(&store, agent, generation, &moved)?
            || store.git_base_notice_sent(&thread)?
        {
            return Ok(());
        }
        let digest = Sha256::digest(format!(
            "git-base-notice:{}:{agent}:{generation}",
            event.seq
        ));
        let mut entropy = [0; 16];
        entropy[6..].copy_from_slice(&digest[..10]);
        let id = ulid::Ulid::from_parts(
            u64::try_from(event.ts).unwrap_or(0),
            u128::from_be_bytes(entropy),
        );
        let mut text = format!(
            "Branch {} against origin/{} at {}.",
            moved.branch, upstream_branch, moved.upstream.oid
        );
        if let Some(base) = base {
            text.push_str(&format!("\n{base} Verify the dependency before rebasing. The recorded base may be the --onto cutoff."));
        }
        append_git_conflicts(&mut text, &conflicts);
        let request = MailSendRequest {
            id: id.to_string(),
            to: vec![agent.to_owned()],
            mode: MailMode::Queue,
            wake: config.wake
                && (!conflicts.is_empty()
                    || store.git_base_has_queued_input(
                        agent,
                        generation,
                        &moved.workspace,
                        &moved.branch,
                    )?),
            text: eliot_core::swarm_note(
                &self.secret_policy_snapshot().redact_text(&text),
                SWARM_MAIL_SENDER,
            ),
            thread: Some(thread),
            reply_to_delivery: None,
        };
        request.validate()?;
        let change = self.mail_send_locked(&mut store, SWARM_MAIL_SENDER, &request, false)?;
        drop(store);
        self.mail_send_finish(&request, change).await?;
        Ok(())
    }

    pub(super) async fn queue_fact_note_with_id(
        &self,
        event: &Event,
        kind: &str,
        text: &str,
        id: Option<&str>,
    ) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        self.queue_fact_note_locked(&mut store, event, kind, text, id)
    }

    fn queue_fact_note_locked(
        &self,
        store: &mut Store,
        event: &Event,
        kind: &str,
        text: &str,
        id: Option<&str>,
    ) -> Result<()> {
        let text = self.secret_policy_snapshot().redact_text(text);
        self.publish_fact_note(store.queue_fact_note(event, kind, &text, id, now_ms()))
    }

    fn publish_fact_note(&self, change: Result<eliot_store::Change<Option<String>>>) -> Result<()> {
        match change {
            Ok(change) => {
                self.publish_locked(&change.events);
                if let Some(id) = change.value {
                    self.schedule_command(id, None);
                }
                if !change.events.is_empty() {
                    self.signal_progress();
                }
                Ok(())
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "bad_state" | "stale_generation" | "unsupported"
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    fn queue_task_card_locked(
        &self,
        store: &mut Store,
        event: &Event,
        brief: Option<&str>,
        selected_task: Option<&str>,
    ) -> Result<()> {
        if let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation)
            && let Some(text) = super::task_card::read_task_card(
                store,
                agent,
                generation,
                brief,
                selected_task,
                &self.secret_policy_snapshot(),
            )?
        {
            self.queue_fact_note_locked(store, event, "task_card", &text, None)?;
        }
        Ok(())
    }

    async fn selected_task_text_for_card(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<String>> {
        let (workspace, task_ref) = {
            let store = self.inner.store.lock().await;
            let Some(row) = store
                .get_agent(agent)?
                .filter(|row| row.generation == generation && !row.is_terminal())
            else {
                return Ok(None);
            };
            let Some(workspace) = row.workspace else {
                return Ok(None);
            };
            let Some(task_ref) = store.work_task_ref(agent, generation)? else {
                return Ok(None);
            };
            (workspace, task_ref)
        };
        Ok(crate::work_truth::task_record_text(&self.inner.data_root, &workspace, &task_ref).await)
    }

    pub(super) fn queue_handoff_task_card(
        &self,
        store: &mut Store,
        handoff: &HandoffResponse,
        events: &[Event],
        task_text: Option<&str>,
    ) {
        if let Some(trigger) = events
            .iter()
            .find(|event| event.kind == "command.applied" && event.data["command_id"] == handoff.id)
        {
            let mut event = trigger.clone();
            event.agent = Some(handoff.to.clone());
            event.generation = Some(handoff.to_generation);
            self.log_task_card_error(self.queue_task_card_locked(
                store,
                &event,
                Some(&handoff.brief_command),
                task_text,
            ));
        }
    }

    pub(super) async fn queue_resume_task_card(&self, agent: &str, generation: i64) {
        let selected_task = match self.selected_task_text_for_card(agent, generation).await {
            Ok(selected) => selected,
            Err(error) => {
                self.log_task_card_error(Err(error));
                return;
            }
        };
        let mut store = self.inner.store.lock().await;
        let result = store
            .session_ready_event(agent, generation)
            .and_then(|ready| {
                ready.map_or(Ok(()), |event| {
                    self.queue_task_card_locked(&mut store, &event, None, selected_task.as_deref())
                })
            });
        self.log_task_card_error(result);
    }

    fn log_task_card_error(&self, result: Result<()>) {
        if let Err(error) = result {
            crate::logging::record(
                &self.secret_policy_snapshot(),
                "daemon.task_card_unavailable",
                &[],
                Some(&error),
            );
        }
    }

    pub(super) async fn inject_wait(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: ContextWaitRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        let read = json!({"id": request.id});
        let mut poll = self.long_poll(request.wait_ms);
        loop {
            let value = self.command_get(&read, principal).await?;
            if value["verb"] != "inject" {
                return Err(Error::new(
                    "invalid",
                    "inject.wait requires an inject command",
                ));
            }
            let state: CommandState = serde_json::from_value(value["state"].clone())
                .map_err(|_| Error::new("internal", "invalid retained inject state"))?;
            if state.is_terminal()
                || state == CommandState::Unknown
                || value["outcome"]["delivery"] == "moved"
                || value["outcome"]["moved"] == true
                || !poll
                    .wake(|event| {
                        event.data["command_id"] == request.id
                            || event.agent.as_deref() == value["target"].as_str()
                    })
                    .await?
            {
                return Ok(value);
            }
        }
    }
}
