//! Keeps each Agent's work recorded in Git from committed Store facts, and
//! moves work to a successor when its session can no longer continue it.
use super::routing::HandoffRoute;
use super::*;
use crate::work_truth::{self, CheckpointRequest, TaskRecord, TaskSource};
use eliot_core::api::{GitCheckpointState, HandoffReason};
use eliot_store::{
    AutomaticHandoff, BriefFacts, GitCheckpointInput, GitCheckpointIntent,
    GitCheckpointReservation, JournalTurn, TaskInputs, TaskInstruction,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};
use tokio::sync::broadcast;

const JOURNAL_TEXT_BYTES: usize = 4 * 1024;
const JOURNAL_BATCH: usize = 32;
const MAX_TASK_SOURCES: usize = 8;
const MAX_TASK_SOURCE_BYTES: u64 = 256 * 1024;
/// A task text this short is a pointer: the file it names is the task.
const MAX_POINTER_TASK_BYTES: usize = 512;
const HANDOFF_BATCH: usize = 16;
const DEFERRED_CHECKPOINT_BATCH: usize = 256;
/// A turn this old with uncommitted files gets a reminder while it still runs.
const UNCOMMITTED_REMINDER_MS: i64 = 10 * 60 * 1_000;

pub(crate) async fn run_work_truth(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut events = router.inner.bus.subscribe();
    let mut cursor = router.inner.store.lock().await.work_truth_cursor()?;
    router.note_worker_task_running("work_truth");
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let page = {
            let mut store = router.inner.store.lock().await;
            let bounds = store.event_bounds()?;
            if cursor < bounds.pruned_through {
                cursor = bounds.pruned_through;
                store.advance_work_truth_cursor(cursor)?;
            }
            let page = store.read_events(cursor, 128, Some(bounds.hi))?;
            if page.is_empty() && cursor < bounds.hi {
                cursor = bounds.hi;
                store.advance_work_truth_cursor(cursor)?;
            }
            page
        };
        if !page.is_empty() {
            for event in page {
                if *shutdown.borrow() {
                    return Ok(());
                }
                if let Err(error) = router.record_work_fact(&event).await {
                    crate::logging::record(
                        &router.secret_policy_snapshot(),
                        "daemon.work_truth_failed",
                        &[("seq", &event.seq.to_string()), ("kind", &event.kind)],
                        Some(&error),
                    );
                }
                cursor = event.seq;
            }
            router
                .inner
                .store
                .lock()
                .await
                .advance_work_truth_cursor(cursor)?;
            continue;
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            event = events.recv() => {
                if matches!(event, Err(broadcast::error::RecvError::Closed)) {
                    return Err(Error::new("unavailable", "work truth event bus is closed"));
                }
            }
        }
    }
}

/// A controlled Agent working in a project directory.
fn records_work(agent: &Agent) -> bool {
    !agent.observe_only
        && !agent.project.is_empty()
        && agent.workspace.is_some()
        && !agent.is_automatic_provider_probe()
}

fn journal_entries(
    agent: &Agent,
    turns: &[JournalTurn],
    policy: &eliot_core::SecretPolicy,
) -> String {
    let mut text = String::new();
    for turn in turns {
        let at = chrono::DateTime::from_timestamp_millis(turn.completed_at)
            .map_or_else(|| turn.completed_at.to_string(), |at| at.to_rfc3339());
        let report = turn.final_text.as_deref().map_or_else(
            || "(no final report)".to_owned(),
            |report| {
                eliot_core::bound_chars(
                    &work_truth::redact_record(report, policy),
                    JOURNAL_TEXT_BYTES,
                )
            },
        );
        text.push_str(&format!(
            "## {at} {} generation {} on {} turn {} ({})\n{report}\n\n",
            agent.id, agent.generation, agent.profile, turn.turn_id, turn.outcome
        ));
    }
    text
}

/// Absolute text-file paths a task names.
fn referenced_files(text: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = Vec::new();
    for token in text.split_whitespace() {
        let token = token.trim_matches(|c: char| "\"'`()[]<>,;".contains(c));
        let token = token.trim_end_matches([':', '.']);
        let path = Path::new(token);
        let is_text = path
            .extension()
            .and_then(|extension| extension.to_str())
            .is_some_and(|extension| {
                extension.eq_ignore_ascii_case("md") || extension.eq_ignore_ascii_case("txt")
            });
        if is_text && path.is_absolute() && !files.iter().any(|known| known == path) {
            files.push(path.to_owned());
            if files.len() == MAX_TASK_SOURCES {
                break;
            }
        }
    }
    files
}

/// Path, size and hash of each file a task the Owner wrote names. When the
/// task is only a pointer to its file, that file is the task, so its redacted
/// text is saved too. A file holding a known secret gets neither hash nor copy.
fn read_task_files(text: &str, policy: &eliot_core::SecretPolicy) -> (Vec<Value>, Vec<TaskSource>) {
    let named = super::task_card::without_source_block(text);
    let pointer = named.trim().len() <= MAX_POINTER_TASK_BYTES;
    let (mut facts, mut sources) = (Vec::new(), Vec::new());
    for path in referenced_files(named) {
        if work_truth::is_credential_path(&path.to_string_lossy()) {
            continue;
        }
        let Ok(metadata) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        let bytes = (metadata.len() <= MAX_TASK_SOURCE_BYTES)
            .then(|| std::fs::read(&path).ok())
            .flatten()
            .filter(|bytes| {
                bytes.len() as u64 <= MAX_TASK_SOURCE_BYTES
                    && !policy.contains_text(&String::from_utf8_lossy(bytes))
            });
        let sha256 = bytes.as_deref().map(eliot_core::api::inference::sha256_hex);
        let captured = bytes
            .filter(|_| pointer)
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .and_then(|content| {
                let name = source_name(&path, &sources)?;
                sources.push(TaskSource {
                    name: name.clone(),
                    text: work_truth::redact_record(&content, policy),
                });
                Some(format!("sources/{name}"))
            });
        facts.push(json!({
            "path": path.to_string_lossy(),
            "bytes": metadata.len(),
            "sha256": sha256,
            "captured": captured,
        }));
    }
    (facts, sources)
}

/// A distinct tree entry name for a saved task file.
fn source_name(path: &Path, taken: &[TaskSource]) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    if name.chars().any(char::is_control) {
        return None;
    }
    let mut candidate = name.to_owned();
    let mut index = 1;
    while taken.iter().any(|source| source.name == candidate) {
        index += 1;
        candidate = format!("{index}-{name}");
    }
    Some(candidate)
}

impl Router {
    async fn record_work_fact(&self, event: &Event) -> Result<()> {
        let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation) else {
            return Ok(());
        };
        match (event.kind.as_str(), event.source.as_str()) {
            ("session.ready", "stream") => self.ensure_work_task(agent, generation, None).await,
            ("turn.started", "stream") => {
                let task_command = match event.data["turn_id"].as_str() {
                    Some(turn) => self
                        .inner
                        .store
                        .lock()
                        .await
                        .turn_command_id(agent, generation, turn)?,
                    None => None,
                };
                self.ensure_work_task(agent, generation, task_command.as_deref())
                    .await
            }
            ("turn.completed", "stream") => {
                let turn = event.data["turn_id"].as_str().map(str::to_owned);
                let checkpoint = self
                    .checkpoint_work(agent, generation, "turn_end", turn)
                    .await?;
                if let Some(input) =
                    checkpoint.filter(|input| input.state == GitCheckpointState::Snapshot)
                {
                    self.remind_uncommitted(event, input.paths.len()).await?;
                }
                Ok(())
            }
            ("git.changed", "git") => {
                let dirty = event.data["dirty"].as_array().map_or(0, Vec::len);
                if dirty == 0 {
                    return Ok(());
                }
                let turn = {
                    let store = self.inner.store.lock().await;
                    let active = store
                        .get_agent(agent)?
                        .filter(|row| row.generation == generation && records_work(row))
                        .and_then(|row| row.active_turn);
                    match active {
                        Some(turn) => store
                            .get_turn(agent, generation, &turn)?
                            .filter(|record| {
                                record.started_at.is_some_and(|started| {
                                    started + UNCOMMITTED_REMINDER_MS <= event.ts
                                })
                            })
                            .map(|_| turn),
                        None => None,
                    }
                };
                let Some(turn) = turn else {
                    return Ok(());
                };
                let due = self
                    .inner
                    .store
                    .lock()
                    .await
                    .git_checkpoint_basis(agent, generation)?
                    .updated_at
                    .is_none_or(|at| at + UNCOMMITTED_REMINDER_MS <= event.ts);
                if due {
                    self.checkpoint_work(agent, generation, "long_turn", Some(turn.clone()))
                        .await?;
                }
                let mut captured = event.clone();
                captured.data["turn_id"] = json!(turn);
                self.remind_uncommitted(&captured, dirty).await
            }
            ("session.ended", "stream") | ("agent.lost", _) => {
                self.checkpoint_ended_session(agent, generation).await
            }
            _ => Ok(()),
        }
    }

    /// The final checkpoint of a session runs only after its whole process
    /// tree is proven gone. Until then a harness child may still be writing.
    async fn checkpoint_ended_session(&self, agent: &str, generation: i64) -> Result<()> {
        let proven = self
            .inner
            .store
            .lock()
            .await
            .session_exit_proven(agent, generation)?;
        self.inner
            .store
            .lock()
            .await
            .defer_session_checkpoint(agent, generation, now_ms())?;
        if proven
            && self
                .checkpoint_work(agent, generation, "session_end", None)
                .await?
                .is_none()
        {
            self.inner
                .store
                .lock()
                .await
                .clear_session_checkpoint(agent, generation)?;
        }
        Ok(())
    }

    /// Take each deferred final checkpoint whose exit proof has arrived.
    pub(crate) async fn finish_session_checkpoints(&self) -> Result<()> {
        let pending = self
            .inner
            .store
            .lock()
            .await
            .deferred_session_checkpoints(DEFERRED_CHECKPOINT_BATCH)?;
        for (agent, generation) in pending {
            let (current, proven) = {
                let store = self.inner.store.lock().await;
                (
                    store.get_agent(&agent)?.map(|row| row.generation),
                    store.session_exit_proven(&agent, generation)?,
                )
            };
            if current != Some(generation) {
                // A newer generation owns the worktree and its checkpoints.
                self.inner
                    .store
                    .lock()
                    .await
                    .clear_session_checkpoint(&agent, generation)?;
            } else if proven
                && self
                    .checkpoint_work(&agent, generation, "session_end", None)
                    .await?
                    .is_none()
            {
                self.inner
                    .store
                    .lock()
                    .await
                    .clear_session_checkpoint(&agent, generation)?;
            }
        }
        Ok(())
    }

    /// One reminder per turn. The daemon's checkpoint protects the files; only
    /// the Agent's own commits carry them to review and merge.
    async fn remind_uncommitted(&self, event: &Event, files: usize) -> Result<()> {
        let text = format!(
            "{files} uncommitted file(s) in this worktree. Swarm saved a private checkpoint, but only your commits reach review and merge. Commit on this branch at your next meaningful checkpoint."
        );
        self.queue_fact_note_with_id(event, "uncommitted_work", &text, None)
            .await
    }

    /// Record the worktree now. Unchanged work writes no Git object.
    pub(super) async fn checkpoint_work(
        &self,
        agent: &str,
        generation: i64,
        trigger: &'static str,
        turn_id: Option<String>,
    ) -> Result<Option<GitCheckpointInput>> {
        self.checkpoint_work_in_lane(agent, generation, trigger, turn_id, None)
            .await
    }

    pub(super) async fn checkpoint_work_in_lane(
        &self,
        agent: &str,
        generation: i64,
        trigger: &'static str,
        turn_id: Option<String>,
        removal_command: Option<&str>,
    ) -> Result<Option<GitCheckpointInput>> {
        let row = self.inner.store.lock().await.get_agent(agent)?;
        let Some(row) = row.filter(|row| row.generation == generation && records_work(row)) else {
            return Ok(None);
        };
        let workspace = row.workspace.clone().unwrap_or_default();
        if row.branch.is_none() && work_truth::current_branch(&workspace).await.is_none() {
            return Ok(None);
        }
        let (_, common_dir) = crate::workspace_git::WorkspaceGit::new()?
            .repository_identity(&workspace)
            .await?;
        let limits = crate::config::load_daemon_config(&self.inner.data_root)
            .unwrap_or_default()
            .checkpoint_limits();
        let mut intent = GitCheckpointIntent {
            common_dir,
            agent: agent.to_owned(),
            generation,
            workspace: workspace.clone(),
            input: GitCheckpointInput::failed(
                trigger,
                turn_id.clone(),
                "checkpoint capture was interrupted before the exact byte count was recorded"
                    .to_owned(),
            ),
        };
        let mut progress = self.inner.progress.subscribe();
        let basis = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                {
                    let mut store = self.inner.store.lock().await;
                    if store.get_agent(agent)?.is_none_or(|current| {
                        current.generation != generation
                            || current.workspace.as_deref() != Some(workspace.as_str())
                    }) {
                        return Ok::<_, Error>(None);
                    }
                    let basis = store.git_checkpoint_basis(agent, generation)?;
                    intent.input.task_ref = store.work_task_ref(agent, generation)?;
                    intent.input.captured_bytes = limits
                        .generation_bytes
                        .saturating_sub(basis.captured_bytes)
                        .min(limits.total_bytes);
                    match store.reserve_git_checkpoint(&intent, removal_command)? {
                        GitCheckpointReservation::Reserved => return Ok(Some(basis)),
                        GitCheckpointReservation::Skipped => return Ok(None),
                        GitCheckpointReservation::Deferred => {}
                    }
                }
                progress.changed().await.map_err(|_| {
                    Error::new("unavailable", "checkpoint repository write lane closed")
                })?;
            }
        })
        .await
        .map_err(|_| {
            Error::new(
                "repository_busy",
                "checkpoint repository write lane is busy",
            )
        })??;
        let Some(basis) = basis else {
            return Ok(None);
        };
        let first = basis.fingerprint.is_none();
        let mut input = work_truth::checkpoint(CheckpointRequest {
            data_root: &self.inner.data_root,
            workspace: &workspace,
            agent,
            generation,
            label: now_ms(),
            trigger,
            turn_id,
            previous: basis.fingerprint,
            previous_snapshot: basis.snapshot,
            generation_used: basis.captured_bytes,
            limits,
            policy: self.secret_policy_snapshot(),
        })
        .await;
        input.task_ref = intent.input.task_ref;
        self.inner
            .store
            .lock()
            .await
            .stage_git_checkpoint(agent, generation, &input)?;
        if let (Some(reference), Some(oid)) = (&input.checkpoint_ref, &input.oid) {
            let root = crate::work_truth_repo::existing(&self.inner.data_root).await?;
            work_truth::publish_checkpoint(&root, reference, oid).await?;
            self.reconcile_checkpoint_ref(agent, generation, &mut input)
                .await?;
        }
        // A turn that changed nothing adds nothing to Git. Its report joins
        // the journal with the next change or when the session ends.
        let changed = match input.state {
            GitCheckpointState::Snapshot => true,
            GitCheckpointState::Clean => !first || input.head != row.base_oid,
            GitCheckpointState::Unchanged | GitCheckpointState::Failed => false,
        };
        if changed || trigger != "turn_end" {
            let (task, turns) = {
                let store = self.inner.store.lock().await;
                (
                    store.work_task_ref(agent, generation)?,
                    store.journal_turns(agent, generation, JOURNAL_BATCH)?,
                )
            };
            if let (Some(task), false) = (task, turns.is_empty()) {
                let policy = self.secret_policy_snapshot();
                let entries = journal_entries(&row, &turns, &policy);
                if work_truth::append_journal(&self.inner.data_root, &workspace, &task, &entries)
                    .await
                    .is_ok()
                {
                    input.journaled_through = turns.last().map(|turn| turn.completed_at);
                }
            }
        }
        let mut store = self.inner.store.lock().await;
        let events = store.record_git_checkpoint(agent, generation, &input, now_ms())?;
        self.publish_locked(&events);
        self.signal_progress();
        Ok(Some(input))
    }

    async fn reconcile_checkpoint_ref(
        &self,
        agent: &str,
        generation: i64,
        input: &mut GitCheckpointInput,
    ) -> Result<()> {
        if input.state != GitCheckpointState::Snapshot {
            return Ok(());
        }
        let (Some(reference), Some(expected)) = (&input.checkpoint_ref, &input.oid) else {
            return Err(Error::new(
                "source_integrity",
                "checkpoint intent has no ref or OID",
            ));
        };
        let root = crate::work_truth_repo::existing(&self.inner.data_root).await?;
        match work_truth::checkpoint_reference(&root, reference).await? {
            Some(actual) if &actual == expected => {
                let protected = self
                    .inner
                    .store
                    .lock()
                    .await
                    .checkpoint_restore_refs(eliot_core::parse_agent_id(agent)?, generation)?;
                if let Err(error) =
                    work_truth::prune_checkpoints(&root, reference, &protected).await
                {
                    self.log_work_truth_error("daemon.checkpoint_prune_failed", &error);
                }
            }
            Some(_) => {
                return Err(Error::new(
                    "source_integrity",
                    "checkpoint ref differs from its intent",
                ));
            }
            None => {
                input.state = GitCheckpointState::Failed;
                input.fingerprint = None;
                input.error = Some("checkpoint ref was not published".to_owned());
                input.checkpoint_ref = None;
                input.oid = None;
            }
        }
        Ok(())
    }

    /// Run before accepting work. Reconcile the exact local ref, without
    /// repeating a capture or ref publication from a previous daemon.
    pub(crate) async fn recover_git_checkpoints(&self) -> Result<()> {
        let intents = self.inner.store.lock().await.git_checkpoint_intents()?;
        for GitCheckpointIntent {
            agent,
            generation,
            mut input,
            ..
        } in intents
        {
            if let Err(error) = self
                .reconcile_checkpoint_ref(&agent, generation, &mut input)
                .await
            {
                self.log_work_truth_error("daemon.checkpoint_recovery_failed", &error);
                continue;
            }
            let mut store = self.inner.store.lock().await;
            let events = store.record_git_checkpoint(&agent, generation, &input, now_ms())?;
            self.publish_locked(&events);
        }
        Ok(())
    }

    /// Bind this generation to the exact original input's Git record.
    pub(super) async fn ensure_work_task(
        &self,
        agent: &str,
        generation: i64,
        task_command: Option<&str>,
    ) -> Result<()> {
        let (row, known, selected, command, goal) = {
            let mut store = self.inner.store.lock().await;
            let Some(row) = store
                .get_agent(agent)?
                .filter(|row| row.generation == generation && records_work(row))
            else {
                return Ok(());
            };
            let known = store.work_task_ref(agent, generation)?;
            let selected = match task_command.filter(|id| !id.is_empty()) {
                Some(id) => store.select_work_task(agent, generation, id, now_ms())?,
                None => None,
            };
            let command = match task_command.filter(|id| !id.is_empty()) {
                Some(_) if selected.is_some() => None,
                Some(id) => store.work_task_send(agent, id)?,
                None if known.is_none() => store.first_work_task_send(agent)?,
                None => None,
            };
            let goal = match store
                .goal_get(agent, "eliot")?
                .filter(|goal| goal.generation == generation)
            {
                Some(goal) => Some(goal.definition),
                None => store.launch_goal_definition(agent, generation)?,
            };
            (row, known, selected, command, goal)
        };
        if selected.is_some() {
            return Ok(());
        }
        let Some(command) = command else {
            return Ok(());
        };
        let task_ref = work_truth::task_ref(&command);
        if known.as_deref() == Some(task_ref.as_str()) {
            return Ok(());
        }
        let workspace = row.workspace.clone().unwrap_or_default();
        let managed = self
            .inner
            .store
            .lock()
            .await
            .worktree_branch_base(&workspace)?;
        let base = managed
            .as_ref()
            .map(|(_, base)| base.clone())
            .or_else(|| row.base_oid.clone());
        let branch = match managed.map(|(branch, _)| branch).or(row.branch.clone()) {
            Some(branch) => Some(branch),
            None => work_truth::current_branch(&workspace).await,
        };
        let Some(branch) = branch else {
            return Ok(());
        };
        let policy = self.secret_policy_snapshot();
        let raw = eliot_store::input_before_owner_rules(&command).unwrap_or_default();
        let text = work_truth::redact_record(raw, &policy);
        let owner_authored = !row
            .profile_choice
            .as_ref()
            .is_some_and(|choice| choice.owner_automation)
            && self
                .inner
                .store
                .lock()
                .await
                .command_is_owner_authored(&command.id)?;
        let (referenced, sources) = if owner_authored {
            let (owned, reader) = (raw.to_owned(), Arc::clone(&policy));
            tokio::task::spawn_blocking(move || read_task_files(&owned, &reader))
                .await
                .map_err(|_| Error::new("internal", "task file reader failed"))?
        } else {
            (Vec::new(), Vec::new())
        };
        let mut card = json!({
            "task_command": command.id,
            "branch": branch,
            "base": base,
            "agent": row.id,
            "generation": row.generation,
            "profile": row.profile,
            "goal": goal.as_ref().map(|goal| &goal.text),
            "check": goal.as_ref().map(|goal| &goal.check),
            "referenced_files": referenced,
            "recorded_at": now_ms(),
        });
        work_truth::redact_record_value(&mut card, &policy);
        work_truth::ensure_task(
            &self.inner.data_root,
            &workspace,
            &TaskRecord {
                task_ref: task_ref.clone(),
                text,
                card,
                sources,
                inputs: TaskInputs {
                    original: TaskInstruction::from_command(&command, String::new()),
                    ..TaskInputs::default()
                },
            },
        )
        .await?;
        self.inner.store.lock().await.record_work_task(
            agent,
            generation,
            &task_ref,
            &command.id,
            &command.principal,
            now_ms(),
        )
    }

    /// Freeze the mandatory task material before a switch dispatches STOP.
    async fn prepare_handoff_task(
        &self,
        source: &Agent,
        task_command: Option<&str>,
    ) -> Result<BriefFacts> {
        if !records_work(source) {
            return Ok(BriefFacts::default());
        }
        self.ensure_work_task(&source.id, source.generation, task_command)
            .await?;
        let (reference, command) = {
            let store = self.inner.store.lock().await;
            (
                store.work_task_ref(&source.id, source.generation)?,
                store.work_task_command(&source.id, source.generation)?,
            )
        };
        match (reference, command) {
            (Some(reference), Some(command)) => {
                work_truth::read_handoff_task(
                    &self.inner.data_root,
                    &reference,
                    command,
                    &self.secret_policy_snapshot(),
                )
                .await
            }
            _ => Ok(BriefFacts::default()),
        }
    }

    /// Freeze task, delivery and Git facts before any switch crosses STOP.
    pub(super) async fn prepare_handoff_facts(
        &self,
        source: &Agent,
        selected_task: Option<&str>,
    ) -> Result<BriefFacts> {
        let mut facts = self.prepare_handoff_task(source, selected_task).await?;
        let Some(workspace) = source.workspace.as_deref().filter(|_| records_work(source)) else {
            return Ok(facts);
        };
        let agent = &source.id;
        let policy = self.secret_policy_snapshot();
        if let (Some(reference), Some(command)) =
            (facts.task_ref.as_deref(), facts.task_command.as_deref())
        {
            let (original, mut corrections, omitted, history_unresolved) = {
                let store = self.inner.store.lock().await;
                let original_id = facts
                    .task_inputs
                    .original
                    .as_ref()
                    .map_or(command, |input| input.command_id.as_str());
                let original = store
                    .get_command(original_id)?
                    .and_then(|row| TaskInstruction::from_command(&row, String::new()));
                let mut corrections = Vec::new();
                for saved in &facts.task_inputs.corrections {
                    if let Some(row) = store.get_command(&saved.command_id)?
                        && row.target.as_deref() == Some(&saved.source)
                        && row.generation == Some(saved.generation)
                        && row.principal == saved.principal
                        && row.verb == saved.verb
                        && row.state != saved.state
                    {
                        let mut refreshed = saved.clone();
                        refreshed.state = row.state;
                        corrections.push(refreshed);
                    }
                }
                let mut omitted = BTreeMap::new();
                if facts.task_inputs.history_incomplete
                    && let Some(origin) = facts.task_inputs.original.as_ref()
                    && origin.principal == "unverified"
                    && (origin.source != *agent || origin.generation != source.generation)
                {
                    let (prior, missing) = store.task_owner_corrections(
                        &origin.source,
                        origin.generation,
                        &origin.command_id,
                        &policy,
                    )?;
                    corrections.extend(prior);
                    if let Some(missing) = missing {
                        omitted.insert(format!("{}/{}", origin.source, origin.generation), missing);
                    }
                }
                let (current, missing) =
                    store.task_owner_corrections(agent, source.generation, command, &policy)?;
                corrections.extend(current);
                if let Some(missing) = missing {
                    omitted.insert(format!("{agent}/{}", source.generation), missing);
                }
                let history_unresolved = store.handoff_history_unresolved(agent)?;
                (original, corrections, omitted, history_unresolved)
            };
            for correction in &mut corrections {
                correction.text = work_truth::redact_record(&correction.text, &policy);
            }
            facts.task_inputs = work_truth::record_task_inputs(
                &self.inner.data_root,
                reference,
                original,
                corrections,
                omitted,
                history_unresolved,
            )
            .await?;
        }
        // The post-STOP checkpoint can advance this retained reference. It
        // cannot change or invalidate the frozen input.
        let (recorded, base) = {
            let store = self.inner.store.lock().await;
            (
                store
                    .agent_work_truth(agent, source.generation)
                    .ok()
                    .flatten(),
                store
                    .worktree_branch_base(workspace)
                    .ok()
                    .flatten()
                    .map(|(_, base)| base)
                    .or_else(|| source.base_oid.clone()),
            )
        };
        let checkpoint = recorded.and_then(|view| view.checkpoint);
        facts.git = match work_truth::handoff_facts(
            workspace,
            base.as_deref(),
            facts.task_ref.as_deref(),
            checkpoint.as_ref(),
            &policy,
        )
        .await
        {
            Ok(facts) => Some(facts),
            Err(error) => {
                self.log_work_truth_error("daemon.handoff_facts_failed", &error);
                None
            }
        };
        Ok(facts)
    }

    pub(super) async fn checkpoint_handoff_source(&self, source: &Agent) {
        if records_work(source)
            && let Err(error) = self
                .checkpoint_work(&source.id, source.generation, "handoff", None)
                .await
        {
            self.log_work_truth_error("daemon.handoff_checkpoint_failed", &error);
        }
    }

    fn log_work_truth_error(&self, name: &str, error: &Error) {
        crate::logging::record(&self.secret_policy_snapshot(), name, &[], Some(error));
    }

    /// Move owed work off a held or ended session or a withheld response.
    /// Nothing is reserved until a successor is actually eligible.
    pub(crate) async fn run_automatic_handoffs(&self) -> Result<()> {
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Ok(());
        }
        let routing = crate::config::load_routing_config(&self.inner.data_root)?;
        let candidates = self.inner.store.lock().await.automatic_handoff_candidates(
            self.inner.started_at_ms,
            now_ms(),
            HANDOFF_BATCH,
            routing.auto_handoff(),
        )?;
        for candidate in candidates {
            let enabled = match candidate.reason {
                HandoffReason::ProviderHold => self.inner.attention.provider_refusal.enabled,
                // Check the route even when automation is disabled so the
                // Owner sees the outstanding provider decision.
                HandoffReason::ProviderResponseWithheld => true,
                _ => self.inner.attention.agent_lost.enabled,
            };
            if enabled && let Err(error) = self.admit_automatic_handoff(&candidate).await {
                self.log_work_truth_error("daemon.automatic_handoff_failed", &error);
            }
        }
        Ok(())
    }

    async fn admit_automatic_handoff(&self, candidate: &AutomaticHandoff) -> Result<()> {
        let route = HandoffRoute {
            reason: candidate.reason,
            kind: candidate.kind,
            role: None,
            allowed: None,
        };
        let mut probe = SubmitCommand::new(String::new(), "launch");
        if let Err(error) = self
            .route_handoff(&mut probe, &candidate.agent, &route)
            .await
        {
            let Some((message, blocked)) =
                super::routing::handoff_waiting_message(&error, candidate.reason)
            else {
                return Err(error);
            };
            let mut store = self.inner.store.lock().await;
            let events = store.record_handoff_waiting(candidate, &message, blocked, now_ms())?;
            self.publish_locked(&events);
            return Ok(());
        }
        let policy = self.secret_policy_snapshot();
        let reserved = {
            let mut store = self.inner.store.lock().await;
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Ok(());
            }
            let change = store.reserve_automatic_handoff(candidate, &policy, now_ms())?;
            self.publish_locked(&change.events);
            if let Some(value) = &change.value {
                self.schedule_command(value.id.clone(), None);
            }
            change.value.is_some()
        };
        if reserved {
            self.signal_progress();
            self.signal_launch_arbitration();
        }
        Ok(())
    }
}
