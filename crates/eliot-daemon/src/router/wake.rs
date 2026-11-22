//! Agent-owned programmatic wake conditions (#833 item 3).

use super::{
    Principal, Router, WaitScope, coordination::decode_coordination_request, principal_name,
};
use crate::common::now_ms;
use eliot_core::{
    Command, CommandState, Error, ProcessState, Result, SecretPolicy, StderrSnapshot,
    api::{AgentWakeCondition, AgentWakeRequest, SwarmWaitRequest, WaitTurnRef},
};
use eliot_store::{
    AgentWakeRecord, AgentWakeRunReceipt as StoreRunReceipt, AgentWakeTurnReadiness,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Component, Path},
};
use tokio::{
    sync::watch,
    time::{Duration, MissedTickBehavior, interval},
};

const WAKE_POLL_INTERVAL: Duration = Duration::from_millis(250);
const WAKE_PRUNE_INTERVAL_MS: i64 = 60_000;
const WAKE_PAGE_SIZE: usize = 128;
const WAKE_RUN_STDERR_TAIL_BYTES: usize = 512;
const WAKE_FACT_MAX_BYTES: usize = 1_024;

impl Router {
    pub(super) async fn agent_wake(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: AgentWakeRequest = decode_coordination_request(
            params,
            &self.inner.secrets,
            "agent.wake",
            AgentWakeRequest::validate,
        )?;
        let Principal::Agent { .. } = principal else {
            return Err(Error::new("not_allowed", "agent.wake is Agent-only"));
        };
        let name = principal_name(principal);
        let response = {
            let mut store = self.inner.store.lock().await;
            store.agent_wake_arm(&name, &request, now_ms())?
        };
        self.signal_progress();
        serde_json::to_value(response).map_err(|_| {
            Error::new(
                "internal",
                "Agent wake acknowledgement could not be encoded",
            )
        })
    }

    async fn agent_wake_page(&self, after: Option<(i64, String)>) -> Result<Vec<AgentWakeRecord>> {
        self.inner
            .store
            .lock()
            .await
            .agent_wake_page(after, WAKE_PAGE_SIZE)
    }

    async fn process_agent_wake(&self, wake: &AgentWakeRecord) -> Result<()> {
        let now = now_ms();
        let equal_seconds_deadline = seconds_condition_is_deadline(wake);
        if wake.state == "waiting" && now >= wake.expires_at && !equal_seconds_deadline {
            let _ =
                self.inner
                    .store
                    .lock()
                    .await
                    .agent_wake_expire(&wake.id, wake.generation, now)?;
            self.signal_progress();
            return Ok(());
        }
        if wake.state == "waiting" {
            match self.agent_wake_condition_fact(wake, now).await? {
                WakeFact::Pending => {
                    if now >= wake.expires_at {
                        let _ = self.inner.store.lock().await.agent_wake_expire(
                            &wake.id,
                            wake.generation,
                            now,
                        )?;
                        self.signal_progress();
                    }
                    return Ok(());
                }
                WakeFact::NoMatch => {
                    let _ = self.inner.store.lock().await.agent_wake_finish(
                        &wake.id,
                        wake.generation,
                        None,
                        "not_matched",
                        now,
                    )?;
                    self.signal_progress();
                    return Ok(());
                }
                WakeFact::Unknown => {
                    let _ = self.inner.store.lock().await.agent_wake_mark_unknown(
                        &wake.id,
                        wake.generation,
                        None,
                        now,
                    )?;
                    self.signal_progress();
                    return Ok(());
                }
                WakeFact::Stale => {
                    let _ = self.inner.store.lock().await.agent_wake_mark_stale(
                        &wake.id,
                        wake.generation,
                        now,
                    )?;
                    self.signal_progress();
                    return Ok(());
                }
                WakeFact::Satisfied(fact) => {
                    let changed = self.inner.store.lock().await.agent_wake_satisfy(
                        &wake.id,
                        wake.generation,
                        &fact,
                        now,
                        equal_seconds_deadline,
                    )?;
                    self.signal_progress();
                    if changed {
                        let mut satisfied = wake.clone();
                        satisfied.state = "satisfied".to_owned();
                        satisfied.fact = Some(fact);
                        return self.process_agent_wake_delivery(&satisfied, now).await;
                    }
                    return Ok(());
                }
            }
        }
        self.process_agent_wake_delivery(wake, now).await
    }

    async fn agent_wake_condition_fact(
        &self,
        wake: &AgentWakeRecord,
        now: i64,
    ) -> Result<WakeFact> {
        let readiness = self.inner.store.lock().await.agent_wake_turn_readiness(
            &wake.agent,
            wake.generation,
            &wake.turn_id,
        )?;
        if readiness == AgentWakeTurnReadiness::Stale {
            return Ok(WakeFact::Stale);
        }
        match &wake.condition {
            AgentWakeCondition::FileAppears { path } => {
                let workspace = {
                    let store = self.inner.store.lock().await;
                    let Some(agent) = store.get_agent(&wake.agent)? else {
                        return Ok(WakeFact::Stale);
                    };
                    if agent.generation != wake.generation || agent.is_terminal() {
                        return Ok(WakeFact::Stale);
                    }
                    let Some(workspace) = store.get_workspace(&wake.agent)? else {
                        return Ok(WakeFact::Stale);
                    };
                    if workspace.target_generation != wake.generation
                        || workspace.phase != eliot_core::WorkspacePhase::Ready
                        || agent.workspace.as_deref() != Some(workspace.worktree_path.as_str())
                        || agent.workspace_phase != Some(eliot_core::WorkspacePhase::Ready)
                    {
                        return Ok(WakeFact::Stale);
                    }
                    workspace
                };
                match file_exists_in_workspace(&workspace.worktree_path, path) {
                    Ok(true) => Ok(WakeFact::Satisfied(format!("file appeared: {path}"))),
                    Ok(false) => Ok(WakeFact::Pending),
                    Err(error) if error.code == "stale_generation" => Ok(WakeFact::Stale),
                    Err(error) => Err(error),
                }
            }
            AgentWakeCondition::ProcessExit { identity } => {
                let record = self.inner.store.lock().await.agent_wake_process(
                    &wake.agent,
                    wake.generation,
                    identity,
                )?;
                let Some(record) = record else {
                    return Ok(WakeFact::Pending);
                };
                if record.state != ProcessState::Exited {
                    return Ok(WakeFact::Pending);
                }
                Ok(WakeFact::Satisfied(format!(
                    "process {} (pid {}, start {}) exited with code {:?}",
                    identity.host_boot_id, identity.pid, identity.start_time, record.exit_code
                )))
            }
            AgentWakeCondition::ArgvExit0 { run_id } => {
                let receipt = self
                    .inner
                    .store
                    .lock()
                    .await
                    .agent_wake_run_receipt(run_id)?;
                match receipt {
                    StoreRunReceipt::Pending => Ok(WakeFact::Pending),
                    StoreRunReceipt::ExitZero => Ok(WakeFact::Satisfied(format!(
                        "authorized Run {run_id} has a retained exit-zero, quiescent receipt"
                    ))),
                    StoreRunReceipt::ExitNonZero { exit_code, stderr } => Ok(WakeFact::Satisfied(
                        agent_wake_run_nonzero_fact(run_id, exit_code, stderr, &self.inner.secrets),
                    )),
                    StoreRunReceipt::NoMatch => Ok(WakeFact::NoMatch),
                    StoreRunReceipt::Unknown => Ok(WakeFact::Unknown),
                }
            }
            AgentWakeCondition::ChildCompletion { turn } => {
                self.child_completion_fact(wake, turn).await
            }
            AgentWakeCondition::Seconds { seconds } => {
                let elapsed_ms = i64::try_from(*seconds)
                    .unwrap_or(i64::MAX)
                    .saturating_mul(1_000);
                if wake.created_at.saturating_add(elapsed_ms) <= now {
                    Ok(WakeFact::Satisfied(format!("{seconds} seconds elapsed")))
                } else {
                    Ok(WakeFact::Pending)
                }
            }
        }
    }

    async fn child_completion_fact(
        &self,
        wake: &AgentWakeRecord,
        child_turn: &WaitTurnRef,
    ) -> Result<WakeFact> {
        let marks = self.wait_unknown_marks().await;
        let request = SwarmWaitRequest::new(Vec::new(), vec![child_turn.clone()], "all", 0);
        let snapshot = {
            let store = self.inner.store.lock().await;
            let Some(parent) = store.get_agent(&wake.agent)? else {
                return Ok(WakeFact::Stale);
            };
            if parent.generation != wake.generation || parent.is_terminal() {
                return Ok(WakeFact::Stale);
            }
            let Some(child) = store.get_agent(&child_turn.agent)? else {
                return Ok(WakeFact::Stale);
            };
            if child.parent.as_deref() != Some(wake.agent.as_str())
                || child.generation != child_turn.generation
            {
                return Ok(WakeFact::Stale);
            }
            Self::wait_snapshot_locked(
                &store,
                &request,
                &Principal::Agent {
                    id: wake.agent.clone(),
                    generation: wake.generation,
                },
                None,
                WaitScope::default(),
                &marks,
            )?
        };
        let Some(result) = snapshot.results.first() else {
            return Ok(WakeFact::Unknown);
        };
        match result.status.as_str() {
            "still_running" => Ok(WakeFact::Pending),
            "unknown" => Ok(WakeFact::Pending),
            "not_found" | "not_visible" => Ok(WakeFact::Stale),
            status => Ok(WakeFact::Satisfied(format!(
                "child {}#{} turn {} finished with status {status}",
                child_turn.agent, child_turn.generation, child_turn.turn_id
            ))),
        }
    }

    async fn process_agent_wake_delivery(&self, wake: &AgentWakeRecord, now: i64) -> Result<()> {
        let command_id = wake.delivery_command_id.as_str();
        let existing = self.inner.store.lock().await.get_command(command_id)?;
        if let Some(command) = existing {
            if !matches_wake_delivery(&command, wake) {
                let _ = self.inner.store.lock().await.agent_wake_mark_unknown(
                    &wake.id,
                    wake.generation,
                    Some(command_id),
                    now,
                )?;
                return Ok(());
            }
            self.record_wake_delivery_state(wake, command_id, command.state, now)
                .await?;
            return Ok(());
        }
        if wake.state == "delivery_pending" {
            let _ = self.inner.store.lock().await.agent_wake_mark_unknown(
                &wake.id,
                wake.generation,
                Some(command_id),
                now,
            )?;
            return Ok(());
        }
        match self.inner.store.lock().await.agent_wake_turn_readiness(
            &wake.agent,
            wake.generation,
            &wake.turn_id,
        )? {
            AgentWakeTurnReadiness::Waiting => {
                if now >= wake.expires_at {
                    let _ = self.inner.store.lock().await.agent_wake_expire(
                        &wake.id,
                        wake.generation,
                        now,
                    )?;
                    self.signal_progress();
                }
                return Ok(());
            }
            AgentWakeTurnReadiness::Stale => {
                let _ = self.inner.store.lock().await.agent_wake_mark_stale(
                    &wake.id,
                    wake.generation,
                    now,
                )?;
                self.signal_progress();
                return Ok(());
            }
            AgentWakeTurnReadiness::Ready => {}
        }
        if now < wake.delivery_next_at {
            return Ok(());
        }
        let Some(fact) = wake.fact.as_deref() else {
            let _ = self.inner.store.lock().await.agent_wake_mark_unknown(
                &wake.id,
                wake.generation,
                None,
                now,
            )?;
            return Ok(());
        };
        let params = json!({
            "id": command_id,
            "target": wake.agent,
            "verb": "send",
            "expected_generation": wake.generation,
            "args": { "text": format!("Wake condition met: {fact}") },
        });
        let submitted = self
            .submit(&params, &Principal::Owner, "command.submit")
            .await;
        let command = self.inner.store.lock().await.get_command(command_id)?;
        if let Some(command) = command {
            self.record_wake_delivery_state(wake, command_id, command.state, now)
                .await?;
            return Ok(());
        }
        let Err(error) = submitted else {
            // The write response preceded the Store read. A missing row here
            // is a consistency gap, so keep the stable id and check it again.
            return Err(Error::new(
                "source_integrity",
                "wake delivery acknowledgement has no retained command",
            ));
        };
        // Store commits admission before native dispatch. With an error and no
        // command row, this is a definite pre-admission refusal; retry only
        // this case and retain the same stable command id.
        let error_text = self
            .inner
            .secrets
            .redact_bounded(&format!("{}: {}", error.code, error.message), 384);
        let failure_reason = error_text
            .chars()
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .collect::<String>();
        let _ = self
            .inner
            .store
            .lock()
            .await
            .agent_wake_delivery_pre_admission_failure(
                &wake.id,
                wake.generation,
                now,
                &failure_reason,
            )?;
        self.signal_progress();
        Ok(())
    }

    async fn record_wake_delivery_state(
        &self,
        wake: &AgentWakeRecord,
        command_id: &str,
        state: CommandState,
        now: i64,
    ) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        match state {
            CommandState::Applied => {
                let _ = store.agent_wake_finish(
                    &wake.id,
                    wake.generation,
                    Some(command_id),
                    "delivered",
                    now,
                )?;
            }
            CommandState::Accepted | CommandState::Queued | CommandState::Dispatched => {
                let _ = store.agent_wake_mark_delivery_pending(
                    &wake.id,
                    wake.generation,
                    command_id,
                )?;
            }
            CommandState::Unknown => {
                let _ = store.agent_wake_mark_unknown(
                    &wake.id,
                    wake.generation,
                    Some(command_id),
                    now,
                )?;
            }
            CommandState::Rejected | CommandState::Failed | CommandState::Cancelled => {
                let _ = store.agent_wake_finish(
                    &wake.id,
                    wake.generation,
                    Some(command_id),
                    "failed",
                    now,
                )?;
            }
        }
        self.signal_progress();
        Ok(())
    }

    async fn prune_agent_wakes(&self, now: i64) -> Result<()> {
        self.inner
            .store
            .lock()
            .await
            .agent_wake_prune(now, WAKE_PAGE_SIZE)?;
        Ok(())
    }
}

enum WakeFact {
    Pending,
    Satisfied(String),
    NoMatch,
    Unknown,
    Stale,
}

/// The ordinary daemon worker uses bounded keyset pages. A new wake is
/// checked within one full page sweep; file/process/run/child facts are all
/// read from the exact retained subject recorded at admission.
pub(crate) async fn run_agent_wakes(
    router: Router,
    mut shutdown: watch::Receiver<bool>,
    reporter: crate::daemon::workers::WorkerReporter,
) -> Result<()> {
    let mut ticks = interval(WAKE_POLL_INTERVAL);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut after: Option<(i64, String)> = None;
    let mut last_prune = now_ms();
    reporter.running("agent_wake");
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            _ = ticks.tick() => {}
        }
        let page = match router.agent_wake_page(after.clone()).await {
            Ok(page) => page,
            Err(error) => {
                crate::logging::record(
                    &eliot_core::SecretPolicy::default(),
                    "daemon.agent_wake_page_failed",
                    &[],
                    Some(&error),
                );
                after = None;
                continue;
            }
        };
        if page.len() == WAKE_PAGE_SIZE {
            after = page.last().map(|row| (row.created_at, row.id.clone()));
        } else {
            after = None;
        }
        for wake in &page {
            if let Err(error) = router.process_agent_wake(wake).await {
                crate::logging::record(
                    &eliot_core::SecretPolicy::default(),
                    "daemon.agent_wake_process_failed",
                    &[],
                    Some(&error),
                );
            }
        }
        let now = now_ms();
        if now.saturating_sub(last_prune) >= WAKE_PRUNE_INTERVAL_MS {
            if let Err(error) = router.prune_agent_wakes(now).await {
                crate::logging::record(
                    &eliot_core::SecretPolicy::default(),
                    "daemon.agent_wake_prune_failed",
                    &[],
                    Some(&error),
                );
            }
            last_prune = now;
        }
    }
}

fn file_exists_in_workspace(root: &str, relative: &str) -> Result<bool> {
    let root_path = Path::new(root);
    let root_metadata = fs::symlink_metadata(root_path)
        .map_err(|_| Error::new("stale_generation", "Agent managed worktree is unavailable"))?;
    if !root_metadata.is_dir()
        || root_metadata.file_type().is_symlink()
        || crate::platform::is_reparse_point(&root_metadata)
    {
        return Err(Error::new(
            "stale_generation",
            "Agent managed worktree identity changed",
        ));
    }
    let canonical_root = fs::canonicalize(root_path)
        .map_err(|_| Error::new("stale_generation", "Agent worktree could not be resolved"))?;
    let mut current = canonical_root.clone();
    let components: Vec<_> = Path::new(relative).components().collect();
    if components.is_empty() {
        return Err(Error::new("invalid", "wake target path is empty"));
    }
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(part) = component else {
            return Err(Error::new(
                "invalid",
                "wake path is not a safe relative path",
            ));
        };
        current.push(part);
        let metadata = match fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(_) => {
                return Err(Error::new(
                    "unavailable",
                    "wake path could not be inspected",
                ));
            }
        };
        if metadata.file_type().is_symlink() || crate::platform::is_reparse_point(&metadata) {
            return Err(Error::new(
                "stale_generation",
                "wake path contains a link or reparse point",
            ));
        }
        if index + 1 == components.len() {
            if !metadata.is_file() {
                return Ok(false);
            }
            let canonical_target = fs::canonicalize(&current)
                .map_err(|_| Error::new("unavailable", "wake target could not be resolved"))?;
            if !canonical_target.starts_with(&canonical_root) {
                return Err(Error::new(
                    "stale_generation",
                    "wake target escaped the Agent worktree",
                ));
            }
            return Ok(true);
        }
        if !metadata.is_dir() {
            return Ok(false);
        }
    }
    Ok(false)
}

fn matches_wake_delivery(command: &Command, wake: &AgentWakeRecord) -> bool {
    let Some(fact) = wake.fact.as_deref() else {
        return false;
    };
    command.id == wake.delivery_command_id
        && command.principal == "owner"
        && command.method == "command.submit"
        && command.verb == "send"
        && command.target.as_deref() == Some(wake.agent.as_str())
        && command.expected_generation == Some(wake.generation)
        && command.args.get("text").and_then(Value::as_str)
            == Some(format!("Wake condition met: {fact}").as_str())
}

fn agent_wake_run_nonzero_fact(
    run_id: &str,
    exit_code: i32,
    stderr: Option<StderrSnapshot>,
    secrets: &SecretPolicy,
) -> String {
    let mut fact = format!("authorized Run {run_id} exited with nonzero code {exit_code}");
    match stderr {
        Some(stderr) => {
            let redacted = secrets.redact_text(&stderr.text);
            let tail = utf8_tail(&redacted, WAKE_RUN_STDERR_TAIL_BYTES);
            if !tail.is_empty() {
                fact.push_str("\nredacted stderr tail:\n");
                fact.push_str(tail);
            } else if stderr.read_error {
                fact.push_str("\nstderr tail unavailable (read error)");
            } else if stderr.truncated {
                fact.push_str("\nstderr tail unavailable (stored capture truncated)");
            } else {
                fact.push_str("\nstderr tail was empty");
            }
            if stderr.truncated {
                fact.push_str("\n[stderr capture truncated]");
            }
            if stderr.read_error {
                fact.push_str("\n[stderr read error]");
            }
            if !stderr.eof {
                fact.push_str("\n[stderr stream did not reach EOF]");
            }
        }
        None => fact.push_str("\nstderr tail unavailable in retained outcome"),
    }
    secrets.redact_bounded(&fact, WAKE_FACT_MAX_BYTES)
}

fn utf8_tail(text: &str, max_bytes: usize) -> &str {
    let mut start = text.len().saturating_sub(max_bytes);
    while !text.is_char_boundary(start) {
        start += 1;
    }
    &text[start..]
}

fn seconds_condition_is_deadline(wake: &AgentWakeRecord) -> bool {
    let AgentWakeCondition::Seconds { seconds } = &wake.condition else {
        return false;
    };
    let due_at = wake.created_at.saturating_add(
        i64::try_from(*seconds)
            .unwrap_or(i64::MAX)
            .saturating_mul(1_000),
    );
    due_at == wake.expires_at
}
