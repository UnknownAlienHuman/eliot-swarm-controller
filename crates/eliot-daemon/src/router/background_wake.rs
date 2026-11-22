//! Automatic background-process wakes when an Agent turn ends with running descendants (#879, #881, #883).

use super::{Principal, Router};
use crate::common::now_ms;
use eliot_core::{Command, CommandState, Event, ProcessState, Result, SecretPolicy};
use eliot_store::{BackgroundWakeArmInput, BackgroundWakeRecord};
use serde_json::json;

const BACKGROUND_WAKE_PAGE_SIZE: usize = 128;
const BACKGROUND_WAKE_PRUNE_LIMIT: usize = 256;

impl Router {
    /// Inspect custody after a committed `turn.completed` and arm at most one
    /// automatic background-process wake when an Agent-started process is still running.
    pub(super) async fn maybe_arm_background_wake(&self, event: &Event) -> Result<()> {
        if event.kind != "turn.completed" || event.source != "stream" {
            return Ok(());
        }
        let outcome = event
            .data
            .get("outcome")
            .and_then(serde_json::Value::as_str);
        if !matches!(outcome, Some("success" | "failure")) {
            return Ok(());
        }
        let (Some(agent_id), Some(generation), Some(turn_id)) = (
            event.agent.as_deref(),
            event.generation,
            event
                .data
                .get("turn_id")
                .and_then(serde_json::Value::as_str),
        ) else {
            return Ok(());
        };

        // One short Store read: eligibility (idle or still this turn, no newer
        // turn) plus bounded fields only.
        let (agent, context) = {
            let store = self.inner.store.lock().await;
            let Some(context) = store.background_wake_arm_context(agent_id, generation, turn_id)?
            else {
                return Ok(());
            };
            let Some(agent) = store.get_agent(agent_id)? else {
                return Ok(());
            };
            (agent, context)
        };

        let configured_mcp = eliot_core::extract_configured_mcp_programs(&agent.profile_snapshot);
        let secrets = self.secret_policy_snapshot();
        let Some(candidate) = self
            .inner
            .monitoring
            .inspect_running_background_processes(&agent, &configured_mcp, &context, &secrets)
            .await?
        else {
            return Ok(());
        };

        let (tool_item_id, output_tail) = match candidate.tool_item {
            Some(item_id) => {
                let tail = self
                    .inner
                    .store
                    .lock()
                    .await
                    .background_wake_tool_output_tail(
                        &agent.id,
                        agent.generation,
                        turn_id,
                        &item_id,
                    )?
                    .map(|text| bounded_redacted_tail(&text, &secrets));
                (Some(item_id), tail)
            }
            None => (
                None,
                Some(eliot_core::BACKGROUND_WAKE_AMBIGUOUS_TAIL.to_owned()),
            ),
        };
        // Held locally and installed only after the wake commits, so every
        // refusal or error drops (closes) the handle.
        let pinned = self
            .inner
            .monitoring
            .pin_background_process(&candidate.identity)
            .await;
        let now = now_ms();
        let expires_at = now.saturating_add(self.inner.background_wake_max_wait_ms);
        let identity = candidate.identity.clone();
        let armed = self.inner.store.lock().await.background_wake_arm(
            &BackgroundWakeArmInput {
                agent: agent.id.clone(),
                generation: agent.generation,
                turn_id: turn_id.to_owned(),
                tool_item_id,
                process: candidate.identity,
                command_line: candidate.command_line,
                command_truncated: candidate.command_truncated,
                output_tail,
                expires_at,
            },
            now,
        )?;
        if armed.is_some() {
            if let Some(pinned) = pinned {
                self.inner.monitoring.install_background_wake_pin(
                    &agent.id,
                    agent.generation,
                    &identity,
                    pinned,
                );
            }
            self.signal_progress();
        }
        Ok(())
    }

    pub(super) async fn process_background_wakes_tick(
        &self,
        after: &mut Option<(i64, String)>,
    ) -> Result<()> {
        let page = self
            .inner
            .store
            .lock()
            .await
            .background_wake_page(after.clone(), BACKGROUND_WAKE_PAGE_SIZE)?;
        if page.len() == BACKGROUND_WAKE_PAGE_SIZE {
            *after = page.last().map(|row| (row.created_at, row.id.clone()));
        } else {
            *after = None;
        }
        for wake in &page {
            if let Err(error) = self.process_one_background_wake(wake).await {
                crate::logging::record(
                    &SecretPolicy::default(),
                    "daemon.background_wake_process_failed",
                    &[("wake_id", &wake.id), ("agent", &wake.agent)],
                    Some(&error),
                );
            }
        }
        Ok(())
    }

    pub(super) async fn prune_background_wakes(&self, now: i64) -> Result<()> {
        self.inner
            .store
            .lock()
            .await
            .background_wake_prune(now, BACKGROUND_WAKE_PRUNE_LIMIT)?;
        Ok(())
    }

    async fn process_one_background_wake(&self, wake: &BackgroundWakeRecord) -> Result<()> {
        let now = now_ms();
        if wake.state == "waiting" {
            let agent = {
                let mut store = self.inner.store.lock().await;
                let agent = store.get_agent(&wake.agent)?;
                let has_stop = store.agent_has_active_stop(&wake.agent, wake.generation)?;
                match agent {
                    Some(agent)
                        if agent.generation == wake.generation
                            && !agent.is_terminal()
                            && !has_stop =>
                    {
                        agent
                    }
                    _ => {
                        self.inner
                            .monitoring
                            .release_background_wake_pin(&wake.agent, wake.generation);
                        let _ = store.background_wake_finish(
                            &wake.id,
                            wake.generation,
                            None,
                            "stale",
                            now,
                        )?;
                        self.signal_progress();
                        return Ok(());
                    }
                }
            };

            let force_sample = now >= wake.expires_at;
            let process = self
                .inner
                .monitoring
                .check_background_wake_process(&agent, &wake.process, force_sample)
                .await?;
            match process {
                Some(record) if record.state == ProcessState::Exited => {
                    let tail = wake.output_tail.clone();
                    let max_wait_secs = (self.inner.background_wake_max_wait_ms / 1_000).max(1);
                    let body = eliot_core::format_background_wake_note_body(
                        "exited",
                        wake.process.pid,
                        &wake.command_line,
                        wake.command_truncated,
                        record.exit_code,
                        tail.as_deref(),
                        max_wait_secs,
                    );
                    let transitioned = self
                        .inner
                        .store
                        .lock()
                        .await
                        .background_wake_begin_delivery(
                            &wake.id,
                            wake.generation,
                            "exited",
                            record.exit_code,
                            tail.as_deref(),
                            &body,
                        )?;
                    self.signal_progress();
                    if transitioned {
                        let mut delivering = wake.clone();
                        delivering.state = "delivering".to_owned();
                        delivering.outcome_kind = Some("exited".to_owned());
                        delivering.exit_code = record.exit_code;
                        delivering.output_tail = tail;
                        delivering.note_text = Some(body);
                        return self.deliver_background_wake(&delivering, now).await;
                    }
                }
                Some(record) if record.state == ProcessState::Unknown => {
                    self.inner
                        .monitoring
                        .release_background_wake_pin(&wake.agent, wake.generation);
                    let _ = self.inner.store.lock().await.background_wake_finish(
                        &wake.id,
                        wake.generation,
                        None,
                        "unknown",
                        now,
                    )?;
                    self.signal_progress();
                }
                _ if now >= wake.expires_at => {
                    self.inner
                        .monitoring
                        .release_background_wake_pin(&wake.agent, wake.generation);
                    let max_wait_secs = wake
                        .expires_at
                        .saturating_sub(wake.created_at)
                        .saturating_div(1_000)
                        .max(1);
                    let body = eliot_core::format_background_wake_note_body(
                        "expired",
                        wake.process.pid,
                        &wake.command_line,
                        wake.command_truncated,
                        None,
                        None,
                        max_wait_secs,
                    );
                    let transitioned = self
                        .inner
                        .store
                        .lock()
                        .await
                        .background_wake_begin_delivery(
                            &wake.id,
                            wake.generation,
                            "expired",
                            None,
                            None,
                            &body,
                        )?;
                    self.signal_progress();
                    if transitioned {
                        let mut delivering = wake.clone();
                        delivering.state = "delivering".to_owned();
                        delivering.outcome_kind = Some("expired".to_owned());
                        delivering.note_text = Some(body);
                        return self.deliver_background_wake(&delivering, now).await;
                    }
                }
                _ => {}
            }
            return Ok(());
        }

        self.deliver_background_wake(wake, now).await
    }

    async fn deliver_background_wake(&self, wake: &BackgroundWakeRecord, now: i64) -> Result<()> {
        let command_id = wake.delivery_command_id.as_str();
        // Bind first: an `if let` scrutinee keeps the Store guard for the whole block.
        let retained = self.inner.store.lock().await.get_command(command_id)?;
        if let Some(command) = retained {
            if !matches_background_wake_command(&command, wake) {
                self.inner
                    .monitoring
                    .release_background_wake_pin(&wake.agent, wake.generation);
                let _ = self.inner.store.lock().await.background_wake_finish(
                    &wake.id,
                    wake.generation,
                    Some(command_id),
                    "unknown",
                    now,
                )?;
                self.signal_progress();
                return Ok(());
            }
            return self
                .record_background_wake_delivery_state(wake, &command, now)
                .await;
        }

        if wake.state == "delivery_pending" {
            self.inner
                .monitoring
                .release_background_wake_pin(&wake.agent, wake.generation);
            let _ = self.inner.store.lock().await.background_wake_finish(
                &wake.id,
                wake.generation,
                Some(command_id),
                "unknown",
                now,
            )?;
            self.signal_progress();
            return Ok(());
        }

        if now < wake.delivery_next_at {
            return Ok(());
        }

        let active_turn = {
            let mut store = self.inner.store.lock().await;
            let agent = store.get_agent(&wake.agent)?;
            let has_stop = store.agent_has_active_stop(&wake.agent, wake.generation)?;
            match agent {
                Some(agent)
                    if agent.generation == wake.generation && !agent.is_terminal() && !has_stop =>
                {
                    matches!(
                        agent.state,
                        eliot_core::AgentState::Busy | eliot_core::AgentState::Waiting
                    )
                    .then_some(agent.active_turn)
                    .flatten()
                }
                _ => {
                    self.inner
                        .monitoring
                        .release_background_wake_pin(&wake.agent, wake.generation);
                    let _ = store.background_wake_finish(
                        &wake.id,
                        wake.generation,
                        None,
                        "stale",
                        now,
                    )?;
                    self.signal_progress();
                    return Ok(());
                }
            }
        };

        let Some(note_body) = wake.note_text.as_deref() else {
            self.inner
                .monitoring
                .release_background_wake_pin(&wake.agent, wake.generation);
            let _ = self.inner.store.lock().await.background_wake_finish(
                &wake.id,
                wake.generation,
                None,
                "unknown",
                now,
            )?;
            self.signal_progress();
            return Ok(());
        };

        if let Some(turn_id) = active_turn {
            let inject_attempt = {
                let mut store = self.inner.store.lock().await;
                let result = store.background_wake_admit_inject(wake, &turn_id, note_body, now);
                if let Ok(change) = &result {
                    self.publish_locked(&change.events);
                }
                result
            };
            match inject_attempt {
                Ok(change) => {
                    if let Some(id) = change.value {
                        self.schedule_command(id, None);
                    }
                    self.signal_progress();
                    return Ok(());
                }
                Err(error) if matches!(error.code.as_str(), "bad_state" | "unsupported") => {}
                Err(_) => {
                    let retained = self.inner.store.lock().await.get_command(command_id)?;
                    if let Some(command) = retained {
                        return self
                            .record_background_wake_delivery_state(wake, &command, now)
                            .await;
                    }
                    let _ = self
                        .inner
                        .store
                        .lock()
                        .await
                        .background_wake_delivery_pre_admission_failure(
                            &wake.id,
                            wake.generation,
                            now,
                        )?;
                    self.signal_progress();
                    return Ok(());
                }
            }
        }

        let framed = eliot_core::swarm_note(note_body, eliot_core::BACKGROUND_WAKE_PRINCIPAL);
        let params = json!({
            "id": command_id,
            "target": wake.agent,
            "verb": "send",
            "expected_generation": wake.generation,
            "args": { "text": framed },
        });
        let submitted = self
            .submit(
                &params,
                &Principal::Swarm(eliot_core::BACKGROUND_WAKE_PRINCIPAL),
                "daemon.background_wake",
            )
            .await;
        let command = self.inner.store.lock().await.get_command(command_id)?;
        if let Some(command) = command {
            return self
                .record_background_wake_delivery_state(wake, &command, now)
                .await;
        }
        if submitted.is_ok() {
            self.inner
                .monitoring
                .release_background_wake_pin(&wake.agent, wake.generation);
            let _ = self.inner.store.lock().await.background_wake_finish(
                &wake.id,
                wake.generation,
                Some(command_id),
                "unknown",
                now,
            )?;
        } else {
            let _ = self
                .inner
                .store
                .lock()
                .await
                .background_wake_delivery_pre_admission_failure(&wake.id, wake.generation, now)?;
        }
        self.signal_progress();
        Ok(())
    }

    async fn record_background_wake_delivery_state(
        &self,
        wake: &BackgroundWakeRecord,
        command: &Command,
        now: i64,
    ) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        match command.state {
            CommandState::Applied => {
                self.inner
                    .monitoring
                    .release_background_wake_pin(&wake.agent, wake.generation);
                let final_state = if wake.outcome_kind.as_deref() == Some("expired") {
                    "expired"
                } else {
                    "delivered"
                };
                let _ = store.background_wake_finish(
                    &wake.id,
                    wake.generation,
                    Some(&command.id),
                    final_state,
                    now,
                )?;
            }
            CommandState::Accepted | CommandState::Queued | CommandState::Dispatched => {
                let _ = store.background_wake_mark_delivery_pending(
                    &wake.id,
                    wake.generation,
                    &command.id,
                    &command.verb,
                )?;
            }
            CommandState::Unknown => {
                self.inner
                    .monitoring
                    .release_background_wake_pin(&wake.agent, wake.generation);
                let _ = store.background_wake_finish(
                    &wake.id,
                    wake.generation,
                    Some(&command.id),
                    "unknown",
                    now,
                )?;
            }
            CommandState::Cancelled => {
                self.inner
                    .monitoring
                    .release_background_wake_pin(&wake.agent, wake.generation);
                let _ = store.background_wake_finish(
                    &wake.id,
                    wake.generation,
                    Some(&command.id),
                    "cancelled",
                    now,
                )?;
            }
            CommandState::Rejected | CommandState::Failed => {
                self.inner
                    .monitoring
                    .release_background_wake_pin(&wake.agent, wake.generation);
                let _ = store.background_wake_finish(
                    &wake.id,
                    wake.generation,
                    Some(&command.id),
                    "failed",
                    now,
                )?;
            }
        }
        self.signal_progress();
        Ok(())
    }
}

fn matches_background_wake_command(command: &Command, wake: &BackgroundWakeRecord) -> bool {
    command.id == wake.delivery_command_id
        && command.principal == eliot_core::BACKGROUND_WAKE_PRINCIPAL
        && command.target.as_deref() == Some(wake.agent.as_str())
        && command.expected_generation == Some(wake.generation)
        && matches!(command.verb.as_str(), "send" | "inject")
}

fn bounded_redacted_tail(raw: &str, secrets: &SecretPolicy) -> String {
    let cleaned: String = raw
        .replace('\0', "\u{fffd}")
        .chars()
        .map(|ch| {
            if ch.is_control() && !matches!(ch, '\n' | '\r' | '\t') {
                ' '
            } else {
                ch
            }
        })
        .collect();
    let redacted = eliot_core::redact_secret_shapes(&secrets.redact_text(cleaned.trim()));
    let limit = eliot_core::MAX_BACKGROUND_WAKE_OUTPUT_TAIL_BYTES;
    if redacted.len() <= limit {
        return redacted;
    }
    let mut start = redacted.len() - limit;
    while !redacted.is_char_boundary(start) {
        start += 1;
    }
    redacted[start..].to_owned()
}
