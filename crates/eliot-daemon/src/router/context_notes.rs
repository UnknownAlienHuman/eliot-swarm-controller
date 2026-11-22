use super::{MODE_OPEN, Principal, Router, insertion_facts::InsertionFact};
use crate::common::now_ms;
use eliot_core::{CommandState, ContextWaitRequest, Error, Event, Result, api::HandoffResponse};
use eliot_store::Store;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use tokio::sync::{broadcast, watch};

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
                        &[("seq", &event.seq.to_string()), ("kind", &event.kind)],
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
    async fn note_committed_fact(&self, event: &Event) -> Result<()> {
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Ok(());
        }
        if event.kind == "turn.started"
            && event.source == "stream"
            && let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation)
        {
            self.inner
                .monitoring
                .lock_background_baseline(agent, generation);
        }
        if event.kind == "turn.completed" && event.source == "stream" {
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
            let mut store = self.inner.store.lock().await;
            if event.kind == "session.context.compacted"
                || store
                    .generation_start_command(agent, generation)?
                    .is_some_and(|command| command.verb == "resume")
            {
                self.queue_task_card_locked(&mut store, event, None)?;
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
                    "The turn changed files but no configured Goal check is recorded before the turn ended.", None,
                )?,
            }
        }
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
    ) -> Result<()> {
        if let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation)
            && let Some(text) = super::task_card::read_task_card(
                store,
                agent,
                generation,
                brief,
                &self.secret_policy_snapshot(),
            )?
        {
            self.queue_fact_note_locked(store, event, "task_card", &text, None)?;
        }
        Ok(())
    }

    pub(super) fn queue_handoff_task_card(
        &self,
        store: &mut Store,
        handoff: &HandoffResponse,
        events: &[Event],
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
            ));
        }
    }

    pub(super) async fn queue_resume_task_card(&self, agent: &str, generation: i64) {
        let mut store = self.inner.store.lock().await;
        let result = store
            .session_ready_event(agent, generation)
            .and_then(|ready| {
                ready.map_or(Ok(()), |event| {
                    self.queue_task_card_locked(&mut store, &event, None)
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
