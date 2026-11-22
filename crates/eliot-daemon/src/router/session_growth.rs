//! React to a retained size warning at the next quiescent session boundary.
use super::*;
use eliot_core::AttentionStep;
use eliot_core::api::{SwitchLaunch, SwitchRequest};

impl Router {
    pub(super) async fn react_to_session_growth(&self, agent: &Agent) -> Result<()> {
        if agent.state != AgentState::Idle
            || agent.archived
            || agent.observe_only
            || agent.is_automatic_provider_probe()
            || self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN
        {
            return Ok(());
        }
        let (warning, usage) = {
            let store = self.inner.store.lock().await;
            (
                store.latest_session_growth_incident(&agent.id, agent.generation)?,
                store.get_session_usage(&agent.id, agent.generation)?,
            )
        };
        let context_ceiling = agent
            .profile_snapshot
            .get("context_ceiling_tokens")
            .and_then(Value::as_i64)
            .filter(|ceiling| *ceiling > 0);
        let ceiling_sample = context_ceiling
            .zip(usage.as_ref())
            .and_then(|(ceiling, usage)| {
                let used = usage.context_used_tokens.filter(|used| *used >= 0)?;
                (used > ceiling).then(|| (used, ceiling, (*usage).clone()))
            });
        if warning.is_none() && ceiling_sample.is_none() {
            return Ok(());
        }
        if !self.inner.attention.session_growth.enabled {
            return Ok(());
        }
        let id = ulid::Ulid::new().to_string();
        let compact = eliot_core::compact_hint_for_harness(&agent.harness).is_some();
        let switch = self.inner.attention.session_growth.switch;
        let (step, action, skipped) = if compact {
            let mut request = SubmitCommand::new(id.clone(), "compact");
            request.target = Some(agent.id.clone());
            request.expected_generation = Some(agent.generation);
            let action = serde_json::to_value(request)
                .map_err(|_| Error::new("internal", "cannot encode session compaction"))?;
            ("compact_for_session_growth", Some(action), "")
        } else if ceiling_sample.is_some() || switch {
            let action = self.prepare_growth_switch(agent, &id).await?;
            ("switch_for_session_growth", action, "no_safe_handoff")
        } else {
            ("observe_session_growth", None, "switch_disabled")
        };
        let incident_id = match (&ceiling_sample, &warning) {
            (Some((used, ceiling, sample)), _) => format!(
                "ceiling-{}-{}-{}-{}",
                agent.generation,
                sample.observed_at.unwrap_or_default(),
                ceiling,
                used
            ),
            (None, Some((seq, _))) => format!("growth-{seq}-{}-{}", agent.id, agent.generation),
            (None, None) => return Ok(()),
        };
        let reaction = AttentionStep {
            incident_id,
            step: step.to_owned(),
            outcome: if action.is_some() { "unknown" } else { skipped }.to_owned(),
            command_id: action.as_ref().map(|_| id),
            turn_id: warning.as_ref().and_then(|(_, turn_id)| turn_id.clone()),
        };
        {
            let thresholds = self.inner.observe.lock_recover().growth;
            let mut store = self.inner.store.lock().await;
            let current = store.get_agent(&agent.id)?;
            let agent_is_current = current.is_some_and(|current| {
                current.generation == agent.generation
                    && current.state == AgentState::Idle
                    && current.active_turn.is_none()
            });
            let growth_is_current = if let Some((used, ceiling, sample)) = &ceiling_sample {
                let current_usage = store.get_session_usage(&agent.id, agent.generation)?;
                current_usage.as_ref() == Some(sample)
                    && current_usage
                        .as_ref()
                        .and_then(|usage| usage.context_used_tokens)
                        .is_some_and(|current_used| {
                            current_used == *used && current_used > *ceiling
                        })
            } else if let Some((seq, _)) = &warning {
                store
                    .latest_session_growth_incident(&agent.id, agent.generation)?
                    .is_some_and(|(current_seq, _)| current_seq == *seq)
                    && store
                        .session_growth_view(
                            &agent.harness,
                            &agent.id,
                            agent.generation,
                            &thresholds,
                        )?
                        .is_some()
            } else {
                false
            };
            if !agent_is_current || !growth_is_current {
                return Ok(());
            }
            let Some(event) =
                store.claim_attention_step(&agent.id, agent.generation, &reaction, now_ms())?
            else {
                return Ok(());
            };
            self.publish_locked(&[event]);
        }
        if let Some(action) = action {
            if compact {
                self.submit(&action, &Principal::Owner, "daemon.session_growth")
                    .await?;
            } else {
                self.swarm_switch(&action, &Principal::Owner, None).await?;
            }
        }
        self.report_session_growth(agent, &reaction).await
    }

    async fn prepare_growth_switch(&self, agent: &Agent, id: &str) -> Result<Option<Value>> {
        if agent.chat || agent.parent.is_some() {
            return Ok(None);
        }
        let Ok(profile) = self.resolve_profile_cached(&agent.profile) else {
            return Ok(None);
        };
        // A mutable profile name cannot silently change the authority of a
        // daemon-initiated replacement.
        if eliot_core::provider_switch_authority(&profile)?
            != eliot_core::provider_switch_authority(&agent.profile_snapshot)?
        {
            return Ok(None);
        }
        let store = self.inner.store.lock().await;
        let Some(turn) = store.list_turns(&agent.id, agent.generation, 1)?.pop() else {
            return Ok(None);
        };
        if !matches!(turn.outcome, Some(TurnOutcome::Success)) {
            return Ok(None);
        }
        let Some(task_command) =
            store.turn_command_id(&agent.id, agent.generation, &turn.turn_id)?
        else {
            return Ok(None);
        };
        let request = SwitchRequest {
            id: id.to_owned(),
            from: agent.id.clone(),
            from_generation: agent.generation,
            task_command,
            commands: Vec::new(),
            launch: SwitchLaunch {
                profile: Some(agent.profile.clone()),
                ..Default::default()
            },
        };
        serde_json::to_value(request)
            .map(Some)
            .map_err(|_| Error::new("internal", "cannot encode session growth switch"))
    }
}
