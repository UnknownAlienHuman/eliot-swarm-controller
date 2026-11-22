//! React to a retained size warning at the next quiescent session boundary.
use super::*;
use eliot_core::AttentionStep;
use eliot_core::api::{SwitchLaunch, SwitchRequest};

const SESSION_GROWTH_CEILING_REASON: &str = "session_growth_ceiling";
const SESSION_GROWTH_CEILING_NOTICE_STEP: &str = "notify_for_session_growth";

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
        let (warning, usage, ceiling_claimed, ceiling_notice_claimed) = {
            let store = self.inner.store.lock().await;
            (
                store.latest_session_growth_incident(&agent.id, agent.generation)?,
                store.get_session_usage(&agent.id, agent.generation)?,
                store.generation_attention_step_claimed(
                    &agent.id,
                    agent.generation,
                    SESSION_GROWTH_CEILING_REASON,
                    None,
                )?,
                store.generation_attention_step_claimed(
                    &agent.id,
                    agent.generation,
                    SESSION_GROWTH_CEILING_REASON,
                    Some(SESSION_GROWTH_CEILING_NOTICE_STEP),
                )?,
            )
        };
        if !self.inner.attention.session_growth.enabled {
            return Ok(());
        }
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
            if ceiling_claimed
                && !ceiling_notice_claimed
                && let Some(notice) = self.claim_ceiling_notice(agent, None, None).await?
            {
                return self.report_session_growth(agent, &notice).await;
            }
            return Ok(());
        }

        if let Some((used, ceiling, sample)) = ceiling_sample {
            let incident_id = session_growth_ceiling_incident_id(agent);
            let compact = eliot_core::compact_hint_for_harness(&agent.harness).is_some();
            let action_id = ulid::Ulid::new().to_string();
            let action = if ceiling_claimed {
                None
            } else if compact {
                let mut request = SubmitCommand::new(action_id.clone(), "compact");
                request.target = Some(agent.id.clone());
                request.expected_generation = Some(agent.generation);
                Some(
                    serde_json::to_value(request)
                        .map_err(|_| Error::new("internal", "cannot encode session compaction"))?,
                )
            } else {
                self.prepare_growth_switch(agent, &action_id).await?
            };
            let step = if compact {
                "compact_for_session_growth"
            } else {
                "switch_for_session_growth"
            };
            let mut action_claimed = false;
            {
                let thresholds = self.inner.observe.lock_recover().growth;
                let mut store = self.inner.store.lock().await;
                let current = store.get_agent(&agent.id)?;
                let agent_is_current = current.is_some_and(|current| {
                    current.generation == agent.generation
                        && current.state == AgentState::Idle
                        && current.active_turn.is_none()
                });
                let current_usage = store.get_session_usage(&agent.id, agent.generation)?;
                let growth_is_current = current_usage.as_ref() == Some(&sample)
                    && current_usage
                        .as_ref()
                        .and_then(|usage| usage.context_used_tokens)
                        .is_some_and(|current_used| current_used == used && current_used > ceiling);
                if !agent_is_current || !growth_is_current {
                    return Ok(());
                }
                let already_claimed = store.generation_attention_step_claimed(
                    &agent.id,
                    agent.generation,
                    SESSION_GROWTH_CEILING_REASON,
                    None,
                )?;
                if !already_claimed {
                    let reaction = AttentionStep {
                        incident_id: incident_id.clone(),
                        step: step.to_owned(),
                        outcome: if action.is_some() {
                            "unknown"
                        } else {
                            "no_safe_handoff"
                        }
                        .to_owned(),
                        command_id: action.as_ref().map(|_| action_id.clone()),
                        turn_id: warning.as_ref().and_then(|(_, turn_id)| turn_id.clone()),
                    };
                    if let Some(event) = store.claim_generation_attention_step(
                        &agent.id,
                        agent.generation,
                        SESSION_GROWTH_CEILING_REASON,
                        &reaction,
                        now_ms(),
                    )? {
                        self.publish_locked(&[event]);
                        action_claimed = true;
                    }
                }
            }

            // Claim and route one follow-up notice even if dispatch returns an
            // uncertain result. The durable claim prevents replay after restart.
            let notice = self
                .claim_ceiling_notice(
                    agent,
                    warning.as_ref().and_then(|(_, turn_id)| turn_id.clone()),
                    None,
                )
                .await?;
            let dispatch_result = match (action_claimed, action.as_ref()) {
                (true, Some(action)) if compact => {
                    self.submit(action, &Principal::Owner, "daemon.session_growth")
                        .await
                }
                (true, Some(action)) => {
                    match eliot_core::provider_switch_authority(&agent.profile_snapshot) {
                        Ok(authority) => {
                            self.swarm_switch(action, &Principal::Owner, None, Some(authority))
                                .await
                        }
                        Err(error) => Err(error),
                    }
                }
                _ => Ok(()),
            };
            let notice_result = if let Some(notice) = notice.as_ref() {
                self.report_session_growth(agent, notice).await
            } else {
                Ok(())
            };
            dispatch_result?;
            return notice_result;
        }

        let Some((seq, turn_id)) = warning else {
            return Ok(());
        };
        if ceiling_claimed {
            if !ceiling_notice_claimed
                && let Some(notice) = self
                    .claim_ceiling_notice(agent, turn_id, Some((seq, None)))
                    .await?
            {
                return self.report_session_growth(agent, &notice).await;
            }
            // A later legacy warning in this generation may overlap a ceiling
            // reaction; never let it repeat a compaction or handoff.
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
        } else if switch {
            let action = self.prepare_growth_switch(agent, &id).await?;
            ("switch_for_session_growth", action, "no_safe_handoff")
        } else {
            ("observe_session_growth", None, "switch_disabled")
        };
        let incident_id = format!("growth-{seq}-{}-{}", agent.id, agent.generation);
        let reaction = AttentionStep {
            incident_id,
            step: step.to_owned(),
            outcome: if action.is_some() { "unknown" } else { skipped }.to_owned(),
            command_id: action.as_ref().map(|_| id),
            turn_id,
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
            let growth_is_current = store
                .latest_session_growth_incident(&agent.id, agent.generation)?
                .is_some_and(|(current_seq, _)| current_seq == seq)
                && store
                    .session_growth_view(&agent.harness, &agent.id, agent.generation, &thresholds)?
                    .is_some();
            if !agent_is_current || !growth_is_current {
                return Ok(());
            }
            // Recheck under the Store lock so a concurrent ceiling reaction
            // takes precedence over this legacy warning.
            if store.generation_attention_step_claimed(
                &agent.id,
                agent.generation,
                SESSION_GROWTH_CEILING_REASON,
                None,
            )? {
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
                let authority = eliot_core::provider_switch_authority(&agent.profile_snapshot)?;
                self.swarm_switch(&action, &Principal::Owner, None, Some(authority))
                    .await?;
            }
        }
        self.report_session_growth(agent, &reaction).await
    }

    async fn claim_ceiling_notice(
        &self,
        agent: &Agent,
        turn_id: Option<String>,
        warning: Option<(i64, Option<String>)>,
    ) -> Result<Option<AttentionStep>> {
        let reaction = AttentionStep {
            incident_id: session_growth_ceiling_incident_id(agent),
            step: SESSION_GROWTH_CEILING_NOTICE_STEP.to_owned(),
            outcome: "unknown".to_owned(),
            command_id: None,
            turn_id,
        };
        let thresholds = self.inner.observe.lock_recover().growth;
        let mut store = self.inner.store.lock().await;
        let current = store.get_agent(&agent.id)?;
        let agent_is_current = current.is_some_and(|current| {
            current.generation == agent.generation
                && current.state == AgentState::Idle
                && current.active_turn.is_none()
        });
        let warning_is_current = if let Some((seq, _)) = warning {
            store
                .latest_session_growth_incident(&agent.id, agent.generation)?
                .is_some_and(|(current_seq, _)| current_seq == seq)
                && store
                    .session_growth_view(&agent.harness, &agent.id, agent.generation, &thresholds)?
                    .is_some()
        } else {
            true
        };
        if !agent_is_current || !warning_is_current {
            return Ok(None);
        }
        let Some(event) = store.claim_generation_attention_step(
            &agent.id,
            agent.generation,
            SESSION_GROWTH_CEILING_REASON,
            &reaction,
            now_ms(),
        )?
        else {
            return Ok(None);
        };
        self.publish_locked(&[event]);
        Ok(Some(reaction))
    }

    async fn prepare_growth_switch(&self, agent: &Agent, id: &str) -> Result<Option<Value>> {
        if agent.chat || agent.parent.is_some() {
            return Ok(None);
        }
        if self.resolve_profile_cached(&agent.profile).is_err() {
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
        if store.is_handoff_brief(&task_command)? {
            return Ok(None);
        }
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

fn session_growth_ceiling_incident_id(agent: &Agent) -> String {
    format!("ceiling-{}-{}", agent.id, agent.generation)
}
