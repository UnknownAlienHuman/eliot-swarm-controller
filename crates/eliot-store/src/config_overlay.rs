use super::*;
use eliot_core::api::AgentConfigView;

/// Retained model/effort overlay for one Agent generation: the requested
/// values from one `configure` command plus the effective values the next
/// native turn carries. The original profile snapshot is never rewritten;
/// it stays on the Agent row while these rows accumulate per generation.
/// A `resume` starts its new generation by inheriting the previous
/// generation's latest overlay as revision 1 with `provenance` set to
/// `inherited_from`; directly configured rows carry `configure`.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentConfig {
    pub agent: String,
    pub generation: i64,
    pub revision: i64,
    pub requested_model: Option<String>,
    pub requested_effort: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub application: String,
    pub provenance: String,
    pub command_id: String,
    pub created_at: i64,
}

impl Store {
    /// Retained `configure` overlay for one exact revision, or `None` when
    /// the command ran under the frozen snapshot (revision 0). A nonzero
    /// revision without a retained row is corruption, never a silent
    /// snapshot fallback.
    pub fn get_agent_config(
        &self,
        agent: &str,
        generation: i64,
        revision: i64,
    ) -> Result<Option<AgentConfig>> {
        if revision == 0 {
            return Ok(None);
        }
        let agent_num = parse_agent_id(agent)?;
        self.connection
            .query_row(
                "SELECT revision, requested, effective, application, provenance, command_id, created_at
                 FROM agent_config WHERE agent = ?1 AND generation = ?2 AND revision = ?3",
                params![agent_num, generation, revision],
                |row| agent_config_from_row(agent_num, generation, row),
            )
            .optional()
            .map_err(sql_error)?
            .map(|config| {
                if config.revision != revision {
                    Err(Error::new(
                        "source_integrity",
                        "retained agent_config revision does not match its row",
                    ))
                } else {
                    Ok(config)
                }
            })
            .transpose()
    }

    /// Latest retained `configure` overlay for one Agent generation, or
    /// `None` when no `configure` ever applied to it.
    pub fn get_agent_config_current(
        &self,
        agent: &str,
        generation: i64,
    ) -> Result<Option<AgentConfig>> {
        let agent_num = parse_agent_id(agent)?;
        let revision: Option<i64> = self
            .connection
            .query_row(
                "SELECT MAX(revision) FROM agent_config WHERE agent = ?1 AND generation = ?2",
                params![agent_num, generation],
                |row| row.get(0),
            )
            .map_err(sql_error)?;
        revision
            .map(|revision| {
                self.get_agent_config(agent, generation, revision)?
                    .ok_or_else(|| {
                        Error::new("internal", "newest agent_config revision disappeared")
                    })
            })
            .transpose()
    }

    /// Effective model/effort for one send command's retained revision, or
    /// `None` when the send runs under the frozen snapshot. Used by the
    /// plugin host before native turn construction and by settlement when
    /// it records what the native request carried. Reads only the retained
    /// revision stamped at dispatch, so a later config-file reload can never
    /// rewrite a pending or unknown command's meaning.
    pub fn send_effective_config(&self, command_id: &str) -> Result<Option<AgentConfig>> {
        self.command_effective_config(command_id, &["send"])
    }

    /// Effective configuration for one retained command's stamped revision,
    /// or `None` when it runs under the frozen snapshot (revision 0). The
    /// revision is read from the retained command row and resolved against
    /// the retained `agent_config` rows only: config-file reloads never
    /// rewrite a retained generation, a pending command, or an
    /// unknown-outcome resolution. `allowed` lists the verbs that may carry
    /// a native configuration (launch, resume, send, configure).
    pub fn command_effective_config(
        &self,
        command_id: &str,
        allowed: &[&str],
    ) -> Result<Option<AgentConfig>> {
        let record = load_command_record(&self.connection, command_id)?.ok_or_else(|| {
            Error::new("not_found", format!("command {command_id} was not found"))
        })?;
        if !allowed.contains(&record.command.verb.as_str()) {
            return Err(Error::new(
                "invalid",
                "effective configuration resolves only for native commands",
            ));
        }
        let Some(target) = record.target else {
            return Err(Error::new(
                "source_integrity",
                "command has no retained Agent target",
            ));
        };
        let generation = record
            .command
            .generation
            .ok_or_else(|| Error::new("source_integrity", "command has no retained generation"))?;
        let agent = format_agent_id(target);
        self.get_agent_config(&agent, generation, record.config_revision)
    }

    /// Effective configuration for one retained turn's stamped revision, or
    /// `None` when the turn ran under the frozen snapshot. Reads only
    /// retained rows, so reloads never rewrite a retained turn's meaning.
    pub fn turn_effective_config(
        &self,
        agent: &str,
        generation: i64,
        turn_id: &str,
    ) -> Result<Option<AgentConfig>> {
        let turn = self
            .get_turn(agent, generation, turn_id)?
            .ok_or_else(|| Error::new("not_found", format!("turn {turn_id} was not found")))?;
        self.get_agent_config(agent, generation, turn.config_revision)
    }

    /// Retained configuration view for one command's exact stamped
    /// generation and revision: the requested and effective model/effort,
    /// the revision, the provenance, and the profile name. `None` when the
    /// command runs under the frozen snapshot, has no retained target, or
    /// carries no native configuration. Reads only retained rows, so a
    /// config-file reload never rewrites a retained generation, a pending
    /// command, or an unknown-outcome resolution.
    pub fn command_config_view(&self, command: &Command) -> Result<Option<AgentConfigView>> {
        if !matches!(
            command.verb.as_str(),
            "launch" | "resume" | "send" | "configure"
        ) {
            return Ok(None);
        }
        if command.config_revision == 0 {
            return Ok(None);
        }
        let Some(target) = command.target.as_deref() else {
            return Ok(None);
        };
        let Some(generation) = command.generation else {
            return Ok(None);
        };
        let agent = self.get_agent(target)?.ok_or_else(|| {
            Error::new(
                "source_integrity",
                format!("command target {target} was not found"),
            )
        })?;
        self.retained_config_view(&agent, generation, command.config_revision)
    }

    /// Retained configuration view for one turn's exact stamped generation
    /// and revision. `None` when the turn ran under the frozen snapshot.
    /// Reads only retained rows, so later revisions never rewrite it.
    pub fn turn_config_view(&self, agent: &Agent, turn: &Turn) -> Result<Option<AgentConfigView>> {
        if turn.config_revision == 0 {
            return Ok(None);
        }
        self.retained_config_view(agent, turn.generation, turn.config_revision)
    }

    pub fn fill_turn_readback(&self, agent: &Agent, turn: &mut Turn) -> Result<()> {
        turn.config = self.turn_config_view(agent, turn)?;
        if agent.harness == "opencode" && turn.outcome == Some(TurnOutcome::Failure) {
            // Older rows predate the retained failure fields. Recover the
            // exact terminal diagnostic while its event is still available.
            if turn.native_error_message.is_none() {
                turn.native_error_message = self
                    .turn_terminal_event(&turn.agent, turn.generation, &turn.turn_id)?
                    .and_then(|event| {
                        event
                            .data
                            .pointer("/native_error/message")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    });
            }
            if turn.config_revision == 0
                && turn.effective_model.is_none()
                && agent.generation == turn.generation
            {
                turn.effective_model = agent
                    .effective
                    .as_ref()
                    .and_then(|effective| effective.get("model"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
        }
        Ok(())
    }

    /// Build the readback view from the exact retained overlay row. The
    /// profile name, snapshot originals, and launch inheritance come from
    /// the retained Agent row; the revision, provenance, and requested and
    /// effective values come from the retained `agent_config` row. A nonzero
    /// revision without a retained row is corruption, never a silent
    /// snapshot fallback.
    fn retained_config_view(
        &self,
        agent: &Agent,
        generation: i64,
        revision: i64,
    ) -> Result<Option<AgentConfigView>> {
        let Some(overlay) = self.get_agent_config(&agent.id, generation, revision)? else {
            return Err(Error::new(
                "source_integrity",
                format!(
                    "retained agent_config revision {revision} for {} generation {generation} is missing",
                    agent.id
                ),
            ));
        };
        Ok(Some(AgentConfigView {
            revision: overlay.revision,
            application: overlay.application.clone(),
            provenance: overlay.provenance.clone(),
            profile: agent.profile.clone(),
            profile_inherited: agent.profile_inherited,
            requested_model: overlay.requested_model.clone(),
            requested_effort: overlay.requested_effort.clone(),
            model: overlay.model.clone(),
            effort: overlay.effort.clone(),
            original_model: agent
                .profile_snapshot
                .get("model")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            original_effort: agent
                .profile_snapshot
                .get("effort")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        }))
    }

    /// Commit one dispatched `configure` command's overlay and stamp the
    /// command with its assigned revision. The revision is `max + 1` within
    /// the Agent generation, assigned here (not at admission) so concurrent
    /// admissions stay monotonic; `command_id` is unique so a reprocessed
    /// dispatch returns the retained row instead of bumping again.
    /// Settlement to `applied` happens after this returns. A pre-commit
    /// persistence error latches the Store like every other mutation, so
    /// the Router cannot settle the command `rejected` past the fault.
    pub fn apply_agent_config(&mut self, command_id: &str, now: i64) -> Result<AgentConfig> {
        self.ensure_writer_healthy()?;
        let result = self.apply_agent_config_inner(command_id, now, None);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Commit one dispatched `configure` command's overlay with natively
    /// verified effective values (OpenCode/Antigravity): the plugin proved
    /// these against the native echo or `GET` before the Router stored
    /// anything. `requested` still records exactly what the owner asked
    /// (re-resolved from the retained args); each `Some` native value
    /// replaces the resolved fill, while `None` keeps it (a native knob
    /// the harness does not have, such as OpenCode effort). A requested
    /// field without a proven native value stores nothing and reports
    /// `source_integrity`: the Router must only call this after verified
    /// plugin proof, never to paper over an unverified application.
    pub fn apply_agent_config_native(
        &mut self,
        command_id: &str,
        now: i64,
        native_model: Option<String>,
        native_effort: Option<String>,
    ) -> Result<AgentConfig> {
        self.ensure_writer_healthy()?;
        let result =
            self.apply_agent_config_inner(command_id, now, Some((native_model, native_effort)));
        self.fault_on_persistence_error(&result);
        result
    }

    fn apply_agent_config_inner(
        &mut self,
        command_id: &str,
        now: i64,
        native: Option<(Option<String>, Option<String>)>,
    ) -> Result<AgentConfig> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let record = load_command_record(&tx, command_id)?.ok_or_else(|| {
            Error::new("not_found", format!("command {command_id} was not found"))
        })?;
        if record.state != CommandState::Dispatched {
            return Err(Error::new(
                "bad_state",
                "configure applies only after a committed dispatch",
            ));
        }
        let config = apply_agent_config_tx(&tx, &record, native, now)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(config)
    }
}

pub(super) fn apply_agent_config_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    native: Option<(Option<String>, Option<String>)>,
    now: i64,
) -> Result<AgentConfig> {
    if record.command.verb != "configure" {
        return Err(Error::new(
            "invalid",
            "agent configuration applies only to configure commands",
        ));
    }
    let command_id = record.command.id.as_str();
    if let Some(existing) = tx
        .query_row(
            "SELECT agent, generation, revision FROM agent_config WHERE command_id = ?1",
            [command_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            },
        )
        .optional()
        .map_err(sql_error)?
    {
        let (agent, generation, revision) = existing;
        return get_agent_config_tx(tx, agent, generation, revision)?
            .ok_or_else(|| Error::new("internal", "retained agent_config row disappeared"));
    }
    let agent_num = record.target.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "configure command has no retained Agent target",
        )
    })?;
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "configure Agent was not found"))?;
    let generation = record.command.generation.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "configure command has no retained generation",
        )
    })?;
    if generation != agent.generation {
        return Err(Error::new(
            "stale_generation",
            format!(
                "configure targets generation {generation}, current is {}",
                agent.generation
            ),
        ));
    }
    if agent.is_terminal()
        || !matches!(
            agent.state,
            AgentState::Idle | AgentState::Busy | AgentState::Waiting
        )
    {
        return Err(Error::new(
            "bad_state",
            "configure requires a live Agent generation with a native session",
        ));
    }
    let (requested, mut resolved_effective, application) = admission::resolve_configure(
        &record.command.args,
        &agent.harness,
        &agent.profile_snapshot,
    )?;
    let previous_revision = current_config_revision_tx(tx, agent_num, generation)?;
    if agent.harness == "muse" {
        let previous = get_agent_config_tx(tx, agent_num, generation, previous_revision)?;
        if requested["model"].is_null() {
            let model = previous
                .as_ref()
                .and_then(|config| config.model.as_deref())
                .or_else(|| agent.effective.as_ref()?.get("model")?.as_str());
            if let Some(model) = model {
                resolved_effective["model"] = json!(model);
            }
        }
        if requested["effort"].is_null()
            && let Some(effort) = previous
                .as_ref()
                .and_then(|config| config.effort.as_deref())
        {
            resolved_effective["effort"] = json!(effort);
        }
    }
    // A native proof replaces the resolved fill exactly where the
    // harness proved it; anything else keeps the resolved value (an
    // unrequested field, or a knob the harness does not have, such as
    // OpenCode effort). A requested field without proof stores nothing.
    let effective = match native {
        None => resolved_effective,
        Some((native_model, native_effort)) => {
            if requested.get("model").and_then(Value::as_str).is_some() && native_model.is_none() {
                return Err(Error::new(
                    "source_integrity",
                    "configure stored no proven native model for its requested model",
                ));
            }
            if agent.harness != "muse"
                && requested.get("effort").and_then(Value::as_str).is_some()
                && native_effort.is_none()
            {
                return Err(Error::new(
                    "source_integrity",
                    "configure stored no proven native effort for its requested effort",
                ));
            }
            let model = admission::check_native_effective(
                native_model,
                "model",
                admission::MAX_CONFIGURE_MODEL_LEN,
            )?
            .or_else(|| {
                resolved_effective
                    .get("model")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            });
            let effort = admission::check_native_effective(
                native_effort,
                "effort",
                admission::MAX_CONFIGURE_EFFORT_LEN,
            )?
            .or_else(|| {
                resolved_effective
                    .get("effort")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            });
            json!({"model": model, "effort": effort})
        }
    };
    let revision = previous_revision + 1;
    let requested_json = encode_json(&requested)?;
    let effective_json = encode_json(&effective)?;
    db!(tx.execute(
        "INSERT INTO agent_config(agent, generation, revision, requested, effective, application, provenance, command_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'configure', ?7, ?8)",
        params![
            agent_num,
            generation,
            revision,
            requested_json,
            effective_json,
            application,
            command_id,
            now,
        ],
    ));
    db!(tx.execute(
        "UPDATE commands SET config_revision = ?1 WHERE id = ?2",
        params![revision, command_id],
    ));
    get_agent_config_tx(tx, agent_num, generation, revision)?
        .ok_or_else(|| Error::new("internal", "new agent_config row was not visible"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        install_fixture_sources, install_live_custody_observations, send_request,
        simulate_normal_stop_proof,
    };

    /// Idle first-generation Agent with an explicit frozen profile snapshot,
    /// ready for `configure` without a native harness.
    fn ready_configure_agent(
        store: &mut Store,
        project: &str,
        harness: &str,
        snapshot: serde_json::Value,
        now: i64,
    ) -> String {
        let launch_id = Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":project, "workspace":"synthetic-workspace", "harness":harness, "profile":"fake", "profile_snapshot":snapshot}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        install_fixture_sources(store, &agent, &launch_id, 1, now);
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:ready"),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: format!("{project}:ready"),
                        data: json!({"command_id":launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    fn configure_request(agent: &str, generation: i64, args: serde_json::Value) -> SubmitCommand {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "configure");
        request.target = Some(agent.to_owned());
        request.args = args;
        request.expected_generation = Some(generation);
        request
    }

    #[test]
    fn muse_configure_preserves_model_across_effort_only_revision() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let agent = ready_configure_agent(
            &mut store,
            "muse-config",
            "muse",
            json!({"model":"m0","effort":"low"}),
            now,
        );
        let request = configure_request(&agent, 1, json!({"model":"m1", "effort":"medium"}));
        let id = request.id.clone();
        store.submit("owner", request, now).unwrap();
        store.dispatch(&id, now).unwrap();
        let config = store
            .apply_agent_config_native(&id, now, Some("m1".into()), None)
            .unwrap();
        assert_eq!(config.effort.as_deref(), Some("medium"));
        store.settle(&id, CommandState::Applied, None, now).unwrap();

        let invalid = configure_request(&agent, 1, json!({"effort":"invented-tier"}));
        assert_eq!(
            store.submit("owner", invalid, now).unwrap_err().code,
            "invalid"
        );
        let request = configure_request(&agent, 1, json!({"effort":"high"}));
        let id = request.id.clone();
        store.submit("owner", request, now).unwrap();
        store.dispatch(&id, now).unwrap();
        let config = store.apply_agent_config(&id, now).unwrap();
        assert_eq!(config.revision, 2);
        assert_eq!(config.model.as_deref(), Some("m1"));
        assert_eq!(config.effort.as_deref(), Some("high"));
    }

    #[test]
    fn owner_applies_unknown_muse_configure_only_with_matching_live_readback() {
        use eliot_core::api::{CommandResolution, CommandResolveRequest};

        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let agent = ready_configure_agent(
            &mut store,
            "muse-resolve",
            "muse",
            json!({"model":"m0"}),
            now,
        );
        let number = parse_agent_id(&agent).unwrap();
        let mut retained = store.get_agent(&agent).unwrap().unwrap();
        retained.native_ref = Some("muse-session".into());
        store
            .connection
            .execute(
                "UPDATE agents SET native_ref = ?1 WHERE id = ?2",
                params![retained.native_ref, number],
            )
            .unwrap();
        let request = configure_request(&agent, 1, json!({"model":"m1", "effort":"low"}));
        let id = request.id.clone();
        store.submit("owner", request, now).unwrap();
        store.dispatch(&id, now).unwrap();
        store.settle(&id, CommandState::Unknown, None, now).unwrap();
        let resolve = CommandResolveRequest {
            id: Ulid::new().to_string(),
            command_id: id.clone(),
            resolution: CommandResolution::Applied,
            reason: "Check native selection".into(),
        };
        assert_eq!(
            store
                .resolve_command("owner", &resolve, now)
                .unwrap_err()
                .code,
            "bad_state"
        );
        let mut proof = ConfigureReadback {
            agent: agent.clone(),
            generation: 1,
            native_ref: "muse-session".into(),
            model: "m0".into(),
        };
        assert_eq!(
            store
                .resolve_command_with_readback("owner", &resolve, Some(&proof), now)
                .unwrap_err()
                .code,
            "bad_state"
        );
        assert_eq!(
            store.get_command(&id).unwrap().unwrap().state,
            CommandState::Unknown
        );
        assert!(store.get_agent_config_current(&agent, 1).unwrap().is_none());
        proof.model = "m1".into();
        let applied = store
            .resolve_command_with_readback("owner", &resolve, Some(&proof), now)
            .unwrap();
        assert_eq!(applied.value.state, CommandState::Applied);
        let command = store.get_command(&id).unwrap().unwrap();
        assert_eq!(command.config_revision, 1);
        let config = store.get_agent_config(&agent, 1, 1).unwrap().unwrap();
        assert_eq!(config.model.as_deref(), Some("m1"));
        assert_eq!(config.effort.as_deref(), Some("low"));
        assert_eq!(
            store.resolve_command("owner", &resolve, now).unwrap().value,
            applied.value
        );
    }

    #[test]
    fn native_configure_refuses_pending_input_at_admission() {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let agent = ready_configure_agent(
            &mut store,
            "claude-pending-config",
            "claude",
            json!({"model":"haiku"}),
            now,
        );
        let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
        send.target = Some(agent.clone());
        send.args = json!({"text":"pending"});
        store.submit("owner", send, now).unwrap();
        let request = configure_request(&agent, 1, json!({"model":"claude-haiku-4-5"}));
        assert_eq!(
            store.submit("owner", request, now).unwrap_err().code,
            "busy"
        );
        assert!(store.get_agent_config_current(&agent, 1).unwrap().is_none());
    }

    #[test]
    fn configure_applies_overlay_and_stamps_monotonic_revisions() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let snapshot = json!({"model": "m0", "effort": "low"});
        let agent = ready_configure_agent(
            &mut store,
            "configure-basic",
            "codex",
            snapshot.clone(),
            now,
        );

        let first = configure_request(&agent, 1, json!({"model": "m1"}));
        let first_id = first.id.clone();
        store
            .submit("owner", first, now + 1)
            .expect("admit configure");
        store
            .dispatch(&first_id, now + 2)
            .expect("dispatch configure");
        let applied = store
            .apply_agent_config(&first_id, now + 3)
            .expect("apply configure");
        assert_eq!(applied.revision, 1, "first overlay is revision 1");
        assert_eq!(applied.requested_model.as_deref(), Some("m1"));
        assert_eq!(applied.requested_effort, None);
        assert_eq!(applied.model.as_deref(), Some("m1"), "requested model wins");
        assert_eq!(
            applied.effort.as_deref(),
            Some("low"),
            "unrequested effort falls back to the snapshot"
        );
        assert_eq!(applied.application, "next_turn");
        store
            .settle(&first_id, CommandState::Applied, None, now + 4)
            .expect("settle configure");

        let second = configure_request(&agent, 1, json!({"effort": "high"}));
        let second_id = second.id.clone();
        store
            .submit("owner", second, now + 5)
            .expect("admit second configure");
        store
            .dispatch(&second_id, now + 6)
            .expect("dispatch second configure");
        let reapplied = store
            .apply_agent_config(&second_id, now + 7)
            .expect("apply second configure");
        assert_eq!(reapplied.revision, 2, "revisions increase monotonically");
        assert_eq!(reapplied.requested_effort.as_deref(), Some("high"));
        assert_eq!(
            reapplied.model.as_deref(),
            Some("m0"),
            "unrequested model falls back to the snapshot"
        );

        // Reapplying the same command is idempotent, never a new revision.
        let replayed = store
            .apply_agent_config(&second_id, now + 8)
            .expect("reapply configure");
        assert_eq!(
            replayed.revision, 2,
            "reapply returns the retained revision"
        );
        let current = store
            .get_agent_config_current(&agent, 1)
            .expect("read current overlay")
            .expect("overlay exists");
        assert_eq!(current.revision, 2);

        // The frozen profile snapshot is unchanged by both overlays.
        let stored: eliot_core::Agent = store
            .get_agent(&agent)
            .expect("load agent")
            .expect("agent exists");
        assert_eq!(
            stored.profile_snapshot, snapshot,
            "snapshot is never rewritten"
        );
    }

    #[test]
    fn configure_native_effective_stores_verified_proof() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");

        // OpenCode: the native model proof replaces the resolved fill, while
        // the unrequested effort keeps the snapshot fill (no native knob).
        let opencode = ready_configure_agent(
            &mut store,
            "configure-native-opencode",
            "opencode",
            json!({"model": "p/m0"}),
            now,
        );
        let first = configure_request(&opencode, 1, json!({"model": "p/m1"}));
        let first_id = first.id.clone();
        store
            .submit("owner", first, now + 1)
            .expect("admit configure");
        store
            .dispatch(&first_id, now + 2)
            .expect("dispatch configure");
        let applied = store
            .apply_agent_config_native(&first_id, now + 3, Some("p/m1".to_owned()), None)
            .expect("apply verified configure");
        assert_eq!(applied.revision, 1);
        assert_eq!(applied.requested_model.as_deref(), Some("p/m1"));
        assert_eq!(applied.requested_effort, None);
        assert_eq!(applied.model.as_deref(), Some("p/m1"));
        assert_eq!(applied.effort, None, "no native knob keeps no effort");
        assert_eq!(applied.application, "next_turn");
        assert_eq!(applied.provenance, "configure");
        store
            .settle(&first_id, CommandState::Applied, None, now + 4)
            .expect("settle configure");

        // Antigravity: requested stays verbatim while the native proof carries
        // the recombined effective id and the explicit level.
        let antigravity = ready_configure_agent(
            &mut store,
            "configure-native-antigravity",
            "antigravity",
            json!({"model": "base-low"}),
            now + 10,
        );
        let second = configure_request(
            &antigravity,
            1,
            json!({"model": "base2-low", "effort": "high"}),
        );
        let second_id = second.id.clone();
        store
            .submit("owner", second, now + 11)
            .expect("admit configure");
        store
            .dispatch(&second_id, now + 12)
            .expect("dispatch configure");
        let applied = store
            .apply_agent_config_native(
                &second_id,
                now + 13,
                Some("base2-high".to_owned()),
                Some("high".to_owned()),
            )
            .expect("apply verified configure");
        assert_eq!(applied.revision, 1);
        assert_eq!(applied.requested_model.as_deref(), Some("base2-low"));
        assert_eq!(applied.requested_effort.as_deref(), Some("high"));
        assert_eq!(applied.model.as_deref(), Some("base2-high"));
        assert_eq!(applied.effort.as_deref(), Some("high"));
        store
            .settle(&second_id, CommandState::Applied, None, now + 14)
            .expect("settle configure");

        // A requested field without proven native proof stores nothing, and a
        // malformed native value is refused at the trust boundary.
        let missing = configure_request(&opencode, 1, json!({"model": "p/m2"}));
        let missing_id = missing.id.clone();
        store
            .submit("owner", missing, now + 20)
            .expect("admit configure");
        store
            .dispatch(&missing_id, now + 21)
            .expect("dispatch configure");
        assert_eq!(
            store
                .apply_agent_config_native(&missing_id, now + 22, None, None)
                .unwrap_err()
                .code,
            "source_integrity",
            "requested model without proof stores nothing"
        );
        assert_eq!(
            store
                .apply_agent_config_native(&missing_id, now + 23, Some("   ".to_owned()), None)
                .unwrap_err()
                .code,
            "invalid",
            "malformed native proof stores nothing"
        );
        assert_eq!(
            store
                .get_agent_config_current(&opencode, 1)
                .expect("read overlay")
                .expect("overlay exists")
                .revision,
            1,
            "failed native applies keep the retained revision"
        );
    }

    #[test]
    fn configure_rejects_authority_unknown_and_malformed_fields() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let agent = ready_configure_agent(
            &mut store,
            "configure-rejections",
            "codex",
            json!({"model": "m0"}),
            now,
        );
        for (args, code) in [
            (json!({"permissions": {"scope": "all"}}), "not_allowed"),
            (json!({"approval": "auto"}), "not_allowed"),
            (json!({"mcp": {}}), "not_allowed"),
            (json!({"env": {}}), "not_allowed"),
            (json!({"route_binding": "x"}), "not_allowed"),
            (json!({"account": "other"}), "not_allowed"),
            (json!({"bogus": "x"}), "unsupported"),
            (json!({"model": "m1", "bogus": "x"}), "unsupported"),
            (json!({}), "invalid"),
            (json!({"model": 1}), "invalid"),
            (json!({"model": ""}), "invalid"),
            (json!({"model": "   "}), "invalid"),
            (json!({"model": null}), "invalid"),
        ] {
            let error = store
                .submit("owner", configure_request(&agent, 1, args), now + 1)
                .unwrap_err();
            assert_eq!(error.code, code, "configure args must be refused honestly");
        }
        // Explicit JSON nulls are absent fields, not values.
        store
            .submit(
                "owner",
                configure_request(&agent, 1, json!({"effort": "low", "model": null})),
                now + 2,
            )
            .expect("null model with a real effort configures effort");
        assert_eq!(
            store
                .get_agent_config_current(&agent, 1)
                .expect("read overlay"),
            None,
            "refused configures store nothing"
        );
    }

    #[test]
    fn configure_gates_harness_generation_state_and_principal() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let codex = ready_configure_agent(
            &mut store,
            "configure-gates",
            "codex",
            json!({"model": "m0"}),
            now,
        );
        let muse = ready_configure_agent(
            &mut store,
            "configure-gates-muse",
            "muse",
            json!({"model": "m0", "effort": "low"}),
            now + 1,
        );
        let claude = ready_configure_agent(
            &mut store,
            "configure-gates-claude",
            "claude",
            json!({"model": "m0"}),
            now + 2,
        );
        let opencode = ready_configure_agent(
            &mut store,
            "configure-gates-opencode",
            "opencode",
            json!({"model": "m0"}),
            now + 3,
        );
        let antigravity = ready_configure_agent(
            &mut store,
            "configure-gates-antigravity",
            "antigravity",
            json!({"model": "m0"}),
            now + 4,
        );

        // Muse effort is a per-turn override and the Muse model switches
        // the live session through `session/setModel`, so both admit here;
        // the model overlay itself is stored by the Router only after
        // native proof.
        store
            .submit(
                "owner",
                configure_request(&muse, 1, json!({"effort": "high"})),
                now + 10,
            )
            .expect("admit Muse effort configure");
        store
            .submit(
                "owner",
                configure_request(&muse, 1, json!({"model": "m9"})),
                now + 11,
            )
            .expect("admit Muse model configure");
        store
            .submit(
                "owner",
                configure_request(&muse, 1, json!({"model": "m9", "effort": "high"})),
                now + 12,
            )
            .expect("admit mixed Muse configure");
        // Claude switches the live session model through `set_model`
        // between turns, so a model configure admits; effort has no live
        // switch and stays refused.
        store
            .submit(
                "owner",
                configure_request(&claude, 1, json!({"model": "m9"})),
                now + 13,
            )
            .expect("admit Claude model configure");
        assert_eq!(
            store
                .submit(
                    "owner",
                    configure_request(&claude, 1, json!({"effort": "high"})),
                    now + 14
                )
                .unwrap_err()
                .code,
            "unsupported",
            "Claude effort has no live switch (launch_only)"
        );
        // OpenCode model and effort (a native model variant) and Antigravity
        // model/effort ride their proven native setters, so admission accepts
        // them; the overlay itself is stored by the Router only after native
        // proof.
        store
            .submit(
                "owner",
                configure_request(&opencode, 1, json!({"model": "p/m1", "effort": "high"})),
                now + 15,
            )
            .expect("admit OpenCode model and effort configure");
        store
            .submit(
                "owner",
                configure_request(
                    &antigravity,
                    1,
                    json!({"model": "base-low", "effort": "high"}),
                ),
                now + 16,
            )
            .expect("admit Antigravity model and effort configure");
        assert_eq!(
            store
                .get_agent_config_current(&muse, 1)
                .expect("read Muse overlay"),
            None,
            "admitted Muse model configure stores nothing before dispatch apply"
        );

        // Stale bindings, non-Owner principals and dead sessions are rejected.
        assert_eq!(
            store
                .submit(
                    "owner",
                    configure_request(&codex, 99, json!({"effort": "high"})),
                    now + 20
                )
                .unwrap_err()
                .code,
            "stale_generation"
        );
        assert_eq!(
            store
                .submit(
                    "agent:a9#1",
                    configure_request(&codex, 1, json!({"effort": "high"})),
                    now + 21
                )
                .unwrap_err()
                .code,
            "not_allowed",
            "configure requires the Owner principal"
        );
        store
            .connection
            .execute(
                "UPDATE agents SET state = 'stopped' WHERE id = ?1",
                [eliot_core::parse_agent_id(&codex).expect("parse agent")],
            )
            .expect("stop Agent");
        assert_eq!(
            store
                .submit(
                    "owner",
                    configure_request(&codex, 1, json!({"effort": "high"})),
                    now + 22
                )
                .unwrap_err()
                .code,
            "bad_state",
            "configure requires a live Agent generation"
        );
    }

    #[test]
    fn sends_run_under_their_dispatch_revision_and_turns_inherit_it() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let agent = ready_configure_agent(
            &mut store,
            "configure-freeze",
            "codex",
            json!({"model": "m0", "effort": "low"}),
            now,
        );

        // First send dispatched under the frozen snapshot, before any overlay.
        let send_one = send_request(&agent, 1, "first input");
        let send_one_id = send_one.id.clone();
        store
            .submit("owner", send_one, now + 1)
            .expect("admit first send");
        store
            .dispatch(&send_one_id, now + 2)
            .expect("dispatch first send");

        // The overlay is admitted while the first send is still unassociated:
        // dispatch queues it on the held settings lane instead of applying.
        let configure = configure_request(&agent, 1, json!({"model": "m1", "effort": "high"}));
        let configure_id = configure.id.clone();
        store
            .submit("owner", configure, now + 3)
            .expect("admit configure");
        store
            .dispatch(&configure_id, now + 4)
            .expect("queue configure");
        assert_eq!(
            store
                .get_command(&configure_id)
                .expect("load configure")
                .expect("configure exists")
                .state,
            CommandState::Queued,
            "unassociated send input holds the settings lane"
        );

        // A later send is admitted while the overlay is still pending.
        let send_two = send_request(&agent, 1, "second input");
        let send_two_id = send_two.id.clone();
        store
            .submit("owner", send_two, now + 6)
            .expect("admit second send");

        // Complete the first turn so both lanes free, then let the queued
        // configure apply before the second send dispatches.
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-freeze:frozen".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![
                        NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: "configure-freeze:frozen-turn:started".to_owned(),
                            data: json!({"command_id": send_one_id, "turn_id": "frozen-turn"}),
                        },
                        NativeEvent {
                            ts: None,
                            kind: "message.completed".to_owned(),
                            native_id: "configure-freeze:frozen-turn:message".to_owned(),
                            data: json!({"command_id": send_one_id, "turn_id": "frozen-turn", "text": "first reply"}),
                        },
                        NativeEvent {
                            ts: None,
                            kind: "turn.completed".to_owned(),
                            native_id: "configure-freeze:frozen-turn:completed".to_owned(),
                            data: json!({"command_id": send_one_id, "turn_id": "frozen-turn", "status": "completed"}),
                        },
                    ],
                    cursor: None,
                },
                now + 7,
            )
            .expect("complete frozen turn");
        store
            .dispatch(&configure_id, now + 8)
            .expect("dispatch queued configure");
        let applied = store
            .apply_agent_config(&configure_id, now + 9)
            .expect("apply configure");
        assert_eq!(applied.revision, 1);
        // The Router settles applied right after the Store commit; only then
        // does the lane release for later input.
        store
            .settle(&configure_id, CommandState::Applied, None, now + 9)
            .expect("settle configure");

        // Association for the first send still binds the snapshot even though
        // the association lands after the overlay: a send committed before an
        // overlay never runs under newer configuration.
        assert_eq!(
            store
                .send_effective_config(&send_one_id)
                .expect("resolve first send"),
            None,
            "pre-overlay send runs under the frozen snapshot"
        );
        // The second send stamps its revision at its own dispatch, after the
        // overlay applied.
        store
            .dispatch(&send_two_id, now + 10)
            .expect("dispatch second send");
        let effective = store
            .send_effective_config(&send_two_id)
            .expect("resolve second send")
            .expect("overlay applies");
        assert_eq!(effective.revision, 1);
        assert_eq!(effective.model.as_deref(), Some("m1"));
        assert_eq!(effective.effort.as_deref(), Some("high"));
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-freeze:overlay".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: "configure-freeze:overlay-turn:started".to_owned(),
                        data: json!({"command_id": send_two_id, "turn_id": "overlay-turn"}),
                    }],
                    cursor: None,
                },
                now + 9,
            )
            .expect("associate overlay turn");
        let frozen = store
            .get_turn(&agent, 1, "frozen-turn")
            .expect("load frozen turn")
            .expect("turn exists");
        assert_eq!(
            frozen.config_revision, 0,
            "turn records the snapshot revision"
        );
        let overlay = store
            .get_turn(&agent, 1, "overlay-turn")
            .expect("load overlay turn")
            .expect("turn exists");
        assert_eq!(
            overlay.config_revision, 1,
            "turn records the revision it ran under"
        );
        let overlay_outcome = store
            .get_command(&send_two_id)
            .expect("load second send")
            .expect("send exists")
            .outcome
            .expect("send has an outcome");
        assert_eq!(overlay_outcome["config_revision"], json!(1));
        assert_eq!(
            overlay_outcome["model"],
            json!("m1"),
            "first overlay turn shows its effective model"
        );
        assert_eq!(overlay_outcome["effort"], json!("high"));
        let frozen_outcome = store
            .get_command(&send_one_id)
            .expect("load first send")
            .expect("send exists")
            .outcome
            .expect("send has an outcome");
        assert!(
            frozen_outcome.get("config_revision").is_none(),
            "snapshot sends keep their exact outcome shape"
        );
    }

    #[test]
    fn overlay_turn_carries_refresh_agent_effective_as_verified() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        // Launch with an admission readback: low effort, verified.
        let launch_id = Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"configure-refresh", "workspace":"synthetic-workspace", "harness":"codex", "profile":"fake", "profile_snapshot":json!({"model":"m0","effort":"low"})}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        install_fixture_sources(&mut store, &agent, &launch_id, 1, now);
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-refresh:ready".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: "configure-refresh:ready".to_owned(),
                        data: json!({"command_id":launch_id, "effective":{"model":"m0","effort":"low","effort_verified":true,"model_verified":true}}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");

        // A plain snapshot turn carrying no model/effort leaves the admission
        // readback untouched.
        let plain = send_request(&agent, 1, "plain input");
        let plain_id = plain.id.clone();
        store
            .submit("owner", plain, now + 1)
            .expect("admit plain send");
        store
            .dispatch(&plain_id, now + 2)
            .expect("dispatch plain send");
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-refresh:plain".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: "configure-refresh:plain:started".to_owned(),
                        data: json!({"command_id": plain_id, "turn_id": "plain-turn"}),
                    }],
                    cursor: None,
                },
                now + 3,
            )
            .expect("associate plain turn");
        let before = store
            .get_agent(&agent)
            .expect("load agent")
            .expect("agent exists");
        assert_eq!(
            before
                .effective
                .as_ref()
                .and_then(|effective| effective.get("effort")),
            Some(&json!("low")),
            "plain turn invents no effective values"
        );

        // Apply an overlay, then associate its turn carrying the native request
        // values: the Agent readback refreshes to the verified new effort.
        let configure = configure_request(&agent, 1, json!({"model": "m1", "effort": "high"}));
        let configure_id = configure.id.clone();
        store
            .submit("owner", configure, now + 4)
            .expect("admit configure");
        store
            .dispatch(&configure_id, now + 5)
            .expect("dispatch configure");
        store
            .apply_agent_config(&configure_id, now + 6)
            .expect("apply configure");
        store
            .settle(&configure_id, CommandState::Applied, None, now + 6)
            .expect("settle configure");
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-refresh:plain-done".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![
                        NativeEvent {
                            ts: None,
                            kind: "message.completed".to_owned(),
                            native_id: "configure-refresh:plain:message".to_owned(),
                            data: json!({"command_id": plain_id, "turn_id": "plain-turn", "text": "plain reply"}),
                        },
                        NativeEvent {
                            ts: None,
                            kind: "turn.completed".to_owned(),
                            native_id: "configure-refresh:plain:completed".to_owned(),
                            data: json!({"command_id": plain_id, "turn_id": "plain-turn", "status": "completed"}),
                        },
                    ],
                    cursor: None,
                },
                now + 7,
            )
            .expect("complete plain turn");
        let overlay_send = send_request(&agent, 1, "overlay input");
        let overlay_id = overlay_send.id.clone();
        store
            .submit("owner", overlay_send, now + 8)
            .expect("admit overlay send");
        store
            .dispatch(&overlay_id, now + 9)
            .expect("dispatch overlay send");
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-refresh:overlay".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: "configure-refresh:overlay:started".to_owned(),
                        data: json!({"command_id": overlay_id, "turn_id": "overlay-turn", "model": "m1", "effort": "high"}),
                    }],
                    cursor: None,
                },
                now + 10,
            )
            .expect("associate overlay turn");
        let after = store
            .get_agent(&agent)
            .expect("load agent")
            .expect("agent exists");
        let effective = after
            .effective
            .expect("effective survives the overlay turn");
        assert_eq!(effective["model"], json!("m1"));
        assert_eq!(effective["effort"], json!("high"));
        assert_eq!(effective["model_verified"], json!(true));
        assert_eq!(effective["effort_verified"], json!(true));
    }

    #[test]
    fn configure_and_send_share_one_admission_order() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let agent = ready_configure_agent(
            &mut store,
            "configure-lane",
            "codex",
            json!({"model": "m0", "effort": "low"}),
            now,
        );
        let state_of = |store: &Store, id: &str| {
            store
                .get_command(id)
                .expect("load command")
                .expect("command exists")
                .state
        };
        let batch =
            |batch_id: &str, kind: &str, native_id: &str, data: serde_json::Value| SourceBatch {
                batch_id: batch_id.to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: kind.to_owned(),
                    native_id: native_id.to_owned(),
                    data,
                }],
                cursor: None,
            };

        // Occupy the Agent with an associated active turn.
        let active = send_request(&agent, 1, "occupying turn");
        let active_id = active.id.clone();
        store
            .submit("owner", active, now + 1)
            .expect("admit occupying send");
        store
            .dispatch(&active_id, now + 2)
            .expect("dispatch occupying send");
        store
            .put_batch(
                batch(
                    "configure-lane:occupying",
                    "turn.started",
                    "configure-lane:occupying:started",
                    json!({"command_id": active_id, "turn_id": "lane-turn-0"}),
                ),
                now + 3,
            )
            .expect("associate occupying turn");

        // Admit SEND S, then configure C, on the busy Agent.
        let send = send_request(&agent, 1, "queued input");
        let send_id = send.id.clone();
        store
            .submit("owner", send, now + 4)
            .expect("admit queued send");
        let configure = configure_request(&agent, 1, json!({"model": "m1"}));
        let configure_id = configure.id.clone();
        store
            .submit("owner", configure, now + 5)
            .expect("admit configure");

        // A configure never overtakes an earlier-admitted send: C queues behind
        // the still-queued S instead of applying immediately.
        store
            .dispatch(&configure_id, now + 6)
            .expect("queue configure");
        assert_eq!(
            state_of(&store, &configure_id),
            CommandState::Queued,
            "configure waits for the earlier queued send"
        );

        // Free the Agent; the earlier send dispatches first and freezes the
        // snapshot revision even though a configure is pending.
        store
            .put_batch(
                batch(
                    "configure-lane:free",
                    "turn.completed",
                    "configure-lane:free:completed",
                    json!({"command_id": active_id, "turn_id": "lane-turn-0", "status": "completed"}),
                ),
                now + 7,
            )
            .expect("complete occupying turn");
        store
            .dispatch(&send_id, now + 8)
            .expect("dispatch earlier send");
        assert_eq!(
            state_of(&store, &send_id),
            CommandState::Dispatched,
            "earlier send passes the later configure"
        );
        assert!(
            store
                .send_effective_config(&send_id)
                .expect("resolve earlier send")
                .is_none(),
            "earlier send runs under the frozen snapshot"
        );

        // C still waits for S's turn association, then applies as revision 1.
        store
            .dispatch(&configure_id, now + 9)
            .expect("redispatch configure");
        assert_eq!(
            state_of(&store, &configure_id),
            CommandState::Queued,
            "configure waits for the earlier unassociated send"
        );
        store
            .put_batch(
                batch(
                    "configure-lane:associated",
                    "turn.started",
                    "configure-lane:associated:started",
                    json!({"command_id": send_id, "turn_id": "lane-turn-1"}),
                ),
                now + 10,
            )
            .expect("associate earlier send");
        store
            .dispatch(&configure_id, now + 11)
            .expect("dispatch configure");
        let applied = store
            .apply_agent_config(&configure_id, now + 12)
            .expect("apply configure");
        assert_eq!(applied.revision, 1);
        store
            .settle(&configure_id, CommandState::Applied, None, now + 12)
            .expect("settle configure");

        // A later input waits for the earlier configure while otherwise ready,
        // then runs under it.
        store
            .put_batch(
                batch(
                    "configure-lane:done",
                    "turn.completed",
                    "configure-lane:done:completed",
                    json!({"command_id": send_id, "turn_id": "lane-turn-1", "status": "completed"}),
                ),
                now + 13,
            )
            .expect("complete earlier turn");
        let configure_two = configure_request(&agent, 1, json!({"effort": "high"}));
        let configure_two_id = configure_two.id.clone();
        store
            .submit("owner", configure_two, now + 14)
            .expect("admit second configure");
        store
            .dispatch(&configure_two_id, now + 15)
            .expect("dispatch second configure");
        let later = send_request(&agent, 1, "later input");
        let later_id = later.id.clone();
        store
            .submit("owner", later, now + 16)
            .expect("admit later send");
        store
            .dispatch(&later_id, now + 17)
            .expect("queue later send");
        assert_eq!(
            state_of(&store, &later_id),
            CommandState::Queued,
            "later input waits for the earlier configure"
        );
        let applied_two = store
            .apply_agent_config(&configure_two_id, now + 18)
            .expect("apply second configure");
        assert_eq!(applied_two.revision, 2);
        store
            .settle(&configure_two_id, CommandState::Applied, None, now + 18)
            .expect("settle second configure");
        store
            .dispatch(&later_id, now + 19)
            .expect("dispatch later send");
        assert_eq!(state_of(&store, &later_id), CommandState::Dispatched);
        let effective = store
            .send_effective_config(&later_id)
            .expect("resolve later send")
            .expect("overlay applies");
        assert_eq!(effective.revision, 2, "next input runs under C");
    }

    #[test]
    fn turn_and_generation_readback_show_resolved_native_effort() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        // The profile omits effort while native startup resolves it (e.g. low).
        let agent = ready_configure_agent(
            &mut store,
            "configure-effort",
            "codex",
            json!({"model": "m0"}),
            now,
        );

        // A model-only configure stores no effective effort of its own.
        let configure = configure_request(&agent, 1, json!({"model": "m1"}));
        let configure_id = configure.id.clone();
        store
            .submit("owner", configure, now + 1)
            .expect("admit configure");
        store
            .dispatch(&configure_id, now + 2)
            .expect("dispatch configure");
        let applied = store
            .apply_agent_config(&configure_id, now + 3)
            .expect("apply configure");
        assert_eq!(applied.revision, 1);
        assert_eq!(applied.effort, None, "no effort was requested or retained");
        assert_eq!(applied.requested_effort, None);
        store
            .settle(&configure_id, CommandState::Applied, None, now + 3)
            .expect("settle configure");

        // The next send runs under the overlay; the native request carries the
        // resolved effort, which the plugin reports on the turn association.
        let send = send_request(&agent, 1, "first turn");
        let send_id = send.id.clone();
        store.submit("owner", send, now + 4).expect("admit send");
        store.dispatch(&send_id, now + 5).expect("dispatch send");
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-effort:turn".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.started".to_owned(),
                        native_id: "configure-effort:turn:started".to_owned(),
                        data: json!({"command_id": send_id, "turn_id": "effort-turn", "effort": "low"}),
                    }],
                    cursor: None,
                },
                now + 6,
            )
            .expect("associate turn");
        let outcome = store
            .get_command(&send_id)
            .expect("load send")
            .expect("send exists")
            .outcome
            .expect("send has an outcome");
        assert_eq!(
            outcome["effort"],
            json!("low"),
            "turn shows the effort actually sent"
        );
        assert_eq!(outcome["model"], json!("m1"));
        let current = store
            .get_agent_config_current(&agent, 1)
            .expect("read overlay")
            .expect("overlay exists");
        assert_eq!(
            current.effort.as_deref(),
            Some("low"),
            "generation readback shows the resolved effort"
        );
        assert_eq!(
            current.requested_effort, None,
            "requested override stays absent"
        );
        let turn = store
            .get_turn(&agent, 1, "effort-turn")
            .expect("load turn")
            .expect("turn exists");
        assert_eq!(turn.config_revision, 1);

        // A later send under the same revision reads back the recorded effort.
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-effort:done".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "turn.completed".to_owned(),
                        native_id: "configure-effort:done:completed".to_owned(),
                        data: json!({"command_id": send_id, "turn_id": "effort-turn", "status": "completed"}),
                    }],
                    cursor: None,
                },
                now + 7,
            )
            .expect("complete turn");
        let second = send_request(&agent, 1, "second turn");
        let second_id = second.id.clone();
        store
            .submit("owner", second, now + 8)
            .expect("admit second send");
        store
            .dispatch(&second_id, now + 9)
            .expect("dispatch second send");
        let effective = store
            .send_effective_config(&second_id)
            .expect("resolve second send")
            .expect("overlay applies");
        assert_eq!(
            effective.effort.as_deref(),
            Some("low"),
            "recorded effort carries to the next input"
        );
    }

    #[test]
    fn resume_inherits_latest_overlay_as_revision_one_with_provenance() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let snapshot = json!({"model": "m0", "effort": "low"});
        let agent = ready_configure_agent(
            &mut store,
            "configure-resume-inherit",
            "codex",
            snapshot.clone(),
            now,
        );
        let configure = configure_request(&agent, 1, json!({"model": "m1"}));
        let configure_id = configure.id.clone();
        store
            .submit("owner", configure, now + 1)
            .expect("admit configure");
        store
            .dispatch(&configure_id, now + 2)
            .expect("dispatch configure");
        let applied = store
            .apply_agent_config(&configure_id, now + 3)
            .expect("apply configure");
        assert_eq!(applied.revision, 1);
        assert_eq!(applied.provenance, "configure");
        store
            .settle(&configure_id, CommandState::Applied, None, now + 3)
            .expect("settle configure");

        // Retain a native subject, then retire generation 1 through the honest
        // terminal fact so `resume` admits.
        let agent_num = eliot_core::parse_agent_id(&agent).expect("Agent id");
        store
            .connection
            .execute(
                "UPDATE agents SET native_scope = ?1, native_ref = ?2 WHERE id = ?3",
                rusqlite::params![
                    "configure-resume-inherit:scope",
                    "configure-resume-inherit:thread",
                    agent_num
                ],
            )
            .expect("retain a native subject for resume");
        install_live_custody_observations(&mut store, &agent, 1, now + 4);
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-resume-inherit:ended".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ended".to_owned(),
                        native_id: "configure-resume-inherit:ended".to_owned(),
                        data: json!({}),
                    }],
                    cursor: None,
                },
                now + 5,
            )
            .expect("commit terminal session fact");
        simulate_normal_stop_proof(&mut store, &agent, 1, now + 5);

        let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
        resume.target = Some(agent.clone());
        resume.expected_generation = Some(1);
        let resume_id = resume.id.clone();
        store
            .submit("owner", resume, now + 6)
            .expect("admit resume");
        store
            .dispatch(&resume_id, now + 6)
            .expect("dispatch resume");
        assert_eq!(
            store
                .get_agent(&agent)
                .expect("load agent")
                .expect("agent exists")
                .generation,
            2,
            "resume starts the next generation"
        );

        // The new generation explicitly inherits the previous overlay as its
        // own revision 1 with `inherited_from` provenance; the frozen snapshot
        // itself is unchanged.
        let inherited = store
            .get_agent_config_current(&agent, 2)
            .expect("read inherited overlay")
            .expect("resume inherits an overlay");
        assert_eq!(inherited.revision, 1);
        assert_eq!(inherited.provenance, "inherited_from");
        assert_eq!(inherited.requested_model.as_deref(), Some("m1"));
        assert_eq!(inherited.model.as_deref(), Some("m1"));
        assert_eq!(inherited.effort.as_deref(), Some("low"));
        assert_eq!(inherited.command_id, resume_id);
        let stored = store
            .get_agent(&agent)
            .expect("load agent")
            .expect("agent exists");
        assert_eq!(
            stored.profile_snapshot, snapshot,
            "snapshot never rewritten"
        );
        let resume_command = store
            .get_command(&resume_id)
            .expect("load resume")
            .expect("resume exists");
        assert_eq!(resume_command.generation, Some(2));
        assert_eq!(
            resume_command.config_revision, 1,
            "resume runs under its inherited revision"
        );

        // The previous generation's overlay is untouched.
        let previous = store
            .get_agent_config(&agent, 1, 1)
            .expect("read previous overlay")
            .expect("previous overlay retained");
        assert_eq!(previous.provenance, "configure");
        assert_eq!(previous.command_id, configure_id);

        // Complete the resume lifecycle so the new generation can take input.
        install_fixture_sources(&mut store, &agent, &resume_id, 2, now + 6);
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-resume-inherit:ready".to_owned(),
                    agent_id: agent.clone(),
                    generation: 2,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: "configure-resume-inherit:ready".to_owned(),
                        data: json!({"command_id": resume_id,
                            "native_scope": "configure-resume-inherit:scope",
                            "native_ref": "configure-resume-inherit:thread"}),
                    }],
                    cursor: None,
                },
                now + 6,
            )
            .expect("commit resumed session ready");
        store
            .settle(&resume_id, CommandState::Applied, None, now + 6)
            .expect("settle resume");

        // The next send in the new generation runs under the inherited overlay.
        let send = send_request(&agent, 2, "after resume");
        let send_id = send.id.clone();
        store.submit("owner", send, now + 7).expect("admit send");
        store.dispatch(&send_id, now + 8).expect("dispatch send");
        let effective = store
            .send_effective_config(&send_id)
            .expect("resolve send")
            .expect("inherited overlay applies");
        assert_eq!(effective.revision, 1);
        assert_eq!(effective.model.as_deref(), Some("m1"));
    }

    #[test]
    fn retained_config_survives_config_file_reload_for_pending_and_unknown() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let snapshot = json!({"model": "m0", "effort": "low"});
        let agent = ready_configure_agent(
            &mut store,
            "configure-reload-proof",
            "codex",
            snapshot.clone(),
            now,
        );
        let configure = configure_request(&agent, 1, json!({"model": "m1", "effort": "high"}));
        let configure_id = configure.id.clone();
        store
            .submit("owner", configure, now + 1)
            .expect("admit configure");
        store
            .dispatch(&configure_id, now + 2)
            .expect("dispatch configure");
        store
            .apply_agent_config(&configure_id, now + 3)
            .expect("apply configure");
        store
            .settle(&configure_id, CommandState::Applied, None, now + 3)
            .expect("settle configure");

        // A first send dispatched under the overlay, plus a pending send admitted
        // while its turn is still unassociated.
        let first = send_request(&agent, 1, "first");
        let first_id = first.id.clone();
        store.submit("owner", first, now + 4).expect("admit first");
        store.dispatch(&first_id, now + 5).expect("dispatch first");
        let pending = send_request(&agent, 1, "pending");
        let pending_id = pending.id.clone();
        store
            .submit("owner", pending, now + 6)
            .expect("admit pending");

        // A config-file reload edits the profile: a new launch resolves the
        // edited snapshot, while the retained generation keeps its frozen one.
        let reloaded_snapshot = json!({"model": "m-edited", "effort": "low"});
        let launch_id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"configure-reload-proof-new", "workspace":"synthetic-workspace", "harness":"codex", "profile":"fake", "profile_snapshot":reloaded_snapshot.clone()}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now + 7,
            )
            .expect("admit reloaded launch");
        let reloaded_target = store
            .get_command(&launch_id)
            .expect("load reloaded launch")
            .expect("launch exists")
            .target
            .expect("launch target");
        let reloaded_row = store
            .get_agent(&reloaded_target)
            .expect("load reloaded agent")
            .expect("agent exists");
        assert_eq!(
            reloaded_row.profile_snapshot, reloaded_snapshot,
            "new launches resolve the reloaded profile"
        );

        let retained = store
            .get_agent(&agent)
            .expect("load retained agent")
            .expect("agent exists");
        assert_eq!(
            retained.profile_snapshot, snapshot,
            "reload never rewrites a retained generation"
        );
        assert_eq!(
            store
                .send_effective_config(&first_id)
                .expect("resolve first")
                .expect("overlay applies")
                .model
                .as_deref(),
            Some("m1"),
            "dispatched send keeps its original configuration"
        );

        // The pending send dispatches after the reload and still runs under the
        // retained overlay, not the reloaded profile. Complete the first turn
        // first so the send lane frees; the lane holds a configure-pending send
        // on its unassociated input.
        store
            .put_batch(
                SourceBatch {
                    batch_id: "configure-reload-proof:turn".to_owned(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![
                        NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: "configure-reload-proof:turn:started".to_owned(),
                            data: json!({"command_id": first_id, "turn_id": "reload-turn"}),
                        },
                        NativeEvent {
                            ts: None,
                            kind: "message.completed".to_owned(),
                            native_id: "configure-reload-proof:turn:message".to_owned(),
                            data: json!({"command_id": first_id, "turn_id": "reload-turn", "text": "first reply"}),
                        },
                        NativeEvent {
                            ts: None,
                            kind: "turn.completed".to_owned(),
                            native_id: "configure-reload-proof:turn:completed".to_owned(),
                            data: json!({"command_id": first_id, "turn_id": "reload-turn", "status": "completed"}),
                        },
                    ],
                    cursor: None,
                },
                now + 8,
            )
            .expect("complete first turn");
        store
            .dispatch(&pending_id, now + 9)
            .expect("dispatch pending");
        let pending_command = store
            .get_command(&pending_id)
            .expect("load pending")
            .expect("send exists");
        assert_eq!(
            pending_command.config_revision, 1,
            "pending send dispatched under its retained revision"
        );
        assert_eq!(
            store
                .command_effective_config(&pending_id, &["send"])
                .expect("resolve pending")
                .expect("overlay applies")
                .model
                .as_deref(),
            Some("m1"),
            "pending command still reads back its original configuration"
        );

        // An unknown outcome still resolves to the retained configuration: the
        // revision stamped at dispatch is read back, never the reloaded file.
        store
            .recover_on_restart(now + 10)
            .expect("fence dispatched send as unknown");
        let unknown = store
            .get_command(&pending_id)
            .expect("load unknown")
            .expect("send exists");
        assert_eq!(unknown.state, CommandState::Unknown);
        assert_eq!(unknown.config_revision, 1);
        assert_eq!(
            store
                .command_effective_config(&pending_id, &["send"])
                .expect("resolve unknown")
                .expect("overlay applies")
                .model
                .as_deref(),
            Some("m1"),
            "unknown-outcome resolution keeps the original configuration"
        );
        assert_eq!(
            store
                .turn_effective_config(&agent, 1, "reload-turn")
                .expect("resolve turn")
                .expect("turn overlay retained")
                .model
                .as_deref(),
            Some("m1"),
            "retained turn keeps its original configuration"
        );
    }

    #[test]
    fn retained_command_and_turn_views_survive_later_revisions() {
        let mut store = Store::open(Path::new(":memory:")).expect("open Store");
        let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
        let snapshot = json!({"model": "m0", "effort": "low"});
        let agent = ready_configure_agent(
            &mut store,
            "configure-view-history",
            "codex",
            snapshot.clone(),
            now,
        );

        // Revision 1, then a send and a completed turn under it.
        let first_configure = configure_request(&agent, 1, json!({"model": "m1"}));
        let first_configure_id = first_configure.id.clone();
        store
            .submit("owner", first_configure, now + 1)
            .expect("admit first configure");
        store
            .dispatch(&first_configure_id, now + 2)
            .expect("dispatch first configure");
        store
            .apply_agent_config(&first_configure_id, now + 3)
            .expect("apply first configure");
        store
            .settle(&first_configure_id, CommandState::Applied, None, now + 3)
            .expect("settle first configure");

        let first = send_request(&agent, 1, "first turn");
        let first_id = first.id.clone();
        store
            .submit("owner", first, now + 4)
            .expect("admit first send");
        store
            .dispatch(&first_id, now + 5)
            .expect("dispatch first send");
        for (kind, native_id, data) in [
            (
                "turn.started",
                "configure-view-history:turn:started",
                json!({"command_id": first_id, "turn_id": "view-turn"}),
            ),
            (
                "message.completed",
                "configure-view-history:turn:message",
                json!({"command_id": first_id, "turn_id": "view-turn", "text": "first reply"}),
            ),
            (
                "turn.completed",
                "configure-view-history:turn:completed",
                json!({"command_id": first_id, "turn_id": "view-turn", "status": "completed"}),
            ),
        ] {
            store
                .put_batch(
                    SourceBatch {
                        batch_id: format!("configure-view-history:{native_id}"),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: kind.to_owned(),
                            native_id: native_id.to_owned(),
                            data,
                        }],
                        cursor: None,
                    },
                    now + 6,
                )
                .expect("commit first turn event");
        }

        let agent_row = store
            .get_agent(&agent)
            .expect("load agent")
            .expect("agent exists");
        let first_command = store
            .get_command(&first_id)
            .expect("load first send")
            .expect("send exists");
        let first_view = store
            .command_config_view(&first_command)
            .expect("resolve first command view")
            .expect("revision 1 view exists");
        assert_eq!(first_view.revision, 1);
        assert_eq!(first_view.provenance, "configure");
        assert_eq!(first_view.profile, "fake");
        assert_eq!(first_view.requested_model.as_deref(), Some("m1"));
        assert_eq!(first_view.model.as_deref(), Some("m1"));
        assert_eq!(first_view.effort.as_deref(), Some("low"));
        assert_eq!(first_view.original_model.as_deref(), Some("m0"));

        let first_turn = store
            .get_turn(&agent, 1, "view-turn")
            .expect("load turn")
            .expect("turn exists");
        assert_eq!(first_turn.config_revision, 1);
        let turn_view = store
            .turn_config_view(&agent_row, &first_turn)
            .expect("resolve turn view")
            .expect("revision 1 turn view exists");
        assert_eq!(turn_view, first_view);
        assert!(!turn_view.profile_inherited);

        // Revision 2 applies afterwards: the retained revision-1 command and
        // turn still read back revision 1, while a new send reads revision 2.
        let second_configure = configure_request(&agent, 1, json!({"model": "m2"}));
        let second_configure_id = second_configure.id.clone();
        store
            .submit("owner", second_configure, now + 7)
            .expect("admit second configure");
        store
            .dispatch(&second_configure_id, now + 8)
            .expect("dispatch second configure");
        store
            .apply_agent_config(&second_configure_id, now + 9)
            .expect("apply second configure");
        store
            .settle(&second_configure_id, CommandState::Applied, None, now + 9)
            .expect("settle second configure");

        let retained_command = store
            .get_command(&first_id)
            .expect("reload first send")
            .expect("send exists");
        assert_eq!(
            store
                .command_config_view(&retained_command)
                .expect("resolve retained command view")
                .expect("revision 1 view survives"),
            first_view,
            "later revisions never rewrite a retained command view"
        );
        let retained_turn = store
            .get_turn(&agent, 1, "view-turn")
            .expect("reload turn")
            .expect("turn exists");
        assert_eq!(
            store
                .turn_config_view(&agent_row, &retained_turn)
                .expect("resolve retained turn view")
                .expect("revision 1 turn view survives"),
            first_view,
            "later revisions never rewrite a retained turn view"
        );

        let second = send_request(&agent, 1, "second turn");
        let second_id = second.id.clone();
        store
            .submit("owner", second, now + 10)
            .expect("admit second send");
        store
            .dispatch(&second_id, now + 11)
            .expect("dispatch second send");
        let second_command = store
            .get_command(&second_id)
            .expect("load second send")
            .expect("send exists");
        let second_view = store
            .command_config_view(&second_command)
            .expect("resolve second command view")
            .expect("revision 2 view exists");
        assert_eq!(second_view.revision, 2);
        assert_eq!(second_view.model.as_deref(), Some("m2"));
        assert_eq!(second_view.profile, "fake");

        // Commands without a native configuration expose no view.
        let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
        stop.target = Some(agent.clone());
        stop.expected_generation = Some(1);
        let stop_id = stop.id.clone();
        store.submit("owner", stop, now + 12).expect("admit stop");
        let stop_command = store
            .get_command(&stop_id)
            .expect("load stop")
            .expect("stop exists");
        assert_eq!(
            store
                .command_config_view(&stop_command)
                .expect("resolve stop view"),
            None,
            "non-native commands expose no configuration view"
        );
    }
}
