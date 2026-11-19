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
