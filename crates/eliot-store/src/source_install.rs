use super::*;
use eliot_core::is_start_verb;

impl Store {
    /// Install the PluginHost-validated manifest source plan for a Starting
    /// generation, before its native start effect. The plan maps each common
    /// native family to its `stream` mechanism; ownership is written in one
    /// transaction that commits before the caller proceeds. An identical
    /// already-installed plan is a no-op; any conflicting installed ownership
    /// or already-committed native fact is refused. Only this
    /// `(agent, generation)` stream plan is considered, so confirmed plans of
    /// native children (other Agent rows) are never overwritten.
    pub fn install_plugin_sources(
        &mut self,
        agent: &str,
        generation: i64,
        start_command_id: &str,
        plan: &BTreeMap<String, String>,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result =
            self.install_plugin_sources_inner(agent, generation, start_command_id, plan, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn install_plugin_sources_inner(
        &mut self,
        agent: &str,
        generation: i64,
        start_command_id: &str,
        plan: &BTreeMap<String, String>,
        now: i64,
    ) -> Result<()> {
        if generation <= 0 {
            return Err(Error::new(
                "stale_generation",
                "install_plugin_sources requires an admitted generation",
            ));
        }
        if plan.len() > MAX_NATIVE_FAMILIES as usize {
            return Err(Error::new(
                "invalid",
                "plugin source plan exceeds its bound",
            ));
        }
        for (family, mechanism) in plan {
            if !source_handoff::is_native_family(family) {
                return Err(Error::new(
                    "unsupported",
                    format!("plugin source family {family} is not a common native family"),
                ));
            }
            if mechanism != INITIAL_SOURCE {
                return Err(Error::new(
                    "unsupported",
                    "plugin source plan mechanism must be stream",
                ));
            }
        }
        validate_ulid(start_command_id)?;
        let agent_num = parse_agent_id(agent)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let current = load_agent_by_num(&tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", format!("Agent {agent} was not found")))?;
        observer::require_controlled(&current)?;
        if current.generation != generation || current.state != AgentState::Starting {
            return Err(Error::new(
                "stale_generation",
                "install_plugin_sources requires the current Starting generation",
            ));
        }
        let record = load_command_record(&tx, start_command_id)?
            .ok_or_else(|| Error::new("not_found", "plugin start command was not found"))?;
        if record.state != CommandState::Dispatched
            || record.target != Some(agent_num)
            || !is_start_verb(record.command.verb.as_str())
            || record.command.generation != Some(generation)
        {
            return Err(Error::new(
                "stale_generation",
                "plugin source plan does not match the dispatched start command generation",
            ));
        }
        let mut statement = tx
            .prepare(
                "SELECT family, source, mechanism FROM source_owners
                 WHERE agent = ?1 AND generation = ?2 ORDER BY family",
            )
            .map_err(sql_error)?;
        let rows = statement
            .query_map(params![agent_num, generation], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .map_err(sql_error)?;
        let mut existing = rows
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        drop(statement);
        // Compare only registered native-family ownership: Monitoring may
        // already have installed OS/Git/daemon rows for this generation, and
        // those must be preserved untouched. A native family owned by any
        // other source or mechanism still conflicts below.
        existing.retain(|(family, _, _)| source_handoff::is_native_family(family));
        let source = load_source_state(&tx, agent_num, generation, INITIAL_SOURCE)?;
        let identical = existing.len() == plan.len()
            && existing.iter().all(|(family, owner, mechanism)| {
                owner == INITIAL_SOURCE && plan.get(family) == Some(mechanism)
            });
        if identical {
            match &source {
                Some(state) if state.state == "open" => {
                    commit_transaction(tx, &self.write_faulted)?;
                    return Ok(());
                }
                Some(_) => {
                    return Err(Error::new(
                        "source_integrity",
                        "plugin source stream is already closed",
                    ));
                }
                None => {}
            }
        }
        if !existing.is_empty() {
            return Err(Error::new(
                "source_integrity",
                "plugin source plan conflicts with already installed ownership",
            ));
        }
        if let Some(state) = &source {
            if state.state != "open" {
                return Err(Error::new(
                    "source_integrity",
                    "plugin source stream is already closed",
                ));
            }
            if state.cursor.is_some()
                || state.last_batch_id.is_some()
                || state.last_batch_digest.is_some()
                || state.last_store_seq.is_some()
            {
                return Err(Error::new(
                    "source_integrity",
                    "native facts are already committed for this generation",
                ));
            }
        }
        if source.is_none() {
            db!(tx.execute(
                "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
                 VALUES (?1, ?2, ?3, 'open', 'ok', ?4)",
                params![agent_num, generation, INITIAL_SOURCE, now],
            ));
        }
        for (family, mechanism) in plan {
            db!(tx.execute(
                "INSERT INTO source_owners(agent, generation, family, source, mechanism)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![agent_num, generation, family, INITIAL_SOURCE, mechanism],
            ));
        }
        commit_transaction(tx, &self.write_faulted)
    }
}
