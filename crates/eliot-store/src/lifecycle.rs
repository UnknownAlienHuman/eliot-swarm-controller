use super::*;
use eliot_core::{is_attachment_verb, is_start_verb, is_user_event_verb};

impl Store {
    pub(super) fn settle_inner(
        &mut self,
        id: &str,
        state: CommandState,
        mut outcome: Option<Value>,
        now: i64,
        start_not_spawned: bool,
    ) -> Result<Change<Command>> {
        validate_settle_outcome(state, &outcome)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let mut record = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("not_found", format!("command {id} was not found")))?;
        check_settle_verb_guards(&tx, &record, state, &outcome, id)?;
        if record.command.verb == "inject" {
            let args = context::decode_inject_args(&record.command.args)?;
            if state == CommandState::Rejected
                && record.state == CommandState::Dispatched
                && args.point == InjectPoint::ActiveTurn
                && args.expires_at_ms.is_none_or(|expires| expires > now)
                && let Some(target) = record.target
                && let Some(agent) = load_agent_by_num(&tx, target)?
                && Some(agent.generation) == record.command.generation
                && context::next_input_available(&agent)
            {
                let outcome = context::move_context_to_next_input(&tx, id, now)?;
                let event = insert_daemon_command_event(
                    &tx,
                    now,
                    "context.moved",
                    Some(target),
                    Some(agent.generation),
                    id,
                    json!({"command_id": id, "outcome": outcome}),
                )?;
                let value = load_command(&tx, id)?
                    .ok_or_else(|| Error::new("source_integrity", "moved context disappeared"))?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value,
                    events: vec![event],
                });
            }
            let delivery = match state {
                CommandState::Applied => Some("delivered_in_turn"),
                CommandState::Rejected | CommandState::Failed | CommandState::Cancelled => {
                    Some("cancelled")
                }
                _ => None,
            };
            if let Some(delivery) = delivery {
                outcome.get_or_insert_with(|| json!({}))["delivery"] = json!(delivery);
            }
        }
        if let Some(command) = check_settle_idempotent(&record, state, outcome.as_ref())? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value: command,
                events: Vec::new(),
            });
        }
        if !core::command_transition_allowed(record.state, state) {
            return Err(Error::new(
                "bad_state",
                "command state transition is not allowed for ordinary settlement",
            ));
        }
        if start_not_spawned {
            let agent_num = record
                .target
                .ok_or_else(|| Error::new("bad_state", "unspawned start has no Agent"))?;
            let generation = record
                .command
                .generation
                .ok_or_else(|| Error::new("bad_state", "unspawned start has no generation"))?;
            let agent = load_agent_by_num(&tx, agent_num)?
                .ok_or_else(|| Error::new("not_found", "unspawned Agent was not found"))?;
            let has_processes: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM processes WHERE owner=?1)",
                    [format!("agent:{}#{generation}", agent.id)],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if state != CommandState::Failed
                || record.state != CommandState::Dispatched
                || !is_start_verb(record.command.verb.as_str())
                || agent.generation != generation
                || agent.state != AgentState::Starting
                || has_processes
            {
                return Err(Error::new(
                    "bad_state",
                    "unspawned start proof does not match the current starting generation",
                ));
            }
            tx.execute(
                "UPDATE source_state SET state='closed', health='error', updated_at=?1
                WHERE agent=?2 AND generation=?3 AND source NOT IN ('git','workspace_process')",
                params![now, agent_num, generation],
            )
            .map_err(sql_error)?;
            // This generation never owned a Job. In particular a conflicting
            // existing name must not authorize a later force-stop of its owner.
            tx.execute(
                "UPDATE agents SET custody_namespace=NULL WHERE id=?1 AND generation=?2",
                params![agent_num, generation],
            )
            .map_err(sql_error)?;
            capacity::release_unstarted_tx(&tx, agent_num, generation, id)?;
        }
        if state == CommandState::Applied
            && record.command.verb == "send"
            && record.command.turn_id.is_none()
        {
            return Err(Error::new(
                "bad_state",
                "send is applied only after exact turn association is committed",
            ));
        }
        if state == CommandState::Applied && is_turn_control(&record.command.verb) {
            validate_control_applied_outcome(&record.command, id, outcome.as_ref())?;
        }

        let target = record.target;
        let generation = record.command.generation;
        let initial_launch_never_dispatched = if record.command.verb == "launch"
            && generation == Some(0)
            && matches!(record.state, CommandState::Accepted | CommandState::Queued)
            && let Some(agent_num) = target
        {
            load_agent_by_num(&tx, agent_num)?
                .is_some_and(|agent| agent.generation == 0 && agent.state == AgentState::Created)
        } else {
            false
        };
        update_command_state(&tx, id, state, outcome.as_ref(), None, now)?;
        let mut events = Vec::new();
        let state_name = command_state_name(state);
        events.push(insert_daemon_command_event(
            &tx,
            now,
            &format!("command.{state_name}"),
            target,
            generation,
            id,
            json!({"command_id": id, "state": state_name, "outcome": outcome}),
        )?);
        if state == CommandState::Applied
            && record.command.is_owner_interrupt()
            && outcome
                .as_ref()
                .is_some_and(|value| value["reason"] == "interrupted_by_owner")
            && let (Some(agent), Some(generation), Some(turn_id)) =
                (target, generation, record.command.turn_id.as_deref())
        {
            settle_owner_interrupted_turn_tx(
                &tx,
                (agent, generation, turn_id),
                id,
                "interrupted_by_owner",
                now,
                &mut events,
            )?;
        }

        if record.command.verb == "send" && state != CommandState::Applied {
            context::settle_send_claims_tx(&tx, &record, state, now, &mut events)?;
        }

        if is_attachment_verb(record.command.verb.as_str()) {
            observer::settle_failure(&tx, &record.command, now, &mut events)?;
        }

        release_settle_capacity(
            &tx,
            &record,
            state,
            id,
            initial_launch_never_dispatched,
            now,
            &mut events,
        )?;

        apply_settle_agent_transition(
            &tx,
            &record,
            state,
            id,
            initial_launch_never_dispatched,
            now,
            &mut events,
        )?;

        record = load_command_record(&tx, id)?
            .ok_or_else(|| Error::new("internal", "settled command disappeared"))?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: record.command,
            events,
        })
    }

    /// Commit a notice for an ACL repair already performed by the daemon.
    /// The caller publishes the event while holding the writer guard.
    pub fn commit_acl_repair_notice(
        &mut self,
        notice: &core::api::NotifyDefinitionArgs,
        now: i64,
    ) -> Result<Event> {
        notice.validate()?;
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let event = insert_daemon_event(
                &tx,
                now,
                "x.user.notify",
                "daemon",
                None,
                None,
                json!({"title": notice.title, "message": notice.message}),
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(event)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Commit one daemon-observed diagnostic event (`activity.heavy`,
    /// `session.growth`, `harness.drift`) with optional Agent/generation
    /// binding, in a single transaction. Kinds are closed: anything else
    /// rejects as invalid so no new surface can smuggle one in. A bound
    /// generation must equal the Agent's current generation; drifted evidence
    /// rejects with `stale_generation` instead of committing under a dead
    /// binding. Targetless drift carries neither agent nor generation.
    /// Returns the committed event for bus publication.
    pub fn commit_observe_event(
        &mut self,
        agent: Option<&str>,
        generation: Option<i64>,
        kind: &str,
        data: Value,
        now: i64,
    ) -> Result<Event> {
        if !matches!(
            kind,
            core::ACTIVITY_HEAVY_KIND
                | core::SESSION_GROWTH_KIND
                | core::HARNESS_DRIFT_KIND
                | "agent.restart_resume_blocked"
        ) {
            return Err(Error::new("invalid", "unknown observe event kind"));
        }
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let (agent_num, generation) = match (agent, generation) {
                (Some(id), Some(bound)) => {
                    let num = core::parse_agent_id(id)?;
                    let current = load_agent_by_num(&tx, num)?.ok_or_else(|| {
                        Error::new("not_found", "observe event Agent was not found")
                    })?;
                    if current.generation != bound {
                        return Err(Error::new(
                            "stale_generation",
                            "observe event generation is no longer current",
                        ));
                    }
                    (Some(num), Some(bound))
                }
                (None, None) => (None, None),
                _ => {
                    return Err(Error::new(
                        "invalid",
                        "observe event needs both agent and generation or neither",
                    ));
                }
            };
            let event =
                insert_daemon_event(&tx, now, kind, "observe", agent_num, generation, data)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(event)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Commit one local `notify`/`emit` user event together with its Applied
    /// settlement in a single transaction: validate the Dispatched command
    /// and its verb/kind, insert the bounded `x.user.*` event, record
    /// Applied with the exact kind/sequence and the normal `command.applied`
    /// fact. Returns the settled command with the committed batch (the user
    /// event first) for in-order publication while still serialized with the
    /// writer. An already Applied command with the same kind returns its
    /// recorded result without a new event; any other non-Dispatched state
    /// (including legacy Unknown rows) is rejected and left untouched.
    pub fn commit_user_event_applied(
        &mut self,
        command_id: &str,
        kind: &str,
        data: Value,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("not_found", "user-event command was not found"))?;
            if !is_user_event_verb(record.command.verb.as_str()) {
                return Err(Error::new("invalid", "not a local user-event command"));
            }
            if record.target.is_some() || record.command.generation.is_some() {
                return Err(Error::new(
                    "invalid",
                    "user-event command must be targetless",
                ));
            }
            let expected_kind = match record.command.verb.as_str() {
                "notify" => {
                    serde_json::from_value::<core::api::NotifyDefinitionArgs>(
                        record.command.args.clone(),
                    )
                    .map_err(|_| Error::new("invalid", "notify definition is malformed"))?;
                    "x.user.notify".to_owned()
                }
                _ => {
                    let definition = serde_json::from_value::<core::api::EmitDefinitionArgs>(
                        record.command.args.clone(),
                    )
                    .map_err(|_| Error::new("invalid", "emit definition is malformed"))?;
                    definition.validate()?;
                    format!("x.user.{}", definition.topic)
                }
            };
            if kind != expected_kind {
                return Err(Error::new(
                    "invalid",
                    "user-event kind does not match its command",
                ));
            }
            if applied_user_event_replay(&record, kind) {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: record.command,
                    events: Vec::new(),
                });
            }
            if record.state != CommandState::Dispatched {
                return Err(Error::new(
                    "bad_state",
                    "user-event command is not dispatched",
                ));
            }
            let mut events = Vec::new();
            events.push(insert_daemon_event_for_command(
                &tx,
                command_id,
                DaemonEventInput::new(now, kind, "user", None, None, data),
            )?);
            let outcome = json!({"kind": kind, "seq": events[0].seq});
            settle_applied_tx(&tx, &record, command_id, outcome, now, &mut events)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("internal", "settled command disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: record.command,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Commit one internal attention `x.user.agent.attention` event with its
    /// exact Agent/generation binding and event-time subject, together with
    /// its Applied settlement in a single transaction. The bound report
    /// carries `agent_id`/`generation` at `data.data`; the persisted subject
    /// snapshot is captured from the current generation in the same
    /// transaction. Stale evidence (unknown Agent or generation drift)
    /// rejects with `stale_generation` and commits nothing, so a superseded
    /// capture can never acquire an authoritative binding. Reuses the
    /// `commit_user_event_applied` settlement shape: idempotent Applied
    /// retries return the recorded result without a new event, and failures
    /// leave the command for the existing Unknown-conservative path.
    pub fn commit_attention_event_applied(
        &mut self,
        command_id: &str,
        kind: &str,
        data: Value,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            if kind != "x.user.agent.attention" {
                return Err(Error::new("invalid", "not an attention user-event command"));
            }
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("not_found", "user-event command was not found"))?;
            // Only the internal attention producer admits the `attention.emit`
            // verb, so a public `emit` can never reach this bound commit even
            // when its topic names the reserved attention topic.
            if record.command.verb != "attention.emit" {
                return Err(Error::new(
                    "invalid",
                    "not an internal attention user-event command",
                ));
            }
            if record.target.is_some() || record.command.generation.is_some() {
                return Err(Error::new(
                    "invalid",
                    "user-event command must be targetless",
                ));
            }
            let definition = serde_json::from_value::<core::api::EmitDefinitionArgs>(
                record.command.args.clone(),
            )
            .map_err(|_| Error::new("invalid", "emit definition is malformed"))?;
            definition.validate()?;
            if format!("x.user.{}", definition.topic) != kind {
                return Err(Error::new(
                    "invalid",
                    "user-event kind does not match its command",
                ));
            }
            if definition.topic != "agent.attention" {
                return Err(Error::new("invalid", "not an attention user-event command"));
            }
            if applied_user_event_replay(&record, kind) {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: record.command,
                    events: Vec::new(),
                });
            }
            if record.state != CommandState::Dispatched {
                return Err(Error::new(
                    "bad_state",
                    "user-event command is not dispatched",
                ));
            }
            let report = data.get("data").ok_or_else(|| {
                Error::new("invalid", "attention event data must carry its report")
            })?;
            let agent_id = report
                .get("agent_id")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::new("invalid", "attention report must carry its agent"))?;
            let generation = report
                .get("generation")
                .and_then(Value::as_i64)
                .ok_or_else(|| {
                    Error::new("invalid", "attention report must carry its generation")
                })?;
            let agent_num = parse_agent_id(agent_id)?;
            let current = load_agent_by_num(&tx, agent_num)?;
            let Some(current) = current else {
                return Err(Error::new(
                    "stale_generation",
                    "attention subject Agent is no longer present",
                ));
            };
            if current.generation != generation
                || (current.state == AgentState::Lost
                    && report["reason"] == "source_unhealthy"
                    && report["phase"] == "incident")
            {
                return Err(Error::new(
                    "stale_generation",
                    "attention evidence targets a stale generation",
                ));
            }
            let turn_id = report
                .get("turn_id")
                .and_then(Value::as_str)
                .filter(|turn| !turn.trim().is_empty())
                .map(str::to_owned);
            let mut data = data;
            if let Some(turn_id) = turn_id {
                data["turn_id"] = json!(turn_id);
            }
            let mut events = Vec::new();
            events.push(insert_daemon_event_for_command(
                &tx,
                command_id,
                DaemonEventInput::new(now, kind, "user", Some(agent_num), Some(generation), data),
            )?);
            let outcome = json!({"kind": kind, "seq": events[0].seq});
            settle_applied_tx(&tx, &record, command_id, outcome, now, &mut events)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("internal", "settled command disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: record.command,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Commit one rule `mail.send` child together with its Applied settlement
    /// in a single transaction: validate the Dispatched child and its frozen
    /// definition, perform the queued-mail effect, record Applied with the
    /// delivery readback and the normal `command.applied` fact. Returns the
    /// settled command with the committed batch for in-order publication while
    /// still serialized with the writer. An already Applied child returns its
    /// recorded result without a new delivery; any other non-Dispatched state
    /// (including legacy Unknown rows) is rejected and left untouched. Never
    /// touches the public write-id namespace: deliveries use a rule-prefixed
    /// ULID dated by admission and deduplicated by the child's own Applied state.
    pub fn commit_mail_send_applied(
        &mut self,
        command_id: &str,
        limits: core::api::MailLimits,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("not_found", "mail command was not found"))?;
            if record.command.verb != "mail.send" {
                return Err(Error::new("invalid", "not a rule mail command"));
            }
            if record.target.is_some() || record.command.generation.is_some() {
                return Err(Error::new("invalid", "mail command must be targetless"));
            }
            let definition = serde_json::from_value::<core::api::MailDefinitionArgs>(
                record.command.args.clone(),
            )
            .map_err(|_| Error::new("invalid", "mail definition is malformed"))?;
            definition.validate()?;
            // Freeze recipient/project scope under the saved originating
            // principal inside this transaction.
            Self::validate_rule_mail_definition_tx(&tx, &record.command.principal, &definition)?;
            if record.state == CommandState::Applied {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: record.command,
                    events: Vec::new(),
                });
            }
            if record.state != CommandState::Dispatched {
                return Err(Error::new("bad_state", "mail command is not dispatched"));
            }
            projects::validate_project_command_dispatch_tx(&tx, &record)?;
            let sender = super::coordination::Sender::Owner;
            let delivery_prefix = rule_mail_delivery_prefix(&record.command)?;
            let (response, mail_events) = Self::mail_effect_tx(
                &tx,
                &sender,
                &definition,
                limits,
                &delivery_prefix,
                Some(command_id),
                now,
            )?;
            if response.deliveries.len() != definition.to.len() {
                return Err(Error::new("internal", "mail effect did not fan out"));
            }
            let outcome = json!({"deliveries": response.deliveries});
            // Bodyless delivery facts lead the settlement batch in row order,
            // so recipient long-polls wake on rule mail too.
            let mut events = mail_events;
            settle_applied_tx(&tx, &record, command_id, outcome, now, &mut events)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("internal", "settled command disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: record.command,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    /// Commit one rule `var.set` child together with its Applied settlement
    /// in a single transaction: validate the Dispatched child and its frozen
    /// definition, perform the versioned variable effect with
    /// compare-and-swap semantics, record Applied with the write readback and
    /// the normal `command.applied` fact. Returns the settled command with
    /// the committed batch (`var.changed` first) for in-order publication
    /// while still serialized with the writer. An already Applied child
    /// returns its recorded result without a new write; any other
    /// non-Dispatched state is rejected and left untouched.
    pub fn commit_var_set_applied(
        &mut self,
        command_id: &str,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("not_found", "variable command was not found"))?;
            if record.command.verb != "var.set" {
                return Err(Error::new("invalid", "not a rule variable command"));
            }
            if record.target.is_some() || record.command.generation.is_some() {
                return Err(Error::new("invalid", "variable command must be targetless"));
            }
            let definition =
                serde_json::from_value::<core::api::VarDefinitionArgs>(record.command.args.clone())
                    .map_err(|_| Error::new("invalid", "variable definition is malformed"))?;
            definition.validate()?;
            let encoded_scope = Self::resolve_rule_var_scope(
                &record.command.principal,
                definition.scope.as_deref(),
            )?;
            if record.state == CommandState::Applied {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: record.command,
                    events: Vec::new(),
                });
            }
            if record.state != CommandState::Dispatched {
                return Err(Error::new(
                    "bad_state",
                    "variable command is not dispatched",
                ));
            }
            projects::validate_project_command_dispatch_tx(&tx, &record)?;
            let encoded_value = encode_json(&definition.value)?;
            if encoded_value.len() > core::api::MAX_VAR_VALUE_BYTES {
                return Err(Error::new(
                    "invalid",
                    "variable value exceeds the 16 KiB bound",
                ));
            }
            let (response, event) = Self::var_effect_tx(
                &tx,
                &record.command.principal,
                &encoded_scope,
                &definition,
                &encoded_value,
                Some(command_id),
                now,
            )?;
            let outcome = serde_json::to_value(&response)
                .map_err(|_| Error::new("internal", "variable outcome cannot be encoded"))?;
            let mut events = vec![event];
            settle_applied_tx(&tx, &record, command_id, outcome, now, &mut events)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("internal", "settled command disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: record.command,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

fn rule_mail_delivery_prefix(command: &Command) -> Result<String> {
    let admitted_at = u64::try_from(command.created_at)
        .ok()
        .filter(|timestamp| *timestamp < (1_u64 << 48))
        .ok_or_else(|| Error::new("source_integrity", "mail admission timestamp is invalid"))?;
    let digest = canonical::digest(&json!({
        "domain": "rule-mail-delivery-v1",
        "command_id": command.id,
    }))?;
    let mut entropy = [0_u8; 16];
    entropy[6..].copy_from_slice(&digest[..10]);
    let id = Ulid::from_parts(admitted_at, u128::from_be_bytes(entropy));
    Ok(format!("rule:{id}"))
}

impl Store {
    /// Apply a dispatched or already-unknown stop after the Store has
    /// committed independent process-custody exit and native-source drain
    /// proof for its exact generation. This is runtime evidence settlement,
    /// not owner assertion or native-effect replay.
    pub fn settle_stop_after_proven_exit(
        &mut self,
        id: &str,
        mut outcome: Value,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let record = load_command_record(&tx, id)?
                .ok_or_else(|| Error::new("not_found", "stop command was not found"))?;
            if record.command.verb != "stop" {
                return Err(Error::new(
                    "bad_state",
                    "process/source proof settlement only applies to stop",
                ));
            }
            if let Some(command) = check_settle_idempotent(&record, CommandState::Applied, None)? {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: command,
                    events: Vec::new(),
                });
            }
            if !matches!(
                record.state,
                CommandState::Dispatched | CommandState::Unknown
            ) {
                return Err(Error::new(
                    "bad_state",
                    "only a dispatched or unknown stop can use process/source proof settlement",
                ));
            }
            let (Some(agent_num), Some(generation)) = (record.target, record.command.generation)
            else {
                return Err(Error::new(
                    "bad_state",
                    "stop has no exact generation binding",
                ));
            };
            let proof = capacity::owned_exit_proof_tx(&tx, agent_num, generation)?;
            if !proof.is_complete() {
                return Err(Error::new(
                    "unknown",
                    format!(
                        "stop cannot apply: {}",
                        proof
                            .missing_reason()
                            .unwrap_or("process/source proof is incomplete"),
                    ),
                ));
            }
            let native_ended: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM events WHERE agent=?1 AND generation=?2
                    AND kind='session.ended' AND source='stream')",
                    params![agent_num, generation],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            let fields = outcome.as_object_mut().ok_or_else(|| {
                Error::new("source_integrity", "applied stop outcome is not an object")
            })?;
            fields.insert("process_custody_exit_proven".into(), json!(true));
            fields.insert("native_source_drain_proven".into(), json!(true));
            fields.insert("native_session_end_observed".into(), json!(native_ended));
            let interrupt_id: Option<String> = tx
                .query_row(
                    "SELECT parent_command FROM commands WHERE id = ?1 AND link_kind = 'interrupt_stop'",
                    [id],
                    |row| row.get(0),
                )
                .optional()
                .map_err(sql_error)?;
            if let Some(interrupt_id) = interrupt_id.as_deref() {
                fields.insert("reason".into(), json!("interrupt_unconfirmed"));
                fields.insert("interrupt_command_id".into(), json!(interrupt_id));
            }
            if native_ended {
                fields.remove("native_stop_reason");
            } else {
                fields.insert(
                    "native_stop_reason".into(),
                    json!("The native session terminal event was not observed."),
                );
            }
            validate_settle_outcome(CommandState::Applied, &Some(outcome.clone()))?;
            check_settle_verb_guards(
                &tx,
                &record,
                CommandState::Applied,
                &Some(outcome.clone()),
                id,
            )?;
            if record.state == CommandState::Dispatched
                && !core::command_transition_allowed(record.state, CommandState::Applied)
            {
                return Err(Error::new(
                    "bad_state",
                    "dispatched stop cannot transition to Applied",
                ));
            }
            update_command_state(&tx, id, CommandState::Applied, Some(&outcome), None, now)?;
            let mut events = vec![insert_daemon_command_event(
                &tx,
                now,
                "command.applied",
                Some(agent_num),
                Some(generation),
                id,
                json!({"command_id": id, "state": "applied", "outcome": outcome}),
            )?];
            if let Some(interrupt_id) = interrupt_id.as_deref() {
                settle_interrupt_after_fallback_tx(
                    &tx,
                    interrupt_id,
                    id,
                    agent_num,
                    generation,
                    now,
                    &mut events,
                )?;
            }
            release_settle_capacity(
                &tx,
                &record,
                CommandState::Applied,
                id,
                false,
                now,
                &mut events,
            )?;
            apply_settle_agent_transition(
                &tx,
                &record,
                CommandState::Applied,
                id,
                false,
                now,
                &mut events,
            )?;
            let command = load_command(&tx, id)?
                .ok_or_else(|| Error::new("source_integrity", "settled stop disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: command,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

/// Applied settlement tail shared by the targetless rule commits: validate
/// the outcome and verb guards, move the command to Applied, append its
/// `command.applied` fact, then release capacity and apply the Agent
/// transition. The caller owns the transaction and has already appended any
/// leading events into `events`.
fn settle_applied_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    command_id: &str,
    outcome: Value,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let outcome = Some(outcome);
    validate_settle_outcome(CommandState::Applied, &outcome)?;
    check_settle_verb_guards(tx, record, CommandState::Applied, &outcome, command_id)?;
    if !core::command_transition_allowed(record.state, CommandState::Applied) {
        return Err(Error::new(
            "bad_state",
            "command state transition is not allowed for ordinary settlement",
        ));
    }
    update_command_state(
        tx,
        command_id,
        CommandState::Applied,
        outcome.as_ref(),
        None,
        now,
    )?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.applied",
        record.target,
        record.command.generation,
        command_id,
        json!({"command_id": command_id, "state": "applied", "outcome": outcome}),
    )?);
    release_settle_capacity(
        tx,
        record,
        CommandState::Applied,
        command_id,
        false,
        now,
        events,
    )?;
    apply_settle_agent_transition(
        tx,
        record,
        CommandState::Applied,
        command_id,
        false,
        now,
        events,
    )
}

fn validate_settle_outcome(state: CommandState, outcome: &Option<Value>) -> Result<()> {
    if !matches!(
        state,
        CommandState::Applied
            | CommandState::Rejected
            | CommandState::Failed
            | CommandState::Unknown
    ) {
        return Err(Error::new(
            "invalid",
            "settlement state is not a result state",
        ));
    }
    if let Some(outcome) = outcome
        && encode_json(outcome)?.len() > MAX_EVENT_BYTES
    {
        return Err(Error::new(
            "frame_too_large",
            "command outcome exceeds 64 KiB",
        ));
    }
    Ok(())
}

fn check_settle_verb_guards(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    outcome: &Option<Value>,
    id: &str,
) -> Result<()> {
    if record.command.verb == "git.integrate"
        && matches!(
            state,
            CommandState::Applied | CommandState::Failed | CommandState::Unknown
        )
    {
        let frozen = projects::decode_frozen_git_integrate(&record.command.args)?;
        let value = outcome.as_ref().ok_or_else(|| {
            Error::new(
                "invalid",
                "git.integrate settlement requires its typed result",
            )
        })?;
        let result = serde_json::from_value::<core::api::GitIntegrateOutcome>(value.clone())
            .map_err(|_| Error::new("invalid", "git.integrate result is malformed"))?;
        result.validate_for(&frozen)?;
        if state == CommandState::Applied
            && (result.status != core::api::GitIntegrateStatus::Merged
                || result.integration_oid.is_none())
        {
            return Err(Error::new(
                "invalid",
                "applied git.integrate requires a merged integration OID",
            ));
        }
    }
    if record.command.verb == "git.fetch" && state == CommandState::Applied {
        let frozen = projects::decode_frozen_git_fetch(&record.command.args)?;
        let value = outcome.as_ref().ok_or_else(|| {
            Error::new(
                "invalid",
                "applied git.fetch requires verified ref readback",
            )
        })?;
        let outcome = serde_json::from_value::<core::api::GitFetchOutcome>(value.clone())
            .map_err(|_| Error::new("invalid", "applied git.fetch readback is malformed"))?;
        outcome.validate_for(&frozen)?;
    }
    if is_attachment_verb(record.command.verb.as_str()) && state == CommandState::Applied {
        return Err(Error::new(
            "not_allowed",
            "observer commands require exact observer settlement",
        ));
    }
    if record.command.verb == "worktree.remove" {
        return Err(Error::new(
            "bad_state",
            "use typed workspace removal settlement",
        ));
    }
    if record.target.is_some() && record.command.verb == "inject" && state == CommandState::Applied
    {
        let args = context::decode_inject_args(&record.command.args)?;
        if args.point == InjectPoint::ActiveTurn {
            context::validate_active_turn_applied_outcome(record, outcome.as_ref())?;
        }
    }
    if let Some(agent_num) = record.target
        && let Some(workspace) = workspace::load_workspace(tx, agent_num)?
        && ((workspace.start_command_id == id
            && record
                .command
                .generation
                .is_some_and(|generation| generation < workspace.target_generation)
            && matches!(
                record.state,
                CommandState::Accepted | CommandState::Queued | CommandState::Unknown
            )
            && !(record.command.verb == "launch"
                && state == CommandState::Rejected
                && matches!(record.state, CommandState::Accepted | CommandState::Queued)
                && !workspace::workspace_cancel_requested(&workspace)
                && (matches!(
                    workspace.phase,
                    WorkspacePhase::SetupReady | WorkspacePhase::Ready
                ) || (workspace.phase == WorkspacePhase::Creating
                    && workspace
                        .outcome
                        .as_ref()
                        .and_then(|value| value.get("native_git_write_dispatched"))
                        .and_then(Value::as_bool)
                        == Some(false)))))
            || (record.command.verb == "stop"
                && workspace
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.get("stop_command_id"))
                    .and_then(Value::as_str)
                    == Some(id)))
    {
        return Err(Error::new(
            "bad_state",
            "use workspace-bound preparation/cancellation settlement",
        ));
    }
    Ok(())
}

fn check_settle_idempotent(
    record: &CommandRecord,
    state: CommandState,
    outcome: Option<&Value>,
) -> Result<Option<Command>> {
    if matches!(
        record.state,
        CommandState::Applied
            | CommandState::Rejected
            | CommandState::Failed
            | CommandState::Cancelled
    ) {
        if record.state == state {
            if state == CommandState::Applied && record.command.verb == "send" {
                if outcome
                    .and_then(|value| value.get("turn_id"))
                    .and_then(Value::as_str)
                    .is_some_and(|turn_id| record.command.turn_id.as_deref() != Some(turn_id))
                {
                    return Err(Error::new(
                        "source_integrity",
                        "settlement turn id conflicts with the committed send association",
                    ));
                }
                return Ok(Some(record.command.clone()));
            }
            if outcome.is_none() || record.command.outcome.as_ref() == outcome {
                return Ok(Some(record.command.clone()));
            }
        }
        if matches!(
            record.state,
            CommandState::Applied
                | CommandState::Rejected
                | CommandState::Failed
                | CommandState::Cancelled
        ) {
            return Err(Error::new(
                "bad_state",
                format!(
                    "command {} already has terminal state {:?}",
                    record.command.id, record.state
                ),
            ));
        }
    }
    if record.state == CommandState::Unknown
        && state == CommandState::Unknown
        && (outcome.is_none() || record.command.outcome.as_ref() == outcome)
    {
        return Ok(Some(record.command.clone()));
    }
    Ok(None)
}

fn release_settle_capacity(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    id: &str,
    initial_launch_never_dispatched: bool,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let target = record.target;
    let generation = record.command.generation;
    let unstarted_resume_failure = record.command.verb == "resume"
        && matches!(record.state, CommandState::Accepted | CommandState::Queued)
        && matches!(state, CommandState::Rejected | CommandState::Failed);
    if initial_launch_never_dispatched
        && matches!(state, CommandState::Rejected | CommandState::Failed)
    {
        capacity::release_unstarted_tx(
            tx,
            target.ok_or_else(|| Error::new("internal", "launch has no Agent"))?,
            1,
            id,
        )?;
    } else if unstarted_resume_failure {
        cancel_unstarted_resume_input_tx(
            tx,
            target.ok_or_else(|| Error::new("internal", "resume has no Agent"))?,
            id,
            "resume_not_dispatched",
            now,
            events,
            false,
        )?;
    } else if matches!(state, CommandState::Rejected | CommandState::Failed)
        && is_start_verb(record.command.verb.as_str())
    {
        // A dispatched Rejected start never dispatched native I/O, so its
        // slot is released immediately. A dispatched Failed start carries the
        // plugin's authoritative proven-no-effect verdict, but its slot is
        // released here only after the owned-exit proof path has proven the
        // generation's processes exited; without that proof the reservation
        // is kept and the later exit proof (or OS-source closure) releases
        // it. Never release a Failed start on a guess.
        if state == CommandState::Failed
            && !capacity::has_owned_exit_and_reader_drain_tx(
                tx,
                target.ok_or_else(|| Error::new("internal", "start has no Agent"))?,
                generation.ok_or_else(|| Error::new("internal", "start has no generation"))?,
            )?
        {
            return Ok(());
        }
        let agent = target.ok_or_else(|| Error::new("internal", "rejected start has no Agent"))?;
        let command_generation =
            generation.ok_or_else(|| Error::new("internal", "rejected start has no generation"))?;
        let capacity_generation = if record.command.verb == "launch" {
            1
        } else if matches!(record.state, CommandState::Accepted | CommandState::Queued) {
            command_generation
                .checked_add(1)
                .ok_or_else(|| Error::new("bad_state", "capacity generation overflow"))?
        } else {
            command_generation
        };
        capacity::release_unstarted_tx(tx, agent, capacity_generation, id)?;
    }
    Ok(())
}

pub(super) fn apply_settle_agent_transition(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    id: &str,
    initial_launch_never_dispatched: bool,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let target = record.target;
    let generation = record.command.generation;
    if initial_launch_never_dispatched {
        let agent_num = target.ok_or_else(|| Error::new("internal", "launch has no Agent"))?;
        fail_unstarted_launch_tx(tx, agent_num, id, now, events)?;
        if let Some(agent) = load_agent_by_num(tx, agent_num)? {
            project_assignment::clear_failed_preparation(tx, &agent, id)?;
        }
        if let Some(old_workspace) = workspace::load_workspace(tx, agent_num)?
            && old_workspace.start_command_id == id
        {
            let mut workspace = old_workspace.clone();
            workspace.phase = WorkspacePhase::Failed;
            workspace.updated_at = now;
            workspace::persist_workspace(tx, &old_workspace, &workspace)?;
            events.push(workspace::workspace_event(tx, &workspace, now)?);
        }
    } else if is_start_verb(record.command.verb.as_str())
        && matches!(state, CommandState::Rejected | CommandState::Failed)
    {
        if let (Some(agent_num), Some(generation)) = (target, generation) {
            let reason = if record.command.verb == "resume" {
                "resume_failed"
            } else {
                "launch_failed"
            };
            fail_start_tx(tx, agent_num, generation, id, reason, now, events)?;
        }
    } else if record.command.verb == "stop"
        && state == CommandState::Applied
        && let (Some(agent_num), Some(generation)) = (target, generation)
    {
        let agent = load_agent_by_num(tx, agent_num)?
            .ok_or_else(|| Error::new("not_found", "stop target Agent was not found"))?;
        if agent.state == AgentState::Lost {
            settle_lost_stop_after_owned_exit_inner(tx, id, now, events, true)?;
        } else {
            stop_agent_tx(tx, agent_num, generation, id, now, events, true)?;
        }
    }
    Ok(())
}

fn restore_resume_workspace_marker_tx(
    tx: &Transaction<'_>,
    current: &WorkspacePreparation,
    command_id: &str,
    agent_num: i64,
    old_generation: i64,
    now: i64,
    allow_workspace_cancel: bool,
) -> Result<WorkspacePreparation> {
    let next_generation = old_generation
        .checked_add(1)
        .ok_or_else(|| Error::new("source_integrity", "resume workspace generation overflow"))?;
    let current_outcome = current.outcome.as_ref().ok_or_else(|| {
        Error::new(
            "source_integrity",
            "resume workspace marker has no prior Ready state",
        )
    })?;
    let current_previous = current_outcome
        .get("resume_previous")
        .ok_or_else(|| Error::new("source_integrity", "resume workspace lost its prior marker"))?;
    let current_reuse = current_outcome.get("reuse").and_then(Value::as_bool) == Some(true);
    let unknown_no_write_cancel = allow_workspace_cancel
        && current.phase == WorkspacePhase::Unknown
        && current_outcome.get("stage").and_then(Value::as_str) == Some("creation")
        && current_outcome
            .get("native_git_write_dispatched")
            .and_then(Value::as_bool)
            == Some(false)
        && workspace::workspace_cancel_requested(current);
    if current.start_command_id != command_id
        || current.target_generation != next_generation
        || !(matches!(
            current.phase,
            WorkspacePhase::Creating | WorkspacePhase::Ready
        ) || unknown_no_write_cancel)
        || current.setup_index != current.frozen_setup.len()
        || !current_reuse
        || workspace::workspace_cancel_requested(current) && !allow_workspace_cancel
    {
        return Err(Error::new(
            "source_integrity",
            "resume withdrawal is not bound to its exact retained workspace marker",
        ));
    }
    let previous_start_id = current_previous
        .get("start_command_id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("source_integrity", "resume prior marker has no start id"))?;
    if current_previous
        .get("target_generation")
        .and_then(Value::as_i64)
        != Some(old_generation)
    {
        return Err(Error::new(
            "source_integrity",
            "resume prior marker belongs to another generation",
        ));
    }
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "resume workspace Agent was not found"))?;
    if parse_agent_id(&current.agent)? != agent_num
        || agent.generation != old_generation
        || !matches!(
            agent.state,
            AgentState::Stopped | AgentState::Failed | AgentState::Lost
        )
        || agent.active_turn.is_some()
        || agent.workspace.as_deref() != Some(current.worktree_path.as_str())
        || agent
            .native_scope
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        || agent
            .native_ref
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(Error::new(
            "source_integrity",
            "resume withdrawal cannot restore a changed old Agent or workspace",
        ));
    }
    let previous_start = load_command_record(tx, previous_start_id)?
        .ok_or_else(|| Error::new("source_integrity", "resume prior start command is missing"))?;
    if previous_start.target != Some(agent_num)
        || !is_start_verb(previous_start.command.verb.as_str())
        || previous_start.command.generation != Some(old_generation)
        || !matches!(
            previous_start.state,
            CommandState::Applied | CommandState::Dispatched | CommandState::Unknown
        )
        || previous_start
            .command
            .expected_generation
            .is_some_and(|expected| expected.checked_add(1) != Some(old_generation))
    {
        return Err(Error::new(
            "source_integrity",
            "resume prior start command does not own the retained workspace generation",
        ));
    }
    let previous_outcome = current_previous
        .get("outcome")
        .cloned()
        .filter(|outcome| !outcome.is_null());
    let mut restored = current.clone();
    restored.start_command_id = previous_start_id.to_owned();
    restored.target_generation = old_generation;
    restored.phase = WorkspacePhase::Ready;
    restored.outcome = previous_outcome;
    restored.updated_at = now;
    Ok(restored)
}

pub(super) fn cancel_unstarted_resume_input_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    command_id: &str,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
    allow_workspace_cancel: bool,
) -> Result<Option<WorkspacePreparation>> {
    let resume = load_command_record(tx, command_id)?
        .ok_or_else(|| Error::new("source_integrity", "resume command row disappeared"))?;
    let old_generation = resume
        .command
        .expected_generation
        .or(resume.command.generation)
        .ok_or_else(|| Error::new("source_integrity", "resume has no retained old generation"))?;
    if resume.target != Some(agent_num)
        || resume.command.verb != "resume"
        || resume.command.generation != Some(old_generation)
        || resume
            .command
            .expected_generation
            .is_some_and(|expected| expected != old_generation)
        || !matches!(
            resume.state,
            CommandState::Rejected | CommandState::Failed | CommandState::Cancelled
        )
    {
        return Err(Error::new(
            "source_integrity",
            "resume cleanup is not bound to its exact undispatched old generation",
        ));
    }
    let agent = load_agent_by_num(tx, agent_num)?
        .ok_or_else(|| Error::new("not_found", "resume target Agent was not found"))?;
    if agent.generation != old_generation
        || !matches!(
            agent.state,
            AgentState::Stopped | AgentState::Failed | AgentState::Lost
        )
        || agent.active_turn.is_some()
        || agent
            .native_scope
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
        || agent
            .native_ref
            .as_deref()
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(Error::new(
            "source_integrity",
            "resume cleanup cannot alter a changed or nonterminal old Agent",
        ));
    }

    let next_generation = old_generation
        .checked_add(1)
        .ok_or_else(|| Error::new("source_integrity", "resume capacity generation overflow"))?;
    let workspace_restore = match workspace::load_workspace(tx, agent_num)? {
        Some(current) => {
            let restored = restore_resume_workspace_marker_tx(
                tx,
                &current,
                command_id,
                agent_num,
                old_generation,
                now,
                allow_workspace_cancel,
            )?;
            Some((current, restored))
        }
        None => None,
    };
    cancel_resume_linked_input_tx(
        tx,
        resume.command,
        agent_num,
        next_generation,
        reason,
        now,
        events,
    )?;
    capacity::release_unstarted_tx(tx, agent_num, next_generation, command_id).and_then(|()| {
        if let Some((current, restored)) = workspace_restore {
            workspace::persist_workspace(tx, &current, &restored)?;
            events.push(workspace::workspace_event(tx, &restored, now)?);
            Ok(Some(restored))
        } else {
            Ok(None)
        }
    })
}

fn cancel_resume_linked_input_tx(
    tx: &Transaction<'_>,
    command: Command,
    agent_num: i64,
    next_generation: i64,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let retained = rules::retained_action_admission(tx, command)?;
    if let Some(input) = retained.linked_command {
        let input_target = input
            .target
            .as_deref()
            .map(parse_agent_id)
            .transpose()?
            .ok_or_else(|| Error::new("source_integrity", "resume input has no Agent target"))?;
        let input_generation = input
            .generation
            .or(input.expected_generation)
            .ok_or_else(|| Error::new("source_integrity", "resume input has no next generation"))?;
        if input_target != agent_num || input_generation != next_generation {
            return Err(Error::new(
                "source_integrity",
                "resume input no longer targets its exact next generation",
            ));
        }
        match input.state {
            CommandState::Accepted | CommandState::Queued => cancel_pending_command_tx(
                tx,
                input_target,
                input_generation,
                &input.id,
                reason,
                now,
                events,
            )?,
            CommandState::Rejected | CommandState::Cancelled => {}
            CommandState::Dispatched
            | CommandState::Unknown
            | CommandState::Applied
            | CommandState::Failed => {
                return Err(Error::new(
                    "bad_state",
                    "resume input has an external outcome; its reservation remains held",
                ));
            }
        }
    }
    Ok(())
}

fn is_turn_control(verb: &str) -> bool {
    matches!(verb, "steer" | "interrupt" | "approve" | "deny" | "answer")
}

fn validate_control_applied_outcome(
    command: &Command,
    command_id: &str,
    outcome: Option<&Value>,
) -> Result<()> {
    let Some(outcome) = outcome else {
        return Err(Error::new(
            "source_integrity",
            "control command cannot be applied without its exact native result",
        ));
    };
    let reported_command = outcome.get("command_id").and_then(Value::as_str);
    let reported_turn = outcome.get("turn_id").and_then(Value::as_str);
    let reported_request = outcome.get("request_id").and_then(Value::as_str);
    if reported_command != Some(command_id)
        || (!command.is_owner_interrupt() && command.turn_id.is_none())
        || reported_turn != command.turn_id.as_deref()
        || reported_request != command.request_id.as_deref()
    {
        return Err(Error::new(
            "source_integrity",
            "control result does not match its exact command, turn, and request binding",
        ));
    }
    Ok(())
}

fn settle_interrupt_after_fallback_tx(
    tx: &Transaction<'_>,
    interrupt_id: &str,
    stop_id: &str,
    agent: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let interrupt = load_command_record(tx, interrupt_id)?
        .ok_or_else(|| Error::new("source_integrity", "stop's linked interrupt disappeared"))?;
    if !interrupt.command.is_owner_interrupt()
        || interrupt.target != Some(agent)
        || interrupt.command.generation != Some(generation)
    {
        return Err(Error::new(
            "source_integrity",
            "stop's linked interrupt binding changed",
        ));
    }
    if interrupt.state.is_terminal() {
        return Ok(());
    }
    let outcome = json!({
        "command_id": interrupt_id,
        "turn_id": interrupt.command.turn_id,
        "reason": "interrupt_unconfirmed_agent_stopped",
        "message": "Harness did not confirm the interrupt within 10 seconds. Swarm stopped the Agent.",
        "fallback_stop_id": stop_id,
    });
    update_command_state(
        tx,
        interrupt_id,
        CommandState::Applied,
        Some(&outcome),
        None,
        now,
    )?;
    events.push(insert_daemon_command_event(
        tx,
        now,
        "command.applied",
        Some(agent),
        Some(generation),
        interrupt_id,
        json!({"command_id": interrupt_id, "state": "applied", "outcome": outcome}),
    )?);
    Ok(())
}

pub(super) fn stop_exited_idle_on_restart_tx(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<bool> {
    // Restart marks prior running process rows unknown before reaching here.
    // Only the prior Host and monitor can have committed this proof of exit.
    if !capacity::has_owned_exit_and_reader_drain_tx(tx, agent, generation)? {
        return Ok(false);
    }
    let stopped = db!(tx.execute(
        "UPDATE agents AS a SET state = 'stopped', updated_at = ?1
         WHERE a.id = ?2 AND a.generation = ?3 AND a.state = 'idle' AND a.observe_only = 0
           AND TRIM(a.native_scope) <> '' AND TRIM(a.native_ref) <> ''
           AND a.active_turn IS NULL AND a.pending = '[]'
           AND NOT EXISTS (SELECT 1 FROM turns t WHERE t.agent = a.id
               AND t.generation = a.generation AND t.state IN ('running', 'unknown'))
           AND NOT EXISTS (SELECT 1 FROM pending_requests r WHERE r.agent = a.id
               AND r.generation = a.generation AND r.state = 'pending')
           AND NOT EXISTS (SELECT 1 FROM commands c WHERE c.target = a.id
               AND c.generation = a.generation AND c.state IN ('accepted', 'queued', 'dispatched', 'unknown'))",
        params![now, agent, generation],
    ));
    if stopped == 0 {
        return Ok(false);
    }
    events.push(insert_daemon_event(
        tx,
        now,
        "agent.stopped",
        "agent",
        Some(agent),
        Some(generation),
        json!({"agent_id": format_agent_id(agent), "reason": "owned_exit_before_restart"}),
    )?);
    super::goals::pause_for_restart_tx(tx, agent, generation, "daemon_restart", now, events)?;
    Ok(true)
}

pub(super) fn transition_lost_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    now: i64,
    reason: &str,
    evidence: &super::AgentLossEvidence,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "Agent was not found"));
    };
    observer::require_controlled(&agent)?;
    if agent.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "loss transition targets a stale generation",
        ));
    }
    if agent.is_terminal() {
        return Ok(());
    }

    let dispatched = command_ids_in_state(tx, agent_num, generation, "dispatched")?;
    for command_id in dispatched {
        let prior = load_command_record(tx, &command_id)?.and_then(|record| record.command.outcome);
        let outcome = context::unknown_claim_outcome(prior.as_ref(), reason);
        update_command_state(
            tx,
            &command_id,
            CommandState::Unknown,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.unknown",
            Some(agent_num),
            Some(generation),
            &command_id,
            json!({"command_id": command_id, "reason": reason}),
        )?);
    }
    for command_id in pending_command_ids(tx, agent_num, generation)? {
        if super::handoff::held_successor_tx(tx, &command_id)? {
            continue;
        }
        cancel_pending_command_tx(
            tx,
            agent_num,
            generation,
            &command_id,
            "generation_lost",
            now,
            events,
        )?;
    }

    let failure = eliot_core::swarm_loss_location(reason)
        .map(|(fallback, location)| (evidence.origin.unwrap_or(fallback), location));
    mark_lost_turn_unknown_tx(
        tx, &agent, agent_num, generation, now, reason, failure, events,
    )?;

    db!(tx.execute(
        "UPDATE agents SET state = 'lost', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', health = 'stale', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
        params![now, agent_num, generation],
    ));
    // The typed `where` names the process boundary; the origin is the
    // producer's own classification when it recorded one, else the fallback
    // for this reason. A failed start on a missing external credential keeps
    // its `environment` origin instead of being retyped as Agent software.
    let mut data = json!({"agent_id": agent.id, "reason": reason});
    if matches!(
        reason,
        core::DAEMON_RESTART_REASON | core::PLUGIN_ROOT_EXITED_REASON
    ) {
        data["previous_state"] = json!(agent.state);
    }
    if let Some(turn_id) = &agent.active_turn {
        data["turn_id"] = json!(turn_id);
    }
    if let Some((origin, location)) = failure {
        data["origin"] = json!(origin.as_str());
        data["where"] = json!(location);
    }
    if let Some(exit) = evidence.exit.as_ref().filter(|exit| !exit.is_empty()) {
        if let Some(exit_code) = exit.exit_code {
            data["exit_code"] = json!(exit_code);
        }
        if let Some(ntstatus) = exit.ntstatus.as_deref() {
            data["ntstatus"] = json!(ntstatus);
        }
    }
    if let Some(tail) = evidence
        .stderr_tail
        .as_deref()
        .filter(|tail| !tail.trim().is_empty())
    {
        data["stderr_tail"] = json!(tail);
    }
    if let Some(origin_evidence) = evidence.origin_evidence.as_deref() {
        data["origin_evidence"] = json!(eliot_core::bound_chars(origin_evidence, 512));
    }
    events.push(insert_daemon_event(
        tx,
        now,
        "agent.lost",
        "agent",
        Some(agent_num),
        Some(generation),
        data,
    )?);
    if reason == core::DAEMON_RESTART_REASON {
        super::goals::pause_for_restart_tx(tx, agent_num, generation, reason, now, events)?;
    }
    super::native_children::end_native_children_with_parent_tx(
        tx,
        Some((agent_num, generation)),
        now,
        events,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
fn mark_lost_turn_unknown_tx(
    tx: &Transaction<'_>,
    agent: &Agent,
    agent_num: i64,
    generation: i64,
    now: i64,
    reason: &str,
    failure: Option<(eliot_core::FailureOrigin, &str)>,
    events: &mut Vec<Event>,
) -> Result<()> {
    if let Some(turn_id) = agent.active_turn.as_deref() {
        let exists = load_turn(tx, agent_num, generation, turn_id)?.is_some();
        if exists {
            db!(tx.execute(
                "UPDATE turns SET state = 'unknown', completed_at = ?1
                 WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4",
                params![now, agent_num, generation, turn_id],
            ));
        } else {
            let start_command_id = command_id_for_turn_tx(tx, agent_num, generation, turn_id)?;
            insert_turn(
                tx,
                TurnInsert {
                    agent: agent_num,
                    generation,
                    turn_id,
                    start_command_id: start_command_id.as_deref(),
                    state: TurnState::Unknown,
                    status: None,
                    outcome: None,
                    native_status: None,
                    final_text: None,
                    started_at: None,
                    completed_at: Some(now),
                    usage: None,
                    config_revision: current_config_revision_tx(tx, agent_num, generation)?,
                    preceded_by: None,
                    effective_model: None,
                    native_trigger: None,
                },
            )?;
        }
        let cause = command_cause_for_turn_tx(tx, agent_num, generation, turn_id)?;
        let mut data = json!({"turn_id": turn_id, "reason": reason});
        if let Some((origin, location)) = failure {
            data["origin"] = json!(origin.as_str());
            data["where"] = json!(location);
        }
        let event = DaemonEventInput::new(
            now,
            "turn.unknown",
            "turn",
            Some(agent_num),
            Some(generation),
            data,
        );
        let event = match cause {
            Some(cause) => event.with_cause(cause),
            None => event,
        };
        events.push(insert_daemon_event_input(tx, event)?);
    }
    Ok(())
}

fn fail_start_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "launch target Agent was not found"));
    };
    observer::require_controlled(&agent)?;
    if agent.generation != generation || agent.state != AgentState::Starting {
        return Ok(());
    }
    for pending_id in pending_command_ids(tx, agent_num, generation)? {
        cancel_pending_command_tx(tx, agent_num, generation, &pending_id, reason, now, events)?;
    }
    db!(tx.execute(
        "UPDATE agents SET state = 'failed', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', health = 'error', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
        params![now, agent_num, generation],
    ));
    // The failed command's outcome is the process evidence: a recorded
    // harness exit (exit_code/NTSTATUS) rides from the outcome into the
    // `agent.failed` event. The outcome's own stderr tails stay on the
    // command outcome that `swarm why` already correlates.
    let mut data = json!({"agent_id": agent.id, "command_id": command_id, "reason": reason});
    if let Some(record) = super::load_command_record(tx, command_id)?
        && let Some(outcome) = record.command.outcome
    {
        if let Some(exit_code) = outcome.get("exit_code").and_then(Value::as_i64) {
            data["exit_code"] = json!(exit_code);
        }
        if let Some(ntstatus) = outcome
            .get("ntstatus")
            .and_then(Value::as_str)
            .filter(|ntstatus| !ntstatus.is_empty())
        {
            data["ntstatus"] = json!(ntstatus.chars().take(16).collect::<String>());
        }
    }
    events.push(insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(
            now,
            "agent.failed",
            "agent",
            Some(agent_num),
            Some(generation),
            data,
        ),
    )?);
    Ok(())
}

/// Close a Lost Agent after the owning manager's exact historical stop
/// reconciliation. The caller resolves the stop/related turn in its own
/// transaction first; this helper only performs the Lost -> Stopped lifecycle
/// effect after committed process and source proof.
pub(crate) fn settle_lost_stop_after_owned_exit_tx(
    tx: &Transaction<'_>,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    settle_lost_stop_after_owned_exit_inner(tx, command_id, now, events, false)
}

fn settle_lost_stop_after_owned_exit_inner(
    tx: &Transaction<'_>,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
    interrupt_owner_turn: bool,
) -> Result<()> {
    let stop = load_command_record(tx, command_id)?
        .ok_or_else(|| Error::new("not_found", "proven stop command was not found"))?;
    if stop.command.verb != "stop" || stop.state != CommandState::Applied {
        return Err(Error::new(
            "bad_state",
            "Lost-to-Stopped requires an Applied stop command",
        ));
    }
    let (Some(agent_num), Some(generation)) = (stop.target, stop.command.generation) else {
        return Err(Error::new(
            "bad_state",
            "proven stop has no generation binding",
        ));
    };
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "proven stop Agent was not found"));
    };
    if agent.generation != generation || agent.state != AgentState::Lost {
        return Ok(());
    }
    if !capacity::has_owned_exit_and_reader_drain_tx(tx, agent_num, generation)? {
        return Err(Error::new(
            "unknown",
            "Lost Agent stop is missing committed process custody exit or native source drain proof",
        ));
    }
    stop_agent_tx(
        tx,
        agent_num,
        generation,
        command_id,
        now,
        events,
        interrupt_owner_turn,
    )
}

fn stop_agent_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    now: i64,
    events: &mut Vec<Event>,
    interrupt_owner_turn: bool,
) -> Result<()> {
    let Some(agent) = load_agent_by_num(tx, agent_num)? else {
        return Err(Error::new("not_found", "stop target Agent was not found"));
    };
    observer::require_controlled(&agent)?;
    if agent.generation != generation {
        return Ok(());
    }
    if agent.state == AgentState::Lost
        && !capacity::has_owned_exit_and_reader_drain_tx(tx, agent_num, generation)?
    {
        return Err(Error::new(
            "unknown",
            "Lost Agent cannot become Stopped without committed process custody exit and native source drain proof",
        ));
    }
    let stop = load_command_record(tx, command_id)?
        .ok_or_else(|| Error::new("not_found", "stop command disappeared during settlement"))?;
    let captured_turn = stop
        .expected_turn_id
        .as_deref()
        .or(agent.active_turn.as_deref());
    let owner_stop = interrupt_owner_turn && stop.command.principal == "owner";
    if owner_stop && let Some(turn_id) = captured_turn {
        settle_owner_interrupted_turn_tx(
            tx,
            (agent_num, generation, turn_id),
            command_id,
            "stopped_by_owner",
            now,
            events,
        )?;
    }
    if matches!(agent.state, AgentState::Stopped | AgentState::Failed) {
        return Ok(());
    }
    for dispatched_id in command_ids_in_state(tx, agent_num, generation, "dispatched")? {
        if dispatched_id == command_id {
            continue;
        }
        let prior =
            load_command_record(tx, &dispatched_id)?.and_then(|record| record.command.outcome);
        let outcome = context::unknown_claim_outcome(prior.as_ref(), "generation_stopped");
        update_command_state(
            tx,
            &dispatched_id,
            CommandState::Unknown,
            Some(&outcome),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            tx,
            now,
            "command.unknown",
            Some(agent_num),
            Some(generation),
            &dispatched_id,
            json!({"command_id": dispatched_id, "reason": "generation_stopped"}),
        )?);
    }
    for pending_id in pending_command_ids(tx, agent_num, generation)? {
        if pending_id == command_id {
            continue;
        }
        cancel_pending_command_tx(
            tx,
            agent_num,
            generation,
            &pending_id,
            "generation_stopped",
            now,
            events,
        )?;
    }
    if let Some(turn_id) = agent.active_turn.as_deref()
        && (!owner_stop || captured_turn != Some(turn_id))
    {
        settle_stopped_turn_unknown_tx(
            tx, agent_num, generation, command_id, turn_id, now, events,
        )?;
    }
    db!(tx.execute(
        "UPDATE agents SET state = 'stopped', active_turn = NULL, pending = '[]',
                           token = NULL, updated_at = ?1
         WHERE id = ?2 AND generation = ?3",
        params![now, agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE pending_requests SET state = 'stale'
         WHERE agent = ?1 AND generation = ?2 AND state = 'pending'",
        params![agent_num, generation],
    ));
    db!(tx.execute(
        "UPDATE source_state SET state = 'closed', updated_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND source NOT IN ('os', 'git', 'workspace_process')",
        params![now, agent_num, generation],
    ));
    events.push(insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(
            now,
            "agent.stopped",
            "agent",
            Some(agent_num),
            Some(generation),
            json!({"agent_id": agent.id, "command_id": command_id}),
        ),
    )?);
    super::native_children::end_native_children_with_parent_tx(
        tx,
        Some((agent_num, generation)),
        now,
        events,
    )
}

fn settle_owner_interrupted_turn_tx(
    tx: &Transaction<'_>,
    (agent_num, generation, turn_id): (i64, i64, &str),
    command_id: &str,
    reason: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let Some(turn) = load_turn(tx, agent_num, generation, turn_id)? else {
        return Ok(());
    };
    if turn.resolution_source == Some(eliot_core::TurnResolutionSource::Owner)
        || (reason == "interrupted_by_owner"
            && turn.outcome != Some(eliot_core::TurnOutcome::Interrupted))
        || (turn.state == eliot_core::TurnState::Completed
            && turn.outcome != Some(eliot_core::TurnOutcome::Interrupted))
    {
        return Ok(());
    }
    let changed = db!(tx.execute(
        "UPDATE turns SET state = 'completed', status = 'interrupted', outcome = 'interrupted',
                          completed_at = COALESCE(completed_at, ?1), resolution_source = 'owner'
         WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4
           AND (state IN ('running', 'unknown') OR outcome = 'interrupted')",
        params![now, agent_num, generation, turn_id],
    ));
    if changed != 1 {
        return Err(Error::new(
            "source_integrity",
            "captured Owner-stopped turn changed before stop settlement",
        ));
    }
    events.push(insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(
            now,
            "turn.resolved",
            "turn",
            Some(agent_num),
            Some(generation),
            json!({
                "command_id": command_id,
                "turn_id": turn_id,
                "status": "interrupted",
                "resolution_source": "owner",
                "reason": reason,
            }),
        ),
    )?);
    Ok(())
}

fn settle_stopped_turn_unknown_tx(
    tx: &Transaction<'_>,
    agent_num: i64,
    generation: i64,
    command_id: &str,
    turn_id: &str,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if !load_turn(tx, agent_num, generation, turn_id)?
        .is_some_and(|turn| turn.state == eliot_core::TurnState::Running)
    {
        // Keep a drained native terminal result or an earlier unknown fact.
        return Ok(());
    }
    let changed = db!(tx.execute(
        "UPDATE turns SET state = 'unknown', completed_at = ?1
         WHERE agent = ?2 AND generation = ?3 AND turn_id = ?4 AND state = 'running'",
        params![now, agent_num, generation, turn_id],
    ));
    if changed != 1 {
        return Ok(());
    }
    events.push(insert_daemon_event_for_command(
        tx,
        command_id,
        DaemonEventInput::new(
            now,
            "turn.unknown",
            "turn",
            Some(agent_num),
            Some(generation),
            json!({"command_id": command_id, "turn_id": turn_id, "reason": "stopped_without_terminal_result"}),
        ),
    )?);
    Ok(())
}

/// An Applied user-event command replays only for the same kind: the caller
/// commits and returns the recorded command without a new event.
fn applied_user_event_replay(record: &CommandRecord, kind: &str) -> bool {
    record.state == CommandState::Applied
        && record
            .command
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("kind"))
            .and_then(Value::as_str)
            == Some(kind)
}
