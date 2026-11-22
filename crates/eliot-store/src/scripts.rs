use super::*;

const SCRIPT_ACTION_ID_DOMAIN: &str = "eliot.script.action.command.v1";

fn valid_script_output_action_request(request: &SubmitCommand) -> bool {
    if request.verb == "emit" {
        return request.validate().is_ok()
            && request.target.is_none()
            && request.expected_generation.is_none()
            && request.expected_turn_id.is_none()
            && request.request_id.is_none()
            && !request.probe
            && request.args.is_object()
            && serde_json::from_value::<core::api::EmitDefinitionArgs>(request.args.clone())
                .is_ok_and(|definition| definition.validate().is_ok());
    }
    matches!(request.verb.as_str(), "stop" | "send" | "interrupt")
        && rules::valid_rule_action_request(request, true)
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScriptRunAdmission {
    pub command: Command,
    pub children: Vec<Command>,
    pub events: Vec<Event>,
    pub is_new: bool,
}

impl Store {
    /// Atomically retain the complete output of one already-dispatched Run.
    ///
    /// The stored Run supplies both principal and project registration binding.
    /// Script output is converted to ordinary ordered command children; no
    /// native action is performed while this transaction is open.
    pub fn admit_script_output(
        &mut self,
        run_id: &str,
        actions: Vec<SubmitCommand>,
        process: core::ScriptProcessResult,
        now: i64,
    ) -> Result<ScriptRunAdmission> {
        self.ensure_writer_healthy()?;
        let result = self.admit_script_output_inner(run_id, actions, process, now);
        self.fault_on_persistence_error(&result);
        result
    }

    fn admit_script_output_inner(
        &mut self,
        run_id: &str,
        actions: Vec<SubmitCommand>,
        process: core::ScriptProcessResult,
        now: i64,
    ) -> Result<ScriptRunAdmission> {
        validate_ulid(run_id)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let run = load_command_record(&tx, run_id)?
            .ok_or_else(|| Error::new("not_found", "script Run command was not found"))?;
        if run.command.verb != "run" {
            return Err(Error::new(
                "bad_state",
                "script output can be admitted only for a Run command",
            ));
        }

        if run.command.state == CommandState::Applied {
            let children = fixed_script_children(&tx, &run.command)?;
            let command = load_command(&tx, run_id)?
                .ok_or_else(|| Error::new("source_integrity", "applied Run command disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(ScriptRunAdmission {
                command,
                children,
                events: Vec::new(),
                is_new: false,
            });
        }
        if run.command.state == CommandState::Unknown {
            return Err(Error::new(
                "bad_state",
                "unknown Run output cannot be replayed or admitted",
            ));
        }
        if run.command.state != CommandState::Dispatched {
            return Err(Error::new(
                "bad_state",
                "script output requires a dispatched Run command",
            ));
        }
        core::ScriptRunApplied {
            children: Vec::new(),
            process: process.clone(),
        }
        .validate()?;
        if encode_json(&actions)?.len() > MAX_FRAME_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "script action output exceeds the 1 MiB command bound",
            ));
        }

        let frozen = decode_frozen_run(&run.command.args)?;
        let origin = if actions.is_empty() {
            rules::validate_script_run_admitted_origin_tx(&tx, &run.command, &frozen)?;
            None
        } else {
            Some(rules::resolve_script_run_origin_tx(
                &tx,
                &run.command,
                &frozen,
            )?)
        };
        projects::validate_project_command_dispatch_tx(&tx, &run)?;

        let mut events = Vec::new();
        let mut child_ids = Vec::with_capacity(actions.len());
        for (index, mut action) in actions.into_iter().enumerate() {
            action.validate()?;
            if !action.id.is_empty() || !valid_script_output_action_request(&action) {
                return Err(Error::new(
                    "invalid",
                    "script output must contain an id-free stop, send, interrupt or emit command",
                ));
            }
            let ordinal = i64::try_from(index)
                .map_err(|_| Error::new("frame_too_large", "script action ordinal overflowed"))?;
            action.id = derived_script_action_id(run_id, ordinal)?;
            let origin = origin.as_ref().ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "script output action has no saved rule origin",
                )
            })?;
            rules::validate_script_output_rate_tx(&tx, origin, action.target.as_deref(), now)?;

            let limits = self.hierarchy_limits;
            let admission = admission::submit_tx(
                &tx,
                &run.command.principal,
                action,
                None,
                now,
                true,
                limits,
                None,
            )?;
            if !admission.is_new {
                return Err(Error::new(
                    "source_integrity",
                    "derived script child exists without its committed Run child set",
                ));
            }
            let child_id = admission.command.id.clone();
            let linked = tx
                .execute(
                    "UPDATE commands
                     SET parent_command = ?1, link_kind = 'rule_action', ordinal = ?2
                     WHERE id = ?3 AND parent_command IS NULL
                       AND link_kind IS NULL AND ordinal IS NULL",
                    params![run_id, ordinal, child_id],
                )
                .map_err(sql_error)?;
            if linked != 1 {
                return Err(Error::new(
                    "source_integrity",
                    "new script child could not be bound to its Run",
                ));
            }
            let child_cause = core::EventCause {
                command_id: child_id.clone(),
                rule: Some(origin.rule.clone()),
                remote_call: origin.cause.remote_call.clone(),
                depth: origin.cause.depth,
            };
            let mut child_events = admission.events;
            rules::annotate_rule_command_event(&tx, &child_id, &mut child_events, child_cause)?;
            events.extend(child_events);
            child_ids.push(child_id);
        }

        let applied = core::ScriptRunApplied {
            children: child_ids,
            process,
        };
        applied.validate()?;
        let mut outcome = serde_json::to_value(applied)
            .map_err(|_| Error::new("internal", "applied Run result cannot be encoded"))?;
        let null_bytes = encode_json(&Value::Null)?.len();
        let wrapper_bytes = encode_json(&json!({
            "command_id": run_id,
            "state": "applied",
            "outcome": Value::Null,
        }))?
        .len()
        .saturating_sub(null_bytes);
        core::bound_script_outcome_stderr(
            &mut outcome,
            MAX_EVENT_BYTES.saturating_sub(wrapper_bytes),
        );
        let applied = serde_json::from_value::<core::ScriptRunApplied>(outcome.clone())
            .map_err(|_| Error::new("source_integrity", "bounded Run result is invalid"))?;
        applied
            .validate()
            .map_err(|_| Error::new("source_integrity", "bounded Run result is invalid"))?;
        let event_data = json!({
            "command_id": run_id,
            "state": "applied",
            "outcome": outcome,
        });
        if encode_json(&event_data)?.len() > MAX_EVENT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "applied Run event exceeds the persisted event bound after stderr trimming",
            ));
        }
        update_command_state(
            &tx,
            run_id,
            CommandState::Applied,
            Some(&event_data["outcome"]),
            None,
            now,
        )?;
        events.push(insert_daemon_command_event(
            &tx,
            now,
            "command.applied",
            None,
            None,
            run_id,
            event_data,
        )?);

        let command = load_command(&tx, run_id)?
            .ok_or_else(|| Error::new("internal", "applied Run command disappeared"))?;
        let children = fixed_script_children(&tx, &command)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(ScriptRunAdmission {
            command,
            children,
            events,
            is_new: true,
        })
    }
}

pub(super) fn decode_frozen_run(args: &Value) -> Result<core::api::FrozenScriptRun> {
    let frozen = serde_json::from_value::<core::api::FrozenScriptRun>(args.clone())
        .map_err(|_| Error::new("invalid", "run args must be a frozen script snapshot"))?;
    frozen.validate()?;
    Ok(frozen)
}

pub(super) fn validate_rule_run_context_tx(
    tx: &Transaction<'_>,
    principal: &str,
    rule: &str,
    revision: i64,
    event_seq: i64,
    args: &Value,
) -> Result<()> {
    let frozen = decode_frozen_run(args)?;
    let core::api::FrozenScriptContext::Event {
        event: frozen_event,
        subject: frozen_subject,
        rule: frozen_rule,
    } = &frozen.context
    else {
        return Err(Error::new(
            "invalid",
            "event rule firing requires an event Run context",
        ));
    };
    if frozen_rule.id != rule || frozen_rule.revision != revision || frozen_event.seq != event_seq {
        return Err(Error::new(
            "invalid",
            "frozen Run context does not match its rule firing",
        ));
    }
    let event = load_event_tx(tx, event_seq)?;
    if frozen_event.as_ref() != &event || frozen_subject != &event.subject {
        return Err(Error::new(
            "invalid",
            "frozen Run context does not match the committed event and subject",
        ));
    }
    if principal.trim().is_empty() {
        return Err(Error::new("invalid", "Run principal is empty"));
    }
    Ok(())
}

pub(super) fn validate_run_dispatch_tx(tx: &Transaction<'_>, run: &CommandRecord) -> Result<()> {
    let frozen = decode_frozen_run(&run.command.args)?;
    rules::validate_script_run_admitted_origin_tx(tx, &run.command, &frozen)?;
    Ok(())
}

pub(super) fn load_event_tx(tx: &Transaction<'_>, seq: i64) -> Result<Event> {
    tx.query_row(
        "SELECT seq, ts, kind, agent, generation, source, native_id, data, subject, cause
         FROM events WHERE seq = ?1",
        [seq],
        event_record_from_row,
    )
    .optional()
    .map_err(sql_error)?
    .ok_or_else(|| Error::new("not_found", "frozen script event was not found"))
    .and_then(event_from_record)
}

fn derived_script_action_id(run_id: &str, ordinal: i64) -> Result<String> {
    let digest = canonical::digest(&json!({
        "domain": SCRIPT_ACTION_ID_DOMAIN,
        "run_id": run_id,
        "ordinal": ordinal,
    }))?;
    let bytes: [u8; 16] = digest[..16]
        .try_into()
        .map_err(|_| Error::new("internal", "script action digest is too short"))?;
    Ok(Ulid::from_bytes(bytes).to_string())
}

fn fixed_script_children(tx: &Transaction<'_>, run: &Command) -> Result<Vec<Command>> {
    let applied = decode_script_run_applied(run.outcome.as_ref())?;
    let expected = applied.children;
    let mut statement = tx
        .prepare(
            "SELECT id, principal, ordinal FROM commands
             WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL
             ORDER BY ordinal",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map([&run.id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })
        .map_err(sql_error)?;
    let stored = rows
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    if stored.len() != expected.len()
        || stored
            .iter()
            .enumerate()
            .any(|(index, (id, principal, ordinal))| {
                expected.get(index) != Some(id)
                    || principal != &run.principal
                    || i64::try_from(index).ok() != Some(*ordinal)
            })
    {
        return Err(Error::new(
            "source_integrity",
            "Run child rows differ from its committed fixed child set",
        ));
    }
    drop(statement);
    expected
        .iter()
        .map(|id| {
            load_command(tx, id)?
                .ok_or_else(|| Error::new("source_integrity", "committed Run child is missing"))
        })
        .collect()
}

fn decode_script_run_applied(outcome: Option<&Value>) -> Result<core::ScriptRunApplied> {
    let outcome = outcome
        .ok_or_else(|| Error::new("source_integrity", "applied Run has no retained result"))?;
    let applied = serde_json::from_value::<core::ScriptRunApplied>(outcome.clone())
        .map_err(|_| Error::new("source_integrity", "stored applied Run result is invalid"))?;
    applied
        .validate()
        .map_err(|_| Error::new("source_integrity", "stored applied Run result is invalid"))?;
    Ok(applied)
}
