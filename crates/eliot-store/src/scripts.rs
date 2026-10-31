use super::*;

const SCRIPT_ACTION_ID_DOMAIN: &str = "eliot.script.action.command.v1";

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
        let origin = rules::resolve_script_run_origin_tx(&tx, &run.command, &frozen)?;
        projects::validate_project_command_dispatch_tx(&tx, &run)?;

        let mut events = Vec::new();
        let mut child_ids = Vec::with_capacity(actions.len());
        for (index, mut action) in actions.into_iter().enumerate() {
            if !action.id.is_empty() || !rules::valid_rule_action_request(&action, true) {
                return Err(Error::new(
                    "invalid",
                    "script output must contain an id-free stop, send or interrupt command",
                ));
            }
            let ordinal = i64::try_from(index)
                .map_err(|_| Error::new("frame_too_large", "script action ordinal overflowed"))?;
            action.id = derived_script_action_id(run_id, ordinal)?;
            rules::validate_script_output_rate_tx(&tx, &origin, action.target.as_deref(), now)?;

            let admission = submit_tx(&tx, &run.command.principal, action, None, now, true)?;
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
    rules::resolve_script_run_origin_tx(tx, &run.command, &frozen)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    use std::path::Path;

    fn store() -> Store {
        Store::open(Path::new(":memory:")).expect("open Store")
    }

    fn rule_input(id: &str, limit: Option<Value>) -> RuleDefinitionInput {
        let mut value = json!({
            "id": id,
            "enabled": true,
            "on": {"event": "turn.completed"},
            "do": [{"action": "run", "cmd": ["pwsh", ".eliot/scripts/check.ps1"]}]
        });
        if let Some(limit) = limit {
            value["limit"] = limit;
        }
        RuleDefinitionInput {
            id: id.to_owned(),
            canonical_definition: serde_json::to_string(&value).expect("encode rule"),
            definition_digest: core::canonical_json_sha256(&value).expect("digest rule"),
            enabled: true,
        }
    }

    fn rule_input_with_allow_self(
        id: &str,
        limit: Option<Value>,
        allow_self: bool,
    ) -> RuleDefinitionInput {
        let mut input = rule_input(id, limit);
        let mut value: Value =
            serde_json::from_str(&input.canonical_definition).expect("decode rule");
        value["allow_self"] = json!(allow_self);
        input.canonical_definition = serde_json::to_string(&value).expect("encode rule");
        input.definition_digest = core::canonical_json_sha256(&value).expect("digest rule");
        input
    }

    fn stop_rule_input(id: &str) -> RuleDefinitionInput {
        let value = json!({
            "id": id,
            "enabled": true,
            "on": {"event": "turn.completed"},
            "do": [{
                "action": "stop",
                "target": "${event.agent}",
                "expected_generation": "${event.generation}"
            }]
        });
        RuleDefinitionInput {
            id: id.to_owned(),
            canonical_definition: serde_json::to_string(&value).expect("encode stop rule"),
            definition_digest: core::canonical_json_sha256(&value).expect("digest stop rule"),
            enabled: true,
        }
    }

    fn insert_agent(store: &mut Store, project: &str, now: i64) -> (i64, String) {
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("open Agent transaction");
        tx.execute(
            "INSERT INTO agents(generation, project, harness, profile, state, created_at, updated_at)
             VALUES (1, ?1, 'fake', 'fake', 'idle', ?2, ?2)",
            params![project, now],
        )
        .expect("insert ready Agent");
        let id = tx.last_insert_rowid();
        tx.commit().expect("commit Agent");
        (id, format_agent_id(id))
    }

    fn native_ready_agent(store: &mut Store, project: &str, now: i64) -> (i64, String) {
        let launch_id = ulid::Ulid::new().to_string();
        let launch = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":project, "workspace":"synthetic-workspace"}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent_id = launch.command.target.expect("launch target");
        store
            .dispatch(&launch_id, now + 1)
            .expect("dispatch launch");
        store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{project}:ready"),
                    agent_id: agent_id.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        kind: "session.ready".to_owned(),
                        native_id: format!("{project}:ready"),
                        data: json!({"command_id":launch_id}),
                        ts: None,
                    }],
                    cursor: None,
                },
                now + 2,
            )
            .expect("commit Agent readiness");
        store
            .settle(&launch_id, CommandState::Applied, None, now + 3)
            .expect("settle launch");
        let agent_num = core::parse_agent_id(&agent_id).expect("parse Agent id");
        (agent_num, agent_id)
    }

    fn insert_event(
        store: &mut Store,
        now: i64,
        agent: Option<(i64, i64)>,
        cause: Option<core::EventCause>,
    ) -> Event {
        let tx = store
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .expect("open event transaction");
        let mut input = DaemonEventInput::new(
            now,
            "turn.completed",
            "script_test",
            agent.map(|(id, _)| id),
            agent.map(|(_, generation)| generation),
            json!({"result":"ok"}),
        );
        input.cause = cause;
        let event = insert_daemon_event_input(&tx, input).expect("insert rule event");
        tx.commit().expect("commit rule event");
        event
    }

    fn frozen_run(event: &Event, rule: &str, revision: i64) -> core::api::FrozenScriptRun {
        let executable = std::env::current_exe()
            .expect("test executable path")
            .to_string_lossy()
            .into_owned();
        let temp = std::env::temp_dir();
        core::api::FrozenScriptRun {
            definition: core::api::RunDefinitionArgs {
                cmd: vec!["pwsh".to_owned(), ".eliot/scripts/check.ps1".to_owned()],
                cwd: None,
                timeout: None,
                env: Default::default(),
            },
            interpreter: core::api::FrozenScriptInterpreter {
                reference: "pwsh".to_owned(),
                executable: FrozenSetupFile {
                    canonical_path: executable,
                    size: 1,
                    sha256: "ab".repeat(32),
                },
                argv_prefix: vec!["-File".to_owned()],
            },
            entry: FrozenSetupFile {
                canonical_path: temp.join("eliot-script-test.ps1").display().to_string(),
                size: 1,
                sha256: "cd".repeat(32),
            },
            context: core::api::FrozenScriptContext::Event {
                event: Box::new(event.clone()),
                subject: event.subject.clone(),
                rule: core::api::ScriptRuleContext {
                    id: rule.to_owned(),
                    revision,
                },
            },
        }
    }

    fn run_request(event: &Event, rule: &str, revision: i64) -> SubmitCommand {
        let mut request = SubmitCommand::new(String::new(), "run");
        request.args =
            serde_json::to_value(frozen_run(event, rule, revision)).expect("encode frozen Run");
        request
    }

    fn admit_run(
        store: &mut Store,
        principal: &str,
        rule: &str,
        now: i64,
        agent: Option<(i64, i64)>,
        limit: Option<Value>,
    ) -> (core::api::RuleStateView, Event, Command) {
        let state = store
            .ensure_rule_state(principal, &rule_input(rule, limit), now)
            .expect("ensure event rule");
        let event = insert_event(store, now + 1, agent, None);
        let admission = store
            .admit_rule_action(
                principal,
                &state,
                event.seq,
                run_request(&event, rule, state.revision),
                now + 2,
            )
            .expect("admit targetless Run child");
        let run = admission.child.expect("Run child").command;
        (admission.state, event, run)
    }

    fn stop_action(target: &str) -> SubmitCommand {
        core::api::ScriptActionRecord::Stop {
            target: target.to_owned(),
            expected_generation: 1,
        }
        .into_submit_command()
        .expect("convert stop output")
    }

    fn send_action(target: &str, text: &str) -> SubmitCommand {
        core::api::ScriptActionRecord::Send {
            target: target.to_owned(),
            expected_generation: 1,
            text: text.to_owned(),
        }
        .into_submit_command()
        .expect("convert send output")
    }

    fn process_result(custody: core::ScriptCustody) -> core::ScriptProcessResult {
        core::ScriptProcessResult {
            custody,
            quiescent: true,
            direct_exit: core::ScriptDirectExit::Exited {
                success: true,
                exit_code: Some(0),
            },
            stderr: None,
        }
    }

    fn child_ids(store: &Store, run_id: &str) -> Vec<String> {
        let mut statement = store
            .connection
            .prepare(
                "SELECT id FROM commands
                 WHERE parent_command = ?1 AND link_kind = 'rule_action' AND ordinal IS NOT NULL
                 ORDER BY ordinal",
            )
            .expect("prepare script child read");
        statement
            .query_map([run_id], |row| row.get(0))
            .expect("query script children")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect script children")
    }

    fn dispatch_run(store: &mut Store, run: &Command, now: i64) -> Command {
        let dispatched = store
            .dispatch(&run.id, now)
            .expect("dispatch targetless Run");
        assert_eq!(dispatched.value.state, CommandState::Dispatched);
        assert!(
            dispatched
                .events
                .iter()
                .any(|event| event.kind == "command.dispatched" && event.agent.is_none())
        );
        dispatched.value
    }

    struct ScriptSendCompletion {
        state: core::api::RuleStateView,
        event: Event,
    }

    fn script_send_completion(
        store: &mut Store,
        rule: &str,
        allow_self: bool,
        now: i64,
    ) -> ScriptSendCompletion {
        let (agent_num, agent_id) = native_ready_agent(store, rule, now);
        let state = store
            .ensure_rule_state(
                "owner",
                &rule_input_with_allow_self(
                    rule,
                    Some(json!({"max":3,"per":"rule","window_ms":60_000})),
                    allow_self,
                ),
                now + 4,
            )
            .expect("ensure Run rule");
        let trigger = insert_event(store, now + 5, Some((agent_num, 1)), None);
        let first = store
            .admit_rule_action(
                "owner",
                &state,
                trigger.seq,
                run_request(&trigger, rule, state.revision),
                now + 6,
            )
            .expect("admit initial rule Run");
        let state = first.state;
        let run = dispatch_run(
            store,
            &first.child.expect("initial Run child").command,
            now + 7,
        );
        let output = store
            .admit_script_output(
                &run.id,
                vec![send_action(&agent_id, "script output")],
                process_result(core::ScriptCustody::Exact),
                now + 8,
            )
            .expect("admit script SEND output");
        let send = output.children.first().expect("script SEND child").clone();
        store
            .dispatch(&send.id, now + 9)
            .expect("dispatch script SEND");

        let turn_id = format!("{rule}-script-send-turn");
        let committed = store
            .put_batch(
                SourceBatch {
                    batch_id: format!("{rule}:script-send-result"),
                    agent_id: agent_id.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![
                        NativeEvent {
                            kind: "turn.started".to_owned(),
                            native_id: format!("{rule}:turn-started"),
                            data: json!({"command_id":send.id, "turn_id":turn_id}),
                            ts: None,
                        },
                        NativeEvent {
                            kind: "message.completed".to_owned(),
                            native_id: format!("{rule}:message-completed"),
                            data: json!({
                                "command_id":send.id,
                                "turn_id":turn_id,
                                "text":"script output"
                            }),
                            ts: None,
                        },
                        NativeEvent {
                            kind: "turn.completed".to_owned(),
                            native_id: format!("{rule}:turn-completed"),
                            data: json!({
                                "command_id":send.id,
                                "turn_id":turn_id,
                                "status":"completed"
                            }),
                            ts: None,
                        },
                    ],
                    cursor: None,
                },
                now + 10,
            )
            .expect("commit native script Send completion");
        let event = committed
            .events
            .iter()
            .find(|event| event.kind == "turn.completed")
            .expect("native turn completion event")
            .clone();
        let stored_cause = command_cause_tx(&store.connection, &send.id)
            .expect("read script Send cause")
            .expect("script Send has a stored cause");
        assert_eq!(event.cause, Some(stored_cause));

        ScriptSendCompletion { state, event }
    }

    fn stored_command_count(store: &Store) -> i64 {
        store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .expect("count stored commands")
    }

    fn stored_rule_action_count(store: &Store) -> i64 {
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM commands
                 WHERE link_kind = 'rule_action' AND ordinal IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .expect("count stored rule actions")
    }

    fn project_registration(id: &str, actions: Vec<String>) -> ProjectRegistrationInput {
        let executable = std::env::current_exe()
            .expect("test executable path")
            .to_string_lossy()
            .into_owned();
        let checkout = std::env::temp_dir().join(format!("eliot-store-{id}"));
        ProjectRegistrationInput {
            id: id.to_owned(),
            checkout: checkout.display().to_string(),
            repo_common_dir: checkout.join(".git").display().to_string(),
            profiles: Vec::new(),
            actions,
            interpreters: std::collections::BTreeMap::from([(
                "pwsh".to_owned(),
                core::api::ScriptInterpreter {
                    program: executable,
                    argv_prefix: vec!["-File".to_owned()],
                },
            )]),
            run_env: Vec::new(),
        }
    }

    #[test]
    fn targetless_run_dispatch_recovers_unknown_and_never_replays_output() {
        let mut store = store();
        let (_, _, run) = admit_run(&mut store, "owner", "recover-script", 10, None, None);
        let run = dispatch_run(&mut store, &run, 13);

        store
            .recover_on_restart(14)
            .expect("recover dispatched Run");
        assert_eq!(
            store
                .get_command(&run.id)
                .expect("read recovered Run")
                .unwrap()
                .state,
            CommandState::Unknown
        );
        assert!(
            store
                .admit_script_output(
                    &run.id,
                    Vec::new(),
                    process_result(core::ScriptCustody::Exact),
                    15,
                )
                .expect_err("unknown Run output cannot be replayed")
                .code
                == "bad_state"
        );
        assert!(child_ids(&store, &run.id).is_empty());
    }

    #[test]
    fn invalid_second_output_rolls_back_the_whole_child_set_and_run_can_fail() {
        let mut store = store();
        let (_, first_agent) = insert_agent(&mut store, "script-atomic", 10);
        let (_, _, run) = admit_run(&mut store, "owner", "atomic-script", 11, None, None);
        let run = dispatch_run(&mut store, &run, 14);

        let process_unknown = core::ScriptProcessResult {
            custody: core::ScriptCustody::Unknown,
            quiescent: false,
            direct_exit: core::ScriptDirectExit::Unknown,
            stderr: None,
        };
        let process_error = store
            .admit_script_output(
                &run.id,
                vec![stop_action(&first_agent)],
                process_unknown,
                15,
            )
            .expect_err("unknown custody cannot produce an applied Run");
        assert_eq!(process_error.code, "invalid");
        assert!(child_ids(&store, &run.id).is_empty());

        let error = store
            .admit_script_output(
                &run.id,
                vec![stop_action(&first_agent), stop_action("a999")],
                process_result(core::ScriptCustody::Exact),
                16,
            )
            .expect_err("second child target is not admitted");
        assert_eq!(error.code, "not_found");
        assert!(child_ids(&store, &run.id).is_empty());
        assert_eq!(
            store.get_command(&run.id).expect("read Run").unwrap().state,
            CommandState::Dispatched
        );

        let failed = store
            .settle(
                &run.id,
                CommandState::Failed,
                Some(json!({"reason":"invalid_script_output"})),
                17,
            )
            .expect("settle known script failure");
        assert_eq!(failed.value.state, CommandState::Failed);
        assert!(child_ids(&store, &run.id).is_empty());
    }

    #[test]
    fn output_ids_replay_quietly_and_run_children_obey_order_unknown_and_failure() {
        let mut store = store();
        let (_, first_agent) = insert_agent(&mut store, "script-order-a", 10);
        let (_, second_agent) = insert_agent(&mut store, "script-order-b", 10);
        let (_, _, run) = admit_run(&mut store, "owner", "ordered-script", 11, None, None);
        let run = dispatch_run(&mut store, &run, 14);

        let admitted = store
            .admit_script_output(
                &run.id,
                vec![stop_action(&first_agent), stop_action(&second_agent)],
                process_result(core::ScriptCustody::Degraded),
                15,
            )
            .expect("commit both script outputs");
        assert!(admitted.is_new);
        assert_eq!(admitted.command.state, CommandState::Applied);
        assert_eq!(admitted.children.len(), 2);
        assert_eq!(
            admitted.command.outcome.as_ref().unwrap()["process"]["direct_exit"],
            json!({"state":"exited", "success":true, "exit_code":0})
        );
        let ids = admitted
            .children
            .iter()
            .map(|child| child.id.clone())
            .collect::<Vec<_>>();
        assert!(ids.iter().all(|id| !id.is_empty()));
        assert_eq!(child_ids(&store, &run.id), ids);
        let applied = admitted.command.outcome.as_ref().unwrap();
        assert_eq!(applied["children"], json!(ids));
        assert!(applied.get("command").is_none());

        let replay = store
            .admit_script_output(
                &run.id,
                Vec::new(),
                process_result(core::ScriptCustody::Exact),
                16,
            )
            .expect("read retained fixed output set");
        assert!(!replay.is_new);
        assert!(replay.events.is_empty());
        assert_eq!(replay.command.outcome, admitted.command.outcome);
        assert_eq!(
            replay
                .children
                .iter()
                .map(|child| child.id.clone())
                .collect::<Vec<_>>(),
            ids
        );
        assert_eq!(
            replay.command.outcome.as_ref().unwrap()["process"]["custody"],
            "degraded",
            "replay returns the original process metadata"
        );

        let held = store.dispatch(&ids[1], 17).expect("hold second sibling");
        assert_eq!(held.value.state, CommandState::Queued);
        let first = store.dispatch(&ids[0], 18).expect("dispatch first sibling");
        assert_eq!(first.value.state, CommandState::Dispatched);

        store
            .connection
            .execute(
                "UPDATE commands SET state = 'unknown' WHERE id = ?1",
                [&ids[0]],
            )
            .expect("simulate unknown predecessor");
        let held_unknown = store
            .dispatch(&ids[1], 19)
            .expect("unknown holds successor");
        assert_eq!(held_unknown.value.state, CommandState::Queued);

        store
            .connection
            .execute(
                "UPDATE commands SET state = 'failed' WHERE id = ?1",
                [&ids[0]],
            )
            .expect("simulate terminal refusal");
        let cancelled = store
            .dispatch(&ids[1], 20)
            .expect("terminal predecessor cancels successor");
        assert_eq!(cancelled.value.state, CommandState::Cancelled);
    }

    #[test]
    fn escaped_stderr_is_bounded_to_applied_event_envelope_without_losing_primary_result() {
        let mut store = store();
        let (_, _, run) = admit_run(&mut store, "owner", "stderr-script", 11, None, None);
        let run = dispatch_run(&mut store, &run, 14);
        let mut process = process_result(core::ScriptCustody::Exact);
        process.stderr = Some(core::StderrSnapshot {
            text: "\0".repeat(core::MAX_STDERR_BYTES),
            truncated: false,
            eof: true,
            read_error: true,
        });

        let admitted = store
            .admit_script_output(&run.id, Vec::new(), process, 15)
            .expect("trim optional stderr and apply the successful Run");
        let applied_event = admitted
            .events
            .iter()
            .find(|event| event.kind == "command.applied")
            .expect("Run applied event");
        assert!(encode_json(&applied_event.data).unwrap().len() <= MAX_EVENT_BYTES);
        assert_eq!(
            admitted.command.outcome.as_ref().unwrap(),
            &applied_event.data["outcome"]
        );

        let outcome = admitted.command.outcome.as_ref().unwrap();
        assert_eq!(outcome["children"], json!([]));
        assert_eq!(outcome["process"]["custody"], "exact");
        assert_eq!(
            outcome["process"]["direct_exit"],
            json!({"state":"exited", "success":true, "exit_code":0})
        );
        let stderr: core::StderrSnapshot =
            serde_json::from_value(outcome["process"]["stderr"].clone()).unwrap();
        assert!(stderr.text.len() < core::MAX_STDERR_BYTES);
        assert!(stderr.truncated);
        assert!(stderr.eof);
        assert!(stderr.read_error);
    }

    #[test]
    fn run_cause_depth_and_self_result_suppression_follow_the_stored_root_rule() {
        let mut store = store();
        let (agent_num, agent_id) = insert_agent(&mut store, "script-cause", 10);
        let state = store
            .ensure_rule_state("owner", &rule_input("cause-script", None), 11)
            .expect("ensure event rule");
        let event = insert_event(&mut store, 12, Some((agent_num, 1)), None);
        let run_admission = store
            .admit_rule_action(
                "owner",
                &state,
                event.seq,
                run_request(&event, "cause-script", state.revision),
                13,
            )
            .expect("admit first Run");
        let run = dispatch_run(
            &mut store,
            &run_admission.child.expect("first Run child").command,
            14,
        );
        let output = store
            .admit_script_output(
                &run.id,
                vec![send_action(&agent_id, "caused")],
                process_result(core::ScriptCustody::Exact),
                15,
            )
            .expect("admit script SEND output");
        let send = &output.children[0];
        let send_cause = command_cause_tx(&store.connection, &send.id)
            .expect("read output cause")
            .expect("output has a cause");
        assert_eq!(send_cause.rule.as_deref(), Some("cause-script"));
        assert_eq!(send_cause.depth, 1);

        let forged_rule_cause = core::EventCause {
            command_id: send.id.clone(),
            rule: Some("forged-rule".to_owned()),
            depth: send_cause.depth,
        };
        let event2 = insert_event(
            &mut store,
            16,
            Some((agent_num, 1)),
            Some(forged_rule_cause.clone()),
        );
        let current = store
            .get_rule_state("owner", "cause-script")
            .expect("read current rule")
            .expect("rule remains stored");
        let suppressed = store
            .admit_rule_action(
                "owner",
                &current,
                event2.seq,
                send_action(&agent_id, "self-send"),
                17,
            )
            .expect("suppress same-rule SEND by stored ancestry");
        assert!(suppressed.parent.is_none());
        assert!(suppressed.child.is_none());

        let event3 = insert_event(
            &mut store,
            18,
            Some((agent_num, 1)),
            Some(forged_rule_cause),
        );
        let current = store
            .get_rule_state("owner", "cause-script")
            .expect("read advanced rule")
            .expect("rule remains stored");
        let high_water_before = store.high_water().expect("read event boundary");
        let commands_before = stored_command_count(&store);
        let actions_before = stored_rule_action_count(&store);
        let suppressed_run = store
            .admit_rule_action(
                "owner",
                &current,
                event3.seq,
                run_request(&event3, "cause-script", current.revision),
                19,
            )
            .expect("suppress Run at the next self-cause depth");
        assert!(suppressed_run.parent.is_none());
        assert!(suppressed_run.child.is_none());
        assert!(suppressed_run.events.is_empty());
        assert_eq!(suppressed_run.state.current_cursor, event3.seq);
        assert_eq!(stored_command_count(&store), commands_before);
        assert_eq!(stored_rule_action_count(&store), actions_before);
        assert_eq!(
            store.high_water().expect("read event boundary"),
            high_water_before
        );
    }

    fn assert_live_script_send_run_case(allow_self: bool) {
        let request_id = ulid::Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).expect("timestamp fits") + 100;
        let rule = if allow_self {
            "live-run-self-allowed"
        } else {
            "live-run-self-suppressed"
        };
        let mut store = store();
        let fixture = script_send_completion(&mut store, rule, allow_self, now);
        let high_water_before = store.high_water().expect("read event boundary");
        let commands_before = stored_command_count(&store);
        let actions_before = stored_rule_action_count(&store);

        let admission = store
            .admit_rule_action(
                "owner",
                &fixture.state,
                fixture.event.seq,
                run_request(&fixture.event, rule, fixture.state.revision),
                now + 20,
            )
            .expect("admit or suppress self-caused live Run");

        if allow_self {
            let parent = admission.parent.expect("allow_self Run parent");
            let run = admission.child.expect("allow_self Run child").command;
            assert_eq!(parent.verb, "rule");
            assert_eq!(run.verb, "run");
            let cause = command_cause_tx(&store.connection, &run.id)
                .expect("read admitted Run cause")
                .expect("admitted Run cause");
            assert_eq!(cause.rule.as_deref(), Some(rule));
            assert_eq!(cause.depth, 2);
            assert_eq!(stored_command_count(&store), commands_before + 2);
            assert_eq!(stored_rule_action_count(&store), actions_before + 1);
            assert!(stored_rule_action_count(&store) <= 3);
            assert!(store.high_water().expect("read event boundary") > high_water_before);
        } else {
            assert!(admission.parent.is_none());
            assert!(admission.child.is_none());
            assert!(admission.events.is_empty());
            assert_eq!(admission.state.current_cursor, fixture.event.seq);
            assert_eq!(stored_command_count(&store), commands_before);
            assert_eq!(stored_rule_action_count(&store), actions_before);
            assert_eq!(
                store.high_water().expect("read event boundary"),
                high_water_before
            );
        }
    }

    #[test]
    fn live_run_from_own_script_send_result_is_suppressed_without_consumption() {
        assert_live_script_send_run_case(false);
    }

    #[test]
    fn allow_self_permits_live_run_from_own_script_send_result_within_limits() {
        assert_live_script_send_run_case(true);
    }

    fn assert_history_script_send_run_case(allow_self: bool) {
        let request_id = ulid::Ulid::new();
        let now = i64::try_from(request_id.timestamp_ms()).expect("timestamp fits") + 100;
        let rule = if allow_self {
            "history-run-self-allowed"
        } else {
            "history-run-self-suppressed"
        };
        let mut store = store();
        let fixture = script_send_completion(&mut store, rule, allow_self, now);
        let through_seq = store.high_water().expect("read catch-up boundary");
        store
            .advance_rule_cursor(
                "owner",
                &fixture.state,
                through_seq,
                RuleCursorOutcome::NoMatch,
                now + 20,
            )
            .expect("advance live cursor through the native result");
        let current = store
            .get_rule_state("owner", rule)
            .expect("read advanced rule")
            .expect("rule remains stored");
        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            rule,
            fixture.state.current_cursor,
            Some(through_seq),
        );
        let actions = [RuleCatchUpAction::command(
            fixture.event.seq,
            run_request(&fixture.event, rule, current.revision),
        )];
        let commands_before = stored_command_count(&store);
        let actions_before = stored_rule_action_count(&store);

        let admission = store
            .admit_rule_catch_up("owner", &request, &current, through_seq, &actions, now + 21)
            .expect("admit or suppress self-caused history Run");

        if allow_self {
            assert_eq!(admission.children.len(), 1);
            let run = &admission.children[0].command;
            assert_eq!(run.verb, "run");
            let cause = command_cause_tx(&store.connection, &run.id)
                .expect("read admitted history Run cause")
                .expect("admitted history Run cause");
            assert_eq!(cause.rule.as_deref(), Some(rule));
            assert_eq!(cause.depth, 2);
            assert_eq!(stored_command_count(&store), commands_before + 2);
            assert_eq!(stored_rule_action_count(&store), actions_before + 1);
            assert!(stored_rule_action_count(&store) <= 3);
            assert!(admission.events.iter().any(|event| {
                event.kind == "rule.fired" && event.data["event_seq"] == fixture.event.seq
            }));
        } else {
            assert!(admission.children.is_empty());
            assert_eq!(
                admission.parent.outcome.as_ref().unwrap()["children"],
                json!([])
            );
            assert_eq!(
                admission.parent.outcome.as_ref().unwrap()["suppressed_event_seqs"],
                json!([fixture.event.seq])
            );
            assert!(!admission.events.iter().any(|event| {
                event.kind == "rule.fired" && event.data["event_seq"] == fixture.event.seq
            }));
            assert_eq!(stored_command_count(&store), commands_before + 1);
            assert_eq!(stored_rule_action_count(&store), actions_before);
        }
    }

    #[test]
    fn catch_up_run_from_own_script_send_result_is_suppressed_without_action_consumption() {
        assert_history_script_send_run_case(false);
    }

    #[test]
    fn allow_self_permits_history_run_from_own_script_send_result_within_limits() {
        assert_history_script_send_run_case(true);
    }

    #[test]
    fn rule_admission_rejects_forged_frozen_event_or_revision() {
        let mut store = store();
        for (rule, mismatch) in [
            ("forged-script-event", "event"),
            ("forged-script-revision", "revision"),
        ] {
            let state = store
                .ensure_rule_state("owner", &rule_input(rule, None), 10)
                .expect("ensure event rule");
            let event = insert_event(&mut store, 11, None, None);
            let mut request = run_request(&event, rule, state.revision);
            match mismatch {
                "event" => {
                    request.args["context"]["event"]["data"]["result"] = json!("caller-asserted")
                }
                "revision" => {
                    request.args["context"]["rule"]["revision"] =
                        json!(state.revision.saturating_add(1))
                }
                _ => unreachable!(),
            }

            let admission = store
                .admit_rule_action("owner", &state, event.seq, request, 12)
                .expect("persist deterministic rule rejection");
            let parent = admission.parent.expect("rejected firing parent");
            assert_eq!(parent.state, CommandState::Rejected);
            assert!(admission.child.is_none());
            assert!(child_ids(&store, &parent.id).is_empty());
            assert!(admission.events.iter().any(|event| {
                event.kind == "rule.rejected" && event.data["code"] == "rule_action_binding_invalid"
            }));
        }
    }

    #[test]
    fn project_registration_revocation_blocks_script_output_admission() {
        let mut store = store();
        let project = "script-revocation";
        let registration = project_registration(project, vec!["run".to_owned()]);
        store
            .upsert_project_registration("owner", &registration, 10)
            .expect("register project run authority");
        let principal = format!("project:{project}");
        let (_, _, run) = admit_run(&mut store, &principal, "project-script", 11, None, None);
        let run = dispatch_run(&mut store, &run, 14);

        let revoked = project_registration(project, Vec::new());
        store
            .upsert_project_registration("owner", &revoked, 15)
            .expect("revoke project Run authority");
        let error = store
            .admit_script_output(
                &run.id,
                Vec::new(),
                process_result(core::ScriptCustody::Exact),
                16,
            )
            .expect_err("revoked registration cannot admit new script results");
        assert_eq!(error.code, "not_allowed");
        assert!(child_ids(&store, &run.id).is_empty());
        assert_eq!(
            store
                .get_command(&run.id)
                .expect("read still-dispatched Run")
                .unwrap()
                .state,
            CommandState::Dispatched
        );
    }

    #[test]
    fn script_children_count_once_under_the_saved_rule_rate_limit() {
        let mut store = store();
        let (_, first_agent) = insert_agent(&mut store, "script-rate-a", 10);
        let (_, second_agent) = insert_agent(&mut store, "script-rate-b", 10);
        let limit = json!({"max":2,"per":"rule","window_ms":60_000});
        let (_, _, run) = admit_run(&mut store, "owner", "rate-script", 11, None, Some(limit));
        let run = dispatch_run(&mut store, &run, 14);

        let error = store
            .admit_script_output(
                &run.id,
                vec![stop_action(&first_agent), stop_action(&second_agent)],
                process_result(core::ScriptCustody::Exact),
                15,
            )
            .expect_err("Run plus two emitted children exceed max=2");
        assert_eq!(error.code, "rate_limited");
        assert!(child_ids(&store, &run.id).is_empty());

        let one = store
            .admit_script_output(
                &run.id,
                vec![stop_action(&first_agent)],
                process_result(core::ScriptCustody::Exact),
                16,
            )
            .expect("Run and one actual child fit max=2");
        assert!(one.is_new);
        assert_eq!(one.children.len(), 1);
    }

    #[test]
    fn targetless_live_run_does_not_borrow_its_event_agent_for_rate_scope() {
        let mut store = store();
        let (agent_num, agent_id) = insert_agent(&mut store, "live-targetless-rate", 10);
        let limit = json!({"max":1,"per":"agent","window_ms":60_000});
        let (_, _, run) = admit_run(
            &mut store,
            "owner",
            "live-targetless-rate",
            11,
            Some((agent_num, 1)),
            Some(limit),
        );
        let run = dispatch_run(&mut store, &run, 14);
        let output = store
            .admit_script_output(
                &run.id,
                vec![stop_action(&agent_id)],
                process_result(core::ScriptCustody::Exact),
                15,
            )
            .expect("only the actual targeted child consumes per-Agent rate");
        assert_eq!(output.children.len(), 1);
    }

    #[test]
    fn catch_up_run_uses_captured_event_range_cause_and_replay() {
        let mut store = store();
        let request_id = ulid::Ulid::new();
        let next_request_id = ulid::Ulid::new();
        let request_ms = i64::try_from(request_id.timestamp_ms()).expect("timestamp fits");
        let next_request_ms =
            i64::try_from(next_request_id.timestamp_ms()).expect("timestamp fits");
        let now = request_ms.max(next_request_ms) + 100;
        let (agent_num, agent_id) = insert_agent(&mut store, "catchup-script", now - 10);

        let prior_state = store
            .ensure_rule_state("owner", &stop_rule_input("prior-cause"), now - 9)
            .expect("ensure cause source rule");
        let prior_event = insert_event(&mut store, now - 8, Some((agent_num, 1)), None);
        let prior_child = store
            .admit_rule_action(
                "owner",
                &prior_state,
                prior_event.seq,
                stop_action(&agent_id),
                now - 7,
            )
            .expect("admit cause source child")
            .child
            .expect("cause source child")
            .command;
        let trigger_cause = command_cause_tx(&store.connection, &prior_child.id)
            .expect("read source cause")
            .expect("source command has cause");
        assert_eq!(trigger_cause.depth, 1);

        let event = insert_event(
            &mut store,
            now - 6,
            Some((agent_num, 1)),
            Some(trigger_cause),
        );
        let rule_limit = json!({"max":1,"per":"agent","window_ms":60_000});
        let state = store
            .ensure_rule_state(
                "owner",
                &rule_input("catchup-script", Some(rule_limit)),
                now - 5,
            )
            .expect("ensure catch-up Run rule");
        let through_seq = state.current_cursor;
        assert!(event.seq <= through_seq);
        let request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "catchup-script",
            event.seq - 1,
            Some(through_seq),
        );
        let actions = [RuleCatchUpAction::command(
            event.seq,
            run_request(&event, "catchup-script", state.revision),
        )];
        let admitted = store
            .admit_rule_catch_up("owner", &request, &state, through_seq, &actions, now)
            .expect("admit event-bound catch-up Run");
        assert_eq!(admitted.parent.verb, "rule.catch_up");
        assert_eq!(admitted.children.len(), 1);
        let run = admitted.children[0].command.clone();
        assert!(run.target.is_none());
        let run_cause = command_cause_tx(&store.connection, &run.id)
            .expect("read catch-up Run cause")
            .expect("catch-up Run has cause");
        assert_eq!(run_cause.rule.as_deref(), Some("catchup-script"));
        assert_eq!(run_cause.depth, 2);
        let parent_cause = command_cause_tx(&store.connection, &admitted.parent.id)
            .expect("read catch-up parent cause")
            .expect("catch-up parent has cause");
        assert_eq!(parent_cause.depth, 1);

        let run = dispatch_run(&mut store, &run, now + 1);
        let output = store
            .admit_script_output(
                &run.id,
                vec![stop_action(&agent_id)],
                process_result(core::ScriptCustody::Exact),
                now + 2,
            )
            .expect("admit catch-up Run output under its saved per-Agent limit");
        let output_cause = command_cause_tx(&store.connection, &output.children[0].id)
            .expect("read catch-up output cause")
            .expect("catch-up output has cause");
        assert_eq!(output_cause.rule.as_deref(), Some("catchup-script"));
        assert_eq!(output_cause.depth, 2);

        let replay = store
            .admit_rule_catch_up("owner", &request, &state, through_seq, &actions, now + 3)
            .expect("replay retained catch-up parent and Run child");
        assert!(!replay.is_new);
        assert!(replay.events.is_empty());
        assert_eq!(replay.children[0].command.id, run.id);
        assert_eq!(
            replay.children[0].command.outcome.as_ref().unwrap()["process"]["custody"],
            "exact"
        );

        let later_event = insert_event(&mut store, now + 4, Some((agent_num, 1)), None);
        let current = store
            .get_rule_state("owner", "catchup-script")
            .expect("read live cursor")
            .expect("catch-up rule remains stored");
        let advanced = store
            .advance_rule_cursor(
                "owner",
                &current,
                later_event.seq,
                RuleCursorOutcome::NoMatch,
                now + 5,
            )
            .expect("advance live cursor past the new event");
        let next_request = core::api::RuleCatchUpRequest::new(
            next_request_id.to_string(),
            "catchup-script",
            through_seq,
            Some(later_event.seq),
        );
        let next_actions = [RuleCatchUpAction::command(
            later_event.seq,
            run_request(&later_event, "catchup-script", advanced.value.revision),
        )];
        let blocked = store
            .admit_rule_catch_up(
                "owner",
                &next_request,
                &advanced.value,
                later_event.seq,
                &next_actions,
                now + 6,
            )
            .expect_err("open output below the Run parent blocks new catch-up work");
        assert_eq!(blocked.code, "bad_state");
    }

    #[test]
    fn catch_up_run_rejects_forged_event_and_dispatch_outside_parent_range() {
        let mut store = store();
        let request_id = ulid::Ulid::new();
        let valid_request_id = ulid::Ulid::new();
        let request_ms = i64::try_from(request_id.timestamp_ms()).expect("timestamp fits");
        let valid_request_ms =
            i64::try_from(valid_request_id.timestamp_ms()).expect("timestamp fits");
        let now = request_ms.max(valid_request_ms) + 100;
        let event1 = insert_event(&mut store, now - 10, None, None);
        let event2 = insert_event(&mut store, now - 9, None, None);
        let event3 = insert_event(&mut store, now - 8, None, None);
        let state = store
            .ensure_rule_state("owner", &rule_input("catchup-forgery", None), now - 7)
            .expect("ensure catch-up rule");
        let through_seq = state.current_cursor;
        assert!(event3.seq <= through_seq);

        let mut forged_event = run_request(&event1, "catchup-forgery", state.revision);
        forged_event.args["context"]["event"]["data"]["result"] = json!("forged");
        let mut outside_range = run_request(&event2, "catchup-forgery", state.revision);
        outside_range.args["context"]["event"]["seq"] = json!(through_seq + 1);
        let mut wrong_rule = run_request(&event3, "catchup-forgery", state.revision);
        wrong_rule.args["context"]["rule"]["id"] = json!("another-rule");
        let rejected_request = core::api::RuleCatchUpRequest::new(
            request_id.to_string(),
            "catchup-forgery",
            event1.seq - 1,
            Some(through_seq),
        );
        let rejected_actions = [
            RuleCatchUpAction::command(event1.seq, forged_event),
            RuleCatchUpAction::command(event2.seq, outside_range),
            RuleCatchUpAction::command(event3.seq, wrong_rule),
        ];
        let rejected = store
            .admit_rule_catch_up(
                "owner",
                &rejected_request,
                &state,
                through_seq,
                &rejected_actions,
                now,
            )
            .expect("persist deterministic catch-up binding refusals");
        assert!(rejected.children.is_empty());
        assert_eq!(
            rejected.parent.outcome.as_ref().unwrap()["children"],
            json!([])
        );
        let refusals = rejected
            .events
            .iter()
            .filter(|event| {
                event.kind == "rule.rejected" && event.data["code"] == "rule_action_binding_invalid"
            })
            .count();
        assert_eq!(refusals, 3);

        let valid_request = core::api::RuleCatchUpRequest::new(
            valid_request_id.to_string(),
            "catchup-forgery",
            event1.seq,
            Some(through_seq),
        );
        let valid_actions = [RuleCatchUpAction::command(
            event2.seq,
            run_request(&event2, "catchup-forgery", state.revision),
        )];
        let valid = store
            .admit_rule_catch_up(
                "owner",
                &valid_request,
                &state,
                through_seq,
                &valid_actions,
                now + 1,
            )
            .expect("admit valid in-range catch-up Run");
        let run = valid.children[0].command.clone();
        let mut tampered_args = run.args.clone();
        tampered_args["context"]["event"]["seq"] = json!(through_seq + 1);
        let encoded = serde_json::to_string(&tampered_args).expect("encode modified test row");
        store
            .connection
            .execute(
                "UPDATE commands SET args = ?1 WHERE id = ?2",
                params![encoded, run.id],
            )
            .expect("simulate frozen range corruption");
        let error = store
            .dispatch(&run.id, now + 2)
            .expect_err("Store dispatch rejects Run outside its catch-up parent range");
        assert_eq!(error.code, "source_integrity");
        assert_eq!(
            store
                .get_command(&run.id)
                .expect("read held Run")
                .unwrap()
                .state,
            CommandState::Accepted
        );
    }
}
