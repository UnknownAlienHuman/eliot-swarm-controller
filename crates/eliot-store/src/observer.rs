//! Observe-only command lifecycle. Native history stays private to the reader.
use super::*;
use eliot_core::ObserverBinding;

const SOURCE: &str = "observe";

fn observed_families(harness: &str) -> Result<&'static [&'static str]> {
    match harness {
        "codex" => Ok(&["session", "turn", "message", "tool"]),
        "fake" => Ok(&["session", "turn", "message"]),
        _ => Err(Error::new(
            "unsupported",
            "harness has no implemented observer reader",
        )),
    }
}

pub(super) fn require_controlled(agent: &Agent) -> Result<()> {
    if agent.observe_only || agent.state == AgentState::Unknown {
        return Err(Error::new(
            "not_allowed",
            "observe-only Agents have no native control authority",
        ));
    }
    Ok(())
}

fn require_observer(agent: &Agent, generation: i64) -> Result<()> {
    if !agent.observe_only || agent.origin != core::AgentOrigin::External || agent.own_identity {
        return Err(Error::new(
            "not_allowed",
            "operation requires an external observe-only Agent",
        ));
    }
    if agent.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "observer generation does not match",
        ));
    }
    Ok(())
}

fn check_keys(value: &Value, allowed: &[&str]) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::new("invalid", "observer payload must be an object"))?;
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(Error::new("invalid", "unsupported observer field"));
    }
    Ok(())
}

fn check_nonsecret(value: &Value) -> Result<()> {
    fn secret(value: &Value) -> bool {
        match value {
            Value::Object(object) => object.iter().any(|(key, value)| {
                let key = key.to_ascii_uppercase();
                matches!(
                    key.as_str(),
                    "TOKEN"
                        | "AUTH"
                        | "AUTHORIZATION"
                        | "CREDENTIALS"
                        | "PASSWORD"
                        | "SECRET_ENV"
                        | "TARGET_ENVIRONMENT"
                ) || key.ends_with("_TOKEN")
                    || key.ends_with("_API_KEY")
                    || secret(value)
            }),
            Value::Array(values) => values.iter().any(secret),
            _ => false,
        }
    }
    if contains_secret_marker(value) || secret(value) {
        return Err(Error::new(
            "secret_in_payload",
            "observer metadata must not contain credentials or auth environment",
        ));
    }
    Ok(())
}

fn detach_fenced(connection: &Connection, agent: i64, generation: i64) -> Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM commands WHERE target = ?1 AND generation = ?2 AND verb = 'detach'
         AND state IN ('accepted','queued','dispatched','unknown'))",
        params![agent, generation], |row| row.get(0),
    ).map_err(sql_error)
}

fn pending_detach(connection: &Connection, agent: i64, generation: i64) -> Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM commands WHERE target = ?1 AND generation = ?2 AND verb = 'detach'
         AND state IN ('accepted','queued','dispatched'))",
        params![agent, generation], |row| row.get(0),
    ).map_err(sql_error)
}

fn source_open(connection: &Connection, agent: i64, generation: i64) -> Result<bool> {
    Ok(load_source_state(connection, agent, generation, SOURCE)?
        .is_some_and(|source| source.state == "open"))
}

fn any_source_open(connection: &Connection, agent: i64) -> Result<bool> {
    connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM source_state WHERE agent = ?1 AND source = 'observe' AND state = 'open')",
        [agent], |row| row.get(0),
    ).map_err(sql_error)
}

pub(super) fn require_reader_source(
    connection: &Connection,
    agent: &Agent,
    generation: i64,
) -> Result<()> {
    require_observer(agent, generation)?;
    let id = parse_agent_id(&agent.id)?;
    if !source_open(connection, id, generation)? || detach_fenced(connection, id, generation)? {
        return Err(Error::new(
            "not_allowed",
            "reader custody requires a committed active observation source",
        ));
    }
    Ok(())
}

pub(super) fn admit(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    digest: &[u8],
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    if request.request_id.is_some() || request.expected_turn_id.is_some() {
        return Err(Error::new(
            "invalid",
            "observation does not take turn/request control bindings",
        ));
    }
    if encode_json(request)?.len() > MAX_FRAME_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "observer command exceeds frame bound",
        ));
    }
    let agent = if request.verb == "attach" {
        if request.target.is_some() {
            return Err(Error::new(
                "invalid",
                "observe attach selects an exact native subject, not an Agent target",
            ));
        }
        check_keys(
            &request.args,
            &[
                "profile",
                "harness",
                "native_scope",
                "native_ref",
                "mode",
                "profile_snapshot",
                "project",
            ],
        )?;
        if required_string(&request.args, "mode")? != "observe" {
            return Err(Error::new(
                "unsupported",
                "only observe attachment is implemented",
            ));
        }
        let harness = required_string(&request.args, "harness")?;
        observed_families(&harness)?;
        let profile = required_string(&request.args, "profile")?;
        let scope = required_string(&request.args, "native_scope")?;
        let native_ref = required_string(&request.args, "native_ref")?;
        if scope.chars().any(char::is_control) || native_ref.chars().any(char::is_control) {
            return Err(Error::new(
                "invalid",
                "native subject contains control characters",
            ));
        }
        let snapshot = request
            .args
            .get("profile_snapshot")
            .filter(|value| value.is_object())
            .ok_or_else(|| {
                Error::new("invalid", "observe attach requires a frozen reader profile")
            })?;
        if encode_json(snapshot)?.len() > MAX_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "observer profile exceeds its bound",
            ));
        }
        check_nonsecret(snapshot)?;
        let project =
            optional_string(&request.args, "project")?.unwrap_or_else(|| "external".to_owned());
        require_nonempty(&project, "observer project")?;
        let existing: Option<i64> = tx
            .query_row(
                "SELECT id FROM agents WHERE native_scope = ?1 AND native_ref = ?2",
                params![scope, native_ref],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?;
        if let Some(id) = existing {
            let agent = load_agent_by_num(tx, id)?
                .ok_or_else(|| Error::new("internal", "canonical native subject disappeared"))?;
            if !agent.observe_only {
                return Err(Error::new(
                    "busy",
                    "native subject already belongs to a controlled Agent",
                ));
            }
            require_observer(&agent, agent.generation)?;
            if agent.harness != harness {
                return Err(Error::new(
                    "source_integrity",
                    "canonical subject belongs to another harness",
                ));
            }
            let pending: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM commands WHERE target = ?1 AND generation = ?2
                 AND verb = 'attach' AND state IN ('accepted','queued','dispatched'))",
                    params![id, agent.generation],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if pending || any_source_open(tx, id)? || pending_detach(tx, id, agent.generation)? {
                return Err(Error::new(
                    "busy",
                    "observer or observation transition is still active",
                ));
            }
            if request
                .expected_generation
                .is_some_and(|expected| expected != agent.generation)
            {
                return Err(Error::new(
                    "stale_generation",
                    "observer attach generation changed",
                ));
            }
            agent
        } else {
            if request
                .expected_generation
                .is_some_and(|expected| expected != 0)
            {
                return Err(Error::new(
                    "stale_generation",
                    "new observer starts from generation zero",
                ));
            }
            db!(tx.execute(
                "INSERT INTO agents(generation,project,harness,profile,profile_snapshot,native_scope,native_ref,state,origin,observe_only,created_at,updated_at)
                 VALUES (0,?1,?2,?3,?4,?5,?6,'unknown','external',1,?7,?7)",
                params![project, harness, profile, encode_json(snapshot)?, scope, native_ref, now],
            ));
            let id = tx.last_insert_rowid();
            events.push(insert_daemon_event(tx, now, "agent.created", "agent", Some(id), Some(0),
                json!({"agent_id": format_agent_id(id), "origin": "external", "observe_only": true, "own_identity": false}))?);
            load_agent_by_num(tx, id)?
                .ok_or_else(|| Error::new("internal", "new observer disappeared"))?
        }
    } else {
        check_keys(&request.args, &["mode"])?;
        if request
            .args
            .get("mode")
            .is_some_and(|mode| mode.as_str() != Some("observe"))
        {
            return Err(Error::new(
                "unsupported",
                "detach only closes an observer reader",
            ));
        }
        let target = request
            .target
            .as_deref()
            .ok_or_else(|| Error::new("invalid", "detach requires an Agent target"))?;
        let id = parse_agent_id(target)?;
        let agent = load_agent_by_num(tx, id)?
            .ok_or_else(|| Error::new("not_found", "observer was not found"))?;
        require_observer(&agent, agent.generation)?;
        if request
            .expected_generation
            .is_some_and(|expected| expected != agent.generation)
        {
            return Err(Error::new(
                "stale_generation",
                "observer detach generation changed",
            ));
        }
        if pending_detach(tx, id, agent.generation)? {
            return Err(Error::new("busy", "an observation detach is unresolved"));
        }
        agent
    };
    insert_command(
        tx,
        &CommandInsert {
            id: &request.id,
            principal,
            method: COMMAND_METHOD,
            payload_digest: digest,
            target: Some(parse_agent_id(&agent.id)?),
            expected_generation: Some(agent.generation),
            generation: Some(agent.generation),
            expected_turn_id: None,
            parent_command: None,
            link_kind: None,
            ordinal: None,
            turn_id: None,
            request_id: None,
            verb: &request.verb,
            args: &request.args,
            state: CommandState::Accepted,
            outcome: None,
            created_at: now,
            updated_at: now,
        },
    )?;
    events.push(insert_daemon_event(
        tx,
        now,
        "command.accepted",
        "command",
        Some(parse_agent_id(&agent.id)?),
        Some(agent.generation),
        json!({"command_id": request.id, "verb": request.verb, "mode": "observe"}),
    )?);
    Ok(())
}

pub(super) fn dispatch(
    tx: &Transaction<'_>,
    command: &CommandRecord,
    agent: &Agent,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    require_observer(
        agent,
        command
            .command
            .generation
            .ok_or_else(|| Error::new("invalid", "observer command has no generation"))?,
    )?;
    if command
        .command
        .expected_generation
        .is_some_and(|expected| expected != agent.generation)
    {
        return Err(Error::new(
            "stale_generation",
            "observation expectation changed",
        ));
    }
    let id = parse_agent_id(&agent.id)?;
    let generation = if command.command.verb == "attach" {
        if any_source_open(tx, id)? || pending_detach(tx, id, agent.generation)? {
            return Err(Error::new(
                "busy",
                "observer source or detach fence is active",
            ));
        }
        if agent.native_scope.as_deref()
            != command
                .command
                .args
                .get("native_scope")
                .and_then(Value::as_str)
            || agent.native_ref.as_deref()
                != command
                    .command
                    .args
                    .get("native_ref")
                    .and_then(Value::as_str)
        {
            return Err(Error::new(
                "source_integrity",
                "claimed native subject changed",
            ));
        }
        let next = agent
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::new("bad_state", "observer generation overflow"))?;
        let profile = required_string(&command.command.args, "profile")?;
        let snapshot = command
            .command
            .args
            .get("profile_snapshot")
            .ok_or_else(|| Error::new("invalid", "frozen observer profile is missing"))?;
        db!(tx.execute(
            "UPDATE agents SET generation = ?1, profile = ?2, profile_snapshot = ?3, state = 'unknown',
             token = NULL, active_turn = NULL, pending = '[]', updated_at = ?4
             WHERE id = ?5 AND generation = ?6 AND observe_only = 1",
            params![next, profile, encode_json(snapshot)?, now, id, agent.generation],
        ));
        for family in observed_families(&agent.harness)? {
            db!(tx.execute(
                "INSERT INTO source_owners(agent,generation,family,source,mechanism) VALUES (?1,?2,?3,'observe','native_read')",
                params![id, next, family],
            ));
        }
        db!(tx.execute(
            "INSERT INTO source_state(agent,generation,source,state,health,updated_at) VALUES (?1,?2,'observe','open','ok',?3)",
            params![id, next, now],
        ));
        events.push(insert_daemon_event(tx, now, "source.opened", "source", Some(id), Some(next),
            json!({"source": SOURCE, "mechanism": "native_read", "families": observed_families(&agent.harness)?}))?);
        next
    } else {
        // This fence stops both new attachment and a late attach-result acknowledgement.
        let mut statement = tx
            .prepare(
                "SELECT id FROM commands WHERE target = ?1 AND generation = ?2 AND verb = 'attach'
             AND state IN ('accepted','queued')",
            )
            .map_err(sql_error)?;
        let ids = statement
            .query_map(params![id, agent.generation], |row| row.get::<_, String>(0))
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql_error)?;
        drop(statement);
        for pending in ids {
            update_command_state(
                tx,
                &pending,
                CommandState::Cancelled,
                Some(&json!({"reason": "observer_detach_before_dispatch"})),
                None,
                now,
            )?;
            events.push(insert_daemon_event(
                tx,
                now,
                "command.cancelled",
                "command",
                Some(id),
                Some(agent.generation),
                json!({"command_id": pending}),
            )?);
        }
        unavailable_commands(
            tx,
            id,
            agent.generation,
            "observer_detach_fence",
            None,
            now,
            events,
        )?;
        db!(tx.execute("UPDATE source_state SET health = 'stale', updated_at = ?1 WHERE agent = ?2 AND generation <= ?3 AND source = 'observe' AND state = 'open'",
            params![now, id, agent.generation]));
        let next = agent
            .generation
            .checked_add(1)
            .ok_or_else(|| Error::new("bad_state", "observer generation overflow"))?;
        db!(tx.execute(
            "UPDATE agents SET generation = ?1, state = 'unknown', token = NULL, updated_at = ?2
            WHERE id = ?3 AND generation = ?4 AND observe_only = 1",
            params![next, now, id, agent.generation]
        ));
        next
    };
    update_command_state(
        tx,
        &command.command.id,
        CommandState::Dispatched,
        None,
        Some(generation),
        now,
    )?;
    events.push(insert_daemon_event(
        tx,
        now,
        "command.dispatched",
        "command",
        Some(id),
        Some(generation),
        json!({"command_id": command.command.id, "verb": command.command.verb, "mode": "observe"}),
    )?);
    Ok(())
}

fn check_result(result: &Value, command: &Command, agent: &Agent, state: &str) -> Result<()> {
    if encode_json(result)?.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "observer result exceeds 64 KiB",
        ));
    }
    check_keys(
        result,
        &[
            "agent_id",
            "generation",
            "command_id",
            "native_scope",
            "native_ref",
            "state",
            "observation_state",
            "metadata",
            "coverage",
        ],
    )?;
    if result.get("agent_id").and_then(Value::as_str) != Some(agent.id.as_str())
        || result.get("generation").and_then(Value::as_i64) != Some(agent.generation)
        || result.get("command_id").and_then(Value::as_str) != Some(command.id.as_str())
        || result.get("native_scope").and_then(Value::as_str) != agent.native_scope.as_deref()
        || result.get("native_ref").and_then(Value::as_str) != agent.native_ref.as_deref()
        || result.get("state").and_then(Value::as_str) != Some(state)
        || result.get("observation_state").and_then(Value::as_str) != Some("unknown")
    {
        return Err(Error::new(
            "source_integrity",
            "observer result does not match its exact command/subject/generation",
        ));
    }
    for field in ["metadata", "coverage"] {
        if (state == "attached" || result.get(field).is_some())
            && result.get(field).is_none_or(|value| !value.is_object())
        {
            return Err(Error::new(
                "invalid",
                "observer metadata and coverage must be objects",
            ));
        }
    }
    check_nonsecret(result)
}

fn unavailable_commands(
    tx: &Transaction<'_>,
    agent: i64,
    generation: i64,
    reason: &str,
    except: Option<&str>,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let mut statement = tx.prepare(
        "SELECT id FROM commands WHERE target = ?1 AND generation = ?2 AND verb IN ('attach','detach') AND state = 'dispatched'
         AND (?3 IS NULL OR id <> ?3)",
    ).map_err(sql_error)?;
    let ids = statement
        .query_map(params![agent, generation, except], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(sql_error)?;
    drop(statement);
    for id in ids {
        let outcome = json!({"reason": reason, "mode": "observe", "observation_state": "unknown"});
        update_command_state(tx, &id, CommandState::Unknown, Some(&outcome), None, now)?;
        events.push(insert_daemon_event(
            tx,
            now,
            "command.unknown",
            "command",
            Some(agent),
            Some(generation),
            json!({"command_id": id, "outcome": outcome}),
        )?);
    }
    Ok(())
}

pub(super) fn settle_failure(
    tx: &Transaction<'_>,
    command: &Command,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let id = command
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "observer command has no target"))?;
    let generation = command
        .generation
        .ok_or_else(|| Error::new("invalid", "observer command has no generation"))?;
    let id = parse_agent_id(id)?;
    let agent = load_agent_by_num(tx, id)?
        .ok_or_else(|| Error::new("not_found", "observer was not found"))?;
    if agent.generation != generation {
        return Ok(());
    }
    require_observer(&agent, generation)?;
    db!(tx.execute(
        "UPDATE source_state SET health = 'stale', updated_at = ?1
        WHERE agent = ?2 AND generation <= ?3 AND source = 'observe' AND state = 'open'",
        params![now, id, generation]
    ));
    events.push(insert_daemon_event(
        tx,
        now,
        "observer.unavailable",
        "agent",
        Some(id),
        Some(generation),
        json!({"command_id": command.id, "source": SOURCE, "observation_state": "unknown"}),
    )?);
    Ok(())
}

impl Store {
    pub fn observer_binding(&self, agent: &str, generation: i64) -> Result<ObserverBinding> {
        let id = parse_agent_id(agent)?;
        let agent = load_agent_by_num(&self.connection, id)?
            .ok_or_else(|| Error::new("not_found", "observer was not found"))?;
        require_observer(&agent, generation)?;
        Ok(ObserverBinding {
            agent: agent.id,
            generation,
            harness: agent.harness,
            native_scope: agent
                .native_scope
                .ok_or_else(|| Error::new("source_integrity", "observer scope is missing"))?,
            native_ref: agent
                .native_ref
                .ok_or_else(|| Error::new("source_integrity", "observer reference is missing"))?,
            profile_snapshot: agent.profile_snapshot,
            source: SOURCE.to_owned(),
            checkpoint: load_source_state(&self.connection, id, generation, SOURCE)?
                .and_then(|source| source.cursor),
        })
    }

    pub fn observer_active(&self, agent: &str, generation: i64) -> Result<bool> {
        let id = parse_agent_id(agent)?;
        let current = load_agent_by_num(&self.connection, id)?
            .ok_or_else(|| Error::new("not_found", "Agent was not found"))?;
        Ok(current.observe_only
            && current.generation == generation
            && source_open(&self.connection, id, generation)?
            && !detach_fenced(&self.connection, id, generation)?)
    }

    pub fn settle_observe_attach(
        &mut self,
        command_id: &str,
        snapshot: Value,
        now: i64,
    ) -> Result<Change<Command>> {
        self.settle_observer(command_id, snapshot, true, now)
    }

    pub fn settle_observe_detach(
        &mut self,
        command_id: &str,
        outcome: Value,
        now: i64,
    ) -> Result<Change<Command>> {
        self.settle_observer(command_id, outcome, false, now)
    }

    fn settle_observer(
        &mut self,
        command_id: &str,
        result: Value,
        attaching: bool,
        now: i64,
    ) -> Result<Change<Command>> {
        self.ensure_writer_healthy()?;
        let change = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let record = load_command_record(&tx, command_id)?
                .ok_or_else(|| Error::new("not_found", "observer command was not found"))?;
            if record.command.verb != if attaching { "attach" } else { "detach" } {
                return Err(Error::new("invalid", "wrong observer settlement API"));
            }
            let id = record
                .target
                .ok_or_else(|| Error::new("invalid", "observer command has no Agent"))?;
            let agent = load_agent_by_num(&tx, id)?
                .ok_or_else(|| Error::new("not_found", "observer was not found"))?;
            require_observer(
                &agent,
                record
                    .command
                    .generation
                    .ok_or_else(|| Error::new("invalid", "observer generation is missing"))?,
            )?;
            check_result(
                &result,
                &record.command,
                &agent,
                if attaching { "attached" } else { "detached" },
            )?;
            if record.state == CommandState::Applied {
                if record.command.outcome.as_ref() != Some(&result) {
                    return Err(Error::new(
                        "source_integrity",
                        "observer result changed after acknowledgement",
                    ));
                }
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value: record.command,
                    events: Vec::new(),
                });
            }
            if !matches!(
                record.state,
                CommandState::Dispatched | CommandState::Unknown
            ) {
                return Err(Error::new(
                    "bad_state",
                    "observer command was not dispatched",
                ));
            }
            if attaching
                && (!source_open(&tx, id, agent.generation)?
                    || detach_fenced(&tx, id, agent.generation)?)
            {
                return Err(Error::new("bad_state", "observer is closed or detaching"));
            }
            let mut events = Vec::new();
            if attaching {
                db!(tx.execute(
                    "UPDATE source_state SET health = 'ok', updated_at = ?1
                    WHERE agent = ?2 AND generation = ?3 AND source = 'observe' AND state = 'open'",
                    params![now, id, agent.generation]
                ));
            } else {
                let mut statement = tx.prepare("SELECT generation FROM source_state
                    WHERE agent = ?1 AND generation < ?2 AND source = 'observe' AND state = 'open' ORDER BY generation")
                    .map_err(sql_error)?;
                let generations = statement
                    .query_map(params![id, agent.generation], |row| row.get::<_, i64>(0))
                    .map_err(sql_error)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?;
                drop(statement);
                for generation in generations {
                    unavailable_commands(
                        &tx,
                        id,
                        generation,
                        "observer_detached",
                        None,
                        now,
                        &mut events,
                    )?;
                    db!(tx.execute("UPDATE source_state SET state = 'closed', updated_at = ?1
                        WHERE agent = ?2 AND generation = ?3 AND source = 'observe' AND state = 'open'", params![now, id, generation]));
                    events.push(insert_daemon_event(
                        &tx,
                        now,
                        "source.closed",
                        "source",
                        Some(id),
                        Some(generation),
                        json!({"source": SOURCE}),
                    )?);
                }
            }
            db!(tx.execute("UPDATE agents SET state = 'unknown', updated_at = ?1 WHERE id = ?2 AND generation = ?3 AND observe_only = 1",
                params![now, id, agent.generation]));
            update_command_state(
                &tx,
                command_id,
                CommandState::Applied,
                Some(&result),
                None,
                now,
            )?;
            events.push(insert_daemon_event(
                &tx,
                now,
                if attaching {
                    "observer.attached"
                } else {
                    "observer.detached"
                },
                "agent",
                Some(id),
                Some(agent.generation),
                json!({"command_id": command_id, "observation_state": "unknown", "source": SOURCE}),
            )?);
            events.push(insert_daemon_event(
                &tx,
                now,
                "command.applied",
                "command",
                Some(id),
                Some(agent.generation),
                json!({"command_id": command_id, "outcome": result}),
            )?);
            let value = load_command(&tx, command_id)?
                .ok_or_else(|| Error::new("internal", "observer command disappeared"))?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change { value, events })
        })();
        self.fault_on_persistence_error(&change);
        change
    }

    pub fn mark_observer_unavailable(
        &mut self,
        agent: &str,
        generation: i64,
        reason: &str,
        closed: bool,
        now: i64,
    ) -> Result<Vec<Event>> {
        self.ensure_writer_healthy()?;
        let change = (|| {
            require_nonempty(reason, "observer unavailable reason")?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let id = parse_agent_id(agent)?;
            let current = load_agent_by_num(&tx, id)?
                .ok_or_else(|| Error::new("not_found", "observer was not found"))?;
            require_observer(&current, current.generation)?;
            if generation <= 0
                || generation > current.generation
                || load_source_state(&tx, id, generation, SOURCE)?.is_none()
            {
                return Err(Error::new(
                    "stale_generation",
                    "unavailable reader has no retained observation binding",
                ));
            }
            let mut events = Vec::new();
            unavailable_commands(&tx, id, generation, reason, None, now, &mut events)?;
            if generation == current.generation {
                db!(tx.execute("UPDATE agents SET state = 'unknown', updated_at = ?1 WHERE id = ?2 AND generation = ?3 AND observe_only = 1", params![now,id,generation]));
            }
            let changed = tx.execute(
                "UPDATE source_state SET state = CASE WHEN ?1 THEN 'closed' ELSE state END, health = 'stale', updated_at = ?2
                 WHERE agent = ?3 AND generation = ?4 AND source = 'observe'
                   AND (health <> 'stale' OR (?1 AND state <> 'closed'))",
                params![closed,now,id,generation],
            ).map_err(sql_error)?;
            if changed != 0 || !events.is_empty() {
                events.push(insert_daemon_event(&tx, now, "observer.unavailable", "agent", Some(id), Some(generation),
                    json!({"source": SOURCE, "reason": reason, "closed": closed, "observation_state": "unknown"}))?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(events)
        })();
        self.fault_on_persistence_error(&change);
        change
    }
}
