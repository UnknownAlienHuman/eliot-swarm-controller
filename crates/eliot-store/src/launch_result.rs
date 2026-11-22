use super::*;
use eliot_core::api::ProjectManagerIdentity;

impl Store {
    pub fn has_project_worktree_check_pass(&self, event: &Event) -> Result<bool> {
        let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation) else {
            return Ok(false);
        };
        worktree_check_pass_exists(
            &self.connection,
            parse_agent_id(agent)?,
            generation,
            event.seq,
        )
    }

    pub fn record_project_worktree_check_pass(
        &mut self,
        event: &Event,
        project: &str,
        now: i64,
    ) -> Result<Change<()>> {
        self.ensure_writer_healthy()?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let empty = Change {
            value: (),
            events: Vec::new(),
        };
        let retained = scripts::load_event_tx(&tx, event.seq)?;
        let (Some(agent), Some(generation), Some(turn)) = (
            retained.agent.as_deref(),
            retained.generation,
            retained.data["turn_id"].as_str(),
        ) else {
            return Ok(empty);
        };
        let agent = parse_agent_id(agent)?;
        let Some(current) = load_agent_by_num(&tx, agent)? else {
            return Ok(empty);
        };
        let worktree = &retained.data["worktree"];
        if retained != *event
            || retained.kind != "turn.completed"
            || retained.data["outcome"] != "success"
            || retained.data["project_check"] != "required"
            || current.generation != generation
            || current.project != project
            || current.is_terminal()
            || current.policy_opt_out_reason.is_some()
            || current.base_oid.as_deref() != worktree["base"].as_str()
            || retained
                .subject
                .as_ref()
                .and_then(|subject| subject.workspace.as_deref())
                != current.workspace.as_deref()
        {
            return Ok(empty);
        }
        if worktree_check_pass_exists(&tx, agent, generation, retained.seq)? {
            return Ok(empty);
        }
        let mut passed = DaemonEventInput::new(
            now,
            "project.worktree.check.passed",
            "project_check",
            Some(agent),
            Some(generation),
            json!({"project": project, "turn_id": turn, "worktree": worktree, "completion_seq": retained.seq}),
        );
        passed.cause = retained.cause;
        let passed = insert_daemon_event_input(&tx, passed)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: (),
            events: vec![passed],
        })
    }

    pub fn rule_action_trigger(&self, action: &str) -> Result<Option<Event>> {
        self.connection
            .query_row(
                "SELECT e.seq, e.ts, e.kind, e.agent, e.generation, e.source, e.native_id,
                        e.data, e.subject, e.cause
                   FROM commands action
                   JOIN commands firing ON firing.id = action.parent_command
                   JOIN events e ON e.seq = json_extract(firing.args, '$.event_seq')
                  WHERE action.id = ?1 AND action.verb = 'launch' AND action.link_kind = 'rule_action'
                    AND firing.verb = 'rule' AND firing.principal = action.principal",
                [action],
                event_record_from_row,
            )
            .optional()
            .map_err(sql_error)?
            .map(event_from_record)
            .transpose()
    }

    pub fn rule_launch_result(
        &self,
        principal: &str,
        rule: &str,
        to: Option<&str>,
        terminal: &Event,
    ) -> Result<Option<(Event, Agent)>> {
        rule_launch_result(&self.connection, principal, rule, to, terminal)
    }
}

pub(super) fn rule_launch_insertion_binding(
    tx: &Transaction<'_>,
    principal: &str,
    definition: &str,
    event_seq: i64,
    request: &SubmitCommand,
) -> Result<Option<bool>> {
    if request.verb != "inject" {
        return Ok(None);
    }
    let definition = decode_json(definition)?;
    let action = &definition["do"][0];
    let Some(rule) = action["from_launch"].as_str() else {
        return Ok(None);
    };
    if action["action"] != "inject" || request.args["point"] != "soonest" {
        return Ok(Some(false));
    }
    let terminal = scripts::load_event_tx(tx, event_seq)?;
    let result = rule_launch_result(tx, principal, rule, action["to"].as_str(), &terminal)?;
    Ok(Some(result.is_some_and(|(_, recipient)| {
        request.target.as_deref() == Some(recipient.id.as_str())
            && request.expected_generation == Some(recipient.generation)
    })))
}

fn worktree_check_pass_exists(
    connection: &Connection,
    agent: i64,
    generation: i64,
    completion_seq: i64,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM events
          WHERE agent = ?1 AND generation = ?2 AND seq > ?3
            AND kind = 'project.worktree.check.passed'
            AND json_extract(data, '$.completion_seq') = ?3)",
            params![agent, generation, completion_seq],
            |row| row.get(0),
        )
        .map_err(sql_error)
}

fn rule_launch_result(
    connection: &Connection,
    principal: &str,
    rule: &str,
    to: Option<&str>,
    terminal: &Event,
) -> Result<Option<(Event, Agent)>> {
    let Some(origin) = rule_launch_trigger(connection, principal, rule, terminal)? else {
        return Ok(None);
    };
    let author = origin
        .agent
        .as_deref()
        .ok_or_else(|| Error::new("source_integrity", "launch trigger has no author Agent"))?;
    let recipient = if let Some(to) = to {
        load_agent_by_num(connection, parse_agent_id(to)?)?
    } else {
        let author = load_agent_by_num(connection, parse_agent_id(author)?)?
            .ok_or_else(|| Error::new("not_found", "author Agent is no longer retained"))?;
        if let Some(parent) = author.parent {
            load_agent_by_num(connection, parse_agent_id(&parent)?)?
        } else {
            match project_assignment::read_assignment(connection, &author.project)?.manager {
                Some(ProjectManagerIdentity::Local { agent, generation }) => {
                    load_agent_by_num(connection, parse_agent_id(&agent)?)?
                        .filter(|agent| agent.generation == generation)
                }
                _ => None,
            }
        }
    };
    let recipient = recipient
        .ok_or_else(|| Error::new("not_found", "review has no local manager recipient"))?;
    Ok(Some((origin, recipient)))
}

/// Follow the linked initial SEND to the originating event in the same rule namespace.
fn rule_launch_trigger(
    connection: &Connection,
    principal: &str,
    rule: &str,
    terminal: &Event,
) -> Result<Option<Event>> {
    let Some(cause) = terminal
        .cause
        .as_ref()
        .filter(|cause| terminal.kind == "turn.completed" && cause.rule.as_deref() == Some(rule))
    else {
        return Ok(None);
    };
    let (Some(agent), Some(generation), Some(turn)) = (
        terminal.agent.as_deref(),
        terminal.generation,
        terminal.data["turn_id"].as_str(),
    ) else {
        return Ok(None);
    };
    let agent = parse_agent_id(agent)?;
    connection
        .query_row(
            "SELECT e.seq, e.ts, e.kind, e.agent, e.generation, e.source, e.native_id,
                    e.data, e.subject, e.cause
               FROM commands send
               JOIN commands launch ON launch.id = send.parent_command
               JOIN commands firing ON firing.id = launch.parent_command
               JOIN events e ON e.seq = json_extract(firing.args, '$.event_seq')
              WHERE send.id = ?1 AND send.principal = ?2 AND send.verb = 'send'
                AND send.link_kind = 'send' AND send.ordinal = 0 AND send.state = 'applied'
                AND send.target = ?3 AND send.generation = ?4 AND send.turn_id = ?5
                AND launch.principal = ?2 AND launch.verb = 'launch' AND launch.target = ?3
                AND launch.link_kind = 'rule_action' AND launch.ordinal = 0
                AND json_extract(launch.args, '$._rule_worktree_guard') = 1
                AND firing.principal = ?2 AND firing.verb = 'rule'
                AND json_extract(firing.args, '$.rule') = ?6",
            params![cause.command_id, principal, agent, generation, turn, rule],
            event_record_from_row,
        )
        .optional()
        .map_err(sql_error)?
        .map(event_from_record)
        .transpose()
}
