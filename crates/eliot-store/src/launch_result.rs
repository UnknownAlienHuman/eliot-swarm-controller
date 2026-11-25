use super::*;
use eliot_core::api::ProjectManagerIdentity;

impl Store {
    pub fn pending_project_check_obligations(&self, after: i64, limit: i64) -> Result<Vec<Event>> {
        let mut statement = self.connection.prepare(
            "SELECT e.seq,e.ts,e.kind,e.agent,e.generation,e.source,e.native_id,e.data,e.subject,e.cause
             FROM project_check_obligations o JOIN events e ON e.seq=o.completion_seq
             WHERE o.state='not_started' AND o.completion_seq>?1
             ORDER BY o.completion_seq LIMIT ?2",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map(params![after, limit.clamp(1, 256)], event_record_from_row)
            .map_err(sql_error)?;
        rows.map(|row| event_from_record(row.map_err(sql_error)?))
            .collect()
    }

    pub fn claim_project_check_obligation(&mut self, event: &Event) -> Result<bool> {
        self.ensure_writer_healthy()?;
        let changed = self.connection.execute(
            "UPDATE project_check_obligations SET state='checker_started'
             WHERE completion_seq=?1 AND state='not_started' AND agent=(SELECT id FROM agents WHERE id=?2)
               AND generation=?3 AND turn_id=?4 AND fingerprint=?5",
            params![event.seq, parse_agent_id(event.agent.as_deref().unwrap_or_default())?, event.generation.unwrap_or_default(), event.data["turn_id"].as_str().unwrap_or_default(), event.data["worktree"]["fingerprint"].as_str().unwrap_or_default()],
        ).map_err(sql_error)?;
        Ok(changed == 1)
    }

    pub fn settle_project_check_obligation(
        &mut self,
        event: &Event,
        outcome: &str,
        now: i64,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        if !matches!(outcome, "unknown" | "stale" | "refused" | "passed") {
            return Err(Error::new(
                "invalid",
                "invalid project check obligation outcome",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        settle_project_check_obligation_tx(&tx, event, outcome, now)?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(())
    }

    pub fn abandon_started_project_check_obligations(&mut self, now: i64) -> Result<()> {
        self.ensure_writer_healthy()?;
        self.connection.execute(
            "UPDATE project_check_obligations SET state='settled',outcome='unknown',settled_at=?1
             WHERE state='checker_started'",
            [now],
        ).map_err(sql_error)?;
        Ok(())
    }

    pub fn has_project_worktree_check_outcome(&self, event: &Event) -> Result<bool> {
        let (Some(agent), Some(generation)) = (event.agent.as_deref(), event.generation) else {
            return Ok(false);
        };
        worktree_check_outcome_exists(
            &self.connection,
            parse_agent_id(agent)?,
            generation,
            event.seq,
        )
    }

    pub fn project_check_unknown_blocks_fingerprint(
        &self,
        agent: &str,
        generation: i64,
        fingerprint: &str,
    ) -> Result<bool> {
        self.connection
            .query_row(
                "SELECT EXISTS(
               SELECT 1 FROM project_check_obligations o
               WHERE o.agent=?1 AND o.generation=?2 AND o.state='settled'
                 AND o.outcome='unknown' AND o.fingerprint=?3
             )",
                params![parse_agent_id(agent)?, generation, fingerprint],
                |row| row.get(0),
            )
            .map_err(sql_error)
    }

    pub fn record_project_worktree_check_outcome(
        &mut self,
        event: &Event,
        project: &str,
        changed_fingerprint: Option<&str>,
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
        if worktree_check_outcome_exists(&tx, agent, generation, retained.seq)? {
            return Ok(empty);
        }
        let mut data = json!({"project": project, "turn_id": turn, "worktree": worktree, "completion_seq": retained.seq});
        let kind = if let Some(fingerprint) = changed_fingerprint {
            data["reason"] = json!("fingerprint_changed");
            data["observed_fingerprint"] = json!(fingerprint);
            "project.worktree.check.refused"
        } else {
            "project.worktree.check.passed"
        };
        let mut outcome = DaemonEventInput::new(
            now,
            kind,
            "project_check",
            Some(agent),
            Some(generation),
            data,
        );
        outcome.cause = retained.cause.clone();
        let outcome = insert_daemon_event_input(&tx, outcome)?;
        let obligation_outcome = if changed_fingerprint.is_some() {
            "refused"
        } else {
            "passed"
        };
        if !settle_project_check_obligation_tx(&tx, &retained, obligation_outcome, now)? {
            let obligation_exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM project_check_obligations WHERE completion_seq=?1)",
                    [retained.seq],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if obligation_exists {
                return Ok(empty);
            }
        }
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value: (),
            events: vec![outcome],
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

fn settle_project_check_obligation_tx(
    tx: &Transaction<'_>,
    event: &Event,
    outcome: &str,
    now: i64,
) -> Result<bool> {
    let (Some(agent), Some(generation), Some(turn)) = (
        event.agent.as_deref(),
        event.generation,
        event.data["turn_id"].as_str(),
    ) else {
        return Ok(false);
    };
    let fingerprint = event.data["worktree"]["fingerprint"]
        .as_str()
        .unwrap_or_default();
    let changed = tx
        .execute(
            "UPDATE project_check_obligations SET state='settled',outcome=?2,settled_at=?3
             WHERE completion_seq=?1 AND agent=?4 AND generation=?5 AND turn_id=?6
               AND fingerprint=?7 AND state IN ('not_started','checker_started')
               AND (?2 != 'passed' OR state='checker_started')",
            params![
                event.seq,
                outcome,
                now,
                parse_agent_id(agent)?,
                generation,
                turn,
                fingerprint
            ],
        )
        .map_err(sql_error)?;
    Ok(changed == 1)
}

pub(super) fn rule_launch_insertion_binding(
    tx: &Transaction<'_>,
    principal: &str,
    definition: &str,
    event_seq: i64,
    request: &SubmitCommand,
) -> Result<Option<bool>> {
    if request.trusted_review_labels.is_some() && request.verb != "inject" {
        return Ok(Some(false));
    }
    if request.verb != "inject" {
        return Ok(None);
    }
    let definition = decode_json(definition)?;
    let action = &definition["do"][0];
    let Some(rule) = action["from_launch"].as_str() else {
        return Ok(request.trusted_review_labels.is_some().then_some(false));
    };
    if action["action"] != "inject" || request.args["point"] != "soonest" {
        return Ok(Some(false));
    }
    let terminal = scripts::load_event_tx(tx, event_seq)?;
    let result = rule_launch_result(tx, principal, rule, action["to"].as_str(), &terminal)?;
    Ok(Some(result.is_some_and(|(origin, recipient)| {
        request.target.as_deref() == Some(recipient.id.as_str())
            && request.expected_generation == Some(recipient.generation)
            && request
                .trusted_review_labels
                .as_ref()
                .is_none_or(|labels| trusted_review_labels_match(labels, &origin))
    })))
}

fn trusted_review_labels_match(labels: &core::TrustedReviewLabels, origin: &Event) -> bool {
    origin
        .data
        .pointer("/worktree/fingerprint")
        .and_then(Value::as_str)
        == Some(labels.worktree_fingerprint.as_str())
}

fn worktree_check_outcome_exists(
    connection: &Connection,
    agent: i64,
    generation: i64,
    completion_seq: i64,
) -> Result<bool> {
    connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM events
          WHERE agent = ?1 AND generation = ?2 AND seq > ?3
            AND kind IN ('project.worktree.check.passed', 'project.worktree.check.refused')
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
