use super::*;

impl Store {
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

    /// Follow the linked initial SEND to the originating event in the same rule namespace.
    pub fn rule_launch_trigger(
        &self,
        principal: &str,
        rule: &str,
        terminal: &Event,
    ) -> Result<Option<Event>> {
        let Some(cause) = terminal
            .cause
            .as_ref()
            .filter(|cause| cause.rule.as_deref() == Some(rule))
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
        self.connection
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
}
