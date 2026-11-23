//! Indexed readback of the latest Owner stop/interrupt failure.

use super::*;
use rusqlite::{OptionalExtension, params};

impl Store {
    /// Read the latest failed or unconfirmed requested Owner control result
    /// for this generation, including the Agent's member of a tree stop.
    pub fn latest_owner_control_failure(&self, agent: &Agent) -> Result<Option<Command>> {
        let agent_num = parse_agent_id(&agent.id)?;
        let command_id = self
            .connection
            .query_row(
                "SELECT id FROM commands
                 WHERE target = ?1 AND generation = ?2 AND principal = 'owner'
                   AND (parent_command IS NULL OR link_kind = 'selector')
                   AND verb IN ('stop', 'interrupt')
                   AND state IN ('applied', 'rejected', 'failed', 'unknown')
                 ORDER BY created_at DESC, rowid DESC LIMIT 1",
                params![agent_num, agent.generation],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(sql_error)?;
        let Some(command_id) = command_id else {
            return Ok(None);
        };
        let command = self.get_command(&command_id)?.ok_or_else(|| {
            Error::new(
                "source_integrity",
                "latest retained control result disappeared during read",
            )
        })?;
        if command.target.as_deref() != Some(agent.id.as_str())
            || command.generation != Some(agent.generation)
        {
            return Err(Error::new(
                "source_integrity",
                "latest retained control result has a mismatched Agent binding",
            ));
        }
        if command.state == eliot_core::CommandState::Applied {
            return Ok(None);
        }
        if !matches!(
            command.state,
            eliot_core::CommandState::Rejected
                | eliot_core::CommandState::Failed
                | eliot_core::CommandState::Unknown
        ) {
            return Err(Error::new(
                "source_integrity",
                "latest retained control result has an unexpected state",
            ));
        }
        Ok(Some(command))
    }
}
