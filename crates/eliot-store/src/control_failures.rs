//! Indexed readback of the latest settled Owner stop/interrupt failure.

use super::*;
use rusqlite::{OptionalExtension, params};

impl Store {
    /// Read one terminal Owner control failure for the Agent's exact generation.
    pub fn latest_owner_control_failure(&self, agent: &Agent) -> Result<Option<Command>> {
        let agent_num = parse_agent_id(&agent.id)?;
        let command_id = self
            .connection
            .query_row(
                "SELECT id FROM commands
                 WHERE target = ?1 AND generation = ?2 AND principal = 'owner'
                   AND verb IN ('stop', 'interrupt')
                   AND state IN ('rejected', 'failed')
                 ORDER BY updated_at DESC, rowid DESC LIMIT 1",
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
                "latest retained control failure disappeared during read",
            )
        })?;
        if command.target.as_deref() != Some(agent.id.as_str())
            || command.generation != Some(agent.generation)
        {
            return Err(Error::new(
                "source_integrity",
                "latest retained control failure has a mismatched Agent binding",
            ));
        }
        Ok(Some(command))
    }
}
