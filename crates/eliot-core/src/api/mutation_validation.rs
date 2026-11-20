//! Pure checks of mutation replies, not permission to retry or settle an effect.
use super::{
    CommandSubmitRequest, CommandSubmitResponse, DaemonShutdownRequest, DaemonShutdownResponse,
    ShutdownMode,
};
use crate::{CommandState, Error, Result, is_start_verb};

impl CommandSubmitRequest {
    pub fn validate_response(&self, response: &CommandSubmitResponse) -> Result<()> {
        if matches!(self.verb.as_str(), "git.fetch" | "notify" | "emit") {
            if self.validate().is_err()
                || response.command_id != self.id
                || response.agent.is_some()
                || response.generation.is_some()
                || response.linked_command.is_some()
            {
                return Err(binding_mismatch());
            }
            return Ok(());
        }
        // A selector parent is targetless: no concrete Agent, no generation,
        // no linked input. Its members read back via command.children.
        if self.selector.is_some() {
            if response.command_id != self.id
                || response.agent.is_some()
                || response.generation.is_some()
                || response.linked_command.is_some()
            {
                return Err(binding_mismatch());
            }
            return Ok(());
        }
        // Every other currently admitted verb resolves a concrete Agent.
        // Generation 0 is valid before dispatch or after failed workspace
        // preparation.
        if response.command_id != self.id
            || response
                .agent
                .as_ref()
                .is_none_or(|agent| agent.trim().is_empty())
            || response.generation.is_none_or(|generation| generation < 0)
            || self
                .target
                .as_ref()
                .is_some_and(|target| response.agent.as_ref() != Some(target))
        {
            return Err(binding_mismatch());
        }
        let has_linked_input = is_start_verb(self.verb.as_str())
            && self
                .args
                .get("text")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|text| !text.trim().is_empty());
        // Store creates this child in the parent's admission transaction and
        // retains it even when cancelled. Valid input cannot lose its link.
        match (&response.linked_command, has_linked_input) {
            (Some(linked), true) if linked.strip_suffix("/send") == Some(self.id.as_str()) => {}
            (None, false) => {}
            _ => return Err(binding_mismatch()),
        }
        // Retained replies may contain any CommandState and an opaque outcome.
        // In particular, a generation-changing command can be read on either
        // side of dispatch, independently of the Agent's current generation.
        Ok(())
    }
}

impl DaemonShutdownRequest {
    pub fn validate_response(&self, response: &DaemonShutdownResponse) -> Result<()> {
        if response.mode != self.mode {
            return Err(binding_mismatch());
        }
        match self.mode {
            ShutdownMode::Drain => {
                let Some(blockers) = &response.blockers else {
                    return Err(binding_mismatch());
                };
                // A retained reply written before the drain report existed
                // carries no `drain`; exact retries replay it unchanged.
                if response
                    .drain
                    .as_ref()
                    .is_some_and(|drain| drain.validate().is_err())
                {
                    return Err(binding_mismatch());
                }
                let expected_state = if blockers.is_empty() {
                    CommandState::Applied
                } else {
                    CommandState::Dispatched
                };
                if response.state != expected_state || response.store_write_faulted.is_some() {
                    return Err(binding_mismatch());
                }
            }
            ShutdownMode::Force => {
                if response.state != CommandState::Dispatched
                    || response.blockers.is_some()
                    || response.drain.is_some()
                    || response.store_write_faulted == Some(false)
                {
                    return Err(binding_mismatch());
                }
            }
        }
        // These are the retained write's blockers/state, not current daemon
        // status, and even Applied is not evidence that the daemon has exited.
        Ok(())
    }
}

fn binding_mismatch() -> Error {
    Error::new(
        "unknown",
        "mutation response does not match the requested binding",
    )
}
