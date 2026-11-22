//! Pure binding checks for implemented read callers.
//! These checks do not settle commands, repair replies, or interpret their state.

use std::collections::HashSet;

use super::{
    AgentResultRequest, AgentResultResponse, CommandChildrenRequest, CommandChildrenResponse,
    CommandGetRequest, RuleGetRequest, RuleStateView, TurnGetRequest,
};
use crate::{Command, CommandState, Error, Result, Turn};

impl RuleGetRequest {
    pub fn validate_response(&self, response: &RuleStateView) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.project != self.project
            || response.validate().is_err()
        {
            return Err(binding_mismatch());
        }
        Ok(())
    }
}

impl CommandGetRequest {
    pub fn validate_response(&self, response: &Command) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.generation.is_some_and(|generation| generation < 0)
            || response.config_revision < 0
            || response.state == CommandState::Unknown && response.turn_id.is_some()
            || response.config.as_ref().is_some_and(|config| {
                config.validate().is_err() || config.revision != response.config_revision
            })
        {
            return Err(binding_mismatch());
        }
        if let Some(turn_id) = &response.turn_id
            && (turn_id.trim().is_empty()
                || response
                    .target
                    .as_ref()
                    .is_none_or(|target| target.trim().is_empty())
                || response.generation.is_none())
        {
            return Err(binding_mismatch());
        }
        Ok(())
    }
}

impl CommandChildrenRequest {
    pub fn validate_response(&self, response: &CommandChildrenResponse) -> Result<()> {
        if self.validate().is_err() || response.children.len() > self.limit {
            return Err(binding_mismatch());
        }
        for child in &response.children {
            CommandGetRequest::new(child.id.clone()).validate_response(child)?;
        }
        let expected_next = response.children.last().map(|child| child.id.as_str());
        if response.next.as_deref() != expected_next {
            return Err(binding_mismatch());
        }
        Ok(())
    }
}

impl TurnGetRequest {
    pub fn validate_response(&self, response: &Turn) -> Result<()> {
        if self.validate().is_err()
            || response.generation < 0
            || response.config_revision < 0
            || response.agent != self.agent
            || response.generation != self.generation
            || response.turn_id != self.turn_id
            || response.config.as_ref().is_some_and(|config| {
                config.validate().is_err() || config.revision != response.config_revision
            })
        {
            return Err(binding_mismatch());
        }
        Ok(())
    }
}

impl AgentResultRequest {
    pub fn validate_response(&self, response: &AgentResultResponse) -> Result<()> {
        let expected_generation = self
            .generation
            .unwrap_or(response.agent_metadata.generation);
        let mut turn_keys = HashSet::new();
        let upper_generation = self.before.as_ref().map_or(response.generation, |before| {
            turn_keys.insert((before.generation, before.turn_id.as_str()));
            before.generation
        });
        if self.validate().is_err()
            || response.agent != self.id
            || response.agent_metadata.id != self.id
            || response.generation < 0
            || response.agent_metadata.generation < 0
            || response.generation != expected_generation
            || upper_generation > response.generation
            || response.turns.len() > self.limit
            || response.turns.iter().any(|turn| {
                turn.agent != response.agent
                    || turn.generation < 0
                    || turn.generation > upper_generation
                    || !turn_keys.insert((turn.generation, turn.turn_id.as_str()))
                    || self
                        .generation
                        .is_some_and(|generation| turn.generation != generation)
                    || turn.config_revision < 0
                    || turn.config.as_ref().is_some_and(|config| {
                        config.validate().is_err() || config.revision != turn.config_revision
                    })
            })
        {
            return Err(binding_mismatch());
        }
        if response
            .turns
            .windows(2)
            .any(|pair| pair[0].generation < pair[1].generation)
        {
            return Err(binding_mismatch());
        }
        let mut generations = response
            .turns
            .iter()
            .map(|turn| turn.generation)
            .collect::<Vec<_>>();
        if self.before.is_none()
            && self.turn_id.is_none()
            && response.generation > 0
            && response.generation == response.agent_metadata.generation
        {
            generations.insert(0, response.generation);
        }
        generations.dedup();
        if !response
            .generation_boundaries
            .iter()
            .map(|boundary| boundary.generation)
            .eq(generations)
            || response.next.as_ref().is_some_and(|next| {
                self.turn_id.is_some()
                    || next.validate().is_err()
                    || response.turns.last().is_none_or(|turn| {
                        turn.generation != next.generation || turn.turn_id != next.turn_id
                    })
                    || self.before.as_ref().is_some_and(|before| {
                        (next.generation, next.insertion_seq)
                            >= (before.generation, before.insertion_seq)
                    })
            })
        {
            return Err(binding_mismatch());
        }
        if let Some(turn_id) = &self.turn_id {
            match response.turns.as_slice() {
                [turn] if turn.turn_id == *turn_id && turn.generation == expected_generation => {}
                _ => return Err(binding_mismatch()),
            }
        }
        Ok(())
    }
}

fn binding_mismatch() -> Error {
    Error::new(
        "unknown",
        "read response does not match the requested binding",
    )
}
