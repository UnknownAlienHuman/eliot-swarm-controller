//! Pure binding checks for implemented read callers.
//! These checks do not settle commands, repair replies, or interpret their state.

use super::{
    AgentResultRequest, AgentResultResponse, CommandGetRequest, RuleGetRequest, RuleStateView,
    TurnGetRequest,
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
            || response.state == CommandState::Unknown && response.turn_id.is_some()
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

impl TurnGetRequest {
    pub fn validate_response(&self, response: &Turn) -> Result<()> {
        if self.validate().is_err()
            || response.generation < 0
            || response.agent != self.agent
            || response.generation != self.generation
            || response.turn_id != self.turn_id
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
        if self.validate().is_err()
            || response.agent != self.id
            || response.agent_metadata.id != self.id
            || response.generation < 0
            || response.agent_metadata.generation < 0
            || response.generation != expected_generation
            || response.turns.len() > self.limit
            || response
                .turns
                .iter()
                .any(|turn| turn.agent != response.agent || turn.generation != response.generation)
        {
            return Err(binding_mismatch());
        }
        if let Some(turn_id) = &self.turn_id {
            match response.turns.as_slice() {
                [turn] if turn.turn_id == *turn_id => {}
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Agent, AgentOrigin, AgentState, CommandState, StderrSnapshot, StderrSource, TurnState,
    };
    use serde_json::json;

    fn command() -> Command {
        let diagnostic = Error::new("unknown", "dummy uncertain outcome").with_stderr(
            StderrSource::Plugin,
            StderrSnapshot {
                text: "dummy sanitized diagnostic".to_owned(),
                truncated: false,
                eof: true,
                read_error: false,
            },
        );
        Command {
            id: "command-1".to_owned(),
            principal: "owner".to_owned(),
            method: "command.submit".to_owned(),
            verb: "send".to_owned(),
            target: Some("a1".to_owned()),
            expected_generation: Some(2),
            generation: Some(2),
            args: json!({"text": "dummy input"}),
            state: CommandState::Unknown,
            turn_id: None,
            request_id: None,
            outcome: Some(serde_json::to_value(diagnostic).unwrap()),
            created_at: 0,
            updated_at: 0,
            probe: false,
        }
    }

    #[test]
    fn command_probe_false_keeps_legacy_wire_shape() {
        let command = command();
        assert!(!command.probe);
        let legacy = serde_json::to_value(&command).unwrap();
        assert!(legacy.get("probe").is_none());

        let decoded: Command = serde_json::from_value(legacy.clone()).unwrap();
        assert!(!decoded.probe);
        let mut probe = decoded;
        probe.probe = true;
        assert_eq!(serde_json::to_value(probe).unwrap()["probe"], true);
    }

    fn turn(agent: &str, generation: i64, id: &str) -> Turn {
        Turn {
            agent: agent.to_owned(),
            generation,
            turn_id: id.to_owned(),
            state: TurnState::Unknown,
            status: None,
            final_text: None,
            started_at: Some(0),
            completed_at: None,
        }
    }

    #[test]
    fn rule_get_response_binds_rule_and_validates_cursor_state() {
        let request = RuleGetRequest::new("rule-1");
        let response = RuleStateView {
            id: "rule-1".to_owned(),
            project: None,
            revision: 1,
            current_cursor: 12,
            retiring_revision: None,
            retiring_cursor: None,
            retire_through: None,
            enabled: true,
            paused_reason: None,
        };
        request.validate_response(&response).unwrap();

        let wire = serde_json::to_value(&response).unwrap();
        assert_eq!(
            wire,
            json!({
                "id":"rule-1",
                "revision":1,
                "current_cursor":12,
                "enabled":true
            })
        );
        assert_eq!(
            serde_json::from_value::<RuleStateView>(wire).unwrap(),
            response
        );

        let mut invalid = response.clone();
        invalid.id = "rule-2".to_owned();
        assert_unknown(request.validate_response(&invalid));
        invalid = response.clone();
        invalid.revision = -1;
        assert_unknown(request.validate_response(&invalid));
        invalid = response.clone();
        invalid.current_cursor = -1;
        assert_unknown(request.validate_response(&invalid));
        invalid = response.clone();
        invalid.project = Some("project-a".to_owned());
        assert_unknown(request.validate_response(&invalid));
        invalid = response;
        invalid.paused_reason = Some(" \t ".to_owned());
        assert_unknown(request.validate_response(&invalid));
    }

    #[test]
    fn rule_get_validates_complete_retirement_readback_and_keeps_absent_wire_legacy() {
        let request = RuleGetRequest::new("rule-1");
        let state = RuleStateView {
            id: "rule-1".to_owned(),
            project: None,
            revision: 2,
            current_cursor: 18,
            retiring_revision: Some(1),
            retiring_cursor: Some(7),
            retire_through: Some(12),
            enabled: true,
            paused_reason: None,
        };
        request.validate_response(&state).unwrap();
        let wire = serde_json::to_value(&state).unwrap();
        assert_eq!(wire["retiring_revision"], 1);
        assert_eq!(wire["retiring_cursor"], 7);
        assert_eq!(wire["retire_through"], 12);
        assert_eq!(
            serde_json::from_value::<RuleStateView>(wire).unwrap(),
            state
        );

        let mut invalid = state.clone();
        invalid.retiring_cursor = None;
        assert_unknown(request.validate_response(&invalid));
        invalid = state.clone();
        invalid.retiring_cursor = Some(13);
        assert_unknown(request.validate_response(&invalid));
        invalid = state;
        invalid.retiring_revision = Some(2);
        assert_unknown(request.validate_response(&invalid));
    }

    fn request(generation: Option<i64>, turn_id: Option<&str>, limit: usize) -> AgentResultRequest {
        AgentResultRequest {
            id: "a1".to_owned(),
            generation,
            turn_id: turn_id.map(str::to_owned),
            limit,
        }
    }

    fn response(generation: i64, turns: Vec<Turn>) -> AgentResultResponse {
        let mut metadata = Agent::new("a1", "dummy-project", "fake", "fake", 0);
        metadata.generation = 7;
        metadata.origin = AgentOrigin::External;
        metadata.observe_only = true;
        metadata.state = AgentState::Unknown;
        AgentResultResponse {
            agent: "a1".to_owned(),
            generation,
            turns,
            agent_metadata: metadata,
        }
    }

    fn assert_unknown(result: Result<()>) {
        let error = result.expect_err("mismatched binding");
        assert_eq!(error.code, "unknown");
        assert_eq!(
            error.message,
            "read response does not match the requested binding"
        );
        assert!(error.diagnostics.is_empty());
    }

    #[test]
    fn command_binding_preserves_unknown_state_and_diagnostics() {
        let request = CommandGetRequest {
            id: "command-1".to_owned(),
        };
        let mut reply = command();
        let before = reply.clone();
        request.validate_response(&reply).unwrap();
        assert_eq!(reply, before);
        assert_eq!(reply.state, CommandState::Unknown);
        assert!(reply.outcome.as_ref().unwrap()["diagnostics"].is_array());
        reply.id = "dummy-private-other-command".to_owned();
        assert_unknown(request.validate_response(&reply));
    }

    #[test]
    fn command_binding_rejects_contradictory_or_incomplete_turn_coordinates() {
        let request = CommandGetRequest::new("command-1");
        let mut negative = command();
        negative.generation = Some(-1);
        assert_unknown(request.validate_response(&negative));
        let mut unknown_with_turn = command();
        unknown_with_turn.turn_id = Some("turn-1".to_owned());
        assert_unknown(request.validate_response(&unknown_with_turn));

        let mut control = command();
        control.verb = "interrupt".to_owned();
        control.state = CommandState::Dispatched;
        control.turn_id = Some("turn-1".to_owned());
        request.validate_response(&control).unwrap();
        for target in [None, Some(String::new()), Some("  ".to_owned())] {
            let mut reply = control.clone();
            reply.target = target;
            assert_unknown(request.validate_response(&reply));
        }
        let mut missing_generation = control.clone();
        missing_generation.generation = None;
        assert_unknown(request.validate_response(&missing_generation));
        let mut empty_turn = control;
        empty_turn.turn_id = Some("  ".to_owned());
        assert_unknown(request.validate_response(&empty_turn));
    }

    #[test]
    fn command_binding_keeps_selector_parents_and_generation_zero_valid() {
        let request = CommandGetRequest::new("command-1");
        let mut parent = command();
        parent.target = None;
        parent.generation = None;
        parent.turn_id = None;
        request.validate_response(&parent).unwrap();
        let mut pre_native = command();
        pre_native.generation = Some(0);
        request.validate_response(&pre_native).unwrap();
        let mut control = pre_native;
        control.state = CommandState::Dispatched;
        control.turn_id = Some("turn-0".to_owned());
        request.validate_response(&control).unwrap();
    }

    #[test]
    fn turn_binding_requires_exact_agent_generation_and_turn_id() {
        let request = TurnGetRequest {
            agent: "a1".to_owned(),
            generation: 2,
            turn_id: "turn-1".to_owned(),
        };
        let reply = turn("a1", 2, "turn-1");
        let before = reply.clone();
        request.validate_response(&reply).unwrap();
        assert_eq!(reply, before);
        for reply in [
            turn("a2", 2, "turn-1"),
            turn("a1", 3, "turn-1"),
            turn("a1", 2, "turn-2"),
            turn("a1", -1, "turn-1"),
        ] {
            assert_unknown(request.validate_response(&reply));
        }
        let zero = TurnGetRequest {
            generation: 0,
            ..request
        };
        zero.validate_response(&turn("a1", 0, "turn-1")).unwrap();
    }

    #[test]
    fn explicit_historical_and_future_generations_are_not_current_metadata() {
        for generation in [0, 2, 9] {
            let request = request(Some(generation), None, 100);
            let reply = response(generation, vec![turn("a1", generation, "turn-1")]);
            let before = reply.clone();
            request.validate_response(&reply).unwrap();
            assert_eq!(reply, before);
            assert_eq!(reply.agent_metadata.state, AgentState::Unknown);
            assert_eq!(reply.turns[0].state, TurnState::Unknown);
            assert_eq!(reply.agent_metadata.generation, 7);
        }
    }

    #[test]
    fn omitted_generation_binds_to_current_metadata_without_repairing_reply() {
        let request = request(None, None, 100);
        request
            .validate_response(&response(7, vec![turn("a1", 7, "turn-1")]))
            .unwrap();
        let historical = response(2, vec![turn("a1", 2, "turn-1")]);
        let before = historical.clone();
        assert_unknown(request.validate_response(&historical));
        assert_eq!(historical, before);
    }

    #[test]
    fn result_rejects_wrong_ids_generations_mixed_turns_and_count() {
        let request = request(Some(2), None, 1);
        let valid = response(2, vec![turn("a1", 2, "turn-1")]);
        request.validate_response(&valid).unwrap();
        let mut wrong_agent = valid.clone();
        wrong_agent.agent = "a2".to_owned();
        assert_unknown(request.validate_response(&wrong_agent));
        let mut wrong_metadata = valid;
        wrong_metadata.agent_metadata.id = "a2".to_owned();
        assert_unknown(request.validate_response(&wrong_metadata));
        for reply in [
            response(3, vec![turn("a1", 3, "turn-1")]),
            response(-1, vec![]),
            response(2, vec![turn("a2", 2, "turn-1")]),
            response(2, vec![turn("a1", 3, "turn-1")]),
            response(2, vec![turn("a1", 2, "turn-1"), turn("a1", 2, "turn-2")]),
        ] {
            assert_unknown(request.validate_response(&reply));
        }
        let mut negative_metadata = response(2, vec![]);
        negative_metadata.agent_metadata.generation = -1;
        assert_unknown(request.validate_response(&negative_metadata));
        let wider = AgentResultRequest {
            limit: 100,
            ..request
        };
        for mixed in [
            vec![turn("a1", 2, "turn-1"), turn("a2", 2, "turn-2")],
            vec![turn("a1", 2, "turn-1"), turn("a1", 3, "turn-2")],
        ] {
            assert_unknown(wider.validate_response(&response(2, mixed)));
        }
    }

    #[test]
    fn exact_turn_requires_one_matching_row_and_a_valid_limit() {
        let request = request(Some(2), Some("turn-1"), 100);
        request
            .validate_response(&response(2, vec![turn("a1", 2, "turn-1")]))
            .unwrap();
        for reply in [
            response(2, vec![]),
            response(2, vec![turn("a1", 2, "turn-2")]),
            response(2, vec![turn("a1", 2, "turn-1"), turn("a1", 2, "turn-2")]),
            response(2, vec![turn("a1", 2, "turn-1"), turn("a1", 2, "turn-1")]),
        ] {
            assert_unknown(request.validate_response(&reply));
        }
        for limit in [0, 1001] {
            let invalid = AgentResultRequest {
                limit,
                ..request.clone()
            };
            assert_unknown(invalid.validate_response(&response(2, vec![turn("a1", 2, "turn-1")])));
        }
    }

    #[test]
    fn reply_checks_add_no_id_normalization_or_length_quota() {
        for id in [" command-1 ".to_owned(), "x".repeat(8192)] {
            let request = CommandGetRequest { id: id.clone() };
            let mut reply = command();
            reply.id = id;
            request.validate_response(&reply).unwrap();
            reply.id.push('x');
            assert_unknown(request.validate_response(&reply));
        }
    }
}
