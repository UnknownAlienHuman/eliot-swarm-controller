//! Pure checks of mutation replies, not permission to retry or settle an effect.
use super::{
    CommandSubmitRequest, CommandSubmitResponse, DaemonShutdownRequest, DaemonShutdownResponse,
    ShutdownMode,
};
use crate::{CommandState, Error, Result};

impl CommandSubmitRequest {
    pub fn validate_response(&self, response: &CommandSubmitResponse) -> Result<()> {
        if self.verb == "git.fetch" {
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
        // Every currently admitted verb resolves a concrete Agent. Generation 0
        // is valid before dispatch or after failed workspace preparation.
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
        let has_linked_input = matches!(self.verb.as_str(), "launch" | "resume")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StderrSnapshot, StderrSource, api::StatusBlocker};
    use serde_json::{Value, json};

    fn request(verb: &str, target: Option<&str>, text: Option<Value>) -> CommandSubmitRequest {
        CommandSubmitRequest {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            target: target.map(str::to_owned),
            verb: verb.to_owned(),
            args: text.map_or_else(|| json!({}), |text| json!({"text":text})),
            expected_generation: None,
            expected_turn_id: None,
            request_id: None,
            probe: false,
        }
    }

    fn response(request: &CommandSubmitRequest) -> CommandSubmitResponse {
        CommandSubmitResponse {
            command_id: request.id.clone(),
            agent: Some("a1".to_owned()),
            generation: Some(0),
            state: CommandState::Accepted,
            outcome: None,
            linked_command: None,
        }
    }

    fn assert_unknown(result: Result<()>) {
        let error = result.expect_err("invalid mutation reply");
        assert_eq!(error.code, "unknown");
        assert_eq!(
            error.message,
            "mutation response does not match the requested binding"
        );
        assert!(error.diagnostics.is_empty());
    }

    #[test]
    fn submit_accepts_retained_states_without_interpreting_outcome() {
        let request = request("send", Some("a1"), Some(json!("dummy input")));
        let diagnostic = Error::new("unknown", "dummy uncertain outcome").with_stderr(
            StderrSource::Plugin,
            StderrSnapshot {
                text: "dummy sanitized diagnostic".to_owned(),
                truncated: false,
                eof: true,
                read_error: false,
            },
        );
        for state in [
            CommandState::Accepted,
            CommandState::Queued,
            CommandState::Dispatched,
            CommandState::Applied,
            CommandState::Rejected,
            CommandState::Failed,
            CommandState::Unknown,
            CommandState::Cancelled,
        ] {
            let mut reply = response(&request);
            reply.state = state;
            reply.outcome = Some(json!({
                "native_result":{"state":"attached","observation_state":"unknown"},
                "diagnostics":diagnostic.diagnostics,
                "turn_id":"dummy-retained-turn"
            }));
            let before = reply.clone();
            request.validate_response(&reply).unwrap();
            assert_eq!(reply, before);
        }
        for outcome in [
            None,
            Some(json!(false)),
            Some(json!(["opaque"])),
            Some(json!("opaque")),
        ] {
            let mut reply = response(&request);
            reply.outcome = outcome;
            let before = reply.clone();
            request.validate_response(&reply).unwrap();
            assert_eq!(reply, before);
        }
    }

    #[test]
    fn submit_keeps_generation_zero_and_both_sides_of_dispatch() {
        for (verb, expected, generations) in [
            ("launch", 0, [0, 1]),
            ("resume", 7, [7, 8]),
            ("attach", 7, [7, 8]),
            ("detach", 7, [7, 8]),
        ] {
            let mut request = request(verb, None, None);
            request.expected_generation = Some(expected);
            for generation in generations {
                for state in [
                    CommandState::Accepted,
                    CommandState::Applied,
                    CommandState::Unknown,
                    CommandState::Failed,
                ] {
                    let mut reply = response(&request);
                    reply.generation = Some(generation);
                    reply.state = state;
                    request.validate_response(&reply).unwrap();
                }
            }
        }
    }

    #[test]
    fn submit_binds_transmitted_id_explicit_target_and_concrete_coordinates() {
        let mut request = request("stop", None, None);
        let mut reply = response(&request);
        reply.agent = Some("a9".to_owned());
        request.validate_response(&reply).unwrap();
        request.target = Some("a9".to_owned());
        request.validate_response(&reply).unwrap();
        request.target = Some("a1".to_owned());
        assert_unknown(request.validate_response(&reply));
        request.target = None;
        for agent in [None, Some(String::new()), Some(" \t".to_owned())] {
            let mut malformed = reply.clone();
            malformed.agent = agent;
            assert_unknown(request.validate_response(&malformed));
        }
        for generation in [None, Some(-1)] {
            let mut malformed = reply.clone();
            malformed.generation = generation;
            assert_unknown(request.validate_response(&malformed));
        }
        reply.command_id = request.id.to_ascii_lowercase();
        assert_unknown(request.validate_response(&reply));
    }

    #[test]
    fn git_fetch_response_requires_exact_targetless_command_binding() {
        let agent_request = request("stop", None, None);
        let request = request("git.fetch", None, None);
        let mut reply = response(&request);
        reply.agent = None;
        reply.generation = None;
        for state in [
            CommandState::Accepted,
            CommandState::Queued,
            CommandState::Dispatched,
            CommandState::Applied,
            CommandState::Rejected,
            CommandState::Failed,
            CommandState::Unknown,
            CommandState::Cancelled,
        ] {
            let mut retained = reply.clone();
            retained.state = state;
            retained.outcome = Some(json!({"opaque_fetch_outcome":true}));
            request.validate_response(&retained).unwrap();
        }

        let mut wrong = reply.clone();
        wrong.command_id = "another-command".to_owned();
        assert_unknown(request.validate_response(&wrong));
        let mut wrong = reply.clone();
        wrong.agent = Some("a1".to_owned());
        assert_unknown(request.validate_response(&wrong));
        let mut wrong = reply.clone();
        wrong.generation = Some(0);
        assert_unknown(request.validate_response(&wrong));
        let mut wrong = reply.clone();
        wrong.linked_command = Some(format!("{}/send", request.id));
        assert_unknown(request.validate_response(&wrong));

        let mut bound = request.clone();
        bound.target = Some("a1".to_owned());
        assert_unknown(bound.validate_response(&reply));
        assert_unknown(agent_request.validate_response(&reply));
    }

    #[test]
    fn linked_input_requires_the_exact_launch_resume_binding() {
        for verb in ["launch", "resume"] {
            let mut request = request(verb, None, Some(json!("dummy input")));
            let mut reply = response(&request);
            reply.state = CommandState::Unknown;
            assert_unknown(request.validate_response(&reply));
            reply.linked_command = Some(format!("{}/send", request.id));
            request.validate_response(&reply).unwrap();
            for linked in [
                "other/send".to_owned(),
                request.id.clone(),
                format!("{}/send/send", request.id),
                format!("{}/send ", request.id),
            ] {
                reply.linked_command = Some(linked);
                assert_unknown(request.validate_response(&reply));
            }
            reply.linked_command = Some(format!("{}/send", request.id));
            for args in [
                json!({}),
                json!({"text":null}),
                json!({"text":false}),
                json!({"text":" "}),
            ] {
                request.args = args;
                assert_unknown(request.validate_response(&reply));
            }
        }
        for verb in ["send", "attach", "stop"] {
            let request = request(verb, Some("a1"), Some(json!("dummy input")));
            let mut reply = response(&request);
            reply.linked_command = Some(format!("{}/send", request.id));
            assert_unknown(request.validate_response(&reply));
        }
    }

    #[test]
    fn launch_resume_valid_text_cannot_return_a_null_link() {
        for verb in ["launch", "resume"] {
            for text in ["dummy input", "  dummy input\n"] {
                let request = request(verb, None, Some(json!(text)));
                for state in [
                    CommandState::Accepted,
                    CommandState::Applied,
                    CommandState::Unknown,
                    CommandState::Failed,
                ] {
                    let mut reply = response(&request);
                    reply.state = state;
                    assert_unknown(request.validate_response(&reply));
                    reply.linked_command = Some(format!("{}/send", request.id));
                    request.validate_response(&reply).unwrap();
                    assert_eq!(request.args["text"], text);
                }
            }
            for text in [None, Some(Value::Null)] {
                let request = request(verb, None, text);
                let reply = response(&request);
                request.validate_response(&reply).unwrap();
            }
        }
    }

    #[test]
    fn shutdown_drain_uses_only_its_retained_blockers() {
        let request = DaemonShutdownRequest::new("dummy-write", ShutdownMode::Drain);
        for blockers in [
            vec![],
            vec![StatusBlocker::CommandUnknown {
                command_id: "dummy-old-command".to_owned(),
            }],
        ] {
            let reply = DaemonShutdownResponse {
                state: if blockers.is_empty() {
                    CommandState::Applied
                } else {
                    CommandState::Dispatched
                },
                mode: ShutdownMode::Drain,
                blockers: Some(blockers),
                store_write_faulted: None,
            };
            let before = reply.clone();
            request.validate_response(&reply).unwrap();
            assert_eq!(reply, before);
            let mut wrong = reply;
            wrong.state = if wrong.state == CommandState::Applied {
                CommandState::Dispatched
            } else {
                CommandState::Applied
            };
            assert_unknown(request.validate_response(&wrong));
        }
    }

    #[test]
    fn shutdown_force_requires_dispatched_without_blockers_and_preserves_fault() {
        let force = DaemonShutdownRequest::new("dummy-write", ShutdownMode::Force);
        for fault in [None, Some(true)] {
            let reply = DaemonShutdownResponse {
                state: CommandState::Dispatched,
                mode: ShutdownMode::Force,
                blockers: None,
                store_write_faulted: fault,
            };
            let before = reply.clone();
            force.validate_response(&reply).unwrap();
            assert_eq!(reply, before);
        }
        let base = DaemonShutdownResponse {
            state: CommandState::Dispatched,
            mode: ShutdownMode::Force,
            blockers: None,
            store_write_faulted: None,
        };
        let mut wrong = base.clone();
        wrong.store_write_faulted = Some(false);
        assert_unknown(force.validate_response(&wrong));
        let mut wrong = base.clone();
        wrong.blockers = Some(vec![]);
        assert_unknown(force.validate_response(&wrong));
        for state in [
            CommandState::Accepted,
            CommandState::Applied,
            CommandState::Unknown,
            CommandState::Failed,
        ] {
            let mut wrong = base.clone();
            wrong.state = state;
            assert_unknown(force.validate_response(&wrong));
        }
    }

    #[test]
    fn shutdown_rejects_wrong_mode_missing_drain_blockers_and_fault_fallthrough() {
        let drain = DaemonShutdownRequest::new("dummy-write", ShutdownMode::Drain);
        let force = DaemonShutdownRequest::new("dummy-write", ShutdownMode::Force);
        let mut reply = DaemonShutdownResponse {
            state: CommandState::Dispatched,
            mode: ShutdownMode::Force,
            blockers: None,
            store_write_faulted: None,
        };
        assert_unknown(drain.validate_response(&reply));
        reply.mode = ShutdownMode::Drain;
        assert_unknown(force.validate_response(&reply));
        assert_unknown(drain.validate_response(&reply));
        reply.state = CommandState::Applied;
        reply.blockers = Some(vec![]);
        for fault in [Some(false), Some(true)] {
            reply.store_write_faulted = fault;
            assert_unknown(drain.validate_response(&reply));
        }
    }

    #[test]
    fn malformed_wire_cannot_enter_shutdown_or_submit_binding_validation() {
        for wire in [
            json!({"state":"applied","mode":"other","blockers":[]}),
            json!({"state":"applied","mode":"drain","blockers":null}),
            json!({"state":"applied","mode":"drain","blockers":{}}),
            json!({"state":"dispatched","mode":"force","store_write_faulted":null}),
            json!({"state":"dispatched","mode":"force","store_write_faulted":"true"}),
        ] {
            assert!(serde_json::from_value::<DaemonShutdownResponse>(wire).is_err());
        }
        let request = request("launch", None, None);
        let wire = serde_json::to_value(response(&request)).unwrap();
        for field in [
            "command_id",
            "agent",
            "generation",
            "state",
            "outcome",
            "linked_command",
        ] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<CommandSubmitResponse>(missing).is_err());
        }
    }
}
