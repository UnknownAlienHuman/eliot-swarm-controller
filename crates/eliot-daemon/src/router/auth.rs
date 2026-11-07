//! Caller authentication and command authority.
//!
//! `authenticate` decodes the daemon credential envelope into a Principal,
//! `authorize_rpc_method` gates Run credentials to `command.get`, and the
//! `authorize_*` helpers scope command submit and read paths to the caller.
//! Rule-action shape validation stays with submission preparation in the
//! parent module; Agent submit policy stays in `hierarchy`.
use super::{
    Principal, Router, hierarchy::authorize_agent_submit, validate_rule_launch_action,
    validate_rule_resume_action,
};
use crate::common::secure_eq;
use eliot_core::{Command, Error, Result, SubmitCommand};
use eliot_plugin::sync::LockExt;
use serde_json::Value;

impl Router {
    pub(super) async fn authenticate(&self, wire_params: Value) -> Result<(Principal, Value)> {
        let mut envelope = wire_params.as_object().cloned().ok_or_else(|| {
            Error::new(
                "invalid",
                "daemon params must contain an authentication envelope",
            )
        })?;
        let auth = envelope
            .remove("_auth")
            .ok_or_else(|| Error::new("not_allowed", "daemon call is missing credentials"))?;
        let payload = envelope
            .remove("_payload")
            .ok_or_else(|| Error::new("invalid", "daemon call is missing its payload"))?;
        if !envelope.is_empty() || !payload.is_object() {
            return Err(Error::new("invalid", "daemon call envelope is malformed"));
        }
        let auth = auth
            .as_object()
            .ok_or_else(|| Error::new("not_allowed", "daemon credentials are malformed"))?;
        let kind = auth.get("kind").and_then(Value::as_str).unwrap_or_default();
        if !auth_fields_are_exact(auth, kind) {
            return Err(Error::new(
                "not_allowed",
                "daemon credential fields are incomplete or conflicting",
            ));
        }
        let credential = auth
            .get("credential")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if credential.is_empty() {
            return Err(Error::new(
                "not_allowed",
                "daemon credentials are incomplete",
            ));
        }
        match kind {
            "owner" => {
                if secure_eq(credential, &self.inner.owner_token) {
                    Ok((Principal::Owner, payload))
                } else {
                    Err(Error::new("not_allowed", "owner credential is invalid"))
                }
            }
            "agent" => {
                let id = auth
                    .get("agent_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let generation = auth
                    .get("generation")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| Error::new("not_allowed", "Agent generation is missing"))?;
                if !self.inner.plugin_host.controls_generation(id, generation) {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation has no live plugin owner",
                    ));
                }
                let stored_token = {
                    let store = self.inner.store.lock().await;
                    store.get_agent_token(id, generation)?
                };
                if !stored_token
                    .as_deref()
                    .is_some_and(|known| secure_eq(credential, known))
                {
                    return Err(Error::new(
                        "not_allowed",
                        "Agent credential is invalid or revoked",
                    ));
                }
                let agent = {
                    let store = self.inner.store.lock().await;
                    store.get_agent(id)?
                }
                .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
                if agent.generation != generation || agent.is_terminal() {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent credential does not match a live generation",
                    ));
                }
                if !self.inner.plugin_host.controls_generation(id, generation) {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation lost its live plugin owner during authentication",
                    ));
                }
                {
                    self.inner
                        .agent_credentials
                        .lock_recover()
                        .insert((id.to_owned(), generation), credential.to_owned());
                    self.inner
                        .secrets
                        .lock_recover()
                        .insert(credential.to_owned());
                }
                Ok((
                    Principal::Agent {
                        id: id.to_owned(),
                        generation,
                    },
                    payload,
                ))
            }
            "run" => {
                let run_command_id = auth
                    .get("run_command_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.trim().is_empty() && !id.contains('\0'))
                    .ok_or_else(|| Error::new("not_allowed", "Run command id is invalid"))?;
                let _saved_principal = self
                    .inner
                    .run_credentials
                    .authenticate(run_command_id, credential)
                    .ok_or_else(|| {
                        Error::new("not_allowed", "Run credential is invalid or revoked")
                    })?;
                Ok((
                    Principal::Run {
                        run_command_id: run_command_id.to_owned(),
                    },
                    payload,
                ))
            }
            _ => Err(Error::new("not_allowed", "daemon principal is unsupported")),
        }
    }
}

pub(super) fn authorize_rpc_method(principal: &Principal, method: &str) -> Result<()> {
    if matches!(principal, Principal::Run { .. }) && method != "command.get" {
        return Err(Error::new(
            "not_allowed",
            "Run credentials may call only command.get",
        ));
    }
    Ok(())
}

fn auth_fields_are_exact(auth: &serde_json::Map<String, Value>, kind: &str) -> bool {
    let fields: &[&str] = match kind {
        "owner" => &["kind", "credential"],
        "agent" => &["kind", "agent_id", "generation", "credential"],
        "run" => &["kind", "run_command_id", "credential"],
        _ => return false,
    };
    auth.len() == fields.len() && fields.iter().all(|field| auth.contains_key(*field))
}

pub(super) fn authorize_command(principal: &Principal, request: &mut SubmitCommand) -> Result<()> {
    if let Principal::ProjectRule { registration } = principal {
        if request.verb == "git.fetch" {
            if !registration
                .actions
                .iter()
                .any(|action| action == "git.fetch")
            {
                return Err(Error::new(
                    "not_allowed",
                    "project rule is not approved for git.fetch",
                ));
            }
            if request.target.is_some()
                || request.expected_generation.is_some()
                || request.expected_turn_id.is_some()
                || request.request_id.is_some()
                || request.probe
            {
                return Err(Error::new(
                    "invalid",
                    "git.fetch command must be targetless and unbound",
                ));
            }
            return Ok(());
        }
        if request.verb == "run" {
            if !registration.actions.iter().any(|action| action == "run") {
                return Err(Error::new(
                    "not_allowed",
                    "project rule is not approved for run",
                ));
            }
            if request.target.is_some()
                || request.expected_generation.is_some()
                || request.expected_turn_id.is_some()
                || request.request_id.is_some()
                || request.probe
            {
                return Err(Error::new(
                    "invalid",
                    "Run command must be targetless and unbound",
                ));
            }
            return Ok(());
        }
        if matches!(request.verb.as_str(), "notify" | "emit") {
            if !registration
                .actions
                .iter()
                .any(|action| action == &request.verb)
            {
                return Err(Error::new(
                    "not_allowed",
                    "project rule is not approved for this action",
                ));
            }
            if request.target.is_some()
                || request.expected_generation.is_some()
                || request.expected_turn_id.is_some()
                || request.request_id.is_some()
                || request.probe
            {
                return Err(Error::new(
                    "invalid",
                    "notify/emit command must be targetless and unbound",
                ));
            }
            return Ok(());
        }
        if request.verb == "launch" {
            return validate_rule_launch_action(request, Some(registration));
        }
        if request.verb == "resume" {
            return validate_rule_resume_action(request, Some(registration));
        }
        if !matches!(
            request.verb.as_str(),
            "stop" | "send" | "steer" | "interrupt" | "approve" | "deny" | "answer"
        ) || !registration
            .actions
            .iter()
            .any(|action| action == &request.verb)
        {
            return Err(Error::new(
                "not_allowed",
                "project rule is not approved for this action",
            ));
        }
        if request.target.is_none() || request.expected_generation.is_none() {
            return Err(Error::new(
                "invalid",
                "project action requires its frozen Agent and generation",
            ));
        }
        if matches!(request.verb.as_str(), "approve" | "deny" | "answer")
            && (request
                .expected_turn_id
                .as_deref()
                .is_none_or(str::is_empty)
                || request.request_id.as_deref().is_none_or(str::is_empty))
        {
            return Err(Error::new(
                "invalid",
                "project request reply requires its captured turn and request",
            ));
        }
        return Ok(());
    }
    if matches!(principal, Principal::Owner) && request.verb == "run" {
        return Err(Error::new(
            "unsupported",
            "standalone Owner Run submission is unsupported",
        ));
    }
    if matches!(principal, Principal::Run { .. }) {
        return Err(Error::new(
            "not_allowed",
            "Run credentials cannot submit commands",
        ));
    }
    let Principal::Agent { id, generation } = principal else {
        return Ok(());
    };
    authorize_agent_submit(id, *generation, request)
}

pub(super) fn authorize_command_read_self(principal: &Principal, command: &Command) -> Result<()> {
    if matches!(principal, Principal::ProjectRule { .. }) {
        return Err(Error::new(
            "not_allowed",
            "project rule authority cannot read commands",
        ));
    }
    if let Principal::Run { run_command_id, .. } = principal
        && command.id.as_str() != run_command_id.as_str()
    {
        return Err(Error::new(
            "not_allowed",
            "Run credentials are scoped to their own command",
        ));
    }
    if let Principal::Agent { id, generation } = principal
        && (command.target.as_deref() != Some(id.as_str())
            || command
                .generation
                .is_some_and(|actual| actual != *generation))
    {
        return Err(Error::new(
            "not_allowed",
            "command is outside the caller's scope",
        ));
    }
    Ok(())
}

pub(super) fn authorize_agent_read_self(
    principal: &Principal,
    id: &str,
    generation: Option<i64>,
) -> Result<()> {
    if matches!(principal, Principal::Run { .. }) {
        return Err(Error::new(
            "not_allowed",
            "Run credentials cannot read Agents",
        ));
    }
    if matches!(principal, Principal::ProjectRule { .. }) {
        return Err(Error::new(
            "not_allowed",
            "project rule authority cannot read Agents",
        ));
    }
    if let Principal::Agent {
        id: own_id,
        generation: own_generation,
    } = principal
        && (id != own_id || generation.is_some_and(|actual| actual != *own_generation))
    {
        return Err(Error::new(
            "not_allowed",
            "Agent read is outside the caller's scope",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::Principal;
    use super::super::run_authorization_tests::command;
    use super::{
        auth_fields_are_exact, authorize_command, authorize_command_read_self, authorize_rpc_method,
    };
    use eliot_core::{SubmitCommand, api::ProjectRegistrationView};
    use serde_json::json;

    #[test]
    fn project_request_replies_require_per_verb_grants_and_keep_exact_bindings() {
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let base = eliot_core::api::ProjectRegistrationView {
            id: "reply-project".to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout,
            profiles: vec!["fake".to_owned()],
            actions: Vec::new(),
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
        };
        let verbs = ["approve", "deny", "answer"];
        for granted in verbs {
            let mut registration = base.clone();
            registration.actions = vec![granted.to_owned()];
            for verb in verbs {
                let mut request = SubmitCommand::new(String::new(), verb);
                request.target = Some("a7".to_owned());
                request.args = if verb == "answer" {
                    json!({"answers":{"q1":{"answers":["Continue"]}}})
                } else {
                    json!({})
                };
                request.expected_generation = Some(7);
                request.expected_turn_id = Some("turn-captured".to_owned());
                request.request_id = Some("request-captured".to_owned());
                let result = authorize_command(
                    &Principal::ProjectRule {
                        registration: registration.clone(),
                    },
                    &mut request,
                );
                if granted == verb {
                    result.expect("each reply verb has its own explicit grant");
                    assert_eq!(request.target.as_deref(), Some("a7"));
                    assert_eq!(request.expected_generation, Some(7));
                    assert_eq!(request.expected_turn_id.as_deref(), Some("turn-captured"));
                    assert_eq!(request.request_id.as_deref(), Some("request-captured"));
                } else {
                    assert_eq!(result.unwrap_err().code, "not_allowed");
                }
            }
        }
    }

    #[test]
    fn project_rule_steer_requires_an_explicit_grant_and_keeps_its_fences() {
        let checkout = std::fs::canonicalize(std::env::temp_dir())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let registration = eliot_core::api::ProjectRegistrationView {
            id: "steer-project".to_owned(),
            checkout: checkout.clone(),
            repo_common_dir: checkout,
            profiles: vec!["fake".to_owned()],
            actions: vec!["steer".to_owned()],
            interpreters: std::collections::BTreeMap::new(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
        };
        let mut request = SubmitCommand::new(String::new(), "steer");
        request.target = Some("a1".to_owned());
        request.args = json!({"text":"follow-up"});
        request.expected_generation = Some(7);
        request.expected_turn_id = Some("turn-captured".to_owned());

        authorize_command(
            &Principal::ProjectRule {
                registration: registration.clone(),
            },
            &mut request,
        )
        .expect("explicit project STEER grant");
        assert_eq!(request.target.as_deref(), Some("a1"));
        assert_eq!(request.expected_generation, Some(7));
        assert_eq!(request.expected_turn_id.as_deref(), Some("turn-captured"));

        let mut revoked = registration;
        revoked.actions = vec!["send".to_owned()];
        let error = authorize_command(
            &Principal::ProjectRule {
                registration: revoked,
            },
            &mut request,
        )
        .unwrap_err();
        assert_eq!(error.code, "not_allowed");
    }

    fn run_principal() -> Principal {
        Principal::Run {
            run_command_id: "run-1".to_owned(),
        }
    }

    #[test]
    fn run_auth_envelope_requires_exact_run_fields() {
        let exact = json!({
            "kind": "run",
            "run_command_id": "run-1",
            "credential": "opaque",
        })
        .as_object()
        .expect("object")
        .clone();
        assert!(auth_fields_are_exact(&exact, "run"));
        let mut conflicting = exact;
        conflicting.insert("agent_id".to_owned(), json!("a1"));
        assert!(!auth_fields_are_exact(&conflicting, "run"));
        assert!(!auth_fields_are_exact(&conflicting, "owner"));
    }

    #[test]
    fn targetless_run_is_project_only_and_requires_the_run_grant() {
        let registration = ProjectRegistrationView {
            id: "sample".to_owned(),
            checkout: "C:\\work\\sample".to_owned(),
            repo_common_dir: "C:\\work".to_owned(),
            profiles: Vec::new(),
            actions: vec!["run".to_owned()],
            interpreters: Default::default(),
            run_env: Vec::new(),
            git: None,
            revision: 1,
        };
        let project = Principal::ProjectRule { registration };
        let mut request = SubmitCommand::new("", "run");
        assert!(authorize_command(&project, &mut request).is_ok());
        request.target = Some("a1".to_owned());
        assert_eq!(
            authorize_command(&project, &mut request).unwrap_err().code,
            "invalid"
        );

        let mut owner_request = SubmitCommand::new("", "run");
        assert_eq!(
            authorize_command(&Principal::Owner, &mut owner_request)
                .unwrap_err()
                .code,
            "unsupported"
        );
        let mut run_request = SubmitCommand::new("", "send");
        assert!(authorize_command(&run_principal(), &mut run_request).is_err());
    }

    #[test]
    fn run_rpc_is_limited_to_own_command_get() {
        let principal = run_principal();
        assert!(authorize_rpc_method(&principal, "command.get").is_ok());
        assert!(authorize_rpc_method(&principal, "command.submit").is_err());
        assert!(authorize_rpc_method(&principal, "command.children").is_err());
        assert!(authorize_command_read_self(&principal, &command("run-1")).is_ok());
        assert!(authorize_command_read_self(&principal, &command("run-2")).is_err());
    }
}
