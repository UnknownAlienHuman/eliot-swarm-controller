//! Delegated remote manager authority over cloud MCP (#657 item 6).
//!
//! A verified remote call whose connector URL selects a configured
//! `[cloud_mcp.managers]` grant authenticates as
//! [`Principal::RemoteManager`]. Its only scope is the project whose current
//! assignment names that grant revision; the Store owns that assignment and
//! rechecks it when a command is admitted and again before dispatch. Here the
//! Router narrows the callable methods and command shapes, and confines reads
//! to that project. Owner mode, the default, never enters this module.
use super::*;
use eliot_core::api::REMOTE_MANAGER_VERBS;
use eliot_store::Store;

/// Methods a delegated remote manager may call. Everything else is refused at
/// the RPC gate, so no Owner-only path sees this principal.
pub(super) const REMOTE_MANAGER_METHODS: [&str; 9] = [
    "api.identity",
    "api.methods",
    "api.help",
    "command.submit",
    "command.get",
    "agent.get",
    "agent.list",
    "agent.result",
    "project.assignment.get",
];

/// The project a delegated remote manager currently manages; `None` for
/// every other principal. A grant without a current assignment is refused.
pub(super) fn remote_project(store: &Store, principal: &Principal) -> Result<Option<String>> {
    match principal {
        Principal::RemoteManager { .. } => store.remote_manager_project(&principal_name(principal)),
        _ => Ok(None),
    }
}

/// Shape gate before preparation: one of the manager verbs, one exact Agent
/// target inside the project, or a root launch whose workspace defaults to
/// the project's registered checkout. Store admission rechecks the project,
/// the approved profile and the assignment transactionally.
pub(super) fn authorize_remote_submit(
    store: &Store,
    principal: &Principal,
    request: &mut SubmitCommand,
) -> Result<()> {
    let Some(project) = remote_project(store, principal)? else {
        return Ok(());
    };
    if !REMOTE_MANAGER_VERBS.contains(&request.verb.as_str()) {
        return Err(Error::new(
            "not_allowed",
            "a remote manager may launch, send, steer, interrupt or stop only",
        ));
    }
    if request.selector.is_some() {
        return Err(Error::new(
            "not_allowed",
            "a remote manager command names one Agent",
        ));
    }
    if request.verb == "launch" {
        let args = request
            .args
            .as_object_mut()
            .ok_or_else(|| Error::new("invalid", "launch args must be an object"))?;
        if args.contains_key("parent") || args.contains_key("chat") {
            return Err(Error::new(
                "not_allowed",
                "a remote manager launches root Agents of its project",
            ));
        }
        if !args.contains_key("workspace") {
            let registration = store
                .get_project_registration("owner", &project)?
                .ok_or_else(|| {
                    Error::new("not_allowed", "remote manager project is unregistered")
                })?;
            args.insert("workspace".to_owned(), Value::String(registration.checkout));
        }
        return Ok(());
    }
    let target = request
        .target
        .as_deref()
        .ok_or_else(|| Error::new("invalid", "a remote manager command needs an Agent target"))?;
    require_project_agent(store, &project, target)
}

/// Agent and command reads stay inside the remote manager's project.
pub(super) fn authorize_remote_agent_read(
    store: &Store,
    principal: &Principal,
    id: &str,
) -> Result<()> {
    let project = remote_project(store, principal)?
        .ok_or_else(|| Error::new("internal", "remote read without a remote principal"))?;
    require_project_agent(store, &project, id)
}

fn require_project_agent(store: &Store, project: &str, id: &str) -> Result<()> {
    let agent = store
        .get_agent(id)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {id} was not found")))?;
    if agent.project != project || agent.chat || agent.observe_only {
        return Err(Error::new(
            "not_allowed",
            "Agent is outside the remote manager's project",
        ));
    }
    Ok(())
}
