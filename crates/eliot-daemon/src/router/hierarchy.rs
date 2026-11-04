//! Controller-launched hierarchy enforcement for the Router (L5.3 part 1).
//!
//! Agent submit scope beyond self, launch project inheritance, selector
//! child scheduling, and Agent read scope. Store admission re-checks
//! launch-parent and selector scope transactionally; this module is the
//! Router half for targeted verbs and reads.

use super::*;
use eliot_core::AgentControl;
use eliot_store::Store;

/// Walk cap for parent-chain scope checks; creation depth is fanout-limited,
/// so this only defends against legacy rows.
const SCOPE_WALK_CAP: usize = 4096;

/// Whether `target_id` is in `caller`'s frozen scope: itself, its subtree
/// for tree control, or its project for project control.
fn agent_in_scope(store: &Store, caller: &Agent, target_id: &str) -> Result<bool> {
    if target_id == caller.id {
        return Ok(true);
    }
    match caller.control {
        AgentControl::Slf => Ok(false),
        AgentControl::Project => {
            let target = store.get_agent(target_id)?;
            Ok(target.is_some_and(|agent| agent.project == caller.project))
        }
        AgentControl::Tree => {
            let mut current = target_id.to_owned();
            for _ in 0..SCOPE_WALK_CAP {
                if current == caller.id {
                    return Ok(true);
                }
                let Some(agent) = store.get_agent(&current)? else {
                    return Ok(false);
                };
                match agent.parent {
                    Some(parent) => current = parent,
                    None => return Ok(false),
                }
            }
            Ok(false)
        }
    }
}

fn caller_row(store: &Store, id: &str) -> Result<Agent> {
    store
        .get_agent(id)?
        .ok_or_else(|| Error::new("not_allowed", "command is outside the caller's scope"))
}

/// Submit scope for Agent principals past `authorize_command`. Targeted
/// verbs honor frozen control and fill a missing expected generation from
/// the target row, mirroring the self case; the Store revalidates at
/// admission. Launch parents and selector members are enforced
/// transactionally at Store admission instead. A missing target row passes
/// through so the Store reports `not_found` at admission.
pub(super) fn authorize_agent_submit_scope(
    store: &Store,
    principal: &Principal,
    request: &mut SubmitCommand,
) -> Result<()> {
    let Principal::Agent { id, .. } = principal else {
        return Ok(());
    };
    if request.verb == "launch" || request.selector.is_some() {
        return Ok(());
    }
    let Some(target) = request.target.as_deref() else {
        return Ok(());
    };
    if target == id {
        return Ok(());
    }
    let caller = caller_row(store, id)?;
    let Some(target_agent) = store.get_agent(target)? else {
        return Ok(());
    };
    if !agent_in_scope(store, &caller, target)? {
        return Err(Error::new(
            "not_allowed",
            "command is outside the caller's scope",
        ));
    }
    if request.expected_generation.is_none() {
        request.expected_generation = Some(target_agent.generation);
    }
    Ok(())
}

/// Launch project inheritance: a child takes its parent's project label.
/// Runs after `normalize_launch` set the workspace default and before the
/// profile freeze and workspace preparation read the project. The Store
/// re-derives the same label authoritatively at admission.
pub(super) fn inherit_launch_project(store: &Store, request: &mut SubmitCommand) -> Result<()> {
    if request.verb != "launch" {
        return Ok(());
    }
    let Some(parent) = request.args.get("parent").and_then(Value::as_str) else {
        return Ok(());
    };
    let parent_agent = store
        .get_agent(parent)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {parent} was not found")))?;
    request.args["project"] = Value::String(parent_agent.project);
    Ok(())
}

/// Fresh child ids of a newly admitted selector parent, from its retained
/// outcome. New children are accepted or queued by construction.
pub(super) fn selector_child_ids(parent: &Command) -> Vec<String> {
    parent
        .outcome
        .as_ref()
        .and_then(|outcome| outcome.get("children"))
        .and_then(Value::as_array)
        .map(|children| {
            children
                .iter()
                .filter_map(|id| id.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Read scope for one command row: the self-only rule first, then frozen
/// control for Agent principals. A targetless command (launch, selector
/// parent) is readable only by its own principal.
pub(super) fn authorize_agent_command_read_scope(
    store: &Store,
    caller_id: &str,
    command: &Command,
) -> Result<()> {
    let caller = caller_row(store, caller_id)?;
    match command.target.as_deref() {
        Some(target) => {
            if agent_in_scope(store, &caller, target)? {
                Ok(())
            } else {
                Err(Error::new(
                    "not_allowed",
                    "command is outside the caller's scope",
                ))
            }
        }
        None => {
            if command.principal == format!("agent:{caller_id}:{}", caller.generation) {
                Ok(())
            } else {
                Err(Error::new(
                    "not_allowed",
                    "command is outside the caller's scope",
                ))
            }
        }
    }
}

/// Read scope for one Agent row: the self-only rule first, then frozen
/// control for Agent principals.
pub(super) fn authorize_agent_read_scope(store: &Store, caller_id: &str, id: &str) -> Result<()> {
    let caller = caller_row(store, caller_id)?;
    if agent_in_scope(store, &caller, id)? {
        Ok(())
    } else {
        Err(Error::new(
            "not_allowed",
            "command is outside the caller's scope",
        ))
    }
}

impl Router {
    /// `authorize_command_read` widened by frozen Agent control. The Store
    /// lock is re-taken only when the self-only rule fails for an Agent
    /// reading past itself; self reads keep their generation errors.
    pub(super) async fn authorize_command_read_scoped(
        &self,
        principal: &Principal,
        command: &Command,
    ) -> Result<()> {
        match authorize_command_read(principal, command) {
            Ok(()) => Ok(()),
            Err(error) => {
                let Principal::Agent { id, .. } = principal else {
                    return Err(error);
                };
                if command.target.as_deref() == Some(id.as_str()) {
                    return Err(error);
                }
                let store = self.inner.store.lock().await;
                authorize_agent_command_read_scope(&store, id, command)
            }
        }
    }

    /// `authorize_agent_read` widened by frozen Agent control. The Store
    /// lock is re-taken only when the self-only rule fails for an Agent
    /// reading past itself; self reads keep their generation errors.
    pub(super) async fn authorize_agent_read_scoped(
        &self,
        principal: &Principal,
        id: &str,
        generation: Option<i64>,
    ) -> Result<()> {
        match authorize_agent_read(principal, id, generation) {
            Ok(()) => Ok(()),
            Err(error) => {
                let Principal::Agent { id: caller_id, .. } = principal else {
                    return Err(error);
                };
                if id == caller_id {
                    return Err(error);
                }
                let store = self.inner.store.lock().await;
                authorize_agent_read_scope(&store, caller_id, id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router() -> Router {
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        Router::new(
            PathBuf::from("synthetic-data-root"),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        )
    }

    fn launch(
        store: &mut Store,
        project: &str,
        parent: Option<&str>,
        control: &str,
        now: i64,
    ) -> String {
        let mut args = json!({
            "project": project,
            "workspace": "synthetic-workspace",
            "profile_snapshot": {"control": control},
        });
        if let Some(parent) = parent {
            args["parent"] = json!(parent);
        }
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: ulid::Ulid::new().to_string(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args,
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .unwrap()
            .command
            .target
            .unwrap()
    }

    fn dispatch_launch(store: &mut Store, agent: &str, now: i64) {
        let launch_id: String = store
            .list_unsettled_commands(100)
            .unwrap()
            .into_iter()
            .find(|command| command.target.as_deref() == Some(agent))
            .unwrap()
            .id;
        store.dispatch(&launch_id, now).unwrap();
    }

    fn agent_principal(id: &str, generation: i64) -> Principal {
        Principal::Agent {
            id: id.to_owned(),
            generation,
        }
    }

    #[test]
    fn agent_launch_defaults_parent_to_self_and_rejects_foreign_parents() {
        let caller = agent_principal("a1", 1);
        let mut request = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "launch");
        request.args = json!({"workspace": "synthetic-workspace", "profile": "fake"});
        authorize_command(&caller, &mut request).unwrap();
        assert_eq!(
            request.args.get("parent").and_then(Value::as_str),
            Some("a1")
        );
        let mut foreign = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "launch");
        foreign.args = json!({
            "workspace": "synthetic-workspace", "profile": "fake", "parent": "a2",
        });
        assert_eq!(
            authorize_command(&caller, &mut foreign).unwrap_err().code,
            "not_allowed"
        );
        let mut resume = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "resume");
        assert_eq!(
            authorize_command(&caller, &mut resume).unwrap_err().code,
            "not_allowed"
        );
    }

    #[test]
    fn agent_submit_scope_honors_frozen_control() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let now = now_ms();
        let manager = launch(&mut store, "scoped", None, "tree", now);
        dispatch_launch(&mut store, &manager, now);
        let leaf = launch(&mut store, "scoped", Some(&manager), "self", now);
        let outsider = launch(&mut store, "elsewhere", None, "self", now);
        let teammate = launch(&mut store, "scoped", None, "project", now);

        // Tree control reaches its subtree and fills the generation.
        let mut scoped = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "send");
        scoped.target = Some(leaf.clone());
        authorize_agent_submit_scope(&store, &agent_principal(&manager, 1), &mut scoped).unwrap();
        assert_eq!(scoped.expected_generation, Some(0));
        // Past the subtree, and for self control past itself, is denied.
        for (caller, target) in [(&manager, &outsider), (&leaf, &manager)] {
            let mut denied = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "stop");
            denied.target = Some(target.clone());
            let caller_generation = if caller == &manager { 1 } else { 0 };
            assert_eq!(
                authorize_agent_submit_scope(
                    &store,
                    &agent_principal(caller, caller_generation),
                    &mut denied,
                )
                .unwrap_err()
                .code,
                "not_allowed"
            );
        }
        // Project control reaches its project but not the subtree rule.
        let mut project = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "stop");
        project.target = Some(manager.clone());
        authorize_agent_submit_scope(&store, &agent_principal(&teammate, 0), &mut project).unwrap();
        // A missing target passes through for the Store's not_found.
        let mut missing = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "stop");
        missing.target = Some("a999".to_owned());
        authorize_agent_submit_scope(&store, &agent_principal(&manager, 1), &mut missing).unwrap();
    }

    #[test]
    fn agent_read_scope_follows_frozen_control() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let now = now_ms();
        let manager = launch(&mut store, "scoped", None, "tree", now);
        let leaf = launch(&mut store, "scoped", Some(&manager), "self", now);
        let outsider = launch(&mut store, "elsewhere", None, "self", now);
        authorize_agent_read_scope(&store, &manager, &leaf).unwrap();
        assert_eq!(
            authorize_agent_read_scope(&store, &manager, &outsider)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            authorize_agent_read_scope(&store, &leaf, &manager)
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }

    #[tokio::test]
    async fn agent_launch_through_the_router_inherits_its_project() {
        let router = router();
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = {
            let mut store = router.inner.store.lock().await;
            let manager = launch(&mut store, "inherited-project", None, "tree", now_ms());
            dispatch_launch(&mut store, &manager, now_ms());
            manager
        };
        let response = router
            .submit(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "verb": "launch",
                    "args": {"workspace": workspace, "profile": "fake"},
                }),
                &agent_principal(&manager, 1),
            )
            .await
            .unwrap();
        let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
        let child = typed.agent.unwrap();
        let store = router.inner.store.lock().await;
        let row = store.get_agent(&child).unwrap().unwrap();
        assert_eq!(row.parent.as_deref(), Some(manager.as_str()));
        assert_eq!(row.project, "inherited-project");
        assert_eq!(row.root, manager);
        assert_eq!(row.depth, 1);
        let _ = std::fs::remove_dir_all(&workspace);
    }

    #[tokio::test]
    async fn owner_stop_selector_through_the_router_expands_and_reads_back() {
        let router = router();
        let now = now_ms();
        let (manager, leaf) = {
            let mut store = router.inner.store.lock().await;
            let manager = launch(&mut store, "fan-out", None, "tree", now);
            dispatch_launch(&mut store, &manager, now);
            let leaf = launch(&mut store, "fan-out", Some(&manager), "self", now);
            dispatch_launch(&mut store, &leaf, now);
            (manager, leaf)
        };
        let parent_id = ulid::Ulid::new().to_string();
        let response = router
            .submit(
                &json!({
                    "id": parent_id,
                    "verb": "stop",
                    "selector": format!("tree:{manager}"),
                    "args": {},
                }),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
        assert_eq!(typed.command_id, parent_id);
        assert_eq!(typed.state, CommandState::Applied);
        let store = router.inner.store.lock().await;
        let parent = store.get_command(&parent_id).unwrap().unwrap();
        let page = store.list_command_children(&parent, None, 100).unwrap();
        assert_eq!(
            page.children
                .iter()
                .map(|child| child.target.clone().unwrap())
                .collect::<Vec<_>>(),
            vec![manager, leaf]
        );
    }

    #[tokio::test]
    async fn manager_reads_its_subtree_but_not_outsiders() {
        let router = router();
        let now = now_ms();
        let (manager, leaf, outsider) = {
            let mut store = router.inner.store.lock().await;
            let manager = launch(&mut store, "scoped", None, "tree", now);
            dispatch_launch(&mut store, &manager, now);
            let leaf = launch(&mut store, "scoped", Some(&manager), "self", now);
            let outsider = launch(&mut store, "elsewhere", None, "self", now);
            (manager, leaf, outsider)
        };
        let caller = agent_principal(&manager, 1);
        let leaf_view: AgentGetResponse = serde_json::from_value(
            router
                .agent_get(&json!({"id": leaf}), &caller)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(leaf_view.agent.id, leaf);
        assert_eq!(
            router
                .agent_get(&json!({"id": outsider}), &caller)
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }
}
