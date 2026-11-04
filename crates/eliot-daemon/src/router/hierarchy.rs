//! Controller-launched hierarchy enforcement for the Router (L5.3 part 1).
//!
//! Agent submit policy, Agent launch confinement, launch project inheritance,
//! selector child scheduling, and Agent read scope. Store admission re-checks
//! launch parents (project, root, controlled parent) and selector scope
//! transactionally; targeted-verb submit scope and read scope are
//! Router-enforced here against the Store's one scope walk.

use super::*;
use eliot_core::AgentControl;
use eliot_store::Store;

/// Agent submit policy past the owner/run/rule gates. Launches default an
/// absent parent to the caller and reject any other parent; selector and
/// non-self targets pass through to the Store-backed scope check in
/// preparation.
pub(super) fn authorize_agent_submit(
    id: &str,
    generation: i64,
    request: &mut SubmitCommand,
) -> Result<()> {
    if request.verb == "launch" {
        // Agents launch only their own children: default the parent to
        // self and reject any other parent.
        if !request.args.is_object() && !request.args.is_null() {
            return Err(Error::new("invalid", "launch args must be an object"));
        }
        match request.args.get("parent") {
            None => {
                request.args["parent"] = Value::String(id.to_owned());
            }
            Some(Value::String(parent)) if parent == id => {}
            Some(_) => {
                return Err(Error::new(
                    "not_allowed",
                    "Agents launch only their own children",
                ));
            }
        }
        return Ok(());
    }
    if !matches!(
        request.verb.as_str(),
        "send" | "stop" | "steer" | "interrupt" | "approve" | "deny" | "answer"
    ) {
        return Err(Error::new(
            "not_allowed",
            "managed Agent credentials cannot launch another Agent",
        ));
    }
    if request.selector.is_some() {
        // Selector member scope is enforced transactionally at Store
        // admission against the snapshotted members.
        return Ok(());
    }
    if request.target.as_deref().is_some_and(|target| target != id) {
        // Non-self targets honor frozen control; scope is checked against
        // the Store later in preparation.
        return Ok(());
    }
    request.target = Some(id.to_owned());
    if request
        .expected_generation
        .is_some_and(|expected| expected != generation)
    {
        return Err(Error::new(
            "stale_generation",
            "command generation does not match the caller",
        ));
    }
    request.expected_generation = Some(generation);
    Ok(())
}

fn control_rank(control: AgentControl) -> u8 {
    match control {
        AgentControl::Slf => 0,
        AgentControl::Tree => 1,
        AgentControl::Project => 2,
    }
}

fn snapshot_without_control(snapshot: &Value) -> Value {
    let mut stripped = snapshot.clone();
    if let Some(object) = stripped.as_object_mut() {
        object.remove("control");
    }
    stripped
}

/// Agent launch confinement (spec coordination.md "Authority": children
/// cannot widen parental authority). Owner launches are unrestricted. An
/// Agent's child reuses the parent's frozen profile snapshot with control
/// narrowed or equal, shares the parent's workspace, and mints no worktree:
/// the spec grants children nothing beyond project/root inheritance. Runs
/// after the profile freeze, before workspace preparation mints anything.
pub(super) fn check_agent_launch_authority(
    store: &Store,
    principal: &Principal,
    request: &SubmitCommand,
) -> Result<()> {
    if !matches!(principal, Principal::Agent { .. }) {
        return Ok(());
    }
    let Some(parent_id) = request.args.get("parent").and_then(Value::as_str) else {
        // No parent: the Store reports the Agent launch as not_allowed.
        return Ok(());
    };
    let Some(parent) = store.get_agent(parent_id)? else {
        // No parent row: project inheritance already reported not_found.
        return Ok(());
    };
    let snapshot = request
        .args
        .get("profile_snapshot")
        .ok_or_else(|| Error::new("internal", "launch profile was not frozen"))?;
    let child_control = match snapshot.get("control") {
        None => AgentControl::Slf,
        Some(Value::String(control)) => AgentControl::parse(control).map_err(|_| {
            Error::new(
                "invalid",
                "profile_snapshot.control must be self, tree, or project",
            )
        })?,
        Some(_) => {
            return Err(Error::new(
                "invalid",
                "profile_snapshot.control must be self, tree, or project",
            ));
        }
    };
    if control_rank(child_control) > control_rank(parent.control) {
        return Err(Error::new(
            "not_allowed",
            "child control exceeds its parent's frozen control",
        ));
    }
    if snapshot_without_control(snapshot) != snapshot_without_control(&parent.profile_snapshot) {
        return Err(Error::new(
            "not_allowed",
            "Agent children reuse their parent's frozen profile",
        ));
    }
    if parent.workspace.as_deref() != request.args.get("workspace").and_then(Value::as_str) {
        return Err(Error::new(
            "not_allowed",
            "Agent children share their parent's workspace",
        ));
    }
    if request.args.get("worktree").is_some() || request.args.get("base").is_some() {
        return Err(Error::new(
            "not_allowed",
            "Agent children cannot mint a new worktree",
        ));
    }
    Ok(())
}

fn caller_row(store: &Store, id: &str) -> Result<Agent> {
    store
        .get_agent(id)?
        .ok_or_else(|| Error::new("not_allowed", "command is outside the caller's scope"))
}

fn not_allowed_scope() -> Error {
    Error::new("not_allowed", "command is outside the caller's scope")
}

/// Submit scope for Agent principals past `authorize_command`. Targeted
/// verbs honor frozen control and fill a missing expected generation from
/// the target row, mirroring the self case. Targeted-verb scope is
/// Router-enforced here; the Store re-checks only launch parents and
/// selector members at admission. Launch parents and selector members pass
/// through to that transactional check. A missing target row passes through
/// so the Store reports `not_found` at admission.
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
    if !store.agent_in_scope(&caller, target)? {
        return Err(not_allowed_scope());
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
fn selector_child_ids(parent: &Command) -> Vec<String> {
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
fn authorize_agent_command_read_scope(
    store: &Store,
    caller_id: &str,
    command: &Command,
) -> Result<()> {
    let caller = caller_row(store, caller_id)?;
    match command.target.as_deref() {
        Some(target) => {
            if store.agent_in_scope(&caller, target)? {
                Ok(())
            } else {
                Err(not_allowed_scope())
            }
        }
        None => {
            if command.principal == format!("agent:{caller_id}:{}", caller.generation) {
                Ok(())
            } else {
                Err(not_allowed_scope())
            }
        }
    }
}

/// Read scope for one Agent row: the self-only rule first, then frozen
/// control for Agent principals.
fn authorize_agent_read_scope(store: &Store, caller_id: &str, id: &str) -> Result<()> {
    let caller = caller_row(store, caller_id)?;
    if store.agent_in_scope(&caller, id)? {
        Ok(())
    } else {
        Err(not_allowed_scope())
    }
}

/// Command-read authorization with frozen Agent control: the self-only rule
/// first, then scope for an Agent reading past itself; self reads keep their
/// generation errors.
pub(super) fn authorize_command_read(
    store: &Store,
    principal: &Principal,
    command: &Command,
) -> Result<()> {
    match authorize_command_read_self(principal, command) {
        Ok(()) => Ok(()),
        Err(error) => {
            let Principal::Agent { id, .. } = principal else {
                return Err(error);
            };
            if command.target.as_deref() == Some(id.as_str()) {
                return Err(error);
            }
            authorize_agent_command_read_scope(store, id, command)
        }
    }
}

/// Agent-read authorization with frozen Agent control: the self-only rule
/// first, then scope for an Agent reading past itself; self reads keep their
/// generation errors.
pub(super) fn authorize_agent_read(
    store: &Store,
    principal: &Principal,
    id: &str,
    generation: Option<i64>,
) -> Result<()> {
    match authorize_agent_read_self(principal, id, generation) {
        Ok(()) => Ok(()),
        Err(error) => {
            let Principal::Agent { id: caller_id, .. } = principal else {
                return Err(error);
            };
            if id == caller_id {
                return Err(error);
            }
            authorize_agent_read_scope(store, caller_id, id)
        }
    }
}

impl Router {
    /// Post-admission scheduling: a selector parent schedules its admitted
    /// children; any other new command schedules itself plus its linked input.
    pub(super) fn schedule_admission(&self, admission: &eliot_store::Admission) {
        if admission.command.selector.is_some() {
            for child in selector_child_ids(&admission.command) {
                self.schedule_command(child, None);
            }
        } else {
            let command_id = admission.command.id.clone();
            let linked_id = admission
                .linked_command
                .as_ref()
                .map(|command| command.id.clone());
            self.schedule_command(command_id, linked_id);
        }
    }

    pub(super) async fn command_children(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: CommandChildrenRequest = serde_json::from_value(params.clone())
            .map_err(|_| Error::new("invalid", "command.children params are invalid"))?;
        request.validate()?;
        let id = request.id.as_str();
        let store = self.inner.store.lock().await;
        let Some(parent) = store.get_command(id)? else {
            // No parent row to authorize against. Only the Owner, who is
            // authorized for every command, may learn whether the id may
            // have been pruned; anyone else reads `not_found`, so an
            // unauthorized caller cannot tell pruned from absent or probe
            // cursors.
            let gap =
                matches!(principal, Principal::Owner) && eliot_store::history_expired(id, now_ms());
            return Err(if gap {
                Error::new(
                    "history_gap",
                    format!("command {id} children are no longer retained"),
                )
            } else {
                Error::new("not_found", format!("command {id} was not found"))
            });
        };
        // Authorize before paging: cursor and membership errors must not
        // answer an unauthorized caller.
        authorize_command_read(&store, principal, &parent)?;
        let mut page =
            store.list_command_children(&parent, request.after.as_deref(), request.limit)?;
        for child in &mut page.children {
            authorize_command_read(&store, principal, child)?;
            self.adjust_command_for_read(child);
        }
        let response = CommandChildrenResponse {
            children: page.children,
            next: page.next,
        };
        serde_json::to_value(response)
            .map_err(|error| Error::new("internal", format!("encode command children: {error}")))
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
        authorize_agent_read(&store, &agent_principal(&manager, 0), &leaf, None).unwrap();
        assert_eq!(
            authorize_agent_read(&store, &agent_principal(&manager, 0), &outsider, None)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            authorize_agent_read(&store, &agent_principal(&leaf, 0), &manager, None)
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }

    #[test]
    fn launch_authority_narrows_control_but_never_widens_or_leaves_home() {
        let mut store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let now = now_ms();
        let manager = launch(&mut store, "scoped", None, "tree", now);
        let principal = agent_principal(&manager, 0);
        let attempt = |args: Value| {
            let mut request = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
            request.args = args;
            request
        };
        // Narrowing to self with the same snapshot is allowed.
        check_agent_launch_authority(
            &store,
            &principal,
            &attempt(json!({
                "parent": manager,
                "workspace": "synthetic-workspace",
                "profile_snapshot": {"control": "self"},
            })),
        )
        .unwrap();
        // Widening to project is refused.
        assert_eq!(
            check_agent_launch_authority(
                &store,
                &principal,
                &attempt(json!({
                    "parent": manager,
                    "workspace": "synthetic-workspace",
                    "profile_snapshot": {"control": "project"},
                })),
            )
            .unwrap_err()
            .code,
            "not_allowed"
        );
        // A different profile is refused even at equal control.
        assert_eq!(
            check_agent_launch_authority(
                &store,
                &principal,
                &attempt(json!({
                    "parent": manager,
                    "workspace": "synthetic-workspace",
                    "profile_snapshot": {"control": "tree", "harness": "codex"},
                })),
            )
            .unwrap_err()
            .code,
            "not_allowed"
        );
        // Another workspace is refused.
        assert_eq!(
            check_agent_launch_authority(
                &store,
                &principal,
                &attempt(json!({
                    "parent": manager,
                    "workspace": "/elsewhere",
                    "profile_snapshot": {"control": "tree"},
                })),
            )
            .unwrap_err()
            .code,
            "not_allowed"
        );
        // A new worktree is refused.
        assert_eq!(
            check_agent_launch_authority(
                &store,
                &principal,
                &attempt(json!({
                    "parent": manager,
                    "workspace": "synthetic-workspace",
                    "worktree": "child-branch",
                    "profile_snapshot": {"control": "tree"},
                })),
            )
            .unwrap_err()
            .code,
            "not_allowed"
        );
        // The owner is unrestricted.
        check_agent_launch_authority(
            &store,
            &Principal::Owner,
            &attempt(json!({
                "parent": manager,
                "workspace": "/elsewhere",
                "profile_snapshot": {"control": "project"},
            })),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn agent_launch_through_the_router_inherits_its_project() {
        let router = router();
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        // The owner launches the parent through the same frozen profile the
        // Agent child must reuse.
        let manager = {
            let response = router
                .submit(
                    &json!({
                        "id": ulid::Ulid::new().to_string(),
                        "verb": "launch",
                        "args": {"workspace": workspace, "profile": "fake"},
                    }),
                    &Principal::Owner,
                )
                .await
                .unwrap();
            let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
            typed.agent.unwrap()
        };
        let response = router
            .submit(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "verb": "launch",
                    "args": {"workspace": workspace, "profile": "fake"},
                }),
                &agent_principal(&manager, 0),
            )
            .await
            .unwrap();
        let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
        let child = typed.agent.unwrap();
        let store = router.inner.store.lock().await;
        let row = store.get_agent(&child).unwrap().unwrap();
        assert_eq!(row.parent.as_deref(), Some(manager.as_str()));
        assert_eq!(
            row.project,
            store.get_agent(&manager).unwrap().unwrap().project
        );
        assert_eq!(row.root, manager);
        assert_eq!(row.depth, 1);
        let _ = std::fs::remove_dir_all(&workspace);
    }

    #[tokio::test]
    async fn agent_launch_outside_the_parent_workspace_is_not_allowed() {
        let router = router();
        let home = std::env::temp_dir().join(format!("eliot-hierarchy-home-{}", ulid::Ulid::new()));
        let away = std::env::temp_dir().join(format!("eliot-hierarchy-away-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&away).unwrap();
        let launch_as = |workspace: &std::path::PathBuf| {
            json!({
                "id": ulid::Ulid::new().to_string(),
                "verb": "launch",
                "args": {"workspace": workspace, "profile": "fake"},
            })
        };
        let response = router
            .submit(&launch_as(&home), &Principal::Owner)
            .await
            .unwrap();
        let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
        let manager = typed.agent.unwrap();
        // A Fake Agent cannot launch a child into another workspace: the
        // child would escape the parent's frozen authority.
        assert_eq!(
            router
                .submit(&launch_as(&away), &agent_principal(&manager, 0))
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let store = router.inner.store.lock().await;
        assert_eq!(store.agent_count().unwrap(), 1);
        let _ = std::fs::remove_dir_all(&home);
        let _ = std::fs::remove_dir_all(&away);
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
