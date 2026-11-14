//! Controller-launched hierarchy enforcement for the Router (L5.3 part 1).
//!
//! Agent submit policy, Agent launch confinement, launch project inheritance,
//! selector child scheduling, and Agent read scope. Store admission re-checks
//! launch parents (project, root, controlled parent) and selector scope
//! transactionally; targeted-verb submit scope and read scope are
//! Router-enforced here against the Store's one scope walk.

use super::auth::{authorize_agent_read_self, authorize_command_read_self};
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
        "send" | "stop" | "steer" | "inject" | "interrupt" | "approve" | "deny" | "answer"
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

/// Snapshot keys that may differ between an allow-listed child and its
/// parent: a cheaper worker/reviewer model (and its effort) is the point of
/// the allow-list, `child_profiles` governs the next hop rather than this
/// launch, and `control` is checked separately as equal or narrower.
fn ignored_child_snapshot_key(key: &str) -> bool {
    matches!(key, "model" | "effort" | "child_profiles" | "control")
}

/// "Same profile except model and effort" over frozen snapshots (#398
/// follow-up). Cross-harness children are rejected at freeze, so both sides
/// carry the same harness here. After removing the ignorable keys and each
/// side's own `profile:<name>` capacity scope, every other key must be
/// equal, including absent-vs-present: that covers permissions, approval
/// policy, approval, MCP policy, pass_env, cmd, route/account/proxy,
/// scratch, setup, native-mode knobs, and the inherited credential
/// bindings. Anything else is a wider (or simply different) authority and
/// rejects.
fn child_snapshot_within_parent(
    child: &Value,
    child_profile: &str,
    parent: &Value,
    parent_profile: &str,
) -> Result<()> {
    let child_stripped = stripped_child_snapshot(child, child_profile)?;
    let parent_stripped = stripped_child_snapshot(parent, parent_profile)?;
    if child_stripped == parent_stripped {
        Ok(())
    } else {
        Err(Error::new(
            "not_allowed",
            "child profile snapshot differs from its parent's frozen profile beyond model and effort",
        ))
    }
}

fn stripped_child_snapshot(snapshot: &Value, profile: &str) -> Result<Value> {
    let Some(object) = snapshot.as_object() else {
        return Err(Error::new(
            "invalid",
            "profile snapshot must be a JSON object",
        ));
    };
    let mut stripped = serde_json::Map::with_capacity(object.len());
    for (key, value) in object {
        if ignored_child_snapshot_key(key) {
            continue;
        }
        if key == "capacity_scopes" {
            stripped.insert(key.clone(), without_own_profile_scope(value, profile)?);
            continue;
        }
        stripped.insert(key.clone(), value.clone());
    }
    Ok(Value::Object(stripped))
}

/// The profile scope (`profile:<name>`) necessarily differs between parent
/// and child; every other capacity scope (account/proxy/route) must match
/// exactly, so a child cannot escape the parent's shared capacity bounds.
fn without_own_profile_scope(scopes: &Value, profile: &str) -> Result<Value> {
    let Some(scopes) = scopes.as_array() else {
        return Err(Error::new(
            "invalid",
            "profile_snapshot.capacity_scopes must be an array",
        ));
    };
    let own = format!("profile:{profile}");
    Ok(Value::Array(
        scopes
            .iter()
            .filter(|entry| entry.get("scope").and_then(Value::as_str) != Some(own.as_str()))
            .cloned()
            .collect(),
    ))
}

/// The child inherits the parent's captured credential bindings verbatim
/// and never re-discovers them: the parent's `pass_env_values` and
/// `native_store_binding` (when the parent has them) replace the child's
/// freshly captured ones, so dispatch derives the same native store.
pub(super) fn inherit_parent_launch_bindings(child: &mut Value, parent: &Value) {
    let Some(child) = child.as_object_mut() else {
        return;
    };
    for key in ["pass_env_values", "native_store_binding"] {
        match parent.get(key) {
            Some(value) => {
                child.insert(key.to_owned(), value.clone());
            }
            None => {
                child.remove(key);
            }
        }
    }
}

/// Agent launch confinement (spec coordination.md "Authority": children
/// cannot widen parental authority). Owner launches are unrestricted. An
/// Agent's child on the parent profile reuses the parent's frozen snapshot;
/// a child on an allow-listed same-harness profile carries that profile's
/// freshly frozen snapshot with only model and effort differing, and the
/// parent's credential bindings and capacity scopes inherited. Control is
/// equal or narrower, the workspace is shared, and no worktree is minted:
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
    // The profile name gate lives at freeze: same-profile children reuse the
    // parent snapshot verbatim, allow-listed children carry their own
    // profile's freshly frozen snapshot. Either way the stripped snapshots
    // must be identical: only model and effort may differ. The freeze always
    // records the child profile name, so it is present here.
    let child_profile = request
        .args
        .get("profile")
        .and_then(Value::as_str)
        .unwrap_or(parent.profile.as_str());
    child_snapshot_within_parent(
        snapshot,
        child_profile,
        &parent.profile_snapshot,
        &parent.profile,
    )?;
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
        let request: CommandChildrenRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
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
            // Historical configuration from each child's exact retained
            // generation and revision, never the current overlay.
            child.config = store.command_config_view(child)?;
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

    /// Router whose data root defines the real `fixture` codex profile, so
    /// owner launches through `submit` resolve a frozen snapshot from config
    /// instead of the removed fake profile.
    fn router_with_fixture_profile() -> (Router, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("eliot-hierarchy-config-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("config.toml"),
            "[profiles.fixture]\nharness = \"codex\"\nmodel = \"fixture-model\"\ncontrol = \"tree\"\n",
        )
        .unwrap();
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let router = Router::new(
            root.clone(),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        );
        (router, root)
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
            "harness": "codex",
            "profile": "fixture",
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
        crate::test_support::install_fixture_sources(&mut *store, agent, &launch_id, 1, now);
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
        // Snapshots must match except for model and effort: an extra
        // harness knob the parent snapshot lacks refuses here, and
        // cross-harness children are rejected at freeze too (#398
        // follow-up). A knob the parent does not bound refuses as well.
        for snapshot in [
            json!({"control": "tree", "harness": "codex"}),
            json!({
                "control": "tree",
                "permissions": "danger-full-access",
            }),
        ] {
            assert_eq!(
                check_agent_launch_authority(
                    &store,
                    &principal,
                    &attempt(json!({
                        "parent": manager,
                        "workspace": "synthetic-workspace",
                        "profile_snapshot": snapshot,
                    })),
                )
                .unwrap_err()
                .code,
                "not_allowed"
            );
        }
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

    #[test]
    fn child_snapshot_comparison_allows_only_model_and_effort_differences() {
        let parent = json!({
            "control": "tree",
            "harness": "codex",
            "model": "manager-model",
            "effort": "medium",
            "permissions": "workspace-write",
            "approval_policy": "on-request",
            "mcp": "inherit",
            "mcp_allow": [],
            "pass_env": ["Q06_SHARED"],
            "pass_env_values": {"CODEX_HOME": null, "CODEX_SQLITE_HOME": null},
            "child_profiles": ["worker"],
            "capacity_scopes": [
                {"scope": "profile:manager", "max_live": null},
                {"scope": "account:shared", "max_live": 1},
            ],
            "account": "shared",
            "scratch": ["target/", "node_modules/"],
        });
        // An identical snapshot is within itself.
        child_snapshot_within_parent(&parent, "manager", &parent, "manager").unwrap();
        // Only model, effort, the next-hop allow-list, narrowed control and
        // the child's own profile scope may differ.
        let mut matching = parent.clone();
        matching["model"] = json!("worker-model");
        matching["effort"] = json!("low");
        matching["control"] = json!("self");
        matching["child_profiles"] = json!([]);
        matching["capacity_scopes"] = json!([
            {"scope": "profile:worker", "max_live": null},
            {"scope": "account:shared", "max_live": 1},
        ]);
        child_snapshot_within_parent(&matching, "worker", &parent, "manager").unwrap();
        // Any other difference rejects: harness, permissions, approval
        // policy, MCP, pass_env, scratch, or the inherited bindings.
        for (key, value) in [
            ("harness", json!("muse")),
            ("permissions", json!("read-only")),
            ("approval_policy", json!("never")),
            ("mcp", json!("minimal")),
            ("mcp_allow", json!(["owner-search"])),
            ("pass_env", json!([])),
            ("scratch", json!([])),
            (
                "pass_env_values",
                json!({"CODEX_HOME": null, "CODEX_SQLITE_HOME": null, "EXTRA": null}),
            ),
            ("cmd", json!("worker-cmd")),
        ] {
            let mut child = matching.clone();
            child[key] = value;
            assert_eq!(
                child_snapshot_within_parent(&child, "worker", &parent, "manager")
                    .unwrap_err()
                    .code,
                "not_allowed",
                "key: {key}"
            );
        }
        // Absent-vs-present rejects in both directions: an omitted knob the
        // parent carries, and an extra knob the parent lacks.
        let mut omitted = matching.clone();
        omitted.as_object_mut().unwrap().remove("pass_env");
        assert_eq!(
            child_snapshot_within_parent(&omitted, "worker", &parent, "manager")
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let mut extra = matching.clone();
        extra["approval"] = json!("allowAll");
        assert_eq!(
            child_snapshot_within_parent(&extra, "worker", &parent, "manager")
                .unwrap_err()
                .code,
            "not_allowed"
        );
        // A missing or loosened account scope rejects: dropping the shared
        // scope would escape the parent's capacity bound (audit finding 3).
        let mut no_account = matching.clone();
        no_account["capacity_scopes"] = json!([{"scope": "profile:worker", "max_live": null}]);
        assert_eq!(
            child_snapshot_within_parent(&no_account, "worker", &parent, "manager")
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let mut loose_account = matching.clone();
        loose_account["capacity_scopes"] = json!([
            {"scope": "profile:worker", "max_live": null},
            {"scope": "account:shared", "max_live": 2},
        ]);
        assert_eq!(
            child_snapshot_within_parent(&loose_account, "worker", &parent, "manager")
                .unwrap_err()
                .code,
            "not_allowed"
        );
        // A non-object snapshot is malformed, never silently allowed.
        assert_eq!(
            child_snapshot_within_parent(&json!([]), "worker", &parent, "manager")
                .unwrap_err()
                .code,
            "invalid"
        );
    }

    /// Router with an explicit multi-profile config for child-profile tests.
    fn router_with_profiles(source: &str) -> (Router, PathBuf) {
        let root =
            std::env::temp_dir().join(format!("eliot-hierarchy-config-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("config.toml"), source).unwrap();
        let store = Store::open(std::path::Path::new(":memory:")).unwrap();
        let router = Router::new(
            root.clone(),
            "owner-credential".to_owned(),
            Arc::new(AsyncMutex::new(store)),
            Bus::new(16),
        );
        (router, root)
    }

    const CHILD_PROFILE_CONFIG: &str = concat!(
        "[accounts.shared]\n",
        "max_live = 2\n",
        "[profiles.manager]\n",
        "harness = \"codex\"\n",
        "model = \"manager-model\"\n",
        "effort = \"medium\"\n",
        "control = \"tree\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "account = \"shared\"\n",
        "child_profiles = [\"worker\"]\n",
        "[profiles.worker]\n",
        "harness = \"codex\"\n",
        "model = \"worker-model\"\n",
        "effort = \"low\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "account = \"shared\"\n",
        "[profiles.stranger]\n",
        "harness = \"codex\"\n",
        "model = \"stranger-model\"\n",
        "control = \"self\"\n",
    );

    const WIDE_CHILD_CONFIG: &str = concat!(
        "[accounts.shared]\n",
        "max_live = 1\n",
        "[accounts.other]\n",
        "max_live = 1\n",
        "[profiles.manager]\n",
        "harness = \"codex\"\n",
        "model = \"manager-model\"\n",
        "effort = \"medium\"\n",
        "control = \"tree\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "account = \"shared\"\n",
        "child_profiles = [\"wide-perms\", \"wide-approval\", \"wide-env\", \"wide-cmd\", \"no-acct\", \"other-acct\"]\n",
        "[profiles.wide-perms]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "effort = \"medium\"\n",
        "control = \"self\"\n",
        "permissions = \"danger-full-access\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "account = \"shared\"\n",
        "[profiles.wide-approval]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "effort = \"medium\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"never\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "account = \"shared\"\n",
        "[profiles.wide-env]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "effort = \"medium\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\", \"Q06_EXTRA\"]\n",
        "account = \"shared\"\n",
        "[profiles.wide-cmd]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "effort = \"medium\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "account = \"shared\"\n",
        "cmd = \"C:/Tools/Codex/codex.exe\"\n",
        "[profiles.no-acct]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "effort = \"medium\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "[profiles.other-acct]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "effort = \"medium\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "account = \"other\"\n",
    );

    async fn launch_manager(router: &Router, workspace: &std::path::Path, profile: &str) -> String {
        let response = router
            .submit(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "verb": "launch",
                    "args": {"workspace": workspace, "profile": profile},
                }),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
        typed.agent.unwrap()
    }

    async fn launch_child(
        router: &Router,
        caller: &str,
        workspace: &std::path::Path,
        profile: Option<&str>,
    ) -> Result<Value> {
        let mut args = json!({"workspace": workspace});
        if let Some(profile) = profile {
            args["profile"] = json!(profile);
        }
        router
            .submit(
                &json!({
                    "id": ulid::Ulid::new().to_string(),
                    "verb": "launch",
                    "args": args,
                }),
                &agent_principal(caller, 0),
            )
            .await
    }

    #[tokio::test]
    async fn allowed_child_profile_launches_with_the_configured_snapshot() {
        let (router, config_root) = router_with_profiles(CHILD_PROFILE_CONFIG);
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "manager").await;
        let response = launch_child(&router, &manager, &workspace, Some("worker"))
            .await
            .unwrap();
        let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
        let child = typed.agent.unwrap();
        let store = router.inner.store.lock().await;
        let child_row = store.get_agent(&child).unwrap().unwrap();
        let manager_row = store.get_agent(&manager).unwrap().unwrap();
        assert_eq!(child_row.profile, "worker");
        assert_eq!(child_row.profile_snapshot["model"], "worker-model");
        assert_eq!(child_row.parent.as_deref(), Some(manager.as_str()));
        assert_eq!(child_row.project, manager_row.project);
        assert_eq!(child_row.root, manager);
        assert_eq!(child_row.depth, 1);
        // The frozen allow-list was captured in the parent snapshot, and the
        // child's record carries its own profile snapshot and parentage for
        // `swarm show` (agent.get) readback.
        assert_eq!(
            manager_row.profile_snapshot["child_profiles"],
            json!(["worker"])
        );
        // Only model, effort, the next-hop allow-list, narrowed control and
        // the child's own profile scope differ.
        assert_eq!(child_row.profile_snapshot["effort"], "low");
        assert_eq!(child_row.profile_snapshot["control"], "self");
        assert_eq!(child_row.profile_snapshot["child_profiles"], json!([]));
        assert!(
            !child_row.profile_inherited,
            "a child on an allowed different profile freezes fresh"
        );
        // The child inherited the parent's credential bindings verbatim and
        // kept the shared account scope with the same bounds (audit
        // findings 2 and 3).
        assert_eq!(
            child_row.profile_snapshot["pass_env_values"],
            manager_row.profile_snapshot["pass_env_values"]
        );
        assert_eq!(
            child_row.profile_snapshot["capacity_scopes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|scope| scope["scope"] != "profile:worker")
                .cloned()
                .collect::<Vec<_>>(),
            manager_row.profile_snapshot["capacity_scopes"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|scope| scope["scope"] != "profile:manager")
                .cloned()
                .collect::<Vec<_>>()
        );
        let parent_account_scope = manager_row.profile_snapshot["capacity_scopes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|scope| scope["scope"] == "account:shared")
            .unwrap()
            .clone();
        assert!(
            child_row.profile_snapshot["capacity_scopes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|scope| scope == &parent_account_scope)
        );
        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn unlisted_child_profile_is_rejected_before_native_effects() {
        let (router, config_root) = router_with_profiles(CHILD_PROFILE_CONFIG);
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "manager").await;
        // `stranger` is a configured profile but is not in the manager's
        // frozen allow-list.
        assert_eq!(
            launch_child(&router, &manager, &workspace, Some("stranger"))
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let store = router.inner.store.lock().await;
        assert_eq!(store.agent_count().unwrap(), 1);
        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn differing_snapshot_on_an_allowed_profile_is_rejected() {
        let (router, config_root) = router_with_profiles(WIDE_CHILD_CONFIG);
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "manager").await;
        // Only model and effort may differ: differing permissions, approval
        // policy, pass_env, cmd, or a missing or different account scope
        // each reject even though the profile is allow-listed.
        for profile in [
            "wide-perms",
            "wide-approval",
            "wide-env",
            "wide-cmd",
            "no-acct",
            "other-acct",
        ] {
            assert_eq!(
                launch_child(&router, &manager, &workspace, Some(profile))
                    .await
                    .unwrap_err()
                    .code,
                "not_allowed",
                "profile: {profile}"
            );
        }
        let store = router.inner.store.lock().await;
        assert_eq!(store.agent_count().unwrap(), 1);
        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn cross_harness_child_is_rejected_at_launch() {
        let (router, config_root) = router_with_profiles(CHILD_PROFILE_CONFIG);
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "manager").await;
        // The owner rewrites the allow-listed worker profile to another
        // harness after the manager launched: the frozen allow-list still
        // names it, but the launch-time harness check refuses (audit
        // finding 1).
        std::fs::write(
            config_root.join("config.toml"),
            concat!(
                "[accounts.shared]\n",
                "max_live = 1\n",
                "[profiles.manager]\n",
                "harness = \"codex\"\n",
                "model = \"manager-model\"\n",
                "effort = \"medium\"\n",
                "control = \"tree\"\n",
                "permissions = \"workspace-write\"\n",
                "approval_policy = \"on-request\"\n",
                "pass_env = [\"Q06_SHARED\"]\n",
                "account = \"shared\"\n",
                "child_profiles = [\"worker\"]\n",
                "[profiles.worker]\n",
                "harness = \"muse\"\n",
                "model = \"worker-model\"\n",
                "cmd = \"C:/Tools/Muse/muse.exe\"\n",
                "control = \"self\"\n",
                "permissions = \"workspace-write\"\n",
                "pass_env = [\"Q06_SHARED\"]\n",
                "account = \"shared\"\n",
            ),
        )
        .unwrap();
        assert_eq!(
            launch_child(&router, &manager, &workspace, Some("worker"))
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let store = router.inner.store.lock().await;
        assert_eq!(store.agent_count().unwrap(), 1);
        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn same_profile_child_reuse_is_unchanged() {
        let (router, config_root) = router_with_fixture_profile();
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "fixture").await;
        {
            let store = router.inner.store.lock().await;
            assert!(
                !store
                    .get_agent(&manager)
                    .unwrap()
                    .unwrap()
                    .profile_inherited,
                "an Owner root launch never inherits its profile"
            );
        }
        // An explicit same-profile name and an omitted profile both reuse
        // the parent snapshot verbatim.
        for profile in [Some("fixture"), None] {
            let response = launch_child(&router, &manager, &workspace, profile)
                .await
                .unwrap();
            let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
            let child = typed.agent.unwrap();
            let store = router.inner.store.lock().await;
            let child_row = store.get_agent(&child).unwrap().unwrap();
            let manager_row = store.get_agent(&manager).unwrap().unwrap();
            assert_eq!(child_row.profile, "fixture");
            assert_eq!(child_row.profile_snapshot, manager_row.profile_snapshot);
            assert!(
                child_row.profile_inherited,
                "an Agent child reusing its parent snapshot verbatim is inherited"
            );
        }
        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn config_reload_after_launch_does_not_widen_the_allow_list() {
        let (router, config_root) = router_with_profiles(CHILD_PROFILE_CONFIG);
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "manager").await;
        // The owner edits the config after the manager launched: the frozen
        // capture still governs, so the originally allowed profile works and
        // the newly listed one does not.
        std::fs::write(
            config_root.join("config.toml"),
            CHILD_PROFILE_CONFIG.replace(
                "child_profiles = [\"worker\"]",
                "child_profiles = [\"stranger\"]",
            ),
        )
        .unwrap();
        launch_child(&router, &manager, &workspace, Some("worker"))
            .await
            .unwrap();
        assert_eq!(
            launch_child(&router, &manager, &workspace, Some("stranger"))
                .await
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn retained_generation_survives_a_real_config_file_reload() {
        const RELOADABLE_CONFIG: &str = concat!(
            "[profiles.reloadable]\n",
            "harness = \"codex\"\n",
            "model = \"m0\"\n",
            "effort = \"low\"\n",
        );
        let (router, config_root) = router_with_profiles(RELOADABLE_CONFIG);
        let workspace = std::env::temp_dir().join(format!("eliot-reload-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        // The launch freezes its snapshot from config.toml through the
        // daemon's resolve path: the daemon keeps no cached copy, so every
        // fresh resolve is the reload.
        let agent = launch_manager(&router, &workspace, "reloadable").await;
        {
            let store = router.inner.store.lock().await;
            let row = store.get_agent(&agent).unwrap().unwrap();
            assert_eq!(row.profile_snapshot["model"], "m0");
            assert!(!row.profile_inherited);
        }

        // Ready the Agent, apply revision 1, then run a turn while a second
        // send stays pending.
        let now = now_ms();
        let launch_id = {
            let store = router.inner.store.lock().await;
            store
                .list_unsettled_commands(100)
                .unwrap()
                .into_iter()
                .find(|command| command.target.as_deref() == Some(agent.as_str()))
                .unwrap()
                .id
        };
        {
            let mut store = router.inner.store.lock().await;
            store.dispatch(&launch_id, now).unwrap();
            crate::test_support::install_fixture_sources(&mut store, &agent, &launch_id, 1, now);
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: "reload-proof:ready".to_owned(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "session.ready".to_owned(),
                            native_id: "reload-proof:ready".to_owned(),
                            data: json!({"command_id": launch_id}),
                        }],
                        cursor: None,
                    },
                    now,
                )
                .unwrap();
            store
                .settle(&launch_id, CommandState::Applied, None, now)
                .unwrap();
        }
        let configure_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut configure = SubmitCommand::new(configure_id.clone(), "configure");
            configure.target = Some(agent.clone());
            configure.expected_generation = Some(1);
            configure.args = json!({"model": "m1"});
            store.submit("owner", configure, now + 1).unwrap();
            store.dispatch(&configure_id, now + 2).unwrap();
            let applied = store.apply_agent_config(&configure_id, now + 3).unwrap();
            assert_eq!(applied.revision, 1);
            store
                .settle(&configure_id, CommandState::Applied, None, now + 3)
                .unwrap();
        }
        let first_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut send = SubmitCommand::new(first_id.clone(), "send");
            send.target = Some(agent.clone());
            send.expected_generation = Some(1);
            send.args = json!({"text": "first turn"});
            store.submit("owner", send, now + 4).unwrap();
            store.dispatch(&first_id, now + 5).unwrap();
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: "reload-proof:turn".to_owned(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "turn.started".to_owned(),
                            native_id: "reload-proof:turn:started".to_owned(),
                            data: json!({"command_id": first_id, "turn_id": "reload-turn"}),
                        }],
                        cursor: None,
                    },
                    now + 5,
                )
                .unwrap();
        }
        let pending_id = ulid::Ulid::new().to_string();
        {
            let mut store = router.inner.store.lock().await;
            let mut pending = SubmitCommand::new(pending_id.clone(), "send");
            pending.target = Some(agent.clone());
            pending.expected_generation = Some(1);
            pending.args = json!({"text": "pending turn"});
            store.submit("owner", pending, now + 6).unwrap();
            store
                .put_batch(
                    eliot_core::SourceBatch {
                        batch_id: "reload-proof:done".to_owned(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![eliot_core::NativeEvent {
                            ts: None,
                            kind: "turn.completed".to_owned(),
                            native_id: "reload-proof:done:completed".to_owned(),
                            data: json!({"command_id": first_id, "turn_id": "reload-turn", "status": "completed"}),
                        }],
                        cursor: None,
                    },
                    now + 6,
                )
                .unwrap();
        }

        // The owner edits the profile in the file: the daemon's next
        // resolve is the reload, and a new launch observes the edit.
        std::fs::write(
            config_root.join("config.toml"),
            RELOADABLE_CONFIG.replace("model = \"m0\"", "model = \"m-edited\""),
        )
        .unwrap();
        let reloaded_workspace =
            std::env::temp_dir().join(format!("eliot-reload-after-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&reloaded_workspace).unwrap();
        let reloaded = launch_manager(&router, &reloaded_workspace, "reloadable").await;
        {
            let store = router.inner.store.lock().await;
            let reloaded_row = store.get_agent(&reloaded).unwrap().unwrap();
            assert_eq!(
                reloaded_row.profile_snapshot["model"], "m-edited",
                "a launch after the edit resolves the reloaded profile"
            );
            let retained_row = store.get_agent(&agent).unwrap().unwrap();
            assert_eq!(
                retained_row.profile_snapshot["model"], "m0",
                "the reload never rewrites a retained generation"
            );
        }

        // The pending send dispatches under its retained revision, then the
        // restart fence marks it unknown: both still read back revision 1.
        {
            let mut store = router.inner.store.lock().await;
            store.dispatch(&pending_id, now + 7).unwrap();
            store.recover_on_restart(now + 8).unwrap();
            let unknown = store.get_command(&pending_id).unwrap().unwrap();
            assert_eq!(unknown.state, CommandState::Unknown);
            assert_eq!(unknown.config_revision, 1);
            let view = store
                .command_config_view(&unknown)
                .unwrap()
                .expect("pending command keeps its view");
            assert_eq!(view.revision, 1);
            assert_eq!(view.provenance, "configure");
            assert_eq!(view.profile, "reloadable");
            assert_eq!(view.model.as_deref(), Some("m1"));
            assert_eq!(view.original_model.as_deref(), Some("m0"));
            let agent_row = store.get_agent(&agent).unwrap().unwrap();
            let turn = store.get_turn(&agent, 1, "reload-turn").unwrap().unwrap();
            let turn_view = store
                .turn_config_view(&agent_row, &turn)
                .unwrap()
                .expect("retained turn keeps its view");
            assert_eq!(turn_view.revision, 1);
            assert_eq!(turn_view.model.as_deref(), Some("m1"));
        }

        // The public readbacks expose the same retained views, and the
        // readback validators accept them.
        let command_value = router
            .command_get(&json!({"id": pending_id}), &Principal::Owner)
            .await
            .unwrap();
        let command: eliot_core::Command = serde_json::from_value(command_value).unwrap();
        eliot_core::api::CommandGetRequest::new(&pending_id)
            .validate_response(&command)
            .unwrap();
        let command_view = command.config.expect("command readback carries its view");
        assert_eq!(command_view.revision, 1);
        assert_eq!(command_view.provenance, "configure");
        assert_eq!(command_view.profile, "reloadable");
        assert_eq!(command_view.model.as_deref(), Some("m1"));

        let turn_value = router
            .turn_get(
                &json!({"agent": agent, "generation": 1, "turn_id": "reload-turn"}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        let turn: eliot_core::Turn = serde_json::from_value(turn_value).unwrap();
        eliot_core::api::TurnGetRequest {
            agent: agent.clone(),
            generation: 1,
            turn_id: "reload-turn".to_owned(),
        }
        .validate_response(&turn)
        .unwrap();
        let read_turn_view = turn.config.expect("turn readback carries its view");
        assert_eq!(read_turn_view.revision, 1);
        assert_eq!(read_turn_view.model.as_deref(), Some("m1"));
        assert_eq!(read_turn_view.profile, "reloadable");

        let _ = std::fs::remove_dir_all(&workspace);
        let _ = std::fs::remove_dir_all(&reloaded_workspace);
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn agent_launch_through_the_router_inherits_its_project() {
        let (router, config_root) = router_with_fixture_profile();
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
                        "args": {"workspace": workspace, "profile": "fixture"},
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
                    "args": {"workspace": workspace, "profile": "fixture"},
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
        let _ = std::fs::remove_dir_all(&config_root);
    }

    #[tokio::test]
    async fn agent_launch_outside_the_parent_workspace_is_not_allowed() {
        let (router, config_root) = router_with_fixture_profile();
        let home = std::env::temp_dir().join(format!("eliot-hierarchy-home-{}", ulid::Ulid::new()));
        let away = std::env::temp_dir().join(format!("eliot-hierarchy-away-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&home).unwrap();
        std::fs::create_dir_all(&away).unwrap();
        let launch_as = |workspace: &std::path::PathBuf| {
            json!({
                "id": ulid::Ulid::new().to_string(),
                "verb": "launch",
                "args": {"workspace": workspace, "profile": "fixture"},
            })
        };
        let response = router
            .submit(&launch_as(&home), &Principal::Owner)
            .await
            .unwrap();
        let typed: CommandSubmitResponse = serde_json::from_value(response).unwrap();
        let manager = typed.agent.unwrap();
        // An Agent cannot launch a child into another workspace: the
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
        let _ = std::fs::remove_dir_all(&config_root);
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
