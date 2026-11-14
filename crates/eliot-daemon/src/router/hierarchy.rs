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

/// Breadth rank for the frozen `permissions` sandbox knob: read-only is the
/// narrowest, danger-full-access the broadest. Unknown values never allow.
fn permission_rank(value: &str) -> Option<u8> {
    match value {
        "read-only" => Some(0),
        "workspace-write" => Some(1),
        "danger-full-access" => Some(2),
        _ => None,
    }
}

/// Breadth rank for the frozen Codex `approval_policy` knob. `never` runs
/// without approval callbacks (unilateral); `on-request` must stop and ask.
/// Unknown values never allow.
fn approval_policy_rank(value: &str) -> Option<u8> {
    match value {
        "on-request" => Some(0),
        "never" => Some(1),
        _ => None,
    }
}

/// Breadth rank for the frozen Muse `approval` knob, least to most
/// permissive by name. Unknown values never allow.
fn approval_rank(value: &str) -> Option<u8> {
    match value {
        "denyUnmatched" => Some(0),
        "promptUnmatched" => Some(1),
        "onRequest" => Some(2),
        "allowAll" => Some(3),
        _ => None,
    }
}

/// Breadth rank for the frozen worker MCP inheritance policy: `minimal`
/// starts only the Agent-scoped server plus the allow list, `inherit` keeps
/// the owner's full native surface. Unknown values never allow.
fn mcp_policy_rank(value: &str) -> Option<u8> {
    match value {
        "minimal" => Some(0),
        "inherit" => Some(1),
        _ => None,
    }
}

fn snapshot_text<'a>(snapshot: &'a Value, key: &str) -> Option<&'a str> {
    snapshot.get(key)?.as_str()
}

fn snapshot_allow_list(snapshot: &Value, key: &str) -> Result<Vec<String>> {
    match snapshot.get(key) {
        None => Ok(Vec::new()),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_owned).ok_or_else(|| {
                    Error::new(
                        "invalid",
                        format!("profile_snapshot.{key} must be an array of strings"),
                    )
                })
            })
            .collect(),
        Some(_) => Err(Error::new(
            "invalid",
            format!("profile_snapshot.{key} must be an array of strings"),
        )),
    }
}

fn env_names_equal(left: &str, right: &str) -> bool {
    if cfg!(windows) {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

fn child_knob_exceeds(key: &str) -> Error {
    Error::new(
        "not_allowed",
        format!("child {key} exceeds its parent's frozen profile"),
    )
}

/// Ranked scalar authority knob: the child value must parse to a known rank
/// at or below the parent's. A knob the child snapshot does not carry is not
/// claimed; a knob it carries but the parent lacks (or either side carries
/// with an unknown value) rejects instead of allowing.
fn check_ranked_knob(
    child: &Value,
    parent: &Value,
    key: &str,
    rank: fn(&str) -> Option<u8>,
) -> Result<()> {
    if child.get(key).is_none() {
        return Ok(());
    }
    match (
        snapshot_text(child, key).and_then(rank),
        snapshot_text(parent, key).and_then(rank),
    ) {
        (Some(child_rank), Some(parent_rank)) if child_rank <= parent_rank => Ok(()),
        _ => Err(child_knob_exceeds(key)),
    }
}

/// Exact-match authority knob: no breadth order is documented for these
/// values, so the child value must equal the parent's. Same presence rules
/// as ranked knobs.
fn check_equal_knob(child: &Value, parent: &Value, key: &str) -> Result<()> {
    if child.get(key).is_none() {
        return Ok(());
    }
    let Some(parent_value) = parent.get(key) else {
        return Err(child_knob_exceeds(key));
    };
    if child.get(key) == Some(parent_value) {
        Ok(())
    } else {
        Err(child_knob_exceeds(key))
    }
}

/// "No broader" over the frozen snapshot authority knobs (issue #398).
/// Model, effort, harness and cmd may differ: changing them is the point
/// and grants no authority. Dispatch-captured inputs (`pass_env_values`,
/// `native_store_binding`), delegation metadata (`child_profiles`), and
/// admission-enforced bindings (capacity scopes, route/account/proxy,
/// scratch, setup) are not authority knobs and stay out of this comparison:
/// nonempty setup is still refused without a worktree downstream and
/// capacity stays enforced at admission. A knob the child does not carry is
/// not claimed; any knob it carries must be bounded by the parent's same
/// knob. Unknown or incomparable values reject; they never allow.
fn child_snapshot_within_parent(child: &Value, parent: &Value) -> Result<()> {
    check_ranked_knob(child, parent, "permissions", permission_rank)?;
    check_ranked_knob(child, parent, "approval_policy", approval_policy_rank)?;
    check_ranked_knob(child, parent, "approval", approval_rank)?;
    check_equal_knob(child, parent, "permission_mode")?;
    check_equal_knob(child, parent, "mode")?;
    if let Some(child_skip) = child.get("dangerously_skip_permissions") {
        let Some(child_skip) = child_skip.as_bool() else {
            return Err(Error::new(
                "invalid",
                "profile_snapshot.dangerously_skip_permissions must be a boolean",
            ));
        };
        if child_skip
            && !parent
                .get("dangerously_skip_permissions")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        {
            return Err(child_knob_exceeds("dangerously_skip_permissions"));
        }
    }
    check_equal_knob(child, parent, "auth_integration")?;
    check_equal_knob(child, parent, "collaboration_mode")?;
    check_mcp_inheritance(child, parent)?;
    check_pass_env_subset(child, parent)?;
    Ok(())
}

/// MCP inheritance is no broader than the parent's: the policy rank must not
/// exceed the parent's, and an explicit allow list must stay within the
/// parent's. A parent on `inherit` keeps the full native surface, so any
/// explicit child allow list stays within it.
fn check_mcp_inheritance(child: &Value, parent: &Value) -> Result<()> {
    if child.get("mcp").is_none() {
        return Ok(());
    }
    check_ranked_knob(child, parent, "mcp", mcp_policy_rank)?;
    let child_allow = snapshot_allow_list(child, "mcp_allow")?;
    if child_allow.is_empty() {
        return Ok(());
    }
    if snapshot_text(child, "mcp") != Some("minimal") {
        return Err(child_knob_exceeds("mcp_allow"));
    }
    if snapshot_text(parent, "mcp") == Some("inherit") {
        return Ok(());
    }
    let parent_allow = snapshot_allow_list(parent, "mcp_allow")?;
    for server in &child_allow {
        if !parent_allow.iter().any(|bound| bound == server) {
            return Err(child_knob_exceeds("mcp_allow"));
        }
    }
    Ok(())
}

/// Inherited environment names are no broader than the parent's: the child
/// set must stay within the parent set.
fn check_pass_env_subset(child: &Value, parent: &Value) -> Result<()> {
    let child_env = snapshot_allow_list(child, "pass_env")?;
    if child_env.is_empty() {
        return Ok(());
    }
    let parent_env = snapshot_allow_list(parent, "pass_env")?;
    for name in &child_env {
        if !parent_env.iter().any(|bound| env_names_equal(name, bound)) {
            return Err(child_knob_exceeds("pass_env"));
        }
    }
    Ok(())
}

/// Agent launch confinement (spec coordination.md "Authority": children
/// cannot widen parental authority). Owner launches are unrestricted. An
/// Agent's child on the parent profile reuses the parent's frozen snapshot;
/// a child on an allow-listed profile carries that profile's freshly frozen
/// snapshot, with every authority knob no broader than the parent's. Either
/// way the child shares the parent's workspace and mints no worktree: the
/// spec grants children nothing beyond project/root inheritance. Runs after
/// the profile freeze, before workspace preparation mints anything.
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
    // profile's freshly frozen snapshot. Either way every authority knob the
    // child carries must be no broader than the parent's same knob; model,
    // effort and harness may differ.
    child_snapshot_within_parent(snapshot, &parent.profile_snapshot)?;
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
        // A different harness alone is not wider authority: model, effort
        // and harness may differ (#398), and the profile-name allow-list
        // gate lives at freeze. Broader permissions still refuse.
        check_agent_launch_authority(
            &store,
            &principal,
            &attempt(json!({
                "parent": manager,
                "workspace": "synthetic-workspace",
                "profile_snapshot": {"control": "tree", "harness": "codex"},
            })),
        )
        .unwrap();
        assert_eq!(
            check_agent_launch_authority(
                &store,
                &principal,
                &attempt(json!({
                    "parent": manager,
                    "workspace": "synthetic-workspace",
                    "profile_snapshot": {
                        "control": "tree",
                        "permissions": "danger-full-access",
                    },
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

    #[test]
    fn child_snapshot_comparison_is_no_broader_and_fail_closed() {
        let parent = json!({
            "control": "tree",
            "harness": "codex",
            "model": "manager-model",
            "permissions": "workspace-write",
            "approval_policy": "on-request",
            "mcp": "minimal",
            "mcp_allow": ["owner-search"],
            "pass_env": ["Q06_SHARED"],
        });
        // An identical snapshot is within itself.
        child_snapshot_within_parent(&parent, &parent).unwrap();
        // Narrower-or-equal passes even with a different model and harness:
        // changing them is the point and grants no authority.
        child_snapshot_within_parent(
            &json!({
                "control": "self",
                "harness": "codex",
                "model": "worker-model",
                "permissions": "read-only",
                "approval_policy": "on-request",
                "mcp": "minimal",
                "mcp_allow": [],
                "pass_env": ["Q06_SHARED"],
            }),
            &parent,
        )
        .unwrap();
        // A broader permission, approval policy, pass_env entry, or MCP
        // allow entry rejects.
        for child in [
            json!({"permissions": "danger-full-access"}),
            json!({"approval_policy": "never"}),
            json!({"pass_env": ["Q06_SHARED", "Q06_EXTRA"]}),
            json!({"mcp_allow": ["owner-search", "other-server"]}),
            json!({"mcp": "inherit", "mcp_allow": []}),
        ] {
            let mut child = child;
            child["control"] = json!("self");
            let mut merged = parent.clone();
            for (key, value) in child.as_object().unwrap() {
                merged[key] = value.clone();
            }
            assert_eq!(
                child_snapshot_within_parent(&merged, &parent)
                    .unwrap_err()
                    .code,
                "not_allowed"
            );
        }
        // Unknown values never allow.
        let mut unknown = parent.clone();
        unknown["permissions"] = json!("superuser");
        assert_eq!(
            child_snapshot_within_parent(&unknown, &parent)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        // A knob the parent snapshot does not bound rejects.
        let mut unbounded = parent.clone();
        unbounded.as_object_mut().unwrap().remove("permissions");
        assert_eq!(
            child_snapshot_within_parent(&parent, &unbounded)
                .unwrap_err()
                .code,
            "not_allowed"
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
        "[profiles.manager]\n",
        "harness = \"codex\"\n",
        "model = \"manager-model\"\n",
        "control = \"tree\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "child_profiles = [\"worker\"]\n",
        "[profiles.worker]\n",
        "harness = \"codex\"\n",
        "model = \"worker-model\"\n",
        "control = \"self\"\n",
        "permissions = \"read-only\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "[profiles.stranger]\n",
        "harness = \"codex\"\n",
        "model = \"stranger-model\"\n",
        "control = \"self\"\n",
    );

    const WIDE_CHILD_CONFIG: &str = concat!(
        "[profiles.manager]\n",
        "harness = \"codex\"\n",
        "model = \"manager-model\"\n",
        "control = \"tree\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "child_profiles = [\"wide-perms\", \"wide-approval\", \"wide-env\"]\n",
        "[profiles.wide-perms]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "control = \"self\"\n",
        "permissions = \"danger-full-access\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "[profiles.wide-approval]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"never\"\n",
        "pass_env = [\"Q06_SHARED\"]\n",
        "[profiles.wide-env]\n",
        "harness = \"codex\"\n",
        "model = \"wide-model\"\n",
        "control = \"self\"\n",
        "permissions = \"workspace-write\"\n",
        "approval_policy = \"on-request\"\n",
        "pass_env = [\"Q06_SHARED\", \"Q06_EXTRA\"]\n",
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
    async fn broader_authority_on_an_allowed_profile_is_rejected() {
        let (router, config_root) = router_with_profiles(WIDE_CHILD_CONFIG);
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "manager").await;
        // Broader permissions, a broader approval policy, and broader
        // pass_env each reject even though the profile is allow-listed.
        for profile in ["wide-perms", "wide-approval", "wide-env"] {
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
    async fn same_profile_child_reuse_is_unchanged() {
        let (router, config_root) = router_with_fixture_profile();
        let workspace = std::env::temp_dir().join(format!("eliot-hierarchy-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&workspace).unwrap();
        let manager = launch_manager(&router, &workspace, "fixture").await;
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
