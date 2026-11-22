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

/// Check the current Agent generation before preparing a fresh launch.
/// Retained retries return their existing admission before this check.
pub(super) fn authorize_agent_submit(store: &Store, principal: &Principal) -> Result<()> {
    if matches!(principal, Principal::Agent { .. }) {
        store.authorize_agent_child_launch(&principal_name(principal))?;
    }
    Ok(())
}

/// Bind the caller's parent or self-target before scope checks and replay.
pub(super) fn bind_agent_submit(
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
    if request.verb == "git.integrate" {
        if request.target.is_some()
            || request.selector.is_some()
            || request.expected_generation.is_some()
            || request.expected_turn_id.is_some()
            || request.request_id.is_some()
            || request.probe
        {
            return Err(Error::new(
                "invalid",
                "git.integrate is targetless and has no Agent command bindings",
            ));
        }
        return Ok(());
    }
    if !matches!(
        request.verb.as_str(),
        "send" | "stop" | "steer" | "inject" | "interrupt" | "approve" | "deny" | "answer"
    ) {
        return Err(Error::new(
            "not_allowed",
            format!("managed Agent credentials cannot submit {}", request.verb),
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

/// Snapshot keys that may differ between an allow-listed child and its
/// parent: a cheaper worker/reviewer model (and its effort, plus the
/// daemon-resolved `daemon_model`) is the point of the allow-list,
/// `child_profiles` governs the next hop rather than this launch,
/// `native_subagents` governs the child's own session rather than this
/// launch, `control` is checked separately as equal or narrower, and
/// `native_launch_metadata` is command-bound launch metadata stamped on the
/// parent at dispatch (replaced on child dispatch, never profile policy).
fn ignored_child_snapshot_key(key: &str) -> bool {
    matches!(
        key,
        "model"
            | "effort"
            | "context_ceiling_tokens"
            | "daemon_model"
            | "child_profiles"
            | "native_subagents"
            | "control"
            | "native_launch_metadata"
    )
}

/// Compare native authority across frozen same-harness profiles. Model,
/// effort and the daemon's context ceiling can vary by approved profile.
/// After removing the ignorable keys, each
/// side's own `profile:<name>` scope and the daemon-stamped scopes, every
/// other key must be equal, including absent-vs-present: that covers
/// permissions, approval policy, approval, MCP policy, pass_env, cmd,
/// route/account/proxy, scratch, setup, native-mode knobs, and the
/// inherited credential bindings. Anything else is a wider (or simply
/// different) authority and rejects.
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
            "child profile snapshot differs outside the permitted profile fields",
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
            stripped.insert(key.clone(), without_own_and_daemon_scopes(value, profile)?);
            continue;
        }
        stripped.insert(key.clone(), value.clone());
    }
    Ok(Value::Object(stripped))
}

/// Compare frozen shared scopes across same-harness profiles. Legacy
/// daemon-owned entries are resolved afresh by Store admission.
fn without_own_and_daemon_scopes(scopes: &Value, profile: &str) -> Result<Value> {
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
            .filter(|entry| {
                entry
                    .get("scope")
                    .and_then(Value::as_str)
                    .is_none_or(|scope| {
                        scope != own && !eliot_core::CapacityScope::is_daemon_owned(scope)
                    })
            })
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
    for key in [
        "pass_env_values",
        "native_route_identity",
        "native_store_binding",
    ] {
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

/// Keep same-harness binding inheritance. An explicitly approved different
/// harness keeps its own bindings and native policies, but cannot shed the
/// parent's shared capacity bounds. Called by both named and role launches.
pub(super) fn inherit_child_launch_bounds(child: &mut Value, parent: &Agent) -> Result<()> {
    if child.get("harness").and_then(Value::as_str) == Some(parent.harness.as_str()) {
        inherit_parent_launch_bindings(child, &parent.profile_snapshot);
    } else {
        child["capacity_scopes"] = cross_harness_capacity_scopes(child, parent)?;
    }
    Ok(())
}

fn cross_harness_capacity_scopes(child: &Value, parent: &Agent) -> Result<Value> {
    let decode = |snapshot: &Value| -> Result<Vec<eliot_core::CapacityScope>> {
        serde_json::from_value(
            snapshot
                .get("capacity_scopes")
                .cloned()
                .unwrap_or(json!([])),
        )
        .map_err(|_| Error::new("invalid", "malformed profile capacity scopes"))
    };
    let mut scopes = decode(child)?;
    let parent_profile_scope = format!("profile:{}", parent.profile);
    for inherited in decode(&parent.profile_snapshot)? {
        if inherited.scope == parent_profile_scope
            || eliot_core::CapacityScope::is_daemon_owned(&inherited.scope)
        {
            continue;
        }
        if let Some(existing) = scopes.iter().find(|scope| scope.scope == inherited.scope) {
            if existing != &inherited {
                return Err(Error::new(
                    "not_allowed",
                    "child profile conflicts with its parent's frozen capacity bound",
                ));
            }
        } else {
            scopes.push(inherited);
        }
    }
    serde_json::to_value(scopes)
        .map_err(|_| Error::new("internal", "child capacity scopes could not be encoded"))
}

/// Agent launch confinement (spec coordination.md "Authority": children
/// cannot widen parental authority). Owner launches are unrestricted. An
/// Agent's child on the parent profile reuses the parent's frozen snapshot;
/// a child on an allow-listed same-harness profile carries that profile's
/// freshly frozen model, effort and context ceiling, and inherits the
/// parent's credential bindings. The daemon-owned capacity scopes
/// are stamped after the check from the inherited project. An allow-listed
/// cross-harness profile is an explicit Owner grant of its own native policy
/// and bindings, retaining the parent's shared capacity scopes. Control is
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
    if child_control > parent.control {
        return Err(Error::new(
            "not_allowed",
            "child control exceeds its parent's frozen control",
        ));
    }
    // Same-harness children retain model-only confinement. Cross-harness
    // policy names have no comparable meaning: the frozen Owner allow-list
    // grants that profile's own policy, while Eliot scope stays confined.
    let child_profile = request
        .args
        .get("profile")
        .and_then(Value::as_str)
        .unwrap_or(parent.profile.as_str());
    if snapshot
        .get("harness")
        .and_then(Value::as_str)
        .is_some_and(|harness| harness != parent.harness)
    {
        let allowed = parent.profile_snapshot["child_profiles"]
            .as_array()
            .is_some_and(|allowed| {
                allowed
                    .iter()
                    .any(|name| name.as_str() == Some(child_profile))
            });
        if !allowed || child_profile == parent.profile {
            return Err(Error::new(
                "not_allowed",
                "cross-harness child profile is not allow-listed",
            ));
        }
        if snapshot.get("capacity_scopes")
            != Some(&cross_harness_capacity_scopes(snapshot, &parent)?)
        {
            return Err(Error::new(
                "not_allowed",
                "child is missing inherited capacity scopes",
            ));
        }
    } else {
        child_snapshot_within_parent(
            snapshot,
            child_profile,
            &parent.profile_snapshot,
            &parent.profile,
        )?;
    }
    if parent.workspace.as_deref() != request.args.get("workspace").and_then(Value::as_str) {
        return Err(Error::new(
            "not_allowed",
            "Agent children share their parent's workspace",
        ));
    }
    if request.args.get("worktree").is_some()
        || request.args.get("base").is_some()
        || request.args.get("auto_worktree").is_some()
    {
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

/// Workspace default for a child: an omitted workspace inherits the
/// parent's workspace verbatim, so the working `{parent, profile, workspace}`
/// shape needs no guesswork. An explicit workspace must still equal the
/// parent's (enforced by `check_agent_launch_authority`), so a child never
/// escapes the parent's authority. Runs before `normalize_launch` validates and canonicalizes the
/// path; a missing parent row or parent workspace defers to the later
/// `not_found`/`invalid` gates.
pub(super) fn default_child_workspace(store: &Store, request: &mut SubmitCommand) -> Result<()> {
    if request.verb != "launch" {
        return Ok(());
    }
    let Some(parent_id) = request.args.get("parent").and_then(Value::as_str) else {
        return Ok(());
    };
    let omitted = matches!(request.args.get("workspace"), None | Some(Value::Null));
    if !omitted {
        return Ok(());
    }
    let Some(parent) = store.get_agent(parent_id)? else {
        return Ok(());
    };
    let Some(workspace) = parent.workspace.clone() else {
        return Ok(());
    };
    if let Some(args) = request.args.as_object_mut() {
        args.insert("workspace".to_owned(), Value::String(workspace));
    }
    Ok(())
}

/// A child keeps its parent's project identity and authorization namespace.
/// Return whether it is borrowing the parent's workspace.
pub(super) fn inherit_launch_project_scope(
    store: &Store,
    request: &mut SubmitCommand,
) -> Result<bool> {
    let Some(parent) = request.args.get("parent").and_then(Value::as_str) else {
        return Ok(false);
    };
    let parent_agent = store
        .get_agent(parent)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {parent} was not found")))?;
    request.args["project"] = Value::String(parent_agent.project_scope().to_owned());
    request.args["registered_project"] = Value::String(parent_agent.project.clone());
    Ok(parent_agent.workspace.as_deref() == request.args["workspace"].as_str())
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
    pub(super) async fn swarm_reparent(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: eliot_core::api::ReparentRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let mut store = self.inner.store.lock().await;
        let change = store.reparent(
            &principal_name(principal),
            &request,
            crate::common::now_ms(),
        )?;
        self.publish_locked(&change.events);
        serde_json::to_value(change.value)
            .map_err(|_| Error::new("internal", "cannot encode reparent response"))
    }

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
