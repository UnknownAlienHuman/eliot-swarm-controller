//! Controller-launched children and tree selectors (L5.3 part 1).
//!
//! Owning contracts: `docs/spec/coordination.md` (claims and hierarchy,
//! prompt guidance) and the hierarchy parts of `core.md`/`configuration.md`.
//! A child inherits its parent's project label and tree root; the parent link
//! is durable. Fanout limits gate launch admission only and never hide an
//! Agent row that already exists. A tree selector expands once at admission
//! to concrete members using the existing Store-only parent plus ordered
//! children representation (`link_kind = 'selector'`); the parent applied
//! means expansion, not children finished.

use super::*;
use eliot_core::api::{HierarchyLimits, parse_tree_selector, selector_child_id};

pub(super) const SELECTOR_LINK_KIND: &str = "selector";
/// Bound on parent-chain walks; creation depth is fanout-limited, so this is
/// only defense against legacy rows, never a reachable depth.
const ANCESTOR_WALK_CAP: i64 = 4096;

pub(super) enum Caller {
    Owner,
    Agent {
        num: i64,
        id: String,
        control: AgentControl,
        project: String,
    },
}

/// Parse the owner-or-Agent principal for hierarchy admission, mirroring the
/// mail sender rule: a missing Agent row or a moved generation reads
/// `stale_generation`, any other principal reads `not_allowed`.
pub(super) fn parse_caller(connection: &Connection, principal: &str) -> Result<Caller> {
    if principal == "owner" {
        return Ok(Caller::Owner);
    }
    if let Some(rest) = principal.strip_prefix("agent:") {
        let Some((id, generation)) = rest.split_once(':') else {
            return Err(Error::new(
                "not_allowed",
                "hierarchy needs the owner or an Agent principal",
            ));
        };
        let num = parse_agent_id(id)?;
        let generation = generation.parse::<i64>().map_err(|_| {
            Error::new(
                "not_allowed",
                "hierarchy needs the owner or an Agent principal",
            )
        })?;
        if generation < 0 {
            return Err(Error::new(
                "not_allowed",
                "hierarchy needs the owner or an Agent principal",
            ));
        }
        let agent = load_agent_by_num(connection, num)?.ok_or_else(|| {
            Error::new("stale_generation", "caller generation is no longer current")
        })?;
        if agent.generation != generation {
            return Err(Error::new(
                "stale_generation",
                "caller generation is no longer current",
            ));
        }
        return Ok(Caller::Agent {
            num,
            id: agent.id,
            control: agent.control,
            project: agent.project,
        });
    }
    Err(Error::new(
        "not_allowed",
        "hierarchy needs the owner or an Agent principal",
    ))
}

/// One subtree snapshot: root first, then descendants in Agent-id order.
/// Reads at most `limit + 1` rows so the caller can detect overflow without
/// an unbounded page.
pub(super) fn subtree_member_nums(
    connection: &Connection,
    root_num: i64,
    limit: i64,
) -> Result<Vec<i64>> {
    let mut statement = connection
        .prepare(
            "WITH RECURSIVE subtree(id) AS (
                 SELECT ?1
                 UNION
                 SELECT a.id FROM agents a JOIN subtree s ON a.parent = s.id
             )
             SELECT id FROM subtree ORDER BY id LIMIT ?2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(params![root_num, limit.saturating_add(1)], |row| row.get(0))
        .map_err(sql_error)?;
    let mut members = Vec::new();
    for row in rows {
        members.push(row.map_err(sql_error)?);
    }
    Ok(members)
}

/// Live children of one parent: terminal Agents (stopped, failed, lost)
/// no longer occupy fanout. Unknown Agents still count: they may recover.
pub(super) fn count_children_tx(tx: &Transaction<'_>, parent_num: i64) -> Result<i64> {
    tx.query_row(
        "SELECT COUNT(*) FROM agents
         WHERE parent = ?1 AND state NOT IN ('stopped', 'failed', 'lost')",
        [parent_num],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

/// Live Agents of one tree: terminal members no longer occupy fanout.
pub(super) fn count_tree_tx(tx: &Transaction<'_>, root_num: i64) -> Result<i64> {
    tx.query_row(
        "SELECT COUNT(*) FROM agents
         WHERE (root = ?1 OR (root IS NULL AND id = ?1))
           AND state NOT IN ('stopped', 'failed', 'lost')",
        [root_num],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

/// Whether `member_num` is `ancestor_num` or descends from it, walking parent
/// links up from the member. Denies past the walk cap instead of scanning.
fn is_in_subtree_of(connection: &Connection, member_num: i64, ancestor_num: i64) -> Result<bool> {
    let mut current = member_num;
    for _ in 0..ANCESTOR_WALK_CAP {
        if current == ancestor_num {
            return Ok(true);
        }
        let parent: Option<i64> = connection
            .query_row(
                "SELECT parent FROM agents WHERE id = ?1",
                [current],
                |row| row.get(0),
            )
            .optional()
            .map_err(sql_error)?
            .flatten();
        match parent {
            Some(num) => current = num,
            None => return Ok(false),
        }
    }
    Ok(false)
}

/// The one frozen-scope walk: itself, its subtree for tree control, or its
/// project label for project control. Shared by Store admission
/// (`caller_may_command`) and Router reads (`Store::agent_in_scope`).
fn scope_allows(
    connection: &Connection,
    caller_id: &str,
    caller_num: i64,
    control: AgentControl,
    project: &str,
    target_id: &str,
) -> Result<bool> {
    if target_id == caller_id {
        return Ok(true);
    }
    match control {
        AgentControl::Slf => Ok(false),
        AgentControl::Project => {
            let target = load_agent_by_num(connection, parse_agent_id(target_id)?)?;
            Ok(target.is_some_and(|agent| agent.project == project))
        }
        AgentControl::Tree => {
            let member_num = parse_agent_id(target_id)?;
            is_in_subtree_of(connection, member_num, caller_num)
        }
    }
}

impl Store {
    /// Whether `target_id` is in `caller`'s frozen scope, for Router reads.
    pub fn agent_in_scope(&self, caller: &Agent, target_id: &str) -> Result<bool> {
        scope_allows(
            &self.connection,
            &caller.id,
            parse_agent_id(&caller.id)?,
            caller.control,
            &caller.project,
            target_id,
        )
    }
}

/// Scope check for one hierarchy member: the owner commands everything; an
/// Agent commands itself, its subtree, or its project per its frozen control.
pub(super) fn caller_may_command(
    connection: &Connection,
    caller: &Caller,
    member: &Agent,
) -> Result<()> {
    let Caller::Agent {
        num: caller_num,
        id: caller_id,
        control,
        project,
        ..
    } = caller
    else {
        return Ok(());
    };
    if scope_allows(
        connection,
        caller_id,
        *caller_num,
        *control,
        project,
        &member.id,
    )? {
        Ok(())
    } else {
        Err(Error::new(
            "not_allowed",
            "command is outside the caller's scope",
        ))
    }
}

pub(super) struct ParentLink {
    pub(super) num: i64,
    pub(super) project: String,
    pub(super) root_num: i64,
    pub(super) depth: i64,
}

/// Resolve `args.parent` for a launch: the parent must exist and be
/// controlled (not observe-only or unknown), an Agent caller launches only
/// its own children, and the child inherits project and root.
pub(super) fn resolve_launch_parent(
    tx: &Transaction<'_>,
    principal: &str,
    args: &Value,
) -> Result<Option<ParentLink>> {
    let Some(parent) = args.get("parent") else {
        if let Some(rest) = principal.strip_prefix("agent:")
            && rest.split_once(':').is_some()
        {
            return Err(Error::new(
                "not_allowed",
                "Agents launch only their own children",
            ));
        }
        return Ok(None);
    };
    let parent_id = parent
        .as_str()
        .ok_or_else(|| Error::new("invalid", "launch parent must be an Agent id"))?;
    let parent_num = parse_agent_id(parent_id)?;
    let caller = parse_caller(tx, principal)?;
    if let Caller::Agent { id, .. } = &caller
        && id != parent_id
    {
        return Err(Error::new(
            "not_allowed",
            "Agents launch only their own children",
        ));
    }
    let parent_agent = load_agent_by_num(tx, parent_num)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {parent_id} was not found")))?;
    // A child needs a controlled parent: observe-only and unknown Agents
    // have no native control authority to parent new work.
    observer::require_controlled(&parent_agent)?;
    let root_num = parse_agent_id(&parent_agent.root)?;
    Ok(Some(ParentLink {
        num: parent_num,
        project: parent_agent.project.clone(),
        root_num,
        depth: parent_agent.depth,
    }))
}

/// Fanout gates for one child admission: depth, direct children, then tree
/// size. Every overflow reads `fanout_limit` and persists nothing.
pub(super) fn check_child_fanout(
    tx: &Transaction<'_>,
    parent: &ParentLink,
    limits: HierarchyLimits,
) -> Result<()> {
    if parent.depth.saturating_add(1) > limits.max_depth {
        return Err(Error::new(
            "fanout_limit",
            "child depth exceeds the hierarchy fanout limit",
        ));
    }
    if count_children_tx(tx, parent.num)? >= limits.max_children_per_agent {
        return Err(Error::new(
            "fanout_limit",
            "Agent children exceed the hierarchy fanout limit",
        ));
    }
    if count_tree_tx(tx, parent.root_num)? >= limits.max_tree_agents {
        return Err(Error::new(
            "fanout_limit",
            "tree Agents exceed the hierarchy fanout limit",
        ));
    }
    Ok(())
}

/// Frozen `control` of a launch from its profile snapshot: absent reads
/// `self`, any other value parses through the one scope parser.
pub(super) fn control_from_snapshot(snapshot: &Value) -> Result<String> {
    match snapshot.get("control") {
        None => Ok(AgentControl::Slf.as_str().to_owned()),
        Some(Value::String(control)) => AgentControl::parse(control)
            .map(|_| control.clone())
            .map_err(|_| {
                Error::new(
                    "invalid",
                    "profile_snapshot.control must be self, tree, or project",
                )
            }),
        Some(_) => Err(Error::new(
            "invalid",
            "profile_snapshot.control must be self, tree, or project",
        )),
    }
}

/// Snapshot one tree selector to its member rows, in member order. Unknown
/// roots never create Agents; scope and overflow reject before any row.
pub(super) fn snapshot_selector_members(
    tx: &Transaction<'_>,
    principal: &str,
    selector: &str,
    limits: HierarchyLimits,
) -> Result<Vec<Agent>> {
    let root_num = parse_tree_selector(selector)?;
    let caller = parse_caller(tx, principal)?;
    let root_id = format_agent_id(root_num);
    load_agent_by_num(tx, root_num)?
        .ok_or_else(|| Error::new("not_found", format!("Agent {root_id} was not found")))?;
    let member_nums = subtree_member_nums(tx, root_num, limits.max_selector_targets)?;
    if member_nums.len() as i64 > limits.max_selector_targets {
        return Err(Error::new(
            "fanout_limit",
            "selector expansion exceeds the hierarchy fanout limit",
        ));
    }
    let mut members = Vec::with_capacity(member_nums.len());
    for num in member_nums {
        let member = load_agent_by_num(tx, num)?.ok_or_else(|| {
            Error::new("internal", "selector member disappeared in its transaction")
        })?;
        caller_may_command(tx, &caller, &member)?;
        members.push(member);
    }
    Ok(members)
}

/// Derived child row identity for one expansion member.
pub(super) fn selector_child_command_id(parent_id: &str, member: &Agent) -> String {
    selector_child_id(parent_id, &member.id)
}

/// Admit one validated selector expansion: one concrete child per snapshotted
/// member with the ordinary per-target gates, then the Store-only applied
/// parent. Any live-member failure rejects the whole expansion with nothing
/// persisted; `send` skips terminal members instead of failing on them.
pub(super) fn admit_selector_expansion(
    tx: &Transaction<'_>,
    principal: &str,
    request: &SubmitCommand,
    payload_digest: &[u8],
    limits: HierarchyLimits,
    now: i64,
) -> Result<Admission> {
    let selector = request
        .selector
        .as_deref()
        .ok_or_else(|| Error::new("internal", "selector expansion has no selector"))?;
    let members = snapshot_selector_members(tx, principal, selector, limits)?;
    let mut events = Vec::new();
    let mut child_ids = Vec::with_capacity(members.len());
    // Admit members unlinked first (the parent row does not exist yet for
    // the commands.parent_command foreign key), then link them once the
    // parent is inserted, mirroring rule-action expansion.
    for member in members.iter() {
        // `send` skips terminal members instead of failing the expansion;
        // `stop` still names them (stopping a terminal Agent is a no-op row,
        // not an error). An all-terminal send applies with no children.
        if request.verb == "send" && member.state.is_terminal() {
            continue;
        }
        observer::require_controlled(member)?;
        let child_id = selector_child_command_id(&request.id, member);
        let mut member_request = SubmitCommand::new(child_id.clone(), request.verb.clone());
        member_request.target = Some(member.id.clone());
        member_request.args = request.args.clone();
        let member_digest = canonical::digest(&admission::command_semantic_payload(
            &member_request,
            member_request.args.clone(),
        )?)?;
        match request.verb.as_str() {
            "send" => admission::admit_send_member(
                tx,
                principal,
                &member_request,
                &member_digest,
                member,
                now,
                &mut events,
            )?,
            "stop" => admission::admit_stop_member(
                tx,
                principal,
                &member_request,
                &member_digest,
                member,
                now,
                &mut events,
            )?,
            _ => {
                return Err(Error::new(
                    "internal",
                    "selector expansion reached an unexpanded verb",
                ));
            }
        }
        child_ids.push(child_id);
    }
    let outcome = json!({"children": child_ids});
    let parent = CommandInsert {
        id: &request.id,
        principal,
        method: COMMAND_METHOD,
        payload_digest,
        target: None,
        selector: Some(selector),
        expected_generation: None,
        generation: None,
        expected_turn_id: None,
        parent_command: None,
        link_kind: None,
        ordinal: None,
        turn_id: None,
        request_id: None,
        probe: false,
        verb: &request.verb,
        args: &request.args,
        state: CommandState::Applied,
        outcome: Some(&outcome),
        created_at: now,
        updated_at: now,
    };
    events.push(admit_command(
        tx,
        &parent,
        json!({"command_id": request.id, "verb": request.verb}),
    )?);
    for (ordinal, child_id) in child_ids.iter().enumerate() {
        let ordinal = i64::try_from(ordinal)
            .map_err(|_| Error::new("internal", "selector expansion exceeds its ordinal bound"))?;
        let linked = tx
            .execute(
                "UPDATE commands
                 SET parent_command = ?1, link_kind = ?2, ordinal = ?3
                 WHERE id = ?4 AND parent_command IS NULL
                   AND link_kind IS NULL AND ordinal IS NULL",
                params![request.id, SELECTOR_LINK_KIND, ordinal, child_id],
            )
            .map_err(sql_error)?;
        if linked != 1 {
            return Err(Error::new(
                "source_integrity",
                "new selector child could not be bound to its parent",
            ));
        }
    }
    let command = load_command(tx, &request.id)?.ok_or_else(|| {
        Error::new(
            "internal",
            "new selector parent was not visible in its transaction",
        )
    })?;
    Ok(Admission {
        command,
        linked_command: None,
        events,
        is_new: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{NativeEvent, SourceBatch};
    use std::{
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn store() -> Store {
        Store::open(Path::new(":memory:")).unwrap()
    }

    fn launch_request(project: &str, parent: Option<&str>) -> SubmitCommand {
        let mut args = json!({"project":project, "workspace":"synthetic-workspace"});
        if let Some(parent) = parent {
            args["parent"] = json!(parent);
        }
        SubmitCommand {
            id: Ulid::new().to_string(),
            target: None,
            selector: None,
            verb: "launch".to_owned(),
            args,
            expected_generation: Some(0),
            expected_turn_id: None,
            request_id: None,
            probe: false,
        }
    }

    fn launch_child(
        store: &mut Store,
        principal: &str,
        parent: &str,
        snapshot: Value,
        now: i64,
    ) -> String {
        let mut request = launch_request("caller-project-label", Some(parent));
        request.args["profile_snapshot"] = snapshot;
        let admission = store
            .submit(principal, request, now)
            .expect("admit child launch");
        admission.command.target.expect("child Agent")
    }

    fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
        let launch_id = Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":project, "workspace":"synthetic-workspace"}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: Ulid::new().to_string(),
                        data: json!({"command_id":launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    fn agent_principal(agent: &str) -> String {
        format!("agent:{agent}:1")
    }

    fn agent_count(store: &Store) -> i64 {
        store
            .connection
            .query_row("SELECT COUNT(*) FROM agents", [], |row| row.get(0))
            .unwrap()
    }

    fn child_ids(store: &Store, parent_id: &str) -> Vec<String> {
        let parent = store.get_command(parent_id).unwrap().expect("parent row");
        store
            .list_command_children(&parent, None, 100)
            .unwrap()
            .children
            .iter()
            .map(|child| child.id.clone())
            .collect()
    }

    fn stop_agent(store: &mut Store, agent: &str, generation: i64, now: i64) {
        let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
        stop.target = Some(agent.to_owned());
        stop.expected_generation = Some(generation);
        let stop_id = stop.id.clone();
        store.submit("owner", stop, now).expect("admit stop");
        store.dispatch(&stop_id, now).expect("dispatch stop");
        store
            .settle(&stop_id, CommandState::Applied, None, now)
            .expect("settle stop");
    }

    fn ready_launched_agent(store: &mut Store, agent: &str, now: i64) {
        let launch_id: String = store
            .connection
            .query_row(
                "SELECT id FROM commands WHERE target = ?1 AND verb = 'launch'",
                params![parse_agent_id(agent).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        store.dispatch(&launch_id, now).expect("dispatch launch");
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: Ulid::new().to_string(),
                        data: json!({"command_id":launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");
    }

    #[test]
    fn migration_23_adds_hierarchy_columns_indexes_and_defaults() {
        let store = store();
        let version: i64 = store
            .connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, super::super::schema::CURRENT_SCHEMA_VERSION);
        assert_eq!(version, 26);
        let mut columns: Vec<String> = store
            .connection
            .prepare("SELECT name FROM pragma_table_info('agents')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap();
        columns.sort();
        for column in ["parent", "root", "depth", "control"] {
            assert!(
                columns.contains(&column.to_owned()),
                "agents.{column}: {columns:?}"
            );
        }
        let selector: Vec<String> = store
            .connection
            .prepare("SELECT name FROM pragma_table_info('commands')")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<String>>>()
            .unwrap();
        assert!(selector.contains(&"selector".to_owned()), "{selector:?}");
        for index in ["agents_parent", "agents_root"] {
            let found: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                    params![index],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(found, 1, "missing {index}");
        }
    }

    #[test]
    fn launch_child_inherits_project_and_root_with_a_durable_parent_link() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "inherited-project", now);
        let child = launch_child(&mut store, "owner", &parent, json!({}), now + 1);
        let child_agent = store.get_agent(&child).unwrap().expect("child row");
        assert_eq!(child_agent.parent.as_deref(), Some(parent.as_str()));
        assert_eq!(child_agent.project, "inherited-project");
        assert_eq!(child_agent.root, parent);
        assert_eq!(child_agent.depth, 1);
        let parent_agent = store.get_agent(&parent).unwrap().expect("parent row");
        assert_eq!(parent_agent.parent, None);
        assert_eq!(parent_agent.root, parent);
        assert_eq!(parent_agent.depth, 0);
        // A grandchild inherits the same root one level deeper.
        let grandchild = launch_child(&mut store, "owner", &child, json!({}), now + 2);
        let grandchild_agent = store
            .get_agent(&grandchild)
            .unwrap()
            .expect("grandchild row");
        assert_eq!(grandchild_agent.parent.as_deref(), Some(child.as_str()));
        assert_eq!(grandchild_agent.root, parent);
        assert_eq!(grandchild_agent.depth, 2);
    }

    #[test]
    fn agent_principal_launches_only_its_own_children() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "agent-tree", now);
        let other = ready_agent(&mut store, "agent-tree", now);
        let child = launch_child(
            &mut store,
            &agent_principal(&parent),
            &parent,
            json!({}),
            now + 1,
        );
        let child_agent = store.get_agent(&child).unwrap().expect("child row");
        assert_eq!(child_agent.parent.as_deref(), Some(parent.as_str()));
        assert_eq!(child_agent.project, "agent-tree");
        // Another parent, no parent, and a stale generation are all refused.
        let foreign = launch_request("agent-tree", Some(&other));
        assert_eq!(
            store
                .submit(&agent_principal(&parent), foreign, now + 2)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let rootless = launch_request("agent-tree", None);
        assert_eq!(
            store
                .submit(&agent_principal(&parent), rootless, now + 3)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let stale = launch_request("agent-tree", Some(&parent));
        assert_eq!(
            store
                .submit(&format!("agent:{parent}:0"), stale, now + 4)
                .unwrap_err()
                .code,
            "stale_generation"
        );
        assert_eq!(agent_count(&store), 3);
    }

    #[test]
    fn launch_parent_must_exist_and_be_an_agent_id() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "parent-checks", now);
        let _ = parent;
        let missing = launch_request("parent-checks", Some("a999"));
        assert_eq!(
            store.submit("owner", missing, now + 1).unwrap_err().code,
            "not_found"
        );
        let mut malformed = launch_request("parent-checks", Some("a1"));
        malformed.args["parent"] = json!("not-an-agent");
        assert_eq!(
            store.submit("owner", malformed, now + 2).unwrap_err().code,
            "invalid"
        );
        let mut typed = launch_request("parent-checks", Some("a1"));
        typed.args["parent"] = json!(7);
        assert_eq!(
            store.submit("owner", typed, now + 3).unwrap_err().code,
            "invalid"
        );
        assert_eq!(agent_count(&store), 1);
    }

    #[test]
    fn fanout_limits_reject_children_without_persisting() {
        let mut child_store = store();
        child_store
            .set_hierarchy_limits(HierarchyLimits {
                max_children_per_agent: 1,
                ..HierarchyLimits::default()
            })
            .unwrap();
        let now = now_ms();
        let parent = ready_agent(&mut child_store, "fanout", now);
        launch_child(&mut child_store, "owner", &parent, json!({}), now + 1);
        let overflow = launch_request("fanout", Some(&parent));
        assert_eq!(
            child_store
                .submit("owner", overflow, now + 2)
                .unwrap_err()
                .code,
            "fanout_limit"
        );
        assert_eq!(agent_count(&child_store), 2);

        let mut tree_store = store();
        tree_store
            .set_hierarchy_limits(HierarchyLimits {
                max_tree_agents: 2,
                ..HierarchyLimits::default()
            })
            .unwrap();
        let parent = ready_agent(&mut tree_store, "fanout", now);
        let child = launch_child(&mut tree_store, "owner", &parent, json!({}), now + 1);
        let grandchild = launch_request("fanout", Some(&child));
        assert_eq!(
            tree_store
                .submit("owner", grandchild, now + 2)
                .unwrap_err()
                .code,
            "fanout_limit"
        );
        assert_eq!(agent_count(&tree_store), 2);

        let mut depth_store = store();
        depth_store
            .set_hierarchy_limits(HierarchyLimits {
                max_depth: 1,
                ..HierarchyLimits::default()
            })
            .unwrap();
        let parent = ready_agent(&mut depth_store, "fanout", now);
        let child = launch_child(&mut depth_store, "owner", &parent, json!({}), now + 1);
        let deep = launch_request("fanout", Some(&child));
        assert_eq!(
            depth_store.submit("owner", deep, now + 2).unwrap_err().code,
            "fanout_limit"
        );
        assert_eq!(agent_count(&depth_store), 2);
    }

    #[test]
    fn above_limit_children_stay_visible_with_no_widened_identity() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "visible", now);
        let first = launch_child(&mut store, "owner", &parent, json!({}), now + 1);
        let second = launch_child(&mut store, "owner", &parent, json!({}), now + 2);
        store
            .set_hierarchy_limits(HierarchyLimits {
                max_children_per_agent: 1,
                ..HierarchyLimits::default()
            })
            .unwrap();
        // Limits gate admission only: every row still reads back unchanged.
        for id in [&parent, &first, &second] {
            let agent = store.get_agent(id).unwrap().expect("visible row");
            assert_eq!(agent.control, AgentControl::Slf);
        }
        let listed = store.list_agents(None, 100).unwrap();
        assert_eq!(listed.len(), 3);
        let overflow = launch_request("visible", Some(&parent));
        assert_eq!(
            store.submit("owner", overflow, now + 3).unwrap_err().code,
            "fanout_limit"
        );
    }

    #[test]
    fn launch_control_freezes_from_its_profile_snapshot() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "control", now);
        let tree = launch_child(
            &mut store,
            "owner",
            &parent,
            json!({"control": "tree"}),
            now + 1,
        );
        assert_eq!(
            store.get_agent(&tree).unwrap().expect("tree row").control,
            AgentControl::Tree
        );
        let project = launch_child(
            &mut store,
            "owner",
            &parent,
            json!({"control": "project"}),
            now + 2,
        );
        assert_eq!(
            store
                .get_agent(&project)
                .unwrap()
                .expect("project row")
                .control,
            AgentControl::Project
        );
        let mut bad = launch_request("control", Some(&parent));
        bad.args["profile_snapshot"] = json!({"control": "fleet"});
        assert_eq!(
            store.submit("owner", bad, now + 3).unwrap_err().code,
            "invalid"
        );
    }

    #[test]
    fn selector_snapshot_excludes_a_child_created_after_admission() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "snapshot", now);
        let first = launch_child(&mut store, "owner", &parent, json!({}), now + 1);
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "stop");
        request.selector = Some(format!("tree:{parent}"));
        let parent_id = request.id.clone();
        let admission = store
            .submit("owner", request, now + 2)
            .expect("expand stop");
        assert!(admission.is_new);
        assert_eq!(admission.command.state, CommandState::Applied);
        assert_eq!(
            admission.command.selector.as_deref(),
            Some(format!("tree:{parent}").as_str())
        );
        assert_eq!(
            child_ids(&store, &parent_id),
            vec![
                format!("{parent_id}/{parent}"),
                format!("{parent_id}/{first}")
            ]
        );
        // A child created after admission is not part of the fixed set.
        let second = launch_child(&mut store, "owner", &parent, json!({}), now + 3);
        assert_eq!(child_ids(&store, &parent_id).len(), 2);
        let late: Vec<String> = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM commands WHERE parent_command = ?1",
                params![parent_id],
                |row| row.get(0),
            )
            .map(|count: i64| vec![count.to_string()])
            .unwrap();
        assert_eq!(late, vec!["2".to_owned()]);
        let _ = second;
    }

    #[test]
    fn selector_send_expands_with_per_member_gates_and_replays() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "broadcast", now);
        let child = launch_child(&mut store, "owner", &parent, json!({}), now + 1);
        // A created (undispatched) member fails the ordinary send gate, so the
        // whole expansion is rejected and nothing is persisted.
        let mut blocked = SubmitCommand::new(Ulid::new().to_string(), "send");
        blocked.selector = Some(format!("tree:{parent}"));
        blocked.args = json!({"text": "hello tree"});
        assert_eq!(
            store.submit("owner", blocked, now + 2).unwrap_err().code,
            "bad_state"
        );
        let commands: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM commands", [], |row| row.get(0))
            .unwrap();
        assert_eq!(commands, 2);

        // Dispatch the child launch, then the same shape expands cleanly.
        let child_launch: String = store
            .connection
            .query_row(
                "SELECT id FROM commands WHERE target = ?1 AND verb = 'launch'",
                params![parse_agent_id(&child).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        store
            .dispatch(&child_launch, now + 3)
            .expect("dispatch child");
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "send");
        request.selector = Some(format!("tree:{parent}"));
        request.args = json!({"text": "hello tree"});
        let parent_id = request.id.clone();
        let admission = store
            .submit("owner", request.clone(), now + 4)
            .expect("expand send");
        assert!(admission.is_new);
        assert_eq!(
            child_ids(&store, &parent_id),
            vec![
                format!("{parent_id}/{parent}"),
                format!("{parent_id}/{child}")
            ]
        );
        // Same id replays the retained parent without duplicating children.
        let replay = store.submit("owner", request, now + 5).expect("replay");
        assert!(!replay.is_new);
        assert_eq!(replay.command.id, parent_id);
        assert_eq!(child_ids(&store, &parent_id).len(), 2);
    }

    #[test]
    fn selector_shape_and_scope_reject_before_any_expansion() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "scope", now);
        let outsider = ready_agent(&mut store, "other", now);
        let child = launch_child(&mut store, "owner", &parent, json!({}), now + 1);

        // Unknown verbs, targets, bindings, and grammars never expand.
        for verb in ["launch", "steer", "interrupt", "approve", "resume"] {
            let mut request = SubmitCommand::new(Ulid::new().to_string(), verb);
            request.selector = Some(format!("tree:{parent}"));
            assert_eq!(
                store.submit("owner", request, now + 2).unwrap_err().code,
                "unsupported",
                "{verb}"
            );
        }
        let mut targeted = SubmitCommand::new(Ulid::new().to_string(), "stop");
        targeted.selector = Some(format!("tree:{parent}"));
        targeted.target = Some(parent.clone());
        assert_eq!(
            store.submit("owner", targeted, now + 3).unwrap_err().code,
            "invalid"
        );
        let mut bound = SubmitCommand::new(Ulid::new().to_string(), "stop");
        bound.selector = Some(format!("tree:{parent}"));
        bound.expected_generation = Some(1);
        assert_eq!(
            store.submit("owner", bound, now + 4).unwrap_err().code,
            "invalid"
        );
        for selector in ["project:scope", "label:team"] {
            let mut future = SubmitCommand::new(Ulid::new().to_string(), "stop");
            future.selector = Some(selector.to_owned());
            assert_eq!(
                store.submit("owner", future, now + 5).unwrap_err().code,
                "unsupported",
                "{selector}"
            );
        }
        let mut missing = SubmitCommand::new(Ulid::new().to_string(), "stop");
        missing.selector = Some("tree:a999".to_owned());
        assert_eq!(
            store.submit("owner", missing, now + 6).unwrap_err().code,
            "not_found"
        );

        // A self-scoped Agent cannot fan out past itself; an outsider's
        // subtree is outside every non-owner scope.
        let mut selfish = SubmitCommand::new(Ulid::new().to_string(), "stop");
        selfish.selector = Some(format!("tree:{parent}"));
        assert_eq!(
            store
                .submit(&agent_principal(&parent), selfish, now + 7)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let mut foreign = SubmitCommand::new(Ulid::new().to_string(), "stop");
        foreign.selector = Some(format!("tree:{outsider}"));
        assert_eq!(
            store
                .submit(&agent_principal(&parent), foreign, now + 8)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let _ = (child, outsider);
    }

    #[test]
    fn agent_scope_follows_its_frozen_control() {
        let mut store = store();
        let now = now_ms();
        let manager = ready_agent(&mut store, "managed", now);
        // Promote the manager row to tree control through a frozen snapshot:
        // control is written at launch, so relaunch under a tree profile.
        let tree_manager = launch_child(
            &mut store,
            "owner",
            &manager,
            json!({"control": "tree"}),
            now + 1,
        );
        store
            .dispatch(
                &store
                    .connection
                    .query_row(
                        "SELECT id FROM commands WHERE target = ?1 AND verb = 'launch'",
                        params![parse_agent_id(&tree_manager).unwrap()],
                        |row| row.get::<_, String>(0),
                    )
                    .unwrap(),
                now + 2,
            )
            .expect("dispatch tree manager");
        let leaf = launch_child(&mut store, "owner", &tree_manager, json!({}), now + 3);
        let outsider = ready_agent(&mut store, "managed", now + 4);

        // Tree control fans out over its own subtree only.
        let mut own = SubmitCommand::new(Ulid::new().to_string(), "stop");
        own.selector = Some(format!("tree:{tree_manager}"));
        let own_id = own.id.clone();
        store
            .submit(&agent_principal(&tree_manager), own, now + 5)
            .expect("tree manager stops its subtree");
        assert_eq!(child_ids(&store, &own_id).len(), 2);
        let mut foreign = SubmitCommand::new(Ulid::new().to_string(), "stop");
        foreign.selector = Some(format!("tree:{outsider}"));
        assert_eq!(
            store
                .submit(&agent_principal(&tree_manager), foreign, now + 6)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        // A manager cannot reach past its subtree root either.
        let mut wider = SubmitCommand::new(Ulid::new().to_string(), "stop");
        wider.selector = Some(format!("tree:{manager}"));
        assert_eq!(
            store
                .submit(&agent_principal(&tree_manager), wider, now + 7)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let _ = leaf;
    }

    #[test]
    fn launch_parent_must_be_controlled() {
        let mut store = store();
        let now = now_ms();
        let admission = store
            .submit("owner", launch_request("controlled-parent", None), now)
            .expect("admit parent launch");
        let parent = admission.command.target.expect("parent Agent");
        let parent_num = parse_agent_id(&parent).unwrap();
        let token: Option<String> = store
            .connection
            .query_row(
                "SELECT token FROM agents WHERE id = ?1",
                params![parent_num],
                |row| row.get(0),
            )
            .unwrap();
        // An observe-only (unknown) parent cannot take children.
        store
            .connection
            .execute(
                "UPDATE agents SET observe_only = 1, state = 'unknown', token = NULL WHERE id = ?1",
                params![parent_num],
            )
            .unwrap();
        let observed = launch_request("controlled-parent", Some(&parent));
        assert_eq!(
            store.submit("owner", observed, now + 1).unwrap_err().code,
            "not_allowed"
        );
        assert_eq!(agent_count(&store), 1);
        // The same parent controlled again takes children.
        store
            .connection
            .execute(
                "UPDATE agents SET observe_only = 0, state = 'created', token = ?1 WHERE id = ?2",
                params![token, parent_num],
            )
            .unwrap();
        let child = launch_child(&mut store, "owner", &parent, json!({}), now + 2);
        assert_eq!(
            store
                .get_agent(&child)
                .unwrap()
                .expect("child row")
                .parent
                .as_deref(),
            Some(parent.as_str())
        );
    }

    #[test]
    fn terminal_children_do_not_occupy_fanout() {
        let mut store = store();
        store
            .set_hierarchy_limits(HierarchyLimits {
                max_children_per_agent: 1,
                max_tree_agents: 3,
                ..HierarchyLimits::default()
            })
            .unwrap();
        let now = now_ms();
        let parent = ready_agent(&mut store, "fanout-terminal", now);
        let first = launch_child(&mut store, "owner", &parent, json!({}), now + 1);
        stop_agent(&mut store, &first, 0, now + 2);
        assert!(
            store
                .get_agent(&first)
                .unwrap()
                .expect("child row")
                .state
                .is_terminal()
        );
        // The stopped child leaves both counts: a replacement fits under the
        // per-parent bound, and a grandchild fits under the tree bound.
        let second = launch_child(&mut store, "owner", &parent, json!({}), now + 3);
        let grandchild = launch_child(&mut store, "owner", &second, json!({}), now + 4);
        let _ = grandchild;
        // Live children still count: a further direct child is rejected.
        let overflow = launch_request("fanout-terminal", Some(&parent));
        assert_eq!(
            store.submit("owner", overflow, now + 5).unwrap_err().code,
            "fanout_limit"
        );
    }

    #[test]
    fn selector_send_skips_terminal_members_but_stop_names_them() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "skip-terminal", now);
        let child = launch_child(&mut store, "owner", &parent, json!({}), now + 1);
        ready_launched_agent(&mut store, &child, now + 2);
        stop_agent(&mut store, &child, 1, now + 3);
        // `send` expands over the live parent only.
        let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
        send.selector = Some(format!("tree:{parent}"));
        send.args = json!({"text": "hello live tree"});
        let send_id = send.id.clone();
        store.submit("owner", send, now + 4).expect("expand send");
        assert_eq!(
            child_ids(&store, &send_id),
            vec![format!("{send_id}/{parent}")]
        );
        // `stop` still names every member including the stopped child.
        let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
        stop.selector = Some(format!("tree:{parent}"));
        let stop_id = stop.id.clone();
        store.submit("owner", stop, now + 5).expect("expand stop");
        assert_eq!(
            child_ids(&store, &stop_id),
            vec![format!("{stop_id}/{parent}"), format!("{stop_id}/{child}")]
        );
    }

    #[test]
    fn migration_23_backfills_root_for_legacy_agents() {
        let path = std::env::temp_dir().join(format!(
            "eliot-hierarchy-backfill-{}",
            Ulid::new().to_string()
        ));
        {
            let legacy_store = Store::open(&path).unwrap();
            assert_eq!(super::super::schema::CURRENT_SCHEMA_VERSION, 26);
            drop(legacy_store);
            // Downgrade to the v22 shape with two legacy rows, then reopen so
            // the v23 block backfills them exactly as it would on upgrade.
            // The v24 claims table postdates the downgrade target, so it
            // goes too; the reopen migrates 22 through the current version.
            let legacy = rusqlite::Connection::open(&path).unwrap();
            legacy
                .execute_batch(
                    "DROP INDEX agents_parent;
                     DROP INDEX agents_root;
                     ALTER TABLE agents DROP COLUMN parent;
                     ALTER TABLE agents DROP COLUMN root;
                     ALTER TABLE agents DROP COLUMN depth;
                     ALTER TABLE agents DROP COLUMN control;
                     ALTER TABLE commands DROP COLUMN selector;
                     DELETE FROM schema_migrations WHERE version = 23;
                     DROP TABLE claims;
                     DELETE FROM schema_migrations WHERE version = 24;
                     DROP TABLE owned_exits;
                     DELETE FROM schema_migrations WHERE version = 25;
                     DROP TABLE native_child_registrations;
                     DELETE FROM schema_migrations WHERE version = 26;
                     INSERT INTO agents(generation, project, harness, profile,
                                        workspace, state, created_at, updated_at)
                     VALUES (1, 'legacy', 'fake', 'fake',
                             'synthetic-workspace', 'idle', 1, 1),
                            (0, 'legacy', 'fake', 'fake',
                             'synthetic-workspace', 'created', 1, 1);",
                )
                .unwrap();
            let version: i64 = legacy
                .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(version, 22);
        }
        let store = Store::open(&path).unwrap();
        let version: i64 = store
            .connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(version, 26);
        for id in ["a1", "a2"] {
            let agent = store.get_agent(id).unwrap().expect("legacy row");
            assert_eq!(agent.root, id);
            assert_eq!(agent.parent, None);
            assert_eq!(agent.depth, 0);
            assert_eq!(agent.control, AgentControl::Slf);
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn parent_terminal_leaves_child_running_without_expansion() {
        let mut store = store();
        let now = now_ms();
        let parent = ready_agent(&mut store, "settle", now);
        let child = launch_child(&mut store, "owner", &parent, json!({}), now + 1);
        ready_launched_agent(&mut store, &child, now + 2);
        let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
        stop.target = Some(parent.clone());
        stop.expected_generation = Some(1);
        let stop_id = stop.id.clone();
        store
            .submit("owner", stop, now + 3)
            .expect("admit parent stop");
        // A targeted stop expands to nothing: no children, no widened effect.
        let parent_row = store.get_command(&stop_id).unwrap().expect("stop row");
        assert_eq!(
            store
                .list_command_children(&parent_row, None, 100)
                .unwrap()
                .children
                .len(),
            0
        );
        store
            .dispatch(&stop_id, now + 4)
            .expect("dispatch parent stop");
        store
            .settle(&stop_id, CommandState::Applied, None, now + 5)
            .expect("settle parent stop");
        let settled = store.get_agent(&parent).unwrap().expect("parent row");
        assert_eq!(settled.state, AgentState::Stopped);
        let running = store.get_agent(&child).unwrap().expect("child row");
        assert_eq!(running.state, AgentState::Idle);
        assert_eq!(running.parent.as_deref(), Some(parent.as_str()));
        assert_eq!(running.root, parent);
    }
}
