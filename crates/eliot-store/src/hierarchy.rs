//! Controller-launched children and tree selectors (L5.3 part 1).
//!
//! Owning contracts: `docs/spec/coordination.md` (claims and hierarchy,
//! prompt guidance) and the hierarchy parts of `core.md`/`configuration.md`.
//! A child inherits its parent's project scope and tree root; the parent link
//! is durable. Fanout limits gate launch and re-parent admission and never hide an
//! Agent row that already exists. A tree selector expands once at admission
//! to concrete members using the existing Store-only parent plus ordered
//! children representation (`link_kind = 'selector'`); the parent applied
//! means expansion, not children finished.

use super::*;
use eliot_core::api::{HierarchyLimits, parse_tree_selector, selector_child_id};

pub(super) const SELECTOR_LINK_KIND: &str = "selector";
/// Bound on parent-chain walks and tree-scope listing; creation depth is
/// fanout-limited, so this is only defense against legacy rows, never a
/// reachable depth. Listing, totals and direct reads share this bound.
pub(crate) const ANCESTOR_WALK_CAP: i64 = 4096;

pub(super) enum Caller {
    Owner,
    Agent {
        num: i64,
        id: String,
        control: AgentControl,
        project: String,
    },
}

impl Caller {
    fn require_child_launch_control(&self) -> Result<()> {
        if let Self::Agent { id, control, .. } = self
            && !matches!(control, AgentControl::Tree | AgentControl::Project)
        {
            return Err(Error::new(
                "not_allowed",
                format!(
                    "{id} has control={} and cannot launch children. Ask its manager.",
                    control.as_str()
                ),
            ));
        }
        Ok(())
    }
}

/// Resolve a live Owner or Agent principal inside the writer transaction.
/// The Agent projection's `own_identity` reflects the current token and
/// control state without retrieving the credential itself.
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
        if agent.generation != generation || agent.is_terminal() {
            return Err(Error::new(
                "stale_generation",
                "caller generation is no longer live and current",
            ));
        }
        if !agent.own_identity {
            return Err(Error::new("not_allowed", "caller identity was revoked"));
        }
        return Ok(Caller::Agent {
            num,
            project: agent.project_scope().to_owned(),
            id: agent.id,
            control: agent.control,
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
pub(crate) fn subtree_member_nums(
    connection: &Connection,
    root_num: i64,
    limit: i64,
) -> Result<Vec<i64>> {
    subtree_member_nums_inner(connection, root_num, limit, false)
}

pub(super) fn controlled_subtree_member_nums(
    connection: &Connection,
    root_num: i64,
    limit: i64,
) -> Result<Vec<i64>> {
    subtree_member_nums_inner(connection, root_num, limit, true)
}

fn subtree_member_nums_inner(
    connection: &Connection,
    root_num: i64,
    limit: i64,
    controlled_only: bool,
) -> Result<Vec<i64>> {
    let mut statement = connection
        .prepare(
            "WITH RECURSIVE subtree(id) AS (
                 SELECT ?1
                 UNION
                 SELECT a.id FROM agents a JOIN subtree s ON a.parent = s.id
                 WHERE NOT ?3 OR a.observe_only = 0
             )
             SELECT id FROM subtree ORDER BY id LIMIT ?2",
        )
        .map_err(sql_error)?;
    let rows = statement
        .query_map(
            params![root_num, limit.saturating_add(1), controlled_only],
            |row| row.get(0),
        )
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
pub(super) fn scope_allows(
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
            Ok(target.is_some_and(|agent| agent.project_scope() == project))
        }
        AgentControl::Tree => {
            let member_num = parse_agent_id(target_id)?;
            is_in_subtree_of(connection, member_num, caller_num)
        }
    }
}

impl Store {
    /// Check the live caller before launch preparation. Admission repeats
    /// the same policy inside the writer transaction.
    pub fn authorize_agent_child_launch(&self, principal: &str) -> Result<()> {
        self.ensure_committed_reads()?;
        parse_caller(&self.connection, principal)?.require_child_launch_control()
    }

    /// Whether `target_id` is in `caller`'s frozen scope, for Router reads.
    pub fn agent_in_scope(&self, caller: &Agent, target_id: &str) -> Result<bool> {
        scope_allows(
            &self.connection,
            &caller.id,
            parse_agent_id(&caller.id)?,
            caller.control,
            caller.project_scope(),
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
    pub(super) project_scope: String,
    pub(super) chat: bool,
    pub(super) root_num: i64,
    pub(super) depth: i64,
    pub(super) profile: String,
    pub(super) harness: String,
    pub(super) workspace: Option<String>,
    pub(super) profile_snapshot: Value,
    pub(super) control: AgentControl,
}

/// Resolve `args.parent` for a launch: the parent must exist and be
/// controlled (not observe-only or unknown), a tree/project-control Agent
/// caller launches only its own children, and the child inherits project and root.
pub(super) fn resolve_launch_parent(
    tx: &Transaction<'_>,
    principal: &str,
    args: &Value,
) -> Result<Option<ParentLink>> {
    let parent = args.get("parent");
    if parent.is_none() && !principal.starts_with("agent:") {
        return Ok(None);
    }
    let caller = parse_caller(tx, principal)?;
    caller.require_child_launch_control()?;
    let parent =
        parent.ok_or_else(|| Error::new("not_allowed", "Agents launch only their own children"))?;
    let parent_id = parent
        .as_str()
        .ok_or_else(|| Error::new("invalid", "launch parent must be an Agent id"))?;
    let parent_num = parse_agent_id(parent_id)?;
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
        project_scope: parent_agent.project_scope().to_owned(),
        project: parent_agent.project,
        chat: parent_agent.chat,
        root_num,
        depth: parent_agent.depth,
        profile: parent_agent.profile.clone(),
        harness: parent_agent.harness.clone(),
        workspace: parent_agent.workspace.clone(),
        profile_snapshot: parent_agent.profile_snapshot.clone(),
        control: parent_agent.control,
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

/// The exception to the Store admission guard that a launch
/// `profile_snapshot` must not carry a dispatch-derived
/// `native_store_binding`: an authenticated nonterminal Agent launching its own
/// controlled child, either reusing the parent's frozen snapshot verbatim
/// (same profile, harness, workspace, full snapshot, control narrowed or
/// equal) or naming a profile from the parent snapshot's captured
/// `child_profiles` allow-list (same harness and workspace, control narrowed
/// or equal, carried binding exactly equal to the parent's). The binding
/// still flows through the dispatch-owned path, and any drift surfaces as
/// `source_integrity` there; the Router freezes and authorizes the snapshot
/// before Store admission. Owner, Run, and every other caller keep the
/// existing rejection.
pub(super) fn admission_allows_carried_parent_binding(
    tx: &Transaction<'_>,
    principal: &str,
    args: &Value,
) -> Result<bool> {
    let caller = parse_caller(tx, principal)?;
    let Caller::Agent { id: caller_id, .. } = &caller else {
        return Ok(false);
    };
    let Some(parent_id) = args.get("parent").and_then(Value::as_str) else {
        return Ok(false);
    };
    if caller_id != parent_id {
        return Ok(false);
    }
    let Some(parent) = resolve_launch_parent(tx, principal, args)? else {
        return Ok(false);
    };
    let child_control =
        control_from_snapshot(args.get("profile_snapshot").unwrap_or(&Value::Null))?;
    let child_control = AgentControl::parse(&child_control)?;
    if child_control > parent.control {
        return Ok(false);
    }
    if !args
        .get("harness")
        .and_then(Value::as_str)
        .is_some_and(|harness| harness == parent.harness)
        || !args
            .get("workspace")
            .and_then(Value::as_str)
            .is_some_and(|workspace| parent.workspace.as_deref() == Some(workspace))
    {
        return Ok(false);
    }
    if args
        .get("profile")
        .and_then(Value::as_str)
        .is_some_and(|name| name == parent.profile)
        && args.get("profile_snapshot") == Some(&parent.profile_snapshot)
    {
        return Ok(true);
    }
    let Some(child_snapshot) = args.get("profile_snapshot") else {
        return Ok(false);
    };
    // A carried binding can only ever be the parent's own: the Router
    // inherits it verbatim, so anything else is not a Router-frozen child.
    if child_snapshot.get("native_store_binding")
        != parent.profile_snapshot.get("native_store_binding")
    {
        return Ok(false);
    }
    Ok(args
        .get("profile")
        .and_then(Value::as_str)
        .is_some_and(|name| {
            name != parent.profile
                && parent
                    .profile_snapshot
                    .get("child_profiles")
                    .and_then(Value::as_array)
                    .is_some_and(|allowed| allowed.iter().any(|entry| entry.as_str() == Some(name)))
        }))
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

/// Admit one validated selector expansion: one concrete child per snapshotted
/// member with the ordinary per-target gates, then the Store-only applied
/// parent. Any live-member failure rejects the whole expansion with nothing
/// persisted; `send` skips terminal members instead of failing on them, and
/// `stop` skips observe-only members instead of failing on them.
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
    let mut skipped = Vec::new();
    // Admit members unlinked first (the parent row does not exist yet for
    // the commands.parent_command foreign key), then link them once the
    // parent is inserted, mirroring rule-action expansion.
    for member in members.iter() {
        // `send` skips terminal members instead of failing the expansion;
        // `stop` still names terminal members (stopping a terminal Agent is
        // a no-op row, not an error) but skips observe-only native children,
        // whose lifecycle belongs to their manager/native harness. An
        // all-terminal send applies with no children.
        if request.verb == "send" && member.state.is_terminal() {
            continue;
        }
        if request.verb == "stop" && member.observe_only {
            skipped.push(json!({"agent": member.id, "reason": "observe_only"}));
            continue;
        }
        observer::require_controlled(member)?;
        let child_id = selector_child_id(&request.id, &member.id);
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
    let outcome = json!({"children": child_ids, "skipped": skipped});
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
