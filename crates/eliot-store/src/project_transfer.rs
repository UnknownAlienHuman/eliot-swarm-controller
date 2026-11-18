//! Transactional Owner transfer over the current assignment and the existing H1/H2 edits.
use super::*;
use eliot_core::api::{
    GoalStatus, HierarchyLimits, PROJECT_TRANSFER_METHOD, ProjectAssignment,
    ProjectManagerIdentity, ProjectTransferAction, ProjectTransferBlocker, ProjectTransferOutcome,
    ProjectTransferRequest, ProjectTransferResult, ReparentRequest,
};
use project_assignment::{AssignmentWrite, read_assignment, write_next_assignment};

pub(super) const LAUNCH_ARG: &str = "project_transfer_launch";

struct Intent {
    id: String,
    saved_context: Option<String>,
    launch: Option<String>,
}

fn intent(conn: &Connection, project: &str) -> Result<Option<Intent>> {
    conn.query_row(
        "SELECT intent, saved_context, launch_command
         FROM project_transfers WHERE project=?1",
        [project],
        |row| {
            Ok(Intent {
                id: row.get(0)?,
                saved_context: row.get(1)?,
                launch: row.get(2)?,
            })
        },
    )
    .optional()
    .map_err(sql_error)
}

pub(super) fn require_no_intent(conn: &Connection, project: &str) -> Result<()> {
    if let Some(active) = intent(conn, project)? {
        return Err(Error::new(
            "transfer_in_progress",
            format!("finish or cancel project transfer {}", active.id),
        ));
    }
    Ok(())
}

pub(super) fn require_deletable(tx: &Transaction<'_>, agent: &Agent) -> Result<()> {
    let assigned_or_staged: bool = db!(tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM project_transfers p JOIN project_assignments a ON p.project=a.project
                       LEFT JOIN commands c ON c.id=p.launch_command
                       WHERE p.project=?1 AND (a.manager=?2 OR c.target=?2))",
        params![agent.project, parse_agent_id(&agent.id)?], |row| row.get(0),
    ));
    if assigned_or_staged {
        return Err(Error::new(
            "transfer_in_progress",
            "cancel or complete transfer before deleting this manager",
        ));
    }
    Ok(())
}

fn identity(agent: &Agent) -> ProjectManagerIdentity {
    ProjectManagerIdentity::Local {
        agent: agent.id.clone(),
        generation: agent.generation,
    }
}

fn source(conn: &Connection, assignment: &ProjectAssignment) -> Result<Agent> {
    let Some(ProjectManagerIdentity::Local { agent, generation }) = &assignment.manager else {
        return Err(Error::new(
            "bad_state",
            "project requires a current local manager",
        ));
    };
    let current = exact_agent(conn, agent, *generation)?;
    if current.parent.is_some()
        || current.root != current.id
        || current.project != assignment.project
    {
        return Err(Error::new(
            "bad_state",
            "the assigned manager is no longer the project root",
        ));
    }
    Ok(current)
}

fn exact_agent(conn: &Connection, id: &str, generation: i64) -> Result<Agent> {
    let agent = load_agent_by_num(conn, parse_agent_id(id)?)?
        .ok_or_else(|| Error::new("not_found", "transfer manager was not found"))?;
    if agent.generation != generation {
        return Err(Error::new(
            "stale_generation",
            "transfer manager generation changed",
        ));
    }
    Ok(agent)
}

fn required_intent(conn: &Connection, project: &str, id: &str) -> Result<Intent> {
    intent(conn, project)?
        .filter(|current| current.id == id)
        .ok_or_else(|| Error::new("stale_assignment", "transfer intent is no longer current"))
}

fn current_assignment(
    conn: &Connection,
    request: &ProjectTransferRequest,
) -> Result<ProjectAssignment> {
    let assignment = read_assignment(conn, &request.project)?;
    if assignment.revision != request.expected_revision {
        return Err(Error::new(
            "stale_assignment",
            "project assignment revision changed",
        ));
    }
    Ok(assignment)
}

impl Store {
    pub fn replay_project_transfer(
        &self,
        request: &ProjectTransferRequest,
        now: i64,
    ) -> Result<Option<ProjectTransferResult>> {
        self.ensure_committed_reads()?;
        AssignmentWrite::new(&request.id, PROJECT_TRANSFER_METHOD, "owner", request, now)?
            .replay(&self.connection)
    }

    pub fn project_transfer_launch_source(
        &self,
        request: &ProjectTransferRequest,
        intent_id: &str,
    ) -> Result<Agent> {
        self.ensure_committed_reads()?;
        required_intent(&self.connection, &request.project, intent_id)?;
        source(
            &self.connection,
            &current_assignment(&self.connection, request)?,
        )
    }

    pub fn project_transfer(
        &mut self,
        request: &ProjectTransferRequest,
        launch: Option<Result<SubmitCommand>>,
        policy: &core::SecretPolicy,
        now: i64,
    ) -> Result<Change<ProjectTransferResult>> {
        request.validate()?;
        let payload = serde_json::to_value(request)
            .map_err(|_| Error::new("internal", "cannot encode project transfer request"))?;
        let write =
            AssignmentWrite::new(&request.id, PROJECT_TRANSFER_METHOD, "owner", request, now)?;
        let limits = self.hierarchy_limits;
        let capacity_policy = self.capacity_policy.clone();
        self.change_project_assignment(write, |tx| {
            if policy.contains_value(&payload) {
                return Err(Error::new("secret_in_payload", "project transfer contains a known credential"));
            }
            let assignment = current_assignment(tx, request)?;
            let source_identity = assignment.manager.clone().ok_or_else(|| Error::new("bad_state", "project requires a current manager"))?;
            let mut value = ProjectTransferResult {
                id: request.id.clone(),
                intent: request.id.clone(),
                outcome: ProjectTransferOutcome::Blocked,
                assignment,
                source: source_identity,
                destination: None,
                launch_command: None,
                blockers: Vec::new(),
                saved_context: None,
                moved: Vec::new(),
                claims: Vec::new(),
                goal_revision: None,
            };
            let mut events = Vec::new();
            match &request.action {
                ProjectTransferAction::Begin { saved_context } => {
                    let from = source(tx, &value.assignment)?;
                    require_no_intent(tx, &request.project)?;
                    if saved_context.is_some() && from.is_terminal() {
                        return Err(Error::new("bad_state", "record the continuation before stopping the source"));
                    }
                    db!(tx.execute(
                        "INSERT INTO project_transfers(project,intent,saved_context) VALUES (?1,?2,?3)",
                        params![request.project, request.id, saved_context],
                    ));
                    value.saved_context = saved_context.clone();
                    value.outcome = ProjectTransferOutcome::Draining;
                    value.blockers = blockers(tx, &from, None, limits.max_selector_targets)?;
                }
                ProjectTransferAction::Cancel { intent } => {
                    let transfer = required_intent(tx, &request.project, intent)?;
                    value.intent = transfer.id;
                    value.launch_command = transfer.launch;
                    if let Some(launch) = replacement_occupies_workspace(tx, value.launch_command.as_deref())? {
                        value.blockers.push(blocker("replacement_custody_open", Some(launch.id)));
                    } else {
                        delete_intent(tx, &request.project)?;
                        value.outcome = ProjectTransferOutcome::Cancelled;
                    }
                }
                ProjectTransferAction::Launch { intent, profile } => {
                    let from = source(tx, &value.assignment)?;
                    let transfer = required_intent(tx, &request.project, intent)?;
                    value.intent = transfer.id.clone();
                    stage_launch(tx, limits, &from, &transfer, profile, launch, now, &mut value, &mut events, capacity_policy.as_deref())?;
                }
                ProjectTransferAction::Commit { intent, destination } => {
                    let from = source(tx, &value.assignment)?;
                    let transfer = required_intent(tx, &request.project, intent)?;
                    value.intent = transfer.id;
                    value.launch_command = transfer.launch;
                    value.destination = Some(destination.clone());
                    value.saved_context = transfer.saved_context;
                    let ProjectManagerIdentity::Local { agent, generation } = destination else {
                        return Err(Error::new("unsupported", "remote project transfer"));
                    };
                    let to = exact_agent(tx, agent, *generation)?;
                    require_destination(tx, &from, &to)?;
                    value.blockers = blockers(tx, &from, Some(&to), limits.max_selector_targets)?;
                    if let Some(launch) = replacement_occupies_workspace(tx, value.launch_command.as_deref())?
                        && (launch.state != CommandState::Applied || launch.target.as_deref() != Some(&to.id))
                    {
                        value.blockers.push(blocker("replacement_custody_open", Some(launch.id)));
                    }
                    if value.blockers.is_empty() {
                        db!(tx.execute_batch("SAVEPOINT project_transfer_commit"));
                        match commit_transfer(tx, &from, &to, &mut value, limits, policy, now, capacity_policy.as_deref()) {
                            Ok(committed) => {
                                db!(tx.execute_batch("RELEASE project_transfer_commit"));
                                events = committed;
                                value.outcome = ProjectTransferOutcome::Transferred;
                            }
                            Err(error) if matches!(error.code.as_str(), "busy" | "bad_state" | "claim_limit" | "fanout_limit") => {
                                db!(tx.execute_batch("ROLLBACK TO project_transfer_commit; RELEASE project_transfer_commit"));
                                value.blockers.push(blocker(&error.code, None));
                            }
                            Err(error) if error.code == "capacity_unavailable" => {
                                db!(tx.execute_batch("ROLLBACK TO project_transfer_commit; RELEASE project_transfer_commit"));
                                value.blockers.push(blocker(&error.code, Some(*error.message)));
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            Ok((value, events))
        })
    }
}

fn delete_intent(tx: &Transaction<'_>, project: &str) -> Result<()> {
    db!(tx.execute("DELETE FROM project_transfers WHERE project=?1", [project]));
    Ok(())
}

fn blocker(code: &str, subject: Option<String>) -> ProjectTransferBlocker {
    ProjectTransferBlocker {
        code: code.to_owned(),
        subject,
    }
}

fn source_custody_closed(conn: &Connection, from: &Agent) -> Result<bool> {
    // Unix live releases explicitly allow escaped descendants. Swarm cannot
    // transfer a writable workspace using that process closure as proof.
    if !cfg!(windows) || !from.is_terminal() {
        return Ok(false);
    }
    let num = parse_agent_id(&from.id)?;
    let outstanding: bool = db!(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM processes
           WHERE state <> 'exited' AND (owner GLOB ?1 OR owner GLOB ?2))",
        params![
            format!("agent:{}#*", from.id),
            format!("workspace:{}#*", from.id)
        ],
        |row| row.get(0),
    ));
    Ok(!outstanding && capacity::has_owned_exit_and_reader_drain_tx(conn, num, from.generation)?)
}

fn blockers(
    tx: &Transaction<'_>,
    from: &Agent,
    to: Option<&Agent>,
    limit: i64,
) -> Result<Vec<ProjectTransferBlocker>> {
    let mut found = Vec::new();
    let from_num = parse_agent_id(&from.id)?;
    let to_num = to.map(|a| parse_agent_id(&a.id)).transpose()?;
    for agent in std::iter::once(from).chain(to) {
        let is_source = agent.id == from.id;
        let num = parse_agent_id(&agent.id)?;
        if (!is_source || !agent.is_terminal()) && agent.state != AgentState::Idle
            || agent.active_turn.is_some()
        {
            found.push(blocker("manager_not_quiescent", Some(agent.id.clone())));
        }
        if agent.state == AgentState::Unknown || has_unknown_gate(tx, num)? {
            found.push(blocker("unknown_effect", Some(agent.id.clone())));
        }
        if !agent.is_terminal() && !send_gate_open(tx, num, agent.generation)? {
            found.push(blocker("send_gate_closed", Some(agent.id.clone())));
        }
    }
    if from.workspace.is_some() && !source_custody_closed(tx, from)? {
        found.push(blocker("source_custody_open", Some(from.id.clone())));
    }
    let managers = params![from_num, to_num, limit.saturating_add(1)];
    let mut approvals = tx
        .prepare(
            "SELECT request_id FROM pending_requests
         WHERE state='pending' AND agent IN (?1,?2) ORDER BY agent,generation,request_id LIMIT ?3",
        )
        .map_err(sql_error)?;
    read_blockers(&mut found, &mut approvals, managers, limit, |id| {
        blocker("pending_approval", Some(id))
    })?;

    let mut commands = tx.prepare(
        "SELECT c.id, c.state FROM commands c LEFT JOIN agents a ON a.id=c.target
         WHERE c.state IN ('accepted','queued','dispatched','unknown') AND (
            (c.target IN (?1,?2) AND c.verb NOT IN ('mail.send','attention.emit'))
            OR (c.principal GLOB ?3 AND NOT (
                c.state='dispatched' AND c.verb='send' AND a.root=?1 AND a.id<>?1
                AND c.generation=a.generation AND c.turn_id IS NOT NULL AND c.turn_id=a.active_turn))
            OR (c.state IN ('accepted','queued','unknown')
                AND (a.root IN (?1,?2) OR EXISTS (
                  SELECT 1 FROM json_each(c.assignment_binding) b
                  WHERE json_extract(b.value,'$.project')=?4)))
         ) ORDER BY c.created_at,c.id LIMIT ?5",
    ).map_err(sql_error)?;
    let rows = commands
        .query_map(
            params![
                from_num,
                to_num,
                format!("agent:{}:*", from.id),
                from.project,
                limit.saturating_add(1)
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(sql_error)?;
    for (i, row) in rows.enumerate() {
        let (id, state) = row.map_err(sql_error)?;
        if i as i64 == limit {
            found.push(blocker("fanout_limit", None));
            break;
        }
        let code = match state.as_str() {
            "unknown" => "unknown_effect",
            "dispatched" => "effect_in_flight",
            _ => "queued_command",
        };
        found.push(blocker(code, Some(id)));
    }

    let mut goals = tx.prepare(
        "SELECT CAST(round_key AS TEXT) FROM goal_markers WHERE agent IN (?1,?2) AND state IN ('running','unknown')
         ORDER BY round_key LIMIT ?3",
    ).map_err(sql_error)?;
    read_blockers(&mut found, &mut goals, managers, limit, |id| {
        blocker("goal_round_unresolved", Some(id))
    })?;
    if goals::eliot_goal_status(tx, from_num)? == Some(GoalStatus::Blocked) {
        found.push(blocker("source_goal_blocked", Some(from.id.clone())));
    }
    Ok(found)
}

fn read_blockers(
    found: &mut Vec<ProjectTransferBlocker>,
    statement: &mut rusqlite::Statement<'_>,
    params: &[&dyn rusqlite::ToSql],
    limit: i64,
    make_blocker: impl Fn(String) -> ProjectTransferBlocker,
) -> Result<()> {
    let rows = statement
        .query_map(params, |row| row.get::<_, String>(0))
        .map_err(sql_error)?;
    for (i, row) in rows.enumerate() {
        let subject = row.map_err(sql_error)?;
        if i as i64 == limit {
            found.push(blocker("fanout_limit", None));
            break;
        }
        found.push(make_blocker(subject));
    }
    Ok(())
}

fn require_destination(conn: &Connection, from: &Agent, to: &Agent) -> Result<()> {
    let registration = projects::project_registration_tx(conn, &from.project)?
        .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
    if to.id == from.id
        || to.project != from.project
        || to.project_scope() != from.project_scope()
        || to.workspace != from.workspace
        || to.parent.is_some()
        || to.root != to.id
        || to.chat
        || to.observe_only
        || !to.own_identity
        || to.archived
        || !registration.profiles.contains(&to.profile)
    {
        return Err(Error::new(
            "not_allowed",
            "destination requires an approved root in the same project and workspace",
        ));
    }
    Ok(())
}

fn replacement_occupies_workspace(conn: &Connection, id: Option<&str>) -> Result<Option<Command>> {
    let Some(id) = id else { return Ok(None) };
    let launch = load_command(conn, id)?
        .ok_or_else(|| Error::new("source_integrity", "transfer launch was not retained"))?;
    if matches!(
        launch.state,
        CommandState::Accepted
            | CommandState::Queued
            | CommandState::Dispatched
            | CommandState::Unknown
    ) {
        return Ok(Some(launch));
    }
    let target = launch
        .target
        .as_deref()
        .ok_or_else(|| Error::new("source_integrity", "replacement launch has no target"))?;
    let Some(agent) = load_agent_by_num(conn, parse_agent_id(target)?)? else {
        return Err(Error::new(
            "source_integrity",
            "replacement Agent was removed",
        ));
    };
    if agent.generation == 0 || source_custody_closed(conn, &agent)? {
        return Ok(None);
    }
    Ok(Some(launch))
}

#[allow(clippy::too_many_arguments)]
fn stage_launch(
    tx: &Transaction<'_>,
    limits: HierarchyLimits,
    from: &Agent,
    transfer: &Intent,
    profile: &str,
    launch: Option<Result<SubmitCommand>>,
    now: i64,
    value: &mut ProjectTransferResult,
    events: &mut Vec<Event>,
    capacity_policy: Option<&capacity::PolicyLoader>,
) -> Result<()> {
    let registration = projects::project_registration_tx(tx, &from.project)?
        .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
    if !registration
        .profiles
        .iter()
        .any(|allowed| allowed == profile)
    {
        return Err(Error::new(
            "not_allowed",
            "replacement profile is outside project policy",
        ));
    }
    let mut candidates = tx
        .prepare(
            "SELECT id FROM agents WHERE project=?1 AND workspace IS ?2 AND profile=?3
           AND id<>?4 AND parent IS NULL AND state='idle' ORDER BY id LIMIT ?5",
        )
        .map_err(sql_error)?;
    let candidates = candidates
        .query_map(
            params![
                from.project,
                from.workspace,
                profile,
                parse_agent_id(&from.id)?,
                limits.max_selector_targets.saturating_add(1)
            ],
            |row| row.get::<_, i64>(0),
        )
        .map_err(sql_error)?;
    value.launch_command = transfer.launch.clone();

    for (i, candidate) in candidates.enumerate() {
        let id = candidate.map_err(sql_error)?;
        if i as i64 == limits.max_selector_targets {
            value.blockers.push(blocker("fanout_limit", None));
            return Ok(());
        }
        let Some(candidate) = load_agent_by_num(tx, id)? else {
            continue;
        };
        match require_destination(tx, from, &candidate)
            .and_then(|()| handoff::quiescent(tx, &candidate, true))
        {
            Ok(()) => {
                value.destination = Some(identity(&candidate));
                value.outcome = ProjectTransferOutcome::DestinationReady;
                return Ok(());
            }
            Err(error) if matches!(error.code.as_str(), "busy" | "not_allowed") => {}
            Err(error) => return Err(error),
        }
    }
    if let Some(launch) = replacement_occupies_workspace(tx, transfer.launch.as_deref())? {
        value
            .blockers
            .push(blocker("replacement_custody_open", Some(launch.id)));
        return Ok(());
    }
    value.blockers = blockers(tx, from, None, limits.max_selector_targets)?;
    if from.workspace.is_none() && !source_custody_closed(tx, from)? {
        value
            .blockers
            .push(blocker("source_custody_open", Some(from.id.clone())));
    }
    if transfer.saved_context.is_none()
        || !registration.actions.iter().any(|action| action == "launch")
    {
        value.blockers.push(blocker(
            "replacement_requires_saved_context_and_policy",
            None,
        ));
    }
    if !value.blockers.is_empty() {
        return Ok(());
    }
    let mut launch = launch
        .ok_or_else(|| Error::new("internal", "missing replacement launch preparation"))??;
    if launch.verb != "launch"
        || launch.args["profile"].as_str() != Some(profile)
        || launch.args["workspace"].as_str() != from.workspace.as_deref()
        || launch.args["project"].as_str() != Some(from.project_scope())
    {
        return Err(Error::new(
            "stale_assignment",
            "replacement preparation differs from the source workspace",
        ));
    }
    let harness = required_string(&launch.args, "harness")?;
    let snapshot = &launch.args["profile_snapshot"];
    let mut candidate = Agent::new("a1", &from.project, harness, profile, now);
    candidate.profile_snapshot = snapshot.clone();
    if provider_holds::profile_held(tx, &candidate)? {
        value
            .blockers
            .push(blocker("provider_hold", Some(profile.into())));
        return Ok(());
    }
    launch.args["registered_project"] = json!(from.project);
    launch.args[LAUNCH_ARG] = json!(transfer.id);
    // The source custody check above proves this generation can release its
    // reservation. Keep that release even if another holder blocks the launch.
    events.extend(capacity::release_owned_exit_event_tx(
        tx,
        parse_agent_id(&from.id)?,
        from.generation,
        "project_transfer",
        now,
    )?);
    db!(tx.execute_batch("SAVEPOINT project_transfer_launch"));
    db!(tx.execute(
        "UPDATE project_transfers SET launch_command=?1 WHERE project=?2",
        params![launch.id, from.project],
    ));
    let admission = match admission::submit_tx(
        tx,
        "owner",
        launch,
        None,
        now,
        false,
        limits,
        capacity_policy,
    ) {
        Ok(admission) => admission,
        Err(error) if error.code == "capacity_unavailable" => {
            db!(tx.execute_batch(
                "ROLLBACK TO project_transfer_launch; RELEASE project_transfer_launch"
            ));
            value.outcome = ProjectTransferOutcome::WaitingCapacity;
            value
                .blockers
                .push(blocker(&error.code, Some(*error.message)));
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    db!(tx.execute_batch("RELEASE project_transfer_launch"));
    events.extend(admission.events);
    value.launch_command = Some(admission.command.id);
    value.outcome = ProjectTransferOutcome::LaunchAdmitted;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn commit_transfer(
    tx: &Transaction<'_>,
    from: &Agent,
    to: &Agent,
    value: &mut ProjectTransferResult,
    limits: HierarchyLimits,
    policy: &core::SecretPolicy,
    now: i64,
    capacity_policy: Option<&capacity::PolicyLoader>,
) -> Result<Vec<Event>> {
    if value.assignment.workspace.is_some() && value.assignment.workspace != from.workspace {
        return Err(Error::new(
            "stale_assignment",
            "the source no longer matches its workspace binding",
        ));
    }
    // H1 edits current membership only. No native identity or generation is rewritten.
    let moved = reparent::reparent_tx(
        tx,
        &ReparentRequest {
            id: value.id.clone(),
            agent: None,
            children_of: Some(from.id.clone()),
            expected_generation: from.generation,
            parent: to.id.clone(),
            parent_generation: to.generation,
        },
        limits,
        now,
        true,
    )?;
    for member in &moved.value.moved {
        let member = exact_agent(tx, &member.agent, member.generation)?;
        if member.project != from.project || member.workspace != from.workspace {
            return Err(Error::new(
                "bad_state",
                "a retained worker has a different project or workspace",
            ));
        }
    }
    let mut events = moved.events;
    let to_num = parse_agent_id(&to.id)?;
    capacity::require_reservation_tx(tx, to_num, to.generation)?;
    if source_custody_closed(tx, from)? {
        let source_num = parse_agent_id(&from.id)?;
        events.extend(capacity::release_owned_exit_event_tx(
            tx,
            source_num,
            from.generation,
            "project_transfer",
            now,
        )?);
    }
    let goal_revision = goals::transfer_goal_tx(tx, from, to, policy, now, &mut events)?;
    let (claims, _, _) = attribution::transfer_claims_tx(tx, from, to, policy, now, &mut events)?;
    if let Some(path) = &value.assignment.workspace {
        workspace::transfer_workspace_custody(tx, path, from, to, now, &mut events)?;
    }
    write_next_assignment(tx, &value.assignment, Some((to_num, to.generation)), None)?;
    delete_intent(tx, &from.project)?;
    capacity::require_assignment_fits(tx, &from.project, capacity_policy)?;
    let assignment = read_assignment(tx, &from.project)?;
    events.push(insert_daemon_event(
        tx, now, "project.transferred", "coordination", Some(parse_agent_id(&to.id)?), Some(to.generation),
        json!({"id": value.id, "project": from.project, "epoch": assignment.epoch, "source": from.id}),
    )?);
    value.assignment = assignment;
    value.moved = moved.value.moved;
    value.claims = claims;
    value.goal_revision = goal_revision;
    Ok(events)
}

/// Owner recovery remains available during drain. All other commands addressed
/// to A, and new Agent operations issued by A, stop at the assignment fence.
pub(super) fn fence(
    conn: &Connection,
    principal: &str,
    verb: &str,
    caller: Option<&Agent>,
    target: Option<&Agent>,
) -> Result<()> {
    if let Some(caller) = caller
        && intent(conn, &caller.project)?.is_some()
        && read_assignment(conn, &caller.project)?.manager == Some(identity(caller))
    {
        return Err(Error::new(
            "transfer_in_progress",
            "project manager is draining for transfer",
        ));
    }
    if let Some(target) = target
        && !matches!(
            (principal, verb),
            (
                "owner",
                "stop" | "interrupt" | "approve" | "deny" | "answer"
            )
        )
        && intent(conn, &target.project)?.is_some()
        && read_assignment(conn, &target.project)?.manager == Some(identity(target))
    {
        return Err(Error::new(
            "transfer_in_progress",
            "project manager is draining for transfer",
        ));
    }
    Ok(())
}

pub(super) fn check_automation_target(conn: &Connection, target: &Agent) -> Result<()> {
    let assignment = read_assignment(conn, &target.project)?;
    if let Some(ProjectManagerIdentity::Local { agent, .. }) = assignment.manager
        && (target.root != agent
            || (target.id == agent && intent(conn, &target.project)?.is_some()))
    {
        return Err(Error::new(
            "not_allowed",
            "automation target is outside the current project assignment",
        ));
    }
    Ok(())
}

pub(super) fn successor_launch(
    conn: &Connection,
    principal: &str,
    verb: &str,
    id: &str,
    args: &Value,
    target: &Agent,
) -> Result<bool> {
    let Some(intent_id) = args.get(LAUNCH_ARG).and_then(Value::as_str) else {
        return Ok(false);
    };
    if principal != "owner" || verb != "launch" || target.parent.is_some() || target.chat {
        return Ok(false);
    }
    let transfer = required_intent(conn, &target.project, intent_id)?;
    let assignment = read_assignment(conn, &target.project)?;
    let from = source(conn, &assignment)?;
    let registration = projects::project_registration_tx(conn, &target.project)?
        .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
    Ok(transfer.saved_context.is_some()
        && transfer.launch.as_deref() == Some(id)
        && registration.actions.iter().any(|action| action == "launch")
        && registration.profiles.contains(&target.profile)
        && from.workspace == target.workspace
        && from.project_scope() == target.project_scope()
        && source_custody_closed(conn, &from)?)
}
