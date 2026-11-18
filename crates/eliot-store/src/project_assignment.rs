use super::*;
use eliot_core::api::{
    PROJECT_ASSIGNMENT_CLAIM_ACTION, ProjectAssignment, ProjectAssignmentClaimRequest,
    ProjectAssignmentLaunchRequest, ProjectAssignmentOutcome, ProjectAssignmentReleaseRequest,
    ProjectAssignmentResult, ProjectAssignmentState, ProjectManagerIdentity,
};

const CLAIM: &str = PROJECT_ASSIGNMENT_CLAIM_ACTION;
const LAUNCH: &str = "project.assignment.launch";
const RELEASE: &str = "project.assignment.release";
pub(super) const MANAGER_LAUNCH_ARG: &str = "project_assignment_launch";

struct AssignmentWrite<'a> {
    id: &'a str,
    method: &'static str,
    principal: &'a str,
    digest: Vec<u8>,
    now: i64,
}

impl<'a> AssignmentWrite<'a> {
    fn new(
        id: &'a str,
        method: &'static str,
        principal: &'a str,
        request: &impl serde::Serialize,
        now: i64,
    ) -> Result<Self> {
        validate_ulid(id)?;
        let payload = serde_json::to_value(request)
            .map_err(|_| Error::new("internal", "assignment request could not be encoded"))?;
        Ok(Self {
            id,
            method,
            principal,
            digest: canonical::digest(&payload)?,
            now,
        })
    }

    fn replay(&self, conn: &Connection) -> Result<Option<ProjectAssignmentResult>> {
        if let Some((principal, method, digest, result)) = write_binding(conn, self.id)? {
            if principal != self.principal || method != self.method || digest != self.digest {
                return Err(Error::new(
                    "id_conflict",
                    "assignment operation ID is already bound",
                ));
            }
            return serde_json::from_str(&result)
                .map(Some)
                .map_err(|error| Error::new("internal", error.to_string()));
        }
        if command_binding(conn, self.id)?.is_some() || digest_id_bound(conn, self.id)? {
            return Err(Error::new(
                "id_conflict",
                "assignment operation ID is already in use",
            ));
        }
        Ok(None)
    }
}

impl Store {
    pub fn project_assignment(&self, principal: &str, project: &str) -> Result<ProjectAssignment> {
        self.ensure_committed_reads()?;
        authorize_project(&self.connection, principal, project)?;
        read_assignment(&self.connection, project)
    }

    /// Whether a worktree path is currently bound to a project manager
    /// assignment. Cleanup planning and worktree removal both refuse bound
    /// paths; release unbinds them.
    pub fn workspace_assignment_bound(&self, path: &str) -> Result<bool> {
        self.ensure_committed_reads()?;
        workspace_path_bound(&self.connection, path)
    }

    pub fn claim_project_manager(
        &mut self,
        principal: &str,
        request: &ProjectAssignmentClaimRequest,
        now: i64,
    ) -> Result<Change<ProjectAssignmentResult>> {
        request.validate()?;
        let write = AssignmentWrite::new(&request.id, CLAIM, principal, request, now)?;
        self.change_project_assignment(write, |tx| {
            let (registration, caller) = authorize_project(tx, principal, &request.project)?;
            let (id, generation) = match (&request.manager, &caller) {
                (Some(ProjectManagerIdentity::Remote { .. }), _) => {
                    return Err(Error::new(
                        "unsupported",
                        "remote manager grants are not supported",
                    ));
                }
                (Some(ProjectManagerIdentity::Local { agent, generation }), _) => {
                    (agent, *generation)
                }
                (None, Some(agent)) => (&agent.id, agent.generation),
                (None, None) => {
                    return Err(Error::new("invalid", "Owner claim requires a manager"));
                }
            };
            if let Some(caller) = &caller
                && (caller.id != *id
                    || caller.generation != generation
                    || !registration.actions.iter().any(|action| action == CLAIM))
            {
                return Err(Error::new(
                    "not_allowed",
                    "project policy does not allow this Agent claim",
                ));
            }
            let current = read_assignment(tx, &request.project)?;
            if current.manager.is_some() {
                return Ok((
                    assignment_result(current, ProjectAssignmentOutcome::Occupied),
                    Vec::new(),
                ));
            }
            let num = parse_agent_id(id)?;
            let agent = load_agent_by_num(tx, num)?
                .ok_or_else(|| Error::new("not_found", "manager Agent was not found"))?;
            if agent.generation != generation {
                return Err(Error::new("stale_generation", "manager generation changed"));
            }
            if agent.project != request.project
                || agent.parent.is_some()
                || agent.observe_only
                || agent.archived
                || agent.chat
                || !manager_live(agent.state)
                || !registration
                    .profiles
                    .iter()
                    .any(|profile| profile == &agent.profile)
            {
                return Err(Error::new(
                    "not_allowed",
                    "claim requires a live, approved root Agent in the registered project",
                ));
            }
            write_next_assignment(tx, &current, Some((num, generation)), None)?;
            Ok((
                assignment_result(
                    read_assignment(tx, &request.project)?,
                    ProjectAssignmentOutcome::Claimed,
                ),
                Vec::new(),
            ))
        })
    }

    pub fn replay_project_manager_launch(
        &self,
        request: &ProjectAssignmentLaunchRequest,
        now: i64,
    ) -> Result<Option<ProjectAssignmentResult>> {
        self.ensure_committed_reads()?;
        AssignmentWrite::new(&request.id, LAUNCH, "owner", request, now)?.replay(&self.connection)
    }

    pub fn launch_project_manager(
        &mut self,
        request: &ProjectAssignmentLaunchRequest,
        prepared: Result<(SubmitCommand, Option<WorkspacePreparationInput>)>,
        now: i64,
    ) -> Result<Change<ProjectAssignmentResult>> {
        request.validate()?;
        let write = AssignmentWrite::new(&request.id, LAUNCH, "owner", request, now)?;
        let limits = self.hierarchy_limits;
        let policy = self.capacity_policy.clone();
        self.change_project_assignment(write, |tx| {
            let (registration, _) = authorize_project(tx, "owner", &request.project)?;
            let current = read_assignment(tx, &request.project)?;
            if current.manager.is_some() {
                return Ok((
                    assignment_result(current, ProjectAssignmentOutcome::Occupied),
                    Vec::new(),
                ));
            }
            let (mut launch, preparation) = prepared?;
            if launch.id != request.id
                || launch.verb != "launch"
                || launch.args["profile"].as_str() != Some(request.profile.as_str())
                || !registration.actions.iter().any(|action| action == "launch")
                || !registration.profiles.contains(&request.profile)
            {
                return Err(Error::new(
                    "not_allowed",
                    "manager launch binding differs from the approved project and profile",
                ));
            }
            let adopted = request.adopt_worktree.as_deref();
            let bound_path = match (adopted, &preparation) {
                (None, Some(input)) => {
                    if launch.args["workspace"].as_str() != Some(registration.checkout.as_str()) {
                        return Err(Error::new(
                            "not_allowed",
                            "manager worktree must start from the registered checkout",
                        ));
                    }
                    input.worktree_path.clone()
                }
                (Some(_), None) => {
                    // The router has normalized the request path and verified
                    // its Git identity before freezing this launch.
                    let workspace = required_string(&launch.args, "workspace")?;
                    workspace::require_absolute_workspace_path(&workspace)?;
                    require_no_adoption_writers(tx, &workspace)?;
                    workspace
                }
                _ => {
                    return Err(Error::new(
                        "invalid",
                        "manager launch prepares a worktree or adopts a checkout, never both",
                    ));
                }
            };
            launch.args["registered_project"] = json!(request.project);
            launch.args[MANAGER_LAUNCH_ARG] = json!(registration.revision);
            let admission = admission::submit_tx(
                tx,
                "owner",
                launch,
                preparation,
                now,
                false,
                limits,
                policy.as_deref(),
            )?;
            bind_manager_workspace(tx, &request.project, &bound_path, adopted.is_some())?;
            let result = ProjectAssignmentResult {
                assignment: read_assignment(tx, &request.project)?,
                outcome: ProjectAssignmentOutcome::LaunchAdmitted,
                command_id: Some(admission.command.id),
            };
            Ok((result, admission.events))
        })
    }

    pub fn release_project_manager(
        &mut self,
        principal: &str,
        request: &ProjectAssignmentReleaseRequest,
        now: i64,
    ) -> Result<Change<ProjectAssignmentResult>> {
        request.validate()?;
        let write = AssignmentWrite::new(&request.id, RELEASE, principal, request, now)?;
        self.change_project_assignment(write, |tx| {
            let (_, caller) = authorize_project(tx, principal, &request.project)?;
            let current = read_assignment(tx, &request.project)?;
            let local = caller.as_ref().map(|agent| ProjectManagerIdentity::Local {
                agent: agent.id.clone(),
                generation: agent.generation,
            });
            if local.is_some() && local != current.manager {
                return Err(Error::new(
                    "not_allowed",
                    "only the current manager or Owner can release",
                ));
            }
            let outcome = if current.epoch != request.expected_epoch {
                ProjectAssignmentOutcome::StaleEpoch
            } else if current.manager.is_none() {
                ProjectAssignmentOutcome::Vacant
            } else if startup_blocks_release(tx, &current)? {
                ProjectAssignmentOutcome::StartupUncertain
            } else {
                write_next_assignment(tx, &current, None, None)?;
                ProjectAssignmentOutcome::Released
            };
            Ok((
                assignment_result(read_assignment(tx, &request.project)?, outcome),
                Vec::new(),
            ))
        })
    }

    fn change_project_assignment(
        &mut self,
        write: AssignmentWrite<'_>,
        apply: impl FnOnce(&Transaction<'_>) -> Result<(ProjectAssignmentResult, Vec<Event>)>,
    ) -> Result<Change<ProjectAssignmentResult>> {
        self.ensure_committed_reads()?;
        if let Some(value) = write.replay(&self.connection)? {
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(value) = write.replay(&tx)? {
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            validate_id_age(write.id, write.now)?;
            let (value, events) = apply(&tx)?;
            coordination::retain_write_tx(
                &tx,
                write.id,
                write.principal,
                write.method,
                &write.digest,
                &json!(value),
                write.now,
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change { value, events })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn check_assignment_dispatch(&self, id: &str) -> Result<()> {
        let command = load_command_record(&self.connection, id)?
            .ok_or_else(|| Error::new("not_found", "assignment command not found"))?;
        check_dispatch(&self.connection, &command)
    }
}

fn authorize_project(
    conn: &Connection,
    principal: &str,
    project: &str,
) -> Result<(core::api::ProjectRegistrationView, Option<Agent>)> {
    let caller = match hierarchy::parse_caller(conn, principal)? {
        hierarchy::Caller::Owner => None,
        hierarchy::Caller::Agent { num, .. } => {
            let agent = load_agent_by_num(conn, num)?
                .ok_or_else(|| Error::new("stale_generation", "assignment caller disappeared"))?;
            if agent.project != project || agent.chat || agent.observe_only {
                return Err(Error::new(
                    "not_allowed",
                    "assignment is outside the caller's registered project",
                ));
            }
            Some(agent)
        }
    };
    let registration = projects::project_registration_tx(conn, project)?
        .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
    Ok((registration, caller))
}

fn assignment_result(
    assignment: ProjectAssignment,
    outcome: ProjectAssignmentOutcome,
) -> ProjectAssignmentResult {
    ProjectAssignmentResult {
        assignment,
        outcome,
        command_id: None,
    }
}

fn manager_live(state: AgentState) -> bool {
    matches!(
        state,
        AgentState::Idle | AgentState::Busy | AgentState::Waiting
    )
}

type AssignmentRow = (Option<i64>, Option<i64>, i64, i64);

fn load_row(conn: &Connection, project: &str) -> Result<Option<AssignmentRow>> {
    conn.query_row(
        "SELECT manager, manager_generation, epoch, revision FROM project_assignments WHERE project=?1",
        [project],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    ).optional().map_err(sql_error)
}

fn startup_blocks_release(conn: &Connection, assignment: &ProjectAssignment) -> Result<bool> {
    let Some(id) = &assignment.start_command_id else {
        return Ok(false);
    };
    let Some(start) = load_command(conn, id)? else {
        return Ok(false);
    };
    // Operator release can revoke a blocked pre-native launch. Its uncertain
    // effect and capacity remain retained independently of the assignment.
    let preparation_unknown = start.verb == "launch"
        && start.generation == Some(0)
        && start.outcome.as_ref().is_some_and(|outcome| {
            outcome["stage"] == "workspace" && outcome["native_dispatched"] == false
        });
    Ok(start.state == CommandState::Dispatched
        || start.state == CommandState::Unknown && !preparation_unknown)
}

pub(super) fn read_assignment(conn: &Connection, project: &str) -> Result<ProjectAssignment> {
    let (num, generation, epoch, revision) = load_row(conn, project)?.unwrap_or((None, None, 0, 0));
    let (workspace, start_command_id, state): (Option<String>, Option<String>, Option<String>) =
        conn.query_row(
            "SELECT a.workspace_path, a.start_command, c.state
             FROM project_assignments a LEFT JOIN commands c ON c.id=a.start_command
             WHERE a.project=?1",
            [project],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(sql_error)?
        .unwrap_or_default();
    let (manager, state) = if let Some((num, generation)) = num.zip(generation) {
        let state = match state.as_deref() {
            Some("unknown") => ProjectAssignmentState::Blocked,
            Some("accepted" | "queued" | "dispatched") => ProjectAssignmentState::Starting,
            _ => match load_agent_by_num(conn, num)? {
                Some(agent) if agent.generation == generation && manager_live(agent.state) => {
                    ProjectAssignmentState::Active
                }
                _ => ProjectAssignmentState::Blocked,
            },
        };
        (
            Some(ProjectManagerIdentity::Local {
                agent: format_agent_id(num),
                generation,
            }),
            state,
        )
    } else {
        (None, ProjectAssignmentState::Vacant)
    };
    Ok(ProjectAssignment {
        project: project.to_owned(),
        manager,
        epoch,
        revision,
        state,
        workspace,
        start_command_id,
    })
}

fn write_next_assignment(
    tx: &Transaction<'_>,
    current: &ProjectAssignment,
    manager: Option<(i64, i64)>,
    start: Option<&str>,
) -> Result<()> {
    let epoch = next_epoch(current.epoch)?;
    let revision = next_epoch(current.revision)?;
    let (manager, generation) = manager.map_or((None, None), |(num, generation)| {
        (Some(num), Some(generation))
    });
    db!(tx.execute(
        "INSERT INTO project_assignments(project,manager,manager_generation,epoch,revision,start_command)
         VALUES(?1,?2,?3,?4,?5,?6)
         ON CONFLICT(project) DO UPDATE SET manager=excluded.manager,
             manager_generation=excluded.manager_generation, epoch=excluded.epoch,
             revision=excluded.revision, start_command=excluded.start_command,
             workspace_path=CASE WHEN excluded.manager IS NOT NULL THEN workspace_path END,
             workspace_adopted=CASE WHEN excluded.manager IS NULL THEN 0 ELSE workspace_adopted END",
        params![current.project,manager,generation,epoch,revision,start],
    ));
    Ok(())
}

pub(super) fn clear_failed_preparation(
    tx: &Transaction<'_>,
    agent: &Agent,
    command_id: &str,
) -> Result<()> {
    let assignment = read_assignment(tx, &agent.project)?;
    if assignment.start_command_id.as_deref() == Some(command_id)
        && assignment.manager
            == Some(ProjectManagerIdentity::Local {
                agent: agent.id.clone(),
                generation: 1,
            })
    {
        write_next_assignment(tx, &assignment, None, None)?;
    }
    Ok(())
}

fn next_epoch(epoch: i64) -> Result<i64> {
    epoch
        .checked_add(1)
        .ok_or_else(|| Error::new("bad_state", "assignment sequence exhausted"))
}

/// Record the current workspace binding for a manager assignment, separate
/// from the historical creator receipt (`start_command`). The binding moves
/// with the assignment: a fresh manager launch overwrites it, release clears
/// it, resume keeps it. A bound worktree is never removed; an adopted checkout
/// additionally never becomes a cleanup artifact.
fn bind_manager_workspace(
    tx: &Transaction<'_>,
    project: &str,
    path: &str,
    adopted: bool,
) -> Result<()> {
    let mut rows = tx.prepare(
        "SELECT workspace_path FROM project_assignments WHERE workspace_path IS NOT NULL AND project <> ?1
         UNION ALL SELECT worktree_path FROM workspaces WHERE phase <> 'removed'
         AND (?2 OR worktree_path <> ?3)",
    )
        .map_err(sql_error)?;
    for other in rows
        .query_map(params![project, adopted, path], |row| {
            row.get::<_, String>(0)
        })
        .map_err(sql_error)?
    {
        if workspaces_overlap(path, &other.map_err(sql_error)?) {
            return Err(Error::new(
                "id_conflict",
                "bound worktree overlaps a retained worktree",
            ));
        }
    }
    db!(tx.execute(
        "UPDATE project_assignments SET workspace_path = ?1, workspace_adopted = ?2 WHERE project = ?3",
        params![path, i64::from(adopted), project],
    ));
    Ok(())
}

fn require_no_adoption_writers(tx: &Transaction<'_>, path: &str) -> Result<()> {
    let mut statement = tx
        .prepare(
            "SELECT workspace FROM agents a WHERE workspace IS NOT NULL
         AND (a.state IN ('created','starting','idle','busy','waiting','unknown')
              OR EXISTS (SELECT 1 FROM processes p WHERE p.state <> 'exited'
                         AND p.owner IN ('agent:a' || a.id || '#' || a.generation,
                                         'workspace:a' || a.id || '#' || a.generation)))",
        )
        .map_err(sql_error)?;
    for other in statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(sql_error)?
    {
        if workspaces_overlap(path, &other.map_err(sql_error)?) {
            return Err(Error::new(
                "id_conflict",
                "adopted checkout overlaps a live Agent workspace",
            ));
        }
    }
    Ok(())
}

fn workspaces_overlap(a: &str, b: &str) -> bool {
    workspace::managed_path_contains(a, b) || workspace::managed_path_contains(b, a)
}

/// Whether any current assignment binds this worktree path. The binding is
/// currency by itself: release clears it and a new launch overwrites it, so a
/// set path always names a live assignment.
pub(super) fn workspace_path_bound(conn: &Connection, path: &str) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM project_assignments WHERE workspace_path = ?1)",
        [path],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

pub(super) fn reserve_launch(
    tx: &Transaction<'_>,
    project: &str,
    manager: i64,
    command_id: &str,
) -> Result<()> {
    let current = read_assignment(tx, project)?;
    if current.manager.is_some() {
        return Err(Error::new(
            "assignment_occupied",
            "project already has a manager",
        ));
    }
    write_next_assignment(tx, &current, Some((manager, 1)), Some(command_id))
}

pub(super) fn advance_resume(
    tx: &Transaction<'_>,
    agent: &Agent,
    manager: i64,
    next_generation: i64,
    command_id: &str,
) -> Result<()> {
    if load_row(tx, &agent.project)?.is_some_and(|(num, generation, _, _)| {
        num == Some(manager) && generation == Some(agent.generation)
    }) {
        let current = read_assignment(tx, &agent.project)?;
        write_next_assignment(
            tx,
            &current,
            Some((manager, next_generation)),
            Some(command_id),
        )?;
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Binding {
    project: String,
    epoch: i64,
    manager: Option<i64>,
    generation: Option<i64>,
}

struct EffectContext {
    projects: Vec<Binding>,
    caller: Option<Agent>,
    target: Option<Agent>,
}

fn effect_context(
    conn: &Connection,
    principal: &str,
    verb: &str,
    args: &Value,
    target: Option<i64>,
) -> Result<EffectContext> {
    if matches!(verb, "mail.send" | "attention.emit" | "attach" | "detach") {
        return Ok(EffectContext {
            projects: Vec::new(),
            caller: None,
            target: None,
        });
    }
    let target = target
        .map(|num| load_agent_by_num(conn, num))
        .transpose()?
        .flatten();
    let caller = principal
        .strip_prefix("agent:")
        .and_then(|name| name.split_once(':'))
        .map(|(id, _)| parse_agent_id(id).and_then(|num| load_agent_by_num(conn, num)))
        .transpose()?
        .flatten();
    let mut projects = Vec::new();
    let target_project = target
        .as_ref()
        .filter(|a| !a.chat)
        .map(|a| a.project.as_str());
    let caller_project = caller
        .as_ref()
        .filter(|a| !a.chat)
        .map(|a| a.project.as_str());
    let rule_project = principal.strip_prefix("project:");
    let repository = (verb == "git.fetch")
        .then(|| args.get("repository")?.as_str())
        .flatten();
    for project in [target_project, caller_project, rule_project, repository]
        .into_iter()
        .flatten()
    {
        if !project.is_empty()
            && !projects.iter().any(|b: &Binding| b.project == project)
            && let Some((manager, generation, epoch, _)) = load_row(conn, project)?
        {
            projects.push(Binding {
                project: project.to_owned(),
                epoch,
                manager,
                generation,
            });
        }
    }
    Ok(EffectContext {
        projects,
        caller,
        target,
    })
}

fn enforce_agent(
    conn: &Connection,
    principal: &str,
    caller: Option<&Agent>,
    target: Option<&Agent>,
    bindings: &[Binding],
) -> Result<()> {
    if bindings.is_empty() || !principal.starts_with("agent:") {
        return Ok(());
    }
    let caller =
        caller.ok_or_else(|| Error::new("stale_generation", "assignment caller disappeared"))?;
    hierarchy::parse_caller(conn, principal)?;
    for binding in bindings {
        let eligible = if let Some((num, generation)) = binding.manager.zip(binding.generation) {
            let manager = format_agent_id(num);
            let registration = projects::project_registration_tx(conn, &binding.project)?;
            registration.is_some()
                && load_agent_by_num(conn, num)?.is_some_and(|agent| {
                    agent.generation == generation && manager_live(agent.state)
                })
                && caller.project == binding.project
                && (caller.id == manager || caller.root == manager)
                && target.is_none_or(|a| {
                    a.project == binding.project && (a.id == manager || a.root == manager)
                })
        } else {
            false
        };
        if !eligible {
            return Err(Error::new(
                "not_allowed",
                "controlling a managed project requires the current assignment",
            ));
        }
    }
    Ok(())
}

fn enforce_launch(verb: &str, target: Option<&Agent>, bindings: &[Binding]) -> Result<()> {
    if matches!(verb, "launch" | "resume")
        && let Some(agent) = target
        && let Some(binding) = bindings
            .iter()
            .find(|binding| binding.project == agent.project)
        && let Some(manager) = binding.manager
        && agent.root != format_agent_id(manager)
    {
        return Err(Error::new(
            "assignment_occupied",
            "project already has a manager; startup requires its current team",
        ));
    }
    Ok(())
}

pub(super) fn capture_binding(
    tx: &Transaction<'_>,
    command: &CommandInsert<'_>,
) -> Result<Option<String>> {
    let EffectContext {
        mut projects,
        caller,
        target,
    } = effect_context(
        tx,
        command.principal,
        command.verb,
        command.args,
        command.target,
    )?;
    if let Some(expected) = command.args.get("assignment_epoch") {
        let expected = expected
            .as_i64()
            .filter(|value| *value >= 0)
            .ok_or_else(|| {
                Error::new("invalid", "assignment_epoch must be a nonnegative integer")
            })?;
        if projects.len() > 1 || projects.first().map_or(0, |binding| binding.epoch) != expected {
            return Err(Error::new(
                "stale_assignment",
                "project assignment epoch changed before admission",
            ));
        }
    }
    enforce_launch(command.verb, target.as_ref(), &projects)?;
    enforce_agent(
        tx,
        command.principal,
        caller.as_ref(),
        target.as_ref(),
        &projects,
    )?;
    if let Some(target) = &target
        && command.link_kind == Some("send")
        && let Some(parent) = command.parent_command
        && let Some(parent) = load_command(tx, parent)?
        && parent.verb == "resume"
        && let Some(binding) = projects.iter_mut().find(|b| b.project == target.project)
        && binding.manager == command.target
        && binding.generation == Some(target.generation)
    {
        binding.epoch = next_epoch(binding.epoch)?;
        binding.generation = command.generation;
    }
    if projects.is_empty() {
        Ok(None)
    } else {
        encode_json(&projects).map(Some)
    }
}

pub(super) fn check_dispatch(conn: &Connection, command: &CommandRecord) -> Result<()> {
    if let Some(revision) = command
        .command
        .args
        .get(MANAGER_LAUNCH_ARG)
        .and_then(Value::as_i64)
    {
        let project = required_string(&command.command.args, "registered_project")?;
        if !projects::project_registration_tx(conn, &project)?
            .is_some_and(|registration| registration.revision == revision)
        {
            return Err(Error::new(
                "stale_assignment",
                "manager launch registration changed",
            ));
        }
    }
    let EffectContext {
        projects,
        caller,
        target,
    } = effect_context(
        conn,
        &command.command.principal,
        &command.command.verb,
        &command.command.args,
        command.target,
    )?;
    let retained: Option<String> = db!(conn.query_row(
        "SELECT assignment_binding FROM commands WHERE id=?1",
        [&command.command.id],
        |r| r.get(0),
    ));
    let retained = retained
        .as_deref()
        .map(serde_json::from_str::<Vec<Binding>>)
        .transpose()
        .map_err(|error| Error::new("internal", error.to_string()))?
        .unwrap_or_default();
    if retained != projects {
        return Err(Error::new(
            "stale_assignment",
            "project assignment epoch changed before dispatch",
        ));
    }
    enforce_launch(&command.command.verb, target.as_ref(), &projects)?;
    enforce_agent(
        conn,
        &command.command.principal,
        caller.as_ref(),
        target.as_ref(),
        &projects,
    )
}

pub(super) fn coordination_binding(conn: &Connection, principal: &str) -> Result<Option<String>> {
    if !principal.starts_with("agent:") {
        return Ok(None);
    }
    let EffectContext {
        projects, caller, ..
    } = effect_context(conn, principal, "coordination", &Value::Null, None)?;
    enforce_agent(conn, principal, caller.as_ref(), None, &projects)?;
    if projects.is_empty() {
        Ok(None)
    } else {
        encode_json(&projects).map(Some)
    }
}

pub(super) fn authorize_coordination(
    conn: &Connection,
    principal: &str,
    target: Option<i64>,
) -> Result<()> {
    if !principal.starts_with("agent:") {
        return Ok(());
    }
    let EffectContext {
        projects,
        caller,
        target,
    } = effect_context(conn, principal, "coordination", &Value::Null, target)?;
    enforce_agent(conn, principal, caller.as_ref(), target.as_ref(), &projects)
}

#[cfg(test)]
#[path = "project_assignment_tests.rs"]
mod tests;
