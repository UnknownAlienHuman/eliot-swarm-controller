//! Delegated authority and remote endpoints of the project transfer protocol.

use super::*;
use eliot_core::{
    RemoteCallContext, RemoteManagerGrant,
    api::{
        ProjectAssignmentClaimRequest, ProjectAssignmentOutcome, ProjectAssignmentResult,
        ProjectManagerIdentity,
    },
};

pub(super) type PolicyLoader = dyn Fn() -> Result<Vec<RemoteManagerGrant>> + Send + Sync;

pub(super) fn is_remote(principal: &str) -> bool {
    principal.starts_with("remote:")
}

pub(super) fn configured_grant(
    load: Option<&PolicyLoader>,
    instance: &str,
    revision: i64,
    project: &str,
) -> Result<RemoteManagerGrant> {
    grants(load)?
        .into_iter()
        .find(|grant| {
            grant.instance == instance && grant.revision == revision && grant.project == project
        })
        .ok_or_else(|| {
            Error::new(
                "not_allowed",
                "remote manager grant does not authorize this project and revision",
            )
        })
}

fn grants(load: Option<&PolicyLoader>) -> Result<Vec<RemoteManagerGrant>> {
    load.ok_or_else(|| Error::new("not_allowed", "remote manager grants are unavailable"))?()
}

fn principal_grant(load: Option<&PolicyLoader>, principal: &str) -> Result<RemoteManagerGrant> {
    let (instance, revision) = principal
        .strip_prefix("remote:")
        .and_then(|value| value.split_once(':'))
        .and_then(|(instance, revision)| Some((instance, revision.parse::<i64>().ok()?)))
        .ok_or_else(|| Error::new("not_allowed", "invalid remote manager principal"))?;
    grants(load)?
        .into_iter()
        .find(|grant| grant.instance == instance && grant.revision == revision)
        .ok_or_else(|| Error::new("not_allowed", "remote manager grant changed or was revoked"))
}

pub(super) fn check_command(
    tx: &Transaction<'_>,
    load: Option<&PolicyLoader>,
    principal: &str,
    verb: &str,
    target: Option<&str>,
    generation: Option<i64>,
    remote: Option<&RemoteCallContext>,
) -> Result<()> {
    if !is_remote(principal) {
        return Ok(());
    }
    if !matches!(verb, "send" | "interrupt" | "stop") {
        return Err(Error::new(
            "not_allowed",
            "remote managers may send, interrupt, or stop",
        ));
    }
    let grant = principal_grant(load, principal)?;
    let remote = remote.filter(|remote| {
        remote.identity.audience == grant.audience && remote.identity.subject == grant.subject
    });
    if remote.is_none() {
        return Err(Error::new(
            "not_allowed",
            "remote command has no matching verified identity",
        ));
    }
    let target =
        target.ok_or_else(|| Error::new("invalid", "remote command requires one exact Agent"))?;
    let agent = member(tx, &grant, target)?;
    if !projects::project_registration_tx(tx, &grant.project)?
        .is_some_and(|registration| registration.actions.iter().any(|action| action == verb))
    {
        return Err(Error::new(
            "not_allowed",
            "project policy does not approve this remote command",
        ));
    }
    if generation != Some(agent.generation) || agent.is_terminal() {
        return Err(Error::new(
            "stale_generation",
            "remote command requires the live Agent generation",
        ));
    }
    project_transfer::require_no_intent(tx, &grant.project)
}

fn identity(grant: &RemoteManagerGrant) -> ProjectManagerIdentity {
    ProjectManagerIdentity::Remote {
        instance: grant.instance.clone(),
        grant_revision: grant.revision,
    }
}

fn member(conn: &Connection, grant: &RemoteManagerGrant, target: &str) -> Result<Agent> {
    let current = project_assignment::read_assignment(conn, &grant.project)?;
    if current.manager.as_ref() != Some(&identity(grant))
        || projects::project_registration_tx(conn, &grant.project)?.is_none()
    {
        return Err(Error::new(
            "stale_assignment",
            "remote manager is no longer assigned to the project",
        ));
    }
    let anchor = team_anchor(conn, &grant.project)?.map(|(num, _)| format_agent_id(num));
    load_agent_by_num(conn, parse_agent_id(target)?)?
        .filter(|agent| {
            agent.project == grant.project
                && !agent.chat
                && !agent.observe_only
                && anchor
                    .as_ref()
                    .is_none_or(|root| &agent.root == root && &agent.id != root)
        })
        .ok_or_else(|| Error::new("not_allowed", "Agent is outside the remote manager's team"))
}

pub(super) fn team_anchor(conn: &Connection, project: &str) -> Result<Option<(i64, i64)>> {
    conn.query_row(
        "SELECT remote_anchor, remote_anchor_generation FROM project_assignments
         WHERE project=?1 AND remote_anchor IS NOT NULL",
        [project],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(sql_error)
}

pub(super) fn write_remote_assignment(
    tx: &Transaction<'_>,
    current: &core::api::ProjectAssignment,
    grant: &RemoteManagerGrant,
    anchor: Option<(i64, i64)>,
) -> Result<()> {
    let (anchor, generation) = anchor.unzip();
    db!(tx.execute(
        "INSERT INTO project_assignments(project,epoch,revision,remote_manager,remote_anchor,remote_anchor_generation)
         VALUES(?1,?2,?3,?4,?5,?6)
         ON CONFLICT(project) DO UPDATE SET manager=NULL,manager_generation=NULL,start_command=NULL,
            epoch=excluded.epoch,revision=excluded.revision,remote_manager=excluded.remote_manager,
            remote_anchor=excluded.remote_anchor,remote_anchor_generation=excluded.remote_anchor_generation",
        params![current.project, project_assignment::next_epoch(current.epoch)?,
            project_assignment::next_epoch(current.revision)?, encode_json(&identity(grant))?, anchor, generation],
    ));
    Ok(())
}

impl Store {
    pub fn set_remote_manager_policy(
        &mut self,
        load: impl Fn() -> Result<Vec<RemoteManagerGrant>> + Send + Sync + 'static,
    ) {
        self.remote_manager_policy = Some(std::sync::Arc::new(load));
    }

    pub fn require_remote_manager_project(
        &self,
        actor: &RemoteManagerGrant,
        project: &str,
    ) -> Result<()> {
        let grant = configured_grant(
            self.remote_manager_policy.as_deref(),
            &actor.instance,
            actor.revision,
            project,
        )?;
        if grant != *actor {
            return Err(Error::new(
                "not_allowed",
                "project is outside the remote manager grant",
            ));
        }
        Ok(())
    }

    pub fn require_remote_manager_member(
        &self,
        actor: &RemoteManagerGrant,
        agent: &str,
    ) -> Result<()> {
        self.require_remote_manager_project(actor, &actor.project)?;
        member(&self.connection, actor, agent).map(|_| ())
    }

    pub fn remote_project_assignment(
        &self,
        actor: &RemoteManagerGrant,
        project: &str,
    ) -> Result<core::api::ProjectAssignment> {
        self.ensure_committed_reads()?;
        self.require_remote_manager_project(actor, project)?;
        project_assignment::read_assignment(&self.connection, project)
    }

    pub fn claim_remote_project_manager(
        &mut self,
        principal: &str,
        actor: Option<&RemoteManagerGrant>,
        request: &ProjectAssignmentClaimRequest,
        now: i64,
    ) -> Result<Change<ProjectAssignmentResult>> {
        request.validate()?;
        let Some(ProjectManagerIdentity::Remote {
            instance,
            grant_revision,
        }) = &request.manager
        else {
            return Err(Error::new(
                "invalid",
                "remote claim requires an explicit manager identity",
            ));
        };
        let expected = request.expected_revision.ok_or_else(|| {
            Error::new(
                "invalid",
                "remote claim requires the current assignment revision",
            )
        })?;
        if principal != "owner" && (!is_remote(principal) || actor.is_none()) {
            return Err(Error::new(
                "not_allowed",
                "remote claim requires Owner or the verified grant",
            ));
        }
        let load = self.remote_manager_policy.clone();
        let write = project_assignment::AssignmentWrite::new(
            &request.id,
            "project.assignment.claim",
            principal,
            request,
            now,
        )?;
        self.change_project_assignment(write, |tx| {
            let grant =
                configured_grant(load.as_deref(), instance, *grant_revision, &request.project)?;
            let registration = projects::project_registration_tx(tx, &request.project)?
                .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
            if actor.is_some_and(|actor| {
                *actor != grant
                    || !registration
                        .actions
                        .iter()
                        .any(|action| action == core::api::PROJECT_ASSIGNMENT_CLAIM_ACTION)
            }) {
                return Err(Error::new(
                    "not_allowed",
                    "remote manager may claim only its configured project and grant",
                ));
            }
            project_transfer::require_no_intent(tx, &request.project)?;
            let current = project_assignment::read_assignment(tx, &request.project)?;
            if current.revision != expected {
                return Err(Error::new(
                    "stale_assignment",
                    "project assignment revision changed",
                ));
            }
            let (assignment, outcome) = if current.manager.is_some() {
                (current, ProjectAssignmentOutcome::Occupied)
            } else {
                if !organization::retained_root_writers(tx, &request.project, None, 1)?.is_empty() {
                    return Err(Error::new(
                        "legacy_project_writers",
                        "retained native writers require an Owner transfer",
                    ));
                }
                write_remote_assignment(tx, &current, &grant, None)?;
                (
                    project_assignment::read_assignment(tx, &request.project)?,
                    ProjectAssignmentOutcome::Claimed,
                )
            };
            Ok((
                ProjectAssignmentResult {
                    assignment,
                    outcome,
                    command_id: None,
                },
                Vec::new(),
            ))
        })
    }
}
