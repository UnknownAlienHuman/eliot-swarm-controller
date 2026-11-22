use super::{
    Agent, AgentControl, AgentState, Change, Error, OwnerProcess, Result, Store, admission,
    canonical, command_binding, commit_transaction, coordination, decode_json, load_agent_by_num,
    projects, validate_id_age, write_binding,
};
use eliot_core::api::{
    MAX_PROJECT_BROADCAST_AGENTS, PROJECT_BROADCAST_METHOD, ProjectAssignmentState,
    ProjectBroadcastRecipient, ProjectBroadcastRequest, ProjectBroadcastResponse,
    ProjectManagerIdentity,
};
use rusqlite::{Transaction, TransactionBehavior, params};
use serde_json::json;

const MAX_PROJECT_BROADCAST_BYTES: usize = 4 * 1024 * 1024;

impl Store {
    pub fn project_broadcast(
        &mut self,
        principal: &str,
        request: &ProjectBroadcastRequest,
        owner_process: Option<&OwnerProcess>,
        admission_mode: Result<()>,
        now: i64,
    ) -> Result<Change<ProjectBroadcastResponse>> {
        request.validate()?;
        if owner_process.is_some() && principal != "owner" {
            return Err(Error::new(
                "not_allowed",
                "Owner process metadata requires an Owner broadcast",
            ));
        }
        self.ensure_committed_reads()?;
        self.ensure_writer_healthy()?;
        let payload = json!({"project": request.project, "text": request.text});
        let payload_digest = canonical::digest(&payload)?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(super::sql_error)?;
            authorize_project_broadcast(&tx, principal, &request.project)?;
            if let Some((old_principal, old_method, old_digest, old_result)) =
                write_binding(&tx, &request.id)?
            {
                if old_principal != principal
                    || old_method != PROJECT_BROADCAST_METHOD
                    || old_digest != payload_digest
                {
                    return Err(Error::new(
                        "id_conflict",
                        "project broadcast id is bound to another principal, method, or payload",
                    ));
                }
                let value =
                    serde_json::from_value::<ProjectBroadcastResponse>(decode_json(&old_result)?)
                        .map_err(|_| {
                        Error::new("internal", "retained project broadcast result is invalid")
                    })?;
                request.validate_response(&value)?;
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(Change {
                    value,
                    events: Vec::new(),
                });
            }
            admission_mode?;
            validate_id_age(&request.id, now)?;
            if command_binding(&tx, &request.id)?.is_some()
                || super::digest_id_bound(&tx, &request.id)?
            {
                return Err(Error::new(
                    "id_conflict",
                    "project broadcast id is already used by a command",
                ));
            }
            let max_agents = self
                .hierarchy_limits
                .max_selector_targets
                .min(MAX_PROJECT_BROADCAST_AGENTS as i64);
            let agents = project_broadcast_agents(&tx, &request.project, max_agents)?;
            if request
                .text
                .len()
                .checked_mul(agents.len())
                .is_none_or(|bytes| bytes > MAX_PROJECT_BROADCAST_BYTES)
            {
                return Err(Error::new(
                    "frame_too_large",
                    "project broadcast exceeds its aggregate text bound",
                ));
            }
            let mut recipients = Vec::with_capacity(agents.len());
            let mut events = Vec::with_capacity(agents.len());
            for agent in agents {
                let turn_id = agent.active_turn.clone().ok_or_else(|| {
                    Error::new("bad_state", "working project Agent has no active turn")
                })?;
                let command_id = ulid::Ulid::new().to_string();
                let mut command = super::SubmitCommand::new(command_id.clone(), "inject");
                command.target = Some(agent.id.clone());
                command.expected_generation = Some(agent.generation);
                command.expected_turn_id = Some(turn_id.clone());
                command.args = json!({"text": request.text, "point": "soonest"});
                let admission = admission::submit_tx(
                    &tx,
                    principal,
                    command,
                    None,
                    now,
                    false,
                    self.hierarchy_limits,
                    self.capacity_policy.as_deref(),
                )?;
                recipients.push(ProjectBroadcastRecipient {
                    agent: agent.id,
                    generation: agent.generation,
                    turn_id,
                    command_id,
                    state: admission.command.state,
                });
                events.extend(admission.events);
            }
            let value = ProjectBroadcastResponse {
                id: request.id.clone(),
                project: request.project.clone(),
                recipients,
            };
            request.validate_response(&value)?;
            let retained = serde_json::to_value(&value).map_err(|_| {
                Error::new("internal", "project broadcast result could not be encoded")
            })?;
            coordination::retain_write_tx(
                &tx,
                &request.id,
                principal,
                PROJECT_BROADCAST_METHOD,
                &payload_digest,
                &retained,
                now,
            )?;
            super::annotate_owner_events(&tx, &mut events, owner_process)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change { value, events })
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

fn authorize_project_broadcast(tx: &Transaction<'_>, principal: &str, project: &str) -> Result<()> {
    projects::project_registration_tx(tx, project)?
        .ok_or_else(|| Error::new("not_found", "project registration was not found"))?;
    match super::hierarchy::parse_caller(tx, principal)? {
        super::hierarchy::Caller::Owner => Ok(()),
        super::hierarchy::Caller::Agent { num, id, .. } => {
            let caller = load_agent_by_num(tx, num)?
                .ok_or_else(|| Error::new("stale_generation", "manager Agent disappeared"))?;
            if caller.control != AgentControl::Project
                || caller.project != project
                || caller.chat
                || caller.observe_only
                || caller.archived
            {
                return Err(Error::new(
                    "not_allowed",
                    "project broadcast requires the current project manager",
                ));
            }
            let assignment = super::project_assignment::read_assignment(tx, project)?;
            if assignment.state != ProjectAssignmentState::Active
                || assignment.manager
                    != Some(ProjectManagerIdentity::Local {
                        agent: id,
                        generation: caller.generation,
                    })
            {
                return Err(Error::new(
                    "not_allowed",
                    "project broadcast requires the current project manager",
                ));
            }
            Ok(())
        }
    }
}

fn project_broadcast_agents(
    tx: &Transaction<'_>,
    project: &str,
    max_agents: i64,
) -> Result<Vec<Agent>> {
    let ids = {
        let mut statement = tx
            .prepare(
                "SELECT id FROM agents
                 WHERE project = ?1 AND archived = 0 AND chat = 0 AND observe_only = 0
                   AND capacity_generation = generation
                   AND state IN ('busy', 'waiting') AND active_turn IS NOT NULL
                 ORDER BY id LIMIT ?2",
            )
            .map_err(super::sql_error)?;
        let rows = statement
            .query_map(params![project, max_agents.saturating_add(1)], |row| {
                row.get::<_, i64>(0)
            })
            .map_err(super::sql_error)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(super::sql_error)?
    };
    if ids.len() as i64 > max_agents {
        return Err(Error::new(
            "fanout_limit",
            format!("project broadcast exceeds the {max_agents}-Agent fanout limit"),
        ));
    }
    ids.into_iter()
        .map(|num| {
            let id = format!("a{num}");
            let agent = load_agent_by_num(tx, num)?.ok_or_else(|| {
                Error::new("internal", "project Agent disappeared in transaction")
            })?;
            if agent.id != id
                || agent.archived
                || agent.chat
                || agent.observe_only
                || !matches!(agent.state, AgentState::Busy | AgentState::Waiting)
                || agent
                    .active_turn
                    .as_deref()
                    .is_none_or(|turn_id| turn_id.trim().is_empty())
            {
                return Err(Error::new(
                    "internal",
                    "project broadcast snapshot changed within its transaction",
                ));
            }
            Ok(agent)
        })
        .collect()
}
