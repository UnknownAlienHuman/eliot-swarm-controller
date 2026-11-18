//! Durable interactive intent and the existing automation timer's wait readback.
use super::*;
use eliot_core::api::{
    AssignmentWaitView, LaunchIntentState, LaunchIntentView, LaunchQueueListRequest,
    LaunchQueueRequest, LaunchQueueView, LaunchWaitReason, ManagerBudgetRequest, ManagerBudgetView,
    ProjectManagerIdentity, TimerStateView,
};

use std::collections::HashMap;

const METHOD: &str = "launch.enqueue";
const BUDGET_METHOD: &str = "project.budget.set";
const MAX_INTERACTIVE_INTENTS: i64 = 1024;
const MAX_AGENT_INTENTS: i64 = 32;
const CURRENT_WAITS: &str = "assignment_waits w JOIN timers t
    ON t.principal = 'owner' AND t.rule = w.timer AND t.schedule_revision = w.revision
      AND t.next_due_at = w.due
    JOIN rule_state r ON r.principal=t.principal AND r.rule=t.rule
       AND r.enabled=1 AND r.paused_reason IS NULL
    LEFT JOIN paused_assignment_timers paused ON paused.rule=t.rule";

pub struct PendingLaunch {
    pub view: LaunchIntentView,
    pub principal: String,
    pub request: SubmitCommand,
    pub created_at: i64,
    pub route: Option<String>,
    parent_generation: Option<i64>,
    assignment_epoch: Option<i64>,
    remote: Option<core::RemoteCallContext>,
}

pub struct ArbitratedTimer {
    pub project: String,
    pub route: Option<String>,
    pub wait: Option<LaunchWaitReason>,
}

pub struct TimerWait {
    pub reason: LaunchWaitReason,
    pub route: Option<String>,
}

impl TimerWait {
    pub fn from_error(error: &Error, route: Option<String>) -> Option<Self> {
        let scope = match error.code.as_str() {
            "capacity_unavailable" => error
                .message
                .strip_prefix("configured capacity bound ")
                .and_then(|text| text.rsplit_once(" max_live="))
                .map_or("reservation", |(scope, _)| scope),
            "provider_held" => error
                .message
                .strip_prefix("provider scope ")
                .and_then(|text| text.split_once(" is held"))
                .map_or("profile", |(scope, _)| scope),
            "transfer_in_progress" => "transfer",
            "workspace_unavailable" => "workspace",
            _ => return None,
        };
        Some(Self {
            reason: LaunchWaitReason {
                code: error.code.clone(),
                scope: scope.to_owned(),
                changed_by: if scope.starts_with("manager_assignment:")
                    && error
                        .message
                        .ends_with("; current manager can change the delegated limit")
                {
                    "owner_or_delegated_manager"
                } else {
                    "owner"
                }
                .to_owned(),
            },
            route,
        })
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T> {
    serde_json::from_str(value)
        .map_err(|_| Error::new("source_integrity", "invalid stored arbitration data"))
}

fn retained<T: serde::de::DeserializeOwned>(
    conn: &Connection,
    id: &str,
    principal: &str,
    method: &str,
    digest: &[u8],
) -> Result<Option<T>> {
    if let Some((owner, kind, old_digest, result)) = write_binding(conn, id)? {
        if owner != principal || kind != method || old_digest != digest {
            return Err(Error::new(
                "id_conflict",
                "mutation id is bound to another write",
            ));
        }
        return Ok(Some(decode(&result)?));
    }
    if command_binding(conn, id)?.is_some() || digest_id_bound(conn, id)? {
        return Err(Error::new(
            "id_conflict",
            "mutation id is bound to another write",
        ));
    }
    Ok(None)
}

fn insert_receipt(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    digest: &[u8],
    result: &impl serde::Serialize,
    now: i64,
) -> Result<()> {
    validate_id_age(id, now)?;
    tx.execute(
        "INSERT INTO writes(id,principal,method,payload_digest,result,created_at)
         VALUES (?1,?2,?3,?4,?5,?6)",
        params![id, principal, method, digest, encode_json(result)?, now],
    )
    .map_err(sql_error)?;
    Ok(())
}

impl Store {
    pub fn replay_launch_intent(
        &self,
        principal: &str,
        request: &LaunchQueueRequest,
    ) -> Result<Option<LaunchIntentView>> {
        self.ensure_committed_reads()?;
        let digest = admission::command_payload_digest(&request.command())?;
        retained(&self.connection, &request.id, principal, METHOD, &digest)
    }

    pub fn enqueue_launch(
        &mut self,
        principal: &str,
        original: &LaunchQueueRequest,
        request: SubmitCommand,
        project: &str,
        remote: Option<&core::RemoteCallContext>,
        now: i64,
    ) -> Result<LaunchIntentView> {
        self.ensure_writer_healthy()?;
        validate_remote_command_context(principal, remote)?;
        let result = (|| {
            let digest = admission::command_payload_digest(&original.command())?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(view) = retained(&tx, &original.id, principal, METHOD, &digest)? {
                return Ok(view);
            }
            // Recheck the retained caller generation and parent authority at the writer.
            hierarchy::resolve_launch_parent(&tx, principal, &request.args)?;
            let parent_generation = parent_generation(&tx, &request)?;
            let assignment_epoch = if principal != "owner" && !project.is_empty() {
                Some(project_assignment::read_assignment(&tx, project)?.epoch)
            } else {
                None
            };
            let count: i64 = tx
                .query_row("SELECT COUNT(*) FROM launch_intents", [], |r| r.get(0))
                .map_err(sql_error)?;
            if count >= MAX_INTERACTIVE_INTENTS {
                return Err(Error::new(
                    "capacity_unavailable",
                    "interactive launch intent queue is full",
                ));
            }
            if principal != "owner" {
                let owned: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM writes w JOIN launch_intents q ON q.id=w.id WHERE w.principal=?1",
                    [principal], |row| row.get(0),
                ).map_err(sql_error)?;
                if owned >= MAX_AGENT_INTENTS {
                    return Err(Error::new(
                        "capacity_unavailable",
                        "caller launch intent queue is full",
                    ));
                }
            }
            let view = LaunchIntentView {
                id: original.id.clone(),
                project: project.to_owned(),
                state: LaunchIntentState::Queued,
                command_id: None,
                reason: None,
            };
            insert_receipt(&tx, &original.id, principal, METHOD, &digest, &view, now)?;
            tx.execute(
                "INSERT INTO launch_intents(id,project,request,parent_generation,assignment_epoch,remote_context)
                 VALUES (?1,?2,?3,?4,?5,?6)",
                params![
                    original.id,
                    project,
                    encode_json(&request)?,
                    parent_generation,
                    assignment_epoch,
                    remote.map(encode_json).transpose()?
                ],
            )
            .map_err(sql_error)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(view)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn launch_intent(&self, principal: &str, id: &str) -> Result<LaunchIntentView> {
        self.ensure_committed_reads()?;
        let Some((owner, method, _, result)) = write_binding(&self.connection, id)? else {
            return Err(Error::new("not_found", "launch intent was not found"));
        };
        if method != METHOD {
            return Err(Error::new("not_found", "launch intent was not found"));
        }
        let view: LaunchIntentView = decode(&result)?;
        if principal != "owner"
            && principal != owner
            && crate::overview::queue_detail_access(&self.connection, principal)?.as_deref()
                != Some(view.project.as_str())
        {
            return Err(Error::new("not_found", "launch intent was not found"));
        }
        Ok(view)
    }

    pub fn pending_launches(&self) -> Result<Vec<PendingLaunch>> {
        self.ensure_committed_reads()?;
        let mut statement = self.connection.prepare(
            "SELECT w.result,w.principal,q.request,w.created_at,q.route,q.parent_generation,q.assignment_epoch,q.remote_context
             FROM launch_intents q JOIN writes w USING (id)
             ORDER BY w.created_at,q.rowid",
        ).map_err(sql_error)?;
        let rows = statement
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get(1)?,
                    r.get::<_, String>(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get::<_, Option<String>>(7)?,
                ))
            })
            .map_err(sql_error)?;
        rows.map(|row| {
            let (
                view,
                principal,
                request,
                created_at,
                route,
                parent_generation,
                assignment_epoch,
                remote,
            ) = row.map_err(sql_error)?;
            Ok(PendingLaunch {
                view: decode(&view)?,
                principal,
                request: decode(&request)?,
                created_at,
                route,
                parent_generation,
                assignment_epoch,
                remote: remote.as_deref().map(decode).transpose()?,
            })
        })
        .collect()
    }

    pub fn admit_launch_intent(
        &mut self,
        intent: &PendingLaunch,
        mut request: SubmitCommand,
        preparation: Option<WorkspacePreparationInput>,
        now: i64,
    ) -> Result<Admission> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if parent_generation(&tx, &request)? != intent.parent_generation
                || (intent.assignment_epoch.is_some()
                    && intent.assignment_epoch
                        != Some(
                            project_assignment::read_assignment(&tx, &intent.view.project)?.epoch,
                        ))
            {
                return Err(Error::new(
                    "stale_generation",
                    "queued launch authority changed",
                ));
            }
            check_arbitrated_project(&tx, &intent.view.project)?;
            request.id = ulid::Ulid::new().to_string();
            request.args["admission_mode"] = json!("arbitrated");
            let previous = intent
                .remote
                .as_ref()
                .map(|_| latest_command_rowid(&tx))
                .transpose()?;
            let mut admission = admission::submit_tx(
                &tx,
                &intent.principal,
                request,
                preparation,
                now,
                false,
                self.hierarchy_limits,
                self.capacity_policy.as_deref(),
            )?;
            if !admission.is_new {
                return Err(Error::new(
                    "source_integrity",
                    "queued launch command id already exists",
                ));
            }
            if let Some((rowid, remote)) = previous.zip(intent.remote.as_ref()) {
                annotate_remote_submission(&tx, &mut admission, remote, rowid)?;
            }
            let mut view = intent.view.clone();
            view.state = LaunchIntentState::Admitted;
            view.command_id = Some(admission.command.id.clone());
            view.reason = None;
            write_intent_state(&tx, &view, None)?;
            advance_cursor(&tx, &view.project)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(admission)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn note_launch_wait(
        &mut self,
        intent: &PendingLaunch,
        reason: LaunchWaitReason,
        route: Option<&str>,
        rejected: bool,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut view = intent.view.clone();
            if rejected {
                view.state = LaunchIntentState::Rejected;
            }
            view.reason = Some(reason);
            write_intent_state(&tx, &view, route)?;
            commit_transaction(tx, &self.write_faulted)
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn arbitration_cursor(&self) -> Result<Option<String>> {
        self.ensure_committed_reads()?;
        self.connection
            .query_row(
                "SELECT project FROM arbitration_cursor WHERE singleton=1",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql_error)
    }

    pub fn timer_wait(&self, state: &TimerStateView) -> Result<Option<(String, TimerWait)>> {
        self.ensure_committed_reads()?;
        let row: Option<(String, String, Option<String>)> = self
            .connection
            .query_row(
                &format!(
                    "SELECT w.project,w.reason,w.route FROM {CURRENT_WAITS}
                      WHERE paused.rule IS NULL AND w.timer=?1 AND w.revision=?2 AND w.due=?3"
                ),
                params![state.id, state.schedule_revision, state.next_due_at],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sql_error)?;
        row.map(|(project, reason, route)| {
            Ok((
                project,
                TimerWait {
                    reason: decode(&reason)?,
                    route,
                },
            ))
        })
        .transpose()
    }

    pub fn launch_queue(
        &self,
        principal: &str,
        request: &LaunchQueueListRequest,
    ) -> Result<LaunchQueueView> {
        self.ensure_committed_reads()?;
        let access = crate::overview::queue_detail_access(&self.connection, principal)?;
        let sql = format!(
            "SELECT key, payload, project, due FROM (
                SELECT 'i:' || q.id AS key,w.result AS payload,q.project,0 AS due
                FROM launch_intents q JOIN writes w USING (id)
                WHERE ?3 OR w.principal = ?4 OR q.project = ?5
                UNION ALL SELECT 'a:' || w.timer,w.reason,w.project,w.due
                FROM {CURRENT_WAITS} WHERE paused.rule IS NULL AND (?3 OR w.project = ?5)
             ) WHERE (?1 IS NULL OR project=?1) AND (?2 IS NULL OR key > ?2)
             ORDER BY key LIMIT 129"
        );
        let mut statement = self.connection.prepare(&sql).map_err(sql_error)?;
        let mut rows = statement
            .query(params![
                request.project,
                request.after,
                principal == "owner",
                principal,
                access
            ])
            .map_err(sql_error)?;
        let mut output = LaunchQueueView {
            interactive: Vec::new(),
            assignments: Vec::new(),
            next_after: None,
        };
        let mut last = None;
        for count in 0..=128 {
            let Some(row) = rows.next().map_err(sql_error)? else {
                break;
            };
            if count == 128 {
                output.next_after = last;
                break;
            }
            let key: String = row.get(0).map_err(sql_error)?;
            let payload: String = row.get(1).map_err(sql_error)?;
            if let Some(rule) = key.strip_prefix("a:") {
                output.assignments.push(AssignmentWaitView {
                    rule: rule.to_owned(),
                    project: row.get(2).map_err(sql_error)?,
                    due_at: row.get(3).map_err(sql_error)?,
                    reason: decode(&payload)?,
                });
            } else {
                output.interactive.push(decode(&payload)?);
            }
            last = Some(key);
        }
        Ok(output)
    }
}

fn parent_generation(conn: &Connection, request: &SubmitCommand) -> Result<Option<i64>> {
    let Some(parent) = request.args.get("parent").and_then(Value::as_str) else {
        return Ok(None);
    };
    let agent = load_agent_by_num(conn, parse_agent_id(parent)?)?
        .ok_or_else(|| Error::new("stale_generation", "queued launch parent disappeared"))?;
    Ok(Some(agent.generation))
}

fn write_intent_state(
    tx: &Transaction<'_>,
    view: &LaunchIntentView,
    route: Option<&str>,
) -> Result<()> {
    let changed = match view.state {
        LaunchIntentState::Queued => tx.execute(
            "UPDATE launch_intents SET route=COALESCE(route,?2) WHERE id=?1",
            params![view.id, route],
        ),
        _ => tx.execute("DELETE FROM launch_intents WHERE id=?1", [&view.id]),
    }
    .map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::new("bad_state", "launch intent is no longer queued"));
    }
    tx.execute(
        "UPDATE writes SET result=?2 WHERE id=?1",
        params![view.id, encode_json(view)?],
    )
    .map_err(sql_error)?;
    Ok(())
}

pub(super) fn check_arbitrated_project(conn: &Connection, project: &str) -> Result<()> {
    let transferring: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM project_transfers WHERE project=?1)",
            [project],
            |r| r.get(0),
        )
        .map_err(sql_error)?;
    if transferring {
        return Err(Error::new(
            "transfer_in_progress",
            "project manager transfer is pending",
        ));
    }
    Ok(())
}

pub(super) fn advance_cursor(tx: &Transaction<'_>, project: &str) -> Result<()> {
    tx.execute(
        "INSERT INTO arbitration_cursor(singleton,project) VALUES (1,?1)
        ON CONFLICT(singleton) DO UPDATE SET project=excluded.project",
        [project],
    )
    .map_err(sql_error)?;
    Ok(())
}

pub(super) fn write_timer_wait(
    tx: &Transaction<'_>,
    state: &TimerStateView,
    project: &str,
    wait: &TimerWait,
) -> Result<()> {
    tx.execute(
        "INSERT INTO assignment_waits(timer,revision,due,project,reason,route)
         VALUES (?1,?2,?3,?4,?5,?6)
         ON CONFLICT(timer) DO UPDATE SET revision=excluded.revision,due=excluded.due,
             project=excluded.project,reason=excluded.reason,route=excluded.route",
        params![
            state.id,
            state.schedule_revision,
            state.next_due_at,
            project,
            encode_json(&wait.reason)?,
            wait.route
        ],
    )
    .map_err(sql_error)?;
    Ok(())
}

pub(super) fn queue_usage(conn: &Connection, project: Option<&str>) -> Result<usize> {
    let total: i64 = conn.query_row(&format!(
        "SELECT (SELECT COUNT(*) FROM launch_intents WHERE ?1 IS NULL OR project=?1)
              + (SELECT COUNT(*) FROM {CURRENT_WAITS} WHERE paused.rule IS NULL AND (?1 IS NULL OR w.project=?1))"
    ),[project],|r| r.get(0)).map_err(sql_error)?;
    Ok(total as usize)
}

fn budget_override(conn: &Connection, project: &str) -> Result<Option<(Option<u64>, u8)>> {
    conn.query_row(
        "SELECT b.max_live, b.priority FROM manager_budgets b JOIN project_assignments p
         ON b.project=p.project AND b.manager=p.manager AND b.generation=p.manager_generation AND b.epoch=p.epoch
         WHERE b.project=?1",
        [project],
        |r| {
            Ok((
                r.get::<_, Option<i64>>(0)?.map(|value| value as u64),
                r.get(1)?,
            ))
        },
    )
    .optional()
    .map_err(sql_error)
}

pub(super) fn effective_manager_limit(
    conn: &Connection,
    project: &str,
    policy: &core::api::ProjectScopeLimits,
) -> Result<Option<u64>> {
    let Some(ceiling) = policy.manager_assignment else {
        return Ok(None);
    };
    let Some(floor) = policy.manager_min_live else {
        return Ok(Some(ceiling));
    };
    let budget = budget_override(conn, project)?.and_then(|(limit, _)| limit);
    Ok(Some(
        budget.map_or(ceiling, |limit| limit.max(floor).min(ceiling)),
    ))
}

impl Store {
    pub fn arbitration_priorities(
        &self,
        limits: &core::api::ScopeLimits,
    ) -> Result<HashMap<String, u8>> {
        self.ensure_committed_reads()?;
        let mut priorities = HashMap::new();
        for (project, limit) in &limits.projects {
            if let Some(max) = limit.manager_priority_max
                && let Some((_, priority)) = budget_override(&self.connection, project)?
            {
                priorities.insert(project.clone(), priority.min(max));
            }
        }
        Ok(priorities)
    }

    pub fn set_manager_budget(
        &mut self,
        principal: &str,
        request: &ManagerBudgetRequest,
        limits: &core::api::ScopeLimits,
        now: i64,
    ) -> Result<ManagerBudgetView> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let digest = canonical::digest(&json!(request))?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            if let Some(view) = retained(&tx, &request.id, principal, BUDGET_METHOD, &digest)? {
                return Ok(view);
            }
            let hierarchy::Caller::Agent { id, num, .. } = hierarchy::parse_caller(&tx, principal)?
            else {
                return Err(Error::new(
                    "not_allowed",
                    "budget changes require the current manager",
                ));
            };
            let assignment = project_assignment::read_assignment(&tx, &request.project)?;
            let agent = load_agent_by_num(&tx, num)?
                .ok_or_else(|| Error::new("stale_generation", "manager no longer exists"))?;
            if assignment.state != core::api::ProjectAssignmentState::Active
                || assignment.manager
                    != Some(ProjectManagerIdentity::Local {
                        agent: id,
                        generation: agent.generation,
                    })
                || assignment.revision != request.expected_revision
            {
                return Err(Error::new("not_allowed", "manager assignment changed"));
            }
            check_arbitrated_project(&tx, &request.project)?;
            let policy = limits
                .projects
                .get(&request.project)
                .ok_or_else(|| Error::new("not_allowed", "Owner has not delegated a budget"))?;
            let (old_max, old_priority) =
                budget_override(&tx, &request.project)?.unwrap_or_default();
            if let Some(limit) = request.max_live {
                if !matches!((policy.manager_min_live,policy.manager_assignment),
                    (Some(floor),Some(ceiling)) if (floor..=ceiling).contains(&limit))
                    || limit > i64::MAX as u64
                {
                    return Err(Error::new(
                        "not_allowed",
                        "budget is outside the Owner's delegated range",
                    ));
                }
                let scope = core::CapacityScope::manager_assignment_scope(&request.project);
                let used = capacity::capacity_scope_usage(&tx, &scope)? as u64;
                if limit < used {
                    return Err(Error::new(
                        "capacity_unavailable",
                        "only unused manager budget can be released",
                    ));
                }
                let previous =
                    effective_manager_limit(&tx, &request.project, policy)?.unwrap_or(limit);
                let increase = limit.saturating_sub(previous);
                if increase > 0 {
                    for (scope, cap) in [
                        (core::CapacityScope::global_scope(), limits.global),
                        (
                            core::CapacityScope::project_scope(&request.project),
                            policy.project,
                        ),
                    ] {
                        if let Some(cap) = cap {
                            let occupied = capacity::capacity_scope_usage(&tx, &scope)? as u64;
                            if increase > cap.saturating_sub(occupied) {
                                return Err(Error::new(
                                    "capacity_unavailable",
                                    format!("{scope} has insufficient free capacity"),
                                ));
                            }
                        }
                    }
                }
            }
            if request.priority.is_some_and(|priority| {
                policy.manager_priority_max.is_none_or(|max| priority > max)
            }) {
                return Err(Error::new(
                    "not_allowed",
                    "priority is outside the Owner's delegated range",
                ));
            }
            let priority = request.priority.unwrap_or(old_priority);
            let max_live = request.max_live.or(old_max);
            tx.execute("INSERT INTO manager_budgets(project,manager,generation,epoch,max_live,priority)
                VALUES (?1,?2,?3,?4,?5,?6) ON CONFLICT(project) DO UPDATE SET manager=excluded.manager,
                generation=excluded.generation,epoch=excluded.epoch,max_live=excluded.max_live,priority=excluded.priority",
                params![request.project,num,agent.generation,assignment.epoch,max_live.map(|value| value as i64),priority]).map_err(sql_error)?;
            let view = ManagerBudgetView {
                project: request.project.clone(),
                max_live: effective_manager_limit(&tx, &request.project, policy)?,
                priority: priority.min(policy.manager_priority_max.unwrap_or(0)),
            };
            insert_receipt(
                &tx,
                &request.id,
                principal,
                BUDGET_METHOD,
                &digest,
                &view,
                now,
            )?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(view)
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}
