//! Read-only owner decision and liveness projections over existing Store rows.

use crate::sql_error;
use eliot_core::api::{
    GoalMode, GoalStatus, LaunchIntentState, LaunchIntentView, OwnerAction, OwnerInboxItem,
    OwnerInboxKind, SwarmLiveness,
};
use eliot_core::liveness::{self, AgentWaitEvidence, LivenessObject, Mover};
use rusqlite::{CachedStatement, Connection, ToSql};
use serde_json::{Value, json};

pub(crate) const SWITCH_FENCE_NEEDS_OWNER: &str = "switch_checkout_release_needs_owner";
pub(crate) const REPEATED_FAILURE_REASON: &str = "repeated_failure";
const ARTIFACT_LEDGER_SUBJECT: &str = "artifact-ledger";

fn artifact_capacity_item(connection: &Connection) -> eliot_core::Result<Option<OwnerInboxItem>> {
    let capacity = super::cleanup::artifact_capacity(connection)?;
    if !capacity.approaching_limit() {
        return Ok(None);
    }
    let at_limit = capacity.active >= capacity.limit;
    let reason = if at_limit {
        "artifact_registration_limit"
    } else {
        "artifact_capacity_near_limit"
    };
    Ok(Some(OwnerInboxItem {
        kind: OwnerInboxKind::ArtifactCapacity,
        subject: ARTIFACT_LEDGER_SUBJECT.to_owned(),
        revision: reason.to_owned(),
        age_ms: 0,
        reason: reason.to_owned(),
        detail: Some(format!(
            "{} of {} active artifact registration slots are in use. Managed launches can still record custody.",
            capacity.active, capacity.limit,
        )),
        recommended: None,
        choices: Vec::new(),
        instructions: Some(
            "Review safe candidates with swarm cleanup plan --scope all.".to_owned(),
        ),
    }))
}

pub(crate) fn project(
    connection: &Connection,
    now: i64,
) -> eliot_core::Result<(Vec<OwnerInboxItem>, SwarmLiveness)> {
    let mut items = Vec::new();
    let mut liveness = SwarmLiveness::default();

    project_requests(connection, now, &mut items, &mut liveness, &[])?;
    project_unknown_commands(connection, now, &mut items, &mut liveness, &[])?;
    project_unknown_restores(connection, now, &mut items, &mut liveness, &[])?;
    project_resume_failures(connection, now, &mut items, &[])?;
    project_unknown_turns(connection, now, &mut items, &mut liveness, &[])?;
    project_holds(connection, now, &mut items, &mut liveness, &[])?;
    project_harness_checks(connection, now, &mut items, &mut liveness, &[])?;
    super::auth_login::project_harness_auth(connection, now, &mut items, &mut liveness, &[])?;
    project_agents(connection, now, &mut items, &mut liveness, &[])?;
    project_goals(connection, now, &mut items, &mut liveness, &[])?;
    project_launch_intents(connection, now, &mut items, &mut liveness, &[])?;
    project_assignment_waits(connection, now, &mut items, &mut liveness, &[])?;
    project_timers(connection, now, &mut items, &mut liveness, &[])?;
    project_handoffs(connection, now, &mut items, &mut liveness, &[])?;
    project_switch_fences(connection, now, &mut items, &mut liveness, &[])?;
    project_work_not_in_git(connection, now, &mut items, &mut liveness, &[])?;
    items.extend(project_repeated_failures(connection, now, None)?);
    items.extend(artifact_capacity_item(connection)?);

    items.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.subject.cmp(&right.subject))
    });
    Ok((items, liveness))
}

/// Reuse the projection for an exact subject, with equality predicates on
/// the source row's indexed keys before reading events or deriving actions.
pub(crate) fn project_item(
    db: &Connection,
    kind: OwnerInboxKind,
    subject: &str,
    now: i64,
) -> eliot_core::Result<Option<OwnerInboxItem>> {
    if kind == OwnerInboxKind::Agent
        && let Some(command_id) = subject.strip_prefix("resume:")
    {
        let mut items = Vec::new();
        project_resume_failures(db, now, &mut items, &[&command_id])?;
        return Ok(items.pop());
    }
    if kind == OwnerInboxKind::Stuck
        && let Some(cause) = subject.strip_prefix("repeat:")
    {
        return Ok(project_repeated_failures(db, now, Some(cause))?.pop());
    }
    type Projector = fn(
        &Connection,
        i64,
        &mut Vec<OwnerInboxItem>,
        &mut SwarmLiveness,
        &[&dyn ToSql],
    ) -> eliot_core::Result<()>;
    let (projector, key): (Projector, &str) = match kind {
        OwnerInboxKind::ArtifactCapacity => {
            return if subject == ARTIFACT_LEDGER_SUBJECT {
                artifact_capacity_item(db)
            } else {
                Ok(None)
            };
        }
        OwnerInboxKind::HarnessRequest => (project_requests, subject),
        OwnerInboxKind::UnknownCommand => match subject.strip_prefix("checkpoint_restore:") {
            Some(id) => (project_unknown_restores, id),
            None => (project_unknown_commands, subject),
        },
        OwnerInboxKind::UnknownTurn => (project_unknown_turns, subject),
        OwnerInboxKind::Hold => (project_holds, subject),
        OwnerInboxKind::HarnessCheck => (project_harness_checks, subject),
        OwnerInboxKind::HarnessAuth => (super::auth_login::project_harness_auth, subject),
        OwnerInboxKind::Agent => (project_agents, subject),
        OwnerInboxKind::BlockedGoal => (project_goals, subject),
        OwnerInboxKind::Handoff => (project_handoffs, subject),
        OwnerInboxKind::WorkNotInGit => (project_work_not_in_git, subject),
        OwnerInboxKind::LaunchQueue | OwnerInboxKind::Stuck => match subject.split_once(':') {
            Some(("command", id)) if kind == OwnerInboxKind::Stuck => {
                (project_unknown_commands, id)
            }
            Some(("switch", id)) if kind == OwnerInboxKind::Stuck => (project_switch_fences, id),
            Some(("hold", scope)) if kind == OwnerInboxKind::Stuck => (project_holds, scope),
            Some(("goal", goal)) if kind == OwnerInboxKind::Stuck => (project_goals, goal),
            Some(("timer", timer)) if kind == OwnerInboxKind::Stuck => (project_timers, timer),
            Some(("launch", id)) => (project_launch_intents, id),
            Some(("assignment", timer)) => (project_assignment_waits, timer),
            Some((agent, generation))
                if kind == OwnerInboxKind::Stuck
                    && generation.starts_with('g')
                    && eliot_core::parse_agent_id(agent).is_ok() =>
            {
                (project_agents, agent)
            }
            _ => return Ok(None),
        },
    };
    let mut items = Vec::new();
    let mut stats = SwarmLiveness::default();
    // Agent and Goal keys are numeric in Store, and requests/turns have
    // composite keys with a generation and an arbitrary native ID.
    let key_kind = if kind == OwnerInboxKind::Stuck {
        subject
            .split_once(':')
            .map_or(subject, |(prefix, _)| prefix)
    } else {
        kind.as_str()
    };
    match key_kind {
        "harness_request" | "unknown_turn" => {
            let Some((agent, tail)) = key.split_once(":g") else {
                return Ok(None);
            };
            let Some((generation, native_id)) = tail.split_once(':') else {
                return Ok(None);
            };
            let (Ok(agent), Ok(generation)) =
                (eliot_core::parse_agent_id(agent), generation.parse::<i64>())
            else {
                return Ok(None);
            };
            projector(
                db,
                now,
                &mut items,
                &mut stats,
                &[&agent, &generation, &native_id],
            )?;
        }
        "work_not_in_git" => {
            let mut parts = key.splitn(3, ':');
            let (Some(agent), Some(generation)) = (parts.next(), parts.next()) else {
                return Ok(None);
            };
            let (Ok(agent), Some(Ok(generation))) = (
                eliot_core::parse_agent_id(agent),
                generation.strip_prefix('g').map(str::parse::<i64>),
            ) else {
                return Ok(None);
            };
            projector(db, now, &mut items, &mut stats, &[&agent, &generation])?;
        }
        "blocked_goal" | "goal" => {
            let Some((agent, mode)) = key.split_once(':') else {
                return Ok(None);
            };
            let Ok(agent) = eliot_core::parse_agent_id(agent) else {
                return Ok(None);
            };
            projector(db, now, &mut items, &mut stats, &[&agent, &mode])?;
        }
        "timer" => {
            let principal_len = if let Some(rest) = key.strip_prefix("project:") {
                rest.find(':').map(|len| len + "project:".len())
            } else {
                key.find(':')
            };
            let Some(index) = principal_len else {
                return Ok(None);
            };
            let (principal, rule) = key.split_at(index);
            let rule = &rule[1..];
            projector(db, now, &mut items, &mut stats, &[&principal, &rule])?;
        }
        "agent" => {
            let Ok(agent) = eliot_core::parse_agent_id(key) else {
                return Ok(None);
            };
            projector(db, now, &mut items, &mut stats, &[&agent])?;
        }
        agent if kind == OwnerInboxKind::Stuck && eliot_core::parse_agent_id(agent).is_ok() => {
            let agent = eliot_core::parse_agent_id(agent)?;
            projector(db, now, &mut items, &mut stats, &[&agent])?;
        }
        _ => projector(db, now, &mut items, &mut stats, &[&key])?,
    }
    Ok(items
        .into_iter()
        .find(|item| item.kind == kind && item.subject == subject))
}

fn project_resume_failures(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(
        db,
        "SELECT c.id,c.updated_at
         FROM commands c
         WHERE c.verb='resume' AND c.principal='owner' AND c.state IN ('rejected','failed')",
        "c.id=?1",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (command_id, updated_at) = row.map_err(sql_error)?;
        let command = super::load_command(db, &command_id)?.ok_or_else(|| {
            crate::Error::new("source_integrity", "retained Resume failure disappeared")
        })?;
        let failure = eliot_core::api::AgentResumeFailure::from_command(&command)?;
        let agent = command.target.as_deref().ok_or_else(|| {
            crate::Error::new("source_integrity", "retained Resume failure has no Agent")
        })?;
        let mut entry = item(
            OwnerInboxKind::Agent,
            format!("resume:{command_id}"),
            format!(
                "{}:{}:{}",
                super::command_state_name(failure.state),
                failure.reason,
                failure.detail.as_deref().unwrap_or("")
            ),
            now,
            updated_at,
            "resume_failed",
            None,
            vec![
                action("inspect_agent", "agent.get", json!({"id":agent})),
                action("inspect_command", "command.get", json!({"id":command_id})),
            ],
        );
        entry.detail = Some(match failure.detail {
            Some(detail) => format!(
                "Resume generation {} failed: {} — {}",
                failure.generation, failure.reason, detail
            ),
            None => format!(
                "Resume generation {} failed: {}",
                failure.generation, failure.reason
            ),
        });
        items.push(entry);
    }
    Ok(())
}

fn project_repeated_failures(
    db: &Connection,
    now: i64,
    cause: Option<&str>,
) -> eliot_core::Result<Vec<OwnerInboxItem>> {
    let sql = "SELECT r.cause, r.occurrence, r.opened_at, r.message
         FROM repeat_failure_alerts r LEFT JOIN agents a ON a.id = r.goal_agent
         WHERE r.expires_at > ?1 AND (r.goal_agent IS NULL OR
           (a.generation = r.goal_generation AND a.state NOT IN ('stopped','failed','lost')))";
    let (mut stmt, bindings): (_, &[&dyn ToSql]) = if let Some(cause) = &cause {
        (
            db.prepare_cached(&format!("{sql} AND r.cause = ?2"))
                .map_err(sql_error)?,
            &[&now, cause],
        )
    } else {
        (db.prepare_cached(sql).map_err(sql_error)?, &[&now])
    };
    let rows = stmt
        .query_map(bindings, |row| {
            let mut alert = item(
                OwnerInboxKind::Stuck,
                format!("repeat:{}", row.get::<_, String>(0)?),
                row.get(1)?,
                now,
                row.get(2)?,
                REPEATED_FAILURE_REASON,
                None,
                Vec::new(),
            );
            alert.detail = Some(row.get(3)?);
            Ok(alert)
        })
        .map_err(sql_error)?;
    rows.map(|row| row.map_err(sql_error)).collect()
}

fn projection_query<'a>(
    db: &'a Connection,
    sql: &str,
    predicate: &str,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<CachedStatement<'a>> {
    if bindings.is_empty() {
        db.prepare_cached(sql)
    } else {
        db.prepare_cached(&format!("{sql} AND {predicate}"))
    }
    .map_err(sql_error)
}

fn project_requests(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db,
        "SELECT r.agent,r.generation,r.request_id,r.turn_id,r.kind,r.details,
                COALESCE((SELECT MIN(e.ts) FROM events e WHERE e.agent=r.agent
                  AND e.generation=r.generation AND e.kind='request.opened'
                  AND e.kind IN ('request.opened','request.resolved','request.closed')
                  AND json_extract(e.data,'$.request_id')=r.request_id),a.updated_at),
                COALESCE((SELECT MAX(e.seq) FROM events e WHERE e.agent=r.agent
                  AND e.generation=r.generation AND e.kind IN ('request.opened','request.resolved','request.closed')
                  AND json_extract(e.data,'$.request_id')=r.request_id),0)
         FROM pending_requests r JOIN agents a ON a.id=r.agent AND a.generation=r.generation
         WHERE r.state='pending' AND json_type(a.profile_snapshot, '$._harness_check') IS NULL", "r.agent=?1 AND r.generation=?2 AND r.request_id=?3", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, i64>(7)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (agent, generation, request, turn, kind, raw_details, since, event_seq) =
            row.map_err(sql_error)?;
        let details: Value = serde_json::from_str(&raw_details)
            .map_err(|e| crate::Error::new("corrupt", format!("invalid request details: {e}")))?;
        let subject = format!("a{agent}:g{generation}:{request}");
        let result = liveness::mover(&LivenessObject::HarnessRequest { since }, now);
        tally(stats, &result);
        let choices = request_choices(agent, generation, &request, &turn, &kind, &details);
        items.push(item(
            OwnerInboxKind::HarnessRequest,
            subject,
            format!("event:{event_seq}"),
            now,
            since,
            "harness_request_needs_owner_reply",
            None,
            choices,
        ));
    }
    Ok(())
}

fn request_choices(
    agent: i64,
    generation: i64,
    request: &str,
    turn: &str,
    kind: &str,
    details: &Value,
) -> Vec<OwnerAction> {
    let verbs: &[&str] = match kind {
        "permission" | "approval" => &["approve", "deny"],
        "question" => {
            let secret = details
                .get("questions")
                .and_then(Value::as_array)
                .is_some_and(|questions| {
                    questions.iter().any(|question| {
                        question.get("is_secret").and_then(Value::as_bool) == Some(true)
                    })
                });
            if secret {
                &["deny"]
            } else {
                &["answer", "deny"]
            }
        }
        _ => &["deny"],
    };
    verbs
        .iter()
        .map(|verb| {
            let args = match *verb {
                "answer" => json!({"answers": null}),
                "approve" | "deny" => json!({"choice_id":null,"feedback":null}),
                _ => json!({}),
            };
            action(
                verb,
                "command.submit",
                json!({
                    "verb":verb, "target":format!("a{agent}"), "expected_generation":generation,
                    "expected_turn_id":turn, "request_id":request, "args":args
                }),
            )
        })
        .collect()
}

fn project_unknown_commands(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db,
        "SELECT c.id,c.verb,c.method,c.target,c.generation,c.turn_id,c.created_at,c.state,
                COALESCE((SELECT e.ts FROM events e
                  WHERE e.kind='command.unknown'
                  AND CAST(json_extract(e.data,'$.command_id') AS TEXT)=c.id ORDER BY e.seq DESC LIMIT 1),c.updated_at),
                COALESCE((SELECT MAX(e.seq) FROM events e
                  WHERE e.kind='command.unknown'
                  AND CAST(json_extract(e.data,'$.command_id') AS TEXT)=c.id),0),
                json_extract(c.outcome,'$.stage'),
                EXISTS(SELECT 1 FROM workspaces w JOIN agents a ON a.id=w.agent
                  WHERE w.agent=c.target AND w.start_command_id=c.id AND w.phase='creating'
                    AND c.verb='launch' AND a.state='created' AND a.generation=0
                    AND c.generation=a.generation AND w.target_generation=a.generation+1
                    AND json_type(w.outcome,'$.native_git_write_dispatched')='false'
                    AND COALESCE(json_type(w.outcome,'$.cancel_requested'),'false')='false'),
                (EXISTS(SELECT 1 FROM processes p WHERE p.owner='agent:a'||c.target||'#'||c.generation AND p.state IN ('running','unknown'))),
                EXISTS(SELECT 1 FROM source_state s WHERE s.agent=c.target AND s.generation=c.generation
                  AND s.source NOT IN ('daemon','os','git','workspace_process') AND s.state='open'),
                EXISTS(SELECT 1 FROM commands parent
                  WHERE parent.id=c.parent_command AND c.link_kind='handoff'
                    AND parent.method='swarm.handoff')
         FROM commands c WHERE c.state IN ('accepted','queued','dispatched','unknown')
           AND NOT EXISTS (SELECT 1 FROM agents check_agent WHERE check_agent.id=c.target
                 AND json_type(check_agent.profile_snapshot, '$._harness_check') IS NOT NULL)", "c.id=?1", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<i64>>(3)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, Option<String>>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, String>(7)?,
                r.get::<_, i64>(8)?,
                r.get::<_, i64>(9)?,
                r.get::<_, Option<String>>(10)?,
                r.get::<_, bool>(11)?,
                r.get::<_, bool>(12)?,
                r.get::<_, bool>(13)?,
                r.get::<_, bool>(14)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (
            id,
            verb,
            method,
            target,
            generation,
            turn,
            _created,
            raw_state,
            since,
            seq,
            stage,
            unstarted_workspace,
            process_active,
            source_open,
            owner_handoff_send,
        ) = row.map_err(sql_error)?;
        let state = match raw_state.as_str() {
            "accepted" => eliot_core::CommandState::Accepted,
            "queued" => eliot_core::CommandState::Queued,
            "dispatched" => eliot_core::CommandState::Dispatched,
            "unknown" => eliot_core::CommandState::Unknown,
            _ => return Err(crate::Error::new("corrupt", "invalid command state")),
        };
        let unknown = state == eliot_core::CommandState::Unknown;
        let result = liveness::mover(
            &LivenessObject::Command {
                state,
                since,
                owner_item: unknown,
                dispatcher_active: state == eliot_core::CommandState::Queued
                    || process_active
                    || source_open,
                effect_active: process_active || source_open,
            },
            now,
        );
        tally(stats, &result);
        if !unknown {
            if let Mover::Stuck { reason } = result {
                let inspect = action("inspect", "command.get", json!({"id":id}));
                let recommended = match (reason, unstarted_workspace, target) {
                    ("command_without_dispatcher", true, Some(agent)) => Some(action(
                        "stop",
                        "command.submit",
                        json!({"verb":"stop", "target":format!("a{agent}"),
                               "expected_generation":generation, "args":{"force":false}}),
                    )),
                    _ => None,
                };
                let mut choices = recommended
                    .iter()
                    .cloned()
                    .chain([inspect])
                    .collect::<Vec<_>>();
                if owner_handoff_send
                    && verb == "send"
                    && matches!(
                        state,
                        eliot_core::CommandState::Accepted | eliot_core::CommandState::Queued
                    )
                {
                    choices.push(action("cancel", "command.cancel", json!({"command_id":id})));
                }
                items.push(item(
                    OwnerInboxKind::Stuck,
                    format!("command:{id}"),
                    format!("state:{raw_state}:updated:{since}:event:{seq}"),
                    now,
                    since,
                    reason,
                    recommended,
                    choices,
                ));
            }
            continue;
        }
        let mut choices = Vec::new();
        let workspace_launch = verb == "launch" && stage.as_deref() == Some("workspace");
        let start_command = matches!(verb.as_str(), "launch" | "resume");
        let mut recommended = None;
        if method == "command.submit" {
            let stop_applied_proven = match (verb.as_str(), target, generation) {
                ("stop", Some(agent), Some(generation)) => {
                    crate::capacity::has_owned_exit_and_reader_drain_tx(db, agent, generation)?
                }
                _ => false,
            };
            if stop_applied_proven {
                let applied = action(
                    "applied",
                    "command.resolve",
                    json!({
                        "command_id":id,"as":"applied",
                        "reason":"Store proves owned processes exited and the native reader drained."
                    }),
                );
                recommended = Some(applied.clone());
                choices.push(applied);
            } else if !start_command {
                for resolution in ["applied", "failed"] {
                    choices.push(action(
                        resolution,
                        "command.resolve",
                        json!({
                            "command_id":id,"as":resolution,"reason":null
                        }),
                    ));
                }
            } else {
                choices.push(action(
                    "failed",
                    "command.resolve",
                    json!({
                        "command_id":id,"as":"failed","reason":null
                    }),
                ));
            }
        }
        let reason = if workspace_launch {
            "workspace_launch_unknown_requires_guarded_readback"
        } else if verb == "stop" && recommended.is_some() {
            "stop_exit_and_reader_drain_proven"
        } else {
            "unknown_command_needs_owner_outcome"
        };
        items.push(item(
            OwnerInboxKind::UnknownCommand,
            id,
            format!("generation:{generation:?}:turn:{turn:?}:event:{seq}"),
            now,
            since,
            reason,
            recommended,
            choices,
        ));
    }
    Ok(())
}

fn project_unknown_restores(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut statement = projection_query(
        db,
        "SELECT r.id,r.workspace,w.created_at,r.applying,r.writer_boot IS NOT NULL,r.custody_error
         FROM checkpoint_restore_intents r JOIN writes w ON w.id = r.id
         WHERE w.result <> 'null'",
        "r.id = ?1",
        bindings,
    )?;
    let rows = statement
        .query_map(bindings, |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, bool>(3)?,
                row.get::<_, bool>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (id, workspace, created_at, applying, armed, custody_error) = row.map_err(sql_error)?;
        let reason = if custody_error.is_some() {
            "checkpoint_restore_writer_custody_check_failed"
        } else if armed {
            "checkpoint_restore_writer_exit_unproven"
        } else {
            "checkpoint_restore_needs_owner_inspection"
        };
        let mut choices = vec![action(
            "release_after_inspection",
            "checkpoint.restore.release",
            json!({"restore_id": id}),
        )];
        if armed {
            #[cfg(windows)]
            choices.push(action(
                "stop_writer",
                "checkpoint.restore.stop",
                json!({"restore_id": id}),
            ));
            choices.push(action(
                "abandon_reservation",
                "checkpoint.restore.abandon",
                json!({"restore_id": id, "confirm": null, "reason": null}),
            ));
        }
        tally(stats, &Mover::Parked { reason });
        let mut restore = item(
            OwnerInboxKind::UnknownCommand,
            format!("checkpoint_restore:{id}"),
            format!("applying:{applying}:armed:{armed}:custody:{custody_error:?}"),
            now,
            created_at,
            reason,
            None,
            choices,
        );
        let workspace = eliot_core::display_path(&workspace);
        restore.detail = Some(match custody_error {
            Some(code) => format!("{workspace} ({code})"),
            None => workspace,
        });
        restore.instructions = Some(if armed {
            "Stop the writer if its custody is proven. Abandoning only releases the reservation and can allow overlapping work while the writer is alive."
        } else {
            "Inspect the worktree and index before releasing the restore reservation."
        }.to_owned());
        items.push(restore);
    }
    Ok(())
}

fn project_unknown_turns(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db,
        "SELECT t.agent,t.generation,t.turn_id,COALESCE(t.completed_at,t.started_at,a.updated_at),
                COALESCE((SELECT MAX(e.seq) FROM events e WHERE e.agent=t.agent AND e.generation=t.generation
                  AND json_type(e.data,'$.turn_id')='text'
                  AND CAST(json_extract(e.data,'$.turn_id') AS TEXT)=t.turn_id),0)
         FROM turns t JOIN agents a ON a.id=t.agent WHERE t.state='unknown' AND json_type(a.profile_snapshot, '$._harness_check') IS NULL", "t.agent=?1 AND t.generation=?2 AND t.turn_id=?3", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (agent, generation, turn, since, seq) = row.map_err(sql_error)?;
        let result = liveness::mover(
            &LivenessObject::Turn {
                state: eliot_core::TurnState::Unknown,
                since,
                owner_item: true,
                executor_active: false,
            },
            now,
        );
        tally(stats, &result);
        let choices = ["completed", "failed", "interrupted"].into_iter().map(|status| action(status, "turn.resolve", json!({
            "agent":format!("a{agent}"),"generation":generation,"turn_id":turn,"status":status,"reason":null
        }))).collect();
        items.push(item(
            OwnerInboxKind::UnknownTurn,
            format!("a{agent}:g{generation}:{turn}"),
            format!("event:{seq}"),
            now,
            since,
            "unknown_turn_needs_owner_outcome",
            None,
            choices,
        ));
    }
    Ok(())
}

/// A successor is working after a turn that may have left partial effects.
fn project_switch_fences(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(
        db,
        "SELECT c.id, c.updated_at FROM commands c
         WHERE c.method = 'swarm.switch'
           AND c.state IN ('unknown','failed','rejected','cancelled')
           AND EXISTS(SELECT 1 FROM switch_checkout_owners f WHERE f.switch_command = c.id)",
        "c.id = ?1",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (id, updated) = row.map_err(sql_error)?;
        stats.stuck += 1;
        *stats
            .reasons
            .entry(SWITCH_FENCE_NEEDS_OWNER.to_owned())
            .or_insert(0) += 1;
        let choices = vec![
            action("inspect", "command.get", json!({"id":id})),
            action(
                "release_checkout",
                "command.resolve",
                json!({"command_id":id,"as":"failed","reason":null}),
            ),
        ];
        items.push(item(
            OwnerInboxKind::Stuck,
            format!("switch:{id}"),
            format!("fence:{updated}"),
            now,
            updated,
            SWITCH_FENCE_NEEDS_OWNER,
            None,
            choices,
        ));
    }
    Ok(())
}

fn project_handoffs(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    _stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(
        db,
        "SELECT e.seq, e.ts, CAST(json_extract(e.data,'$.switch_command') AS TEXT),
                CAST(json_extract(e.data,'$.continued_to') AS TEXT)
         FROM events e INDEXED BY events_kind
         WHERE e.kind = 'handoff.settled' AND json_extract(e.data,'$.uncertain') = 1
           AND json_extract(e.data,'$.state') = 'applied'",
        "CAST(json_extract(e.data,'$.switch_command') AS TEXT) = ?1",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (seq, since, switch, successor) = row.map_err(sql_error)?;
        items.push(item(
            OwnerInboxKind::Handoff,
            switch,
            format!("event:{seq}"),
            now,
            since,
            "handoff_after_uncertain_turn",
            None,
            vec![action("inspect", "agent.get", json!({"id": successor}))],
        ));
    }
    let mut stmt = projection_query(
        db,
        "SELECT e.seq, e.ts, a.id, a.generation, json_extract(e.data,'$.message'),
                json_extract(e.data,'$.reason'),
                COALESCE(json_extract(e.data,'$.withheld_routes_blocked'),0)
         FROM events e INDEXED BY events_kind
         JOIN agents a ON a.id = e.agent AND a.generation = e.generation
         WHERE e.kind = 'handoff.waiting' AND a.archived = 0
           AND NOT EXISTS(SELECT 1 FROM agent_successors s WHERE s.agent = a.id)
           AND ((json_extract(e.data,'$.withheld_routes_blocked') = 1 AND a.state IN ('idle','lost','failed'))
                OR (json_extract(e.data,'$.reason') = 'session_lost' AND a.state IN ('lost','failed'))
                OR (json_extract(e.data,'$.reason') = 'provider_hold' AND a.state = 'idle'
                    AND EXISTS(SELECT 1 FROM provider_holds h
                        WHERE h.kind IN ('quota','rate_limit')
                          AND h.scope = CASE WHEN json_type(a.profile_snapshot,'$.account') = 'text'
                              THEN 'account:' || json_extract(a.profile_snapshot,'$.account') || '.' || a.harness
                              ELSE 'profile:' || a.profile END))
                OR (json_extract(e.data,'$.reason') = 'provider_response_withheld'
                    AND a.state IN ('idle','lost','failed')
                    AND EXISTS(SELECT 1 FROM events withheld INDEXED BY events_agent
                        WHERE withheld.agent = e.agent AND withheld.generation = e.generation
                          AND withheld.kind = 'provider.response_withheld'
                          AND json_type(withheld.data,'$.turn_id') = 'text'
                          AND json_extract(withheld.data,'$.turn_id') <> ''
                          AND json_type(withheld.data,'$.task_command') = 'text'
                          AND json_extract(withheld.data,'$.task_command') <> ''
                          AND withheld.seq < e.seq)))
           AND NOT EXISTS(SELECT 1 FROM events newer INDEXED BY events_agent
               WHERE newer.agent = a.id AND newer.generation = a.generation AND newer.seq > e.seq
                 AND newer.kind IN ('handoff.waiting','handoff.queued','handoff.settled'))
           AND (json_extract(e.data,'$.reason') <> 'provider_response_withheld'
                OR (NOT EXISTS(SELECT 1 FROM events newer_success INDEXED BY events_agent
                        WHERE newer_success.agent = a.id AND newer_success.generation = a.generation
                          AND newer_success.seq > e.seq
                          AND newer_success.kind = 'provider.failover_settled'
                          AND json_extract(newer_success.data,'$.state') = 'applied')
                    AND NOT EXISTS(SELECT 1 FROM events newer_turn INDEXED BY events_agent
                        WHERE newer_turn.agent = a.id AND newer_turn.generation = a.generation
                          AND newer_turn.seq > e.seq
                          AND newer_turn.kind IN ('turn.started','turn.completed'))))",
        "'waiting:a' || a.id || ':g' || a.generation = ?1",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, bool>(6)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (seq, since, agent, generation, message, reason, blocked) = row.map_err(sql_error)?;
        let inspect = action("inspect", "agent.get", json!({"id":format!("a{agent}")}));
        let (choices, instructions) = if reason == "provider_response_withheld" || blocked {
            (
                vec![
                    inspect,
                    action(
                        "review_profiles",
                        "settings.read",
                        json!({"sections":["profiles","routing"]}),
                    ),
                ],
                Some(
                    "Review configured profiles and routing to choose another preset, or leave this handoff pending.",
                ),
            )
        } else {
            (vec![inspect], None)
        };
        let mut projected = item(
            OwnerInboxKind::Handoff,
            format!("waiting:a{agent}:g{generation}"),
            format!("event:{seq}"),
            now,
            since,
            &message,
            None,
            choices,
        );
        projected.instructions = instructions.map(str::to_owned);
        items.push(projected);
    }
    Ok(())
}

/// Work that a lost session or a removed directory could still cost.
fn project_work_not_in_git(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    _stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(
        db,
        "SELECT c.agent, c.generation, c.state, c.updated_at
         FROM git_checkpoints c JOIN agents a ON a.id = c.agent AND a.generation = c.generation
         WHERE a.archived = 0
           AND (c.state = 'failed'
                OR (c.state = 'snapshot' AND a.state IN ('stopped','lost','failed')))
           AND NOT EXISTS(SELECT 1 FROM events x INDEXED BY events_kind
               WHERE x.kind = 'agent.handed_off' AND x.agent = c.agent
                 AND x.generation = c.generation)",
        "c.agent = ?1 AND c.generation = ?2",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (agent, generation, state, since) = row.map_err(sql_error)?;
        let reason = if state == "failed" {
            "checkpoint_failed"
        } else {
            "ended_with_uncommitted_work"
        };
        items.push(item(
            OwnerInboxKind::WorkNotInGit,
            format!("a{agent}:g{generation}:checkpoint"),
            format!("{state}:{since}"),
            now,
            since,
            reason,
            None,
            vec![action(
                "inspect",
                "agent.get",
                json!({"id": format!("a{agent}")}),
            )],
        ));
    }
    drop(stmt);
    let mut stmt = projection_query(
        db,
        "SELECT w.agent, w.target_generation, w.updated_at, w.repo_path, w.worktree_path,
                w.requested_branch, c.checkpoint_ref
         FROM workspaces w JOIN agents a ON a.id = w.agent
         LEFT JOIN git_checkpoints c
             ON c.agent = w.agent AND c.generation = w.target_generation
         WHERE w.phase = 'removed' AND a.archived = 0
           AND json_extract(w.outcome,'$.result.directory_absent_before_remove') = 1
           AND (c.state = 'snapshot'
                OR json_extract(w.outcome,'$.last_available_git_observation.dirty') = 1)",
        "w.agent = ?1 AND w.target_generation = ?2",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, Option<String>>(6)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (agent, generation, since, repo, worktree, branch, checkpoint) =
            row.map_err(sql_error)?;
        let mut missing = item(
            OwnerInboxKind::WorkNotInGit,
            format!("a{agent}:g{generation}:worktree"),
            format!("removed:{since}"),
            now,
            since,
            "worktree_missing_with_uncommitted_work",
            None,
            vec![action(
                "inspect",
                "agent.get",
                json!({"id": format!("a{agent}")}),
            )],
        );
        missing.instructions = Some(restore_commands(
            &eliot_core::display_path(&repo),
            &eliot_core::display_path(&worktree),
            &branch,
            &format!("a{agent}"),
            generation,
            checkpoint.as_deref(),
        ));
        items.push(missing);
    }
    Ok(())
}

/// The commands that bring a missing worktree and its saved files back.
fn restore_commands(
    repo: &str,
    worktree: &str,
    branch: &str,
    agent: &str,
    generation: i64,
    checkpoint: Option<&str>,
) -> String {
    let mut commands = format!(
        "git -C {} worktree prune\ngit -C {} worktree add {} {}",
        powershell_quote(repo),
        powershell_quote(repo),
        powershell_quote(worktree),
        powershell_quote(branch),
    );
    if let Some(checkpoint) = checkpoint {
        commands.push_str(&format!(
            "\nswarm checkpoint restore --agent {agent} --generation {generation} --checkpoint-ref {} --workspace {}",
            powershell_quote(checkpoint),
            powershell_quote(worktree),
        ));
    }
    commands
}

fn powershell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn project_holds(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db,
        "SELECT h.scope,h.kind,h.retry_after,h.probe_command,h.state,h.held_since,h.next_probe_at,
                h.automatic_probes,h.probe_outcome_unknown,
                h.updated_at,
                COALESCE((SELECT MAX(e.seq) FROM events e WHERE CAST(json_extract(e.data,'$.scope') AS TEXT)=h.scope
                  AND e.kind IN ('provider.hold_updated','provider.hold_probe_started','provider.hold_probe_budget_exhausted',
                    'provider.hold_probe_outcome_unknown','provider.hold_cleared','provider.hold_probe_deferred')),0),
                EXISTS(SELECT 1 FROM commands c WHERE c.id=h.probe_command AND c.state IN ('accepted','queued','dispatched')),
                h.source
         FROM provider_holds h WHERE h.state IN ('held','probing')", "h.scope=?1", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<i64>>(2)?,
                r.get::<_, Option<String>>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, Option<i64>>(6)?,
                r.get::<_, i64>(7)?,
                r.get::<_, bool>(8)?,
                r.get::<_, i64>(9)?,
                r.get::<_, i64>(10)?,
                r.get::<_, bool>(11)?,
                r.get::<_, Option<String>>(12)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (
            scope,
            raw_kind,
            retry,
            probe_command,
            raw_state,
            since,
            next_probe,
            attempts,
            unknown,
            updated,
            seq,
            probe_active,
            source,
        ) = row.map_err(sql_error)?;
        let state = match raw_state.as_str() {
            "held" => eliot_core::ProviderHoldState::Held,
            "probing" => eliot_core::ProviderHoldState::Probing,
            _ => return Err(crate::Error::new("corrupt", "invalid provider hold state")),
        };
        let kind = match raw_kind.as_str() {
            "rate_limit" => eliot_core::ProviderRestrictionKind::RateLimit,
            "quota" => eliot_core::ProviderRestrictionKind::Quota,
            "auth" => eliot_core::ProviderRestrictionKind::Auth,
            "model_removed" => eliot_core::ProviderRestrictionKind::ModelRemoved,
            "model_mismatch" => eliot_core::ProviderRestrictionKind::ModelMismatch,
            _ => {
                return Err(crate::Error::new(
                    "corrupt",
                    "invalid provider restriction kind",
                ));
            }
        };
        let reset_at = next_probe.or(retry);
        let unattributed = eliot_core::is_unattributed_provider_hold(&scope);
        let needs_owner = unattributed
            || unknown
            || (state == eliot_core::ProviderHoldState::Held
                && (reset_at.is_none()
                    || attempts >= i64::from(eliot_core::MAX_AUTOMATIC_PROVIDER_PROBES)));
        let result = liveness::mover(
            &LivenessObject::Hold {
                state,
                kind,
                since,
                owner_item: needs_owner,
                probe_active,
                reset_at,
            },
            now,
        );
        tally(stats, &result);
        if needs_owner {
            let choices = if state == eliot_core::ProviderHoldState::Held && probe_command.is_none()
            {
                vec![action(
                    "clear_hold",
                    "provider.clear_hold",
                    json!({
                        "scope":scope,"corrective_evidence":null,
                        "expected_probe_command":null
                    }),
                )]
            } else {
                vec![action("inspect", "provider.holds", json!({}))]
            };
            let reason = if unattributed {
                "legacy_account_hold_needs_owner_clear_harness_unattributed"
            } else if unknown {
                "provider_probe_outcome_unknown"
            } else if retry.is_none() && next_probe.is_none() {
                "provider_hold_needs_owner_release"
            } else {
                "provider_probe_budget_exhausted"
            };
            let mismatch_reason = source
                .as_deref()
                .and_then(|raw| serde_json::from_str::<eliot_core::ProviderHoldSource>(raw).ok())
                .and_then(|source| source.model_mismatch)
                .map(|mismatch| mismatch.reason());
            items.push(item(
                OwnerInboxKind::Hold,
                scope,
                format!("held:{since}:updated:{updated}:event:{seq}"),
                now,
                since,
                mismatch_reason.as_deref().unwrap_or(reason),
                None,
                choices,
            ));
        } else if let Mover::Stuck { reason } = result {
            let inspect = action("inspect", "provider.holds", json!({}));
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("hold:{scope}"),
                format!("held:{since}:updated:{updated}:event:{seq}"),
                now,
                since,
                reason,
                None,
                vec![inspect],
            ));
        }
    }
    Ok(())
}

/// One confirmed failure per blocked harness. A pending newer check keeps
/// the last confirmed failure visible until a pass restores routing.
fn project_harness_checks(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(
        db,
        "SELECT h.harness,
                COALESCE(f.checked_version, h.checked_version),
                COALESCE(f.checked_at, h.checked_at),
                COALESCE(f.check_profile, h.check_profile, ''),
                COALESCE(f.failure_step, h.failure_step),
                COALESCE(f.failure_error, h.failure_error)
         FROM harness_checks h
         LEFT JOIN harness_check_history f
           ON h.outcome <> 'failed' AND f.harness=h.harness AND f.version_key=(
               SELECT old.version_key FROM harness_check_history old
               WHERE old.harness=h.harness AND old.outcome='failed'
               ORDER BY old.checked_at DESC, old.version_key LIMIT 1
           )
         WHERE h.automatic_blocked=1 AND (h.outcome='failed' OR f.version_key IS NOT NULL)",
        "('harness:' || h.harness)=?1",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (harness, checked, checked_at, profile, step, error) = row.map_err(sql_error)?;
        tally(
            stats,
            &Mover::Asked {
                reason: "harness_check_failed",
                item: OwnerInboxKind::HarnessCheck,
            },
        );
        items.push(item(
            OwnerInboxKind::HarnessCheck,
            format!("harness:{harness}"),
            format!("checked:{checked}:profile:{profile}:step:{step}"),
            now,
            checked_at,
            &format!("harness {harness} {checked} self-check failed at {step}: {error}"),
            None,
            Vec::new(),
        ));
    }
    Ok(())
}

fn project_agents(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db,
        "SELECT a.id,a.generation,a.state,a.updated_at,a.active_turn,a.archived,
                a.native_scope,a.native_ref,a.observe_only,
                CASE WHEN a.state IN ('created','starting','failed','lost') THEN EXISTS(SELECT 1 FROM commands c
                  WHERE c.target=a.id AND (c.generation=a.generation OR c.expected_generation=a.generation)
                  AND c.verb IN ('launch','resume') AND c.state IN ('accepted','queued','dispatched')) ELSE 0 END,
                EXISTS(SELECT 1 FROM processes p WHERE p.owner IN (
                  'agent:a'||a.id||'#'||a.generation,'workspace:a'||a.id||'#'||a.generation) AND p.state='running'),
                COALESCE((SELECT MAX(e.seq) FROM events e INDEXED BY events_agent WHERE e.agent=a.id AND e.generation=a.generation),0),
                CASE WHEN a.state IN ('failed','lost') THEN COALESCE((SELECT MAX(e.ts) FROM events e
                  WHERE e.agent=a.id AND e.generation=a.generation AND e.kind IN ('agent.failed','agent.lost')),a.updated_at) ELSE a.updated_at END,
                EXISTS(SELECT 1 FROM pending_requests r WHERE r.agent=a.id
                  AND r.generation=a.generation AND r.state='pending'),
                EXISTS(SELECT 1 FROM turns t WHERE t.agent=a.id AND t.generation=a.generation
                  AND t.turn_id=a.active_turn AND t.state='running'),
                EXISTS(SELECT 1 FROM source_state s WHERE s.agent=a.id AND s.generation=a.generation
                  AND s.state='open' AND s.source NOT IN ('daemon','os','git','workspace_process')),
                EXISTS(SELECT 1 FROM turns t WHERE t.agent=a.id AND t.generation=a.generation
                  AND t.state IN ('running','unknown')),
                EXISTS(SELECT 1 FROM commands c WHERE c.target=a.id
                  AND (c.generation=a.generation OR c.expected_generation=a.generation)
                  AND c.state IN ('accepted','queued','dispatched','unknown')),
                CASE WHEN a.state IN ('busy','waiting') AND a.active_turn IS NULL THEN EXISTS(
                  SELECT 1 FROM processes p WHERE p.owner='agent:a'||a.id||'#'||a.generation
                    AND p.role='plugin' AND p.state='running') ELSE 0 END,
                aw.expires_at,aw.created_at,json_extract(aw.condition,'$.kind'),
                json_extract(aw.condition,'$.seconds')
         FROM agents a
         LEFT JOIN agent_wakes aw ON aw.agent=a.id AND aw.generation=a.generation
            AND aw.state='waiting' AND EXISTS(
                SELECT 1 FROM turns wt
                WHERE wt.agent=aw.agent AND wt.generation=aw.generation AND wt.turn_id=aw.turn_id
                  AND (
                    (a.state='idle' AND wt.state='completed'
                      AND (a.active_turn IS NULL OR a.active_turn=aw.turn_id))
                    OR (a.state='waiting' AND a.active_turn=aw.turn_id AND wt.state='running'
                      AND (EXISTS(SELECT 1 FROM processes wp WHERE wp.owner IN (
                             'agent:a'||a.id||'#'||a.generation,
                             'workspace:a'||a.id||'#'||a.generation) AND wp.state='running')
                        OR EXISTS(SELECT 1 FROM source_state ws WHERE ws.agent=a.id
                             AND ws.generation=a.generation AND ws.state='open'
                             AND ws.source NOT IN ('daemon','os','git','workspace_process'))))
                  ))
         WHERE a.observe_only=0 AND a.archived=0
            AND json_type(a.profile_snapshot, '$._harness_check') IS NULL
            AND a.state IN ('created','starting','idle','busy','waiting','failed','lost','unknown')", "a.id=?1", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Option<String>>(4)?,
                r.get::<_, bool>(5)?,
                r.get::<_, Option<String>>(6)?,
                r.get::<_, Option<String>>(7)?,
                r.get::<_, bool>(8)?,
                r.get::<_, bool>(9)?,
                r.get::<_, bool>(10)?,
                r.get::<_, i64>(11)?,
                r.get::<_, i64>(12)?,
                r.get::<_, bool>(13)?,
                r.get::<_, bool>(14)?,
                r.get::<_, bool>(15)?,
                r.get::<_, bool>(16)?,
                r.get::<_, bool>(17)?,
                r.get::<_, bool>(18)?,
                r.get::<_, Option<i64>>(19)?,
                r.get::<_, Option<i64>>(20)?,
                r.get::<_, Option<String>>(21)?,
                r.get::<_, Option<i64>>(22)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (
            id,
            generation,
            raw_state,
            since,
            active_turn,
            archived,
            native_scope,
            native_ref,
            observe_only,
            launch_queued,
            process_running,
            event_seq,
            terminal_at,
            has_request,
            observed_turn,
            source_open,
            unsettled_turn,
            unsettled_command,
            owned_plugin_running,
            wake_expires_at,
            wake_created_at,
            wake_kind,
            wake_seconds,
        ) = row.map_err(sql_error)?;
        let state = match raw_state.as_str() {
            "created" => eliot_core::AgentState::Created,
            "starting" => eliot_core::AgentState::Starting,
            "idle" => eliot_core::AgentState::Idle,
            "busy" => eliot_core::AgentState::Busy,
            "waiting" => eliot_core::AgentState::Waiting,
            "failed" => eliot_core::AgentState::Failed,
            "lost" => eliot_core::AgentState::Lost,
            "unknown" => eliot_core::AgentState::Unknown,
            _ => {
                return Err(crate::Error::new(
                    "corrupt",
                    "invalid agent lifecycle state",
                ));
            }
        };
        if matches!(
            state,
            eliot_core::AgentState::Lost | eliot_core::AgentState::Failed
        ) {
            // A handoff closes this item: the work continues on the successor.
            if archived || crate::handoff::current_successor(db, id)?.is_some() {
                continue;
            }
            let resume_safe = native_scope
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
                && native_ref.as_deref().is_some_and(|s| !s.trim().is_empty())
                && active_turn.is_none()
                && !launch_queued
                && !process_running
                && !observe_only;
            let unknown_start = crate::unknown_start_command(db, id)?;
            let choice = if let Some(command_id) = &unknown_start {
                Some(action(
                    "resolve",
                    "command.resolve",
                    json!({
                        "command_id":command_id, "as":"failed", "reason":null
                    }),
                ))
            } else {
                resume_safe.then(|| action("resume","command.submit",json!({
                    "verb":"resume","target":format!("a{id}"),"expected_generation":generation,"args":{}
                })))
            };
            let reason = if unknown_start.is_some() {
                if state == eliot_core::AgentState::Lost {
                    "lost_agent_resolve_unknown_start_first"
                } else {
                    "failed_agent_resolve_unknown_start_first"
                }
            } else if raw_state == "lost" {
                "lost_agent_needs_owner_attention"
            } else {
                "failed_agent_needs_owner_attention"
            };
            let result = liveness::mover(
                &LivenessObject::Agent {
                    state,
                    since: terminal_at,
                    owner_item: true,
                    launch_queued: false,
                    active_turn: false,
                    wait_evidence: None,
                },
                now,
            );
            tally(stats, &result);
            items.push(item(
                OwnerInboxKind::Agent,
                format!("a{id}"),
                format!("g{generation}:updated:{since}:event:{event_seq}"),
                now,
                terminal_at,
                reason,
                choice.clone(),
                choice.into_iter().collect(),
            ));
        } else {
            let result = liveness::mover(
                &LivenessObject::Agent {
                    state,
                    since,
                    owner_item: has_request,
                    launch_queued: launch_queued
                        || (state == eliot_core::AgentState::Starting && process_running),
                    active_turn: observed_turn && (source_open || process_running),
                    wait_evidence: agent_wait_evidence(
                        wake_expires_at,
                        wake_created_at,
                        wake_kind.as_deref(),
                        wake_seconds,
                        now,
                    ),
                },
                now,
            );
            tally(stats, &result);
            if let Mover::Stuck { reason } = result {
                let stop_safe = matches!(
                    state,
                    eliot_core::AgentState::Busy | eliot_core::AgentState::Waiting
                ) && active_turn.is_none()
                    && !has_request
                    && !unsettled_turn
                    && !unsettled_command
                    && source_open
                    && owned_plugin_running
                    && !observe_only;
                let mut choices = Vec::new();
                let recommended = if stop_safe {
                    let stop = action(
                        "stop",
                        "command.submit",
                        json!({
                            "verb":"stop",
                            "target":format!("a{id}"),
                            "expected_generation":generation,
                            "args":{}
                        }),
                    );
                    choices.push(stop.clone());
                    Some(stop)
                } else {
                    None
                };
                let inspect = action("inspect", "agent.get", json!({"id":format!("a{id}")}));
                choices.push(inspect);
                items.push(item(
                    OwnerInboxKind::Stuck,
                    format!("a{id}:g{generation}"),
                    format!("state:{raw_state}:updated:{since}:event:{event_seq}"),
                    now,
                    since,
                    reason,
                    recommended,
                    choices,
                ));
            }
        }
    }
    Ok(())
}

fn agent_wait_evidence(
    expires_at: Option<i64>,
    created_at: Option<i64>,
    kind: Option<&str>,
    seconds: Option<i64>,
    now: i64,
) -> Option<AgentWaitEvidence> {
    if expires_at? <= now {
        return None;
    }
    match kind? {
        "seconds" => {
            let wait_ms = u64::try_from(seconds?)
                .ok()?
                .checked_mul(1_000)
                .and_then(|millis| i64::try_from(millis).ok())?;
            let due_at = created_at?.checked_add(wait_ms)?;
            (due_at > now).then_some(AgentWaitEvidence::Timer)
        }
        "file_appears" | "process_exit" | "argv_exit0" | "child_completion" => {
            Some(AgentWaitEvidence::SelfWake)
        }
        _ => None,
    }
}

fn project_goals(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let deferral_limit_reason = crate::goals::COMPLETION_DEFERRAL_LIMIT_REASON;
    let sql = format!("SELECT g.agent,g.mode,g.generation,g.revision,json_extract(g.state,'$.status'),g.updated_at,
       a.generation=g.generation AND a.state IN ('idle','busy','waiting')
         AND NOT EXISTS(SELECT 1 FROM goal_markers m WHERE m.agent=g.agent AND m.mode=g.mode
            AND m.generation=g.generation AND m.revision=g.revision AND m.state='unknown'),
       EXISTS(SELECT 1 FROM goal_markers m WHERE m.agent=g.agent AND m.mode=g.mode AND m.generation=g.generation AND m.revision=g.revision AND m.state='running'),
       COALESCE((SELECT MAX(e.seq) FROM events e WHERE e.agent=g.agent AND e.generation=g.generation AND e.kind='goal.changed'),0),
       json_extract(g.state,'$.cadence_anchor_ms') + json_extract(g.definition,'$.cadence_ms')
           * (COALESCE(json_extract(g.state,'$.last_committed_slot'),0) + 1),
       COALESCE(json_extract(g.state,'$.paused_reason') = '{deferral_limit_reason}',0)
       FROM goals g JOIN agents a ON a.id=g.agent WHERE (json_extract(g.state,'$.status') IN ('active','blocked')
         OR (json_extract(g.state,'$.status') = 'paused'
             AND json_extract(g.state,'$.paused_reason') = '{deferral_limit_reason}'))");
    let mut stmt = projection_query(db, &sql, "g.agent=?1 AND g.mode=?2", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, bool>(6)?,
                r.get::<_, bool>(7)?,
                r.get::<_, i64>(8)?,
                r.get::<_, Option<i64>>(9)?,
                r.get::<_, bool>(10)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (
            agent,
            raw_mode,
            generation,
            revision,
            raw_state,
            since,
            reconciler_active,
            round_active,
            event_seq,
            timer_due,
            deferral_paused,
        ) = row.map_err(sql_error)?;
        let state = match raw_state.as_str() {
            "active" => GoalStatus::Active,
            "blocked" => GoalStatus::Blocked,
            "paused" => GoalStatus::Paused,
            "cleared" => GoalStatus::Cleared,
            "completed" => GoalStatus::Completed,
            _ => return Err(crate::Error::new("corrupt", "invalid goal status")),
        };
        let status = match state {
            GoalStatus::Active | GoalStatus::Blocked | GoalStatus::Paused => state,
            GoalStatus::Cleared | GoalStatus::Completed => continue,
        };
        let result = liveness::mover(
            &LivenessObject::Goal {
                state: status,
                since,
                owner_item: status == GoalStatus::Blocked || deferral_paused,
                round_active: round_active || (reconciler_active && timer_due.is_none()),
                timer_due: timer_due.filter(|_| reconciler_active),
            },
            now,
        );
        tally(stats, &result);
        let mode = match raw_mode.as_str() {
            "eliot" => GoalMode::Eliot,
            "native" => GoalMode::Native,
            _ => return Err(crate::Error::new("corrupt", "invalid goal mode")),
        };
        let subject = format!("a{agent}:{}", mode_name(mode));
        if status == GoalStatus::Blocked || deferral_paused {
            let reason = if deferral_paused {
                deferral_limit_reason
            } else {
                "goal_blocked"
            };
            let clear = action(
                "clear",
                "goal.clear",
                json!({"agent":format!("a{agent}"),"mode":mode_name(mode)}),
            );
            items.push(item(
                OwnerInboxKind::BlockedGoal,
                subject,
                format!("g{generation}:goal:{revision}:event:{event_seq}"),
                now,
                since,
                reason,
                None,
                vec![clear],
            ));
        } else if let Mover::Stuck { reason } = result {
            let inspect = action(
                "inspect",
                "goal.get",
                json!({"agent":format!("a{agent}"),"mode":mode_name(mode)}),
            );
            let round = crate::goals::stuck_goal_round_args(db, agent, mode_name(mode), now)?
                .map(|args| action("round", "goal.round", args));
            let (recommended, choices) = if let Some(round) = round {
                (Some(round.clone()), vec![round, inspect])
            } else {
                (None, vec![inspect])
            };
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("goal:{subject}"),
                format!("g{generation}:goal:{revision}:event:{event_seq}"),
                now,
                since,
                reason,
                recommended,
                choices,
            ));
        }
    }
    Ok(())
}
fn mode_name(mode: GoalMode) -> &'static str {
    match mode {
        GoalMode::Eliot => "eliot",
        GoalMode::Native => "native",
    }
}

fn project_launch_intents(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(
        db,
        "SELECT q.id,w.created_at,w.result,q.route
         FROM launch_intents q JOIN writes w ON w.id=q.id WHERE 1",
        "q.id=?1",
        bindings,
    )?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (id, since, raw_view, route) = row.map_err(sql_error)?;
        let view: LaunchIntentView = serde_json::from_str(&raw_view).map_err(|e| {
            crate::Error::new("corrupt", format!("invalid launch intent result: {e}"))
        })?;
        let command_active = view.command_id.is_some();
        let owner_linked = view.reason.as_ref().is_some_and(|reason| {
            matches!(
                reason.changed_by.as_str(),
                "owner" | "owner_or_delegated_manager"
            )
        });
        let result = liveness::mover(
            &LivenessObject::LaunchIntent {
                state: view.state.clone(),
                since,
                command_active: command_active || view.state == LaunchIntentState::Queued,
                owner_item: owner_linked,
            },
            now,
        );
        tally(stats, &result);
        if owner_linked {
            let read = action("inspect", "launch.intent.get", json!({"id":id}));
            items.push(item(
                OwnerInboxKind::LaunchQueue,
                format!("launch:{id}"),
                format!("{since}:{raw_view}:{route:?}"),
                now,
                since,
                "launch_queue_waits_for_owner_change",
                None,
                vec![read],
            ));
        } else if matches!(result, Mover::Stuck { .. }) {
            let reason = match result {
                Mover::Stuck { reason } => reason,
                _ => unreachable!(),
            };
            let inspect = action("inspect", "launch.intent.get", json!({"id":id}));
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("launch:{id}"),
                format!("{since}:{raw_view}:{route:?}"),
                now,
                since,
                reason,
                None,
                vec![inspect],
            ));
        }
    }
    Ok(())
}

fn project_assignment_waits(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db,
        "SELECT w.timer,w.revision,w.due,w.project,w.reason,t.next_due_at,r.enabled,r.paused_reason,
                p.rule IS NOT NULL,w.route
         FROM assignment_waits w JOIN timers t ON t.principal='owner' AND t.rule=w.timer
             AND t.schedule_revision=w.revision AND t.next_due_at=w.due
         JOIN rule_state r ON r.principal=t.principal AND r.rule=t.rule
         LEFT JOIN paused_assignment_timers p ON p.rule=t.rule WHERE 1",
        "w.timer=?1", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, Option<i64>>(5)?,
                r.get::<_, bool>(6)?,
                r.get::<_, Option<String>>(7)?,
                r.get::<_, bool>(8)?,
                r.get::<_, Option<String>>(9)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (
            timer,
            revision,
            due,
            project,
            raw_reason,
            next_due,
            enabled,
            paused_reason,
            explicitly_paused,
            route,
        ) = row.map_err(sql_error)?;
        let reason: eliot_core::api::LaunchWaitReason =
            serde_json::from_str(&raw_reason).map_err(|e| {
                crate::Error::new("corrupt", format!("invalid assignment wait reason: {e}"))
            })?;
        let owner_linked = matches!(
            reason.changed_by.as_str(),
            "owner" | "owner_or_delegated_manager"
        );
        let active =
            enabled && paused_reason.is_none() && !explicitly_paused && next_due == Some(due);
        let result = liveness::mover(
            &LivenessObject::AssignmentWait {
                since: due,
                due,
                timer_active: active,
                owner_linked,
            },
            now,
        );
        tally(stats, &result);
        if owner_linked {
            let read = action(
                "inspect",
                "launch.queue",
                json!({"project":project,"after":format!("a:{timer}")}),
            );
            items.push(item(
                OwnerInboxKind::LaunchQueue,
                format!("assignment:{timer}"),
                format!("{revision}:{due}:{raw_reason}:{route:?}"),
                now,
                due,
                "assignment_wait_needs_owner_change",
                None,
                vec![read],
            ));
        } else if let Mover::Stuck { reason } = result {
            let inspect = action(
                "inspect",
                "launch.queue",
                json!({"project":project,"after":format!("a:{timer}")}),
            );
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("assignment:{timer}"),
                format!("{revision}:{due}:{raw_reason}:{route:?}"),
                now,
                due,
                reason,
                None,
                vec![inspect],
            ));
        }
    }
    Ok(())
}

fn project_timers(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db,
        "SELECT t.principal,t.rule,t.schedule_revision,COALESCE(t.last_key,t.anchor),t.next_due_at,
                COALESCE((SELECT MAX(e.seq) FROM events e
                   WHERE e.kind IN ('rule.fired','rule.skipped','rule.rejected')
                   AND CAST(json_extract(e.data,'$.rule') AS TEXT)=t.rule
                   AND json_extract(e.data,'$.schedule_revision')=t.schedule_revision),0)
         FROM timers t JOIN rule_state r ON r.principal=t.principal AND r.rule=t.rule
         LEFT JOIN paused_assignment_timers p ON t.principal='owner' AND p.rule=t.rule
         WHERE t.next_due_at IS NOT NULL AND r.enabled=1 AND r.paused_reason IS NULL AND p.rule IS NULL", "t.principal=?1 AND t.rule=?2", bindings)?;
    let rows = stmt
        .query_map(bindings, |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, Option<i64>>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })
        .map_err(sql_error)?;
    for row in rows {
        let (principal, rule, revision, anchor, due, event_seq) = row.map_err(sql_error)?;
        let due = due
            .ok_or_else(|| crate::Error::new("corrupt", "timer query returned a null due time"))?;
        let result = liveness::mover(
            &LivenessObject::Timer {
                since: anchor,
                due: Some(due),
                active: true,
            },
            now,
        );
        tally(stats, &result);
        if let Mover::Stuck { reason } = result {
            let subject = format!("{principal}:{rule}");
            let mut inspect_args = json!({"id":rule});
            if let Some(project) = principal.strip_prefix("project:") {
                inspect_args["project"] = json!(project);
            }
            let inspect = action("inspect", "timer.get", inspect_args);
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("timer:{subject}"),
                format!("timer:{revision}:due:{due}:event:{event_seq}"),
                now,
                due,
                reason,
                None,
                vec![inspect],
            ));
        }
    }
    Ok(())
}

fn tally(stats: &mut SwarmLiveness, result: &Mover) {
    let reason = match result {
        Mover::Moving { .. } => {
            stats.moving += 1;
            return;
        }
        Mover::Asked { .. } => {
            stats.asked += 1;
            return;
        }
        Mover::Parked { reason } => {
            stats.parked += 1;
            *reason
        }
        Mover::Stuck { reason } => {
            stats.stuck += 1;
            *reason
        }
        Mover::Long { reason } => {
            stats.long += 1;
            *reason
        }
    };
    *stats.reasons.entry(reason.to_owned()).or_insert(0) += 1;
}
fn action(choice: &str, method: &str, args: Value) -> OwnerAction {
    OwnerAction {
        choice: choice.to_owned(),
        method: method.to_owned(),
        args,
    }
}
#[allow(
    clippy::too_many_arguments,
    reason = "The decision item's identity, timestamps, and actions are explicit at each call site"
)]
fn item(
    kind: OwnerInboxKind,
    subject: String,
    subject_revision: String,
    now: i64,
    since: i64,
    reason: &str,
    recommended: Option<OwnerAction>,
    choices: Vec<OwnerAction>,
) -> OwnerInboxItem {
    // The caller supplies a subject revision. Bind every projected choice and
    // recommendation here, including each action's method and arguments.
    let binding = json!([subject_revision, reason, &recommended, &choices]);
    let revision = eliot_core::api::inference::sha256_hex(binding.to_string().as_bytes());
    OwnerInboxItem {
        kind,
        subject,
        revision,
        age_ms: now.saturating_sub(since).max(0),
        reason: reason.to_owned(),
        detail: None,
        recommended,
        choices,
        instructions: None,
    }
}
