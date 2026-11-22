//! Read-only owner decision and liveness projections over existing Store rows.

use crate::sql_error;
use eliot_core::api::{
    GoalMode, GoalStatus, LaunchIntentState, LaunchIntentView, OwnerAction, OwnerInboxItem,
    OwnerInboxKind, SwarmLiveness,
};
use eliot_core::liveness::{self, LivenessObject, Mover};
use rusqlite::{CachedStatement, Connection, ToSql};
use serde_json::{Value, json};

pub(crate) fn project(
    connection: &Connection,
    now: i64,
) -> eliot_core::Result<(Vec<OwnerInboxItem>, SwarmLiveness)> {
    let mut items = Vec::new();
    let mut liveness = SwarmLiveness::default();

    project_requests(connection, now, &mut items, &mut liveness, &[])?;
    project_unknown_commands(connection, now, &mut items, &mut liveness, &[])?;
    project_unknown_turns(connection, now, &mut items, &mut liveness, &[])?;
    project_holds(connection, now, &mut items, &mut liveness, &[])?;
    project_agents(connection, now, &mut items, &mut liveness, &[])?;
    project_goals(connection, now, &mut items, &mut liveness, &[])?;
    project_launch_intents(connection, now, &mut items, &mut liveness, &[])?;
    project_assignment_waits(connection, now, &mut items, &mut liveness, &[])?;
    project_timers(connection, now, &mut items, &mut liveness, &[])?;

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
    type Projector = fn(
        &Connection,
        i64,
        &mut Vec<OwnerInboxItem>,
        &mut SwarmLiveness,
        &[&dyn ToSql],
    ) -> eliot_core::Result<()>;
    let (projector, key): (Projector, &str) = match kind {
        OwnerInboxKind::HarnessRequest => (project_requests, subject),
        OwnerInboxKind::UnknownCommand => (project_unknown_commands, subject),
        OwnerInboxKind::UnknownTurn => (project_unknown_turns, subject),
        OwnerInboxKind::Hold => (project_holds, subject),
        OwnerInboxKind::Agent => (project_agents, subject),
        OwnerInboxKind::BlockedGoal => (project_goals, subject),
        OwnerInboxKind::LaunchQueue | OwnerInboxKind::Stuck => match subject.split_once(':') {
            Some(("command", id)) if kind == OwnerInboxKind::Stuck => {
                (project_unknown_commands, id)
            }
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
         WHERE r.state='pending'", "r.agent=?1 AND r.generation=?2 AND r.request_id=?3", bindings)?;
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
                CASE WHEN c.state='unknown' THEN COALESCE((SELECT e.ts FROM events e INDEXED BY autonomy_fleet_unknown_event_lookup
                  WHERE e.kind='command.unknown'
                  AND CAST(json_extract(e.data,'$.command_id') AS TEXT)=c.id ORDER BY e.ts DESC, e.seq DESC LIMIT 1),c.updated_at) ELSE c.updated_at END,
                CASE WHEN c.state='unknown' THEN COALESCE((SELECT MAX(e.seq) FROM events e INDEXED BY autonomy_fleet_unknown_event_lookup
                  WHERE e.kind='command.unknown'
                  AND CAST(json_extract(e.data,'$.command_id') AS TEXT)=c.id),0) ELSE 0 END,
                json_extract(c.outcome,'$.stage'),c.args,
                (EXISTS(SELECT 1 FROM processes p WHERE p.owner='agent:a'||c.target||'#'||c.generation AND p.state IN ('running','unknown'))),
                EXISTS(SELECT 1 FROM source_state s WHERE s.agent=c.target AND s.generation=c.generation
                  AND s.source NOT IN ('daemon','os','git','workspace_process') AND s.state='open')
         FROM commands c WHERE c.state IN ('accepted','queued','dispatched','unknown')", "c.id=?1", bindings)?;
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
                r.get::<_, String>(11)?,
                r.get::<_, bool>(12)?,
                r.get::<_, bool>(13)?,
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
            _raw_args,
            process_active,
            source_open,
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
                items.push(item(
                    OwnerInboxKind::Stuck,
                    format!("command:{id}"),
                    format!("event:{seq}"),
                    now,
                    since,
                    reason,
                    None,
                    Vec::new(),
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
         FROM turns t JOIN agents a ON a.id=t.agent WHERE t.state='unknown'", "t.agent=?1 AND t.generation=?2 AND t.turn_id=?3", bindings)?;
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
                EXISTS(SELECT 1 FROM commands c WHERE c.id=h.probe_command AND c.state IN ('accepted','queued','dispatched'))
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
            _ => {
                return Err(crate::Error::new(
                    "corrupt",
                    "invalid provider restriction kind",
                ));
            }
        };
        let reset_at = next_probe.or(retry);
        let needs_owner = unknown
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
            let reason = if unknown {
                "provider_probe_outcome_unknown"
            } else if retry.is_none() && next_probe.is_none() {
                "provider_hold_needs_owner_release"
            } else {
                "provider_probe_budget_exhausted"
            };
            items.push(item(
                OwnerInboxKind::Hold,
                scope,
                format!("held:{since}:updated:{updated}:event:{seq}"),
                now,
                since,
                reason,
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
                Some(inspect.clone()),
                vec![inspect],
            ));
        }
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
                CASE WHEN a.state IN ('created','starting','failed','lost') THEN EXISTS(SELECT 1 FROM commands c INDEXED BY commands_target_active_start
                  WHERE c.target=a.id AND (c.generation=a.generation OR c.expected_generation=a.generation)
                  AND c.verb IN ('launch','resume') AND c.state IN ('accepted','queued','dispatched')) ELSE 0 END,
                CASE WHEN a.state IN ('starting','busy','waiting','failed','lost') THEN (
                  EXISTS(SELECT 1 FROM processes p WHERE p.owner='agent:a'||a.id||'#'||a.generation AND p.state='running')
                  OR EXISTS(SELECT 1 FROM processes p WHERE p.owner='workspace:a'||a.id||'#'||a.generation AND p.state='running')
                ) ELSE 0 END,
                COALESCE((SELECT MAX(e.seq) FROM events e INDEXED BY events_agent WHERE e.agent=a.id AND e.generation=a.generation),0),
                CASE WHEN a.state IN ('failed','lost') THEN COALESCE((SELECT MAX(e.ts) FROM events e INDEXED BY autonomy_fleet_agent_state_event
                  WHERE e.agent=a.id AND e.generation=a.generation AND e.kind IN ('agent.failed','agent.lost')),a.updated_at) ELSE a.updated_at END,
                CASE WHEN a.state='waiting' THEN EXISTS(SELECT 1 FROM pending_requests r WHERE r.agent=a.id
                  AND r.generation=a.generation AND r.state='pending') ELSE 0 END,
                CASE WHEN a.active_turn IS NOT NULL THEN EXISTS(SELECT 1 FROM turns t WHERE t.agent=a.id AND t.generation=a.generation
                  AND t.turn_id=a.active_turn AND t.state='running') ELSE 0 END,
                CASE WHEN a.state IN ('idle','busy','waiting','unknown') THEN EXISTS(SELECT 1 FROM source_state s WHERE s.agent=a.id AND s.generation=a.generation
                  AND s.state='open' AND s.source NOT IN ('daemon','os','git','workspace_process')) ELSE 0 END
         FROM agents a WHERE a.observe_only=0 AND a.archived=0
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
            if archived {
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
                },
                now,
            );
            tally(stats, &result);
            if let Mover::Stuck { reason } = result {
                let inspect = action("inspect", "agent.get", json!({"id":format!("a{id}")}));
                items.push(item(
                    OwnerInboxKind::Stuck,
                    format!("a{id}:g{generation}"),
                    format!("event:{event_seq}"),
                    now,
                    since,
                    reason,
                    Some(inspect.clone()),
                    vec![inspect],
                ));
            }
        }
    }
    Ok(())
}

fn project_goals(
    db: &Connection,
    now: i64,
    items: &mut Vec<OwnerInboxItem>,
    stats: &mut SwarmLiveness,
    bindings: &[&dyn ToSql],
) -> eliot_core::Result<()> {
    let mut stmt = projection_query(db, "SELECT g.agent,g.mode,g.generation,g.revision,json_extract(g.state,'$.status'),g.updated_at,
       a.generation=g.generation AND a.state IN ('idle','busy','waiting')
         AND NOT EXISTS(SELECT 1 FROM goal_markers m WHERE m.agent=g.agent AND m.mode=g.mode
            AND m.generation=g.generation AND m.revision=g.revision AND m.state='unknown'),
       EXISTS(SELECT 1 FROM goal_markers m WHERE m.agent=g.agent AND m.mode=g.mode AND m.generation=g.generation AND m.revision=g.revision AND m.state='running'),
       COALESCE((SELECT MAX(e.seq) FROM events e INDEXED BY events_goal_changed WHERE e.agent=g.agent AND e.generation=g.generation AND e.kind='goal.changed'),0),
       json_extract(g.state,'$.cadence_anchor_ms') + json_extract(g.definition,'$.cadence_ms')
           * (COALESCE(json_extract(g.state,'$.last_committed_slot'),0) + 1)
       FROM goals g JOIN agents a ON a.id=g.agent WHERE json_extract(g.state,'$.status') IN ('active','blocked')", "g.agent=?1 AND g.mode=?2", bindings)?;
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
                owner_item: status == GoalStatus::Blocked,
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
        if status == GoalStatus::Blocked {
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
                "goal_blocked",
                None,
                vec![clear],
            ));
        } else if let Mover::Stuck { reason } = result {
            let inspect = action(
                "inspect",
                "goal.get",
                json!({"agent":format!("a{agent}"),"mode":mode_name(mode)}),
            );
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("goal:{subject}"),
                format!("g{generation}:goal:{revision}:event:{event_seq}"),
                now,
                since,
                reason,
                Some(inspect.clone()),
                vec![inspect],
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
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("launch:{id}"),
                format!("{since}:{raw_view}:{route:?}"),
                now,
                since,
                reason,
                None,
                Vec::new(),
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
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("assignment:{timer}"),
                format!("{revision}:{due}:{raw_reason}:{route:?}"),
                now,
                due,
                reason,
                None,
                Vec::new(),
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
            items.push(item(
                OwnerInboxKind::Stuck,
                format!("timer:{subject}"),
                format!("timer:{revision}:due:{due}:event:{event_seq}"),
                now,
                due,
                reason,
                None,
                Vec::new(),
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
    revision: String,
    now: i64,
    since: i64,
    reason: &str,
    recommended: Option<OwnerAction>,
    choices: Vec<OwnerAction>,
) -> OwnerInboxItem {
    let binding = json!([revision, reason, &recommended, &choices]);
    let revision = eliot_core::api::inference::sha256_hex(binding.to_string().as_bytes());
    OwnerInboxItem {
        kind,
        subject,
        revision,
        age_ms: now.saturating_sub(since).max(0),
        reason: reason.to_owned(),
        recommended,
        choices,
    }
}
