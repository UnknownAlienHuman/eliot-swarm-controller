use crate::usage::{RoleUsage, manager_at_sql};
use eliot_core::api::{
    AutonomyOptOut, AutonomyOwnerWaiting, AutonomyProject, AutonomyProjectPolicy, AutonomyRow,
    AutonomyUsage, AutonomyWaitingItem,
};
use rusqlite::{Connection, params};
use std::collections::BTreeMap;

const INPUT_CAP: usize = 10_000;
const FETCH_LIMIT: i64 = INPUT_CAP as i64 + 1;
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
const LIST_CAP: usize = 20;

pub(super) struct Fleet {
    pub(super) project_policy: AutonomyRow<AutonomyProjectPolicy>,
    pub(super) owner_waiting: AutonomyRow<AutonomyOwnerWaiting>,
    pub(super) usage: AutonomyRow<AutonomyUsage>,
}

pub(super) const INDEXES: &str = r#"
CREATE INDEX IF NOT EXISTS autonomy_fleet_agents_created_range
    ON agents(created_at DESC, id DESC) WHERE origin = 'eliot';
CREATE INDEX IF NOT EXISTS autonomy_fleet_agent_created_event
    ON events(agent, generation, seq) WHERE kind = 'agent.created';
CREATE INDEX IF NOT EXISTS autonomy_fleet_current_unknown_commands
    ON commands(id) WHERE state = 'unknown';
CREATE INDEX IF NOT EXISTS autonomy_fleet_unknown_event_lookup
    ON events(CAST(json_extract(data, '$.command_id') AS TEXT), ts DESC, seq DESC)
    WHERE kind = 'command.unknown';
CREATE INDEX autonomy_child_creation ON agents(parent, created_at);
CREATE INDEX IF NOT EXISTS autonomy_fleet_agent_state_event
    ON events(agent, generation, kind, ts DESC, seq DESC)
    WHERE kind IN ('agent.failed', 'agent.lost');
CREATE INDEX IF NOT EXISTS autonomy_fleet_turn_window
    ON turns(COALESCE(completed_at, started_at, 0), agent, generation, turn_id);
"#;

pub(super) fn read(
    tx: &Connection,
    since: i64,
    until: i64,
    confirmed: &crate::StuckStates,
) -> eliot_core::Result<Fleet> {
    if since > until {
        return Err(crate::Error::new(
            "invalid",
            "autonomy fleet window starts after it ends",
        ));
    }

    Ok(Fleet {
        project_policy: read_project_policy(tx, since, until)?,
        owner_waiting: read_owner_waiting(tx, until, confirmed)?,
        usage: read_usage(tx, since, until)?,
    })
}

fn read_project_policy(
    tx: &Connection,
    since: i64,
    until: i64,
) -> eliot_core::Result<AutonomyRow<AutonomyProjectPolicy>> {
    let mut statement = tx
        .prepare_cached(
            "SELECT a.id,
                    COALESCE(
                        CASE WHEN json_type(e.data, '$.project') = 'text'
                             THEN json_extract(e.data, '$.project') END,
                        a.project
                    ),
                    a.policy_applied, a.policy_opt_out_reason,
                    e.seq IS NOT NULL AND json_type(e.data, '$.project') = 'text'
             FROM agents AS a INDEXED BY autonomy_fleet_agents_created_range
             LEFT JOIN events AS e ON e.seq = (
                 SELECT created.seq
                 FROM events AS created INDEXED BY autonomy_fleet_agent_created_event
                 WHERE created.agent = a.id AND created.generation = 0
                   AND created.kind = 'agent.created'
                 ORDER BY created.seq
                 LIMIT 1
             )
             WHERE a.origin = 'eliot' AND a.created_at BETWEEN ?1 AND ?2
             ORDER BY a.created_at DESC, a.id DESC
             LIMIT ?3",
        )
        .map_err(crate::sql_error)?;
    let mut rows = statement
        .query(params![since, until, FETCH_LIMIT])
        .map_err(crate::sql_error)?;

    let mut created = 0_u64;
    let mut applied = 0_u64;
    let mut opted_out = 0_u64;
    let mut by_project = BTreeMap::<String, (u64, u64, u64)>::new();
    let mut opt_outs = Vec::with_capacity(LIST_CAP);
    let mut opt_out_count = 0_u64;
    let mut examined = 0_usize;
    let mut input_overflow = false;
    let mut missing_creation_evidence = false;

    while let Some(row) = rows.next().map_err(crate::sql_error)? {
        if examined == INPUT_CAP {
            input_overflow = true;
            break;
        }
        examined += 1;

        let id = row.get::<_, i64>(0).map_err(crate::sql_error)?;
        let project = row.get::<_, String>(1).map_err(crate::sql_error)?;
        let policy_applied = row.get::<_, bool>(2).map_err(crate::sql_error)?;
        let reason = row.get::<_, Option<String>>(3).map_err(crate::sql_error)?;
        let has_creation_evidence = row.get::<_, bool>(4).map_err(crate::sql_error)?;

        missing_creation_evidence |= !has_creation_evidence;
        created += 1;
        applied += u64::from(policy_applied);
        opted_out += u64::from(reason.is_some());

        let tally = by_project.entry(project.clone()).or_default();
        tally.0 += 1;
        tally.1 += u64::from(policy_applied);
        tally.2 += u64::from(reason.is_some());

        if let Some(reason) = reason {
            opt_out_count += 1;
            if opt_outs.len() < LIST_CAP {
                opt_outs.push(AutonomyOptOut {
                    agent: crate::format_agent_id(id),
                    project,
                    reason: eliot_core::api::sanitize_project_policy_opt_out_reason(reason),
                });
            }
        }
    }

    let projects_truncated = input_overflow || by_project.len() > LIST_CAP;
    let opt_outs_truncated = input_overflow || opt_out_count > LIST_CAP as u64;
    let projects = by_project
        .into_iter()
        .take(LIST_CAP)
        .map(|(project, (created, applied, opted_out))| AutonomyProject {
            project,
            created,
            applied,
            opted_out,
        })
        .collect();
    let counts = AutonomyProjectPolicy {
        created,
        applied,
        opted_out,
        projects,
        projects_truncated,
        opt_outs,
        opt_outs_truncated,
    };

    let mut reasons = Vec::new();
    if input_overflow {
        reasons.push(format!(
            "creation scan exceeded {INPUT_CAP} retained Agent rows. Counts cover the newest {INPUT_CAP}"
        ));
    }
    if missing_creation_evidence {
        reasons.push(
            "some retained Agents lack an agent.created project record. Their current project labels were used"
                .to_owned(),
        );
    }
    Ok(row_with_coverage(
        "sqlite:agents.created_at+agent.created",
        reasons,
        counts,
    ))
}

fn read_owner_waiting(
    tx: &Connection,
    until: i64,
    confirmed: &crate::StuckStates,
) -> eliot_core::Result<AutonomyRow<AutonomyOwnerWaiting>> {
    let (items, _) = crate::owner_inbox::project(tx, until)?;
    let waiting = items
        .into_iter()
        .filter(|item| {
            item.age_ms > DAY_MS && crate::owner_decisions::is_reportable(item, until, confirmed)
        })
        .collect::<Vec<_>>();
    let mut by_kind = std::collections::BTreeMap::new();
    for item in &waiting {
        *by_kind.entry(item.kind).or_insert(0_u64) += 1;
    }
    let unknown_commands = waiting
        .iter()
        .filter(|item| item.kind == eliot_core::api::OwnerInboxKind::UnknownCommand)
        .count() as u64;
    let failed_or_lost_agents = waiting
        .iter()
        .filter(|item| item.kind == eliot_core::api::OwnerInboxKind::Agent)
        .count() as u64;
    let provider_holds = waiting
        .iter()
        .filter(|item| item.kind == eliot_core::api::OwnerInboxKind::Hold)
        .count() as u64;
    let mut oldest = waiting
        .iter()
        .map(|item| {
            let kind = match item.kind {
                eliot_core::api::OwnerInboxKind::UnknownCommand => "command.unknown",
                eliot_core::api::OwnerInboxKind::Agent if item.reason.starts_with("lost_agent") => {
                    "agent.lost"
                }
                eliot_core::api::OwnerInboxKind::Agent => "agent.failed",
                eliot_core::api::OwnerInboxKind::Hold => "provider_hold",
                kind => kind.as_str(),
            };
            (
                item.age_ms,
                AutonomyWaitingItem {
                    kind: kind.to_owned(),
                    id: item.subject.clone(),
                    since_ms: until.saturating_sub(item.age_ms),
                },
            )
        })
        .collect::<Vec<_>>();
    oldest.sort_by(|(age_a, a), (age_b, b)| {
        age_b
            .cmp(age_a)
            .then_with(|| a.kind.cmp(&b.kind))
            .then_with(|| a.id.cmp(&b.id))
    });
    let oldest_truncated = oldest.len() > LIST_CAP;
    oldest.truncate(LIST_CAP);
    let oldest = oldest.into_iter().map(|(_, item)| item).collect();
    let counts = AutonomyOwnerWaiting {
        unknown_commands,
        failed_or_lost_agents,
        provider_holds,
        total: waiting.len() as u64,
        by_kind,
        oldest,
        oldest_truncated,
    };
    Ok(row_with_coverage(
        "sqlite:owner_inbox.project",
        Vec::new(),
        counts,
    ))
}
fn read_usage(
    tx: &Connection,
    since: i64,
    until: i64,
) -> eliot_core::Result<AutonomyRow<AutonomyUsage>> {
    let manager = manager_at_sql("agents.id", "candidates.observed_at");
    let mut statement = tx
        .prepare_cached(&format!(
            "WITH candidates AS (
                 SELECT agent, generation, turn_id, state, usage,
                        COALESCE(completed_at, started_at, 0) AS observed_at
                 FROM turns INDEXED BY autonomy_fleet_turn_window
                 WHERE COALESCE(completed_at, started_at, 0) BETWEEN ?1 AND ?2
                 ORDER BY COALESCE(completed_at, started_at, 0) DESC,
                          agent DESC, generation DESC, turn_id DESC
                 LIMIT ?3
             )
             SELECT candidates.state, candidates.usage, agents.harness,
                    CASE WHEN agents.id IS NULL THEN NULL
                         ELSE {manager} END,
                    agents.origin
             FROM candidates
             LEFT JOIN agents ON agents.id = candidates.agent
             ORDER BY candidates.observed_at DESC, candidates.agent DESC,
                      candidates.generation DESC, candidates.turn_id DESC",
        ))
        .map_err(crate::sql_error)?;
    let mut rows = statement
        .query(params![since, until, FETCH_LIMIT])
        .map_err(crate::sql_error)?;

    let mut counts = AutonomyUsage {
        manager_input_tokens: 0,
        executor_input_tokens: 0,
        observed_calls: 0,
        manager_input_share: None,
        average_context_tokens: None,
    };
    let mut context_sum = 0_u128;
    let mut examined = 0_usize;
    let mut input_overflow = false;
    let mut missing_usage = false;
    let mut role_usage = RoleUsage::default();
    let mut missing_context = false;
    let mut unsettled_turn = false;
    let mut missing_agent = false;

    while let Some(row) = rows.next().map_err(crate::sql_error)? {
        if examined == INPUT_CAP {
            input_overflow = true;
            break;
        }
        examined += 1;

        let Some(origin) = row.get::<_, Option<String>>(4).map_err(crate::sql_error)? else {
            missing_agent = true;
            continue;
        };
        if origin != "eliot" {
            continue;
        }
        let manager = row.get::<_, bool>(3).map_err(crate::sql_error)?;

        let state = row.get::<_, String>(0).map_err(crate::sql_error)?;
        if state != "completed" {
            unsettled_turn = true;
            continue;
        }
        let Some(usage_json) = row.get::<_, Option<String>>(1).map_err(crate::sql_error)? else {
            missing_usage = true;
            continue;
        };
        let harness = row.get::<_, String>(2).map_err(crate::sql_error)?;
        let usage = crate::decode_turn_usage(&usage_json, &harness)?;

        role_usage.observe_turn(manager, &usage)?;

        if let Some(context) = usage.first_call_input_tokens {
            let context = u64::try_from(context).map_err(|_| {
                crate::Error::new(
                    "source_integrity",
                    "negative first-call context token count",
                )
            })?;
            context_sum = context_sum
                .checked_add(u128::from(context))
                .ok_or_else(|| {
                    crate::Error::new("source_integrity", "autonomy context token sum overflow")
                })?;
            counts.observed_calls = counts.observed_calls.checked_add(1).ok_or_else(|| {
                crate::Error::new("source_integrity", "autonomy observed call count overflow")
            })?;
        } else {
            missing_context = true;
        }
    }

    if counts.observed_calls > 0 {
        counts.average_context_tokens = Some(context_sum as f64 / counts.observed_calls as f64);
    }
    let input = role_usage.input_summary();
    counts.manager_input_tokens = input.managers;
    counts.executor_input_tokens = input.executors;
    counts.manager_input_share = input.manager_share;

    let mut reasons = Vec::new();
    if input_overflow {
        reasons.push(format!(
            "turn scan exceeded {INPUT_CAP} rows. Usage covers the newest {INPUT_CAP}"
        ));
    }
    if missing_usage {
        reasons.push("some completed turns have no retained turn-usage record".to_owned());
    }
    if input.incomplete {
        reasons.push(
            "some retained turn-usage records omit uncached or cached input counters".to_owned(),
        );
    }
    if missing_context {
        reasons.push("some turn-usage records omit first-call context samples".to_owned());
    }
    if counts.observed_calls > 0 {
        reasons.push(
            "average context uses first-call samples. Later calls have no separate samples"
                .to_owned(),
        );
    }
    if unsettled_turn {
        reasons.push("unsettled turns are excluded because their usage is not final".to_owned());
    }
    if missing_agent {
        reasons.push("some retained turns lack current Agent hierarchy evidence".to_owned());
    }
    let source = "sqlite:turns.usage.normalized+agents.parent";
    if !input.observed
        && counts.observed_calls == 0
        && (missing_usage || input.incomplete || unsettled_turn || missing_agent)
    {
        return Ok(AutonomyRow::unavailable(source, &reasons.join(". ")));
    }
    Ok(row_with_coverage(source, reasons, counts))
}

fn row_with_coverage<T>(source: &str, reasons: Vec<String>, counts: T) -> AutonomyRow<T> {
    if reasons.is_empty() {
        AutonomyRow::complete(source, counts)
    } else {
        AutonomyRow::partial(source, &reasons.join(". "), counts)
    }
}
