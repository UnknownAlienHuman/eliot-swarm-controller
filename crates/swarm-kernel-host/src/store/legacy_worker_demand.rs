//! Read-only activation predicates for the legacy host reconcilers.
//!
//! A descriptor/catalog row is never a demand signal. Route workers are
//! selected by the exact retained built-in runtime/artifact pair; the other
//! workers start only for their configured feature or an existing durable
//! obligation that their own reconciliation path understands.
use crate::{config::Config, error::Result};
use rusqlite::{Connection, params};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LegacyWorkerDemand {
    pub(crate) checks: bool,
    pub(crate) scripts: bool,
    pub(crate) opencode: bool,
    pub(crate) zed: bool,
    pub(crate) scheduler: bool,
    pub(crate) automation_scheduler: bool,
    pub(crate) automation: bool,
    pub(crate) launcher: bool,
    pub(crate) native_mcp: bool,
    pub(crate) native_mcp_tools: bool,
    pub(crate) forge: bool,
}

fn exists(db: &Connection, query: &str) -> Result<bool> {
    Ok(db.query_row(query, [], |row| row.get(0))?)
}

fn zed_route_enabled(config: &Config) -> bool {
    config.routes.iter().any(|route| {
        route.enabled
            && route.runtime == crate::runtime::zed::RUNTIME
            && matches!(
                route.module_artifact_id.as_str(),
                crate::runtime::zed::ARTIFACT_ID
                    | crate::runtime::zed::PREVIOUS_ARTIFACT_ID
                    | crate::runtime::zed::LEGACY_ARTIFACT_ID
            )
    })
}

fn unquarantined_launch_damage(db: &Connection) -> Result<bool> {
    let mut statement = db.prepare(
        "SELECT operation_id,effective_request_json FROM operations \
         WHERE method='swarm.launch' AND state IN ('queued','outcome_unknown') \
           AND NOT json_valid(effective_request_json) ORDER BY operation_id",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (operation_id, raw) = row?;
        match super::launcher_native_mcp::launch_request_is_quarantined(db, &operation_id, &raw) {
            Ok(true) => {}
            Ok(false) => return Ok(true),
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "AUTOMATION_RECORD_CORRUPT"
                        | "AUTOMATION_RECORD_INVALID"
                        | "AUTOMATION_RECORD_VERSION"
                        | "AUTOMATION_RECORD_TOO_LARGE"
                        | "AUTOMATION_QUARANTINE_RECORD_CORRUPT"
                        | "AUTOMATION_QUARANTINE_IDENTITY_CONFLICT"
                ) =>
            {
                return Ok(true);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

fn unquarantined_check_damage(db: &Connection) -> Result<bool> {
    let mut statement = db.prepare(
        "SELECT c.check_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id \
         WHERE c.state='queued' AND o.state='queued' AND NOT json_valid(c.spec_json) \
         ORDER BY c.check_id",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        match super::checks::pending_source_is_quarantined(db, &row?) {
            Ok(true) => {}
            Ok(false) => return Ok(true),
            // INVALID_PARAMS is the exact marker reader's JSON parse error.
            // Keep these subject failures with Checks; SQLite still propagates.
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "INVALID_PARAMS" | "CHECK_PENDING_QUARANTINE_CORRUPT"
                ) =>
            {
                return Ok(true);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}

fn unquarantined_automation_entry_damage(db: &Connection) -> Result<bool> {
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key LIKE 'automation:v1:entry:%' \
         AND NOT json_valid(value_json) ORDER BY key",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (key, raw) = row?;
        if automation_entry_damage_requires_activation(db, &key, &raw)? {
            return Ok(true);
        }
    }

    // The retained v1 producer seals a closed AutomationEntry schema. `scope`
    // is required and must be an object; detect this concrete valid-JSON
    // schema failure without parsing the entry ledger in the activation path.
    // Iterate every matching damaged row so an earlier exact quarantine does
    // not mask a later unquarantined obligation. Keep the same key range and
    // JSON validity guard; healthy entry payloads are not parsed in Rust.
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta \
         WHERE key >= ?1 AND key < ?2 \
           AND CASE WHEN json_valid(value_json) THEN \
             json_type(value_json,'$.schema_version')='integer' \
             AND json_extract(value_json,'$.schema_version')=1 \
             AND json_type(value_json,'$.record')='object' \
             AND json_type(value_json,'$.record.schema_version')='integer' \
             AND json_extract(value_json,'$.record.schema_version')=1 \
             AND json_type(value_json,'$.record.scope') IS NOT 'object' \
           ELSE 0 END \
         ORDER BY key",
    )?;
    let rows = statement.query_map(
        params!["automation:v1:entry:", "automation:v1:entry;"],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )?;
    for row in rows {
        let (key, raw) = row?;
        if automation_entry_damage_requires_activation(db, &key, &raw)? {
            return Ok(true);
        }
    }

    Ok(false)
}

fn automation_entry_damage_requires_activation(
    db: &Connection,
    key: &str,
    raw: &str,
) -> Result<bool> {
    match super::automation_publication::entry_quarantine_matches(db, key, raw) {
        Ok(true) => Ok(false),
        Ok(false) => Ok(true),
        Err(error)
            if matches!(
                error.code.as_str(),
                "AUTOMATION_RECORD_CORRUPT"
                    | "AUTOMATION_RECORD_INVALID"
                    | "AUTOMATION_RECORD_VERSION"
                    | "AUTOMATION_RECORD_TOO_LARGE"
                    | "AUTOMATION_QUARANTINE_RECORD_CORRUPT"
                    | "AUTOMATION_QUARANTINE_IDENTITY_CONFLICT"
            ) =>
        {
            // Keep retained subject/marker damage with its existing owner.
            // Actual SQLite and connection failures still propagate.
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn snapshot(db: &Connection, config: &Config) -> Result<LegacyWorkerDemand> {
    let mut demand = LegacyWorkerDemand {
        checks: config.checks.enabled,
        opencode: config.routes.iter().any(|route| {
            route.enabled
                && crate::config::opencode_route_kind(&route.runtime, &route.module_artifact_id)
                    == Some(crate::config::OpenCodeRouteKind::Builtin)
        }),
        zed: zed_route_enabled(config),
        scheduler: config.schedules.iter().any(|schedule| schedule.enabled),
        forge: config.forge.enabled,
        ..LegacyWorkerDemand::default()
    };

    demand.checks |= exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM check_runs AS c JOIN operations AS o \
         ON o.operation_id=c.operation_id \
         WHERE c.state IN ('running','reconciling') \
            OR (c.state='queued' AND o.state IN ('sending','outcome_unknown')) \
            OR (c.state='queued' AND o.state='queued' \
                AND CASE WHEN json_valid(c.spec_json) \
                    THEN json_extract(c.spec_json,'$.cancel_requested') IS NOT NULL \
                    ELSE 0 END)) ",
    )?;
    demand.checks |= unquarantined_check_damage(db)?;
    demand.scripts = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM script_runs \
         WHERE state IN ('queued','sending','running','reconciling','outcome_unknown'))",
    )?;

    // These exact predicates intentionally exclude the standalone
    // `eliot-opencode-v2.rust-http.1` descriptor. It is runtime `module` and is
    // owned by the isolated module supervisor, never this built-in worker.
    demand.opencode |= db.query_row(
        "SELECT EXISTS(SELECT 1 FROM bindings WHERE released_at_ms IS NULL \
         AND module_artifact_id IN (?2,?3) \
         AND CASE WHEN json_valid(route_json) THEN json_extract(route_json,'$.runtime')=?1 \
                  ELSE 1 END)",
        params![
            crate::runtime::opencode_v2::RUNTIME,
            crate::runtime::opencode_v2::ARTIFACT_ID,
            crate::runtime::opencode_v2::TASK_PROMPT_ARTIFACT_ID
        ],
        |row| row.get::<_, bool>(0),
    )?;
    demand.zed |= db.query_row(
        "SELECT EXISTS(SELECT 1 FROM bindings WHERE released_at_ms IS NULL \
         AND module_artifact_id IN (?2,?3,?4) \
         AND CASE WHEN json_valid(route_json) THEN json_extract(route_json,'$.runtime')=?1 \
                  ELSE 1 END)",
        params![
            crate::runtime::zed::RUNTIME,
            crate::runtime::zed::ARTIFACT_ID,
            crate::runtime::zed::PREVIOUS_ARTIFACT_ID,
            crate::runtime::zed::LEGACY_ARTIFACT_ID
        ],
        |row| row.get::<_, bool>(0),
    )?;

    demand.scheduler |= exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM meta WHERE \
           (key LIKE 'automation:v1:cron:active:%' OR key LIKE 'automation:v1:cron:held:%' \
            OR key LIKE 'goals:v1:due:%') \
           OR (key LIKE 'automation:v1:entry:%' \
               AND CASE WHEN json_valid(value_json) \
                   THEN json_extract(value_json,'$.record.enabled')=1 \
                    AND json_type(value_json,'$.record.cron')='object' \
                   ELSE 0 END))",
    )?;
    // The opt-in service owns outstanding RPCs for its configured lifetime;
    // exhausting its due page must not cancel its caller. Legacy demand is unchanged.
    demand.automation_scheduler = super::automation_scheduler::has_demand(config);
    demand.automation = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM meta WHERE \
           (key LIKE 'automation:v1:entry:%' \
            AND CASE WHEN json_valid(value_json) \
             THEN json_extract(value_json,'$.record.enabled')=1 \
              AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                       WHERE step.value IN ('work_dispatch','review_dispatch', \
                         'review_disposition','repair_dispatch','acceptance','publication', \
                         'check_run','goal_progression','script_run')) \
             ELSE 0 END) \
           OR key LIKE 'coordination:watch:v1:active:%') \
         OR EXISTS(SELECT 1 FROM operations WHERE \
           state IN ('queued','sending','native_accepted','outcome_unknown') \
           AND CASE WHEN json_valid(effective_request_json) \
               THEN json_type(effective_request_json,'$.automation_on_behalf')='object' \
               ELSE caller_id=?1 END)",
        [crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID],
        |row| row.get::<_, bool>(0),
    )?;
    demand.automation |= unquarantined_automation_entry_damage(db)?;

    demand.launcher = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='swarm.launch' \
           AND state IN ('queued','outcome_unknown') \
           AND CASE WHEN json_valid(effective_request_json) \
            THEN json_extract(effective_request_json,'$.launch_manifest.state') \
             IN ('pending_workspace','awaiting_binding','awaiting_capability', \
                 'awaiting_participant_credential','outcome_unknown') \
            ELSE 0 END) \
         OR EXISTS(SELECT 1 FROM workspace_leases WHERE state IN ('preparing','held','outcome_unknown')) \
         OR EXISTS(SELECT 1 FROM workspace_leases l JOIN attempts a ON a.attempt_id=l.attempt_id \
           WHERE l.state='stale' AND a.task_id=l.task_id AND a.task_revision=l.task_revision \
             AND a.owner_id=l.owner_client_id AND a.released_at_ms IS NOT NULL \
             AND a.state IN ('accepted','failed','cancelled','superseded') \
             AND CASE WHEN json_valid(l.clean_state_json) \
               THEN json_extract(l.clean_state_json,'$.status')='workspace_absence_verified' \
               ELSE 0 END) \
         OR EXISTS(SELECT 1 FROM owned_service_starts \
                   WHERE state IN ('reserved','outcome_unknown','service_observed'))",
    )?;
    // Invalid launch bytes still demand their existing bounded isolator.
    // An exact, validated quarantine removes only this unchanged obligation.
    demand.launcher |= unquarantined_launch_damage(db)?;
    demand.native_mcp = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='swarm.launch' \
           AND state='queued' \
           AND CASE WHEN json_valid(effective_request_json) \
            THEN json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_native_mcp' \
           AND (json_type(effective_request_json,'$.launch_manifest.native_mcp_readback.state') IS NULL \
             OR json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state') \
                IN ('retry_wait','observed_partial','reading')) ELSE 0 END) ",
    )?;
    demand.native_mcp_tools = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='swarm.launch' \
           AND state='queued' \
           AND CASE WHEN json_valid(effective_request_json) \
            THEN json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_native_mcp' \
           AND json_extract(effective_request_json,'$.launch_manifest.runtime.dispatch_permitted')=0 \
           AND json_extract(effective_request_json,'$.launch_manifest.progress.task_dispatch')='not_started' \
           AND json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state')='observed_partial' \
            ELSE 0 END)",
    )?;
    demand.forge |= exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='forge.publish_ref' \
           AND state IN ('queued','sending','native_accepted','outcome_unknown'))",
    )?;

    Ok(demand)
}
