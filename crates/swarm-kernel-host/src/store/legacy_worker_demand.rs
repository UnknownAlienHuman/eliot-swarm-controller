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

fn route_enabled(config: &Config, runtime: &str, artifact: &str) -> bool {
    config.routes.iter().any(|route| {
        route.enabled && route.runtime == runtime && route.module_artifact_id == artifact
    })
}

pub(super) fn snapshot(db: &Connection, config: &Config) -> Result<LegacyWorkerDemand> {
    let mut demand = LegacyWorkerDemand {
        checks: config.checks.enabled,
        opencode: config.routes.iter().any(|route| {
            route.enabled
                && crate::config::opencode_route_kind(&route.runtime, &route.module_artifact_id)
                    == Some(crate::config::OpenCodeRouteKind::Builtin)
        }),
        zed: route_enabled(
            config,
            crate::runtime::zed::RUNTIME,
            crate::runtime::zed::ARTIFACT_ID,
        ),
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
                AND json_extract(c.spec_json,'$.cancel_requested') IS NOT NULL)) ",
    )?;
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
         AND json_extract(route_json,'$.runtime')=?1 AND module_artifact_id IN (?2,?3))",
        params![
            crate::runtime::opencode_v2::RUNTIME,
            crate::runtime::opencode_v2::ARTIFACT_ID,
            crate::runtime::opencode_v2::TASK_PROMPT_ARTIFACT_ID
        ],
        |row| row.get::<_, bool>(0),
    )?;
    demand.zed |= db.query_row(
        "SELECT EXISTS(SELECT 1 FROM bindings WHERE released_at_ms IS NULL \
         AND json_extract(route_json,'$.runtime')=?1 AND module_artifact_id=?2)",
        params![
            crate::runtime::zed::RUNTIME,
            crate::runtime::zed::ARTIFACT_ID
        ],
        |row| row.get::<_, bool>(0),
    )?;

    demand.scheduler |= exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM meta WHERE \
           (key LIKE 'automation:v1:cron:active:%' OR key LIKE 'automation:v1:cron:held:%' \
            OR key LIKE 'goals:v1:due:%') \
           OR (key LIKE 'automation:v1:entry:%' \
               AND json_extract(value_json,'$.record.enabled')=1 \
               AND json_type(value_json,'$.record.cron')='object'))",
    )?;
    // The extracted process is opt-in and demanded only by its exact
    // read-only due projection. The legacy predicate above remains unchanged.
    demand.automation_scheduler = super::automation_scheduler::has_demand(db, config)?;
    demand.automation = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM meta WHERE \
           (key LIKE 'automation:v1:entry:%' \
            AND json_extract(value_json,'$.record.enabled')=1 \
            AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                       WHERE step.value IN ('work_dispatch','review_dispatch', \
                         'review_disposition','repair_dispatch','acceptance','publication', \
                         'check_run','goal_progression','script_run'))) \
           OR key LIKE 'coordination:watch:v1:active:%') \
         OR EXISTS(SELECT 1 FROM operations WHERE \
           state IN ('queued','sending','native_accepted','outcome_unknown') \
           AND json_type(effective_request_json,'$.automation_on_behalf')='object')",
    )?;

    demand.launcher = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='swarm.launch' \
           AND state IN ('queued','outcome_unknown') \
           AND json_extract(effective_request_json,'$.launch_manifest.state') \
             IN ('pending_workspace','awaiting_binding','awaiting_capability', \
                 'awaiting_participant_credential','outcome_unknown')) \
         OR EXISTS(SELECT 1 FROM workspace_leases WHERE state IN ('preparing','held','outcome_unknown')) \
         OR EXISTS(SELECT 1 FROM owned_service_starts \
                   WHERE state IN ('reserved','outcome_unknown','service_observed'))",
    )?;
    demand.native_mcp = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='swarm.launch' \
           AND state='queued' \
           AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_native_mcp' \
           AND (json_type(effective_request_json,'$.launch_manifest.native_mcp_readback.state') IS NULL \
             OR json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state') \
                IN ('retry_wait','observed_partial','reading'))) ",
    )?;
    demand.native_mcp_tools = exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='swarm.launch' \
           AND state='queued' \
           AND json_extract(effective_request_json,'$.launch_manifest.state')='awaiting_native_mcp' \
           AND json_extract(effective_request_json,'$.launch_manifest.runtime.dispatch_permitted')=0 \
           AND json_extract(effective_request_json,'$.launch_manifest.progress.task_dispatch')='not_started' \
           AND json_extract(effective_request_json,'$.launch_manifest.native_mcp_readback.state')='observed_partial')",
    )?;
    demand.forge |= exists(
        db,
        "SELECT EXISTS(SELECT 1 FROM operations WHERE method='forge.publish_ref' \
           AND state IN ('queued','sending','native_accepted','outcome_unknown'))",
    )?;

    Ok(demand)
}
