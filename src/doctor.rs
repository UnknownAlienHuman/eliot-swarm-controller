//! Read-only controller diagnostics behind `doctor.inspect`.
//!
//! Doctor reads facts that are already recorded — the Store, the resolved
//! configuration and the data directory's own files. It mutates nothing,
//! starts no process and calls no native runtime or model. Each finding names
//! a cause and the next addressed step; an unknown stays visible instead of
//! being hidden behind a green summary. Repairs belong to their own addressed
//! operations (module reconnect, `agent.reconcile`, check recovery): this
//! module performs none of them and never calls resume/respawn, changes
//! ACL/UAC/auth, or enables compaction (implementation plan §4/§11,
//! architecture §15).
//!
//! The report carries identities and counts, never credential material:
//! client rows are aggregated without token hashes, bindings expose only
//! whitelisted state scalars (never `route_json` or native evidence), check
//! profiles are named without executables or environment, and retained
//! incident details pass through the same redaction as native diagnostics.

use crate::{config::Config, error::Result, model};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use std::path::{Component, Path};

/// Same migration the Store embeds; the digest readback proves the opened
/// database matches the schema compiled into this binary.
const SCHEMA: &str = include_str!("../migrations/001_core.sql");
/// Artifact file existence is cross-checked for at most this many newest
/// records, so a diagnostic read stays bounded. `complete` reports coverage.
const ARTIFACT_FILE_CHECK_LIMIT: i64 = 1024;
const OPEN_INCIDENTS_LIMIT: i64 = 10;
const UNRELEASED_BINDINGS_LIMIT: i64 = 20;

/// Database/config report plus the artifact paths the caller must still
/// cross-check against the data directory (see `attach_filesystem`).
pub struct Inspection {
    pub report: Value,
    artifact_paths: Vec<String>,
    artifact_records: i64,
}

fn meta(db: &Connection, key: &str) -> Result<Option<Value>> {
    // The Store's meta reader is private to its module; this is the same
    // single-row lookup, kept read-only here.
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |r| {
            r.get(0)
        })
        .optional()?;
    raw.map(|s| serde_json::from_str(&s).map_err(Into::into))
        .transpose()
}

fn count(db: &Connection, sql: &str) -> Result<i64> {
    Ok(db.query_row(sql, [], |r| r.get(0))?)
}

fn counts_by(db: &Connection, table: &str, column: &str) -> Result<Value> {
    // Table/column names are fixed literals from this module, never input.
    let sql = format!("SELECT {column},count(*) FROM {table} GROUP BY {column} ORDER BY {column}");
    let mut statement = db.prepare(&sql)?;
    let rows = statement
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut map = serde_json::Map::new();
    for (key, n) in rows {
        map.insert(key, json!(n));
    }
    Ok(Value::Object(map))
}

fn finding(code: &str, severity: &str, summary: String, next_step: &str) -> Value {
    json!({"code":code,"severity":severity,"summary":summary,"next_step":next_step})
}

pub fn inspect(db: &Connection, config: &Config) -> Result<Inspection> {
    let now = model::now_ms()?;
    let mut findings: Vec<Value> = Vec::new();

    // --- schema and controller identity -----------------------------------
    let user_version: i64 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let application_id: i64 = db.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let journal_mode: String = db.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
    let synchronous: i64 = db.pragma_query_value(None, "synchronous", |r| r.get(0))?;
    let foreign_keys: i64 = db.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
    let recorded_digest = meta(db, "schema_digest")?;
    let expected_digest = model::digest(SCHEMA.as_bytes());
    let digest_matches = recorded_digest.as_ref() == Some(&json!(expected_digest));
    if !digest_matches {
        findings.push(finding(
            "SCHEMA_DIGEST_MISMATCH",
            "gap",
            "the recorded schema digest does not match the migration compiled into this binary"
                .into(),
            "Do not run mutations against this database; preserve the data directory and compare it with the binary that created it.",
        ));
    }
    let execution_mode = meta(db, "execution_mode")?;
    let new_work = execution_mode
        .as_ref()
        .and_then(|mode| mode["new_work"].as_str().map(str::to_owned));
    if new_work.as_deref() != Some("enabled") {
        findings.push(finding(
            "NEW_WORK_NOT_ENABLED",
            "info",
            format!(
                "new-work admission is {}",
                new_work.as_deref().unwrap_or("unrecorded")
            ),
            "Already admitted work is unaffected. An operator re-enables admission with host.mode (new_work=enabled).",
        ));
    }

    // --- operations ---------------------------------------------------------
    let operations_by_state = counts_by(db, "operations", "state")?;
    let outcome_unknown = operations_by_state["outcome_unknown"].as_i64().unwrap_or(0);
    if outcome_unknown > 0 {
        findings.push(finding(
            "OUTCOME_UNKNOWN_OPERATIONS",
            "attention",
            format!("{outcome_unknown} operation(s) were sent without a known native outcome"),
            "Reconcile each one against its binding with agent.reconcile; never resend the original command blindly.",
        ));
    }
    let overdue_queued: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE state='queued' AND due_at_ms<?1",
        [now],
        |r| r.get(0),
    )?;
    if overdue_queued > 0 {
        findings.push(finding(
            "QUEUED_OPERATIONS_OVERDUE",
            "info",
            format!("{overdue_queued} queued operation(s) are past due and remain durable"),
            "They stay queued until the dispatcher or the owning module picks them up; inspect them with operation.get/operation.list.",
        ));
    }

    // --- bindings and modules ------------------------------------------------
    let bindings_by_state = counts_by(db, "bindings", "state")?;
    let mut statement = db.prepare(
        "SELECT binding_id,generation,lane_id,module_artifact_id,state,json_extract(state_json,'$.connection'),created_at_ms FROM bindings WHERE released_at_ms IS NULL ORDER BY created_at_ms,binding_id,generation LIMIT ?1",
    )?;
    let unreleased = statement
        .query_map([UNRELEASED_BINDINGS_LIMIT], |r| {
            Ok(json!({
                "binding_id": r.get::<_, String>(0)?,
                "generation": r.get::<_, i64>(1)?,
                "lane_id": r.get::<_, String>(2)?,
                "module_artifact_id": r.get::<_, String>(3)?,
                "state": r.get::<_, String>(4)?,
                "connection": r.get::<_, Option<String>>(5)?,
                "age_ms": now - r.get::<_, i64>(6)?,
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let unreleased_total: i64 = db.query_row(
        "SELECT count(*) FROM bindings WHERE released_at_ms IS NULL",
        [],
        |r| r.get(0),
    )?;
    for (state, code, step) in [
        (
            "reconciling",
            "BINDINGS_RECONCILING",
            "Reconnect the module read path or reconcile the named binding; doctor performs no resume or respawn and the binding's work must not be blindly reassigned.",
        ),
        (
            "opening",
            "BINDINGS_OPENING",
            "Inspect the exact binding with agent.state; opening completes only through module hello/adapter evidence, not through a doctor action.",
        ),
    ] {
        let n = unreleased.iter().filter(|b| b["state"] == state).count();
        if n > 0 {
            findings.push(finding(
                code,
                "attention",
                format!("{n} unreleased binding(s) shown are in state '{state}'"),
                step,
            ));
        }
    }
    let mut statement = db.prepare(
        "SELECT module_artifact_id,count(*),coalesce(sum(state='ready'),0),coalesce(sum(json_extract(state_json,'$.connection')='connected'),0),coalesce(sum(released_at_ms IS NULL),0) FROM bindings GROUP BY module_artifact_id ORDER BY module_artifact_id",
    )?;
    let modules = statement
        .query_map([], |r| {
            Ok(json!({
                "module_artifact_id": r.get::<_, String>(0)?,
                "bindings": r.get::<_, i64>(1)?,
                "ready": r.get::<_, i64>(2)?,
                "connected": r.get::<_, i64>(3)?,
                "unreleased": r.get::<_, i64>(4)?,
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // --- attempts ------------------------------------------------------------
    let attempts_by_state = counts_by(db, "attempts", "state")?;
    let recovery_pending = attempts_by_state["recovery_pending"].as_i64().unwrap_or(0);
    if recovery_pending > 0 {
        findings.push(finding(
            "ATTEMPTS_RECOVERY_PENDING",
            "attention",
            format!("{recovery_pending} attempt(s) are marked recovery_pending"),
            "Reconcile the attempt's recorded native family in an addressed way; recovery_pending is a need for verification, not proof of death or permission to reassign.",
        ));
    }
    let unreleased_attempts: i64 = db.query_row(
        "SELECT count(*) FROM attempts WHERE released_at_ms IS NULL",
        [],
        |r| r.get(0),
    )?;

    // --- checks ---------------------------------------------------------------
    let checks_by_state = counts_by(db, "check_runs", "state")?;
    let checks_reconciling = checks_by_state["reconciling"].as_i64().unwrap_or(0);
    if checks_reconciling > 0 {
        findings.push(finding(
            "CHECKS_RECONCILING",
            "attention",
            format!("{checks_reconciling} check run(s) are reconciling after a worker interruption"),
            "Check recovery verifies the recorded worker identity and process-group disposition; the command is not replayed and no verdict is guessed.",
        ));
    }
    let held_resources: i64 = db.query_row(
        "SELECT count(*) FROM check_runs WHERE resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL",
        [],
        |r| r.get(0),
    )?;
    let held_by_terminal: i64 = db.query_row(
        "SELECT count(*) FROM check_runs WHERE resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL AND state IN ('failed','error','incomplete','cancelled')",
        [],
        |r| r.get(0),
    )?;
    if held_by_terminal > 0 {
        findings.push(finding(
            "CHECK_RESOURCES_HELD",
            "info",
            format!("{held_by_terminal} finished check run(s) still hold their target resource"),
            "A resource is released only on proven disposition of its own job; doctor does not release it and other resources are unaffected.",
        ));
    }

    // --- incidents -------------------------------------------------------------
    let mut statement = db.prepare(
        "SELECT incident_id,dedup_key,occurrences,details_json,opened_at_ms,last_seen_at_ms FROM incidents WHERE state='open' ORDER BY last_seen_at_ms DESC,incident_id LIMIT ?1",
    )?;
    let open_incidents = statement
        .query_map([OPEN_INCIDENTS_LIMIT], |r| {
            let details: Value =
                serde_json::from_str(&r.get::<_, String>(3)?).unwrap_or(Value::Null);
            Ok(json!({
                "incident_id": r.get::<_, String>(0)?,
                "dedup_key": r.get::<_, String>(1)?,
                "occurrences": r.get::<_, i64>(2)?,
                "details": crate::redaction::value(details),
                "opened_at_ms": r.get::<_, i64>(4)?,
                "last_seen_at_ms": r.get::<_, i64>(5)?,
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let open_incident_count: i64 = db.query_row(
        "SELECT count(*) FROM incidents WHERE state='open'",
        [],
        |r| r.get(0),
    )?;
    if open_incident_count > 0 {
        findings.push(finding(
            "OPEN_INCIDENTS",
            "attention",
            format!("{open_incident_count} incident(s) are open; repeats update the same incident instead of creating new work"),
            "Work from the incident's dedup_key and retained evidence; doctor records no resolution and starts no repair task on its own.",
        ));
    }

    // --- clients (aggregates only; token hashes never leave the Store) --------
    let mut statement = db.prepare("SELECT value_json FROM meta WHERE key LIKE 'client:%'")?;
    let client_rows = statement
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut roles = serde_json::Map::new();
    let mut disabled_clients: i64 = 0;
    for raw in &client_rows {
        let value: Value = serde_json::from_str(raw)?;
        let role = value["role"].as_str().unwrap_or("unknown").to_string();
        let entry = roles.entry(role).or_insert_with(|| json!(0));
        *entry = json!(entry.as_i64().unwrap_or(0) + 1);
        if value["disabled"] == true {
            disabled_clients += 1;
        }
    }

    // --- artifacts (database side; file presence is attached by the caller) ---
    let artifact_records: i64 = count(db, "SELECT count(*) FROM artifacts")?;
    let artifact_bytes: i64 = db.query_row(
        "SELECT coalesce(sum(byte_length),0) FROM artifacts",
        [],
        |r| r.get(0),
    )?;
    let mut statement = db.prepare(
        "SELECT relative_path FROM artifacts ORDER BY created_at_ms DESC,artifact_id LIMIT ?1",
    )?;
    let artifact_paths = statement
        .query_map([ARTIFACT_FILE_CHECK_LIMIT], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    let tables = json!({
        "tasks": count(db, "SELECT count(*) FROM tasks")?,
        "attempts": count(db, "SELECT count(*) FROM attempts")?,
        "bindings": count(db, "SELECT count(*) FROM bindings")?,
        "operations": count(db, "SELECT count(*) FROM operations")?,
        "observations": count(db, "SELECT count(*) FROM observations")?,
        "artifacts": artifact_records,
        "check_runs": count(db, "SELECT count(*) FROM check_runs")?,
        "incidents": count(db, "SELECT count(*) FROM incidents")?,
    });

    let report = json!({
        "method": "doctor.inspect",
        "version": env!("CARGO_PKG_VERSION"),
        "read_only": true,
        "generated_at_ms": now,
        "controller": {
            "controller_id": meta(db, "controller_id")?,
            "host_epoch": meta(db, "host_epoch")?,
        },
        "schema": {
            "user_version": user_version,
            "application_id": application_id,
            "digest_matches_compiled_migration": digest_matches,
            "sqlite": rusqlite::version(),
            "journal_mode": journal_mode,
            "synchronous": synchronous,
            "foreign_keys": foreign_keys,
        },
        "admission": {"new_work": new_work},
        "storage": {"tables": tables, "data_dir": null},
        "operations": {
            "by_state": operations_by_state,
            "overdue_queued": overdue_queued,
        },
        "bindings": {
            "by_state": bindings_by_state,
            "unreleased_total": unreleased_total,
            "unreleased": unreleased,
        },
        "modules": modules,
        "attempts": {
            "by_state": attempts_by_state,
            "unreleased": unreleased_attempts,
        },
        "checks": {
            "by_state": checks_by_state,
            "resources_held": held_resources,
        },
        "incidents": {
            "open": open_incident_count,
            "items": open_incidents,
            "items_truncated": open_incident_count > OPEN_INCIDENTS_LIMIT,
        },
        "clients": {
            "registered": client_rows.len() as i64,
            "by_role": Value::Object(roles),
            "disabled": disabled_clients,
        },
        "artifacts": {
            "records": artifact_records,
            "total_bytes": artifact_bytes,
            "files": null,
        },
        "routes": config
            .routes
            .iter()
            .map(|r| json!({
                "alias": r.alias,
                "runtime": r.runtime,
                "module_artifact_id": r.module_artifact_id,
                "enabled": r.enabled,
            }))
            .collect::<Vec<_>>(),
        "live_qualification": false,
        "config": {
            "schema_version": config.schema_version,
            "queue_capacity": config.storage.queue_capacity,
            "ipc": {
                "max_connections": config.ipc.max_connections,
                "max_inflight_per_connection": config.ipc.max_inflight_per_connection,
                "max_frame_bytes": config.ipc.max_frame_bytes,
                "write_timeout_seconds": config.ipc.write_timeout_seconds,
            },
            "checks": {
                "enabled": config.checks.enabled,
                "max_running": config.checks.max_running,
                "cache_reuse": false,
                "profiles": config
                    .checks
                    .profiles
                    .iter()
                    .map(|p| json!({"profile_id": p.profile_id, "profile_revision": p.profile_revision}))
                    .collect::<Vec<_>>(),
            },
        },
        "known_gaps": [
            {
                "area": "forge_publication",
                "status": "not_implemented",
                "note": "Git/GitHub publication is not performed or recorded by this controller yet, so there are no publication facts to audit.",
            },
            {
                "area": "doctor_repair",
                "status": "report_only",
                "note": "doctor.inspect performs no repair, resume, respawn, ACL/UAC/auth or configuration change; safe recovery stays with the addressed owning operations.",
            },
            {
                "area": "native_live_qualification",
                "status": "not_performed",
                "note": "Fixture and CI evidence is not a live vendor run; routes therefore report live_qualification=false.",
            },
        ],
        "findings": findings,
    });
    Ok(Inspection {
        report,
        artifact_paths,
        artifact_records,
    })
}

/// Fill in the filesystem side of the report: data-directory facts and the
/// artifact-file cross-check. Pure metadata reads — no file contents, no
/// writes. Recorded paths that would escape the data directory are counted
/// as invalid instead of being followed.
pub fn attach_filesystem(inspection: &mut Inspection, data_dir: &Path) {
    let file_fact = |path: &Path| -> Value {
        match std::fs::metadata(path) {
            Ok(meta) if meta.is_file() => json!({"present": true, "bytes": meta.len()}),
            Ok(_) => json!({"present": false, "bytes": null}),
            Err(_) => json!({"present": false, "bytes": null}),
        }
    };
    let mut checked: i64 = 0;
    let mut missing: i64 = 0;
    let mut invalid_paths: i64 = 0;
    for relative in &inspection.artifact_paths {
        let path = Path::new(relative);
        if path.is_absolute()
            || path.components().any(|c| {
                matches!(
                    c,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            invalid_paths += 1;
            continue;
        }
        checked += 1;
        if !data_dir.join(path).is_file() {
            missing += 1;
        }
    }
    let complete = inspection.artifact_records <= checked + invalid_paths;
    inspection.report["storage"]["data_dir"] = json!({
        "path": data_dir.display().to_string(),
        "database": file_fact(&data_dir.join("swarm.db")),
        "wal": file_fact(&data_dir.join("swarm.db-wal")),
        "artifacts_dir_present": data_dir.join("artifacts").is_dir(),
    });
    inspection.report["artifacts"]["files"] = json!({
        "checked": checked,
        "missing": missing,
        "invalid_recorded_paths": invalid_paths,
        "complete": complete,
    });
    if (missing > 0 || invalid_paths > 0)
        && let Some(findings) = inspection.report["findings"].as_array_mut()
    {
        findings.push(finding(
            "ARTIFACT_FILES_MISSING",
            "gap",
            format!(
                "{missing} recorded artifact file(s) are missing and {invalid_paths} recorded path(s) are not usable",
            ),
            "Treat dependent submissions and checks as evidence gaps, never as passed; restore the data directory from backup instead of fabricating contents.",
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;

    fn fixture_db() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(SCHEMA).unwrap();
        let digest = model::digest(SCHEMA.as_bytes());
        db.execute(
            "INSERT INTO meta(key,value_json) VALUES('schema_digest',?1),('controller_id','\"ctl-1\"'),('host_epoch','3'),('execution_mode','{\"new_work\":\"enabled\"}')",
            params![serde_json::to_string(&digest).unwrap()],
        )
        .unwrap();
        db.execute(
            "INSERT INTO meta(key,value_json) VALUES('client:operator-1','{\"role\":\"operator\",\"token_hash\":\"deadbeef\",\"disabled\":false}')",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) VALUES('b1',1,'lane-1','inst-1','muse-sdk-test','reconciling','{}','{\"connection\":\"disconnected\"}',1000)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES('op1','operator-1','req-1','agent.send','{}','{}','outcome_unknown',1000,1000,1000)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('t1','p1',1,'open','{}',1000,1000)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,created_at_ms,updated_at_ms) VALUES('a1','t1',1,'{}','operator-1','native_manager','recovery_pending',1000,1000)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO incidents(incident_id,dedup_key,state,occurrences,details_json,opened_at_ms,last_seen_at_ms) VALUES('i1','check:c1:failure','open',2,'{\"error\":\"boom\"}',1000,2000)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,created_at_ms) VALUES('art1','artifacts/art1.bin','result',3,1000)",
            [],
        )
        .unwrap();
        db
    }

    #[test]
    fn inspect_reports_recorded_facts_and_findings_without_secrets() {
        let db = fixture_db();
        let inspection = inspect(&db, &Config::default()).unwrap();
        let report = &inspection.report;
        assert_eq!(report["read_only"], true);
        assert_eq!(report["schema"]["digest_matches_compiled_migration"], true);
        assert_eq!(report["controller"]["host_epoch"], 3);
        assert_eq!(report["bindings"]["unreleased_total"], 1);
        assert_eq!(
            report["bindings"]["unreleased"][0]["connection"],
            "disconnected"
        );
        assert_eq!(report["modules"][0]["module_artifact_id"], "muse-sdk-test");
        assert_eq!(report["incidents"]["open"], 1);
        assert_eq!(report["clients"]["registered"], 1);
        let codes: Vec<&str> = report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["code"].as_str().unwrap())
            .collect();
        assert!(codes.contains(&"BINDINGS_RECONCILING"), "{codes:?}");
        assert!(codes.contains(&"OUTCOME_UNKNOWN_OPERATIONS"), "{codes:?}");
        assert!(codes.contains(&"ATTEMPTS_RECOVERY_PENDING"), "{codes:?}");
        assert!(codes.contains(&"OPEN_INCIDENTS"), "{codes:?}");
        let rendered = report.to_string();
        assert!(
            !rendered.contains("deadbeef"),
            "token hash leaked: {rendered}"
        );
        assert!(
            !rendered.contains("token_hash"),
            "hash field leaked: {rendered}"
        );
    }

    #[test]
    fn filesystem_facts_count_missing_artifact_files() {
        let db = fixture_db();
        let mut inspection = inspect(&db, &Config::default()).unwrap();
        let dir = std::env::temp_dir().join(format!("swarm-doctor-{}", model::new_id()));
        std::fs::create_dir_all(dir.join("artifacts")).unwrap();
        attach_filesystem(&mut inspection, &dir);
        let files = &inspection.report["artifacts"]["files"];
        assert_eq!(files["checked"], 1);
        assert_eq!(files["missing"], 1);
        assert_eq!(files["complete"], true);
        assert_eq!(
            inspection.report["storage"]["data_dir"]["database"]["present"],
            false
        );
        let codes: Vec<&str> = inspection.report["findings"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["code"].as_str().unwrap())
            .collect();
        assert!(codes.contains(&"ARTIFACT_FILES_MISSING"), "{codes:?}");
        std::fs::write(dir.join("artifacts/art1.bin"), b"abc").unwrap();
        let mut inspection = inspect(&db, &Config::default()).unwrap();
        attach_filesystem(&mut inspection, &dir);
        assert_eq!(inspection.report["artifacts"]["files"]["missing"], 0);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
