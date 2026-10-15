use super::{seed_clients, seed_subject, start_store_with_config};
use crate::{config::Config, model, policy::OWNER_POLICY_V2_ID, store::Store};
use rusqlite::params;
use serde_json::json;

fn quiet_config() -> Config {
    let mut config = Config::default();
    config.checks.enabled = false;
    config.routes.clear();
    config.schedules.clear();
    config.forge.enabled = false;
    config.automation_scheduler.enabled = false;
    config
}

async fn insert_poison_launch(store: &Store, operation_id: &str, raw: &str) {
    let operation_id = operation_id.to_owned();
    let raw = raw.to_owned();
    store.run(move |db| {
        db.pragma_update(None, "ignore_check_constraints", true)?;
        let result = db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,'review-owner-v2',?1,'swarm.launch','{}',?2,'queued',1,1,1)",
            params![operation_id, raw],
        );
        db.pragma_update(None, "ignore_check_constraints", false)?;
        result?;
        Ok(())
    }).await.unwrap();
}

#[tokio::test]
async fn legacy_demand_poison_reaches_existing_check_and_launch_isolators() {
    let (owner, directory, _) =
        start_store_with_config("legacy-demand-poison", quiet_config()).await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "legacy-demand-poison",
        "review-owner-v2",
        OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let check_id = model::new_id();
    let retained_check_id = check_id.clone();
    owner.store.run(move |db| {
        let operation_id = model::new_id();
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,'review-owner-v2',?1,'check.run','{}','{}',?2,?3,'queued',1,1,1)",
            params![operation_id, subject.task_id, subject.attempt_id],
        )?;
        db.pragma_update(None, "ignore_check_constraints", true)?;
        let inserted = db.execute(
            "INSERT INTO check_runs(check_id,operation_id,attempt_id,candidate_ref,cache_key,resource_key,spec_json,state,created_at_ms) \
             VALUES(?1,?2,?3,?4,'poison-cache','poison-resource','{broken-check-spec','queued',1)",
            params![check_id, operation_id, subject.attempt_id, subject.candidate_ref],
        );
        db.pragma_update(None, "ignore_check_constraints", false)?;
        inserted?;
        Ok(())
    }).await.unwrap();
    insert_poison_launch(&owner.store, "legacy-poison-launch", "{broken-launch").await;
    let demand = owner.store.legacy_worker_demand_snapshot().await.unwrap();
    assert!(demand.checks);
    assert!(demand.launcher);
    assert!(!demand.native_mcp && !demand.native_mcp_tools);
    owner.store.reconcile_checks_once().await.unwrap();
    let launch = owner.store.reconcile_launches_once().await.unwrap();
    assert_eq!(launch["examined"], 0);
    owner.store.run(move |db| {
        let quarantined: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
            [format!("checks:pending:v1:quarantine:{}", model::digest(retained_check_id.as_bytes()))],
            |row| row.get(0),
        )?;
        assert!(quarantined);
        let retained: String = db.query_row("SELECT effective_request_json FROM operations WHERE operation_id='legacy-poison-launch'", [], |row| row.get(0))?;
        assert_eq!(retained, "{broken-launch");
        Ok(())
    }).await.unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn legacy_demand_quarantined_launch_is_stable_and_changed_bytes_reactivate() {
    let (owner, directory, _) =
        start_store_with_config("legacy-demand-stability", quiet_config()).await;
    insert_poison_launch(&owner.store, "stable-poison-launch", "{first-damage").await;
    assert!(
        owner
            .store
            .legacy_worker_demand_snapshot()
            .await
            .unwrap()
            .launcher
    );
    owner.store.reconcile_launches_once().await.unwrap();
    let writes = owner.store.run(|db| Ok(db.total_changes())).await.unwrap();
    for _ in 0..10 {
        assert!(
            !owner
                .store
                .legacy_worker_demand_snapshot()
                .await
                .unwrap()
                .launcher
        );
        owner.store.reconcile_launches_once().await.unwrap();
    }
    assert_eq!(
        owner.store.run(|db| Ok(db.total_changes())).await.unwrap(),
        writes
    );
    owner.store.run(|db| {
        db.pragma_update(None, "ignore_check_constraints", true)?;
        let changed = db.execute("UPDATE operations SET effective_request_json='{second-damage' WHERE operation_id='stable-poison-launch'", []);
        db.pragma_update(None, "ignore_check_constraints", false)?;
        changed?;
        Ok(())
    }).await.unwrap();
    assert!(
        owner
            .store
            .legacy_worker_demand_snapshot()
            .await
            .unwrap()
            .launcher
    );
    owner.store.reconcile_launches_once().await.unwrap();
    assert!(
        !owner
            .store
            .legacy_worker_demand_snapshot()
            .await
            .unwrap()
            .launcher
    );
    owner
        .store
        .run(|db| {
            let count: i64 = db.query_row(
                "SELECT count(*) FROM meta WHERE key LIKE 'launcher:v1:quarantine:native_mcp:%'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 2);
            Ok(())
        })
        .await
        .unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn legacy_demand_corrupt_entry_and_route_activate_only_existing_owners() {
    let (owner, directory, _) =
        start_store_with_config("legacy-demand-entry", quiet_config()).await;
    owner.store.run(|db| {
        let key = crate::automation::config::entry_key("review-owner-v2", "fixture", "poison-entry")?;
        db.pragma_update(None, "ignore_check_constraints", true)?;
        let entry = db.execute("INSERT INTO meta(key,value_json) VALUES(?1,'{broken-entry')", [key]);
        let binding = db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
             VALUES('poison-opencode-binding',1,'lane','instance',?1,'ready','{broken-route','{}',1)",
            [crate::runtime::opencode_v2::ARTIFACT_ID],
        );
        db.pragma_update(None, "ignore_check_constraints", false)?;
        entry?;
        binding?;
        Ok(())
    }).await.unwrap();
    let demand = owner.store.legacy_worker_demand_snapshot().await.unwrap();
    assert!(demand.automation && demand.opencode);
    assert!(!demand.scheduler && !demand.launcher && !demand.zed);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn legacy_demand_valid_json_damaged_entry_activates_and_quarantine_stays_stable() {
    let (owner, directory, _) =
        start_store_with_config("legacy-demand-json-schema", quiet_config()).await;
    let key = crate::automation::config::entry_key("review-owner-v2", "fixture", "damaged-scope")
        .unwrap();
    let source_digest = owner
        .store
        .run(move |db| {
            let mut record = crate::automation::config::AutomationEntry::new(
                "review-owner-v2",
                "fixture",
                "damaged-scope",
                1,
            )
            .value()?;
            record["enabled"] = json!(true);
            record["steps"] = json!([]);
            record["scope"] = serde_json::Value::Null;
            crate::automation::config::write_record(db, &key, &record)?;

            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            assert!(serde_json::from_str::<serde_json::Value>(&raw).is_ok());
            let error = crate::store::automation_reconcile::parse_automation_entry(
                &raw,
                "automation entry",
            )
            .unwrap_err();
            assert_eq!(error.code, "AUTOMATION_RECORD_INVALID");
            Ok(model::digest(raw.as_bytes()))
        })
        .await
        .unwrap();

    let demand = owner.store.legacy_worker_demand_snapshot().await.unwrap();
    assert!(demand.automation);
    assert!(!demand.scheduler);

    // The v1 `scope: null` predicate activates Automation for this schema
    // damage; the empty step list only leaves ordinary demand absent.
    // The existing bounded publication owner quarantines the exact entry.
    owner.store.reconcile_automations_once().await.unwrap();
    let digest_for_lookup = source_digest.clone();
    let marker_before = owner
        .store
        .run(move |db| {
            let marker: String = db.query_row(
                "SELECT value_json FROM meta \
                 WHERE key LIKE 'automation:v1:publication:quarantine:%' \
                   AND json_extract(value_json,'$.record.evidence.source_digest')=?1 \
                 ORDER BY key LIMIT 1",
                [digest_for_lookup],
                |row| row.get(0),
            )?;
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&marker)?["record"]["code"],
                "AUTOMATION_RECORD_INVALID"
            );
            Ok(marker)
        })
        .await
        .unwrap();

    for _ in 0..10 {
        let demand = owner.store.legacy_worker_demand_snapshot().await.unwrap();
        assert!(
            !demand.automation,
            "unchanged exact quarantine must not reactivate Automation"
        );
    }
    let marker_after = owner
        .store
        .run(move |db| {
            let marker: String = db.query_row(
                "SELECT value_json FROM meta \
                 WHERE key LIKE 'automation:v1:publication:quarantine:%' \
                   AND json_extract(value_json,'$.record.evidence.source_digest')=?1 \
                 ORDER BY key LIMIT 1",
                [source_digest],
                |row| row.get(0),
            )?;
            Ok(marker)
        })
        .await
        .unwrap();
    assert_eq!(marker_after, marker_before);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn insert_scope_null_v1_automation_entry(
    store: &Store,
    automation_id: &str,
) -> (String, String) {
    let owner_id = "review-owner-v2";
    let project_id = "fixture";
    let key = crate::automation::config::entry_key(owner_id, project_id, automation_id).unwrap();
    let retained_key = key.clone();
    let automation_id = automation_id.to_owned();
    let source_digest = store
        .run(move |db| {
            let mut record = crate::automation::config::AutomationEntry::new(
                owner_id,
                project_id,
                &automation_id,
                1,
            )
            .value()?;
            record["enabled"] = json!(true);
            record["steps"] = json!([]);
            record["scope"] = serde_json::Value::Null;
            crate::automation::config::write_record(db, &retained_key, &record)?;

            let raw: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [&retained_key],
                |row| row.get(0),
            )?;
            let sealed: serde_json::Value = serde_json::from_str(&raw)?;
            assert_eq!(sealed["schema_version"], json!(1));
            assert_eq!(sealed["record"]["schema_version"], json!(1));
            assert_eq!(sealed["record"]["scope"], serde_json::Value::Null);
            assert_eq!(sealed["record"]["steps"], json!([]));
            assert_eq!(sealed["record"]["enabled"], json!(true));
            let error = crate::store::automation_reconcile::parse_automation_entry(
                &raw,
                "automation entry",
            )
            .unwrap_err();
            assert_eq!(error.code, "AUTOMATION_RECORD_INVALID");
            Ok(model::digest(raw.as_bytes()))
        })
        .await
        .unwrap();
    (key, source_digest)
}

async fn scope_null_entry_is_exactly_quarantined(store: &Store, key: &str) -> bool {
    let key = key.to_owned();
    store
        .run(move |db| {
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            crate::store::automation_publication::entry_quarantine_matches(db, &key, &raw)
        })
        .await
        .unwrap()
}

async fn publication_entry_markers(store: &Store, digests: [String; 2]) -> Vec<(String, String)> {
    store
        .run(move |db| {
            let [first_digest, second_digest] = digests;
            let mut statement = db.prepare(
                "SELECT key,value_json FROM meta \
                 WHERE key LIKE 'automation:v1:publication:quarantine:%' \
                   AND json_extract(value_json,'$.record.evidence.source_digest') IN (?1,?2) \
                 ORDER BY key",
            )?;
            let rows = statement
                .query_map([first_digest, second_digest], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn legacy_demand_valid_schema_damage_checks_past_exact_quarantine() {
    let (owner, directory, _) =
        start_store_with_config("legacy-demand-schema-quarantine-order", quiet_config()).await;
    let (first_key, first_digest) =
        insert_scope_null_v1_automation_entry(&owner.store, "damaged-first").await;
    assert!(
        owner
            .store
            .legacy_worker_demand_snapshot()
            .await
            .unwrap()
            .automation
    );

    owner.store.reconcile_automations_once().await.unwrap();
    assert!(scope_null_entry_is_exactly_quarantined(&owner.store, &first_key).await);

    let (second_key, second_digest) =
        insert_scope_null_v1_automation_entry(&owner.store, "damaged-later").await;
    assert!(first_key < second_key, "the quarantined row sorts first");
    assert!(!scope_null_entry_is_exactly_quarantined(&owner.store, &second_key).await);

    let demand = owner.store.legacy_worker_demand_snapshot().await.unwrap();
    assert!(
        demand.automation,
        "record.scope=null is the v1 schema predicate; empty steps only suppress ordinary demand"
    );

    owner.store.reconcile_automations_once().await.unwrap();
    assert!(scope_null_entry_is_exactly_quarantined(&owner.store, &first_key).await);
    assert!(scope_null_entry_is_exactly_quarantined(&owner.store, &second_key).await);
    let digests = [first_digest, second_digest];
    let markers_before = publication_entry_markers(&owner.store, digests.clone()).await;
    assert_eq!(markers_before.len(), 2);
    let writes_before_observations = owner.store.run(|db| Ok(db.total_changes())).await.unwrap();
    for _ in 0..10 {
        assert!(
            !owner
                .store
                .legacy_worker_demand_snapshot()
                .await
                .unwrap()
                .automation,
            "unchanged exact quarantines must leave Automation dormant"
        );
    }
    let markers_after = publication_entry_markers(&owner.store, digests).await;
    assert_eq!(markers_after, markers_before);
    assert_eq!(
        owner.store.run(|db| Ok(db.total_changes())).await.unwrap(),
        writes_before_observations
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn legacy_demand_valid_bits_and_actual_schema_failure_remain_distinct() {
    let (owner, directory, _) =
        start_store_with_config("legacy-demand-valid", quiet_config()).await;
    let demand = owner.store.legacy_worker_demand_snapshot().await.unwrap();
    assert!(!demand.checks && !demand.automation && !demand.launcher && !demand.native_mcp);
    owner
        .store
        .run(|db| {
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES('automation:v1:entry:valid',?1)",
                [model::canonical(
                    &json!({"record":{"enabled":true,"steps":["review_dispatch"],"cron":{}}}),
                )?],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let demand = owner.store.legacy_worker_demand_snapshot().await.unwrap();
    assert!(demand.automation && demand.scheduler);
    owner
        .store
        .run(|db| {
            db.execute_batch("CREATE TEMP TABLE check_runs(broken TEXT);")?;
            let error =
                crate::store::legacy_worker_demand::snapshot(db, &quiet_config()).unwrap_err();
            db.execute_batch("DROP TABLE temp.check_runs;")?;
            assert_eq!(error.code, "STORE_ERROR");
            Ok(())
        })
        .await
        .unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
