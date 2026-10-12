//! Public Store consumers for optional-worker lifecycle projection isolation.
use super::*;
use serde_json::{Value, json};
use std::path::PathBuf;

async fn start_status_store(label: &str) -> (StoreOwner, PathBuf, Principal) {
    let (owner, directory, _) = start_store(label).await;
    let manager = principal("app138-status-manager", Role::Manager);
    let client_id = manager.client_id.clone();
    owner
        .store
        .run(move |db| {
            set_meta(
                db,
                &format!("client:{client_id}"),
                &json!({"role":"manager","disabled":false}),
            )?;
            Ok(())
        })
        .await
        .expect("register status manager");
    owner
        .store
        .record_host_start()
        .await
        .expect("record fixture host lifecycle");
    (owner, directory, manager)
}

fn assert_isolated_worker_status(host_status: &Value) {
    let lifecycle = &host_status["host_lifecycle"];
    assert_eq!(lifecycle["current"]["schema_version"], 1);
    assert!(
        lifecycle["current"]["host_epoch"]
            .as_i64()
            .unwrap_or_default()
            > 0
    );

    let malformed = &lifecycle["optional_workers"]["checks"];
    assert_eq!(malformed["state"], "unknown");
    assert_eq!(malformed["status"], "invalid");
    assert_eq!(malformed["error_code"], "HOST_LIFECYCLE_INVALID");
    assert_eq!(malformed["manager_action_required"], true);
    assert_eq!(malformed["retry_authorized"], false);
    assert_eq!(malformed.as_object().unwrap().len(), 5);

    let valid_peer = &lifecycle["optional_workers"]["scripts"];
    assert_eq!(valid_peer["state"], "running");
    assert_eq!(valid_peer["consecutive_failures"], 4);
    assert_eq!(valid_peer["updated_at_ms"], 17);
}

#[tokio::test]
async fn public_status_and_monitor_isolate_one_malformed_optional_worker() {
    let (owner, directory, manager) = start_status_store("app138-worker-isolation").await;
    let health = json!({
        "schema_version":1,
        "workers":{
            "checks":{
                "state":"corrupt-state",
                "consecutive_failures":0,
                "last_error_code":null,
                "retry_after_ms":null,
                "updated_at_ms":17,
                "last_failure":null,
                "restart_count":0,
                "child":null
            },
            "scripts":{
                "state":"running",
                "consecutive_failures":4,
                "last_error_code":null,
                "retry_after_ms":null,
                "updated_at_ms":17,
                "last_failure":null,
                "restart_count":1,
                "child":null
            }
        }
    });
    owner
        .store
        .run(move |db| set_meta(db, "host:optional-workers:v1", &health))
        .await
        .expect("seed one malformed and one valid worker receipt");

    let host_status = owner
        .store
        .call(manager.clone(), "host.status".into(), json!({}))
        .await
        .expect("public host.status preserves lifecycle and valid worker peer");
    assert_isolated_worker_status(&host_status);

    let snapshot = owner
        .store
        .call(manager, "monitor.snapshot".into(), json!({}))
        .await
        .expect("public monitor.snapshot preserves its host projection");
    assert!(snapshot["journal_cut"].is_object());
    assert_isolated_worker_status(&snapshot["state"]["host"]);

    owner.close().await.expect("close fixture Store");
    std::fs::remove_dir_all(directory).expect("remove fixture directory");
}

#[tokio::test]
async fn public_status_consumers_keep_store_database_errors_hard() {
    let (owner, directory, manager) = start_status_store("app138-store-error").await;
    owner
        .store
        .run(|db| {
            db.execute_batch("ALTER TABLE meta RENAME TO app138_saved_meta")?;
            Ok(())
        })
        .await
        .expect("temporarily remove public metadata table from this isolated fixture");

    let host_error = owner
        .store
        .call(manager.clone(), "host.status".into(), json!({}))
        .await
        .expect_err("host.status must propagate its metadata database error");
    assert_eq!(host_error.code, "STORE_ERROR");

    let monitor_error = owner
        .store
        .call(manager, "monitor.snapshot".into(), json!({}))
        .await
        .expect_err("monitor.snapshot must propagate its metadata database error");
    assert_eq!(monitor_error.code, "STORE_ERROR");

    owner
        .store
        .run(|db| {
            db.execute_batch("ALTER TABLE app138_saved_meta RENAME TO meta")?;
            Ok(())
        })
        .await
        .expect("restore fixture metadata table before Store shutdown");
    owner.close().await.expect("close fixture Store");
    std::fs::remove_dir_all(directory).expect("remove fixture directory");
}
