use super::*;
use crate::{
    config::{Config, Route},
    error::Result,
    platform::{DataRoot, bootstrap_credential},
    runtime::opencode_v2::{self as oc, tests::Fixture},
    store::StoreOwner,
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;

async fn start(fixture: &Fixture) -> (StoreOwner, Principal) {
    let directory = fixture.dir.join("controller");
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory;
    config.routes.push(Route {
        admission_policy: Default::default(),
        workspace_option: None,
        owned_service: None,
        alias: "fixture".into(),
        runtime: oc::RUNTIME.into(),
        module_artifact_id: oc::TASK_PROMPT_ARTIFACT_ID.into(),
        enabled: true,
        native_options: json!(fixture.options),
    });
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let principal = owner.store.authenticate(credential).await.unwrap();
    (owner, principal)
}

async fn write(
    store: &Store,
    principal: &Principal,
    method: &str,
    mut input: Value,
) -> Result<Value> {
    input["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), input)
        .await
}

async fn wait_operation(store: &Store, principal: &Principal, operation: &Value, state: &str) {
    let operation_id = operation["operation_id"].as_str().unwrap().to_owned();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed = store
                .call(
                    principal.clone(),
                    "operation.get".into(),
                    json!({"operation_id":operation_id}),
                )
                .await
                .unwrap();
            if observed["state"] == state {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

async fn attached_principal(store: &Store, binding_id: String, boot: String) -> Principal {
    store
        .run(move |db| {
            let binding = operations::get_binding(db, &binding_id, 1)?;
            attach(db, &binding, &boot, "external_shared_service")
        })
        .await
        .unwrap()
}

async fn pending(store: &Store, principal: Principal) -> Result<Vec<RuntimeCommand>> {
    let config = store.config.clone();
    store
        .run(move |db| reg23_isolation::pending_commands(db, &principal, &config))
        .await
}

/// Corrupt global cursor metadata is a hard failure of each real selector.
/// It must reach the owning supervisor without another native mutation.
#[tokio::test]
async fn hard_selector_cursor_failures_reach_the_owning_supervisor() {
    for reader in ["pending", "result", "execution"] {
        let fixture = Fixture::new().await;
        let (owner, operator) = start(&fixture).await;
        let (stop, receiver) = watch::channel(false);
        let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
        let opened = write(
            &owner.store,
            &operator,
            "agent.open",
            json!({"lane_id":format!("hard-selector-{reader}"),"route":"fixture"}),
        )
        .await
        .unwrap();
        wait_operation(&owner.store, &operator, &opened, "settled").await;
        stop.send(true).unwrap();
        worker.await.unwrap().unwrap();

        let binding_id = opened["binding_id"].as_str().unwrap().to_owned();
        let cursor = format!(
            "opencode:v1:cursor:{reader}:{}",
            model::digest(format!("{binding_id}\0{}", 1).as_bytes())
        );
        owner
            .store
            .run(move |db| super::super::set_meta(db, &cursor, &Value::Null))
            .await
            .unwrap();
        let native_mutations_before = fixture
            .world
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|request| request.method != "GET")
            .count();
        let (stop, receiver) = watch::channel(false);
        let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
        let result = tokio::time::timeout(Duration::from_secs(15), worker)
            .await
            .expect("hard selector error must reach the owning supervisor")
            .unwrap()
            .unwrap_err();
        assert_eq!(result.code, "OPENCODE_ISOLATION_CURSOR_CORRUPT", "{reader}");
        assert_eq!(
            fixture
                .world
                .lock()
                .unwrap()
                .requests
                .iter()
                .filter(|request| request.method != "GET")
                .count(),
            native_mutations_before,
            "hard {reader} selection must not replay native input"
        );
        let retained = owner
            .store
            .call(
                operator.clone(),
                "operation.get".into(),
                json!({"operation_id":opened["operation_id"]}),
            )
            .await
            .unwrap();
        assert_eq!(retained["state"], "settled");
        drop(stop);
        owner.close().await.unwrap();
    }
}

async fn isolation_counts(store: &Store) -> (i64, i64) {
    store
        .run(|db| {
            let quarantines = db.query_row(
                "SELECT count(*) FROM meta WHERE key LIKE 'opencode:v1:row-quarantine:%'",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            let cursors = db.query_row(
                "SELECT count(*) FROM meta WHERE key LIKE 'opencode:v1:cursor:pending:%'",
                [],
                |row| row.get::<_, i64>(0),
            )?;
            Ok((quarantines, cursors))
        })
        .await
        .unwrap()
}

async fn pending_cursor(store: &Store) -> Value {
    store
        .run(|db| {
            let raw: String = db.query_row(
                "SELECT value_json FROM meta WHERE key LIKE 'opencode:v1:cursor:pending:%'",
                [],
                |row| row.get(0),
            )?;
            Ok(serde_json::from_str(&raw)?)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn pending_selection_quarantines_one_retained_row_and_resumes_atomically() {
    let fixture = Fixture::new().await;
    let (owner, operator) = start(&fixture).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));

    let opened = write(
        &owner.store,
        &operator,
        "agent.open",
        json!({"lane_id":"row-isolation","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &operator, &opened, "settled").await;

    let binding_id = opened["binding_id"].as_str().unwrap().to_owned();
    let sent = write(
        &owner.store,
        &operator,
        "agent.send",
        json!({
            "binding_id":binding_id,
            "generation":1,
            "text":"fixture-only retained operation",
            "delivery":"next_turn"
        }),
    )
    .await
    .unwrap();

    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();

    let open_id = opened["operation_id"].as_str().unwrap().to_owned();
    let send_id = sent["operation_id"].as_str().unwrap().to_owned();
    let (damaged_id, healthy_id) = if open_id < send_id {
        (open_id.clone(), send_id.clone())
    } else {
        (send_id.clone(), open_id.clone())
    };
    let binding_for_damage = binding_id.clone();
    let damaged_for_update = damaged_id.clone();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE operations SET state='outcome_unknown',settled_at_ms=NULL
                 WHERE operation_id IN (?1,?2) AND binding_id=?3",
                params![open_id, send_id, binding_for_damage],
            )?;
            db.execute(
                "UPDATE operations SET original_request_json='[]' WHERE operation_id=?1",
                params![damaged_for_update],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let module_principal = attached_principal(
        &owner.store,
        binding_id.clone(),
        "row-isolation-test".into(),
    )
    .await;

    owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER injected_opencode_quarantine_insert
                 BEFORE INSERT ON meta
                 WHEN NEW.key LIKE 'opencode:v1:row-quarantine:%'
                 BEGIN SELECT RAISE(ABORT,'injected quarantine write failure'); END;
                 CREATE TRIGGER injected_opencode_quarantine_update
                 BEFORE UPDATE ON meta
                 WHEN NEW.key LIKE 'opencode:v1:row-quarantine:%'
                 BEGIN SELECT RAISE(ABORT,'injected quarantine write failure'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let failed = pending(&owner.store, module_principal.clone()).await;
    assert!(
        failed.is_err(),
        "quarantine failure must abort the Store consumer"
    );
    assert_eq!(isolation_counts(&owner.store).await, (0, 0));

    owner
        .store
        .run(|db| {
            db.execute_batch(
                "DROP TRIGGER injected_opencode_quarantine_insert;
                 DROP TRIGGER injected_opencode_quarantine_update;
                 CREATE TRIGGER injected_opencode_cursor_insert
                 BEFORE INSERT ON meta
                 WHEN NEW.key LIKE 'opencode:v1:cursor:pending:%'
                 BEGIN SELECT RAISE(ABORT,'injected cursor write failure'); END;
                 CREATE TRIGGER injected_opencode_cursor_update
                 BEFORE UPDATE ON meta
                 WHEN NEW.key LIKE 'opencode:v1:cursor:pending:%'
                 BEGIN SELECT RAISE(ABORT,'injected cursor write failure'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let failed = pending(&owner.store, module_principal.clone()).await;
    assert!(
        failed.is_err(),
        "cursor failure must abort the Store consumer"
    );
    assert_eq!(isolation_counts(&owner.store).await, (0, 0));

    owner
        .store
        .run(|db| {
            db.execute_batch(
                "DROP TRIGGER injected_opencode_cursor_insert;
                 DROP TRIGGER injected_opencode_cursor_update;",
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let commands = pending(&owner.store, module_principal).await.unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].operation_id, healthy_id);
    assert_eq!(
        pending_cursor(&owner.store).await["after_operation_id"],
        healthy_id
    );
    let healthy_for_state = healthy_id.clone();
    let healthy_state = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT state FROM operations WHERE operation_id=?1",
                params![healthy_for_state],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    assert_eq!(healthy_state, "outcome_unknown");
    owner.close().await.unwrap();

    let (owner, _operator) = start(&fixture).await;
    let module_principal =
        attached_principal(&owner.store, binding_id, "row-isolation-restart".into()).await;
    assert_eq!(
        pending_cursor(&owner.store).await["after_operation_id"],
        healthy_id
    );
    let resumed = pending(&owner.store, module_principal).await.unwrap();
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].operation_id, healthy_id);
    owner.close().await.unwrap();
}
