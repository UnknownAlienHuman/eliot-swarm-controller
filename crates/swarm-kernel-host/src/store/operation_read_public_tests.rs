use super::super::{Store, StoreOwner};
use crate::{
    config::Config,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const PROJECT: &str = "operation-read-bounded-public";
const OPERATION_POSITION_STREAM: &str = "controller:read-position:operation";

struct Fixture {
    owner: StoreOwner,
    directory: PathBuf,
    manager_a: Principal,
    manager_b: Principal,
}

async fn start() -> Fixture {
    let directory = std::env::temp_dir().join(format!("swarm-operation-read-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    let manager_a = register_manager(&owner.store, &operator, "operation-read-manager-a").await;
    let manager_b = register_manager(&owner.store, &operator, "operation-read-manager-b").await;
    Fixture {
        owner,
        directory,
        manager_a,
        manager_b,
    }
}

impl Fixture {
    async fn close(self) {
        self.owner.close().await.unwrap();
        std::fs::remove_dir_all(self.directory).unwrap();
    }
}

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    let token = format!("operation-read-token-{client_id}");
    store
        .call(
            operator.clone(),
            "client.register".into(),
            json!({
                "client_request_id":model::new_id(),
                "client_id":client_id,
                "role":"manager",
                "token_hash":model::digest(token.as_bytes()),
            }),
        )
        .await
        .unwrap();
    store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

fn task_spec(label: &str) -> Value {
    let mut spec: Value =
        serde_json::from_str(include_str!("../../config/task.example.json")).unwrap();
    spec["objective"] = json!(format!("Bounded Operation read fixture {label}"));
    spec
}

async fn create(store: &Store, manager: &Principal, label: &str) -> Value {
    store
        .call(
            manager.clone(),
            "task.create".into(),
            json!({
                "client_request_id":model::new_id(),
                "project_id":PROJECT,
                "origin_key":format!("operation-read-{label}"),
                "spec":task_spec(label),
            }),
        )
        .await
        .unwrap()
}

async fn claim(store: &Store, manager: &Principal, task: &Value) -> Value {
    store
        .call(
            manager.clone(),
            "task.claim".into(),
            json!({
                "client_request_id":model::new_id(),
                "task_id":task["task_id"],
                "expected_revision":task["revision"],
                "start_owner":"native_manager",
            }),
        )
        .await
        .unwrap()
}

async fn call(store: &Store, principal: &Principal, method: &str, request: Value) -> Value {
    store
        .call(principal.clone(), method.into(), request)
        .await
        .unwrap_or_else(|error| panic!("{method}: {error:?}"))
}

async fn max_observation_id(store: &Store) -> i64 {
    store
        .run(|db| {
            Ok(db.query_row(
                "SELECT COALESCE(MAX(observation_id),0) FROM observations",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn max_operation_position(store: &Store) -> i64 {
    store
        .run(|db| {
            Ok(db.query_row(
                "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE source_stream_id=?1",
                [OPERATION_POSITION_STREAM],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn operation_positions_after(store: &Store, after: i64, limit: usize) -> Vec<i64> {
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT observation_id FROM observations WHERE source_stream_id=?1 AND observation_id>?2 ORDER BY observation_id LIMIT ?3",
            )?;
            let rows = statement.query_map(
                params![OPERATION_POSITION_STREAM, after, limit as i64],
                |row| row.get(0),
            )?;
            Ok(rows.collect::<rusqlite::Result<Vec<i64>>>()?)
        })
        .await
        .unwrap()
}

async fn operation_event_positions_for_prefix(
    store: &Store,
    prefix: String,
    after: i64,
) -> Vec<i64> {
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT observation.observation_id FROM observations AS observation JOIN operations AS operation ON operation.operation_id=observation.operation_id WHERE operation.client_request_id LIKE ?1 AND observation.observation_id>?2 AND observation.source_stream_id NOT IN ('controller:messages','controller:read-position:operation','controller:read-position:task') ORDER BY observation.observation_id",
            )?;
            let rows = statement.query_map(params![format!("{prefix}-%"), after], |row| {
                row.get(0)
            })?;
            Ok(rows.collect::<rusqlite::Result<Vec<i64>>>()?)
        })
        .await
        .unwrap()
}

async fn operation_ids_for_prefix(store: &Store, prefix: String) -> Vec<String> {
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT operation_id FROM operations WHERE client_request_id LIKE ?1 ORDER BY client_request_id",
            )?;
            let rows = statement.query_map([format!("{prefix}-%")], |row| row.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<String>>>()?)
        })
        .await
        .unwrap()
}

fn assert_no_hidden_identity(
    page: &Value,
    hidden_operation_ids: &[String],
    hidden_prefix: &str,
    hidden_task_id: &str,
    mismatched_attempt_id: &str,
    damaged_operation_id: &str,
) {
    let encoded = model::canonical(page).unwrap();
    for identity in hidden_operation_ids.iter().map(String::as_str).chain([
        hidden_prefix,
        hidden_task_id,
        mismatched_attempt_id,
        damaged_operation_id,
    ]) {
        assert!(
            !encoded.contains(identity),
            "hidden Operation identity escaped a bounded page: {encoded}"
        );
    }
    assert!(page.get("filtered_count").is_none());
}

#[tokio::test]
async fn store_authenticated_operation_lists_and_delta_preserve_bounded_gaps_and_cursor_tail() {
    let f = start().await;
    let store = &f.owner.store;
    let owned_task = create(store, &f.manager_a, "owner").await;
    let owned_attempt = claim(store, &f.manager_a, &owned_task).await;
    let owned_attempt_id = owned_attempt["attempt_id"].as_str().unwrap().to_owned();
    let list_after = max_operation_position(store).await;
    let delta_after = max_observation_id(store).await;
    let scan_limit = super::super::projection::MAX_PROJECTED_ITEMS;
    let hidden_prefix = format!("operation-read-hidden-{}", model::new_id());

    for index in 0..scan_limit {
        let error = store
            .call(
                f.manager_b.clone(),
                "task.claim".into(),
                json!({
                    "client_request_id":format!("{hidden_prefix}-{index}"),
                    "task_id":owned_task["task_id"],
                    "expected_revision":owned_task["revision"],
                    "start_owner":"native_manager",
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "TASK_ALREADY_OWNED");
    }

    let hidden_operation_ids = operation_ids_for_prefix(store, hidden_prefix.clone()).await;
    assert_eq!(hidden_operation_ids.len(), scan_limit);
    let hidden_positions = operation_positions_after(store, list_after, scan_limit).await;
    assert_eq!(hidden_positions.len(), scan_limit);
    let expected_list_cursor = *hidden_positions.last().unwrap();
    let hidden_event_positions =
        operation_event_positions_for_prefix(store, hidden_prefix.clone(), delta_after).await;
    assert!(hidden_event_positions.len() >= scan_limit);
    let expected_delta_cursor = hidden_event_positions[scan_limit - 1];

    let damaged_task = create(store, &f.manager_b, "damaged-relation").await;
    let damaged_operation_id = damaged_task["operation_id"].as_str().unwrap().to_owned();
    let damaged_operation_for_update = damaged_operation_id.clone();
    let mismatched_attempt_for_update = owned_attempt_id.clone();
    let changed = store
        .run(move |db| {
            Ok(db.execute(
                "UPDATE operations SET attempt_id=?1 WHERE operation_id=?2",
                params![mismatched_attempt_for_update, damaged_operation_for_update],
            )?)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);

    let later_task = create(store, &f.manager_a, "visible-tail").await;
    let later_operation_id = later_task["operation_id"].as_str().unwrap().to_owned();
    let hidden_task_id = damaged_task["task_id"].as_str().unwrap();

    let first_list = call(
        store,
        &f.manager_a,
        "operation.list",
        json!({"after":list_after,"limit":1}),
    )
    .await;
    assert!(first_list["items"].as_array().unwrap().is_empty());
    assert_eq!(first_list["next_after"], expected_list_cursor);
    assert_eq!(first_list["has_newer"], true);
    assert_eq!(first_list["coverage_complete"], true);
    assert_eq!(first_list["gap_count"], 0);
    assert_no_hidden_identity(
        &first_list,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let first_delta = call(
        store,
        &f.manager_a,
        "report.delta",
        json!({"after":delta_after,"limit":1}),
    )
    .await;
    assert!(first_delta["items"].as_array().unwrap().is_empty());
    assert_eq!(first_delta["next_cursor"], expected_delta_cursor);
    assert_eq!(first_delta["projection"]["has_newer"], true);
    assert_eq!(first_delta["projection"]["coverage_complete"], true);
    assert_eq!(first_delta["projection"]["gap_count"], 0);
    assert!(
        first_delta["projection"]["range"]
            .get("filtered_count")
            .is_none()
    );
    assert_no_hidden_identity(
        &first_delta,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let list_gap = call(
        store,
        &f.manager_a,
        "operation.list",
        json!({"after":first_list["next_after"],"limit":1}),
    )
    .await;
    assert_eq!(list_gap["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        list_gap["items"][0]["gap"]["reason"],
        "retained_relation_damaged"
    );
    assert_eq!(
        list_gap["items"][0]["gap"]["error_code"],
        "OBJECT_SCOPE_DAMAGED"
    );
    assert!(list_gap["items"][0].get("operation_id").is_none());
    assert_eq!(list_gap["coverage_complete"], false);
    assert_eq!(list_gap["gap_count"], 1);
    assert_eq!(list_gap["has_newer"], true);
    assert_eq!(list_gap["next_after"], list_gap["items"][0]["cursor"]);
    assert_no_hidden_identity(
        &list_gap,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let list_tail = call(
        store,
        &f.manager_a,
        "operation.list",
        json!({"after":list_gap["next_after"],"limit":1}),
    )
    .await;
    assert_eq!(list_tail["items"].as_array().unwrap().len(), 1);
    assert_eq!(list_tail["items"][0]["operation_id"], later_operation_id);
    assert_eq!(list_tail["coverage_complete"], true);
    assert_eq!(list_tail["gap_count"], 0);
    assert_no_hidden_identity(
        &list_tail,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let delta_gap = call(
        store,
        &f.manager_a,
        "report.delta",
        json!({"after":first_delta["next_cursor"],"limit":1}),
    )
    .await;
    assert_eq!(delta_gap["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        delta_gap["items"][0]["gap"]["reason"],
        "retained_relation_damaged"
    );
    assert_eq!(
        delta_gap["items"][0]["gap"]["error_code"],
        "OBJECT_SCOPE_DAMAGED"
    );
    assert!(delta_gap["items"][0].get("operation_id").is_none());
    assert!(delta_gap["items"][0].get("payload").is_none());
    assert!(delta_gap["items"][0].get("kind").is_none());
    assert_eq!(delta_gap["projection"]["coverage_complete"], false);
    assert_eq!(delta_gap["projection"]["gap_count"], 1);
    assert_eq!(delta_gap["projection"]["has_newer"], true);
    assert!(
        delta_gap["projection"]["range"]
            .get("filtered_count")
            .is_none()
    );
    assert_eq!(delta_gap["next_cursor"], delta_gap["items"][0]["cursor"]);
    assert_no_hidden_identity(
        &delta_gap,
        &hidden_operation_ids,
        &hidden_prefix,
        hidden_task_id,
        &owned_attempt_id,
        &damaged_operation_id,
    );

    let mut cursor = delta_gap["next_cursor"].as_i64().unwrap();
    let cursor_for_count = cursor;
    let remaining_observations = store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM observations WHERE observation_id>?1",
                [cursor_for_count],
                |row| row.get::<_, i64>(0),
            )? as usize)
        })
        .await
        .unwrap();
    let mut recovered = false;
    for _ in 0..=remaining_observations {
        let page = call(
            store,
            &f.manager_a,
            "report.delta",
            json!({"after":cursor,"limit":1}),
        )
        .await;
        assert_no_hidden_identity(
            &page,
            &hidden_operation_ids,
            &hidden_prefix,
            hidden_task_id,
            &owned_attempt_id,
            &damaged_operation_id,
        );
        if page["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["operation_id"] == later_operation_id)
        {
            assert_eq!(page["projection"]["coverage_complete"], true);
            assert_eq!(page["projection"]["gap_count"], 0);
            recovered = true;
            break;
        }
        let next = page["next_cursor"].as_i64().unwrap();
        assert!(
            next > cursor,
            "delta cursor stopped before the visible tail"
        );
        cursor = next;
    }
    assert!(
        recovered,
        "report.delta cursor skipped the first later visible Operation"
    );
    f.close().await;
}
