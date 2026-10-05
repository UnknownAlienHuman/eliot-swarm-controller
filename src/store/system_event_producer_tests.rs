use super::*;
use crate::platform::{DataRoot, bootstrap_credential};
use std::sync::Arc;

async fn started_store() -> (StoreOwner, Principal, std::path::PathBuf) {
    let directory = std::env::temp_dir().join(format!("eliot-system-events-{}", model::new_id()));
    std::fs::create_dir_all(&directory).expect("create temporary Store directory");
    let root = DataRoot::acquire(&directory).expect("acquire temporary Store root");
    let credential = bootstrap_credential(&root.path).expect("create Operator credential");
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .expect("start Store");
    let operator = owner
        .store
        .authenticate(credential)
        .await
        .expect("authenticate Operator");
    (owner, operator, directory)
}

async fn write(
    store: &Store,
    principal: &Principal,
    method: &str,
    mut value: Value,
) -> Result<Value> {
    value["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
}

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    let token = format!("token-for-{client_id}");
    write(
        store,
        operator,
        "client.register",
        json!({"client_id":client_id,"role":"manager","token_hash":model::digest(token.as_bytes())}),
    )
    .await
    .expect("register Manager");
    store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .expect("authenticate Manager")
}

#[tokio::test]
async fn rejected_admission_retains_one_safe_failure_event_on_request_replay() {
    let (owner, operator, directory) = started_store().await;
    let request = json!({
        "client_request_id":"rejected-event-fixture",
        "recipient":"missing-recipient",
        "text":"private-rejection-sentinel"
    });
    for _ in 0..2 {
        let error = owner
            .store
            .call(operator.clone(), "message.send".to_owned(), request.clone())
            .await
            .expect_err("unregistered recipient must reject the request");
        assert_eq!(error.code, "NOT_FOUND");
    }
    owner.store.run(|db| {
        let operation_id: String = db.query_row(
            "SELECT operation_id FROM operations WHERE method='message.send' AND client_request_id='rejected-event-fixture' AND state='rejected'",
            [], |row| row.get(0),
        )?;
        let (count, observation_id, raw): (i64, i64, String) = db.query_row(
            "SELECT count(*),min(observation_id),min(payload_json) FROM observations WHERE operation_id=?1 AND source_stream_id='controller:operations' AND kind='operation.rejected'",
            [&operation_id], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        assert_eq!(count, 1);
        let event = automation_intake::observed_event_by_id(db, observation_id)?.unwrap();
        let projection = automation_intake::safe_event_projection(db, &event)?;
        assert_eq!(projection.status, Some(crate::automation::event_rules::EventStatus::Rejected));
        assert_eq!(projection.error_code.as_deref(), Some("OPERATION_REJECTED"));
        assert_eq!(projection.occurrence_id.as_deref(), Some(format!("operation:{operation_id}:operation_rejected").as_str()));
        assert!(!raw.contains("private-rejection-sentinel"));
        assert!(!raw.contains("missing-recipient"));
        let committed: i64 = db.query_row(
            "SELECT count(*) FROM observations WHERE operation_id=?1 AND source_stream_id='controller:messages'", [&operation_id], |row| row.get(0),
        )?;
        assert_eq!(committed, 0, "rejected admission has no committed message event");
        Ok(())
    }).await.unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn message_and_reply_aliases_keep_only_safe_headers_and_share_their_cause() {
    let (owner, operator, directory) = started_store().await;
    let alice = register_manager(&owner.store, &operator, "alice").await;
    let bob = register_manager(&owner.store, &operator, "bob").await;
    let original = write(
        &owner.store,
        &alice,
        "message.send",
        json!({"recipient":"bob","text":"private-original-sentinel"}),
    )
    .await
    .expect("send original message");
    let reply = write(
        &owner.store,
        &bob,
        "message.send",
        json!({
            "recipient":"alice",
            "text":"private-reply-sentinel",
            "in_reply_to":original["delivery_id"],
            "in_reply_to_digest":original["payload_digest"],
        }),
    )
    .await
    .expect("send validated reply");
    let reply_operation = reply["operation_id"]
        .as_str()
        .expect("reply Operation")
        .to_owned();
    let expected_reply_key = format!("reply:{reply_operation}");
    let expected_sent_key = format!("sent:{reply_operation}");
    let query_operation = reply_operation.clone();
    let observations = owner
        .store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT kind,source_event_key,payload_json FROM observations \
                 WHERE source_stream_id='controller:messages' AND operation_id=?1 ORDER BY kind",
            )?;
            let rows = statement
                .query_map([query_operation], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .expect("read committed message observations");

    assert_eq!(observations.len(), 2);
    let mut safe = observations
        .iter()
        .map(|(kind, key, raw)| {
            (
                kind.as_str(),
                key.as_str(),
                serde_json::from_str::<Value>(raw).expect("safe event JSON"),
            )
        })
        .collect::<Vec<_>>();
    safe.sort_by_key(|(kind, _, _)| *kind);
    assert_eq!(safe[0].0, "message.reply_sent");
    assert_eq!(safe[1].0, "message.sent");
    assert_eq!(safe[0].1, expected_reply_key.as_str());
    assert_eq!(safe[1].1, expected_sent_key.as_str());
    assert_eq!(safe[0].2["phase"], "message_send_committed");
    assert_eq!(safe[0].2["status"], "sent");
    assert_eq!(safe[0].2["occurrence_id"], safe[1].2["occurrence_id"]);
    assert_eq!(safe[0].2.as_object().unwrap().len(), 4);
    let safe_json = serde_json::to_string(&safe).expect("serialize safe projections");
    assert!(!safe_json.contains("private-original-sentinel"));
    assert!(!safe_json.contains("private-reply-sentinel"));
    assert!(!safe_json.contains("alice"));
    assert!(!safe_json.contains("bob"));

    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store directory");
}

#[test]
fn interrupted_host_aliases_use_the_exact_epoch_pair_without_changing_receipt() {
    let mut db = Connection::open_in_memory().expect("open lifecycle fixture DB");
    db.execute_batch(
        "CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);
         CREATE TABLE observations(
             source_stream_id TEXT NOT NULL,
             source_event_key TEXT NOT NULL,
             operation_id TEXT,
             kind TEXT NOT NULL,
             payload_json TEXT NOT NULL,
             recorded_at_ms INTEGER NOT NULL
         );",
    )
    .expect("create lifecycle fixture schema");
    set_meta(&db, "host_epoch", &json!(9)).expect("set current host epoch");
    set_meta(
        &db,
        "host:lifecycle:v1",
        &json!({
            "schema_version":1,
            "host_epoch":8,
            "state":"running",
            "started_at_ms":100,
            "updated_at_ms":200,
        }),
    )
    .expect("retain previous running host");

    let tx = db
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .expect("begin restart transaction");
    host_lifecycle::start(&tx, 300).expect("record restart interruption");
    tx.commit().expect("commit restart transaction");

    let rows = {
        let mut statement = db
            .prepare(
                "SELECT kind,payload_json FROM observations \
                 WHERE source_stream_id='controller:host-lifecycle' ORDER BY kind",
            )
            .expect("prepare observation query");
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .expect("query observations")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect observations")
    };
    assert_eq!(rows.len(), 2);
    let exit: Value =
        serde_json::from_str(&rows.iter().find(|(kind, _)| kind == "host.exit").unwrap().1)
            .expect("host.exit payload");
    let interrupted: Value = serde_json::from_str(
        &rows
            .iter()
            .find(|(kind, _)| kind == "host.interrupted")
            .unwrap()
            .1,
    )
    .expect("host.interrupted payload");
    let occurrence = "host-interruption:8:9";
    assert_eq!(exit["phase"], "host_interruption_observed");
    assert_eq!(exit["occurrence_id"], occurrence);
    assert_eq!(exit["previous_host_epoch"], 8);
    assert_eq!(exit["current_host_epoch"], 9);
    assert!(exit.get("cause").is_none());
    assert!(exit.get("status").is_none());
    assert_eq!(exit["retry_authorized"], false);
    assert_eq!(interrupted["occurrence_id"], occurrence);
    assert_eq!(interrupted["previous_host_epoch"], 8);
    assert_eq!(interrupted["current_host_epoch"], 9);
    assert_eq!(interrupted["error_code"], "HOST_INTERRUPTED");
    assert_eq!(interrupted["status"], "unknown");
    assert_eq!(interrupted.as_object().unwrap().len(), 7);

    let status = host_lifecycle::status(&db).expect("read public lifecycle receipt");
    assert_eq!(status["last_exit"]["host_epoch"], 8);
    assert_eq!(status["last_exit"].as_object().unwrap().len(), 6);
    assert!(status["last_exit"].get("occurrence_id").is_none());
}

fn assert_result_page_projection(eof: bool) {
    const PHASE: &str = "native_result_page_recorded";

    let operation_id = if eof {
        "result-page-eof"
    } else {
        "result-page-more"
    };
    let binding_id = "binding-result";
    let module_id = "module-result";
    let module_link_id = "module-link-result";
    let mut db = Connection::open_in_memory().expect("open result fixture DB");
    db.execute_batch("PRAGMA foreign_keys=ON;")
        .expect("enable Store foreign-key checks");
    db.execute_batch(include_str!("../../migrations/001_core.sql"))
        .expect("install the real core Store schema");

    let module_registration = json!({
        "role":"module",
        "disabled":false,
        "binding_id":binding_id,
        "binding_generation":1
    });
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
        params![
            format!("client:{module_id}"),
            model::canonical(&module_registration).expect("encode module registration")
        ],
    )
    .expect("register fixture Module");
    let binding_observation = json!({
        "module_client_id":module_id,
        "module_link_id":module_link_id
    });
    db.execute(
            "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
             VALUES(?1,1,'lane-result','instance-result','artifact-result','ready','scope-result','root-result','{}',?2,1)",
            params![
                binding_id,
                model::canonical(&binding_observation).expect("encode binding observation")
            ],
        )
        .expect("register fixture binding");

    let selector = json!({"native_output":"stdout"});
    let request = json!({
        "selector":selector,
        "offset_bytes":0,
        "length_bytes":4
    });
    db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,native_refs_json,result_json,due_at_ms,created_at_ms,updated_at_ms) \
             VALUES(?1,?2,?3,'agent.result',?4,'{}',?5,1,'sending','{}',NULL,1,1,1)",
            params![
                operation_id,
                module_id,
                format!("request:{operation_id}"),
                model::canonical(&request).expect("encode result request"),
                binding_id
            ],
        )
        .expect("admit fixture result Operation");

    let page = crate::artifacts::ResultPage {
        source: json!({"native_output":"stdout"}),
        offset_bytes: 0,
        byte_length: 4,
        total_bytes: if eof { 4 } else { 8 },
        eof,
        media_type: "text/plain".into(),
        content_base64: "cGFnZQ==".into(),
        page_sha256: model::digest(b"page"),
    };
    let bytes = page.decode().expect("validate result page and EOF");
    let mut metadata = page.metadata();
    metadata["operation_id"] = json!(operation_id);
    metadata["binding_id"] = json!(binding_id);
    metadata["generation"] = json!(1);
    metadata["native_root_id"] = json!("root-result");
    metadata["native_scope_key"] = json!("scope-result");
    metadata["selector"] = selector;
    metadata["requested_offset"] = json!(0);
    metadata["requested_length"] = json!(4);
    let artifact = crate::artifacts::ArtifactFiles::record(operation_id, &bytes, metadata);
    let principal = Principal {
        client_id: module_id.into(),
        link_id: module_link_id.into(),
        role: Role::Module,
    };

    results::record(&mut db, &principal, &artifact)
        .expect("persist result page and normalized event atomically");

    let observations = {
        let mut statement = db
            .prepare(
                "SELECT source_stream_id,kind,operation_id,payload_json \
                     FROM observations WHERE operation_id=?1 ORDER BY source_stream_id",
            )
            .expect("prepare result observation query");
        statement
            .query_map([operation_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .expect("query raw and normalized result observations")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect result observations")
    };
    assert_eq!(observations.len(), 2);
    let raw = observations
        .iter()
        .find(|(source, kind, _, _)| {
            source == &format!("module:{module_id}") && kind == "runtime.result"
        })
        .expect("raw module result observation");
    let normalized = observations
        .iter()
        .find(|(source, kind, _, _)| {
            source == "controller:runtime" && kind == "native.result.available"
        })
        .expect("normalized result-page observation");
    assert_eq!(raw.2, operation_id);
    assert_eq!(normalized.2, operation_id);
    let raw_payload: Value = serde_json::from_str(&raw.3).expect("decode raw result DTO");
    let normalized_payload: Value =
        serde_json::from_str(&normalized.3).expect("decode normalized event DTO");
    assert_eq!(raw_payload["details"]["eof"], eof);
    assert_eq!(normalized_payload["schema_version"], 1);
    assert_eq!(normalized_payload["phase"], PHASE);
    assert_eq!(
        normalized_payload["status"],
        if eof { "completed" } else { "incomplete" }
    );
    let occurrence_id = format!("operation:{operation_id}:{PHASE}");
    assert_eq!(normalized_payload["occurrence_id"], occurrence_id);
    assert!(normalized_payload.get("eof").is_none());
    assert_eq!(normalized_payload.as_object().unwrap().len(), 4);
    let cause_id = |phase: &str, occurrence_id: &str| {
        model::digest(
            model::canonical(&json!({"phase":phase,"occurrence_id":occurrence_id}))
                .expect("encode semantic event identity")
                .as_bytes(),
        )
    };
    let normalized_cause_id = cause_id(
        normalized_payload["phase"].as_str().unwrap(),
        normalized_payload["occurrence_id"].as_str().unwrap(),
    );
    let raw_alias_occurrence = format!("operation:{}:{PHASE}", raw.2);
    assert_eq!(
        normalized_cause_id,
        cause_id(PHASE, &raw_alias_occurrence),
        "raw runtime.result and normalized page facts must resolve to one cause"
    );
    let terminal_phase = "native_outcome_terminal";
    let terminal_occurrence = format!("operation:{operation_id}:{terminal_phase}");
    assert_ne!(
        normalized_cause_id,
        cause_id(terminal_phase, &terminal_occurrence),
        "result-page and terminal-outcome occurrences remain distinct"
    );

    let operation: (String, String) = db
        .query_row(
            "SELECT state,result_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("read settled result Operation");
    assert_eq!(operation.0, "settled");
    let result: Value = serde_json::from_str(&operation.1).expect("decode result outcome");
    assert_eq!(result["outcome"], "applied");
    assert_eq!(result["details"]["eof"], eof);
    assert_eq!(
        normalized_payload["status"],
        if eof { "completed" } else { "incomplete" }
    );
}

#[test]
fn result_page_projection_keeps_eof_status_and_phase_on_the_exact_operation() {
    assert_result_page_projection(true);
    assert_result_page_projection(false);
}
