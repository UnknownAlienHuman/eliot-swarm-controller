use super::*;
use crate::platform::{DataRoot, bootstrap_credential};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::sync::Arc;

const TERMINAL_EVENT_MANAGER_ID: &str = "host-terminal-event-manager";
const TERMINAL_EVENT_PROJECT_ID: &str = "host-terminal-event-project";
const TERMINAL_EVENT_AUTOMATION_ID: &str = "host-terminal-event-script-trigger";
const TERMINAL_EVENT_SCRIPT_ID: &str = "host_terminal_event_script";

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

fn powershell_path() -> std::path::PathBuf {
    let executable = if cfg!(windows) { "pwsh.exe" } else { "pwsh" };
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .map(|directory| directory.join(executable))
        .filter_map(|path| std::fs::canonicalize(path).ok())
        .find(|path| {
            std::fs::symlink_metadata(path)
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        })
        .expect("pwsh is needed only for registered bundle identity checks")
}

async fn register_terminal_event_script(store: &Store, manager: &Principal) {
    let bundle = json!({
        "script_id":TERMINAL_EVENT_SCRIPT_ID,
        "interpreter_kind":"powershell",
        "interpreter_path":powershell_path(),
        "entrypoint":"main.ps1",
        "argv":[],
        "trust":"trusted_local",
        "inherit_environment":[],
        "controller_effects":[],
        "input_schema":{"type":"object","properties":{},"required":[],"additional_properties":true},
        "result_schema":{"type":"null"},
        "files":[{"path":"main.ps1","content_base64":STANDARD.encode(b"Write-Output {}")}],
    });
    write(store, manager, "script.register", json!({"bundle":bundle}))
        .await
        .expect("register the script bundle without executing it");
    write(
        store,
        manager,
        "script.activate",
        json!({"script_id":TERMINAL_EVENT_SCRIPT_ID,"revision":1}),
    )
    .await
    .expect("activate the immutable script revision");
}

async fn configure_terminal_event_trigger(store: &Store, manager: &Principal) {
    let changes = json!([{
        "automation_id":TERMINAL_EVENT_AUTOMATION_ID,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "steps":["script_run"],
            "script_run":{"script_id":TERMINAL_EVENT_SCRIPT_ID},
            "event_rules":[
                {"source_id":"controller:host-lifecycle","event_kind":"host.exit","status":null,"action":"script_run"},
                {"source_id":"controller:host-lifecycle","event_kind":"host.failed","status":null,"action":"script_run"}
            ]
        }
    }]);
    let preview = store
        .call(
            manager.clone(),
            "automation.config.preview".to_owned(),
            json!({"project_id":TERMINAL_EVENT_PROJECT_ID,"changes":changes}),
        )
        .await
        .expect("preview host terminal ScriptRun selectors");
    assert_eq!(preview["valid"], true, "{preview}");
    write(
        store,
        manager,
        "automation.config.apply",
        json!({
            "project_id":TERMINAL_EVENT_PROJECT_ID,
            "changes":changes,
            "preview_digest":preview["plan_sha256"]
        }),
    )
    .await
    .expect("apply host terminal ScriptRun selectors");
}

async fn explain_terminal_event_trigger(store: &Store, manager: &Principal) -> Value {
    store
        .call(
            manager.clone(),
            "automation.config.explain".to_owned(),
            json!({
                "project_id":TERMINAL_EVENT_PROJECT_ID,
                "automation_id":TERMINAL_EVENT_AUTOMATION_ID
            }),
        )
        .await
        .expect("read current-Manager ScriptRun state and linked causes")
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

#[tokio::test]
async fn terminal_host_exit_is_a_safe_any_event_and_failure_views_share_one_script_cause() {
    // This exercises the ordinary Store producer, committed observations,
    // the current Manager's ScriptRun cursor, and its retained operation link.
    // The separate script supervisor is not started, so no interpreter runs.
    let (owner, operator, directory) = started_store().await;
    let manager = register_manager(&owner.store, &operator, TERMINAL_EVENT_MANAGER_ID).await;
    write(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":TERMINAL_EVENT_MANAGER_ID}),
    )
    .await
    .expect("designate the fixture Manager");
    register_terminal_event_script(&owner.store, &manager).await;
    configure_terminal_event_trigger(&owner.store, &manager).await;
    owner
        .store
        .record_host_start()
        .await
        .expect("record initial host startup");
    owner
        .store
        .record_host_ready()
        .await
        .expect("record ready host lifecycle");

    let initial_epoch = owner
        .store
        .run(|db| {
            Ok(crate::store::meta(db, "host_epoch")?
                .and_then(|value| value.as_i64())
                .unwrap_or_default())
        })
        .await
        .expect("read current host epoch");
    assert!(initial_epoch > 0);
    owner
        .store
        .run(move |db| {
            crate::store::set_meta(
                db,
                "host:last-exit:v1",
                &json!({
                    "schema_version":1,
                    "host_epoch":initial_epoch,
                    "observed_at_ms":1,
                    "error_code":"HOST_INTERRUPTED",
                    "manager_action_required":true,
                    "retry_authorized":false,
                }),
            )?;
            Ok(())
        })
        .await
        .expect("seed the original schema-version-1 exit shape");
    let old_receipt = owner
        .store
        .call(
            manager.clone(),
            "swarm.exceptions.get".to_owned(),
            json!({"after":0,"limit":32}),
        )
        .await
        .expect("current Manager can read the old schema-version-1 receipt");
    assert_eq!(
        old_receipt["host_lifecycle"]["last_exit"]["error_code"],
        "HOST_INTERRUPTED"
    );
    assert!(
        old_receipt["host_lifecycle"]["last_exit"]
            .get("failed_supervisor")
            .is_none()
    );
    assert!(
        old_receipt["host_lifecycle"]["last_exit"]
            .get("failure_category")
            .is_none()
    );

    owner
        .store
        .record_host_exit(Some("SUPERVISOR_FAILED".to_owned()), Some("scripts"))
        .await
        .expect("commit a named supervisor failure");
    let failure_observations = owner
        .store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT observation_id,kind,payload_json FROM observations \
                 WHERE source_stream_id='controller:host-lifecycle' \
                 AND source_event_key IN (?1,?2) ORDER BY observation_id",
            )?;
            let rows = statement
                .query_map(
                    rusqlite::params![
                        format!("failed:{initial_epoch}"),
                        format!("terminal:{initial_epoch}")
                    ],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                        ))
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut projected = Vec::with_capacity(rows.len());
            for (observation_id, event_kind, raw) in rows {
                let event = automation_intake::observed_event_by_id(db, observation_id)?
                    .expect("committed host lifecycle observation");
                let projection = automation_intake::safe_event_projection(db, &event)?;
                projected.push(json!({
                    "event_kind":event_kind,
                    "raw":serde_json::from_str::<Value>(&raw)?,
                    "status":projection.status.map(crate::automation::event_rules::EventStatus::as_str),
                    "failure_category":projection.failure_category,
                    "failed_supervisor":projection.failed_supervisor,
                    "error_code":projection.error_code,
                    "occurrence_phase":projection.occurrence_phase,
                    "occurrence_id":projection.occurrence_id
                }));
            }
            Ok(projected)
        })
        .await
        .expect("read and project the committed terminal failure views");
    assert_eq!(failure_observations.len(), 2, "{failure_observations:?}");
    let raw_failure = failure_observations
        .iter()
        .find(|row| row["event_kind"] == "host.exit")
        .expect("raw terminal host exit");
    let normalized_failure = failure_observations
        .iter()
        .find(|row| row["event_kind"] == "host.failed")
        .expect("normalized terminal host failure");
    for row in [raw_failure, normalized_failure] {
        assert_eq!(row["status"], "failed");
        assert_eq!(row["failure_category"], "supervisor_failed");
        assert_eq!(row["failed_supervisor"], "scripts");
        assert_eq!(row["error_code"], Value::Null);
        assert_eq!(row["occurrence_phase"], "host_terminal_exit_observed");
        assert_eq!(
            row["occurrence_id"],
            format!("host-terminal-exit:{initial_epoch}")
        );
        assert!(row["raw"].get("error_code").is_none());
        assert!(row["raw"].get("message").is_none());
    }
    assert_eq!(
        raw_failure["occurrence_id"],
        normalized_failure["occurrence_id"]
    );

    let failure_readback = owner
        .store
        .call(
            manager.clone(),
            "swarm.exceptions.get".to_owned(),
            json!({"after":0,"limit":32}),
        )
        .await
        .expect("read the terminal failure through the current Manager route");
    let latest_failure = &failure_readback["host_lifecycle"]["latest_failure"];
    assert_eq!(latest_failure["error_code"], "SUPERVISOR_FAILED");
    assert_eq!(latest_failure["failure_category"], "supervisor_failed");
    assert_eq!(latest_failure["failed_supervisor"], "scripts");

    let failure_pass = owner
        .store
        .reconcile_automations_once()
        .await
        .expect("ordinary automation reconciliation admits the host failure");
    assert_eq!(
        failure_pass["script_run"]["considered"], 1,
        "{failure_pass}"
    );
    assert_eq!(
        failure_pass["script_run"]["outcomes"][0]["state"],
        "admitted"
    );
    let failure_explanation = explain_terminal_event_trigger(&owner.store, &manager).await;
    let failure_links = failure_explanation["linked_operation_history"]["items"]
        .as_array()
        .expect("manager-visible linked ScriptRun operation");
    assert_eq!(failure_links.len(), 1, "{failure_explanation}");
    let failure_cause = &failure_links[0]["cause"];
    assert_eq!(failure_cause["event_kind"], "host.failed");
    assert_eq!(failure_cause["status"], "failed");
    assert_eq!(failure_cause["failure_category"], "supervisor_failed");
    assert_eq!(failure_cause["failed_supervisor"], "scripts");
    assert_eq!(failure_cause["occurrence_id"], raw_failure["occurrence_id"]);
    assert!(failure_cause["error_code"].is_null());
    let recent = failure_explanation["script_run"]["recent"]
        .as_array()
        .expect("manager-visible durable ScriptRun history");
    assert!(
        recent
            .iter()
            .any(|entry| entry["disposition"] == "coalesced")
    );
    assert!(
        recent
            .iter()
            .any(|entry| entry["disposition"] == "admitted")
    );

    let next_epoch = initial_epoch + 1;
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            crate::store::set_meta(&tx, "host_epoch", &json!(next_epoch))?;
            tx.commit()?;
            Ok(())
        })
        .await
        .expect("advance to a distinct host epoch for graceful completion");
    owner
        .store
        .record_host_start()
        .await
        .expect("record next host epoch");
    owner
        .store
        .record_host_ready()
        .await
        .expect("mark next host epoch ready");
    owner
        .store
        .record_host_exit(None, None)
        .await
        .expect("commit an ordinary graceful host stop");
    let completed_projection = owner
        .store
        .run(move |db| {
            let observation_id: i64 = db.query_row(
                "SELECT observation_id FROM observations WHERE source_stream_id='controller:host-lifecycle' \
                 AND source_event_key=?1 AND kind='host.exit'",
                [format!("terminal:{next_epoch}")],
                |row| row.get(0),
            )?;
            let event = automation_intake::observed_event_by_id(db, observation_id)?
                .expect("completed host exit observation");
            Ok(automation_intake::safe_event_projection(db, &event)?)
        })
        .await
        .expect("project a graceful terminal lifecycle observation");
    assert_eq!(
        completed_projection.status,
        Some(crate::automation::event_rules::EventStatus::Completed)
    );
    assert_eq!(completed_projection.failure_category, None);
    assert_eq!(completed_projection.failed_supervisor, None);
    assert_eq!(
        completed_projection.occurrence_phase.as_deref(),
        Some("host_terminal_exit_observed")
    );
    assert_eq!(
        completed_projection.occurrence_id.as_deref(),
        Some(format!("host-terminal-exit:{next_epoch}").as_str())
    );

    let completion_pass = owner
        .store
        .reconcile_automations_once()
        .await
        .expect("ordinary automation reconciliation admits graceful host completion");
    assert_eq!(
        completion_pass["script_run"]["considered"], 1,
        "{completion_pass}"
    );
    assert_eq!(
        completion_pass["script_run"]["outcomes"][0]["state"],
        "admitted"
    );
    let completion_explanation = explain_terminal_event_trigger(&owner.store, &manager).await;
    let linked = completion_explanation["linked_operation_history"]["items"]
        .as_array()
        .expect("manager-visible completion ScriptRun");
    assert_eq!(linked.len(), 2, "{completion_explanation}");
    let completion_cause = linked
        .iter()
        .map(|link| &link["cause"])
        .find(|cause| cause["event_kind"] == "host.exit" && cause["status"] == "completed")
        .expect("completed host.exit cause");
    assert_eq!(
        completion_cause["occurrence_phase"],
        "host_terminal_exit_observed"
    );
    assert_eq!(
        completion_cause["occurrence_id"],
        completed_projection.occurrence_id.clone().unwrap()
    );
    assert!(completion_cause["failure_category"].is_null());
    assert!(completion_cause["failed_supervisor"].is_null());

    let (runs, started) = owner
        .store
        .run(|db| {
            db.query_row(
                "SELECT COUNT(*),COUNT(started_at_ms) FROM script_runs WHERE script_id=?1",
                [TERMINAL_EVENT_SCRIPT_ID],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
            )
            .map_err(Into::into)
        })
        .await
        .expect("confirm queued admission without interpreter execution");
    assert_eq!(runs, 2);
    assert_eq!(started, 0);

    let preserved_failure = owner
        .store
        .call(
            manager,
            "swarm.exceptions.get".to_owned(),
            json!({"after":0,"limit":32}),
        )
        .await
        .expect("read persistent failure after graceful stop");
    assert_eq!(
        preserved_failure["host_lifecycle"]["latest_failure"]["failure_category"],
        "supervisor_failed"
    );
    owner.close().await.expect("close temporary Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store directory");
}
