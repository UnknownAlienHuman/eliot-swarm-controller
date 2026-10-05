//! Store-only qualification for generic ScriptRun events with no Task scope.
//!
//! This fixture enters through `Store::reconcile_automations_once`, which
//! stages the typed host event, admits the normal queued Operation, and keeps
//! the pending cursor. It prepares and verifies a retained bundle but never
//! calls the script worker or launches the configured interpreter.

#[cfg(test)]
mod tests {
    use crate::{
        automation::{authorization, config as automation_config},
        config::Config,
        model::{self, Credential, Principal},
        platform::{DataRoot, bootstrap_credential},
        store::{Store, StoreOwner},
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use rusqlite::{OptionalExtension, TransactionBehavior};
    use serde_json::{Value, json};
    use std::{path::PathBuf, sync::Arc};

    const OWNER_ID: &str = "o6-taskless-owner";
    const PROJECT_ID: &str = "o6-taskless-project";
    const AUTOMATION_ID: &str = "o6-taskless-host-event";
    const SCRIPT_ID: &str = "o6_taskless_event_script";

    async fn start_store(label: &str) -> (StoreOwner, PathBuf, Credential, Principal) {
        let directory =
            std::env::temp_dir().join(format!("swarm-o6-taskless-{label}-{}", model::new_id()));
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
            .authenticate(credential.clone())
            .await
            .expect("authenticate Operator");
        (owner, directory, credential, operator)
    }

    async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
        let token = format!("token-for-{client_id}-{}", model::new_id());
        store
            .call(
                operator.clone(),
                "client.register".to_owned(),
                json!({
                    "client_request_id":format!("register-{client_id}"),
                    "client_id":client_id,
                    "role":"manager",
                    "token_hash":model::digest(token.as_bytes()),
                }),
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

    fn powershell_path() -> PathBuf {
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
            .expect("the existing pwsh executable is needed only for bundle identity checks")
    }

    async fn apply_automation_patch(
        store: &Store,
        manager: &Principal,
        expected_revision: i64,
        patch: Value,
        request_label: &str,
    ) -> Value {
        let changes = json!([{
            "automation_id":AUTOMATION_ID,
            "expected_revision":expected_revision,
            "include_existing":false,
            "patch":patch,
        }]);
        let preview = store
            .call(
                manager.clone(),
                "automation.config.preview".to_owned(),
                json!({"project_id":PROJECT_ID,"changes":changes}),
            )
            .await
            .expect("preview automation configuration");
        assert_eq!(preview["valid"], true, "{preview}");
        store
            .call(
                manager.clone(),
                "automation.config.apply".to_owned(),
                json!({
                    "client_request_id":format!("{request_label}-{}", model::new_id()),
                    "project_id":PROJECT_ID,
                    "changes":changes,
                    "preview_digest":preview["plan_sha256"],
                }),
            )
            .await
            .expect("apply automation configuration")
    }

    async fn record_interruption(store: &Store, previous_epoch: i64, current_epoch: i64) {
        store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                crate::store::set_meta(&tx, "host_epoch", &json!(current_epoch))?;
                crate::store::set_meta(
                    &tx,
                    "host:lifecycle:v1",
                    &json!({
                        "schema_version":1,
                        "host_epoch":previous_epoch,
                        "state":"running",
                        "started_at_ms":1,
                        "updated_at_ms":1,
                    }),
                )?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect("seed prior running host receipt");
        // The production Store path constructs both host.exit and the typed,
        // payload-safe host.interrupted observation from this prior receipt.
        store
            .record_host_start()
            .await
            .expect("record host restart");
    }

    async fn admission_snapshot(store: &Store, owner_id: &str) -> Value {
        let owner_id = owner_id.to_owned();
        store
            .run(move |db| {
                let count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE method='script.run'",
                    [],
                    |row| row.get(0),
                )?;
                let run_count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM script_runs WHERE script_id=?1",
                    [SCRIPT_ID],
                    |row| row.get(0),
                )?;
                let operation: Option<(String, String, String, Option<String>, Option<String>, String, String, String)> = db
                    .query_row(
                        "SELECT operation_id,caller_id,client_request_id,task_id,attempt_id,state,result_json,effective_request_json \
                         FROM operations WHERE method='script.run' ORDER BY created_at_ms,operation_id LIMIT 1",
                        [],
                        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
                    )
                    .optional()?;
                let script_run: Option<(String, Option<String>, Option<i64>, Option<String>, String, String)> =
                    if let Some((operation_id, ..)) = operation.as_ref() {
                        db.query_row(
                            "SELECT run_id,task_id,task_revision,attempt_id,state,spec_json FROM script_runs WHERE operation_id=?1",
                            [operation_id],
                            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
                        ).optional()?
                    } else {
                        None
                    };
                let Some((operation_id,caller_id,request_id,task_id,attempt_id,state,result_json,effective_json)) = operation else {
                    return Ok(json!({"count":count,"script_run_count":run_count,"operation":null,"script_run":null,"link":null,"source":null,"dispatch":null}));
                };
                let link = authorization::operation_link(db, &operation_id)?;
                let cause = link.as_ref().map(|link| link.cause.clone()).unwrap_or(Value::Null);
                let observation_id = cause["observation_id"].as_i64();
                let source = observation_id
                    .map(|id| {
                        db.query_row(
                            "SELECT source_stream_id,kind,operation_id,payload_json FROM observations WHERE observation_id=?1",
                            [id],
                            |row| Ok(json!({"source_stream_id":row.get::<_,String>(0)?,"kind":row.get::<_,String>(1)?,"operation_id":row.get::<_,Option<String>>(2)?,"payload":serde_json::from_str::<Value>(&row.get::<_,String>(3)?).map_err(|error| rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(error)))?})),
                        ).optional()
                    })
                    .transpose()?
                    .flatten()
                    .unwrap_or(Value::Null);
                let key = automation_config::script_dispatch_state_key(
                    &owner_id,
                    PROJECT_ID,
                    AUTOMATION_ID,
                )?;
                let dispatch = automation_config::read_record(db, &key, "ScriptRun trigger cursor")?;
                Ok(json!({
                    "count":count,
                    "script_run_count":run_count,
                    "operation":{
                        "operation_id":operation_id,
                        "caller_id":caller_id,
                        "request_id":request_id,
                        "task_id":task_id,
                        "attempt_id":attempt_id,
                        "state":state,
                        "result":serde_json::from_str::<Value>(&result_json)?,
                        "effective":serde_json::from_str::<Value>(&effective_json)?,
                    },
                    "script_run":script_run.map(|(run_id,task_id,task_revision,attempt_id,state,spec_json)| -> serde_json::Result<Value> {
                        Ok(json!({
                            "run_id":run_id,"task_id":task_id,"task_revision":task_revision,
                            "attempt_id":attempt_id,"state":state,"spec":serde_json::from_str::<Value>(&spec_json)?
                        }))
                    }).transpose()?,
                    "link":link.map(|link| serde_json::to_value(link)).transpose()?,
                    "source":source,
                    "dispatch":dispatch,
                }))
            })
            .await
            .expect("read Store admission, source, retained link and cursor")
    }

    #[tokio::test]
    async fn generic_taskless_safe_event_admits_once_and_cannot_be_forged_by_script_run() {
        let (owner, directory, _credential, operator) = start_store("admission").await;
        let manager = register_manager(&owner.store, &operator, OWNER_ID).await;
        owner
            .store
            .call(
                operator.clone(),
                "gm.handover".to_owned(),
                json!({"client_request_id":"o6-taskless-gm","client_id":OWNER_ID}),
            )
            .await
            .expect("designate the fixture Manager");

        let bundle = json!({
            "script_id":SCRIPT_ID,
            "interpreter_kind":"powershell",
            "interpreter_path":powershell_path(),
            "entrypoint":"main.ps1",
            "argv":[],
            "trust":"trusted_local",
            "inherit_environment":[],
            // A taskless run must shed even a real revision's declared effect.
            "controller_effects":["task_owner_message"],
            "input_schema":{"type":"object","properties":{},"required":[],"additional_properties":true},
            "result_schema":{"type":"null"},
            "files":[{"path":"main.ps1","content_base64":STANDARD.encode(b"Write-Output {}")}],
        });
        owner
            .store
            .call(
                manager.clone(),
                "script.register".to_owned(),
                json!({"client_request_id":"o6-taskless-script-register","bundle":bundle}),
            )
            .await
            .expect("register script bundle without running it");
        owner
            .store
            .call(
                manager.clone(),
                "script.activate".to_owned(),
                json!({
                    "client_request_id":"o6-taskless-script-activate",
                    "script_id":SCRIPT_ID,
                    "revision":1,
                }),
            )
            .await
            .expect("activate retained script revision without running it");

        let applied = apply_automation_patch(
            &owner.store,
            &manager,
            0,
            json!({
                "enabled":true,
                "steps":["script_run"],
                "script_run":{"script_id":SCRIPT_ID},
                "event_rules":[{
                    "source_id":"controller:host-lifecycle",
                    "event_kind":"host.interrupted",
                    "status":"unknown",
                    "action":"script_run",
                }],
            }),
            "o6-taskless-enable",
        )
        .await;
        assert_eq!(applied["applied"], true);

        owner
            .store
            .record_host_start()
            .await
            .expect("record initial host startup");
        owner
            .store
            .record_host_ready()
            .await
            .expect("mark initial Store host ready");
        record_interruption(&owner.store, 1, 2).await;

        let first = owner
            .store
            .reconcile_automations_once()
            .await
            .expect("run normal automation and ScriptRun reconciliation");
        assert_eq!(first["script_run"]["considered"], 1, "{first}");
        assert_eq!(first["script_run"]["outcomes"][0]["state"], "admitted");

        let replay = owner
            .store
            .reconcile_automations_once()
            .await
            .expect("replay the already advanced source cursor");
        assert_eq!(replay["script_run"]["considered"], 0, "{replay}");

        let snapshot = admission_snapshot(&owner.store, OWNER_ID).await;
        assert_eq!(snapshot["count"], 1);
        assert_eq!(snapshot["script_run_count"], 1);
        let operation = &snapshot["operation"];
        assert_eq!(
            operation["caller_id"],
            authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        );
        assert_eq!(operation["state"], "queued");
        assert_eq!(operation["task_id"], Value::Null);
        assert_eq!(operation["attempt_id"], Value::Null);
        assert_eq!(operation["result"]["task_id"], Value::Null);
        assert_eq!(operation["result"]["task_revision"], Value::Null);
        assert_eq!(operation["result"]["attempt_id"], Value::Null);
        assert_eq!(snapshot["script_run"]["state"], "queued");
        assert_eq!(snapshot["script_run"]["task_id"], Value::Null);
        assert_eq!(snapshot["script_run"]["task_revision"], Value::Null);
        assert_eq!(snapshot["script_run"]["attempt_id"], Value::Null);
        for field in ["task_id", "task_revision", "attempt_id"] {
            assert_eq!(
                snapshot["script_run"]["spec"]["invocation"][field],
                Value::Null
            );
        }

        let cause = &snapshot["link"]["cause"];
        assert_eq!(snapshot["link"]["action"], "script.run");
        assert_eq!(snapshot["link"]["effective_manager_id"], OWNER_ID);
        assert_eq!(cause["kind"], "system_event");
        assert_eq!(cause["source_id"], "controller:host-lifecycle");
        assert_eq!(cause["event_kind"], "host.interrupted");
        for field in ["task_id", "task_revision", "attempt_id"] {
            assert!(
                cause.get(field).is_none_or(Value::is_null),
                "cause retained {field}"
            );
        }
        assert_eq!(
            snapshot["source"]["source_stream_id"],
            "controller:host-lifecycle"
        );
        assert_eq!(snapshot["source"]["kind"], "host.interrupted");
        assert_eq!(snapshot["source"]["operation_id"], Value::Null);

        assert_eq!(
            operation["effective"]["script_run"]["controller_effects"],
            json!([])
        );
        assert_eq!(snapshot["script_run"]["spec"]["capabilities"], json!([]));
        assert!(
            snapshot["script_run"]["spec"]["invocation"]
                .get("controller_effects")
                .is_none(),
            "invocation metadata must not carry an alternate effect grant"
        );
        assert_eq!(
            snapshot["script_run"]["spec"]["automation_on_behalf"]["cause"],
            *cause
        );
        assert!(
            snapshot["dispatch"]["pending"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        // The pending intent precedes immutable revision capture at admission.
        // History retains that intent; the Operation link adds script_revision.
        let mut staged_cause = cause.clone();
        staged_cause
            .as_object_mut()
            .unwrap()
            .remove("script_revision");
        assert!(
            snapshot["dispatch"]["recent"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| {
                    item["disposition"] == "admitted"
                        && item["cause"] == staged_cause
                        && item["details"]["operation_id"] == operation["operation_id"]
                })
        );

        let manual = owner
            .store
            .call(
                manager.clone(),
                "script.run".to_owned(),
                json!({
                    "client_request_id":"o6-taskless-manual-missing-scope",
                    "script_id":SCRIPT_ID,
                    "expected_script_revision":1,
                    "input":{"kind":"manual"},
                }),
            )
            .await
            .expect_err("manual ScriptRun without Task/Attempt must be rejected");
        assert_eq!(manual.code, "INVALID_PARAMS");

        let forged = owner
            .store
            .call(
                manager.clone(),
                "script.run".to_owned(),
                json!({
                    "client_request_id":"o6-taskless-forged-event-missing-scope",
                    "script_id":SCRIPT_ID,
                    "expected_script_revision":1,
                    "attempt_id":null,
                    "expected_task_revision":null,
                    "input":{
                        "kind":"system.event",
                        "id":cause["id"],
                        "observation_id":cause["observation_id"],
                        "source_id":cause["source_id"],
                        "event_kind":cause["event_kind"],
                        "task_id":null,
                        "task_revision":null,
                        "attempt_id":null,
                    },
                }),
            )
            .await
            .expect_err("API-forged taskless event context must not mint trigger authority");
        assert_eq!(forged.code, "INVALID_PARAMS");
        assert_eq!(admission_snapshot(&owner.store, OWNER_ID).await["count"], 1);

        // Disable the configured trigger and prove a later event stays outside
        // the admitted Operation ledger.
        let disabled = apply_automation_patch(
            &owner.store,
            &manager,
            1,
            json!({"enabled":false}),
            "o6-taskless-disable",
        )
        .await;
        assert_eq!(disabled["applied"], true);
        owner
            .store
            .record_host_ready()
            .await
            .expect("mark the simulated restarted host ready");
        record_interruption(&owner.store, 2, 3).await;
        owner
            .store
            .reconcile_automations_once()
            .await
            .expect("reconcile while the ScriptRun route is disabled");
        let disabled_snapshot = admission_snapshot(&owner.store, OWNER_ID).await;
        assert_eq!(disabled_snapshot["count"], 1);
        assert_eq!(disabled_snapshot["script_run_count"], 1);
        assert_eq!(
            disabled_snapshot["dispatch"]["pending"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        owner.close().await.expect("close temporary Store");
        std::fs::remove_dir_all(directory).expect("remove owned temporary Store directory");
    }
}
