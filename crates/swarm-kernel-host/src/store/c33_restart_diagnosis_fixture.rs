//! Regression fixture for a native-MCP launch held during Store restart recovery.
//! It uses only Store/SQLite state; it never starts OpenCode or calls the provider.

#[cfg(test)]
mod tests {
    use crate::{
        config::Config,
        error::Result,
        model::{self, Credential, Principal},
        platform::{DataRoot, bootstrap_credential},
        store::{Store, StoreOwner},
    };
    use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
    use serde_json::{Value, json};
    use std::{path::Path, sync::Arc};

    const STALE_OPERATION: &str = "c33-a-stale-launch";
    const NEIGHBOR_OPERATION: &str = "c33-z-observed-neighbor";
    const RECORD_PREFIX: &str = "launcher:native_mcp_tools:v1:";
    const SUPERVISOR_PREFIX: &str = "launcher:native_mcp_tools:supervisor:v1:";
    const CURSOR_KEY: &str = "launcher:native_mcp_tools:supervisor:v1:cursor";

    async fn start_store(
        directory: &Path,
        existing_credential: Option<Credential>,
    ) -> (StoreOwner, Credential, Principal) {
        let root = DataRoot::acquire(directory).expect("acquire temporary Store root");
        let credential = existing_credential.unwrap_or_else(|| {
            bootstrap_credential(&root.path).expect("create Operator credential")
        });
        let mut config = Config::default();
        config.storage.data_dir = directory.to_path_buf();
        let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
            .await
            .expect("start Store owner");
        let operator = owner
            .store
            .authenticate(credential.clone())
            .await
            .expect("authenticate local Operator");
        (owner, credential, operator)
    }

    fn private_key(prefix: &str, operation_id: &str) -> String {
        format!("{prefix}{}", model::digest(operation_id.as_bytes()))
    }

    fn digest(label: &str) -> String {
        format!("sha256:{}", model::digest(label.as_bytes()))
    }

    fn retained_record(operation_id: &str, identity_digest: &str, observed: bool) -> Value {
        let assignment = json!({
            "task_id":format!("task-{operation_id}"),
            "task_revision":1,
            "attempt_id":format!("attempt-{operation_id}"),
            "binding_id":format!("binding-{operation_id}"),
            "binding_generation":1,
            "native_session_id":"ses_c33_fixture",
            "participant_id":"fixture-participant",
            "mcp_profile":"participant",
            "grant_revision":1,
            "participation_basis":"attempt_owner",
            "assignment_id":null,
            "review_assignment_id":null,
        });
        let location = "C:/fixture/native-service";
        let location_digest = digest(location);
        json!({
            "schema_version":1,
            "kind":"launcher_native_mcp_tools",
            "operation_id":operation_id,
            "launch_identity_digest":identity_digest,
            "assignment":assignment,
            "service":{
                "id":"fixture-native-service",
                "pid":4312,
                "version":"2.0.7",
                "directory_sha256":location_digest,
            },
            "install":{
                "state":"registered",
                "intent":{
                    "service_id":"fixture-native-service",
                    "location_sha256":location_digest,
                    "assignment":assignment,
                },
            },
            "challenge":{
                "state":if observed {"observed"} else {"not_started"},
                "metadata":if observed {
                    json!({
                        "schema":"opencode-v2-native-mcp-challenge-v1",
                        "challenge_id":format!("retained-challenge-{operation_id}"),
                        "issued_at_ms":100,
                        "expires_at_ms":9999999999999i64,
                        "assignment":assignment,
                        "service_id":"fixture-native-service",
                        "service_pid":4312,
                        "service_version":"2.0.7",
                        "directory":location,
                    })
                } else {Value::Null},
                "replaced_metadata":[],
                "replacement_archive":{"count":0,"digest":null},
            },
            "tools_readback":null,
            "last_error":null,
            "dispatch_permitted":false,
        })
    }

    fn retained_schedule(
        operation_id: &str,
        identity_digest: &str,
        now: i64,
        state: &str,
        failures: i64,
    ) -> Value {
        let running = state == "running";
        json!({
            "schema_version":1,
            "kind":"launcher_native_mcp_tools_supervisor",
            "operation_id":operation_id,
            "launch_identity_digest":identity_digest,
            "state":state,
            "claim_generation":if running {4} else {0},
            "started_at_ms":if running {json!(now.saturating_sub(1))} else {Value::Null},
            "next_retry_at_ms":if state == "ready" {json!(now)} else {Value::Null},
            "finished_at_ms":if running {Value::Null} else {json!(now.saturating_sub(1))},
            "failure_attempts":failures,
            "last_error_code":null,
        })
    }

    async fn seed_launch(
        store: &Store,
        operator: &Principal,
        operation_id: &'static str,
        digest: String,
        observed: bool,
        schedule_state: &'static str,
        failures: i64,
    ) -> Result<String> {
        let operation_id = operation_id.to_owned();
        let principal = operator.clone();
        let now = model::now_ms()?;
        let record = retained_record(&operation_id, &digest, observed);
        let schedule = retained_schedule(&operation_id, &digest, now, schedule_state, failures);
        let record_raw = model::canonical(&record)?;
        store.run(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let task_id = format!("task-{operation_id}");
            let attempt_id = format!("attempt-{operation_id}");
            let binding_id = format!("binding-{operation_id}");
            let manifest = json!({
                "launch_manifest":{
                    "state":"awaiting_native_mcp",
                    "runtime":{"dispatch_permitted":false},
                    "progress":{"task_dispatch":"not_started"},
                    "native_mcp_readback":{"state":"observed_partial"},
                    "actor":{
                        "kind":"direct",
                        "client_id":principal.client_id,
                        "role":"operator",
                        "link_id":principal.link_id,
                    },
                    "task":{
                        "task_id":task_id,
                        "observed_revision":1,
                        "attempt_id":attempt_id,
                        "project_id":"c33-fixture-project",
                    },
                    "binding":{"binding_id":binding_id,"generation":1},
                },
            });
            let request = model::canonical(&json!({"client_request_id":format!("fixture-{operation_id}")}))?;
            tx.execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
                 VALUES(?1,'c33-fixture-project',1,'open','{}',?2,?2)",
                params![task_id, now],
            )?;
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,?2,'fixture-module-instance','fixture-module-artifact','ready','{}','{}',?3)",
                params![binding_id, format!("fixture-lane-{operation_id}"), now],
            )?;
            tx.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,1,'{}',?3,'controller',?4,1,'reserved',?5,?5)",
                params![attempt_id, task_id, principal.client_id, binding_id, now],
            )?;
            tx.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) \
                 VALUES(?1,?2,?3,'swarm.launch',?4,?5,?6,?7,?8,1,'queued',?9,?9,?9)",
                params![
                    operation_id,
                    principal.client_id,
                    format!("fixture-request-{operation_id}"),
                    request,
                    model::canonical(&manifest)?,
                    task_id,
                    attempt_id,
                    binding_id,
                    now,
                ],
            )?;
            super::super::set_meta(&tx, &private_key(RECORD_PREFIX, &operation_id), &record)?;
            super::super::set_meta(&tx, &private_key(SUPERVISOR_PREFIX, &operation_id), &schedule)?;
            tx.commit()?;
            Ok(record_raw)
        }).await
    }

    fn read_meta_raw(db: &Connection, key: &str) -> Result<Option<String>> {
        Ok(db
            .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
                row.get(0)
            })
            .optional()?)
    }

    #[tokio::test]
    async fn restart_defers_stale_launch_locally_and_preserves_neighbor_proof() {
        let directory = std::env::temp_dir().join(format!("swarm-c33-restart-{}", model::new_id()));
        std::fs::create_dir_all(&directory).expect("create temporary Store directory");
        let (owner, credential, operator) = start_store(&directory, None).await;
        let stale_digest = digest("stale-launch-identity");
        let neighbor_digest = digest("observed-neighbor-identity");
        seed_launch(
            &owner.store,
            &operator,
            STALE_OPERATION,
            stale_digest,
            false,
            "ready",
            0,
        )
        .await
        .expect("seed exact queued stale launch row");
        let neighbor_before = seed_launch(
            &owner.store,
            &operator,
            NEIGHBOR_OPERATION,
            neighbor_digest,
            true,
            "running",
            2,
        )
        .await
        .expect("seed queued neighbor with retained observed C8 challenge");
        owner
            .store
            .record_host_start()
            .await
            .expect("record initial host start");
        owner
            .store
            .record_host_ready()
            .await
            .expect("mark initial host ready");
        owner.close().await.expect("close first Store owner");

        let (reopened, _, restarted_operator) = start_store(&directory, Some(credential)).await;
        assert_eq!(restarted_operator.client_id, operator.client_id);
        let restarted_binding_states = reopened.store.run(move |db| {
            let count: i64 = db.query_row(
                "SELECT COUNT(*) FROM bindings WHERE state='reconciling' AND released_at_ms IS NULL",
                [],
                |row| row.get(0),
            )?;
            Ok(count)
        }).await.expect("read startup-reconciling bindings");
        assert_eq!(restarted_binding_states, 2);
        reopened
            .store
            .record_host_start()
            .await
            .expect("record restarted host start");
        reopened
            .store
            .record_host_ready()
            .await
            .expect("mark restarted Store ready");

        let tick = reopened
            .store
            .reconcile_native_mcp_tools_once()
            .await
            .expect("one startup C8 tick must defer stale launches instead of killing the host");
        assert_eq!(
            tick["state"], "idle",
            "no native call may be claimed while the binding is reconciling"
        );
        assert!(tick["next_retry_at_ms"].as_i64().is_some());

        let operation_ids = vec![STALE_OPERATION.to_owned(), NEIGHBOR_OPERATION.to_owned()];
        let after = reopened.store.run(move |db| {
            let mut schedules = Vec::new();
            let mut records = Vec::new();
            let mut manifests = Vec::new();
            for operation_id in &operation_ids {
                let schedule_raw = read_meta_raw(db, &private_key(SUPERVISOR_PREFIX, operation_id))?
                    .ok_or_else(|| crate::error::Error::new("STORE_ERROR", "fixture schedule missing"))?;
                let record_raw = read_meta_raw(db, &private_key(RECORD_PREFIX, operation_id))?
                    .ok_or_else(|| crate::error::Error::new("STORE_ERROR", "fixture record missing"))?;
                let effective: String = db.query_row(
                    "SELECT effective_request_json FROM operations WHERE operation_id=?1 AND state='queued'",
                    [operation_id],
                    |row| row.get(0),
                )?;
                schedules.push(serde_json::from_str::<Value>(&schedule_raw)?);
                records.push(json!({"raw":record_raw,"value":serde_json::from_str::<Value>(&record_raw)?}));
                manifests.push(serde_json::from_str::<Value>(&effective)?);
            }
            let cursor_raw = read_meta_raw(db, CURSOR_KEY)?
                .ok_or_else(|| crate::error::Error::new("STORE_ERROR", "fixture cursor missing"))?;
            let cursor = serde_json::from_str::<Value>(&cursor_raw)?;
            let lifecycle = super::super::meta(db, "host:lifecycle:v1")?.unwrap_or(Value::Null);
            Ok(json!({"schedules":schedules,"records":records,"manifests":manifests,"cursor":cursor,"lifecycle":lifecycle}))
        }).await.expect("read durable C8 deferrals");
        assert_eq!(
            after["cursor"], NEIGHBOR_OPERATION,
            "the scan must continue to the later candidate"
        );
        assert_eq!(
            after["lifecycle"]["state"], "running",
            "the Store lifecycle must remain healthy"
        );
        for (index, operation_id) in [STALE_OPERATION, NEIGHBOR_OPERATION].iter().enumerate() {
            assert_eq!(
                after["schedules"][index]["state"], "retry_wait",
                "{operation_id}"
            );
            assert_eq!(after["schedules"][index]["last_error_code"], "STALE_LAUNCH");
            assert!(
                after["schedules"][index]["next_retry_at_ms"]
                    .as_i64()
                    .unwrap()
                    > model::now_ms().unwrap()
            );
            assert_eq!(
                after["manifests"][index]["launch_manifest"]["runtime"]["dispatch_permitted"],
                false
            );
            assert_eq!(
                after["manifests"][index]["launch_manifest"]["progress"]["task_dispatch"],
                "not_started"
            );
        }
        assert_eq!(
            after["records"][1]["raw"].as_str(),
            Some(neighbor_before.as_str()),
            "retained challenge receipt bytes must not change during startup hold"
        );
        assert_eq!(
            after["records"][1]["value"]["challenge"]["metadata"]["challenge_id"],
            format!("retained-challenge-{NEIGHBOR_OPERATION}")
        );

        // A fast second pass must honor the persisted deadline instead of
        // rewriting the backoff every two-second supervisor tick.
        reopened
            .store
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                super::super::set_meta(&tx, CURSOR_KEY, &json!(""))?;
                tx.commit()?;
                Ok(())
            })
            .await
            .expect("rewind bounded candidate cursor for immediate-poll check");
        let retry_tick = reopened
            .store
            .reconcile_native_mcp_tools_once()
            .await
            .expect("deferred entries are skipped until their durable deadline");
        assert_eq!(retry_tick["state"], "idle");
        assert_eq!(retry_tick["next_retry_at_ms"], tick["next_retry_at_ms"]);

        // Store-level SQLite failures remain fatal rather than being folded
        // into the per-launch stale-scope outcome.
        reopened.store.run(move |db| {
            db.execute_batch("CREATE TRIGGER fixture_fail_c8_cursor BEFORE UPDATE OF value_json ON meta WHEN OLD.key='launcher:native_mcp_tools:supervisor:v1:cursor' BEGIN SELECT RAISE(ABORT,'fixture sqlite failure'); END;")?;
            Ok(())
        }).await.expect("install isolated failure trigger");
        let sqlite_error = reopened
            .store
            .reconcile_native_mcp_tools_once()
            .await
            .expect_err("SQLite failure while advancing the cursor must propagate");
        assert_eq!(sqlite_error.code, "STORE_ERROR");
        reopened.close().await.expect("close restarted Store owner");
        std::fs::remove_dir_all(&directory).expect("remove the exact fixture Store directory");
    }
}
