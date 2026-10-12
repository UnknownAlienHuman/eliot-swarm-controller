//! Public Store consumer coverage for bounded native terminal metadata.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};

const MODULE_CLIENT_ID: &str = "app144-terminal-metadata-module";
const MODULE_LINK_ID: &str = "app144-terminal-metadata-link";
const BINDING_ID: &str = "app144-terminal-metadata-binding";
const NATIVE_SESSION_ID: &str = "app144-native-session";
const NATIVE_SCOPE_KEY: &str = "app144-native-scope";
const BOOT_ID: &str = "app144-module-boot";
const MODULE_ARTIFACT_ID: &str = "eliot-opencode-v2.rust-http.1@0.5.0";

fn event_ref(id: &str, sequence: i64) -> Value {
    json!({
        "id":id,
        "seq":sequence,
        "sha256":crate::model::digest(id.as_bytes())
    })
}

#[tokio::test]
async fn public_module_observe_bounds_terminal_evidence_metadata() {
    let (owner, directory, _) = start_store("app144-terminal-metadata").await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "app144-terminal-metadata",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let module = Principal {
        link_id: MODULE_LINK_ID.to_owned(),
        client_id: MODULE_CLIENT_ID.to_owned(),
        role: Role::Module,
    };
    let binding_state = json!({
        "connection":"connected",
        "bridge_boot_id":BOOT_ID,
        "module_link_id":MODULE_LINK_ID,
        "module_client_id":MODULE_CLIENT_ID,
        "native_scope_key":NATIVE_SCOPE_KEY,
        "native_root_id":NATIVE_SESSION_ID
    });
    let route = json!({
        "alias":"app144-terminal-metadata",
        "runtime":"opencode",
        "module_artifact_id":MODULE_ARTIFACT_ID,
        "enabled":true,
        "native_options":{}
    });
    let attempt_id = subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,'app144-lane','app144-instance',?2,'ready',?3,?4,?5,?6,?7)",
                params![
                    BINDING_ID,
                    MODULE_ARTIFACT_ID,
                    NATIVE_SCOPE_KEY,
                    NATIVE_SESSION_ID,
                    crate::model::canonical(&route)?,
                    crate::model::canonical(&binding_state)?,
                    crate::model::now_ms()?,
                ],
            )?;
            tx.execute(
                "UPDATE attempts SET state='reserved',binding_id=?2,binding_generation=1 \
                 WHERE attempt_id=?1 AND released_at_ms IS NULL",
                params![attempt_id, BINDING_ID],
            )?;
            tx.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    format!("client:{MODULE_CLIENT_ID}"),
                    crate::model::canonical(&json!({
                        "role":"module",
                        "disabled":false,
                        "binding_id":BINDING_ID,
                        "binding_generation":1
                    }))?,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    let baseline = owner
        .store
        .call(
            module.clone(),
            "module.observe".into(),
            json!({
                "event_id":"app144-baseline",
                "sequence":1,
                "state":{
                    "boot_id":BOOT_ID,
                    "native_root_id":NATIVE_SESSION_ID,
                    "native_scope_key":NATIVE_SCOPE_KEY,
                    "turns":[],
                    "observed_children":[]
                }
            }),
        )
        .await
        .unwrap();
    let baseline_observation_id = baseline["observation_id"]
        .as_i64()
        .expect("public baseline observation returns its committed identity");

    let valid = event_ref("evt-app144-valid", 11);
    let oversized_event = json!({
        "id":"e".repeat(513),
        "seq":12,
        "sha256":crate::model::digest(b"oversized event identity")
    });
    let wrong_type_cursor_event = event_ref("evt-app144-wrong-cursor-type", 14);
    let oversized_cursor_event = event_ref("evt-app144-oversized-cursor", 15);
    let cases = vec![
        ("valid", valid.clone(), json!(11), valid.clone(), json!(11)),
        (
            "oversized_event",
            oversized_event,
            json!(12),
            Value::Null,
            Value::Null,
        ),
        (
            "wrong_type_event",
            json!("untyped native event text"),
            json!(13),
            Value::Null,
            Value::Null,
        ),
        (
            "wrong_type_cursor",
            wrong_type_cursor_event.clone(),
            json!({"cursor":14}),
            wrong_type_cursor_event,
            Value::Null,
        ),
        (
            "oversized_cursor",
            oversized_cursor_event.clone(),
            json!("c".repeat(513)),
            oversized_cursor_event,
            Value::Null,
        ),
    ];
    let producers = cases
        .iter()
        .map(|(label, _, _, _, _)| {
            json!({
                "assignment_id":format!("assignment-{label}"),
                "native_session_id":NATIVE_SESSION_ID,
                "native_run_id":format!("run-{label}"),
                "disposition":"admitted",
                "observed_in":baseline_observation_id
            })
        })
        .collect::<Vec<_>>();
    let turns = cases
        .iter()
        .map(|(label, event, cursor, _, _)| {
            json!({
                "sessionId":NATIVE_SESSION_ID,
                "turnId":format!("run-{label}"),
                "terminal":"failed",
                "event":event,
                "viewCursor":cursor,
                "stage":"execution_failed",
                "error_code":"NATIVE_CHILD_FAILED"
            })
        })
        .collect::<Vec<_>>();
    let attempt_id = subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1 AND released_at_ms IS NULL",
                params![attempt_id, crate::model::canonical(&json!(producers))?],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let recorded = owner
        .store
        .call(
            module,
            "module.observe".into(),
            json!({
                "event_id":"app144-terminal-evidence",
                "sequence":2,
                "state":{
                    "boot_id":BOOT_ID,
                    "native_root_id":NATIVE_SESSION_ID,
                    "native_scope_key":NATIVE_SCOPE_KEY,
                    "turns":turns,
                    "observed_children":[]
                }
            }),
        )
        .await
        .unwrap();
    assert_eq!(recorded["recorded"], true);

    let attempt_id = subject.attempt_id.clone();
    let retained = owner
        .store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT producers_json FROM attempts WHERE attempt_id=?1",
                [&attempt_id],
                |row| row.get(0),
            )?;
            Ok(serde_json::from_str::<Vec<Value>>(&raw)?)
        })
        .await
        .unwrap();
    assert_eq!(retained.len(), cases.len());
    for (index, (label, _, _, expected_event, expected_cursor)) in cases.iter().enumerate() {
        assert_eq!(
            retained[index]["assignment_id"],
            format!("assignment-{label}")
        );
        assert_eq!(retained[index]["native_run_id"], format!("run-{label}"));
        assert_eq!(retained[index]["disposition"], "failed");
        assert_eq!(
            &retained[index]["terminal_evidence"]["event"],
            expected_event
        );
        assert_eq!(
            &retained[index]["terminal_evidence"]["view_cursor"],
            expected_cursor
        );
    }

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
