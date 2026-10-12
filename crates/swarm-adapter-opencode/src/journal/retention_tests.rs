use super::*;
use serde_json::json;
use std::{
    fs,
    path::{Path, PathBuf},
};

const BINDING_ID: &str = "fixture-binding";
const GENERATION: i64 = 1;
const NATIVE_SCOPE: &str = "fixture-scope";
const ROUTE_SHA256: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

struct FixtureRoot(PathBuf);

impl FixtureRoot {
    fn new() -> Self {
        Self(
            std::env::temp_dir().join(format!("swarm-opencode-retention-{}", uuid::Uuid::new_v4())),
        )
    }
}

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn fixture_intent(operation_id: &str, method: &str) -> OperationIntent {
    let module_receipt: swarm_contracts::runtime::ModuleReceiptIdentity =
        serde_json::from_value(json!({
            "schema_version":1,
            "module_id":"eliot.opencode",
            "artifact":{"artifact_id":"fixture-opencode","version":"0.1.0"},
            "protocol":{"major":1,"minor":0},
            "binding_id":BINDING_ID,
            "binding_generation":GENERATION,
            "operation_id":operation_id,
            "input_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        }))
        .unwrap();
    OperationIntent {
        version: 2,
        operation_id: operation_id.to_owned(),
        module_receipt,
        method: method.to_owned(),
        binding_id: BINDING_ID.to_owned(),
        generation: GENERATION,
        native_scope_key: NATIVE_SCOPE.to_owned(),
        native_root_id: None,
        native_input_id: None,
        prompt_sha256: None,
        prompt_bytes: None,
        reconcile_target_operation_id: None,
        result_input_status: None,
        result_assistant: None,
        dispatch_admission: None,
        route_sha256: ROUTE_SHA256.to_owned(),
        model: json!({"id":"fixture-model"}),
        marker: json!({"fixture":"retention"}),
    }
}

fn write_acknowledged_reply(journal: &Journal, operation_id: &str, resolved: Option<bool>) {
    let intent = fixture_intent(operation_id, "agent.reply");
    journal.write_intent(&intent).unwrap();
    let details = resolved.map_or_else(|| json!({}), |value| json!({"resolved":value}));
    let outcome = json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "native_scope_key":NATIVE_SCOPE,
        "details":details
    });
    journal.queue_outcome_value(&outcome).unwrap();
    journal.acknowledge_outcome(&outcome).unwrap();
    journal
        .remove_pending(
            &journal
                .outbox
                .join(format!("outcome-{}.json", operation_key(operation_id))),
        )
        .unwrap();
}

fn operation_path(root: &Path, operation_id: &str) -> PathBuf {
    root.join("operations")
        .join(format!("{}.jsonl", operation_key(operation_id)))
}

#[test]
fn retention_pass_resumes_cursor_after_reopen_and_keeps_unresolved_reconcile_target() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    let target_id = "retention-target-agent-reply";
    write_acknowledged_reply(&journal, target_id, None);

    let reconcile_id = "retention-unresolved-reconcile";
    let mut reconcile_intent = fixture_intent(reconcile_id, "agent.reconcile");
    reconcile_intent.reconcile_target_operation_id = Some(target_id.to_owned());
    journal.write_intent(&reconcile_intent).unwrap();
    let reconcile_outcome = json!({
        "operation_id":reconcile_id,
        "outcome":"applied",
        "native_scope_key":NATIVE_SCOPE,
        "details":{"resolved":false,"native_replay":false}
    });
    journal.queue_outcome_value(&reconcile_outcome).unwrap();
    journal.acknowledge_outcome(&reconcile_outcome).unwrap();
    journal
        .remove_pending(
            &journal
                .outbox
                .join(format!("outcome-{}.json", operation_key(reconcile_id))),
        )
        .unwrap();

    let safe_ids: Vec<String> = (0..34)
        .map(|index| format!("retention-safe-{index:02}"))
        .collect();
    for operation_id in &safe_ids {
        write_acknowledged_reply(&journal, operation_id, None);
    }

    journal.recover_outbox().unwrap();
    let first = journal
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert_eq!(first.examined, 32);
    assert!((31..=32).contains(&first.reclaimed));
    assert!(!first.complete);
    assert!(
        safe_ids
            .iter()
            .any(|id| !operation_path(&root.0, id).exists())
    );
    assert!(operation_path(&root.0, target_id).exists());
    assert!(operation_path(&root.0, reconcile_id).exists());

    let saved_cursor = journal
        .retention
        .lock()
        .unwrap()
        .cursor_for_test()
        .map(str::to_owned);
    assert!(saved_cursor.is_some());
    drop(journal);

    let reopened =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert_eq!(
        reopened
            .retention
            .lock()
            .unwrap()
            .cursor_for_test()
            .map(str::to_owned),
        saved_cursor
    );
    reopened.recover_outbox().unwrap();
    let second = reopened
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert_eq!(second.examined, 35 - first.reclaimed);
    assert_eq!(second.reclaimed, 34 - first.reclaimed);
    assert!(!second.complete);
    assert_eq!(first.reclaimed + second.reclaimed, 34);

    for operation_id in safe_ids {
        assert!(!operation_path(&root.0, &operation_id).exists());
    }
    assert!(operation_path(&root.0, target_id).exists());
    assert!(operation_path(&root.0, reconcile_id).exists());
    assert_eq!(
        reopened.load(reconcile_id).unwrap().outcome.unwrap()["details"]["resolved"],
        false
    );
}
