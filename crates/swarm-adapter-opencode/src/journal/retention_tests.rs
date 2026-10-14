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

fn write_acknowledged_reply_with_root(journal: &Journal, operation_id: &str, root_id: &str) {
    let mut intent = fixture_intent(operation_id, "agent.reply");
    intent.native_root_id = Some(root_id.to_owned());
    journal.write_intent(&intent).unwrap();
    let outcome = json!({
        "operation_id":operation_id,
        "outcome":"applied",
        "native_scope_key":NATIVE_SCOPE,
        "native_root_id":root_id,
        "details":{}
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
        "details":{
            "module_receipt":reconcile_intent.module_receipt.clone(),
            "target_operation_id":target_id,
            "completion_condition":"native_readback_recorded",
            "resolved":false,
            "native_replay":false
        }
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
    assert!((31..=32).contains(&first.namespace_removed));
    assert!(!first.queue_drained);
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
    assert_eq!(second.examined, 35 - first.namespace_removed);
    assert_eq!(second.namespace_removed, 34 - first.namespace_removed);
    assert!(!second.queue_drained);
    assert_eq!(first.namespace_removed + second.namespace_removed, 34);

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

#[test]
fn retention_keeps_unknown_removal_across_empty_pass_and_reopen() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    let operation_id = "retention-unknown-removal";
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, operation_id, &root_id);
    assert!(journal.recover_outbox().unwrap().is_empty());

    let operation_path = operation_path(&root.0, operation_id);
    let outbox_path = journal
        .outbox
        .join(format!("outcome-{}.json", operation_key(operation_id)));
    let acknowledged = journal.load(operation_id).unwrap();
    assert_eq!(
        acknowledged.acknowledged_sha256.as_deref(),
        acknowledged.outcome_sha256.as_deref()
    );
    let first = super::retention::reclaim_acknowledged_with_remover(
        &journal,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        |path| {
            let cursor_bytes = fs::read(root.0.join("retention-cursor.json")).unwrap();
            let cursor: serde_json::Value = serde_json::from_slice(&cursor_bytes).unwrap();
            assert_eq!(cursor["removal_durability_unknown"].as_bool(), Some(true));
            let outcome = swarm_process::remove_private_acknowledged(path)?;
            Ok(swarm_process::RemovalOutcome {
                removed: outcome.removed,
                durability: swarm_process::RemovalDurability::Unknown,
            })
        },
    )
    .unwrap();

    assert_eq!(first.namespace_removed, 1);
    assert!(first.queue_drained);
    assert!(first.durability_unknown);
    assert!(!operation_path.exists());
    assert!(!outbox_path.exists());
    assert_eq!(
        journal
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap()
            .as_deref(),
        Some(root_id.as_str())
    );
    drop(journal);

    let reopened =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert!(
        reopened
            .retention
            .lock()
            .unwrap()
            .removal_durability_unknown_for_test()
    );
    assert!(reopened.recover_outbox().unwrap().is_empty());
    let second = reopened
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();

    assert_eq!(second.examined, 0);
    assert_eq!(second.namespace_removed, 0);
    assert!(second.queue_drained);
    assert!(second.durability_unknown);
    assert!(!operation_path.exists());
    assert!(!outbox_path.exists());
    assert_eq!(
        reopened
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap()
            .as_deref(),
        Some(root_id.as_str())
    );
}

#[test]
fn retention_legacy_or_corrupt_cursor_never_means_confirmed_removal() {
    let legacy_cursor = serde_json::to_vec(&json!({
        "version":1,
        "binding_id":BINDING_ID,
        "generation":GENERATION,
        "native_scope_key":NATIVE_SCOPE,
        "route_sha256":ROUTE_SHA256,
        "cursor":null
    }))
    .unwrap();

    let legacy_root = FixtureRoot::new();
    let journal = Journal::open(
        &legacy_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    drop(journal);
    fs::write(legacy_root.0.join("retention-cursor.json"), legacy_cursor).unwrap();
    let reopened = Journal::open(
        &legacy_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    assert!(
        reopened
            .retention
            .lock()
            .unwrap()
            .removal_durability_unknown_for_test()
    );
    drop(reopened);

    let corrupt_root = FixtureRoot::new();
    let journal = Journal::open(
        &corrupt_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    drop(journal);
    fs::write(corrupt_root.0.join("retention-cursor.json"), b"{not-json").unwrap();
    let reopened = Journal::open(
        &corrupt_root.0,
        BINDING_ID,
        GENERATION,
        NATIVE_SCOPE,
        ROUTE_SHA256,
    )
    .unwrap();
    assert!(
        reopened
            .retention
            .lock()
            .unwrap()
            .removal_durability_unknown_for_test()
    );
}
