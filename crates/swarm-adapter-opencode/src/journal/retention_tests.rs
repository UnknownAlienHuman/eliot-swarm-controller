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

/// Startup recovery stays bounded on a large acknowledged history with a
/// sparse unresolved tail: every inventory call parses at most one window,
/// the persisted startup cursor resumes across restarts, retention
/// converges through bounded retries, and the exact sparse tail survives
/// with ACK truth intact and no native replay.
#[test]
fn startup_recovery_bounds_large_history_and_resumes_sparse_tail() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    for index in 0..100 {
        write_acknowledged_reply(&journal, &format!("startup-bulk-{index:03}"), None);
    }
    let unknown_id = "startup-unknown-intent";
    journal
        .write_intent(&fixture_intent(unknown_id, "agent.reply"))
        .unwrap();
    let pending_id = "startup-pending-outcome";
    journal
        .write_intent(&fixture_intent(pending_id, "agent.reply"))
        .unwrap();
    journal
        .queue_outcome_value(&json!({
            "operation_id": pending_id,
            "outcome": "applied",
            "native_scope_key": NATIVE_SCOPE,
            "details": {}
        }))
        .unwrap();
    let target_id = "startup-reconcile-target";
    write_acknowledged_reply(&journal, target_id, None);
    let reconcile_id = "startup-reconcile-open";
    let mut reconcile_intent = fixture_intent(reconcile_id, "agent.reconcile");
    reconcile_intent.reconcile_target_operation_id = Some(target_id.to_owned());
    journal.write_intent(&reconcile_intent).unwrap();
    let reconcile_outcome = json!({
        "operation_id": reconcile_id,
        "outcome": "applied",
        "native_scope_key": NATIVE_SCOPE,
        "details": {
            "module_receipt": reconcile_intent.module_receipt.clone(),
            "target_operation_id": target_id,
            "completion_condition": "native_readback_recorded",
            "resolved": false,
            "native_replay": false
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

    // First boot classifies exactly one window and keeps the tail pending.
    let boot_counts = journal.startup_scan_counts();
    assert_eq!(boot_counts.operation_bodies_parsed, 0);
    let first = journal.recover_outbox().unwrap();
    let after_first = journal.startup_scan_counts();
    assert_eq!(
        after_first.operation_bodies_parsed - boot_counts.operation_bodies_parsed,
        32
    );
    assert!(first.len() <= 32);
    assert!(!journal.startup_recovery_complete());
    let saved_cursor = journal
        .retention
        .lock()
        .unwrap()
        .startup_cursor_for_test()
        .map(str::to_owned);
    assert!(saved_cursor.is_some());

    // A restart resumes from the persisted position under the same bound.
    drop(journal);
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .startup_cursor_for_test()
            .map(str::to_owned),
        saved_cursor
    );
    let boot_second = journal.startup_scan_counts();
    journal.recover_outbox().unwrap();
    let after_second = journal.startup_scan_counts();
    assert!(after_second.operation_bodies_parsed - boot_second.operation_bodies_parsed <= 32);

    // Retention converges through bounded retries: no call parses more
    // than one window, and the sparse tail is never deleted or replayed.
    for _ in 0..16 {
        let before = journal.startup_scan_counts();
        let summary = journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap();
        let after = journal.startup_scan_counts();
        assert!(
            after.operation_bodies_parsed - before.operation_bodies_parsed <= 32,
            "inventory parses stay within one window per call"
        );
        if summary.queue_drained && journal.startup_recovery_complete() {
            break;
        }
    }
    assert!(journal.startup_recovery_complete());
    let drained = journal
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert!(drained.queue_drained);

    for index in 0..100 {
        assert!(
            !operation_path(&root.0, &format!("startup-bulk-{index:03}")).exists(),
            "acknowledged bulk history is reclaimed"
        );
    }
    assert!(operation_path(&root.0, unknown_id).exists());
    assert!(operation_path(&root.0, pending_id).exists());
    assert!(operation_path(&root.0, target_id).exists());
    assert!(operation_path(&root.0, reconcile_id).exists());
    assert!(journal.load(unknown_id).unwrap().recovery_unknown());
    assert!(!journal.load(unknown_id).unwrap().native_replay_permitted());
    let pending_history = journal.load(pending_id).unwrap();
    assert!(pending_history.outcome.is_some());
    assert!(pending_history.acknowledged_sha256.is_none());
    assert!(!pending_history.native_replay_permitted());
    assert!(
        journal
            .outbox
            .join(format!("outcome-{}.json", operation_key(pending_id)))
            .exists(),
        "the unacked delivery stays pending"
    );
    assert_eq!(
        journal.load(reconcile_id).unwrap().outcome.unwrap()["details"]["resolved"],
        false
    );
    assert!(journal.startup_scan_counts().unresolved_found >= 1);
    // Progress lives only in the shared cursor file: no second index.
    assert!(!root.0.join("startup-cursor.json").exists());
    let cursor_bytes = fs::read(root.0.join("retention-cursor.json")).unwrap();
    let cursor: serde_json::Value = serde_json::from_slice(&cursor_bytes).unwrap();
    assert_eq!(cursor["version"].as_u64(), Some(3));
}

/// Cold legacy inventory is prepared off the service loop; body classification
/// remains windowed, while a write-ahead memo preserves cursor progress and
/// makes an append visible to the later exact-closure pass.
#[test]
fn cold_legacy_before_hello_is_bounded_and_append_resumes() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();

    for index in 0..64 {
        write_acknowledged_reply(&journal, &format!("cold-legacy-{index:03}"), None);
    }
    let unresolved_id = "cold-legacy-unresolved";
    journal
        .write_intent(&fixture_intent(unresolved_id, "agent.reply"))
        .unwrap();
    let address_dir = root.0.join(retention::ADDRESS_CHECKPOINT_DIR);
    for entry in fs::read_dir(&address_dir).unwrap() {
        fs::remove_file(entry.unwrap().path()).unwrap();
    }
    drop(journal);

    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert!(journal.recover_outbox_before_hello().unwrap().is_empty());
    let before_hello = journal.startup_scan_counts();
    assert_eq!(before_hello.operations_listed, 0);
    assert_eq!(before_hello.operation_bodies_parsed, 0);
    assert!(!journal.startup_recovery_complete());
    let targeted = journal.load(unresolved_id).unwrap();
    assert_eq!(
        targeted.intent.as_ref().unwrap().operation_id,
        unresolved_id
    );
    assert!(targeted.recovery_unknown());
    assert!(!targeted.native_replay_permitted());

    // Preparing all names reads metadata only. The service can poll while
    // this work runs; a body window remains capped at 32 entries.
    journal.prepare_startup_inventory().unwrap();
    assert_eq!(journal.startup_scan_counts().operations_listed, 65);
    assert_eq!(journal.startup_scan_counts().operation_bodies_parsed, 0);
    let first = journal.recover_outbox().unwrap();
    assert!(first.len() <= 32);
    assert_eq!(journal.startup_scan_counts().operation_bodies_parsed, 32);
    let saved_cursor = journal
        .retention
        .lock()
        .unwrap()
        .startup_cursor_for_test()
        .map(str::to_owned);
    assert!(saved_cursor.is_some());
    drop(journal);

    // Restart first restores the cursor. An append preserves that traversal
    // position; its existing address memo is read after the legacy census.
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .startup_cursor_for_test()
            .map(str::to_owned),
        saved_cursor
    );
    journal.prepare_startup_inventory().unwrap();
    let appended_id = "cold-legacy-appended-after-restart";
    let cursor_before_append = journal
        .retention
        .lock()
        .unwrap()
        .startup_cursor_for_test()
        .map(str::to_owned);
    journal
        .write_intent(&fixture_intent(appended_id, "agent.reply"))
        .unwrap();
    assert_eq!(
        journal
            .retention
            .lock()
            .unwrap()
            .startup_cursor_for_test()
            .map(str::to_owned),
        cursor_before_append
    );

    let first_after_append = journal.recover_outbox().unwrap();
    assert!(first_after_append.len() <= 32);
    let second_after_append = journal.recover_outbox().unwrap();
    assert!(second_after_append.len() <= 32);
    let final_after_append = journal.recover_outbox().unwrap();
    assert!(final_after_append.len() <= 32);
    assert!(
        journal.startup_recovery_complete(),
        "recovery remains incomplete: scan={:?}, cursor={:?}",
        journal.startup_scan_counts(),
        journal.retention.lock().unwrap().startup_cursor_for_test()
    );
    assert!(
        first_after_append
            .iter()
            .chain(&second_after_append)
            .chain(&final_after_append)
            .any(|state| state.operation_id.as_deref() == Some(appended_id))
    );
    let appended = journal.load(appended_id).unwrap();
    assert!(appended.recovery_unknown());
    assert!(!appended.native_replay_permitted());
    assert!(operation_path(&root.0, appended_id).exists());
}

/// Steady state after convergence and drain: reopening reads only the
/// durable closure with targeted loads. Listed entries stay at zero while
/// bodies parsed equal the exact sparse tail, independent of lifetime
/// history.
#[test]
fn steady_recovery_reads_only_the_sparse_tail() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    for index in 0..100 {
        write_acknowledged_reply(&journal, &format!("steady-bulk-{index:03}"), None);
    }
    let unknown_id = "steady-unknown-intent";
    journal
        .write_intent(&fixture_intent(unknown_id, "agent.reply"))
        .unwrap();
    journal.recover_outbox().unwrap();
    for _ in 0..16 {
        let summary = journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap();
        if summary.queue_drained && journal.startup_recovery_complete() {
            break;
        }
    }
    assert!(journal.startup_recovery_complete());
    drop(journal);

    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let before = journal.startup_scan_counts();
    let states = journal.recover_outbox().unwrap();
    let after = journal.startup_scan_counts();
    assert!(journal.startup_recovery_complete());
    assert_eq!(
        after.operations_listed - before.operations_listed,
        0,
        "steady recovery performs no directory enumeration"
    );
    assert_eq!(
        after.operation_bodies_parsed - before.operation_bodies_parsed,
        1,
        "steady recovery parses exactly the sparse tail"
    );
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].operation_id.as_deref(), Some(unknown_id));
}

/// A durable compact root checkpoint answers hello without reparsing
/// operation bodies, while the pending-bounded outbox scan still runs.
#[test]
fn hello_root_checkpoint_skips_operations_rescan() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let root_id = expected_root_id(BINDING_ID, GENERATION);
    write_acknowledged_reply_with_root(&journal, "hello-root-evidence", &root_id);
    journal.recover_outbox().unwrap();
    let reclaimed = journal
        .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    assert_eq!(reclaimed.namespace_removed, 1);
    assert_eq!(
        journal
            .native_root_checkpoint_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap()
            .as_deref(),
        Some(root_id.as_str())
    );

    for index in 0..100 {
        write_acknowledged_reply(&journal, &format!("hello-bulk-{index:03}"), None);
    }
    let pending_id = "hello-pending-outcome";
    journal
        .write_intent(&fixture_intent(pending_id, "agent.reply"))
        .unwrap();
    journal
        .queue_outcome_value(&json!({
            "operation_id": pending_id,
            "outcome": "applied",
            "native_scope_key": NATIVE_SCOPE,
            "details": {}
        }))
        .unwrap();
    // Converge first so the reopened hello answers from the checkpoint
    // plus the bounded dirty set instead of the lifetime history.
    journal.recover_outbox().unwrap();
    for _ in 0..16 {
        let summary = journal
            .reclaim_acknowledged(BINDING_ID, GENERATION, NATIVE_SCOPE)
            .unwrap();
        if summary.queue_drained && journal.startup_recovery_complete() {
            break;
        }
    }
    assert!(journal.startup_recovery_complete());
    drop(journal);

    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    let before = journal.startup_scan_counts();
    let root_hint = journal
        .native_root_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap();
    let after = journal.startup_scan_counts();
    assert_eq!(root_hint.as_deref(), Some(root_id.as_str()));
    assert_eq!(
        after.operations_listed - before.operations_listed,
        0,
        "hello performs no operations-directory enumeration in steady state"
    );
    assert_eq!(
        after.operation_bodies_parsed - before.operation_bodies_parsed,
        0,
        "hello checks address memos plus outbox, not operation bodies, for conflicts"
    );
    assert_eq!(
        after.outbox_entries_scanned - before.outbox_entries_scanned,
        1,
        "the pending outbox receipt is still consulted"
    );
}

/// A self-consistent but foreign root fails closed at classification and
/// at hello: steady state never binds it, and nothing is deleted around it.
#[test]
fn conflicting_operation_root_fails_closed() {
    let root = FixtureRoot::new();
    let journal =
        Journal::open(&root.0, BINDING_ID, GENERATION, NATIVE_SCOPE, ROUTE_SHA256).unwrap();
    write_acknowledged_reply(&journal, "conflict-healthy", None);
    let mut bad_intent = fixture_intent("conflict-foreign-root", "agent.reply");
    bad_intent.native_root_id =
        Some("ses_swarm_ffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_owned());
    journal.write_intent(&bad_intent).unwrap();
    let bad_outcome = json!({
        "operation_id": "conflict-foreign-root",
        "outcome": "applied",
        "native_scope_key": NATIVE_SCOPE,
        "details": {}
    });
    journal.queue_outcome_value(&bad_outcome).unwrap();
    journal.acknowledge_outcome(&bad_outcome).unwrap();
    journal
        .remove_pending(&journal.outbox.join(format!(
            "outcome-{}.json",
            operation_key("conflict-foreign-root")
        )))
        .unwrap();
    let classification = journal.recover_outbox().unwrap_err();
    assert_eq!(classification.code, "NATIVE_IDENTITY_MISMATCH");
    let hello = journal
        .native_root_for_hello(BINDING_ID, GENERATION, NATIVE_SCOPE)
        .unwrap_err();
    assert_eq!(hello.code, "NATIVE_IDENTITY_MISMATCH");
    assert!(operation_path(&root.0, "conflict-healthy").exists());
    assert!(operation_path(&root.0, "conflict-foreign-root").exists());
}
