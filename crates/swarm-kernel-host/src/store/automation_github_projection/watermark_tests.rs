use super::*;

fn fixture() -> (Connection, AutomationEntry) {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(super::super::SCHEMA).unwrap();
    let mut entry = AutomationEntry::new("manager", "fixture", "labels", 1);
    entry.enabled = true;
    entry.steps = vec![AutomationStep::GithubProjection];
    entry.github_projection = Some(config::GithubProjectionSettings {
        source_id: "fixture".into(),
        label: "accepted".into(),
        present: true,
    });
    (db, entry)
}

fn observe(db: &Connection, stream: &str, kind: &str) {
    db.execute(
        "INSERT INTO observations(source_stream_id,kind,payload_json,recorded_at_ms) \
         VALUES(?1,?2,'{}',1)",
        params![stream, kind],
    )
    .unwrap();
}

#[test]
fn quiet_host_catch_up_covers_global_cut_without_acceptance_facts() {
    let (mut db, entry) = fixture();
    observe(&db, "controller:read-position:operation", "read.position");
    let tx = db.transaction().unwrap();
    let cut = super::super::automation::observation_cut(&tx).unwrap();
    configure_activation(&tx, None, &entry, true, cut, 1).unwrap();
    let result = reconcile_entry(&tx, &entry, 1, 2).unwrap();
    assert_eq!(result["processed"], 0);
    assert_eq!(result["cursor"], cut);
    assert_eq!(result["catch_up_until"], Value::Null);
    let effects: i64 = tx
        .query_row("SELECT count(*) FROM operations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(effects, 0);
}

#[test]
fn bounded_catch_up_preserves_pages_and_defers_facts_after_activation_cut() {
    let (mut db, entry) = fixture();
    for _ in 0..3 {
        observe(&db, ACCEPTANCE_STREAM, "task.acceptance");
    }
    observe(&db, "controller:other", "task.acceptance");
    let tx = db.transaction().unwrap();
    let cut = super::super::automation::observation_cut(&tx).unwrap();
    configure_activation(&tx, None, &entry, true, cut, 1).unwrap();
    observe(&tx, ACCEPTANCE_STREAM, "task.acceptance");

    let no_budget = reconcile_entry(&tx, &entry, 0, 2).unwrap();
    assert_eq!(no_budget["cursor"], 0);
    assert_eq!(no_budget["catch_up_until"], cut);
    for expected_cursor in [1, 2] {
        let page = reconcile_entry(&tx, &entry, 1, 3).unwrap();
        assert_eq!(page["cursor"], expected_cursor);
        assert_eq!(page["catch_up_until"], cut);
    }
    let last_replay_page = reconcile_entry(&tx, &entry, 1, 4).unwrap();
    assert_eq!(last_replay_page["cursor"], cut);
    assert_eq!(last_replay_page["catch_up_until"], Value::Null);
    let live_page = reconcile_entry(&tx, &entry, 1, 5).unwrap();
    assert_eq!(live_page["cursor"], cut + 1);
    assert_eq!(live_page["catch_up_until"], Value::Null);
    let effects: i64 = tx
        .query_row("SELECT count(*) FROM operations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(effects, 0, "unbound facts cannot reserve GitHub effects");
}

/// Classify one retained managed-label Operation through the real decoder and
/// classifier, exercising the exact persisted result shape from
/// `store::github_effects`.
fn classify_label(state: &str, result: Option<Value>) -> Value {
    let retained = ExistingAutomaticOperation::decode(
        "op-label".to_owned(),
        state.to_owned(),
        result.map(|value| value.to_string()),
        matches!(state, "sending" | "native_accepted" | "outcome_unknown").then_some(1),
        matches!(state, "settled" | "rejected" | "cancelled").then_some(2),
    )
    .unwrap();
    json!(super::classify_retained_projection(retained))
}

/// The exact applied receipt `store::github_effects` persists after a confirmed
/// remote readback.
fn confirmed_label_receipt() -> Value {
    json!({
        "operation_id":"op-label",
        "label":"accepted",
        "desired_present":true,
        "observed_present":true,
        "write_attempted":true,
        "readback":"confirmed",
        "outcome":"applied"
    })
}

#[test]
fn cancelled_label_effect_does_not_claim_projection_applied() {
    let classified = classify_label(
        "cancelled",
        Some(json!({"reason":"cancel queued projection","cancelled_by":"cancel-op-label"})),
    );
    assert_eq!(classified["status"], "projection_cancelled");
    assert_eq!(classified["terminal_not_applied"], true);
    assert_eq!(classified["coalesced"], false);
    assert_eq!(classified["effect_started"], false);
}

#[test]
fn rejected_label_effect_retains_explicit_terminal_disposition() {
    let classified = classify_label(
        "rejected",
        Some(json!({"operation_id":"op-label","outcome":"rejected","code":"scope_denied"})),
    );
    assert_eq!(classified["status"], "projection_rejected");
    assert_eq!(classified["terminal_not_applied"], true);
    assert_eq!(classified["coalesced"], false);
}

#[test]
fn settled_no_write_does_not_advance_as_success() {
    // A settled state carrying a no-write outcome is a terminal result that did
    // not apply, so the projection must not be reported as satisfied.
    let classified = classify_label(
        "settled",
        Some(json!({
            "operation_id":"op-label",
            "desired_present":true,
            "observed_present":null,
            "write_attempted":false,
            "readback":"not_confirmed",
            "outcome":"rejected_before_write",
            "error":{"code":"FORBIDDEN","message":"current authority no longer permits this projection"}
        })),
    );
    assert_eq!(classified["status"], "projection_not_applied");
    assert_eq!(classified["terminal_not_applied"], true);
    assert_eq!(classified["coalesced"], false);
}

#[test]
fn settled_confirmed_readback_is_projection_applied() {
    let classified = classify_label("settled", Some(confirmed_label_receipt()));
    assert_eq!(classified["status"], "projection_applied");
    assert_eq!(classified["terminal_not_applied"], false);
    assert_eq!(classified["effect_started"], true);
}

#[test]
fn projection_pending_states_stay_unresolved_without_effect() {
    for state in ["queued", "sending", "native_accepted"] {
        let classified = classify_label(state, None);
        assert_eq!(classified["status"], "projection_in_flight");
        assert_eq!(classified["coalesced"], false);
    }
    let unknown = classify_label("outcome_unknown", None);
    assert_eq!(unknown["status"], "projection_readback_required");
    assert_eq!(unknown["readback_required"], true);
}

#[test]
fn unknown_label_operation_state_is_corruption_not_a_new_state() {
    let error = ExistingAutomaticOperation::decode(
        "op-label".to_owned(),
        "maybe_applied".to_owned(),
        None,
        None,
        None,
    )
    .expect_err("an unknown lifecycle state is retained-data corruption");
    assert_eq!(error.code, "AUTOMATION_OPERATION_CORRUPT");
}

#[test]
fn malformed_label_result_is_not_silently_absent() {
    // A damaged result must be reported as corruption rather than treated as an
    // absent result and classified as a fresh in-flight projection.
    let error = ExistingAutomaticOperation::decode(
        "op-label".to_owned(),
        "settled".to_owned(),
        Some("{not json".to_owned()),
        None,
        Some(2),
    )
    .expect_err("a malformed retained result is corruption");
    assert_eq!(error.code, "AUTOMATION_OPERATION_CORRUPT");
}

/// Insert one retained `github.effect.managed_label` Operation row exactly as
/// the Store persists it, then return its id.
fn insert_label_operation(
    tx: &Connection,
    caller: &str,
    request_id: &str,
    state: &str,
    result: Option<Value>,
) -> String {
    let operation_id = format!("op-{request_id}");
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,?4,'{}','{}',?5,?6,?7,1,1,1)",
        params![
            operation_id,
            caller,
            request_id,
            METHOD,
            state,
            result.map(|value| value.to_string()),
            if state == "settled" { Some(1) } else { None::<i64> },
        ],
    )
    .unwrap();
    operation_id
}

/// A retained cancelled managed-label Operation must not be reported as an
/// applied projection, and the acceptance cursor must not advance past it as
/// though the semantic slot were satisfied.
#[test]
fn retained_cancelled_label_row_does_not_advance_the_cursor() {
    let (mut db, entry) = fixture();
    let tx = db.transaction().unwrap();
    let cut = super::super::automation::observation_cut(&tx).unwrap();
    configure_activation(&tx, None, &entry, true, cut, 1).unwrap();

    insert_label_operation(
        &tx,
        &entry.owner_manager_id,
        "label-cancelled",
        "cancelled",
        Some(json!({
            "label":"accepted",
            "desired_present":true,
            "observed_present":false,
            "readback":"confirmed",
            "outcome":"cancelled_before_effect"
        })),
    );

    // The retained row itself is a terminal not-applied Operation, so the
    // projection domain must classify it truthfully rather than coalescing it.
    let state: String = tx
        .query_row(
            "SELECT state FROM operations WHERE client_request_id='label-cancelled'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "cancelled");

    // Reconciling with no new acceptance facts leaves the cursor at the cut, so
    // a cancelled effect never masquerades as a satisfied projection.
    let page = reconcile_entry(&tx, &entry, 8, 8).unwrap();
    assert_eq!(page["cursor"], cut);
    tx.execute("ROLLBACK", []).unwrap();
}

/// A retained unknown managed-label Operation stays unresolved and never
/// authorizes a replay under the same semantic request ID.
#[test]
fn retained_unknown_label_row_requires_readback_and_does_not_replay() {
    let (mut db, entry) = fixture();
    let tx = db.transaction().unwrap();
    let cut = super::super::automation::observation_cut(&tx).unwrap();
    configure_activation(&tx, None, &entry, true, cut, 1).unwrap();

    let operation_id = insert_label_operation(
        &tx,
        &entry.owner_manager_id,
        "label-unknown",
        "outcome_unknown",
        Some(json!({
            "label":"accepted",
            "desired_present":true,
            "observed_present":null,
            "write_attempted":true,
            "readback":"unavailable",
            "outcome":"desired_state_not_observed_after_write"
        })),
    );

    // Exactly one Operation is retained for this semantic request: no replay
    // or replacement row is created while the outcome is unknown.
    let count: i64 = tx
        .query_row(
            "SELECT count(*) FROM operations WHERE client_request_id='label-unknown'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let retained_state: String = tx
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [&operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_state, "outcome_unknown");

    let page = reconcile_entry(&tx, &entry, 8, 8).unwrap();
    assert_eq!(page["cursor"], cut);
    tx.execute("ROLLBACK", []).unwrap();
}
