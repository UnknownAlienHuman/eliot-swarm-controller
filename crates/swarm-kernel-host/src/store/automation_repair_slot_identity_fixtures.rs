use super::*;
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const MANAGER_ID: &str = "manager-slot-fixture";
const BINDING_ID: &str = "binding-slot-fixture";

fn review_identity() -> ReviewSlotIdentity {
    ReviewSlotIdentity {
        task_id: "task-slot-fixture".to_owned(),
        attempt_id: "attempt-slot-fixture".to_owned(),
        task_revision: 3,
        submission_ref: "submission-slot-fixture".to_owned(),
        candidate_ref: "candidate-slot-fixture".to_owned(),
        review_policy_generation: "policy-4".to_owned(),
        review_slot: "primary".to_owned(),
    }
}

fn package(reason: &str) -> ReviewFindingsPackage {
    package_with_result(reason, "review-result-slot-fixture")
}

fn package_with_result(reason: &str, review_result_operation_id: &str) -> ReviewFindingsPackage {
    ReviewFindingsPackage::new(
        review_identity(),
        "assignment-slot-fixture".to_owned(),
        review_result_operation_id.to_owned(),
        vec![ReviewFinding {
            finding_id: "finding-stable-id".to_owned(),
            requirement_ids: vec!["requirement-one".to_owned()],
            reason: reason.to_owned(),
            evidence_refs: vec!["evidence-one".to_owned()],
            requested_change: "apply the retained correction".to_owned(),
        }],
    )
    .expect("fixture package is valid")
}

fn slot_id(package: &ReviewFindingsPackage, schema_version: u32) -> String {
    semantic_slot_id_for_schema(MANAGER_ID, &package.identity, package, schema_version)
        .expect("fixture slot identity is valid")
}

fn request(package: &ReviewFindingsPackage, client_request_id: &str) -> Value {
    json!({
        "client_request_id":client_request_id,
        "binding_id":BINDING_ID,
        "generation":7,
        "delivery":"next_turn",
        "text":crate::automation::repair::render_correction_text(&package.identity, package)
    })
}

fn fixture_db() -> Connection {
    let db = Connection::open_in_memory().expect("open fixture database");
    db.execute_batch(
        "CREATE TABLE meta(key TEXT PRIMARY KEY,value_json TEXT NOT NULL);
         CREATE TABLE operations(
             operation_id TEXT PRIMARY KEY,
             caller_id TEXT NOT NULL,
             method TEXT NOT NULL,
             state TEXT NOT NULL,
             original_request_json TEXT NOT NULL,
             task_id TEXT,
             attempt_id TEXT,
             binding_id TEXT,
             binding_generation INTEGER,
             client_request_id TEXT NOT NULL
         );",
    )
    .expect("create fixture tables");
    db
}

fn seed_direct_slot(
    db: &Connection,
    package: &ReviewFindingsPackage,
    schema_version: u32,
    semantic_slot_id: &str,
    findings_digest: Option<String>,
    finding_id: Option<String>,
    operation_id: &str,
    stored_request_id: &str,
) {
    let original_request = request(package, stored_request_id);
    let original = model::canonical(&original_request).expect("canonical fixture request");
    let parameters_digest = repair_request_digest(&original_request).expect("request digest");
    let receipt = RepairSlotReceipt {
        schema_version,
        semantic_slot_id: semantic_slot_id.to_owned(),
        effective_manager_id: MANAGER_ID.to_owned(),
        task_id: package.identity.task_id.clone(),
        task_revision: package.identity.task_revision,
        attempt_id: package.identity.attempt_id.clone(),
        submission_ref: package.identity.submission_ref.clone(),
        candidate_ref: package.identity.candidate_ref.clone(),
        finding_id,
        findings_digest,
        parameters_digest,
        operation_id: operation_id.to_owned(),
        reserved_at_ms: 100,
        source_attempt_owner_id: None,
        review_assignment_sponsor_id: None,
        decision_manager_id: None,
        transfer_operation_ids: None,
        captured_transfer_gm_epoch: None,
    };
    config::write_record(
        db,
        &slot_key(semantic_slot_id),
        &serde_json::to_value(receipt).expect("serialize fixture receipt"),
    )
    .expect("retain fixture receipt");
    db.execute(
        "INSERT INTO operations(
             operation_id,caller_id,method,state,original_request_json,task_id,attempt_id,
             binding_id,binding_generation,client_request_id
         ) VALUES(?1,?2,'agent.send','settled',?3,?4,?5,?6,7,?7)",
        params![
            operation_id,
            MANAGER_ID,
            original,
            package.identity.task_id,
            package.identity.attempt_id,
            BINDING_ID,
            stored_request_id,
        ],
    )
    .expect("retain fixture Operation");
}

fn direct_slot(package: ReviewFindingsPackage) -> DirectRepairSlot {
    let semantic_slot_id = slot_id(&package, SLOT_SCHEMA_VERSION);
    let request = request(&package, &semantic_slot_id);
    let request_digest = repair_request_digest(&request).expect("current request digest");
    DirectRepairSlot {
        manager_id: MANAGER_ID.to_owned(),
        identity: package.identity.clone(),
        findings_package: package,
        assignment_id: "assignment-slot-fixture".to_owned(),
        result_operation_id: "review-result-slot-fixture".to_owned(),
        disposition_operation_id: "disposition-slot-fixture".to_owned(),
        feedback_operation_id: "feedback-slot-fixture".to_owned(),
        feedback_observation_id: 9,
        binding_id: BINDING_ID.to_owned(),
        binding_generation: 7,
        semantic_slot_id,
        request,
        request_digest,
    }
}

#[test]
fn singleton_retained_field_changes_split_new_digest_slots_but_keep_v1_key() {
    let original = package("the original retained reason");
    let changed = package("the changed retained reason");

    assert_eq!(
        original.findings[0].finding_id,
        changed.findings[0].finding_id
    );
    assert_eq!(
        slot_id(&original, LEGACY_REPAIR_SCHEMA_VERSION),
        slot_id(&changed, LEGACY_REPAIR_SCHEMA_VERSION)
    );
    assert_ne!(
        slot_id(&original, SLOT_SCHEMA_VERSION),
        slot_id(&changed, SLOT_SCHEMA_VERSION)
    );
    assert_eq!(original.semantic_subject_key(), original.findings_digest);
}

#[test]
fn pre_package_singleton_text_reuses_the_immutable_operation_and_rejects_tampering() {
    let package = package("the original retained singleton reason");
    let db = fixture_db();
    let legacy_id = slot_id(&package, LEGACY_REPAIR_SCHEMA_VERSION);
    let operation_id = "operation-before-package-rendering";
    seed_direct_slot(
        &db,
        &package,
        LEGACY_REPAIR_SCHEMA_VERSION,
        &legacy_id,
        None,
        Some(package.findings[0].finding_id.clone()),
        operation_id,
        "retained-singleton-request",
    );
    let mut original = request(&package, "retained-singleton-request");
    // Golden bytes from the pre-package renderer (3fff155), independent of
    // the retained decoder under test.
    let historical_text = "A manager applied this correction request to your current Task Attempt. Keep the same unreleased Attempt, address the exact finding and requirements below, and submit a new candidate linked to the prior submission.\n\nTask: task-slot-fixture\nTask revision: 3\nAttempt: attempt-slot-fixture\nPrior submission: submission-slot-fixture\nPrior candidate: candidate-slot-fixture\nFinding: finding-stable-id\n\nReason:\nthe original retained singleton reason\n\nRequested change:\napply the retained correction\n\nRequirements:\n- requirement-one\n\nEvidence references:\n- evidence-one";
    assert_eq!(
        historical_singleton_text(&package).as_deref(),
        Some(historical_text)
    );
    original["text"] = json!(historical_text);
    let original_json = model::canonical(&original).expect("canonical historical request");
    db.execute(
        "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
        params![operation_id, original_json],
    )
    .expect("retain historical request bytes");
    let key = slot_key(&legacy_id);
    let mut receipt = config::read_record(&db, &key, "fixture slot")
        .expect("read receipt")
        .expect("receipt exists");
    receipt["parameters_digest"] =
        json!(repair_request_digest(&original).expect("historical digest"));
    config::write_record(&db, &key, &receipt).expect("retain exact historical parameters");
    assert!(matches!(
        resolve_direct_slot(&db, &direct_slot(package.clone())).expect("read historical singleton"),
        RepairSlotResolution::Existing { operation_id: retained, .. } if retained == operation_id
    ));

    original["text"] = json!(format!("{}\nchanged", original["text"].as_str().unwrap()));
    db.execute(
        "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
        params![operation_id, model::canonical(&original).unwrap()],
    )
    .expect("inject changed bytes");
    assert_eq!(
        resolve_direct_slot(&db, &direct_slot(package))
            .expect_err("changed immutable text cannot reuse the historical slot")
            .code,
        "REPAIR_SLOT_CORRUPT"
    );
}

#[test]
fn changed_package_on_legacy_singleton_key_is_a_bounded_conflict() {
    let previous = package("the previous retained reason");
    let current = package_with_result("the updated retained reason", "review-result-updated");
    assert_eq!(
        previous.findings[0].finding_id,
        current.findings[0].finding_id
    );
    assert_eq!(
        slot_id(&previous, LEGACY_REPAIR_SCHEMA_VERSION),
        slot_id(&current, LEGACY_REPAIR_SCHEMA_VERSION)
    );
    assert_ne!(
        slot_id(&previous, SLOT_SCHEMA_VERSION),
        slot_id(&current, SLOT_SCHEMA_VERSION)
    );

    let db = fixture_db();
    let legacy_id = slot_id(&previous, LEGACY_REPAIR_SCHEMA_VERSION);
    seed_direct_slot(
        &db,
        &previous,
        LEGACY_REPAIR_SCHEMA_VERSION,
        &legacy_id,
        Some(previous.findings_digest.clone()),
        None,
        "operation-previous-package",
        "retained-v1-request-id",
    );

    let resolution = resolve_direct_slot(&db, &direct_slot(current))
        .expect("different retained package is distinguished from a corrupt slot");
    assert!(matches!(
        resolution,
        RepairSlotResolution::Conflict { operation_id, .. }
            if operation_id == "operation-previous-package"
    ));
}

#[test]
fn exact_v1_slots_with_both_historical_receipt_shapes_coalesce_without_another_operation() {
    let package = package("retained reason");
    for (index, findings_digest, finding_id) in [
        ("without-digest", None, Some("finding-stable-id".to_owned())),
        (
            "intermediate-digest",
            Some(package.findings_digest.clone()),
            None,
        ),
    ] {
        let db = fixture_db();
        let legacy_id = slot_id(&package, LEGACY_REPAIR_SCHEMA_VERSION);
        let operation_id = format!("operation-{index}");
        seed_direct_slot(
            &db,
            &package,
            LEGACY_REPAIR_SCHEMA_VERSION,
            &legacy_id,
            findings_digest,
            finding_id,
            &operation_id,
            "retained-v1-request-id",
        );
        let before: i64 = db
            .query_row("SELECT count(*) FROM operations", [], |row| row.get(0))
            .expect("count initial operations");

        let resolution = resolve_direct_slot(&db, &direct_slot(package.clone()))
            .expect("exact retained v1 slot resolves");

        assert!(matches!(
            resolution,
            RepairSlotResolution::Existing { operation_id: found, .. } if found == operation_id
        ));
        let after: i64 = db
            .query_row("SELECT count(*) FROM operations", [], |row| row.get(0))
            .expect("count final operations");
        assert_eq!(before, after);
    }
}

#[test]
fn forged_digest_and_mixed_version_slot_identity_are_denied() {
    let package = package("retained reason");
    let legacy_id = slot_id(&package, LEGACY_REPAIR_SCHEMA_VERSION);
    let db = fixture_db();
    seed_direct_slot(
        &db,
        &package,
        LEGACY_REPAIR_SCHEMA_VERSION,
        &legacy_id,
        Some("forged-package-digest".to_owned()),
        Some("finding-stable-id".to_owned()),
        "operation-forged-digest",
        "retained-v1-request-id",
    );
    let error = resolve_direct_slot(&db, &direct_slot(package.clone()))
        .expect_err("a finding ID cannot mask a forged retained digest");
    assert_eq!(error.code, "REPAIR_SLOT_CORRUPT");

    let db = fixture_db();
    seed_direct_slot(
        &db,
        &package,
        LEGACY_REPAIR_SCHEMA_VERSION,
        &legacy_id,
        Some("forged-package-digest-only".to_owned()),
        None,
        "operation-forged-digest-only",
        "retained-v1-request-id",
    );
    let error = resolve_direct_slot(&db, &direct_slot(package.clone()))
        .expect_err("a forged digest that disagrees with immutable Operation text is denied");
    assert_eq!(error.code, "REPAIR_SLOT_CORRUPT");

    let db = fixture_db();
    seed_direct_slot(
        &db,
        &package,
        SLOT_SCHEMA_VERSION,
        &legacy_id,
        Some(package.findings_digest.clone()),
        None,
        "operation-mixed-version",
        "retained-v2-request-id",
    );
    let error = resolve_direct_slot(&db, &direct_slot(package))
        .expect_err("a v2 receipt cannot occupy the v1 finding-ID key");
    assert_eq!(error.code, "REPAIR_SLOT_CORRUPT");
    assert!(!slot_schema_matches_link(
        LINK_SCHEMA_VERSION,
        LEGACY_REPAIR_SCHEMA_VERSION
    ));
}

#[test]
fn legacy_slot_with_forged_operation_binding_is_denied() {
    let package = package("retained reason");
    let legacy_id = slot_id(&package, LEGACY_REPAIR_SCHEMA_VERSION);
    let db = fixture_db();
    seed_direct_slot(
        &db,
        &package,
        LEGACY_REPAIR_SCHEMA_VERSION,
        &legacy_id,
        None,
        Some("finding-stable-id".to_owned()),
        "operation-forged-binding",
        "retained-v1-request-id",
    );
    db.execute(
        "UPDATE operations SET binding_id='forged-binding' WHERE operation_id=?1",
        ["operation-forged-binding"],
    )
    .expect("forge retained Operation binding");

    let error = resolve_direct_slot(&db, &direct_slot(package))
        .expect_err("Operation binding mismatch is not reduced to a benign conflict");
    assert_eq!(error.code, "REPAIR_SLOT_CORRUPT");
}

#[test]
fn simultaneous_digest_and_legacy_slots_are_explicitly_ambiguous() {
    let package = package("retained reason");
    let current_id = slot_id(&package, SLOT_SCHEMA_VERSION);
    let legacy_id = slot_id(&package, LEGACY_REPAIR_SCHEMA_VERSION);
    assert_ne!(current_id, legacy_id);
    let db = fixture_db();
    seed_direct_slot(
        &db,
        &package,
        SLOT_SCHEMA_VERSION,
        &current_id,
        Some(package.findings_digest.clone()),
        None,
        "operation-current",
        "retained-v2-request-id",
    );
    seed_direct_slot(
        &db,
        &package,
        LEGACY_REPAIR_SCHEMA_VERSION,
        &legacy_id,
        None,
        Some("finding-stable-id".to_owned()),
        "operation-legacy",
        "retained-v1-request-id",
    );

    let error = resolve_direct_slot(&db, &direct_slot(package))
        .expect_err("two retained identities require explicit readback");
    assert_eq!(error.code, "REPAIR_SLOT_AMBIGUOUS");
}
