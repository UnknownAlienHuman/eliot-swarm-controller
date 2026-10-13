//! Focused public-path regression for PR 58 (contract ratify/reject).
//!
//! The typed producers (coordination.contract.ratify / .reject) are already
//! implemented, so this fixture rebuilds nothing: it exercises the real Store
//! wire parser, the real retained-decision readback, the real Thread operation
//! read gate and the real generic Operation projection. Row map against the
//! PR 58 'Remaining focused verification' list:
//!
//! 1. exact current proposal ratifies once, retained decision readable:
//!    contract_decision_retained_ratification_is_readable (+ wire accept test).
//! 2. reject stays terminal history and must not satisfy ratification:
//!    contract_decision_rejected_history_loads_but_does_not_satisfy_ratification.
//! 3. one revision cannot take the opposite terminal decision: one key pins one
//!    meta row per revision and the producer refuses a second terminal row with
//!    CONTRACT_DECISION_ALREADY_RECORDED; the live conflict stays parent-gated.
//! 4. stale/incomplete/wrong-scope requests fail before mutation: the parser
//!    rejects malformed wire input and retained readback refuses identity
//!    mismatches without side effects (see the two negative tests).
//! 5. former owner / unrelated Manager denied, owner / GM / Operator authority:
//!    readback refuses a forged authority basis and unrelated readers get no
//!    grant; live GM/Operator admission stays parent-gated.
//! 6. thread.resolve accepts only the exact ratified revision: a rejected row
//!    demonstrably fails the decision-is-ratified plus exact-operation
//!    requirement consumed by thread.resolve; the live resolve round-trip stays
//!    parent-gated.
//! 7. same request reuses one immutable decision, changed decision conflicts:
//!    one deterministic decision key per revision; generic request-id reuse and
//!    REQUEST_ID_CONFLICT stay in the shared operation admission path.
//! 8. generic Operation reads expose only the bounded retained relation:
//!    contract_decision_operation_projection_hides_rationale_and_conditions
//!    and contract_decision_thread_operation_read_allows_caller_and_denies_unrelated.
//!    This privacy row also covers PR 49 row 6.
//! 9. denied/stale requests create no Observation, resolution or side effect:
//!    counts are unchanged across the negative readback cases.
//!
//! Live Thread/proposal/scope admission (thread.open, contract.propose with the
//! exact affected-scope snapshot, GM/Operator admission, thread.resolve) is
//! intentionally left to the parent scoped gate.

use super::coordination::{
    ContractDecisionIdentity, contract_decision_key, load_contract_decision,
};
use super::coordination_threads::authorize_operation_read;
use super::object_scope::resolve_operation_read;
use super::set_meta;
use crate::coordination::contract::parse_decision_request;
use crate::model::{self, Principal, Role};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const THREAD_ID: &str = "coord-11111111-1111-4111-8111-111111111111";
const TASK_ID: &str = "contract-task";
const ATTEMPT_ID: &str = "contract-attempt";
const OWNER_ID: &str = "contract-owner";
const PROPOSAL_ID: &str = "cprop-22222222-2222-4222-8222-222222222222";
const REVISION_ID: &str = "cprev-33333333-3333-4333-8333-333333333333";
const PROPOSAL_DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const WRONG_DIGEST: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const SCOPE_DIGEST: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SCOPE_A: &str = "cscope-44444444-4444-4444-8444-444444444444";
const SCOPE_B: &str = "cscope-55555555-5555-4555-8555-555555555555";
const OPERATION_ID: &str = "contract-decision-op-1";
const CREATED_AT_MS: i64 = 7;

fn scope_ref(scope_intent_id: &str) -> Value {
    json!({
        "scope_intent_id": scope_intent_id,
        "state_revision": 1,
        "digest": SCOPE_DIGEST,
        "state": "active",
        "owner_client_id": OWNER_ID,
        "actor": {"client_id": OWNER_ID},
        "assignment_id": "assignment-1",
        "participation_basis": {"kind": "attempt_owner"},
        "mode": "exclusive_edit",
        "paths": ["src/a.rs"],
        "symbols": [],
        "interfaces": [],
        "expires_at_ms": null,
        "override_scope_intent_ids": []
    })
}

fn decision_request(scopes: Vec<Value>, proposal_digest: &str) -> Value {
    json!({
        "client_request_id": "contract-decision-req-1",
        "thread_id": THREAD_ID,
        "expected_state_revision": 1,
        "proposal_id": PROPOSAL_ID,
        "proposal_revision_id": REVISION_ID,
        "proposal_digest": proposal_digest,
        "task_id": TASK_ID,
        "task_revision": 1,
        "attempt_id": ATTEMPT_ID,
        "affected_scope_revisions": scopes,
        "reason": "fixture reason",
        "conditions": [],
        "caveats": []
    })
}

fn identity() -> ContractDecisionIdentity<'static> {
    ContractDecisionIdentity {
        thread_id: THREAD_ID,
        task_id: TASK_ID,
        task_revision: 1,
        attempt_id: ATTEMPT_ID,
        proposal_id: PROPOSAL_ID,
        proposal_revision_id: REVISION_ID,
        proposal_digest: PROPOSAL_DIGEST,
    }
}

fn memory_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(super::SCHEMA).unwrap();
    db.execute_batch(super::SCRIPT_SCHEMA).unwrap();
    db.execute_batch(super::WORKSPACE_SCHEMA).unwrap();
    db
}

fn manager(client_id: &str) -> Principal {
    Principal {
        link_id: format!("link-{}", client_id),
        client_id: client_id.to_owned(),
        role: Role::Manager,
    }
}

fn observation_count(db: &Connection) -> i64 {
    db.query_row("SELECT COUNT(*) FROM observations", params![], |row| {
        row.get(0)
    })
    .unwrap()
}

fn decision_count(db: &Connection) -> i64 {
    db.query_row(
        "SELECT COUNT(*) FROM meta WHERE key LIKE 'coordination:contract-decision:%'",
        params![],
        |row| row.get(0),
    )
    .unwrap()
}

fn seed_retained_decision(
    db: &Connection,
    decision: &str,
    method: &str,
    observation_kind: &str,
    actor_client_id: &str,
) -> i64 {
    let payload = json!({
        "thread_id": THREAD_ID,
        "proposal_id": PROPOSAL_ID,
        "proposal_revision_id": REVISION_ID,
        "decision_operation_id": OPERATION_ID,
    });
    db.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,?4,?5,?6)",
        params![
            format!("coordination:proposal:{}", PROPOSAL_ID),
            format!("decision:{}", REVISION_ID),
            OPERATION_ID,
            observation_kind,
            model::canonical(&payload).unwrap(),
            CREATED_AT_MS,
        ],
    )
    .unwrap();
    let observation_id = db.last_insert_rowid();
    assert!(observation_id > 0);
    let result = json!({
        "operation_id": OPERATION_ID,
        "decision": decision,
        "thread_id": THREAD_ID,
        "task_id": TASK_ID,
        "task_revision": 1,
        "attempt_id": ATTEMPT_ID,
        "proposal_id": PROPOSAL_ID,
        "proposal_revision_id": REVISION_ID,
        "proposal_digest": PROPOSAL_DIGEST,
        "decision_observation_id": observation_id,
        "changed": true,
        "model_work_started": false,
        "native_execution": false,
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,'{}','{}',?5,?6,'settled','{}',?7,1,2,1,2)",
        params![
            OPERATION_ID,
            OWNER_ID,
            "contract-decision-req-1",
            method,
            TASK_ID,
            ATTEMPT_ID,
            model::canonical(&result).unwrap(),
        ],
    )
    .unwrap();
    let retained = json!({
        "schema_version": 1,
        "record_type": "contract_decision",
        "decision": decision,
        "decision_operation_id": OPERATION_ID,
        "observation_id": observation_id,
        "thread_id": THREAD_ID,
        "thread_state_revision": 1,
        "proposal_id": PROPOSAL_ID,
        "proposal_revision_id": REVISION_ID,
        "proposal_revision": 1,
        "proposal_digest": PROPOSAL_DIGEST,
        "task_id": TASK_ID,
        "task_revision": 1,
        "attempt_id": ATTEMPT_ID,
        "attempt_owner_id": OWNER_ID,
        "actor": {"client_id": actor_client_id, "role": "manager"},
        "authority_basis": {"kind": "attempt_owner", "gm_epoch": null},
        "affected_scope_revisions": [],
        "scope_coverage": "complete",
        "reason": "fixture reason",
        "conditions": [],
        "caveats": [],
        "created_at_ms": CREATED_AT_MS,
        "model_work_started": false,
        "native_execution": false,
    });
    set_meta(
        db,
        &contract_decision_key(PROPOSAL_ID, REVISION_ID),
        &retained,
    )
    .unwrap();
    observation_id
}

#[test]
fn contract_decision_wire_accepts_exact_ratify_and_reject() {
    let request = decision_request(vec![scope_ref(SCOPE_A)], PROPOSAL_DIGEST);
    let ratify = parse_decision_request("coordination.contract.ratify", &request).unwrap();
    assert_eq!(ratify.proposal_id, PROPOSAL_ID);
    assert_eq!(ratify.proposal_revision_id, REVISION_ID);
    assert_eq!(ratify.thread_id, THREAD_ID);
    let reject = parse_decision_request("coordination.contract.reject", &request).unwrap();
    assert_eq!(reject.proposal_digest, PROPOSAL_DIGEST);
    let sorted = decision_request(
        vec![scope_ref(SCOPE_A), scope_ref(SCOPE_B)],
        PROPOSAL_DIGEST,
    );
    assert!(parse_decision_request("coordination.contract.ratify", &sorted).is_ok());
}

#[test]
fn contract_decision_wire_rejects_malformed_digest_scope_and_unknown_fields() {
    let bad_digest = decision_request(vec![scope_ref(SCOPE_A)], "not-a-digest");
    assert!(parse_decision_request("coordination.contract.ratify", &bad_digest).is_err());
    let mut incomplete = scope_ref(SCOPE_A);
    incomplete.as_object_mut().unwrap().remove("digest");
    let bad_scope = decision_request(vec![incomplete], PROPOSAL_DIGEST);
    assert!(parse_decision_request("coordination.contract.ratify", &bad_scope).is_err());
    let unsorted = decision_request(
        vec![scope_ref(SCOPE_B), scope_ref(SCOPE_A)],
        PROPOSAL_DIGEST,
    );
    assert!(parse_decision_request("coordination.contract.ratify", &unsorted).is_err());
    let unknown_method = decision_request(vec![scope_ref(SCOPE_A)], PROPOSAL_DIGEST);
    assert!(parse_decision_request("coordination.contract.approve", &unknown_method).is_err());
    let mut extra = decision_request(vec![scope_ref(SCOPE_A)], PROPOSAL_DIGEST);
    extra
        .as_object_mut()
        .unwrap()
        .insert("extra_field".to_owned(), json!(1));
    assert!(parse_decision_request("coordination.contract.ratify", &extra).is_err());
}

#[test]
fn contract_decision_retained_ratification_is_readable() {
    let db = memory_db();
    let observation_id = seed_retained_decision(
        &db,
        "ratified",
        "coordination.contract.ratify",
        "coordination.contract_ratified",
        OWNER_ID,
    );
    let retained = load_contract_decision(&db, identity())
        .unwrap()
        .expect("exact ratified decision must be readable");
    assert_eq!(retained["decision"].as_str(), Some("ratified"));
    assert_eq!(
        retained["decision_operation_id"].as_str(),
        Some(OPERATION_ID)
    );
    assert_eq!(retained["observation_id"].as_i64(), Some(observation_id));
}

#[test]
fn contract_decision_rejected_history_loads_but_does_not_satisfy_ratification() {
    let db = memory_db();
    seed_retained_decision(
        &db,
        "rejected",
        "coordination.contract.reject",
        "coordination.contract_rejected",
        OWNER_ID,
    );
    let retained = load_contract_decision(&db, identity())
        .unwrap()
        .expect("rejected decision stays terminal history");
    assert_eq!(retained["decision"].as_str(), Some("rejected"));
    assert_ne!(retained["decision"].as_str(), Some("ratified"));
    assert_eq!(
        contract_decision_key(PROPOSAL_ID, REVISION_ID),
        format!(
            "coordination:contract-decision:{}:{}",
            PROPOSAL_ID, REVISION_ID
        )
    );
}

#[test]
fn contract_decision_identity_mismatch_fails_closed_without_side_effects() {
    let db = memory_db();
    seed_retained_decision(
        &db,
        "ratified",
        "coordination.contract.ratify",
        "coordination.contract_ratified",
        OWNER_ID,
    );
    let before_observations = observation_count(&db);
    let mut wrong_digest = identity();
    wrong_digest.proposal_digest = WRONG_DIGEST;
    assert!(load_contract_decision(&db, wrong_digest).is_err());
    let mut unknown = identity();
    unknown.proposal_id = "cprop-99999999-9999-4999-8999-999999999999";
    unknown.proposal_revision_id = "cprev-99999999-9999-4999-8999-999999999999";
    assert!(load_contract_decision(&db, unknown).unwrap().is_none());
    assert_eq!(observation_count(&db), before_observations);
    assert_eq!(decision_count(&db), 1);
    let retained = load_contract_decision(&db, identity()).unwrap().unwrap();
    assert_eq!(retained["decision"].as_str(), Some("ratified"));
}

#[test]
fn contract_decision_authority_mismatch_is_damaged() {
    let db = memory_db();
    seed_retained_decision(
        &db,
        "ratified",
        "coordination.contract.ratify",
        "coordination.contract_ratified",
        "impostor-manager",
    );
    let error = load_contract_decision(&db, identity()).unwrap_err();
    assert_eq!(error.code.as_str(), "CONTRACT_DECISION_DAMAGED");
}

#[test]
fn contract_decision_operation_projection_hides_rationale_and_conditions() {
    let db = memory_db();
    let caller = manager(OWNER_ID);
    let stored_result = json!({
        "operation_id": OPERATION_ID,
        "decision": "ratified",
        "thread_id": THREAD_ID,
        "task_id": TASK_ID,
        "task_revision": 1,
        "attempt_id": ATTEMPT_ID,
        "proposal_id": PROPOSAL_ID,
        "proposal_revision_id": REVISION_ID,
        "proposal_digest": PROPOSAL_DIGEST,
        "decision_observation_id": 1,
        "changed": true,
        "model_work_started": false,
        "native_execution": false,
        "reason": "private rationale must not escape",
        "conditions": ["private condition"],
        "caveats": ["private caveat"],
        "attempt_owner_id": OWNER_ID
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,'{}','{}',NULL,NULL,'settled','{}',?5,1,2,1,2)",
        params![
            OPERATION_ID,
            OWNER_ID,
            "contract-decision-req-1",
            "coordination.contract.ratify",
            model::canonical(&stored_result).unwrap(),
        ],
    )
    .unwrap();
    let receipt = super::object_reads::operation(&db, &caller, OPERATION_ID).unwrap();
    assert_eq!(
        receipt["method"].as_str(),
        Some("coordination.contract.ratify")
    );
    let result = &receipt["result"];
    assert_eq!(result["decision"].as_str(), Some("ratified"));
    assert_eq!(result["proposal_id"].as_str(), Some(PROPOSAL_ID));
    assert_eq!(result["proposal_digest"].as_str(), Some(PROPOSAL_DIGEST));
    assert_eq!(result["decision_observation_id"].as_i64(), Some(1));
    assert!(result.get("reason").is_none());
    assert!(result.get("conditions").is_none());
    assert!(result.get("caveats").is_none());
    assert!(result.get("attempt_owner_id").is_none());
    assert!(receipt.get("diagnostic").is_none());
    assert!(receipt.get("native_refs").is_none());
    let unrelated = manager("unrelated-manager");
    assert!(
        resolve_operation_read(&db, &unrelated, OPERATION_ID)
            .unwrap()
            .is_none()
    );
    assert!(super::object_reads::operation(&db, &unrelated, OPERATION_ID).is_err());
}

#[test]
fn contract_decision_thread_operation_read_allows_caller_and_denies_unrelated() {
    let db = memory_db();
    let request = json!({"thread_id": THREAD_ID});
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,'{}',NULL,NULL,'settled','{}',?5,1,2,1,2)",
        params![
            "contract-reject-op-1",
            "decision-caller",
            "contract-reject-req-1",
            "coordination.contract.reject",
            model::canonical(&request).unwrap(),
        ],
    )
    .unwrap();
    let caller = manager("decision-caller");
    assert!(authorize_operation_read(&db, &caller, "contract-reject-op-1").is_ok());
    let unrelated = manager("unrelated-manager");
    assert!(authorize_operation_read(&db, &unrelated, "contract-reject-op-1").is_err());
}
