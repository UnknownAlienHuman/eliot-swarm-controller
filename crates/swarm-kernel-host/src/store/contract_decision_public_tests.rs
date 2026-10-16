//! PR58 contract decisions: preserved private unit fixtures plus authenticated
//! StoreOwner regressions in `public_store`. Only the latter author the public
//! nine-row matrix. All authoritative setup there uses Store.call; its sole SQL
//! mutation injects a missing scope record after public scope admission.
//!
//! Public row map: ratify/replay/resolve (1,3,6,7,9), reject history (2,3,6,7,9),
//! stale identity/scope and missing coverage (4,9), current/denied authority
//! (5,9), bounded generic Operation get/list/delta (8, also PR49 row 6).
//! Test execution and compiler qualification remain the parent's responsibility.

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
    // Seed the referenced Task and Attempt before the Operation. This private
    // SQL fixture has no Store lifecycle to create them.
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,?1,1,'open','{}',1,1)",
        params![TASK_ID],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,created_at_ms,updated_at_ms) VALUES(?1,?2,1,'{}',?3,'controller','reserved',1,1)",
        params![ATTEMPT_ID, TASK_ID, OWNER_ID],
    )
    .unwrap();
    // observations.operation_id is a foreign key. Create its parent in a
    // nonterminal state, then settle it with the result that names the child.
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,'{}','{}',?5,?6,'queued','{}',NULL,1,NULL,1,1)",
        params![
            OPERATION_ID,
            OWNER_ID,
            "contract-decision-req-1",
            method,
            TASK_ID,
            ATTEMPT_ID,
        ],
    )
    .unwrap();
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
    assert_eq!(
        db.execute(
            "UPDATE operations SET state='settled',result_json=?1,settled_at_ms=2,updated_at_ms=2 WHERE operation_id=?2 AND state='queued'",
            params![model::canonical(&result).unwrap(), OPERATION_ID],
        )
        .unwrap(),
        1
    );
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

mod public_store {
    use super::super::{Store, StoreOwner};
    use crate::{
        config::Config,
        error::Result,
        model::{self, Credential, Principal, Role},
        platform::{DataRoot, bootstrap_credential},
    };
    use serde_json::{Value, json};
    use std::{path::PathBuf, sync::Arc};

    const PRIVATE_REASON: &str = "PRIVATE_RATIONALE_public_decision";
    const PRIVATE_CONDITION: &str = "PRIVATE_CONDITION_public_decision";
    const PRIVATE_CAVEAT: &str = "PRIVATE_CAVEAT_public_decision";

    // The existing program_tests start_store pattern, with public authentication
    // and admission throughout. No seed_clients/seed_subject SQL facts are used.
    struct Fixture {
        owner: StoreOwner,
        directory: PathBuf,
        operator: Principal,
        manager: Principal,
        former_owner: Principal,
        gm: Principal,
        unrelated: Principal,
        observer: Principal,
        participant: Principal,
        gm_epoch: Value,
        task_id: Value,
        attempt_id: Value,
        former_attempt_id: Value,
        thread_id: Value,
        proposal: Value,
        scope_ref: Value,
    }

    async fn register(
        store: &Store,
        operator: &Principal,
        client_id: &str,
        role: &str,
    ) -> Principal {
        let token = format!("{}{}", model::new_id(), model::new_id());
        store
            .call(
                operator.clone(),
                "client.register".into(),
                json!({
                    "client_request_id":model::new_id(), "client_id":client_id,
                    "role":role, "token_hash":model::digest(token.as_bytes()),
                }),
            )
            .await
            .unwrap();
        store
            .authenticate(Credential {
                client_id: client_id.into(),
                token,
            })
            .await
            .unwrap()
    }

    fn proposal_request(thread_id: &Value, supersedes: Value) -> Value {
        json!({
            "client_request_id":model::new_id(), "thread_id":thread_id,
            "supersedes_revision_id":supersedes, "topic":"exact public contract",
            "affected":{"paths":["src/contract.rs"], "symbols":[], "schemas":[]},
            "statement":{
                "producer":"typed decision producer", "consumer":"Thread resolution",
                "identity":"exact proposal revision and digest", "payload":"bounded relation",
                "observation_boundary":"immutable decision Observation",
                "failure_semantics":"deny before decision mutation", "versioning":"revision pinned",
            },
            "acceptance_conditions":[], "claims":[], "open_questions":[],
        })
    }

    impl Fixture {
        async fn new(label: &str) -> Self {
            let directory = std::env::temp_dir()
                .join(format!("swarm-contract-public-{label}-{}", model::new_id()));
            std::fs::create_dir_all(&directory).unwrap();
            let root = DataRoot::acquire(&directory).unwrap();
            let credential = bootstrap_credential(&root.path).unwrap();
            let mut config = Config::default();
            config.storage.data_dir = root.path.clone();
            let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
                .await
                .unwrap();
            let store = &owner.store;
            let operator = store.authenticate(credential).await.unwrap();
            assert_eq!(operator.role, Role::Operator);
            let manager = register(store, &operator, "decision-owner", "manager").await;
            let former_owner = register(store, &operator, "former-decision-owner", "manager").await;
            let gm = register(store, &operator, "decision-gm", "manager").await;
            let unrelated =
                register(store, &operator, "unrelated-decision-manager", "manager").await;
            let observer =
                register(store, &operator, "unrelated-decision-observer", "observer").await;
            let handover = store
                .call(
                    operator.clone(),
                    "gm.handover".into(),
                    json!({
                        "client_request_id":model::new_id(), "client_id":gm.client_id,
                    }),
                )
                .await
                .unwrap();
            let created = store.call(operator.clone(), "task.create".into(), json!({
                "client_request_id":model::new_id(), "project_id":"public-contract-matrix",
                "spec":{"objective":"Qualify exact public contract decisions", "phase":"implementation",
                    "owner_policy_id":crate::policy::OWNER_POLICY_V1_ID,
                    "requirements":[{"id":"R58", "statement":"one immutable scoped decision"}]},
            })).await.unwrap();
            let task_id = created["task_id"].clone();
            // Make former_owner an actual prior Task owner through public release
            // and claim, rather than a label on an invented principal.
            let former = store.call(former_owner.clone(), "task.claim".into(), json!({
                "client_request_id":model::new_id(), "task_id":task_id, "expected_revision":1,
            })).await.unwrap();
            let former_attempt_id = former["attempt_id"].clone();
            store.call(former_owner.clone(), "attempt.release".into(), json!({
                "client_request_id":model::new_id(), "attempt_id":former_attempt_id,
                "outcome":"cancelled", "reason":"public ownership transfer", "assignment_closed":true,
            })).await.unwrap();
            let claimed = store.call(manager.clone(), "task.claim".into(), json!({
                "client_request_id":model::new_id(), "task_id":task_id, "expected_revision":1,
            })).await.unwrap();
            let attempt_id = claimed["attempt_id"].clone();
            let token = format!("{}{}", model::new_id(), model::new_id());
            store
                .call(
                    manager.clone(),
                    "coordination.participant.register".into(),
                    json!({
                        "client_request_id":model::new_id(), "client_id":"decision-proposer",
                        "token_hash":model::digest(token.as_bytes()), "task_id":task_id,
                        "task_revision":1, "attempt_id":attempt_id,
                        "participation_basis":{"kind":"attempt_owner"},
                    }),
                )
                .await
                .unwrap();
            let participant = store
                .authenticate(Credential {
                    client_id: "decision-proposer".into(),
                    token,
                })
                .await
                .unwrap();
            assert_eq!(participant.role, Role::Participant);
            let scope = store.call(participant.clone(), "code.scope.propose".into(), json!({
                "client_request_id":model::new_id(), "task_id":task_id, "task_revision":1,
                "attempt_id":attempt_id, "mode":"exclusive_edit", "paths":["src/contract.rs"],
                "symbols":[], "interfaces":[], "baseline_candidate_ref":"advisory-public-baseline",
                "reason":"exact affected contract path",
            })).await.unwrap();
            store.call(manager.clone(), "code.scope.accept".into(), json!({
                "client_request_id":model::new_id(), "scope_intent_id":scope["scope_intent_id"],
                "expected_state_revision":scope["state_revision"], "proposal_digest":scope["digest"],
                "reason":"accept exact affected path",
            })).await.unwrap();
            let inspected = store
                .call(
                    manager.clone(),
                    "code.scope.inspect".into(),
                    json!({
                        "task_id":task_id, "task_revision":1, "attempt_id":attempt_id,
                        "scope_intent_id":scope["scope_intent_id"],
                    }),
                )
                .await
                .unwrap();
            assert_eq!(inspected["coverage"], "complete");
            assert_eq!(inspected["items"].as_array().unwrap().len(), 1);
            let accepted = &inspected["items"][0];
            assert_eq!(accepted["state"], "active");
            // Closed decision-reference wire shape derived from the public
            // inspect receipt, including every accepted-scope field.
            let scope_ref = json!({
                "scope_intent_id":accepted["scope_intent_id"], "state_revision":accepted["state_revision"],
                "digest":accepted["digest"], "state":accepted["state"],
                "owner_client_id":accepted["actor"]["client_id"], "actor":accepted["actor"],
                "assignment_id":accepted["assignment_id"], "participation_basis":accepted["participation_basis"],
                "mode":accepted["mode"], "paths":accepted["paths"], "symbols":accepted["symbols"],
                "interfaces":accepted["interfaces"], "expires_at_ms":accepted["expires_at_ms"],
                "override_scope_intent_ids":accepted["override_scope_intent_ids"],
            });
            let opened = store.call(manager.clone(), "coordination.thread.open".into(), json!({
                "client_request_id":model::new_id(), "task_id":task_id, "attempt_id":attempt_id,
                "topic_kind":"contract", "subject":"public contract decision matrix",
                "participants":[{"client_id":participant.client_id, "generation":null, "reason":"exact proposer"}],
                "reasonability":{
                    "blocking_fact":"producer and consumer need an exact contract",
                    "decision_needed":"ratify or reject retained proposal", "why_coordination_is_needed":"shared boundary",
                    "expected_output":"immutable terminal decision", "close_condition":"exact ratification Operation",
                },
                "related_scopes":[{"kind":"path", "value":"src/contract.rs"}], "delivery_mode":"mailbox_only",
            })).await.unwrap();
            let thread_id = opened["thread_id"].clone();
            let proposal = store
                .call(
                    participant.clone(),
                    "coordination.contract.propose".into(),
                    proposal_request(&thread_id, Value::Null),
                )
                .await
                .unwrap();
            Self {
                owner,
                directory,
                operator,
                manager,
                former_owner,
                gm,
                unrelated,
                observer,
                participant,
                gm_epoch: handover["gm_epoch"].clone(),
                task_id,
                attempt_id,
                former_attempt_id,
                thread_id,
                proposal,
                scope_ref,
            }
        }

        async fn call(&self, actor: &Principal, method: &str, request: Value) -> Result<Value> {
            self.owner
                .store
                .call(actor.clone(), method.into(), request)
                .await
        }

        fn decision(&self, proposal: &Value) -> Value {
            json!({
                "client_request_id":model::new_id(), "thread_id":self.thread_id, "expected_state_revision":1,
                "proposal_id":proposal["proposal_id"], "proposal_revision_id":proposal["proposal_revision_id"],
                "proposal_digest":proposal["proposal_digest"], "task_id":self.task_id, "task_revision":1,
                "attempt_id":self.attempt_id, "affected_scope_revisions":[self.scope_ref],
                "reason":PRIVATE_REASON, "conditions":[PRIVATE_CONDITION], "caveats":[PRIVATE_CAVEAT],
            })
        }

        fn resolve(&self, proposal: &Value, operation_id: &Value) -> Value {
            json!({
                "client_request_id":model::new_id(), "thread_id":self.thread_id, "expected_state_revision":1,
                "outcome":"resolved", "resolution_summary":"exact retained ratification",
                "selected_proposal_revision_id":proposal["proposal_revision_id"],
                "manager_ratification_operation_id":operation_id, "remaining_objections":[], "follow_up_operation_ids":[],
            })
        }

        async fn contract(&self, proposal: &Value) -> Value {
            self.call(
                &self.operator,
                "coordination.contract.get",
                json!({
                    "thread_id":self.thread_id, "proposal_id":proposal["proposal_id"],
                    "proposal_revision_id":proposal["proposal_revision_id"],
                }),
            )
            .await
            .unwrap()
        }

        async fn thread(&self) -> Value {
            self.call(
                &self.operator,
                "coordination.thread.get",
                json!({"thread_id":self.thread_id}),
            )
            .await
            .unwrap()
        }

        async fn delta(&self, after: i64) -> Value {
            let page = self
                .call(
                    &self.operator,
                    "report.delta",
                    json!({"after":after, "limit":200}),
                )
                .await
                .unwrap();
            assert_eq!(
                page["projection"]["has_newer"], false,
                "bounded fixture must fit one page: {page}"
            );
            assert_eq!(page["projection"]["coverage_complete"], true);
            page
        }

        async fn snapshot(&self) -> Value {
            json!({
                "thread":self.thread().await,
                "contract":self.contract(&self.proposal).await,
                "proposals":self.call(&self.operator, "coordination.contract.list", json!({"thread_id":self.thread_id})).await.unwrap(),
                "scopes":self.call(&self.operator, "code.scope.inspect", json!({"task_id":self.task_id, "attempt_id":self.attempt_id})).await.unwrap(),
                "task":self.call(&self.operator, "task.get", json!({"task_id":self.task_id})).await.unwrap(),
                "attempt":self.call(&self.operator, "attempt.get", json!({"attempt_id":self.attempt_id})).await.unwrap(),
            })
        }

        async fn denied(&self, actor: &Principal, method: &str, request: Value, code: &str) {
            let before = self.snapshot().await;
            let after = self.delta(0).await["next_cursor"].as_i64().unwrap();
            let error = self.call(actor, method, request).await.unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(
                self.snapshot().await,
                before,
                "denied admission changed a retained business fact"
            );
            let delta = self.delta(after).await;
            assert!(
                delta["items"].as_array().unwrap().iter().all(|item| {
                    !matches!(
                        item["kind"].as_str(),
                        Some(
                            "coordination.contract_ratified"
                                | "coordination.contract_rejected"
                                | "coordination.thread.resolve"
                                | "coordination.contract.ratify"
                                | "coordination.contract.reject"
                        )
                    )
                }),
                "denied admission retained a decision/resolution effect: {delta}"
            );
        }

        async fn assert_one_decision(&self, proposal: &Value, receipt: &Value, kind: &str) {
            let page = self.delta(0).await;
            let decisions: Vec<&Value> = page["items"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|item| {
                    matches!(
                        item["kind"].as_str(),
                        Some("coordination.contract_ratified" | "coordination.contract_rejected")
                    ) && item["payload"]["proposal_id"] == proposal["proposal_id"]
                        && item["payload"]["proposal_revision_id"]
                            == proposal["proposal_revision_id"]
                })
                .collect();
            assert_eq!(
                decisions.len(),
                1,
                "one terminal decision Observation per exact revision"
            );
            assert_eq!(decisions[0]["kind"], kind);
            assert_eq!(decisions[0]["operation_id"], receipt["operation_id"]);
            assert_eq!(
                decisions[0]["payload"]["decision_operation_id"],
                receipt["operation_id"]
            );
            assert_eq!(decisions[0]["cursor"], receipt["decision_observation_id"]);
        }

        async fn supersede(&self, proposal: &Value) -> Value {
            self.call(
                &self.participant,
                "coordination.contract.propose",
                proposal_request(&self.thread_id, proposal["proposal_revision_id"].clone()),
            )
            .await
            .unwrap()
        }

        async fn close(self) {
            self.owner.close().await.unwrap();
            std::fs::remove_dir_all(self.directory).unwrap();
        }
    }

    #[tokio::test]
    async fn ratify_replay_opposite_conflict_and_exact_thread_resolution() {
        let f = Fixture::new("ratify").await;
        let request = f.decision(&f.proposal);
        let receipt = f
            .call(&f.manager, "coordination.contract.ratify", request.clone())
            .await
            .unwrap();
        assert_eq!(receipt["decision"], "ratified");
        assert_eq!(receipt["task_id"], f.task_id);
        assert_eq!(receipt["attempt_id"], f.attempt_id);
        assert_eq!(receipt["proposal_digest"], f.proposal["proposal_digest"]);
        assert_eq!(receipt["model_work_started"], false);
        assert_eq!(receipt["native_execution"], false);
        let retained = f.contract(&f.proposal).await;
        assert_eq!(
            retained["decision"]["decision_operation_id"],
            receipt["operation_id"]
        );
        assert_eq!(
            retained["decision"]["affected_scope_revisions"],
            request["affected_scope_revisions"]
        );
        assert_eq!(
            retained["decision"]["authority_basis"]["kind"],
            "attempt_owner"
        );
        assert_eq!(
            retained["decision"]["actor"]["client_id"],
            f.manager.client_id
        );
        assert_eq!(retained["decision"]["reason"], PRIVATE_REASON);
        let before = f.snapshot().await;
        let cursor = f.delta(0).await["next_cursor"].as_i64().unwrap();
        assert_eq!(
            f.call(&f.manager, "coordination.contract.ratify", request.clone())
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(f.snapshot().await, before);
        assert!(
            f.delta(cursor).await["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let mut changed = request.clone();
        changed["conditions"] = json!(["changed decision content"]);
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            changed,
            "REQUEST_ID_CONFLICT",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            request,
            "REQUEST_ID_CONFLICT",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            f.decision(&f.proposal),
            "CONTRACT_DECISION_ALREADY_RECORDED",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            f.decision(&f.proposal),
            "CONTRACT_DECISION_ALREADY_RECORDED",
        )
        .await;
        f.assert_one_decision(&f.proposal, &receipt, "coordination.contract_ratified")
            .await;
        // This is a real settled proposal Operation, not a fabricated Operation ID.
        f.denied(
            &f.manager,
            "coordination.thread.resolve",
            f.resolve(&f.proposal, &f.proposal["operation_id"]),
            "RATIFICATION_INVALID",
        )
        .await;
        let resolution_request = f.resolve(&f.proposal, &receipt["operation_id"]);
        let resolved = f
            .call(
                &f.manager,
                "coordination.thread.resolve",
                resolution_request.clone(),
            )
            .await
            .unwrap();
        assert_eq!(resolved["state"], "resolved");
        assert_eq!(resolved["state_revision"], 2);
        assert_eq!(
            f.thread().await["thread"]["resolution"]["manager_ratification_operation_id"],
            receipt["operation_id"]
        );
        let after_resolution = f.delta(0).await["next_cursor"].as_i64().unwrap();
        assert_eq!(
            f.call(
                &f.manager,
                "coordination.thread.resolve",
                resolution_request
            )
            .await
            .unwrap(),
            resolved
        );
        assert!(
            f.delta(after_resolution).await["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        f.close().await;
    }

    #[tokio::test]
    async fn reject_replay_is_terminal_history_and_cannot_resolve_thread() {
        let f = Fixture::new("reject").await;
        let request = f.decision(&f.proposal);
        let receipt = f
            .call(&f.manager, "coordination.contract.reject", request.clone())
            .await
            .unwrap();
        let retained = f.contract(&f.proposal).await;
        assert_eq!(retained["decision"]["decision"], "rejected");
        assert_eq!(
            retained["decision"]["decision_operation_id"],
            receipt["operation_id"]
        );
        let before = f.snapshot().await;
        let cursor = f.delta(0).await["next_cursor"].as_i64().unwrap();
        assert_eq!(
            f.call(&f.manager, "coordination.contract.reject", request.clone())
                .await
                .unwrap(),
            receipt
        );
        assert_eq!(f.snapshot().await, before);
        assert!(
            f.delta(cursor).await["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            request,
            "REQUEST_ID_CONFLICT",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            f.decision(&f.proposal),
            "CONTRACT_DECISION_ALREADY_RECORDED",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            f.decision(&f.proposal),
            "CONTRACT_DECISION_ALREADY_RECORDED",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.thread.resolve",
            f.resolve(&f.proposal, &receipt["operation_id"]),
            "RATIFICATION_INVALID",
        )
        .await;
        assert_eq!(f.thread().await["thread"]["state"], "open");
        assert_eq!(f.thread().await["thread"]["state_revision"], 1);
        f.assert_one_decision(&f.proposal, &receipt, "coordination.contract_rejected")
            .await;
        let next = f.supersede(&f.proposal).await;
        assert_eq!(next["proposal_id"], f.proposal["proposal_id"]);
        assert_eq!(next["revision"], 2);
        assert_eq!(
            f.contract(&f.proposal).await,
            retained,
            "rejection remains immutable history after supersession"
        );
        assert!(f.contract(&next).await["decision"].is_null());
        f.close().await;
    }

    #[tokio::test]
    async fn stale_identity_and_incomplete_scope_fail_without_decision_effects() {
        let f = Fixture::new("stale").await;
        let other_task = f
            .call(
                &f.operator,
                "task.create",
                json!({
                    "client_request_id":model::new_id(), "project_id":"public-contract-matrix",
                    "spec":{"objective":"wrong Task control", "phase":"implementation",
                        "requirements":[{"id":"R", "statement":"unrelated Task"}]},
                }),
            )
            .await
            .unwrap();
        let sibling = f
            .call(
                &f.participant,
                "coordination.contract.propose",
                proposal_request(&f.thread_id, Value::Null),
            )
            .await
            .unwrap();
        let mut wrong_task = f.decision(&f.proposal);
        wrong_task["task_id"] = other_task["task_id"].clone();
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            wrong_task,
            "STALE_REVISION",
        )
        .await;
        let mut wrong_attempt = f.decision(&f.proposal);
        wrong_attempt["attempt_id"] = f.former_attempt_id.clone();
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            wrong_attempt,
            "STALE_REVISION",
        )
        .await;
        let mut wrong_task_revision = f.decision(&f.proposal);
        wrong_task_revision["task_revision"] = json!(2);
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            wrong_task_revision,
            "STALE_REVISION",
        )
        .await;
        let mut wrong_thread_revision = f.decision(&f.proposal);
        wrong_thread_revision["expected_state_revision"] = json!(2);
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            wrong_thread_revision,
            "STALE_REVISION",
        )
        .await;
        let mut wrong_proposal = f.decision(&f.proposal);
        wrong_proposal["proposal_id"] = sibling["proposal_id"].clone();
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            wrong_proposal,
            "STALE_CONTRACT_REVISION",
        )
        .await;
        let mut wrong_digest = f.decision(&f.proposal);
        wrong_digest["proposal_digest"] = json!("0".repeat(64));
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            wrong_digest,
            "STALE_CONTRACT_REVISION",
        )
        .await;
        let mut missing_revision = f.decision(&f.proposal);
        missing_revision["proposal_revision_id"] = json!(format!("cprev-{}", model::new_id()));
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            missing_revision,
            "CONTRACT_PROPOSAL_NOT_FOUND",
        )
        .await;
        let mut omitted_scope = f.decision(&f.proposal);
        omitted_scope["affected_scope_revisions"] = json!([]);
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            omitted_scope,
            "STALE_SCOPE_REVISION",
        )
        .await;
        let mut stale_scope = f.decision(&f.proposal);
        stale_scope["affected_scope_revisions"][0]["state_revision"] = json!(1);
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            stale_scope,
            "STALE_SCOPE_REVISION",
        )
        .await;
        let mut stale_scope_digest = f.decision(&f.proposal);
        stale_scope_digest["affected_scope_revisions"][0]["digest"] = json!("0".repeat(64));
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            stale_scope_digest,
            "STALE_SCOPE_REVISION",
        )
        .await;
        let mut incomplete_scope_ref = f.decision(&f.proposal);
        incomplete_scope_ref["affected_scope_revisions"][0]
            .as_object_mut()
            .unwrap()
            .remove("paths");
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            incomplete_scope_ref,
            "INVALID_PARAMS",
        )
        .await;
        let next = f.supersede(&f.proposal).await;
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            f.decision(&f.proposal),
            "STALE_CONTRACT_REVISION",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.contract.reject",
            f.decision(&f.proposal),
            "STALE_CONTRACT_REVISION",
        )
        .await;
        assert!(f.contract(&f.proposal).await["decision"].is_null());
        let ratified = f
            .call(
                &f.manager,
                "coordination.contract.ratify",
                f.decision(&next),
            )
            .await
            .unwrap();
        f.assert_one_decision(&next, &ratified, "coordination.contract_ratified")
            .await;
        f.close().await;
    }

    #[tokio::test]
    async fn resolve_requires_current_revision_and_its_exact_ratification_operation() {
        let f = Fixture::new("resolve-current").await;
        let old = f
            .call(
                &f.manager,
                "coordination.contract.ratify",
                f.decision(&f.proposal),
            )
            .await
            .unwrap();
        let next = f.supersede(&f.proposal).await;
        f.denied(
            &f.manager,
            "coordination.thread.resolve",
            f.resolve(&f.proposal, &old["operation_id"]),
            "STALE_CONTRACT_REVISION",
        )
        .await;
        f.denied(
            &f.manager,
            "coordination.thread.resolve",
            f.resolve(&next, &old["operation_id"]),
            "RATIFICATION_REQUIRED",
        )
        .await;
        let current = f
            .call(
                &f.manager,
                "coordination.contract.ratify",
                f.decision(&next),
            )
            .await
            .unwrap();
        f.denied(
            &f.manager,
            "coordination.thread.resolve",
            f.resolve(&next, &old["operation_id"]),
            "RATIFICATION_INVALID",
        )
        .await;
        let resolved = f
            .call(
                &f.manager,
                "coordination.thread.resolve",
                f.resolve(&next, &current["operation_id"]),
            )
            .await
            .unwrap();
        assert_eq!(resolved["state"], "resolved");
        assert_eq!(
            resolved["resolution"]["selected_proposal_revision_id"],
            next["proposal_revision_id"]
        );
        assert_eq!(
            resolved["resolution"]["manager_ratification_operation_id"],
            current["operation_id"]
        );
        f.close().await;
    }

    #[tokio::test]
    async fn current_gm_and_local_operator_admit_former_and_unrelated_actors_denied() {
        let f = Fixture::new("authority").await;
        f.denied(
            &f.former_owner,
            "coordination.contract.ratify",
            f.decision(&f.proposal),
            "FORBIDDEN",
        )
        .await;
        f.denied(
            &f.unrelated,
            "coordination.contract.reject",
            f.decision(&f.proposal),
            "FORBIDDEN",
        )
        .await;
        f.denied(
            &f.participant,
            "coordination.contract.ratify",
            f.decision(&f.proposal),
            "FORBIDDEN",
        )
        .await;
        f.denied(
            &f.observer,
            "coordination.contract.reject",
            f.decision(&f.proposal),
            "FORBIDDEN",
        )
        .await;
        let gm_receipt = f
            .call(
                &f.gm,
                "coordination.contract.ratify",
                f.decision(&f.proposal),
            )
            .await
            .unwrap();
        let gm_decision = f.contract(&f.proposal).await;
        assert_eq!(
            gm_decision["decision"]["authority_basis"]["kind"],
            "current_gm"
        );
        assert_eq!(
            gm_decision["decision"]["authority_basis"]["gm_epoch"],
            f.gm_epoch
        );
        assert_eq!(
            gm_decision["decision"]["actor"]["client_id"],
            f.gm.client_id
        );
        f.assert_one_decision(&f.proposal, &gm_receipt, "coordination.contract_ratified")
            .await;
        let next = f.supersede(&f.proposal).await;
        let gm_rejection = f
            .call(&f.gm, "coordination.contract.reject", f.decision(&next))
            .await
            .unwrap();
        let gm_rejected = f.contract(&next).await;
        assert_eq!(
            gm_rejected["decision"]["authority_basis"]["kind"],
            "current_gm"
        );
        assert_eq!(
            gm_rejected["decision"]["authority_basis"]["gm_epoch"],
            f.gm_epoch
        );
        f.assert_one_decision(&next, &gm_rejection, "coordination.contract_rejected")
            .await;
        let operator_proposal = f.supersede(&next).await;
        f.call(
            &f.operator,
            "gm.handover",
            json!({
                "client_request_id":model::new_id(), "client_id":f.unrelated.client_id,
            }),
        )
        .await
        .unwrap();
        f.denied(
            &f.gm,
            "coordination.contract.ratify",
            f.decision(&operator_proposal),
            "FORBIDDEN",
        )
        .await;
        let operator_ratification = f
            .call(
                &f.operator,
                "coordination.contract.ratify",
                f.decision(&operator_proposal),
            )
            .await
            .unwrap();
        let operator_ratified = f.contract(&operator_proposal).await;
        assert_eq!(
            operator_ratified["decision"]["authority_basis"]["kind"],
            "local_operator"
        );
        assert_eq!(
            operator_ratified["decision"]["actor"]["client_id"],
            f.operator.client_id
        );
        f.assert_one_decision(
            &operator_proposal,
            &operator_ratification,
            "coordination.contract_ratified",
        )
        .await;
        let rejected_proposal = f.supersede(&operator_proposal).await;
        let operator_receipt = f
            .call(
                &f.operator,
                "coordination.contract.reject",
                f.decision(&rejected_proposal),
            )
            .await
            .unwrap();
        let operator_decision = f.contract(&rejected_proposal).await;
        assert_eq!(
            operator_decision["decision"]["authority_basis"]["kind"],
            "local_operator"
        );
        assert!(operator_decision["decision"]["authority_basis"]["gm_epoch"].is_null());
        assert_eq!(
            operator_decision["decision"]["actor"]["client_id"],
            f.operator.client_id
        );
        f.assert_one_decision(
            &rejected_proposal,
            &operator_receipt,
            "coordination.contract_rejected",
        )
        .await;
        f.close().await;
    }

    #[tokio::test]
    async fn missing_publicly_admitted_scope_forces_partial_coverage_and_no_decision() {
        let f = Fixture::new("coverage-fault").await;
        let key = format!(
            "code.scope:v1:record:{}",
            f.scope_ref["scope_intent_id"].as_str().unwrap()
        );
        // The only SQL mutation in the public fixture: delete one exact current
        // record admitted by code.scope.propose/accept; leave its active index.
        f.owner
            .store
            .run_fixture(move |db| {
                assert_eq!(db.execute("DELETE FROM meta WHERE key=?1", [key])?, 1);
                Ok(())
            })
            .await
            .unwrap();
        let conflicts = f
            .call(
                &f.operator,
                "code.scope.conflicts",
                json!({
                    "task_id":f.task_id, "attempt_id":f.attempt_id,
                }),
            )
            .await
            .unwrap();
        assert_eq!(conflicts["coverage"], "partial");
        assert!(
            conflicts["gaps"]
                .as_array()
                .unwrap()
                .contains(&json!("active_scope_record_missing"))
        );
        f.denied(
            &f.manager,
            "coordination.contract.ratify",
            f.decision(&f.proposal),
            "SCOPE_COVERAGE_INCOMPLETE",
        )
        .await;
        f.denied(
            &f.operator,
            "coordination.contract.reject",
            f.decision(&f.proposal),
            "SCOPE_COVERAGE_INCOMPLETE",
        )
        .await;
        assert!(f.contract(&f.proposal).await["decision"].is_null());
        assert_eq!(f.thread().await["thread"]["state"], "open");
        f.close().await;
    }

    fn assert_private_fields_absent(value: &Value) {
        let encoded = model::canonical(value).unwrap();
        assert!(!encoded.contains(PRIVATE_REASON));
        assert!(!encoded.contains(PRIVATE_CONDITION));
        assert!(!encoded.contains(PRIVATE_CAVEAT));
        assert!(value.get("original_request").is_none());
        assert!(value.get("effective_request").is_none());
    }

    #[tokio::test]
    async fn generic_operation_get_list_delta_bound_relations_and_hide_private_decision_text() {
        let f = Fixture::new("privacy").await;
        let receipt = f
            .call(
                &f.gm,
                "coordination.contract.ratify",
                f.decision(&f.proposal),
            )
            .await
            .unwrap();
        let operation_request = json!({"operation_id":receipt["operation_id"]});
        let caller = f
            .call(&f.gm, "operation.get", operation_request.clone())
            .await
            .unwrap();
        let owner_read = f
            .call(&f.manager, "operation.get", operation_request.clone())
            .await
            .unwrap();
        let operator_read = f
            .call(&f.operator, "operation.get", operation_request.clone())
            .await
            .unwrap();
        assert_eq!(owner_read["task_id"], f.task_id);
        assert_eq!(owner_read["attempt_id"], f.attempt_id);
        assert_eq!(owner_read["result"], caller["result"]);
        assert_eq!(
            owner_read["result"]["proposal_revision_id"],
            f.proposal["proposal_revision_id"]
        );
        assert_eq!(
            owner_read["result"]["decision_observation_id"],
            receipt["decision_observation_id"]
        );
        assert!(owner_read.get("client_request_id").is_none());
        assert!(owner_read.get("diagnostic").is_none());
        assert!(operator_read["diagnostic"].is_object());
        assert_private_fields_absent(&caller);
        assert_private_fields_absent(&owner_read);
        assert_private_fields_absent(&operator_read);
        assert_eq!(
            f.call(&f.unrelated, "operation.get", operation_request.clone())
                .await
                .unwrap_err()
                .code,
            "NOT_FOUND"
        );
        assert_eq!(
            f.call(&f.observer, "operation.get", operation_request.clone())
                .await
                .unwrap_err()
                .code,
            "NOT_FOUND"
        );
        let full = f.contract(&f.proposal).await;
        assert_eq!(full["decision"]["reason"], PRIVATE_REASON);
        assert_eq!(full["decision"]["conditions"], json!([PRIVATE_CONDITION]));
        assert_eq!(full["decision"]["caveats"], json!([PRIVATE_CAVEAT]));
        let next = f.supersede(&f.proposal).await;
        let rejected_decision = f
            .call(&f.gm, "coordination.contract.reject", f.decision(&next))
            .await
            .unwrap();
        let rejected_operation = json!({"operation_id":rejected_decision["operation_id"]});
        let retained_rejection = f
            .call(&f.manager, "operation.get", rejected_operation.clone())
            .await
            .unwrap();
        assert_eq!(retained_rejection["result"]["decision"], "rejected");
        assert_eq!(retained_rejection["task_id"], f.task_id);
        assert_eq!(retained_rejection["attempt_id"], f.attempt_id);
        assert_eq!(
            retained_rejection["result"]["proposal_revision_id"],
            next["proposal_revision_id"]
        );
        assert!(retained_rejection.get("diagnostic").is_none());
        assert!(retained_rejection.get("client_request_id").is_none());
        assert_private_fields_absent(&retained_rejection);
        assert_eq!(
            f.call(&f.unrelated, "operation.get", rejected_operation)
                .await
                .unwrap_err()
                .code,
            "NOT_FOUND"
        );
        let list = f
            .call(&f.manager, "operation.list", json!({"limit":200}))
            .await
            .unwrap();
        assert_eq!(list["has_newer"], false);
        assert!(
            list["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["operation_id"] == receipt["operation_id"])
        );
        assert!(
            list["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["operation_id"] == rejected_decision["operation_id"])
        );
        assert_private_fields_absent(&list);
        let denied_list = f
            .call(&f.unrelated, "operation.list", json!({"limit":200}))
            .await
            .unwrap();
        assert_eq!(denied_list["has_newer"], false);
        assert!(denied_list["items"].as_array().unwrap().is_empty());
        let denied_delta = f
            .call(
                &f.unrelated,
                "report.delta",
                json!({"after":0, "limit":200}),
            )
            .await
            .unwrap();
        assert_eq!(denied_delta["projection"]["has_newer"], false);
        assert!(
            denied_delta["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|item| item["operation_id"] != receipt["operation_id"])
        );
        assert!(
            denied_delta["items"]
                .as_array()
                .unwrap()
                .iter()
                .all(|item| item["operation_id"] != rejected_decision["operation_id"])
        );
        assert_private_fields_absent(&denied_delta);
        let owner_delta = f
            .call(&f.manager, "report.delta", json!({"after":0, "limit":200}))
            .await
            .unwrap();
        assert_eq!(owner_delta["projection"]["has_newer"], false);
        assert!(owner_delta["items"].as_array().unwrap().iter().any(|item| {
            item["kind"] == "coordination.contract_ratified"
                && item["operation_id"] == receipt["operation_id"]
                && item["payload"]["proposal_revision_id"] == f.proposal["proposal_revision_id"]
        }));
        assert!(owner_delta["items"].as_array().unwrap().iter().any(|item| {
            item["kind"] == "coordination.contract_rejected"
                && item["operation_id"] == rejected_decision["operation_id"]
                && item["payload"]["proposal_revision_id"] == next["proposal_revision_id"]
        }));
        assert_private_fields_absent(&owner_delta);
        let rejected_request = f.decision(&f.proposal);
        f.denied(
            &f.unrelated,
            "coordination.contract.reject",
            rejected_request.clone(),
            "FORBIDDEN",
        )
        .await;
        let rejection_cursor = f.delta(0).await["next_cursor"].as_i64().unwrap();
        assert_eq!(
            f.call(
                &f.unrelated,
                "coordination.contract.reject",
                rejected_request.clone()
            )
            .await
            .unwrap_err()
            .code,
            "FORBIDDEN"
        );
        assert!(
            f.delta(rejection_cursor).await["items"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let rejected_list = f
            .call(
                &f.unrelated,
                "operation.list",
                json!({"limit":200, "state":"rejected"}),
            )
            .await
            .unwrap();
        let rejected = rejected_list["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["client_request_id"] == rejected_request["client_request_id"])
            .unwrap();
        let rejected_get = f
            .call(
                &f.unrelated,
                "operation.get",
                json!({"operation_id":rejected["operation_id"]}),
            )
            .await
            .unwrap();
        assert_eq!(rejected_get["state"], "rejected");
        assert_eq!(
            rejected_get["client_request_id"],
            rejected_request["client_request_id"]
        );
        assert!(rejected_get["task_id"].is_null());
        assert!(rejected_get["attempt_id"].is_null());
        assert_private_fields_absent(&rejected_get);
        assert_eq!(
            f.call(
                &f.former_owner,
                "operation.get",
                json!({"operation_id":rejected["operation_id"]})
            )
            .await
            .unwrap_err()
            .code,
            "NOT_FOUND"
        );
        f.close().await;
    }
}
