//! Focused R09/#35 replacement-matrix proof over public Store/MCP rows.
//!
//! The replacement state machine, correction-content dedupe, o7 repair
//! cycles and REG12 lineage already carry named evidence and are reused
//! here without modification. This fixture adds only the absent rows:
//! unanswered and inconclusive replacement, ReturnedForCorrection
//! replacement while the Attempt is needs_correction, refusal of Pass,
//! undisposed ChangesRequested, wrong predecessor, same reviewer, stale
//! slot pointer and changed candidate/revision, late-predecessor exclusion
//! from applicability/acceptance/feedback/repair, successor read scope,
//! and bounded review.list pagination.
use super::*;
use crate::model::{Credential, Principal, Role};
use serde_json::json;
use std::sync::Arc;

const MANAGER_ID: &str = "review-owner-r35-matrix";
const PROJECT_ID: &str = "fixture";
const AUTOMATION_ID: &str = "r35-matrix";

fn retained_finding() -> serde_json::Value {
    json!({
        "finding_id":"missing-r1-evidence",
        "requirement_ids":["R1"],
        "reason":"The candidate omits the requested evidence.",
        "evidence_refs":["evidence://review/r35"],
        "requested_change":"Add the retained evidence for R1."
    })
}

fn findings_one() -> Vec<serde_json::Value> {
    vec![retained_finding()]
}

fn manager() -> Principal {
    Principal {
        link_id: model::new_id(),
        client_id: MANAGER_ID.to_owned(),
        role: Role::Manager,
    }
}

fn reviewer_id(n: u32) -> String {
    format!("r35-reviewer-{n:02}")
}

fn reviewer_token(n: u32) -> String {
    format!("r35-reviewer-token-{n:02}")
}

async fn start_store(label: &str) -> (StoreOwner, std::path::PathBuf, Credential) {
    let directory =
        std::env::temp_dir().join(format!("swarm-r35-matrix-{label}-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = crate::platform::DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    (owner, directory, credential)
}

async fn seed_manager(store: &Store) {
    store
        .run(|db| {
            set_meta(
                db,
                &format!("client:{MANAGER_ID}"),
                &json!({"role":Role::Manager,"disabled":false}),
            )?;
            set_meta(db, "gm", &json!({"client_id":MANAGER_ID,"epoch":1}))?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn seed_subject(store: &Store, suffix: &str) -> (String, String, String, String) {
    let suffix = suffix.to_owned();
    store
        .run(move |db| {
            let task_id = format!("task-{suffix}");
            let attempt_id = format!("attempt-{suffix}");
            let candidate_ref =
                format!("source-{}", model::digest(format!("candidate-{suffix}").as_bytes()));
            let submission_ref =
                format!("submission-{}", model::digest(format!("submission-{suffix}").as_bytes()));
            let submission_operation_id = format!("submit-op-{suffix}");
            let candidate_bytes = b"explicit source snapshot fixture";
            let candidate_digest = model::digest(candidate_bytes);
            let candidate_metadata = json!({
                "attempt_id":attempt_id,
                "task_revision":1,
                "fixture":"r35 replacement matrix",
            });
            let spec_json = json!({
                "objective":format!("R35 replacement matrix fixture {suffix}"),
                "phase":"implementation",
                "requirements":[{"id":"R1","statement":"Preserve the exact review finding scope"}],
                "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
            });
            let spec: crate::model::TaskSpec = serde_json::from_value(spec_json.clone())?;
            let edition =
                crate::policy::accepted_edition(Some(crate::policy::OWNER_POLICY_V2_ID))?;
            let owner_policy = serde_json::to_value(edition)?;
            let snapshot = json!({
                "spec":spec_json,
                "revision":1,
                "dependency_acceptances":[],
                "baseline_candidate":null,
                "owner_policy":owner_policy,
                "brief":spec.brief(),
            });
            db.execute(
                "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,'fixture',1,'open',?2,1,1)",
                params![task_id, model::canonical(&spec_json)?],
            )?;
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'source_snapshot',?3,?4,1,?5)",
                params![
                    candidate_ref,
                    format!("artifacts/{candidate_ref}.bin"),
                    i64::try_from(candidate_bytes.len()).unwrap(),
                    candidate_digest,
                    model::canonical(&candidate_metadata)?,
                ],
            )?;
            let submission_document = json!({
                "schema_version":1,
                "operation_id":submission_operation_id,
                "task_id":task_id,
                "attempt_id":attempt_id,
                "task_revision":1,
                "phase":"implementation",
                "owner_id":MANAGER_ID,
                "submitted_by":MANAGER_ID,
                "previous_submission_ref":null,
                "candidate_ref":candidate_ref,
                "candidate_sha256":candidate_digest,
                "candidate_kind":"source_snapshot",
                "candidate_byte_length":candidate_bytes.len(),
                "summary":"fixture submission",
                "claims":[],
                "claim_counts":{"total":0,"met":0,"not_met":0,"deferred":0,"unreported":0},
                "evidence_level":"submitter_report",
                "source_checkout_verified":false,
            });
            let submission_digest =
                model::digest(model::canonical(&submission_document)?.as_bytes());
            let submission_metadata = json!({"operation_id":submission_operation_id});
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',?3,?4,1,?5)",
                params![
                    submission_ref,
                    format!("artifacts/{submission_ref}.bin"),
                    i64::try_from(model::canonical(&submission_document)?.len()).unwrap(),
                    submission_digest,
                    model::canonical(&submission_metadata)?,
                ],
            )?;
            db.execute(
                "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES(?1,?2,1,?3,?4,'controller','submitted','[]',?5,?6,1,1)",
                params![
                    attempt_id,
                    task_id,
                    model::canonical(&snapshot)?,
                    MANAGER_ID,
                    submission_ref,
                    candidate_ref,
                ],
            )?;
            let submit_request = json!({
                "client_request_id":format!("submit-request-{suffix}"),
                "attempt_id":attempt_id,
                "expected_revision":1,
                "expected_submission_ref":null,
                "candidate_ref":candidate_ref,
            });
            db.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'task.submit',?4,?5,?6,?7,'settled',?8,1,1,1,1)",
                params![
                    submission_operation_id,
                    MANAGER_ID,
                    format!("submit-request-{suffix}"),
                    model::canonical(&submit_request)?,
                    model::canonical(&json!({"submission_document":submission_document}))?,
                    task_id,
                    attempt_id,
                    model::canonical(&json!({
                        "operation_id":submission_operation_id,
                        "outcome":"applied",
                        "attempt_id":attempt_id,
                        "submission_ref":submission_ref,
                        "candidate_ref":candidate_ref,
                        "task_accepted":false,
                    }))?,
                ],
            )?;
            Ok((task_id, attempt_id, submission_ref, candidate_ref))
        })
        .await
        .unwrap()
}

async fn register(
    store: &Store,
    subject: &(String, String, String, String),
    reviewer: &str,
    token: &str,
) {
    let pending_scope = json!({
        "review_assignment_id":null,
        "task_id":subject.0,
        "attempt_id":subject.1,
        "task_revision":1,
        "submission_ref":subject.2,
        "candidate_ref":subject.3,
    });
    store
        .call(
            manager(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":format!("register-{reviewer}"),
                "client_id":reviewer,
                "token_hash":model::digest(token.as_bytes()),
                "task_id":subject.0,
                "task_revision":1,
                "attempt_id":subject.1,
                "participation_basis":{"kind":"sponsored_reviewer","review_scope":pending_scope},
            }),
        )
        .await
        .unwrap();
}

async fn auth(store: &Store, reviewer: &str, token: &str) -> Principal {
    store
        .authenticate(Credential {
            client_id: reviewer.to_owned(),
            token: token.to_owned(),
        })
        .await
        .unwrap()
}

async fn assign(
    store: &Store,
    subject: &(String, String, String, String),
    reviewer: &str,
    token: &str,
    request_id: &str,
    replaces: Option<&str>,
) -> (Principal, String) {
    let mut assign = json!({
        "client_request_id":request_id,
        "attempt_id":subject.1,
        "expected_revision":1,
        "submission_ref":subject.2,
        "candidate_ref":subject.3,
        "reviewer_client_id":reviewer,
    });
    if let Some(prior) = replaces {
        assign["replaces_review_assignment_id"] = json!(prior);
        assign["replacement_reason"] =
            json!("r35 matrix replacement retains the predecessor as historical");
        assign["replacement_evidence_refs"] = json!([subject.3]);
    }
    let assignment = store
        .call(manager(), "review.assign".into(), assign)
        .await
        .unwrap();
    let assignment_id = assignment["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let reviewer = auth(store, reviewer, token).await;
    (reviewer, assignment_id)
}

async fn submit(
    store: &Store,
    reviewer: &Principal,
    subject: &(String, String, String, String),
    assignment_id: &str,
    request_id: &str,
    verdict: &str,
    coverage: &str,
    findings: &[serde_json::Value],
) -> String {
    let result = store
        .call(
            reviewer.clone(),
            "review.submit".into(),
            json!({
                "client_request_id":request_id,
                "review_assignment_id":assignment_id,
                "submission_ref":subject.2,
                "candidate_ref":subject.3,
                "verdict":verdict,
                "coverage":coverage,
                "findings":findings,
                "evidence_refs":["evidence://review/r35"],
            }),
        )
        .await
        .unwrap();
    assert_eq!(result["verdict"], verdict);
    result["operation_id"].as_str().unwrap().to_owned()
}

async fn review_get(store: &Store, principal: Principal, assignment_id: &str) -> serde_json::Value {
    store
        .call(
            principal,
            "review.get".into(),
            json!({"review_assignment_id":assignment_id}),
        )
        .await
        .unwrap()
}

async fn review_list(
    store: &Store,
    principal: Principal,
    request: serde_json::Value,
) -> serde_json::Value {
    store
        .call(principal, "review.list".into(), request)
        .await
        .unwrap()
}

async fn request_changes(
    store: &Store,
    subject: &(String, String, String, String),
    assignment_id: &str,
    result_operation_id: &str,
    request_id: &str,
) -> serde_json::Value {
    store
        .call(
            manager(),
            "task.request_changes".into(),
            json!({
                "schema_version":2,
                "client_request_id":request_id,
                "attempt_id":subject.1,
                "expected_revision":1,
                "submission_ref":subject.2,
                "candidate_ref":subject.3,
                "review_assignment_id":assignment_id,
                "review_result_operation_id":result_operation_id,
                "finding_ids":["missing-r1-evidence"],
            }),
        )
        .await
        .unwrap()
}

async fn refuse_assign(store: &Store, request: serde_json::Value, code: &str) {
    let error = store
        .call(manager(), "review.assign".into(), request)
        .await
        .expect_err("refused replacement must fail");
    assert_eq!(error.code.as_str(), code);
}

async fn refuse_request_changes(store: &Store, request: serde_json::Value, code: &str) {
    let error = store
        .call(manager(), "task.request_changes".into(), request)
        .await
        .expect_err("stale feedback authority must fail");
    assert_eq!(error.code.as_str(), code);
}

async fn attempt_state(store: &Store, attempt_id: &str) -> String {
    let attempt_id = attempt_id.to_owned();
    store
        .run(move |db| {
            let state: String = db.query_row(
                "SELECT state FROM attempts WHERE attempt_id=?1",
                [&attempt_id],
                |row| row.get(0),
            )?;
            Ok(state)
        })
        .await
        .unwrap()
}

async fn configure_entry(store: &Store, steps: Vec<crate::automation::actions::AutomationStep>) {
    store
        .run(move |db| {
            let tx = db.transaction()?;
            let mut entry = crate::automation::config::AutomationEntry::new(
                MANAGER_ID,
                PROJECT_ID,
                AUTOMATION_ID,
                10,
            );
            entry.enabled = true;
            entry.steps = steps;
            crate::automation::config::validate_entry(&entry)?;
            let entry_key =
                crate::automation::config::entry_key(MANAGER_ID, PROJECT_ID, AUTOMATION_ID)?;
            crate::automation::config::write_record(&tx, &entry_key, &entry.value()?)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn r35_unanswered_replacement_retires_predecessor_slot() {
    let (owner, directory, _credential) = start_store("unanswered").await;
    let store = &owner.store;
    seed_manager(store).await;
    let subject = seed_subject(store, "r35-unanswered").await;
    let s = &subject;
    let (r1_id, r1_token) = (reviewer_id(1), reviewer_token(1));
    let (r2_id, r2_token) = (reviewer_id(2), reviewer_token(2));
    register(store, s, &r1_id, &r1_token).await;
    register(store, s, &r2_id, &r2_token).await;
    let (_r1, a1) = assign(store, s, &r1_id, &r1_token, "r35-unanswered-assign-1", None).await;
    let (r2, a2) = assign(
        store,
        s,
        &r2_id,
        &r2_token,
        "r35-unanswered-assign-2",
        Some(&a1),
    )
    .await;
    assert_ne!(a1, a2);
    let old_view = review_get(store, manager(), &a1).await;
    assert!(old_view["result"].is_null());
    assert_eq!(old_view["current_slot"], false);
    assert_eq!(old_view["current_candidate"], false);
    let new_view = review_get(store, manager(), &a2).await;
    assert_eq!(new_view["current_slot"], true);
    assert_eq!(new_view["current_candidate"], true);
    assert_eq!(
        new_view["assignment"]["supersedes_review_assignment_id"].as_str(),
        Some(a1.as_str())
    );
    refuse_assign(
        store,
        json!({
            "client_request_id": "r35-unanswered-stale",
            "attempt_id": s.1,
            "expected_revision": 1,
            "submission_ref": s.2,
            "candidate_ref": s.3,
            "reviewer_client_id": r1_id,
            "replaces_review_assignment_id": a1,
            "replacement_reason": "r35 stale slot probe",
            "replacement_evidence_refs": [s.3],
        }),
        "REVIEW_REPLACEMENT_STALE",
    )
    .await;
    let intact = review_get(store, manager(), &a2).await;
    assert_eq!(intact["current_slot"], true);
    let predecessor = review_get(store, r2, &a1).await;
    assert_eq!(predecessor["current_slot"], false);
    assert_eq!(
        predecessor["assignment"]["review_assignment_id"].as_str(),
        Some(a1.as_str())
    );
    drop(owner);
    let _ = std::fs::remove_dir_all(&directory);
}

#[tokio::test]
async fn r35_inconclusive_result_replaced_once() {
    let (owner, directory, _credential) = start_store("inconclusive").await;
    let store = &owner.store;
    seed_manager(store).await;
    let subject = seed_subject(store, "r35-inconclusive").await;
    let s = &subject;
    let (r1_id, r1_token) = (reviewer_id(3), reviewer_token(3));
    let (r2_id, r2_token) = (reviewer_id(4), reviewer_token(4));
    register(store, s, &r1_id, &r1_token).await;
    register(store, s, &r2_id, &r2_token).await;
    let (r1, a1) = assign(
        store,
        s,
        &r1_id,
        &r1_token,
        "r35-inconclusive-assign-1",
        None,
    )
    .await;
    let _op1 = submit(
        store,
        &r1,
        s,
        &a1,
        "r35-inconclusive-submit-1",
        "inconclusive",
        "partial",
        &[],
    )
    .await;
    let (_r2, a2) = assign(
        store,
        s,
        &r2_id,
        &r2_token,
        "r35-inconclusive-assign-2",
        Some(&a1),
    )
    .await;
    assert_ne!(a1, a2);
    let old_view = review_get(store, manager(), &a1).await;
    assert_eq!(old_view["result"]["verdict"], "inconclusive");
    assert_eq!(old_view["result"]["applicability"], "current_candidate");
    assert_eq!(old_view["current_slot"], false);
    assert_eq!(old_view["current_candidate"], false);
    let new_view = review_get(store, manager(), &a2).await;
    assert_eq!(new_view["current_slot"], true);
    assert_eq!(new_view["current_candidate"], true);
    drop(owner);
    let _ = std::fs::remove_dir_all(&directory);
}

#[tokio::test]
async fn r35_returned_for_correction_replacement_and_refusals() {
    let (owner, directory, _credential) = start_store("correction").await;
    let store = &owner.store;
    seed_manager(store).await;
    let chain = seed_subject(store, "r35-chain").await;
    let c = &chain;
    let (c1_id, c1_token) = (reviewer_id(5), reviewer_token(5));
    let (c2_id, c2_token) = (reviewer_id(6), reviewer_token(6));
    register(store, c, &c1_id, &c1_token).await;
    register(store, c, &c2_id, &c2_token).await;
    let (c1, a1) = assign(store, c, &c1_id, &c1_token, "r35-chain-assign-1", None).await;
    let op1 = submit(
        store,
        &c1,
        c,
        &a1,
        "r35-chain-submit-1",
        "changes_requested",
        "complete",
        &findings_one(),
    )
    .await;
    let manual = request_changes(store, c, &a1, &op1, "r35-chain-request-changes").await;
    assert_eq!(manual["applied"], true);
    assert_eq!(manual["status"], "needs_correction");
    assert_eq!(
        attempt_state(store, &c.1).await.as_str(),
        "needs_correction"
    );
    let (_c2, a2) = assign(store, c, &c2_id, &c2_token, "r35-chain-assign-2", Some(&a1)).await;
    assert_ne!(a1, a2);
    assert_eq!(
        review_get(store, manager(), &a2).await["current_slot"],
        true
    );
    refuse_request_changes(
        store,
        json!({
            "schema_version":2,
            "client_request_id":"r35-chain-stale-authority",
            "attempt_id": c.1,
            "expected_revision":1,
            "submission_ref": c.2,
            "candidate_ref": c.3,
            "review_assignment_id": a1,
            "review_result_operation_id": op1,
            "finding_ids":["missing-r1-evidence"],
        }),
        "REVIEW_FINDING_NOT_FOUND",
    )
    .await;
    let pass = seed_subject(store, "r35-pass").await;
    let p = &pass;
    let (p1_id, p1_token) = (reviewer_id(7), reviewer_token(7));
    let (p2_id, p2_token) = (reviewer_id(8), reviewer_token(8));
    register(store, p, &p1_id, &p1_token).await;
    register(store, p, &p2_id, &p2_token).await;
    let (pp1, pa1) = assign(store, p, &p1_id, &p1_token, "r35-pass-assign-1", None).await;
    let _pop = submit(
        store,
        &pp1,
        p,
        &pa1,
        "r35-pass-submit-1",
        "pass",
        "complete",
        &[],
    )
    .await;
    refuse_assign(
        store,
        json!({
            "client_request_id": "r35-pass-refused",
            "attempt_id": p.1,
            "expected_revision": 1,
            "submission_ref": p.2,
            "candidate_ref": p.3,
            "reviewer_client_id": p2_id,
            "replaces_review_assignment_id": pa1,
            "replacement_reason": "r35 pass refusal probe",
            "replacement_evidence_refs": [p.3],
        }),
        "REVIEW_REPLACEMENT_NOT_ALLOWED",
    )
    .await;
    assert_eq!(
        review_get(store, manager(), &pa1).await["current_slot"],
        true
    );
    let undis = seed_subject(store, "r35-undisposed").await;
    let u = &undis;
    let (u1_id, u1_token) = (reviewer_id(9), reviewer_token(9));
    let (u2_id, u2_token) = (reviewer_id(10), reviewer_token(10));
    register(store, u, &u1_id, &u1_token).await;
    register(store, u, &u2_id, &u2_token).await;
    let (uu1, ua1) = assign(store, u, &u1_id, &u1_token, "r35-undisposed-assign-1", None).await;
    let _uop = submit(
        store,
        &uu1,
        u,
        &ua1,
        "r35-undisposed-submit-1",
        "changes_requested",
        "complete",
        &findings_one(),
    )
    .await;
    refuse_assign(
        store,
        json!({
            "client_request_id": "r35-undisposed-refused",
            "attempt_id": u.1,
            "expected_revision": 1,
            "submission_ref": u.2,
            "candidate_ref": u.3,
            "reviewer_client_id": u2_id,
            "replaces_review_assignment_id": ua1,
            "replacement_reason": "r35 undisposed refusal probe",
            "replacement_evidence_refs": [u.3],
        }),
        "REVIEW_REPLACEMENT_UNDISPOSED",
    )
    .await;
    assert_eq!(
        review_get(store, manager(), &ua1).await["current_slot"],
        true
    );
    let stale = seed_subject(store, "r35-stale").await;
    let t = &stale;
    let (t1_id, t1_token) = (reviewer_id(11), reviewer_token(11));
    let (t2_id, t2_token) = (reviewer_id(12), reviewer_token(12));
    register(store, t, &t1_id, &t1_token).await;
    register(store, t, &t2_id, &t2_token).await;
    let (_tt1, ta1) = assign(store, t, &t1_id, &t1_token, "r35-stale-assign-1", None).await;
    refuse_assign(
        store,
        json!({
            "client_request_id": "r35-wrong-predecessor",
            "attempt_id": t.1,
            "expected_revision": 1,
            "submission_ref": t.2,
            "candidate_ref": t.3,
            "reviewer_client_id": t2_id,
            "replaces_review_assignment_id": "review-assignment-unknown",
            "replacement_reason": "r35 wrong predecessor probe",
            "replacement_evidence_refs": [t.3],
        }),
        "REVIEW_REPLACEMENT_STALE",
    )
    .await;
    refuse_assign(
        store,
        json!({
            "client_request_id": "r35-same-reviewer",
            "attempt_id": t.1,
            "expected_revision": 1,
            "submission_ref": t.2,
            "candidate_ref": t.3,
            "reviewer_client_id": t1_id,
            "replaces_review_assignment_id": ta1,
            "replacement_reason": "r35 same reviewer probe",
            "replacement_evidence_refs": [t.3],
        }),
        "REVIEW_REPLACEMENT_SAME_REVIEWER",
    )
    .await;
    refuse_assign(
        store,
        json!({
            "client_request_id": "r35-changed-candidate",
            "attempt_id": t.1,
            "expected_revision": 1,
            "submission_ref": t.2,
            "candidate_ref": pass.3,
            "reviewer_client_id": t2_id,
            "replaces_review_assignment_id": ta1,
            "replacement_reason": "r35 changed candidate probe",
            "replacement_evidence_refs": [t.3],
        }),
        "REVIEW_ANCHOR_MISMATCH",
    )
    .await;
    refuse_assign(
        store,
        json!({
            "client_request_id": "r35-changed-revision",
            "attempt_id": t.1,
            "expected_revision": 2,
            "submission_ref": t.2,
            "candidate_ref": t.3,
            "reviewer_client_id": t2_id,
            "replaces_review_assignment_id": ta1,
            "replacement_reason": "r35 changed revision probe",
            "replacement_evidence_refs": [t.3],
        }),
        "REVIEW_ANCHOR_MISMATCH",
    )
    .await;
    assert_eq!(
        review_get(store, manager(), &ta1).await["current_slot"],
        true
    );
    drop(owner);
    let _ = std::fs::remove_dir_all(&directory);
}

#[tokio::test]
async fn r35_late_predecessor_excluded_from_consumers() {
    let (owner, directory, _credential) = start_store("late").await;
    let store = &owner.store;
    seed_manager(store).await;
    configure_entry(
        store,
        vec![
            crate::automation::actions::AutomationStep::ReviewDisposition,
            crate::automation::actions::AutomationStep::Acceptance,
            crate::automation::actions::AutomationStep::RepairDispatch,
        ],
    )
    .await;
    let subject = seed_subject(store, "r35-late").await;
    let s = &subject;
    let (f1_id, f1_token) = (reviewer_id(15), reviewer_token(15));
    let (f2_id, f2_token) = (reviewer_id(16), reviewer_token(16));
    let (fx_id, fx_token) = (reviewer_id(17), reviewer_token(17));
    register(store, s, &f1_id, &f1_token).await;
    register(store, s, &f2_id, &f2_token).await;
    register(store, s, &fx_id, &fx_token).await;
    let (f1, a1) = assign(store, s, &f1_id, &f1_token, "r35-late-assign-1", None).await;
    let op1 = submit(
        store,
        &f1,
        s,
        &a1,
        "r35-late-submit-1",
        "changes_requested",
        "complete",
        &findings_one(),
    )
    .await;
    let manual = request_changes(store, s, &a1, &op1, "r35-late-request-changes").await;
    assert_eq!(manual["applied"], true);
    assert_eq!(manual["status"], "needs_correction");
    let (f2, a2) = assign(store, s, &f2_id, &f2_token, "r35-late-assign-2", Some(&a1)).await;
    // The predecessor result is retained but historically scoped: the live
    // view no longer treats it as the current candidate.
    let old_view = review_get(store, manager(), &a1).await;
    assert_eq!(old_view["result"]["verdict"], "changes_requested");
    assert_eq!(old_view["result"]["applicability"], "current_candidate");
    assert_eq!(old_view["current_slot"], false);
    assert_eq!(old_view["current_candidate"], false);
    assert_eq!(
        review_get(store, manager(), &a2).await["current_slot"],
        true
    );
    // A late result for the retired assignment cannot even be retained: the
    // live slot gate refuses the submit.
    let late = store
        .call(
            f1,
            "review.submit".into(),
            json!({
                "client_request_id": "r35-late-submit-2",
                "review_assignment_id": a1,
                "submission_ref": s.2,
                "candidate_ref": s.3,
                "verdict": "pass",
                "coverage": "complete",
                "findings": [],
                "evidence_refs": ["evidence://review/r35"],
            }),
        )
        .await
        .expect_err("late predecessor result must be refused");
    assert_eq!(late.code.as_str(), "STALE_REVIEW_ASSIGNMENT");
    // Acceptance resolves the live slot pointer, so the predecessor cause
    // is not canonical acceptance evidence.
    let acceptance = consume_acceptance(store, &a1, &op1).await;
    assert_eq!(acceptance["status"], "not_canonical_result_event");
    assert_eq!(acceptance["task_accepted"], false);
    // Repair resolves live finding provenance against a resultless
    // successor, so the predecessor cause cannot become package input.
    let repair = consume_repair(store, &a1, &op1).await;
    assert_eq!(repair["status"], "skipped");
    assert_eq!(repair["reason"], "selected_finding_is_no_longer_actionable");
    // Feedback resolves live finding provenance, so the predecessor cause
    // cannot authorize feedback again.
    let stale_assignment = a1.clone();
    let stale_result = op1.clone();
    let feedback_error = store
        .run(move |db| {
            let tx = db.transaction()?;
            let entry =
                crate::automation::config::load_entry(&tx, MANAGER_ID, PROJECT_ID, AUTOMATION_ID)?
                    .unwrap();
            let result = super::automation_disposition::consume_review_result_for_entry(
                &tx,
                &Config::default(),
                &entry,
                &stale_assignment,
                &stale_result,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .expect_err("superseded feedback authority must fail");
    assert_eq!(feedback_error.code.as_str(), "REVIEW_FINDING_NOT_FOUND");
    let predecessor = review_get(store, f2, &a1).await;
    assert_eq!(predecessor["result"]["verdict"], "changes_requested");
    assert_eq!(predecessor["current_slot"], false);
    let fx = auth(store, &fx_id, &fx_token).await;
    let forbidden = store
        .call(fx, "review.get".into(), json!({"review_assignment_id": a1}))
        .await
        .expect_err("unrelated reviewer must be denied");
    assert_eq!(forbidden.code.as_str(), "FORBIDDEN");
    let missing = store
        .call(
            manager(),
            "review.get".into(),
            json!({"review_assignment_id": "review-assignment-unknown"}),
        )
        .await
        .expect_err("unknown assignment must fail");
    assert_eq!(missing.code.as_str(), "REVIEW_ASSIGNMENT_NOT_FOUND");
    drop(owner);
    let _ = std::fs::remove_dir_all(&directory);
}

#[tokio::test]
async fn r35_review_list_pagination_has_no_filtered_skips() {
    let (owner, directory, _credential) = start_store("list").await;
    let store = &owner.store;
    seed_manager(store).await;
    let task_a = seed_subject(store, "r35-list-a").await;
    let task_b = seed_subject(store, "r35-list-b").await;
    let la = &task_a;
    let lb = &task_b;
    let (aa1_id, aa1_token) = (reviewer_id(18), reviewer_token(18));
    let (aa2_id, aa2_token) = (reviewer_id(19), reviewer_token(19));
    let (bb1_id, bb1_token) = (reviewer_id(20), reviewer_token(20));
    register(store, la, &aa1_id, &aa1_token).await;
    register(store, la, &aa2_id, &aa2_token).await;
    register(store, lb, &bb1_id, &bb1_token).await;
    let (_aa1, a1) = assign(store, la, &aa1_id, &aa1_token, "r35-list-assign-a1", None).await;
    let (bb1, b1) = assign(store, lb, &bb1_id, &bb1_token, "r35-list-assign-b1", None).await;
    let (_aa2, a2) = assign(
        store,
        la,
        &aa2_id,
        &aa2_token,
        "r35-list-assign-a2",
        Some(&a1),
    )
    .await;
    fn item_id(page: &serde_json::Value, index: usize) -> &str {
        page["items"][index]["assignment"]["review_assignment_id"]
            .as_str()
            .unwrap()
    }
    let p1 = review_list(store, manager(), json!({"limit": 1})).await;
    assert_eq!(p1["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(item_id(&p1, 0), a1.as_str());
    assert_eq!(p1["has_newer"], true);
    let cursor1 = p1["next_after_observation_id"].as_i64().unwrap();
    let p2 = review_list(
        store,
        manager(),
        json!({"limit": 1, "after": 0, "after_observation_id": cursor1}),
    )
    .await;
    assert_eq!(item_id(&p2, 0), b1.as_str());
    assert_eq!(p2["has_newer"], true);
    let cursor2 = p2["next_after_observation_id"].as_i64().unwrap();
    let p3 = review_list(
        store,
        manager(),
        json!({"limit": 1, "after": 0, "after_observation_id": cursor2}),
    )
    .await;
    assert_eq!(item_id(&p3, 0), a2.as_str());
    assert_eq!(p3["has_newer"], false);
    assert_eq!(p3["coverage"], "complete");
    let f1 = review_list(store, manager(), json!({"task_id": la.0, "limit": 1})).await;
    assert_eq!(item_id(&f1, 0), a1.as_str());
    assert_eq!(f1["has_newer"], true);
    let fcursor = f1["next_after_observation_id"].as_i64().unwrap();
    let f2 = review_list(
        store,
        manager(),
        json!({"task_id": la.0, "limit": 1, "after": 0, "after_observation_id": fcursor}),
    )
    .await;
    assert_eq!(item_id(&f2, 0), a2.as_str());
    assert_eq!(f2["has_newer"], false);
    let only = review_list(store, bb1, json!({"limit": 10})).await;
    assert_eq!(only["items"].as_array().map(Vec::len), Some(1));
    assert_eq!(item_id(&only, 0), b1.as_str());
    drop(owner);
    let _ = std::fs::remove_dir_all(&directory);
}

async fn consume_acceptance(
    store: &Store,
    assignment_id: &str,
    result_id: &str,
) -> serde_json::Value {
    let assignment_id = assignment_id.to_owned();
    let result_id = result_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction()?;
            let entry =
                crate::automation::config::load_entry(&tx, MANAGER_ID, PROJECT_ID, AUTOMATION_ID)?
                    .unwrap();
            let result = super::automation_acceptance::consume_review_result_for_entry(
                &tx,
                &entry,
                &assignment_id,
                &result_id,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .unwrap()
}

async fn consume_repair(store: &Store, assignment_id: &str, result_id: &str) -> serde_json::Value {
    let assignment_id = assignment_id.to_owned();
    let result_id = result_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction()?;
            let entry =
                crate::automation::config::load_entry(&tx, MANAGER_ID, PROJECT_ID, AUTOMATION_ID)?
                    .unwrap();
            let result = super::automation_repair::consume_review_result_for_entry(
                &tx,
                &Config::default(),
                &entry,
                &assignment_id,
                &result_id,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .unwrap()
}
