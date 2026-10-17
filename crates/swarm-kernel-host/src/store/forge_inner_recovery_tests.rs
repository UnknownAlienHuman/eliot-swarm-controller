//! Recovery reducers and Store-state regressions. Synthetic immutable worker
//! records exercise the production post-owner/plan-check seam, not live OS
//! departure proof. No Git, worker process, native client or network is run.
use super::*;

const OP: &str = "inner-recovery-fixture";
const REPOSITORY: &str = "github.com/fixture/inner-recovery";
const JOB: &str = "11111111-1111-4111-8111-111111111111";
const OWNER: &str = "22222222-2222-4222-8222-222222222222";

struct Fixture {
    db: Connection,
    intent: PublicationIntent,
    receipt: Value,
    plan: Value,
}
impl Fixture {
    fn new(held: bool) -> Self {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        let intent = PublicationIntent {
            operation_id: OP.into(), project_id: "project".into(),
            canonical_repository: REPOSITORY.into(), attempt_id: "attempt".into(),
            task_revision: 1, admitted_gm_epoch: 0, submission_ref: "submission".into(),
            accepted_operation_id: "acceptance".into(), candidate_ref: "candidate".into(),
            candidate_sha256: "a".repeat(64), commit: "b".repeat(40), tree: "c".repeat(40),
            remote_name: "origin".into(), target_ref: "refs/heads/main".into(),
            expected_old_ref: Some("d".repeat(40)), expected_create: false,
            force: false, policy_revision: "fixture-v1".into(),
        };
        intent.validate().unwrap();
        let plan = json!({"schema_version":1,"kind":"forge_publish","job_id":JOB,
            "operation_id":OP,"owner_token":OWNER,"phase":"push_once","intent":intent,
            "git_executable":std::env::temp_dir().join("never-executed-git"),
            "timeout_seconds":1,"max_output_bytes":1024,
            "project":{"canonical_repository":REPOSITORY,"repository_path":std::env::temp_dir(),
                "remote_name":"origin","policy_revision":"fixture-v1","target_refs":["refs/heads/main"]},
            "push_endpoint":"https://example.invalid/fixture/inner-recovery"});
        let receipt = json!({"version":1,"kind":"forge_publish","job_id":JOB,
            "operation_id":OP,"phase":"push_once","owner_token":OWNER,
            "plan_sha256":model::digest(&serde_json::to_vec(&plan).unwrap()),
            "owner":{"version":1,"token":OWNER,
                "process":{"purpose":"module","pid":123,"creation_filetime":456}}});
        // These owner numbers are test data only. They never reach a process
        // query, family-empty probe, worker launch or a durable live receipt.
        for (id,kind) in [("submission","submission"),("candidate","candidate")] {
            db.execute("INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,?3,0,NULL,1,'{}')",params![id,format!("fixture/{id}.json"),kind]).unwrap();
        }
        db.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES('acceptance','operator','acceptance-request','task.accept','{}','{}','settled','{}',0,1,1,1)",[]).unwrap();
        db.execute("INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('task','project',1,'open','{}',1,1)",[]).unwrap();
        db.execute("INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,submission_ref,candidate_ref,released_at_ms,created_at_ms,updated_at_ms) VALUES('attempt','task',1,'{}','operator','controller','accepted','submission','candidate',1,1,1)",[]).unwrap();
        db.execute("UPDATE tasks SET state='accepted',accepted_attempt_id='attempt',accepted_operation_id='acceptance',accepted_revision=1,accepted_phase='complete',accepted_candidate_ref='candidate' WHERE task_id='task'",[]).unwrap();
        let mut result = json!({"outcome":"unknown","publication":"operator_intervention_required",
            "process_tree_unconfirmed":held,"process_tree_status":"unconfirmed",
            "process_tree_cleanup":"git_process_tree_unconfirmed",
            "resolution":"manual_operator_intervention_required",
            "publication_may_have_started":true,"native_worker":receipt});
        if !held {
            result["publication"]=json!("requires_readback");
            result["process_tree_status"]=json!("departed_empty");
            result.as_object_mut().unwrap().remove("process_tree_cleanup");
            result.as_object_mut().unwrap().remove("resolution");
        }
        db.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,sent_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'operator','inner-request','forge.publish_ref','{}',?2,'task','attempt','outcome_unknown',?3,0,2,1,1)",params![OP,model::canonical(&json!({"publication_intent":intent})).unwrap(),model::canonical(&result).unwrap()]).unwrap();
        Self { db, intent, receipt, plan }
    }
    fn operation(&self) -> Value { operations::get_operation(&self.db, OP).unwrap() }
    fn result(&self, empty: bool, unconfirmed: bool) -> Value {
        json!({"schema_version":1,"kind":"forge_publish","job_id":JOB,"operation_id":OP,
            "owner_token":OWNER,"plan_sha256":self.receipt["plan_sha256"],"phase":"push_once",
            "authorized":true,"outcome":"unknown","reason":"push_outcome_not_confirmed_by_exact_readback",
            "remote_ref":null,"push_exit_code":null,"timed_out":false,"stderr_sha256":null,"stderr_bytes":null,
            "process_tree_empty":empty,"process_tree_unconfirmed":unconfirmed})
    }
    fn classify(&self, result: &Value) -> Result<WorkerResultStatus> {
        classify_worker_result(&self.operation(), &self.receipt, RetainedWorkerFile::Bytes(serde_json::to_vec(result).unwrap()))
    }
    fn store_recovery(&self, result: Value) {
        self.db.execute("UPDATE operations SET result_json=?2 WHERE operation_id=?1",params![OP,model::canonical(&result).unwrap()]).unwrap();
    }
    fn block(&self, error: &Error) {
        self.store_recovery(worker_recovery_block_result(&self.operation(), &error.code, 3).unwrap());
    }
    fn assert_readback_stays_unknown(&mut self) {
        // A freshly matching remote ref must still pass through the actual
        // Store finish reducer and its repository hold, not a mirror assertion.
        finish(&mut self.db, OP, ForgeOutcome::Applied {readback:RefReadback::At(self.intent.commit.clone()),push_exit_code:None,timed_out:false,stderr_digest:None,stderr_bytes:None}).unwrap();
        let operation = self.operation();
        assert_eq!(operation["state"], "outcome_unknown");
        assert!(operation["settled_at_ms"].is_null());
        assert_eq!(operation["result"]["outcome"], "unknown");
        assert_eq!(operation["result"]["process_tree_unconfirmed"], true);
        assert_eq!(operation["result"]["publication"], "operator_intervention_required");
        assert_eq!(operation["result"]["native_worker"], self.receipt);
        assert_eq!(unresolved_process_tree_hold(&self.db, REPOSITORY).unwrap().as_deref(), Some(OP));
        assert_eq!(require_no_process_tree_hold(&self.db, REPOSITORY).unwrap_err().code, "FORGE_PROCESS_TREE_UNCONFIRMED");
        assert!(require_no_process_tree_hold(&self.db, "github.com/fixture/unrelated").is_ok());
    }
    fn readback_work(&mut self) -> ForgeWork {
        // Reconciliation validates this path but does not execute it. Match the
        // existing Store fixtures without requiring an installed Git binary.
        let mut config = ForgeConfig { enabled: true, git_executable: std::env::current_exe().unwrap(), ..Default::default() };
        config.projects.insert("project".into(), ForgeProject {canonical_repository:REPOSITORY.into(),repository_path:std::env::temp_dir(),remote_name:"origin".into(),policy_revision:"fixture-v1".into(),target_refs:vec!["refs/heads/main".into()]});
        let work = begin_reconciliation(&mut self.db, Principal {link_id:"fixture".into(),client_id:"operator".into(),role:Role::Operator}, OP, &config).unwrap().unwrap();
        assert_eq!(work.mode, WorkMode::ReadbackOnly);
        assert!(work.candidate.is_none());
        work
    }
}

#[test]
fn forge_inner_recovery_missing_malformed_and_partial_results_retain_hold() {
    for retained in [RetainedWorkerFile::Missing,RetainedWorkerFile::OverLimit,
        RetainedWorkerFile::Bytes(b"{truncated".to_vec()),
        RetainedWorkerFile::Bytes(b"{}".to_vec()),
        RetainedWorkerFile::Bytes(br#"{"process_tree_empty":true,"process_tree_unconfirmed":false}"#.to_vec())]
    {
        let mut fixture = Fixture::new(true);
        let status = classify_worker_result(&fixture.operation(), &fixture.receipt, retained).unwrap();
        assert!(!status.proves_inner_tree_empty());
        let error = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 3).unwrap_err();
        assert_eq!(error.code, "FORGE_GIT_TREE_TERMINATION");
        fixture.block(&error);
        fixture.assert_readback_stays_unknown();
    }
}

#[test]
fn forge_inner_recovery_boolean_shape_is_not_inner_cleanup_proof() {
    for (empty,unconfirmed) in [(false,false),(false,true),(true,true)] {
        for held in [true,false] {
            let mut fixture = Fixture::new(held);
            let status = fixture.classify(&fixture.result(empty,unconfirmed)).unwrap();
            assert_eq!(status, WorkerResultStatus::Present {inner_tree_empty:false});
            let error = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 3).unwrap_err();
            assert_eq!(error.code, "FORGE_GIT_TREE_TERMINATION");
            fixture.block(&error);
            fixture.assert_readback_stays_unknown();
        }
    }
}

#[test]
fn forge_inner_recovery_malformed_prior_hold_is_not_clearance_authority() {
    for flag in [json!(1),Value::Null,json!("true")] {
        let mut fixture = Fixture::new(true);let mut operation = fixture.operation();
        operation["result"]["process_tree_unconfirmed"]=flag;
        fixture.store_recovery(operation["result"].clone());
        let error = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, WorkerResultStatus::Missing, 3).unwrap_err();
        fixture.block(&error);fixture.assert_readback_stays_unknown();
    }
}

#[test]
fn forge_inner_recovery_clean_exact_result_permits_only_readback_before_settlement() {
    let mut fixture = Fixture::new(true);
    validate_forge_worker_plan(&fixture.db, &fixture.operation(), &fixture.receipt, &fixture.plan).unwrap();
    let status = fixture.classify(&fixture.result(true,false)).unwrap();
    assert!(status.proves_inner_tree_empty());
    fixture.store_recovery(worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 3).unwrap());
    let recovered = fixture.operation();
    assert_eq!(recovered["state"], "outcome_unknown");
    assert!(recovered["settled_at_ms"].is_null());
    assert_eq!(recovered["result"]["outcome"], "unknown");
    assert_eq!(recovered["result"]["native_worker_recovery"]["status"], "readback_permitted");
    assert_eq!(recovered["result"]["process_tree_unconfirmed"], false);
    let work = fixture.readback_work();
    finish(&mut fixture.db, OP, ForgeOutcome::Applied {readback:RefReadback::At(work.intent.commit),push_exit_code:None,timed_out:false,stderr_digest:None,stderr_bytes:None}).unwrap();
    assert_eq!(fixture.operation()["state"], "settled");
}

#[test]
fn forge_inner_recovery_denied_noexec_record_still_requires_empty_tree() {
    for empty in [false,true] {
        let fixture = Fixture::new(true);
        let mut result = fixture.result(empty,false);
        result["authorized"] = json!(false);result["outcome"] = json!("not_authorized");result["reason"] = json!("authorization_denied");
        let status = fixture.classify(&result).unwrap();
        let recovered = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 3);
        if empty {assert_eq!(recovered.unwrap()["native_worker_recovery"]["status"], "readback_permitted");}
        else {assert_eq!(recovered.unwrap_err().code, "FORGE_GIT_TREE_TERMINATION");}
    }
}

#[test]
fn forge_inner_recovery_foreign_result_scope_never_releases_hold() {
    for field in ["kind","job_id","operation_id","owner_token","plan_sha256","phase"] {
        let mut fixture = Fixture::new(true);let mut result = fixture.result(true,false);result[field]=json!("foreign");
        let error = fixture.classify(&result).unwrap_err();assert_eq!(error.code, "FORGE_WORKER_RESULT_SCOPE_MISMATCH");
        fixture.block(&error);fixture.assert_readback_stays_unknown();
    }
}

#[test]
fn forge_inner_recovery_changed_owner_or_plan_cannot_consume_clean_result() {
    for field in ["owner","plan_sha256"] {
        let mut fixture = Fixture::new(true);let status = fixture.classify(&fixture.result(true,false)).unwrap();
        let mut receipt = fixture.receipt.clone();
        if field=="owner" {receipt["owner"]["process"]["creation_filetime"]=json!(999);}else{receipt[field]=json!("e".repeat(64));}
        let error = worker_recovery_ready_result(&fixture.operation(), &receipt, status, 3).unwrap_err();
        assert_eq!(error.code, "FORGE_WORKER_RECEIPT_CHANGED");fixture.block(&error);fixture.assert_readback_stays_unknown();
    }
    for field in ["owner_token","job_id","operation_id","phase","intent"] {
        let mut fixture = Fixture::new(true);let mut plan = fixture.plan.clone();
        if field=="intent" {plan[field]["commit"]=json!("e".repeat(40));}else{plan[field]=json!("foreign");}
        let error = validate_forge_worker_plan(&fixture.db, &fixture.operation(), &fixture.receipt, &plan).unwrap_err();
        assert_eq!(error.code, "FORGE_WORKER_PLAN_SCOPE_MISMATCH");fixture.block(&error);fixture.assert_readback_stays_unknown();
    }
}

#[test]
fn forge_inner_recovery_outer_only_error_metadata_cannot_clear_inner_hold() {
    for code in ["FORGE_WORKER_PLAN_SCOPE_MISMATCH","FORGE_WORKER_RESULT_SCOPE_MISMATCH","FORGE_WORKER_EVIDENCE_UNAVAILABLE","FORGE_WORKER_EVIDENCE_INVALID"] {
        let mut fixture = Fixture::new(true);
        let blocked = worker_recovery_block_result(&fixture.operation(), code, 3).unwrap();
        assert_eq!(blocked["native_worker_recovery"]["process_family"], "departed_empty");
        assert_eq!(blocked["process_tree_unconfirmed"], true);
        assert_eq!(blocked["process_tree_cleanup"], "git_process_tree_unconfirmed");
        fixture.store_recovery(blocked);fixture.assert_readback_stays_unknown();
    }
}

#[test]
fn forge_inner_recovery_conflicting_stored_and_file_results_do_not_upgrade() {
    let mut fixture = Fixture::new(true);let mut operation = fixture.operation();
    operation["result"]["native_worker_result"]=fixture.result(false,true);
    let status = classify_worker_result(&operation, &fixture.receipt, RetainedWorkerFile::Bytes(serde_json::to_vec(&fixture.result(true,false)).unwrap())).unwrap();
    assert_eq!(status, WorkerResultStatus::Malformed);
    fixture.store_recovery(operation["result"].clone());
    let error = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 3).unwrap_err();fixture.block(&error);fixture.assert_readback_stays_unknown();
}

#[test]
fn forge_inner_recovery_legacy_unheld_missing_result_keeps_readback_only_behavior() {
    let mut fixture = Fixture::new(false);
    let status = classify_worker_result(&fixture.operation(), &fixture.receipt, RetainedWorkerFile::Missing).unwrap();
    fixture.store_recovery(worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 3).unwrap());
    assert_eq!(fixture.operation()["state"], "outcome_unknown");
    assert_eq!(fixture.operation()["result"]["process_tree_unconfirmed"], false);
    fixture.readback_work();
}
