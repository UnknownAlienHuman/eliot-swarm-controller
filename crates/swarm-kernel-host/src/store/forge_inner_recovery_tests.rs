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
        Self::with_db(db, held)
    }
    fn with_db(db: Connection, held: bool) -> Self {
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
    fn readback_job(&self) -> (NativeWorkerJob, Value, Value) {
        let job_id = "33333333-3333-4333-8333-333333333333";
        let owner_token = "44444444-4444-4444-8444-444444444444";
        let mut plan = self.plan.clone();
        plan["job_id"] = json!(job_id);
        plan["owner_token"] = json!(owner_token);
        plan["phase"] = json!("readback_only");
        plan["push_endpoint"] = Value::Null;
        let job = NativeWorkerJob {
            kind: "forge_publish",
            phase_name: "readback_only",
            job_id: job_id.into(),
            operation_id: OP.into(),
            owner_token: owner_token.into(),
            plan_sha256: model::digest(&serde_json::to_vec(&plan).unwrap()),
            directory: PathBuf::new(),
            plan_path: PathBuf::new(),
            owner_path: PathBuf::new(),
            authorization_path: PathBuf::new(),
            result_path: PathBuf::new(),
            executable: PathBuf::new(),
            executable_sha256: "f".repeat(64),
            timeout_seconds: 1,
        };
        let owner = json!({"version":1,"token":owner_token,
            "process":{"purpose":"module","pid":124,"creation_filetime":457}});
        (job, owner, plan)
    }
    fn readback_result(&self, receipt: &Value, empty: bool, unconfirmed: bool) -> Value {
        let mut result = self.result(empty, unconfirmed);
        for field in ["job_id", "owner_token", "plan_sha256", "phase"] {
            result[field] = receipt[field].clone();
        }
        result
    }
}

// Use the real Store startup transaction both times. Only synthetic test
// credentials/state are created; no host, worker, process query or Git runs.
struct RestartStore {
    directory: PathBuf,
    config: Config,
    credential: crate::model::Credential,
    supervisor_credential: crate::model::Credential,
}
impl RestartStore {
    fn new() -> Self {
        let directory = std::env::temp_dir().join(format!("forge-inner-recovery-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = directory.clone();
        Self {
            directory,
            config,
            credential: crate::model::Credential { client_id: "operator".into(), token: "synthetic-fixture".into() },
            supervisor_credential: crate::model::Credential { client_id: model::INTERNAL_MODULE_SUPERVISOR_CLIENT_ID.into(), token: "synthetic-supervisor-fixture".into() },
        }
    }
    fn open(&self) -> Connection {
        super::super::open_database(
            &self.directory, &self.credential, &self.supervisor_credential,
            &self.config, None, None, None,
        ).unwrap()
    }
    fn restart(&self, fixture: Fixture) -> Fixture {
        let Fixture { db, intent, receipt, plan } = fixture;
        drop(db);
        Fixture { db: self.open(), intent, receipt, plan }
    }
}
impl Drop for RestartStore {
    fn drop(&mut self) { std::fs::remove_dir_all(&self.directory).unwrap(); }
}

fn clear_first_worker(fixture: &Fixture) {
    let status = fixture.classify(&fixture.result(true, false)).unwrap();
    let mut result = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 3).unwrap();
    result["native_worker_result"] = fixture.result(true, false);
    fixture.store_recovery(result);
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

#[test]
fn forge_inner_recovery_new_readback_hold_survives_store_restart_and_old_clean_proof() {
    for case in ["missing", "truncated", "unclean", "old_result"] {
        let store = RestartStore::new();
        let mut fixture = Fixture::with_db(store.open(), true);
        clear_first_worker(&fixture);
        let original_result = fixture.result(true, false);
        let work = fixture.readback_work();
        let (job, owner, plan) = fixture.readback_job();
        let receipt = native_worker_receipt(&job, &owner);
        validate_forge_worker_plan(&fixture.db, &fixture.operation(), &receipt, &plan).unwrap();
        readback_authorized(&mut fixture.db, &work, &store.config, &job, &owner).unwrap();
        // Authorization may only be written after this durable attribution.
        let authorized = fixture.operation();
        assert_eq!(authorized["result"]["native_worker_readback"], receipt);
        assert_eq!(authorized["result"]["native_worker"], fixture.receipt);
        assert_eq!(authorized["result"]["process_tree_unconfirmed"], true);
        finish(&mut fixture.db, OP, runner_error_outcome(Error::new(
            "FORGE_GIT_TREE_TERMINATION", "synthetic readback worker inner hold",
        ))).unwrap();

        // Reconstruct the actual Store database with no in-memory custody.
        fixture = store.restart(fixture);
        let operation = fixture.operation();
        assert_eq!(current_native_worker(&operation), Some(&receipt));
        assert_eq!(operation["result"]["native_worker_result"], original_result);
        let old_status = classify_worker_result(&operation, &fixture.receipt,
            RetainedWorkerFile::Bytes(serde_json::to_vec(&original_result).unwrap())).unwrap();
        assert!(old_status.proves_inner_tree_empty());
        assert_eq!(worker_recovery_ready_result(&operation, &fixture.receipt, old_status, 4)
            .unwrap_err().code, "FORGE_WORKER_RECEIPT_CHANGED");

        let retained = match case {
            "missing" => RetainedWorkerFile::Missing,
            "truncated" => RetainedWorkerFile::Bytes(b"{truncated".to_vec()),
            "unclean" => RetainedWorkerFile::Bytes(serde_json::to_vec(&fixture.readback_result(&receipt, false, true)).unwrap()),
            "old_result" => RetainedWorkerFile::Bytes(serde_json::to_vec(&original_result).unwrap()),
            _ => unreachable!(),
        };
        let error = match classify_worker_result(&fixture.operation(), &receipt, retained) {
            Ok(status) => worker_recovery_ready_result(&fixture.operation(), &receipt, status, 4).unwrap_err(),
            Err(error) => error,
        };
        assert!(matches!(error.code.as_str(), "FORGE_GIT_TREE_TERMINATION" | "FORGE_WORKER_RESULT_SCOPE_MISMATCH"));
        fixture.block(&error);
        fixture.assert_readback_stays_unknown();
        assert_eq!(fixture.operation()["result"]["native_worker_readback"], receipt);
        // Recording a rejection must not accidentally enable missing-B retry.
        assert_eq!(worker_recovery_ready_result(&fixture.operation(), &receipt,
            WorkerResultStatus::Missing, 5).unwrap_err().code, "FORGE_GIT_TREE_TERMINATION");
    }
}

#[test]
fn forge_inner_recovery_legacy_consumed_cleanup_cannot_clear_later_unattributed_hold() {
    let store = RestartStore::new();
    let mut fixture = Fixture::with_db(store.open(), true);
    clear_first_worker(&fixture);
    // Reproduce the old Host path: B's receipt was never retained and finish
    // copied A's already-consumed cleanup metadata into B's new hold.
    finish(&mut fixture.db, OP, runner_error_outcome(Error::new(
        "FORGE_GIT_TREE_TERMINATION", "synthetic legacy readback worker hold",
    ))).unwrap();
    fixture = store.restart(fixture);
    for _ in 0..2 {
        let status = fixture.classify(&fixture.result(true, false)).unwrap();
        assert!(status.proves_inner_tree_empty());
        let error = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 4).unwrap_err();
        assert_eq!(error.code, "FORGE_GIT_TREE_TERMINATION");
        fixture.block(&error);
        fixture.assert_readback_stays_unknown();
        assert_eq!(fixture.operation()["result"]["native_worker_recovery"]["cleanup_previously_consumed"], true);
    }
}

#[test]
fn forge_inner_recovery_current_readback_clean_result_can_settle_after_restart() {
    let store = RestartStore::new();
    let mut fixture = Fixture::with_db(store.open(), true);
    clear_first_worker(&fixture);
    let original_result = fixture.result(true, false);
    let work = fixture.readback_work();
    let (job, owner, plan) = fixture.readback_job();
    let receipt = native_worker_receipt(&job, &owner);
    validate_forge_worker_plan(&fixture.db, &fixture.operation(), &receipt, &plan).unwrap();
    readback_authorized(&mut fixture.db, &work, &store.config, &job, &owner).unwrap();
    fixture = store.restart(fixture);
    let mut result = fixture.readback_result(&receipt, true, false);
    result["outcome"] = json!("applied");
    result["remote_ref"] = json!({"present":true,"commit":fixture.intent.commit});
    // Exercise the same post-family-empty completion transaction as production.
    readback_worker_completed(&mut fixture.db, &receipt, &result).unwrap();
    assert_eq!(fixture.operation()["state"], "outcome_unknown");
    assert_eq!(fixture.operation()["result"]["process_tree_unconfirmed"], false);
    let outcome = map_forge_worker_result(&job, &fixture.intent, &result);
    assert!(matches!(outcome, ForgeOutcome::Applied { .. }));
    finish(&mut fixture.db, OP, outcome).unwrap();
    let settled = fixture.operation();
    assert_eq!(settled["state"], "settled");
    assert_eq!(settled["result"]["outcome"], "applied");
    assert_eq!(settled["result"]["native_worker"], fixture.receipt);
    assert_eq!(settled["result"]["native_worker_result"], original_result);
    assert_eq!(settled["result"]["native_worker_readback"], receipt);
    assert_eq!(settled["result"]["native_worker_readback_result"], result);
    assert!(require_no_process_tree_hold(&fixture.db, REPOSITORY).is_ok());
}

#[test]
fn forge_inner_recovery_readback_attribution_rejects_foreign_jobs_and_prior_holds() {
    for case in ["operation", "phase", "owner", "prior_hold"] {
        let mut fixture = Fixture::new(false);
        let work = fixture.readback_work();
        let (mut job, mut owner, _) = fixture.readback_job();
        match case {
            "operation" => job.operation_id = "foreign".into(),
            "phase" => job.phase_name = "push_once",
            "owner" => owner["token"] = json!("foreign"),
            "prior_hold" => {
                let mut result = fixture.operation()["result"].clone();
                result["process_tree_unconfirmed"] = json!(true);
                fixture.store_recovery(result);
            }
            _ => unreachable!(),
        }
        let before = fixture.operation();
        let error = readback_authorized(&mut fixture.db, &work, &Config::default(), &job, &owner).unwrap_err();
        assert_eq!(error.code, if case == "prior_hold" { "FORGE_PROCESS_TREE_UNCONFIRMED" } else { "FORGE_WORKER_PLAN_SCOPE_MISMATCH" });
        assert_eq!(fixture.operation(), before);
        assert!(fixture.operation()["result"].get("native_worker_readback").is_none());
    }
}

#[test]
fn forge_inner_recovery_malformed_current_receipt_never_falls_back_to_original() {
    for receipt in [Value::Null, json!({}), json!("malformed")] {
        let mut fixture = Fixture::new(false);
        clear_first_worker(&fixture);
        let mut result = fixture.operation()["result"].clone();
        result["native_worker_readback"] = receipt.clone();
        result["process_tree_unconfirmed"] = json!(true);
        fixture.store_recovery(result);
        let status = fixture.classify(&fixture.result(true, false)).unwrap();
        let error = worker_recovery_ready_result(&fixture.operation(), &fixture.receipt, status, 4).unwrap_err();
        assert_eq!(error.code, "FORGE_WORKER_RECEIPT_CHANGED");
        fixture.block(&error);
        fixture.assert_readback_stays_unknown();
        assert_eq!(current_native_worker(&fixture.operation()), Some(&receipt));
    }
}
