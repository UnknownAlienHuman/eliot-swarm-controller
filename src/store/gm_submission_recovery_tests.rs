//! Readback-only recovery for task submissions across GM handover.
use super::*;
use crate::artifacts::{ArtifactFiles, ArtifactRecord};
use rusqlite::params;
use serde_json::{Value, json};

struct Subject {
    task_id: String,
    attempt_id: String,
    candidate_ref: String,
}

struct OriginalOperation {
    operation_id: String,
    expected_submission: ArtifactRecord,
}

struct PendingRecovery {
    recovery_id: String,
    target_id: String,
    verified: bool,
}

fn principal(client_id: &str) -> Principal {
    Principal {
        link_id: format!("link-{client_id}"),
        client_id: client_id.to_owned(),
        role: Role::Manager,
    }
}

fn database() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    db.execute_batch(SCHEMA).unwrap();
    for client_id in ["original-gm", "successor-gm"] {
        set_meta(
            &db,
            &format!("client:{client_id}"),
            &json!({"role":"manager","disabled":false}),
        )
        .unwrap();
    }
    set_meta(&db, "gm", &json!({"client_id":"original-gm","epoch":1})).unwrap();
    set_meta(&db, "execution_mode", &json!({"new_work":"enabled"})).unwrap();
    db
}

fn artifact_files() -> ArtifactFiles {
    let root = std::env::temp_dir().join(format!("swarm-gm-submit-recovery-{}", model::new_id()));
    std::fs::create_dir_all(&root).unwrap();
    let root = std::fs::canonicalize(root).unwrap();
    ArtifactFiles::new(&root).unwrap()
}

fn seed_subject(db: &Connection, files: &ArtifactFiles, suffix: &str) -> Subject {
    let task_id = format!("task-recovery-{suffix}");
    let attempt_id = format!("attempt-recovery-{suffix}");
    let candidate_bytes = format!("candidate bytes for {suffix}").into_bytes();
    let candidate_ref = format!("source-{}", model::digest(&candidate_bytes));
    let candidate_metadata = json!({"attempt_id":attempt_id,"task_revision":1});
    let candidate = ArtifactRecord {
        kind: "source_snapshot".into(),
        artifact_id: candidate_ref.clone(),
        relative_path: format!("artifacts/{candidate_ref}.bin"),
        byte_length: candidate_bytes.len() as u64,
        content_digest: model::digest(&candidate_bytes),
        metadata: candidate_metadata.clone(),
    };
    files.publish(&candidate, &candidate_bytes).unwrap();

    let spec_value = json!({
        "objective":"Recover the exact published submission",
        "phase":"implementation",
        "requirements":[{"id":"R1","statement":"Retain the submitted artifact"}],
        "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID
    });
    let spec: crate::model::TaskSpec = serde_json::from_value(spec_value.clone()).unwrap();
    let owner_policy = crate::policy::accepted_edition(Some(crate::policy::OWNER_POLICY_V2_ID))
        .and_then(|edition| serde_json::to_value(edition).map_err(Into::into))
        .unwrap();
    let snapshot = json!({
        "spec":spec_value,
        "revision":1,
        "dependency_acceptances":[],
        "baseline_candidate":null,
        "owner_policy":owner_policy,
        "brief":spec.brief()
    });
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'recovery-project',1,'open',?2,1,1)",
        params![task_id, model::canonical(&spec_value).unwrap()],
    )
    .unwrap();
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) \
         VALUES(?1,?2,'source_snapshot',?3,?4,1,?5)",
        params![
            candidate_ref,
            candidate.relative_path,
            candidate.byte_length as i64,
            candidate.content_digest,
            model::canonical(&candidate_metadata).unwrap()
        ],
    )
    .unwrap();
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,1,?3,'original-gm','controller','running','[]',1,1)",
        params![attempt_id, task_id, model::canonical(&snapshot).unwrap()],
    )
    .unwrap();
    Subject {
        task_id,
        attempt_id,
        candidate_ref,
    }
}

fn submit_then_crash(
    db: &mut Connection,
    files: &ArtifactFiles,
    owner: &Principal,
    subject: &Subject,
    suffix: &str,
    publish: bool,
    config: &Config,
) -> OriginalOperation {
    let receipt = super::mutate(
        db,
        owner,
        "task.submit",
        &json!({
            "client_request_id":format!("submit-{suffix}"),
            "attempt_id":subject.attempt_id,
            "expected_revision":1,
            "expected_submission_ref":null,
            "candidate_ref":subject.candidate_ref,
            "summary":"submission prepared before GM handover",
            "claims":[]
        }),
        config,
    )
    .unwrap();
    let operation_id = receipt["operation_id"].as_str().unwrap().to_owned();
    let (candidate, document) = super::submissions::begin(db, owner.clone(), &operation_id)
        .unwrap()
        .unwrap();
    files.verify(&candidate).unwrap();
    let (expected_submission, bytes) = ArtifactFiles::submission(&operation_id, &document).unwrap();
    if publish {
        files.publish(&expected_submission, &bytes).unwrap();
    }
    let changed = db
        .execute(
            "UPDATE operations SET state='outcome_unknown' WHERE operation_id=?1 AND state='sending'",
            [&operation_id],
        )
        .unwrap();
    assert_eq!(changed, 1);
    OriginalOperation {
        operation_id,
        expected_submission,
    }
}

fn prepare_recovery(
    db: &mut Connection,
    files: &ArtifactFiles,
    manager: &Principal,
    target: &OriginalOperation,
    request_id: &str,
    config: &Config,
) -> PendingRecovery {
    let receipt = super::mutate(
        db,
        manager,
        "task.submit.recover",
        &json!({"client_request_id":request_id,"operation_id":target.operation_id}),
        config,
    )
    .unwrap();
    let recovery_id = receipt["operation_id"].as_str().unwrap().to_owned();
    let (target_id, expected_artifact) =
        match super::submissions::begin_recovery(db, manager.clone(), &recovery_id).unwrap() {
            super::submissions::SubmissionRecoveryStart::Verify {
                target_operation_id,
                expected_artifact,
            } => (target_operation_id, expected_artifact),
            super::submissions::SubmissionRecoveryStart::Complete(value) => {
                panic!("new recovery completed before readback: {value}")
            }
        };
    assert_eq!(target_id, target.operation_id);
    let verified = files.verify_existing(&expected_artifact).unwrap();
    PendingRecovery {
        recovery_id,
        target_id,
        verified,
    }
}

fn finish_recovery(
    db: &mut Connection,
    manager: &Principal,
    pending: &PendingRecovery,
) -> crate::error::Result<Value> {
    super::submissions::finish_recovery(
        db,
        manager.clone(),
        &pending.recovery_id,
        &pending.target_id,
        pending.verified,
    )
}

fn recover(
    db: &mut Connection,
    files: &ArtifactFiles,
    manager: &Principal,
    target: &OriginalOperation,
    request_id: &str,
    config: &Config,
) -> Value {
    let pending = prepare_recovery(db, files, manager, target, request_id, config);
    finish_recovery(db, manager, &pending).unwrap()
}

#[test]
fn successor_gm_recovers_only_an_existing_exact_submission_artifact() {
    let mut db = database();
    let files = artifact_files();
    let config = Config::default();
    let original_gm = principal("original-gm");
    let successor_gm = principal("successor-gm");
    let published = seed_subject(&db, &files, "published");
    let missing = seed_subject(&db, &files, "missing");
    let stale = seed_subject(&db, &files, "stale");
    let published_target = submit_then_crash(
        &mut db,
        &files,
        &original_gm,
        &published,
        "published",
        true,
        &config,
    );
    let missing_target = submit_then_crash(
        &mut db,
        &files,
        &original_gm,
        &missing,
        "missing",
        false,
        &config,
    );
    let stale_target = submit_then_crash(
        &mut db,
        &files,
        &original_gm,
        &stale,
        "stale",
        true,
        &config,
    );
    db.execute(
        "UPDATE tasks SET revision=2 WHERE task_id=?1",
        [&stale.task_id],
    )
    .unwrap();

    super::mutate(
        &mut db,
        &original_gm,
        "gm.handover",
        &json!({"client_request_id":"handover-to-successor","client_id":"successor-gm"}),
        &config,
    )
    .unwrap();

    let pending_published_recovery = prepare_recovery(
        &mut db,
        &files,
        &successor_gm,
        &published_target,
        "recover-published",
        &config,
    );
    super::mutate(
        &mut db,
        &successor_gm,
        "gm.handover",
        &json!({"client_request_id":"handover-back-to-original","client_id":"original-gm"}),
        &config,
    )
    .unwrap();
    let stale_issuer =
        finish_recovery(&mut db, &successor_gm, &pending_published_recovery).unwrap_err();
    assert_eq!(stale_issuer.code, "FORBIDDEN");
    let still_unknown: String = db
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [&published_target.operation_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(still_unknown, "outcome_unknown");
    let pending_state: String = db
        .query_row(
            "SELECT state FROM operations WHERE operation_id=?1",
            [&pending_published_recovery.recovery_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending_state, "queued");
    super::mutate(
        &mut db,
        &original_gm,
        "gm.handover",
        &json!({"client_request_id":"handover-back-to-successor","client_id":"successor-gm"}),
        &config,
    )
    .unwrap();
    let finalized = finish_recovery(&mut db, &successor_gm, &pending_published_recovery).unwrap();
    assert_eq!(finalized["outcome"], "recovered");
    assert_eq!(finalized["target_outcome"], "applied");
    let published_state: (String, String, String, String) = db
        .query_row(
            "SELECT o.state,o.caller_id,json_extract(o.effective_request_json,'$.submission_document.submitted_by'),a.state \
             FROM operations o JOIN attempts a ON a.attempt_id=o.attempt_id WHERE o.operation_id=?1",
            [&published_target.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(published_state.0, "settled");
    assert_eq!(published_state.1, "original-gm");
    assert_eq!(published_state.2, "original-gm");
    assert_eq!(published_state.3, "submitted");
    let published_ref: String = db
        .query_row(
            "SELECT submission_ref FROM attempts WHERE attempt_id=?1",
            [&published.attempt_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        published_ref,
        published_target.expected_submission.artifact_id
    );
    let repeat_receipt = super::mutate(
        &mut db,
        &successor_gm,
        "task.submit.recover",
        &json!({"client_request_id":"recover-already-settled","operation_id":published_target.operation_id}),
        &config,
    )
    .unwrap();
    let super::submissions::SubmissionRecoveryStart::Complete(repeat_result) =
        super::submissions::begin_recovery(
            &mut db,
            successor_gm.clone(),
            repeat_receipt["operation_id"].as_str().unwrap(),
        )
        .unwrap()
    else {
        panic!("settled target requested another file read");
    };
    assert_eq!(repeat_result["outcome"], "already_settled");
    assert_eq!(
        repeat_result["target_result"]["submission_ref"],
        published_ref
    );

    recover(
        &mut db,
        &files,
        &successor_gm,
        &missing_target,
        "recover-missing",
        &config,
    );
    assert!(
        !files
            .verify_existing(&missing_target.expected_submission)
            .unwrap()
    );
    let missing_state: (String, Option<String>, String) = db
        .query_row(
            "SELECT o.state,a.submission_ref,a.state FROM operations o \
             JOIN attempts a ON a.attempt_id=o.attempt_id WHERE o.operation_id=?1",
            [&missing_target.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(missing_state.0, "outcome_unknown");
    assert_eq!(missing_state.1, None);
    assert_eq!(missing_state.2, "running");

    let stale_result = recover(
        &mut db,
        &files,
        &successor_gm,
        &stale_target,
        "recover-stale",
        &config,
    );
    assert_eq!(stale_result["outcome"], "recovered");
    assert_eq!(stale_result["target_outcome"], "stale_submission_scope");
    assert!(
        files
            .verify_existing(&stale_target.expected_submission)
            .unwrap()
    );
    let stale_state: (String, Option<String>, String, String) = db
        .query_row(
            "SELECT o.state,a.submission_ref,a.state,json_extract(o.result_json,'$.outcome') \
             FROM operations o JOIN attempts a ON a.attempt_id=o.attempt_id WHERE o.operation_id=?1",
            [&stale_target.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(stale_state.0, "settled");
    assert_eq!(stale_state.1, None);
    assert_eq!(stale_state.2, "running");
    assert_eq!(stale_state.3, "stale_submission_scope");
}
