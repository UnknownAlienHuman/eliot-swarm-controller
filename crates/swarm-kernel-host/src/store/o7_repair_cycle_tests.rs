//! One public Store path through correction, fresh review, and exact acceptance.
use super::*;
use crate::artifacts::ArtifactRecord;
use rusqlite::{OptionalExtension, params};
use serde_json::json;

async fn register_source_candidate(
    store: &Store,
    task_id: &str,
    attempt_id: &str,
    source: &str,
) -> String {
    let source_digest = model::digest(source.as_bytes());
    let source_length = source.len();
    let manifest = json!({
        "schema_version":1,
        "files":[{
            "path":"src/lib.rs",
            "content":source,
            "content_sha256":source_digest,
            "byte_length":source_length,
        }],
    });
    let bytes = model::canonical(&manifest).unwrap().into_bytes();
    let artifact_id = format!("source-{}", model::digest(&bytes));
    let metadata = json!({
        "task_id":task_id,
        "attempt_id":attempt_id,
        "task_revision":1,
        "coverage":"complete",
    });
    let record = ArtifactRecord {
        kind: "source_snapshot".into(),
        artifact_id: artifact_id.clone(),
        relative_path: format!("artifacts/{artifact_id}.bin"),
        byte_length: u64::try_from(bytes.len()).unwrap(),
        content_digest: model::digest(&bytes),
        metadata: metadata.clone(),
    };
    let publish_record = record.clone();
    let publish_bytes = bytes.clone();
    store
        .file_io(move |files| files.publish(&publish_record, &publish_bytes))
        .await
        .unwrap();

    let db_id = record.artifact_id;
    let db_path = record.relative_path;
    let db_length = i64::try_from(record.byte_length).unwrap();
    let db_digest = record.content_digest;
    let db_metadata = model::canonical(&metadata).unwrap();
    let created_at = model::now_ms().unwrap();
    store
        .run(move |db| {
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) \
                 VALUES(?1,?2,'source_snapshot',?3,?4,?5,?6)",
                params![db_id, db_path, db_length, db_digest, created_at, db_metadata],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    artifact_id
}

async fn submit_candidate(
    store: &Store,
    owner: &Principal,
    task_id: &str,
    attempt_id: &str,
    candidate_ref: &str,
    expected_submission_ref: Option<&str>,
    request_id: &str,
) -> ReviewSubject {
    let receipt = store
        .call(
            owner.clone(),
            "task.submit".into(),
            json!({
                "client_request_id":request_id,
                "attempt_id":attempt_id,
                "expected_revision":1,
                "expected_submission_ref":expected_submission_ref,
                "candidate_ref":candidate_ref,
                "summary":"Apply the exact retained candidate for review.",
                "claims":[],
            }),
        )
        .await
        .unwrap();
    let operation_id = receipt["operation_id"].as_str().unwrap().to_owned();
    let operation = store
        .call(
            owner.clone(),
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(operation["method"], "task.submit");
    assert_eq!(operation["state"], "settled");
    assert_eq!(operation["result"]["outcome"], "applied");
    assert_eq!(operation["result"]["attempt_id"], attempt_id);
    assert_eq!(operation["result"]["candidate_ref"], candidate_ref);

    let submission_ref = operation["result"]["submission_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let submission = store
        .call(
            owner.clone(),
            "task.submission".into(),
            json!({"submission_ref":submission_ref}),
        )
        .await
        .unwrap();
    assert_eq!(submission["task_id"], task_id);
    assert_eq!(submission["attempt_id"], attempt_id);
    assert_eq!(submission["candidate_ref"], candidate_ref);
    assert_eq!(
        submission["previous_submission_ref"],
        json!(expected_submission_ref)
    );
    ReviewSubject {
        task_id: task_id.to_owned(),
        attempt_id: attempt_id.to_owned(),
        submission_ref,
        candidate_ref: candidate_ref.to_owned(),
    }
}

fn acceptance_request(subject: &ReviewSubject, request_id: &str) -> serde_json::Value {
    json!({
        "client_request_id":request_id,
        "attempt_id":subject.attempt_id,
        "expected_revision":1,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
        "expected_feedback_observation_id":0,
        "reason":"Accept this exact candidate after its assigned review passed.",
        "reviews":[{
            "requirement_id":"R1",
            "rationale":"The reviewed candidate retains the required evidence.",
            "evidence":[subject.candidate_ref],
        }],
        "check_ids":[],
    })
}

type OperationByteSnapshot = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
);

async fn configure_review_repair_entry(store: &Store, manager_id: &str, automation_id: &str) {
    let manager_id = manager_id.to_owned();
    let automation_id = automation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut entry = crate::automation::config::AutomationEntry::new(
                &manager_id,
                "fixture",
                &automation_id,
                10,
            );
            // The constructor's fourth argument is `now_ms`, not the revision.
            entry.revision = 10;
            entry.enabled = true;
            entry.steps = vec![
                crate::automation::actions::AutomationStep::ReviewDisposition,
                crate::automation::actions::AutomationStep::RepairDispatch,
            ];
            crate::automation::config::validate_entry(&entry)?;
            let key = crate::automation::config::entry_key(&manager_id, "fixture", &automation_id)?;
            crate::automation::config::write_record(&tx, &key, &entry.value()?)?;
            let cut: i64 = tx.query_row(
                "SELECT COALESCE(MAX(observation_id),0) FROM observations",
                [],
                |row| row.get(0),
            )?;
            crate::store::review_disposition::configure_activation(
                &tx, None, &entry, true, cut, 10,
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn set_repair_binding_state(store: &Store, binding_id: &str, state: &str) {
    assert!(matches!(state, "ready" | "reconciling"));
    let binding_id = binding_id.to_owned();
    let state = state.to_owned();
    store
        .run(move |db| {
            let changed = db.execute(
                "UPDATE bindings SET state=?1 WHERE binding_id=?2 AND generation=1 AND released_at_ms IS NULL",
                params![state, binding_id],
            )?;
            assert_eq!(changed, 1, "the exact fixture binding remains retained");
            Ok(())
        })
        .await
        .unwrap();
}

async fn consume_review_disposition_and_repair(
    store: &Store,
    manager_id: &str,
    automation_id: &str,
    assignment_id: &str,
    result_operation_id: &str,
    retained_feedback_operation_id: Option<&str>,
) -> std::result::Result<Value, (Box<crate::error::Error>, Value)> {
    let reconciled = store.reconcile_review_dispositions_once().await.unwrap();
    assert_eq!(
        reconciled["review_results"]["quarantined"], 0,
        "{reconciled}"
    );
    let manager_id = manager_id.to_owned();
    let automation_id = automation_id.to_owned();
    let assignment_id = assignment_id.to_owned();
    let result_operation_id = result_operation_id.to_owned();
    let retained_feedback_operation_id = retained_feedback_operation_id.map(str::to_owned);
    let diagnostic_assignment_id = assignment_id.clone();
    let diagnostic_result_operation_id = result_operation_id.clone();
    let diagnostic_feedback_operation_id = retained_feedback_operation_id.clone();
    let consumed = store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let entry =
                crate::automation::config::load_entry(&tx, &manager_id, "fixture", &automation_id)?
                    .ok_or_else(|| {
                        Error::new("TEST_ENTRY_MISSING", "review repair entry is missing")
                    })?;
            assert_eq!(entry.owner_manager_id, manager_id);
            assert_eq!(entry.project_id, "fixture");
            assert_eq!(entry.automation_id, automation_id);
            assert_eq!(entry.revision, 10);
            assert!(entry.enabled);
            assert_eq!(
                entry.steps,
                vec![
                    crate::automation::actions::AutomationStep::ReviewDisposition,
                    crate::automation::actions::AutomationStep::RepairDispatch,
                ]
            );
            let runtime_config = super::repair_fixture_config();
            let result = crate::store::automation_disposition::consume_review_result_for_entry(
                &tx,
                &runtime_config,
                &entry,
                &assignment_id,
                &result_operation_id,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await;
    match consumed {
        Ok(value) => Ok(value),
        Err(error) => {
            // Read only after the failed transaction has rolled back. This is
            // failure-only evidence for the real Store consumer, not a second
            // disposition or RepairDispatch implementation.
            let diagnostic = store
                .run(move |db| {
                    repair_boundary_failure_snapshot(
                        db,
                        &diagnostic_assignment_id,
                        &diagnostic_result_operation_id,
                        diagnostic_feedback_operation_id.as_deref(),
                    )
                })
                .await
                .unwrap_or_else(|snapshot_error| {
                    json!({"diagnostic_snapshot_error":format!("{snapshot_error:#?}")})
                });
            Err((Box::new(error), diagnostic))
        }
    }
}

fn expect_review_disposition_consumer_success(
    result: std::result::Result<Value, (Box<crate::error::Error>, Value)>,
) -> Value {
    match result {
        Ok(value) => value,
        Err((error, diagnostic)) => panic!(
            "real ReviewDisposition -> RepairDispatch consumer failed; structured error: {error:#?}\nexpected-versus-retained validator evidence:\n{}",
            serde_json::to_string_pretty(&diagnostic).unwrap_or_else(|format_error| {
                format!("{diagnostic:#?}; JSON error: {format_error}")
            })
        ),
    }
}

fn diagnostic_observation(
    db: &rusqlite::Connection,
    event_key: &str,
    kind: &str,
) -> crate::error::Result<Value> {
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 AND kind=?2 \
             ORDER BY observation_id DESC LIMIT 1",
            params![event_key, kind],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(row.map_or(Value::Null, |(payload_json, operation_id)| {
        let payload = serde_json::from_str(&payload_json).unwrap_or(Value::Null);
        json!({
            "operation_id":operation_id,
            "payload_json":payload_json,
            "payload":payload
        })
    }))
}

fn diagnostic_operation(
    db: &rusqlite::Connection,
    operation_id: &str,
) -> crate::error::Result<Value> {
    type DiagnosticOperationRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        Option<String>,
        String,
    );
    let row: Option<DiagnosticOperationRow> = db
        .query_row(
            "SELECT caller_id,method,state,result_json,task_id,attempt_id,original_request_json,\
                    effective_request_json,client_request_id FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                ))
            },
        )
        .optional()?;
    Ok(row.map_or(
        Value::Null,
        |(
            caller_id,
            method,
            state,
            result_json,
            task_id,
            attempt_id,
            original_request_json,
            effective_request_json,
            client_request_id,
        )| {
            let result = result_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or(Value::Null);
            let original_request =
                serde_json::from_str(&original_request_json).unwrap_or(Value::Null);
            let effective_request = effective_request_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or(Value::Null);
            json!({
                "operation_id":operation_id,
                "caller_id":caller_id,
                "method":method,
                "state":state,
                "task_id":task_id,
                "attempt_id":attempt_id,
                "client_request_id":client_request_id,
                "original_request_json":original_request_json,
                "original_request":original_request,
                "effective_request_json":effective_request_json,
                "effective_request":effective_request,
                "result_json":result_json,
                "result":result
            })
        },
    ))
}

fn diagnostic_operation_link(
    db: &rusqlite::Connection,
    operation_id: &str,
) -> crate::error::Result<Value> {
    let key = crate::automation::config::operation_link_key(operation_id)?;
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
            row.get(0)
        })
        .optional()?;
    Ok(raw.map_or(Value::Null, |raw_value_json| {
        let envelope = serde_json::from_str(&raw_value_json).unwrap_or(Value::Null);
        let record = envelope.get("record").cloned().unwrap_or(Value::Null);
        let cause = record.get("cause").cloned().unwrap_or(Value::Null);
        json!({
            "key":key,
            "raw_value_json":raw_value_json,
            "envelope":envelope,
            "record":record,
            "cause":cause
        })
    }))
}

fn diagnostic_equality(predicate: &str, expected: Value, retained: Value) -> Value {
    let matches = expected == retained;
    json!({
        "predicate":predicate,
        "expected":expected,
        "retained":retained,
        "matches":matches
    })
}

fn repair_boundary_failure_snapshot(
    db: &rusqlite::Connection,
    current_assignment_id: &str,
    current_result_operation_id: &str,
    expected_feedback_operation_id: Option<&str>,
) -> crate::error::Result<Value> {
    let current_assignment = diagnostic_observation(
        db,
        &format!("assignment:{current_assignment_id}"),
        "review.assignment",
    )?;
    let current_result = diagnostic_observation(
        db,
        &format!("result:{current_assignment_id}"),
        "review.result",
    )?;
    let current_disposition = diagnostic_observation(
        db,
        &format!("disposition:{current_assignment_id}"),
        "review.disposition",
    )?;
    let current_identity = current_assignment["payload"]["identity"].clone();
    let current_findings = current_result["payload"]["result"]["findings"].clone();
    let current_package = match (
        serde_json::from_value::<crate::review::ReviewSlotIdentity>(current_identity.clone()),
        serde_json::from_value::<Vec<crate::review::ReviewFinding>>(current_findings.clone()),
    ) {
        (Ok(identity), Ok(findings)) => crate::review::ReviewFindingsPackage::new(
            identity,
            current_assignment_id.to_owned(),
            current_result_operation_id.to_owned(),
            findings,
        )
        .ok(),
        _ => None,
    };
    let current_package_value = current_package
        .as_ref()
        .and_then(|package| serde_json::to_value(package).ok())
        .unwrap_or(Value::Null);
    let current_content_digest = current_package
        .as_ref()
        .and_then(|package| package.correction_content_digest().ok());

    let current_disposition_payload = current_disposition
        .get("payload")
        .cloned()
        .unwrap_or(Value::Null);
    let feedback_operation_id = expected_feedback_operation_id
        .map(str::to_owned)
        .or_else(|| {
            current_disposition_payload
                .get("task_feedback_operation_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        });
    let feedback_operation = match feedback_operation_id.as_deref() {
        Some(operation_id) => diagnostic_operation(db, operation_id)?,
        None => Value::Null,
    };
    let feedback_link = match feedback_operation_id.as_deref() {
        Some(operation_id) => diagnostic_operation_link(db, operation_id)?,
        None => Value::Null,
    };
    let feedback_result = feedback_operation
        .get("result")
        .cloned()
        .unwrap_or(Value::Null);
    let feedback_request = feedback_operation
        .get("original_request")
        .cloned()
        .unwrap_or(Value::Null);
    let retained_package = feedback_result
        .get("findings_package")
        .cloned()
        .unwrap_or(Value::Null);
    let retained_package_typed =
        serde_json::from_value::<crate::review::ReviewFindingsPackage>(retained_package.clone())
            .ok();
    let retained_content_digest = retained_package_typed
        .as_ref()
        .and_then(|package| package.correction_content_digest().ok());
    let retained_assignment_id = retained_package
        .get("review_assignment_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let retained_result_operation_id = retained_package
        .get("review_result_operation_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let retained_assignment = diagnostic_observation(
        db,
        &format!("assignment:{retained_assignment_id}"),
        "review.assignment",
    )?;
    let retained_result = diagnostic_observation(
        db,
        &format!("result:{retained_assignment_id}"),
        "review.result",
    )?;
    let retained_disposition = diagnostic_observation(
        db,
        &format!("disposition:{retained_assignment_id}"),
        "review.disposition",
    )?;
    let retained_assignment_operation = match retained_assignment["operation_id"].as_str() {
        Some(operation_id) => diagnostic_operation(db, operation_id)?,
        None => Value::Null,
    };
    let retained_result_operation = if retained_result_operation_id.is_empty() {
        Value::Null
    } else {
        diagnostic_operation(db, retained_result_operation_id)?
    };
    let retained_disposition_operation = match retained_disposition["operation_id"].as_str() {
        Some(operation_id) => diagnostic_operation(db, operation_id)?,
        None => Value::Null,
    };
    let link_record = feedback_link.get("record").cloned().unwrap_or(Value::Null);
    let link_cause = feedback_link.get("cause").cloned().unwrap_or(Value::Null);
    let request_package = feedback_request
        .get("package")
        .cloned()
        .unwrap_or(Value::Null);

    let checks = vec![
        diagnostic_equality(
            "current assignment payload ID matches the requested assignment",
            json!(current_assignment_id),
            current_assignment["payload"]["review_assignment_id"].clone(),
        ),
        diagnostic_equality(
            "current result observation operation matches the requested result",
            json!(current_result_operation_id),
            current_result["operation_id"].clone(),
        ),
        diagnostic_equality(
            "current result assignment matches the requested assignment",
            json!(current_assignment_id),
            current_result["payload"]["review_assignment_id"].clone(),
        ),
        diagnostic_equality(
            "retained package identity matches current review identity",
            current_identity.clone(),
            retained_package["identity"].clone(),
        ),
        diagnostic_equality(
            "retained package ordered findings match current review findings",
            current_package_value["findings"].clone(),
            retained_package["findings"].clone(),
        ),
        diagnostic_equality(
            "retained and current correction-content digests match",
            current_content_digest
                .as_deref()
                .map(|value| json!(value))
                .unwrap_or(Value::Null),
            retained_content_digest
                .as_deref()
                .map(|value| json!(value))
                .unwrap_or(Value::Null),
        ),
        diagnostic_equality(
            "feedback request package matches retained feedback result package",
            retained_package.clone(),
            request_package,
        ),
        diagnostic_equality(
            "feedback request client ID matches retained Operation client ID",
            feedback_request
                .get("client_request_id")
                .cloned()
                .unwrap_or(Value::Null),
            feedback_operation
                .get("client_request_id")
                .cloned()
                .unwrap_or(Value::Null),
        ),
        diagnostic_equality(
            "feedback link action matches task.request_changes",
            json!("task.request_changes"),
            link_record["action"].clone(),
        ),
        diagnostic_equality(
            "feedback link cause kind matches the ReviewDisposition producer",
            json!("review_result"),
            link_cause["kind"].clone(),
        ),
        diagnostic_equality(
            "feedback link cause id matches retained package assignment",
            json!(retained_assignment_id),
            link_cause["id"].clone(),
        ),
        diagnostic_equality(
            "feedback link cause review_assignment_id matches retained package assignment",
            json!(retained_assignment_id),
            link_cause["review_assignment_id"].clone(),
        ),
        diagnostic_equality(
            "feedback link cause operation_id matches retained package result operation",
            json!(retained_result_operation_id),
            link_cause["operation_id"].clone(),
        ),
        diagnostic_equality(
            "feedback link cause identity matches retained package identity",
            retained_package["identity"].clone(),
            link_cause["identity"].clone(),
        ),
        diagnostic_equality(
            "retained assignment observation matches retained package assignment",
            json!(retained_assignment_id),
            retained_assignment["payload"]["review_assignment_id"].clone(),
        ),
        diagnostic_equality(
            "retained result observation operation matches retained package result",
            json!(retained_result_operation_id),
            retained_result["operation_id"].clone(),
        ),
    ];
    let false_validator_predicates = checks
        .iter()
        .filter(|check| check.get("matches") == Some(&Value::Bool(false)))
        .filter_map(|check| check.get("predicate").cloned())
        .collect::<Vec<_>>();

    Ok(json!({
        "expected_current":{
            "review_assignment_id":current_assignment_id,
            "review_result_operation_id":current_result_operation_id,
            "identity":current_identity,
            "review_findings":current_findings,
            "reconstructed_findings_package":current_package_value
        },
        "retained_feedback":{
            "operation_id":feedback_operation_id,
            "operation":feedback_operation,
            "operation_link_exact_bytes_and_record":feedback_link,
            "findings_package":retained_package,
            "correction_content_digest":retained_content_digest,
            "retained_assignment_observation":retained_assignment,
            "retained_assignment_operation":retained_assignment_operation,
            "retained_result_observation":retained_result,
            "retained_result_operation":retained_result_operation,
            "retained_disposition_observation":retained_disposition,
            "retained_disposition_operation":retained_disposition_operation
        },
        "current_persisted_after_rollback":{
            "assignment_observation":current_assignment,
            "review_result_observation":current_result,
            "disposition_observation":current_disposition
        },
        "provenance_comparison":{
            "current_assignment_id":current_assignment_id,
            "retained_package_assignment_id":retained_assignment_id,
            "assignment_provenance_differs_as_expected":current_assignment_id != retained_assignment_id,
            "current_result_operation_id":current_result_operation_id,
            "retained_package_result_operation_id":retained_result_operation_id,
            "result_provenance_differs_as_expected":current_result_operation_id != retained_result_operation_id
        },
        "validator_predicate_checks":checks,
        "false_validator_predicates":false_validator_predicates
    }))
}

async fn operation_byte_snapshot(store: &Store, operation_id: &str) -> OperationByteSnapshot {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT caller_id,method,client_request_id,original_request_json,effective_request_json,result_json,state \
                 FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )?)
        })
        .await
        .unwrap()
}

async fn meta_json_bytes(store: &Store, key: &str) -> String {
    let key = key.to_owned();
    store
        .run(move |db| {
            Ok(
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?,
            )
        })
        .await
        .unwrap()
}

async fn observation_payload_bytes(
    store: &Store,
    stream: &str,
    kind: &str,
    operation_id: &str,
) -> String {
    let stream = stream.to_owned();
    let kind = kind.to_owned();
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT payload_json FROM observations \
                 WHERE source_stream_id=?1 AND kind=?2 AND operation_id=?3 \
                 ORDER BY observation_id DESC LIMIT 1",
                params![stream, kind, operation_id],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn disposition_payload_bytes(store: &Store, assignment_id: &str) -> String {
    let key = format!("disposition:{assignment_id}");
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT payload_json FROM observations \
                 WHERE source_stream_id='controller:review' AND source_event_key=?1 \
                   AND kind='review.disposition'",
                [&key],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn delivered_feedback_count(store: &Store) -> i64 {
    store
        .run(|db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM observations \
                 WHERE source_stream_id='controller:review' AND kind='task.feedback' \
                   AND json_extract(payload_json,'$.message_id') IS NOT NULL",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn repair_dispatch_count_for_attempt(store: &Store, attempt_id: &str) -> i64 {
    let attempt_id = attempt_id.to_owned();
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM operations WHERE method='agent.send' \
                   AND json_extract(effective_request_json,'$.automation_on_behalf.cause.identity.attempt_id')=?1",
                [&attempt_id],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

async fn disposition_payload(store: &Store, assignment_id: &str) -> Value {
    let key = format!("disposition:{assignment_id}");
    store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT payload_json FROM observations \
                 WHERE source_stream_id='controller:review' AND source_event_key=?1 \
                   AND kind='review.disposition'",
                [&key],
                |row| row.get(0),
            )?;
            Ok(serde_json::from_str(&raw)?)
        })
        .await
        .unwrap()
}

async fn repair_operation_for_review(
    store: &Store,
    assignment_id: &str,
    result_operation_id: &str,
) -> String {
    let assignment_id = assignment_id.to_owned();
    let result_operation_id = result_operation_id.to_owned();
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT operation_id FROM operations WHERE method='agent.send' \
                 AND json_extract(effective_request_json,'$.automation_on_behalf.cause.review_assignment_id')=?1 \
                 AND json_extract(effective_request_json,'$.automation_on_behalf.cause.review_result_operation_id')=?2 \
                 ORDER BY created_at_ms,operation_id LIMIT 1",
                params![assignment_id, result_operation_id],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn same_attempt_return_repair_fresh_review_and_acceptance_bind_to_candidate_b() {
    let (owner, directory, bootstrap) = start_store("o7-same-attempt-cycle").await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    seed_clients(&owner.store).await;
    owner
        .store
        .run(|db| super::super::set_meta(db, "execution_mode", &json!({"new_work":"enabled"})))
        .await
        .unwrap();

    let task_owner = principal("review-owner-v2", Role::Manager);
    let current_gm = principal("review-gm", Role::Manager);
    let created = owner
        .store
        .call(
            operator,
            "task.create".into(),
            json!({
                "client_request_id":"create-o7-correction-cycle",
                "project_id":"fixture",
                "spec":{
                    "objective":"Correct one reviewed candidate and accept only its successor.",
                    "phase":"implementation",
                    "requirements":[{
                        "id":"R1",
                        "statement":"Candidate B contains the retained implementation evidence.",
                        "verification":{"review_required":true,"check_profiles":[]},
                    }],
                    "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                    "acceptance":{"schema_id":"swarm.acceptance_policy","schema_version":2},
                },
            }),
        )
        .await
        .unwrap();
    assert_eq!(created["created"], true);
    let task_id = created["task_id"].as_str().unwrap().to_owned();
    let claimed = owner
        .store
        .call(
            task_owner.clone(),
            "task.claim".into(),
            json!({
                "client_request_id":"claim-o7-correction-cycle",
                "task_id":task_id,
                "expected_revision":1,
                "owner_id":task_owner.client_id,
            }),
        )
        .await
        .unwrap();
    let attempt_id = claimed["attempt_id"].as_str().unwrap().to_owned();

    // Candidate bytes are fixture inputs; both submission documents and their
    // applied Operation/readback are produced by the real Store task.submit path.
    let candidate_a = register_source_candidate(
        &owner.store,
        &task_id,
        &attempt_id,
        "Candidate A is missing the required implementation evidence.",
    )
    .await;
    let submission_a = submit_candidate(
        &owner.store,
        &task_owner,
        &task_id,
        &attempt_id,
        &candidate_a,
        None,
        "submit-o7-candidate-a",
    )
    .await;
    let (reviewer_a, assignment_a) = assign_sponsored_reviewer(
        &owner.store,
        task_owner.clone(),
        &submission_a,
        "o7-candidate-a-reviewer",
        "o7-candidate-a-reviewer-token",
    )
    .await;
    let result_a_request =
        review_result_request(&submission_a, &assignment_a, "review-o7-candidate-a");
    let result_a = owner
        .store
        .call(reviewer_a, "review.submit".into(), result_a_request)
        .await
        .unwrap();
    assert_eq!(result_a["verdict"], "changes_requested");
    assert_eq!(result_a["task_transition"], "none");
    assert_eq!(result_a["acceptance_changed"], false);
    assert_eq!(result_a["publication_started"], false);

    let returned = owner
        .store
        .call(
            task_owner.clone(),
            "task.request_changes".into(),
            json!({
                "schema_version":2,
                "client_request_id":"return-o7-candidate-a",
                "attempt_id":attempt_id,
                "expected_revision":1,
                "submission_ref":submission_a.submission_ref,
                "candidate_ref":candidate_a,
                "review_assignment_id":assignment_a,
                "review_result_operation_id":result_a["operation_id"].as_str().unwrap(),
                "finding_ids":["missing-r1-evidence"],
            }),
        )
        .await
        .unwrap();
    assert_eq!(returned["applied"], true);
    assert_eq!(returned["status"], "needs_correction");
    assert_eq!(
        returned["review_provenance"]["review_assignment_id"],
        assignment_a
    );
    let after_return = owner
        .store
        .call(
            task_owner.clone(),
            "attempt.get".into(),
            json!({"attempt_id":attempt_id}),
        )
        .await
        .unwrap();
    assert_eq!(after_return["attempt_id"], attempt_id);
    assert_eq!(after_return["state"], "needs_correction");
    let task_after_return = owner
        .store
        .call(
            task_owner.clone(),
            "task.get".into(),
            json!({"task_id":task_id}),
        )
        .await
        .unwrap();
    assert_eq!(task_after_return["state"], "open");
    assert!(task_after_return["accepted_operation_id"].is_null());

    let candidate_b = register_source_candidate(
        &owner.store,
        &task_id,
        &attempt_id,
        "Candidate B adds the required retained implementation evidence.",
    )
    .await;
    let submission_b = submit_candidate(
        &owner.store,
        &task_owner,
        &task_id,
        &attempt_id,
        &candidate_b,
        Some(&submission_a.submission_ref),
        "submit-o7-candidate-b",
    )
    .await;
    assert_eq!(submission_b.task_id, submission_a.task_id);
    assert_eq!(submission_b.attempt_id, submission_a.attempt_id);
    assert_ne!(submission_b.submission_ref, submission_a.submission_ref);
    assert_ne!(submission_b.candidate_ref, submission_a.candidate_ref);

    let (reviewer_b, assignment_b) = assign_sponsored_reviewer(
        &owner.store,
        task_owner.clone(),
        &submission_b,
        "o7-candidate-b-reviewer",
        "o7-candidate-b-reviewer-token",
    )
    .await;
    assert_ne!(assignment_b, assignment_a);
    let result_b = owner
        .store
        .call(
            reviewer_b,
            "review.submit".into(),
            passing_review_result_request(&submission_b, &assignment_b, "review-o7-candidate-b"),
        )
        .await
        .unwrap();
    assert_eq!(result_b["verdict"], "pass");
    assert_eq!(result_b["coverage"], "complete");
    assert_eq!(result_b["submission_ref"], submission_b.submission_ref);
    assert_eq!(result_b["candidate_ref"], candidate_b);

    let old_review = owner
        .store
        .call(
            task_owner.clone(),
            "review.get".into(),
            json!({"review_assignment_id":assignment_a}),
        )
        .await
        .unwrap();
    assert_eq!(old_review["result"]["verdict"], "changes_requested");
    assert_eq!(
        old_review["assignment"]["identity"]["candidate_ref"],
        candidate_a
    );
    assert_eq!(old_review["current_candidate"], false);
    let new_review = owner
        .store
        .call(
            task_owner.clone(),
            "review.get".into(),
            json!({"review_assignment_id":assignment_b}),
        )
        .await
        .unwrap();
    assert_eq!(new_review["result"]["verdict"], "pass");
    assert_eq!(
        new_review["assignment"]["identity"]["submission_ref"],
        submission_b.submission_ref
    );
    assert_eq!(
        new_review["assignment"]["identity"]["candidate_ref"],
        candidate_b
    );
    assert_eq!(new_review["current_candidate"], true);

    let stale_acceptance = owner
        .store
        .call(
            current_gm.clone(),
            "task.accept".into(),
            acceptance_request(&submission_a, "accept-stale-o7-candidate-a"),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_acceptance.code, "STALE_SUBMISSION");
    let before_acceptance = owner
        .store
        .call(
            current_gm.clone(),
            "task.get".into(),
            json!({"task_id":task_id}),
        )
        .await
        .unwrap();
    assert_eq!(before_acceptance["state"], "open");
    assert!(before_acceptance["accepted_operation_id"].is_null());

    let acceptance_receipt = owner
        .store
        .call(
            current_gm.clone(),
            "task.accept".into(),
            acceptance_request(&submission_b, "accept-o7-candidate-b"),
        )
        .await
        .unwrap();
    let acceptance_id = acceptance_receipt["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let acceptance = owner
        .store
        .call(
            current_gm.clone(),
            "operation.get".into(),
            json!({"operation_id":acceptance_id}),
        )
        .await
        .unwrap();
    assert_eq!(acceptance["method"], "task.accept");
    assert_eq!(acceptance["state"], "settled");
    assert_eq!(acceptance["result"]["outcome"], "applied");
    assert_eq!(acceptance["result"]["attempt_id"], attempt_id);
    assert_eq!(
        acceptance["result"]["submission_ref"],
        submission_b.submission_ref
    );
    assert_eq!(acceptance["result"]["candidate_ref"], candidate_b);
    let accepted_task = owner
        .store
        .call(current_gm, "task.get".into(), json!({"task_id":task_id}))
        .await
        .unwrap();
    assert_eq!(accepted_task["accepted_operation_id"], acceptance_id);
    assert_eq!(accepted_task["accepted_attempt_id"], attempt_id);
    assert_eq!(accepted_task["accepted_candidate_ref"], candidate_b);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn delayed_first_repair_reuses_middle_same_content_slot_after_feedback_without_repair() {
    let (owner, directory, bootstrap) = start_store_with_config(
        "o7-cross-review-repair-content",
        super::repair_fixture_config(),
    )
    .await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    seed_clients(&owner.store).await;
    let task_owner = principal("review-owner-v2", Role::Manager);
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({
                "client_request_id":"handover-o7-cross-review-repair",
                "client_id":task_owner.client_id
            }),
        )
        .await
        .unwrap();
    let subject = seed_subject(
        &owner.store,
        "o7-cross-review-repair",
        &task_owner.client_id,
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    add_submission_source_fact(&owner.store, "o7-cross-review-repair", &subject).await;
    let attempt_id = subject.attempt_id.clone();
    let binding_id = attach_ready_repair_binding(&owner.store, &subject).await;
    set_repair_binding_state(&owner.store, &binding_id, "reconciling").await;

    let (reviewer_one, assignment_one) = assign_sponsored_reviewer(
        &owner.store,
        task_owner.clone(),
        &subject,
        "o7-cross-review-repair-reviewer-one",
        "o7-cross-review-repair-token-one",
    )
    .await;
    let reviewer_two_id = "o7-cross-review-repair-reviewer-two";
    let reviewer_two_token = "o7-cross-review-repair-token-two";
    let pending_scope = json!({
        "review_assignment_id":null,
        "task_id":subject.task_id,
        "attempt_id":subject.attempt_id,
        "task_revision":1,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
    });
    owner
        .store
        .call(
            task_owner.clone(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":"register-o7-cross-review-repair-reviewer-two",
                "client_id":reviewer_two_id,
                "token_hash":model::digest(reviewer_two_token.as_bytes()),
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "participation_basis":{
                    "kind":"sponsored_reviewer",
                    "review_scope":pending_scope
                },
            }),
        )
        .await
        .unwrap();

    let reviewer_three_id = "o7-cross-review-repair-reviewer-three";
    let reviewer_three_token = "o7-cross-review-repair-token-three";
    let pending_scope_three = json!({
        "review_assignment_id":null,
        "task_id":subject.task_id,
        "attempt_id":subject.attempt_id,
        "task_revision":1,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
    });
    owner
        .store
        .call(
            task_owner.clone(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":"register-o7-cross-review-repair-reviewer-three",
                "client_id":reviewer_three_id,
                "token_hash":model::digest(reviewer_three_token.as_bytes()),
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "participation_basis":{
                    "kind":"sponsored_reviewer",
                    "review_scope":pending_scope_three
                },
            }),
        )
        .await
        .unwrap();

    let automation_id = "o7-cross-review-repair";
    configure_review_repair_entry(&owner.store, &task_owner.client_id, automation_id).await;
    let result_one = owner
        .store
        .call(
            reviewer_one,
            "review.submit".into(),
            review_result_request(
                &subject,
                &assignment_one,
                "review-o7-cross-review-repair-one",
            ),
        )
        .await
        .unwrap();
    assert_eq!(result_one["verdict"], "changes_requested");
    let result_one_id = result_one["operation_id"].as_str().unwrap().to_owned();

    let first = expect_review_disposition_consumer_success(
        consume_review_disposition_and_repair(
            &owner.store,
            &task_owner.client_id,
            automation_id,
            &assignment_one,
            &result_one_id,
            None,
        )
        .await,
    );
    assert_eq!(first["disposition_applied"], true, "{first}");
    assert_eq!(first["review_assignment_id"], assignment_one);
    assert_eq!(first["review_result_operation_id"], result_one_id);
    assert_eq!(first["repair_dispatch"]["status"], "pending");
    assert_eq!(
        first["repair_dispatch"]["reason"],
        "current_owner_binding_not_ready"
    );
    assert_eq!(
        first["repair_dispatch"]["wake_when"],
        json!(["owner_binding_ready"])
    );
    assert_eq!(
        repair_dispatch_count_for_attempt(&owner.store, &attempt_id).await,
        0
    );
    let feedback_operation_id = first["feedback"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(delivered_feedback_count(&owner.store).await, 1);
    let feedback_before = operation_byte_snapshot(&owner.store, &feedback_operation_id).await;
    assert_eq!(feedback_before.1, "task.request_changes");
    assert_eq!(feedback_before.6, "settled");

    let feedback_request: Value = serde_json::from_str(&feedback_before.3).unwrap();
    let retained_feedback_package: crate::review::ReviewFindingsPackage =
        serde_json::from_value(feedback_request["package"].clone()).unwrap();
    assert_eq!(
        retained_feedback_package.review_assignment_id,
        assignment_one
    );
    let content_feedback_request_id = model::digest(
        model::canonical(&json!([
            task_owner.client_id,
            subject.submission_ref,
            retained_feedback_package
                .correction_content_digest()
                .unwrap()
        ]))
        .unwrap()
        .as_bytes(),
    );
    assert_eq!(feedback_before.2, content_feedback_request_id);
    let feedback_result: Value =
        serde_json::from_str(feedback_before.5.as_deref().unwrap()).unwrap();
    assert_eq!(
        feedback_result["findings_package"]["review_assignment_id"],
        assignment_one
    );

    let feedback_link_key =
        crate::automation::config::operation_link_key(&feedback_operation_id).unwrap();
    let feedback_link_before = meta_json_bytes(&owner.store, &feedback_link_key).await;
    let feedback_link_envelope: Value = serde_json::from_str(&feedback_link_before).unwrap();
    assert_eq!(feedback_link_envelope["schema_version"], 1);
    let feedback_link = feedback_link_envelope["record"].clone();
    assert_eq!(feedback_link["schema_version"], 1);
    assert_eq!(feedback_link["operation_id"], feedback_operation_id);
    assert_eq!(feedback_link["action"], "task.request_changes");
    assert_eq!(feedback_link["cause"]["kind"], "review_result");
    assert_eq!(feedback_link["cause"]["id"], assignment_one);
    assert_eq!(feedback_link["cause"]["operation_id"], result_one_id);
    let feedback_observation_before = observation_payload_bytes(
        &owner.store,
        "controller:review",
        "task.feedback",
        &feedback_operation_id,
    )
    .await;
    let first_disposition_before = disposition_payload(&owner.store, &assignment_one).await;
    let first_disposition_bytes = disposition_payload_bytes(&owner.store, &assignment_one).await;
    assert_eq!(
        first_disposition_before["task_feedback_operation_id"],
        feedback_operation_id
    );
    set_repair_binding_state(&owner.store, &binding_id, "ready").await;

    let replacement = owner
        .store
        .call(
            task_owner.clone(),
            "review.assign".into(),
            json!({
                "client_request_id":"assign-o7-cross-review-repair-reviewer-two",
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "reviewer_client_id":reviewer_two_id,
                "replaces_review_assignment_id":assignment_one,
                "replacement_reason":"an independent second review of the same candidate is retained",
                "replacement_evidence_refs":[subject.candidate_ref],
            }),
        )
        .await
        .unwrap();
    let assignment_two = replacement["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(assignment_two, assignment_one);
    let reviewer_two = owner
        .store
        .authenticate(crate::model::Credential {
            client_id: reviewer_two_id.to_owned(),
            token: reviewer_two_token.to_owned(),
        })
        .await
        .unwrap();
    let result_two = owner
        .store
        .call(
            reviewer_two,
            "review.submit".into(),
            review_result_request(
                &subject,
                &assignment_two,
                "review-o7-cross-review-repair-two",
            ),
        )
        .await
        .unwrap();
    assert_eq!(result_two["verdict"], "changes_requested");
    let result_two_id = result_two["operation_id"].as_str().unwrap().to_owned();

    // This enters the current committed automation entry through the real
    // ReviewDisposition caller, which validates the retained feedback package
    // before its RepairDispatch consumer resolves the original slot.
    let second = expect_review_disposition_consumer_success(
        consume_review_disposition_and_repair(
            &owner.store,
            &task_owner.client_id,
            automation_id,
            &assignment_two,
            &result_two_id,
            Some(&feedback_operation_id),
        )
        .await,
    );
    assert_eq!(second["disposition_applied"], true, "{second}");
    assert_eq!(second["review_assignment_id"], assignment_two);
    assert_eq!(second["review_result_operation_id"], result_two_id);
    assert_eq!(second["feedback"]["operation_id"], feedback_operation_id);
    assert_eq!(second["feedback"]["resend_suppressed"], true);
    assert_eq!(second["repair_dispatch"]["status"], "pending");
    assert_eq!(second["repair_dispatch"]["delivery_verified"], false);
    assert_eq!(
        second["repair_dispatch"]["wake_when"],
        json!(["exact_delivery_operation_readback"])
    );
    assert_eq!(
        second["repair_dispatch"]["review_assignment_id"],
        assignment_two
    );
    assert_eq!(
        second["repair_dispatch"]["review_result_operation_id"],
        result_two_id
    );

    let repair_operation_id =
        repair_operation_for_review(&owner.store, &assignment_two, &result_two_id).await;
    assert_eq!(
        repair_dispatch_count_for_attempt(&owner.store, &attempt_id).await,
        1
    );
    let repair_before = operation_byte_snapshot(&owner.store, &repair_operation_id).await;
    let repair_request: Value = serde_json::from_str(&repair_before.3).unwrap();
    assert_eq!(repair_request["binding_id"], binding_id);
    assert_eq!(repair_request["generation"], 1);
    let repair_link_key = format!("repair:v1:operation-link:{repair_operation_id}");
    let repair_link_before = meta_json_bytes(&owner.store, &repair_link_key).await;
    let repair_link_envelope: Value = serde_json::from_str(&repair_link_before).unwrap();
    assert_eq!(repair_link_envelope["schema_version"], 1);
    let repair_link = repair_link_envelope["record"].clone();
    assert_eq!(
        repair_link["schema_version"], 2,
        "the retained RepairDispatch record has its own schema version inside the sealed v1 envelope"
    );
    assert_eq!(repair_link["operation_id"], repair_operation_id);
    assert_eq!(repair_link["review_assignment_id"], assignment_two);
    assert_eq!(repair_link["review_result_operation_id"], result_two_id);
    let package_two: crate::review::ReviewFindingsPackage =
        serde_json::from_value(repair_link["findings_package"].clone()).unwrap();
    assert_eq!(
        package_two.findings_digest,
        second["disposition"]["findings_digest"]
    );
    let content_digest_a = retained_feedback_package
        .correction_content_digest()
        .unwrap();
    let content_digest_b = package_two.correction_content_digest().unwrap();
    assert_eq!(content_digest_a, content_digest_b);
    let repair_slot_key = format!(
        "repair:v1:semantic-slot:{}",
        repair_link["semantic_slot_id"].as_str().unwrap()
    );
    let repair_slot_before = meta_json_bytes(&owner.store, &repair_slot_key).await;

    let second_disposition = disposition_payload(&owner.store, &assignment_two).await;
    assert_eq!(second_disposition["review_assignment_id"], assignment_two);
    assert_eq!(
        second_disposition["review_result_operation_id"],
        result_two_id
    );
    assert_eq!(
        second_disposition["task_feedback_operation_id"],
        feedback_operation_id
    );
    assert_eq!(
        second_disposition["identity"],
        first_disposition_before["identity"]
    );
    assert_ne!(
        second["disposition"]["findings_digest"], feedback_result["findings_digest"],
        "the current review cause retains its own provenance digest"
    );
    assert_eq!(delivered_feedback_count(&owner.store).await, 1);
    assert_eq!(
        repair_operation_for_review(&owner.store, &assignment_two, &result_two_id).await,
        repair_operation_id
    );

    let replacement_three = owner
        .store
        .call(
            task_owner.clone(),
            "review.assign".into(),
            json!({
                "client_request_id":"assign-o7-cross-review-repair-reviewer-three",
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "reviewer_client_id":reviewer_three_id,
                "replaces_review_assignment_id":assignment_two,
                "replacement_reason":"a third review retains the same ordered correction content",
                "replacement_evidence_refs":[subject.candidate_ref],
            }),
        )
        .await
        .unwrap();
    let assignment_three = replacement_three["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(assignment_three, assignment_two);
    let reviewer_three = owner
        .store
        .authenticate(crate::model::Credential {
            client_id: reviewer_three_id.to_owned(),
            token: reviewer_three_token.to_owned(),
        })
        .await
        .unwrap();
    let result_three = owner
        .store
        .call(
            reviewer_three,
            "review.submit".into(),
            review_result_request(
                &subject,
                &assignment_three,
                "review-o7-cross-review-repair-three",
            ),
        )
        .await
        .unwrap();
    assert_eq!(result_three["verdict"], "changes_requested");
    let result_three_id = result_three["operation_id"].as_str().unwrap().to_owned();
    let third_consumed = consume_review_disposition_and_repair(
        &owner.store,
        &task_owner.client_id,
        automation_id,
        &assignment_three,
        &result_three_id,
        Some(&feedback_operation_id),
    )
    .await;
    let package_three = crate::review::ReviewFindingsPackage::new(
        package_two.identity.clone(),
        assignment_three.clone(),
        result_three_id.clone(),
        package_two.findings.clone(),
    )
    .unwrap();
    let content_digest = content_digest_b;
    let third = match third_consumed {
        Ok(value) => value,
        Err((error, diagnostic)) => panic!(
            "third same-content consumer failed after B queued its RepairDispatch; A/B/C provenance digests={}, {}, {}; common correction_content_digest={}; error={error:?}; diagnostic={diagnostic}",
            retained_feedback_package.findings_digest,
            package_two.findings_digest,
            package_three.findings_digest,
            content_digest,
        ),
    };
    assert_eq!(third["disposition_applied"], true, "{third}");
    assert_eq!(third["review_assignment_id"], assignment_three);
    assert_eq!(third["review_result_operation_id"], result_three_id);
    assert_eq!(third["feedback"]["operation_id"], feedback_operation_id);
    assert_eq!(third["feedback"]["resend_suppressed"], true);
    assert_eq!(third["repair_dispatch"]["status"], "pending");
    assert_eq!(third["repair_dispatch"]["delivery_verified"], false);
    assert_eq!(
        third["repair_dispatch"]["wake_when"],
        json!(["exact_delivery_operation_readback"])
    );
    assert_eq!(
        third["repair_dispatch"]["review_assignment_id"],
        assignment_three
    );
    assert_eq!(
        third["repair_dispatch"]["review_result_operation_id"],
        result_three_id
    );
    let third_disposition = disposition_payload(&owner.store, &assignment_three).await;
    assert_eq!(
        third_disposition["task_feedback_operation_id"],
        feedback_operation_id
    );
    assert_ne!(package_two.findings_digest, package_three.findings_digest);
    assert_eq!(
        retained_feedback_package
            .correction_content_digest()
            .unwrap(),
        content_digest
    );
    assert_eq!(
        package_three.correction_content_digest().unwrap(),
        content_digest
    );
    assert_eq!(
        repair_dispatch_count_for_attempt(&owner.store, &attempt_id).await,
        1,
        "C must reuse B's exact unresolved slot and Operation; A/B/C provenance digests={}, {}, {}; common correction_content_digest={content_digest}",
        retained_feedback_package.findings_digest,
        package_two.findings_digest,
        package_three.findings_digest,
    );
    assert_eq!(
        repair_operation_for_review(&owner.store, &assignment_two, &result_two_id).await,
        repair_operation_id
    );

    assert_eq!(
        operation_byte_snapshot(&owner.store, &feedback_operation_id).await,
        feedback_before,
        "cross-review coalescing preserves the original feedback Operation bytes"
    );
    assert_eq!(
        meta_json_bytes(&owner.store, &feedback_link_key).await,
        feedback_link_before,
        "cross-review coalescing preserves the original feedback link bytes"
    );
    assert_eq!(
        observation_payload_bytes(
            &owner.store,
            "controller:review",
            "task.feedback",
            &feedback_operation_id,
        )
        .await,
        feedback_observation_before,
        "cross-review coalescing preserves the original feedback observation bytes"
    );
    assert_eq!(
        operation_byte_snapshot(&owner.store, &repair_operation_id).await,
        repair_before,
        "cross-review resolution preserves the original RepairDispatch Operation bytes"
    );
    assert_eq!(
        meta_json_bytes(&owner.store, &repair_link_key).await,
        repair_link_before,
        "cross-review resolution preserves the original RepairDispatch link bytes"
    );
    assert_eq!(
        meta_json_bytes(&owner.store, &repair_slot_key).await,
        repair_slot_before,
        "cross-review resolution preserves the original semantic slot bytes"
    );
    assert_eq!(
        disposition_payload_bytes(&owner.store, &assignment_one).await,
        first_disposition_bytes,
        "the prior review disposition remains immutable"
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
