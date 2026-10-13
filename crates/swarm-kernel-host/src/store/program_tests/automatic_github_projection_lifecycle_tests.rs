//! Connected lifecycle coverage for pending automatic GitHub projections.
//!
//! The Store/reconcile path creates and admits the semantic managed-label
//! Operation. The test then retains an ambiguous result at an explicit fixture
//! boundary, exercises the real readback path through an in-memory API, and
//! resumes projection reconciliation only after exact readback settlement.

use super::*;
use std::{future::Future, pin::Pin};

use crate::github::client::{
    GitHubLabelApi, IssueLabelSnapshot, RepositoryReadback, RepositoryRef,
};

const AUTOMATION_ID: &str = "github-projection-lifecycle";
const SOURCE_ID: &str = "projection-fixture-source";
const SOURCE_REVISION: &str = "fixture-source-revision-1";
const LABEL: &str = "eliot-accepted";
const REPOSITORY_ID: i64 = 4401;
const ISSUE_ID: i64 = 7701;
const ISSUE_NUMBER: i64 = 7;
// This fixture has one Acceptance-only automation entry beside the projection
// entry. The shared global key cursor can therefore produce one empty filtered
// page before it reaches the projection again.
const MAX_FIXTURE_ENTRY_PAGES: usize = 2;

struct FixtureLabelReadback {
    issue_id: i64,
}

impl GitHubLabelApi for FixtureLabelReadback {
    fn repository<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<RepositoryReadback>> + Send + 'a>> {
        Box::pin(async {
            Ok(RepositoryReadback {
                id: REPOSITORY_ID,
                full_name: "fixture-owner/projection-fixture".to_owned(),
                html_url: "https://github.com/fixture-owner/projection-fixture".to_owned(),
            })
        })
    }

    fn issue_labels<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<IssueLabelSnapshot>> + Send + 'a>> {
        let issue_id = self.issue_id;
        Box::pin(async move {
            Ok(IssueLabelSnapshot {
                id: issue_id,
                number: ISSUE_NUMBER,
                labels: vec![LABEL.to_owned()],
            })
        })
    }

    fn set_label<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
        _label: &'a str,
        _present: bool,
    ) -> Pin<Box<dyn Future<Output = crate::error::Result<()>> + Send + 'a>> {
        Box::pin(async { panic!("readback-only reconciliation must never issue a label write") })
    }
}

async fn insert_registered_source_fixture(store: &Store, task_id: &str, created_by: &str) {
    let task_id = task_id.to_owned();
    let created_by = created_by.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let now_ms = model::now_ms()?;
            // Match the retained registered-source mapping shape used by the
            // existing github_effect_tests fixtures. No provider call occurs.
            tx.execute(
                "INSERT INTO github_sources(source_id,project_id,host,owner,repository_name,repository_id,next_page,poll_generation,last_poll_status,last_coverage_json,created_by,created_at_ms,updated_at_ms) \
                 VALUES(?1,'fixture','github.com','fixture-owner','projection-fixture',?2,1,0,'never','{}',?3,?4,?4)",
                params![SOURCE_ID, REPOSITORY_ID, created_by, now_ms],
            )?;
            tx.execute(
                "INSERT INTO github_issue_items(source_id,issue_id,issue_number,current_fact_digest,current_event_key,source_revision,payload_json,task_id,mapping_status,observed_generation,last_seen_at_ms) \
                 VALUES(?1,?2,?3,'fixture-fact','fixture-event',?4,'{\"state\":\"open\"}',?5,'mapped',1,?6)",
                params![
                    SOURCE_ID,
                    ISSUE_ID,
                    ISSUE_NUMBER,
                    SOURCE_REVISION,
                    task_id,
                    now_ms
                ],
            )?;
            tx.execute(
                "INSERT INTO github_work_pool_members(source_id,task_id,issue_id,selected,selection_order,discovered_at_ms,updated_at_ms) \
                 VALUES(?1,?2,?3,1,0,?4,?4)",
                params![SOURCE_ID, task_id, ISSUE_ID, now_ms],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn projection_reconcile_once(store: &Store) -> Value {
    store
        .run(|db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let result = crate::store::automation_github_projection::reconcile(
                &tx,
                16,
                16,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .unwrap()
}

async fn retained_label_rows(
    store: &Store,
    task_id: &str,
) -> Vec<(
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<i64>,
    Option<i64>,
)> {
    let task_id = task_id.to_owned();
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT operation_id,state,client_request_id,original_request_json,effective_request_json,result_json,sent_at_ms,settled_at_ms \
                 FROM operations WHERE method='github.effect.managed_label' AND caller_id=?1 AND task_id=?2 ORDER BY operation_id",
            )?;
            let rows = statement
                .query_map(
                    params![
                        crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                        task_id
                    ],
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
                        ))
                    },
                )?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
        .unwrap()
}

async fn retain_ambiguous_effect_fixture(store: &Store, operation_id: &str) {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (original_json, effective_json): (String, String) = tx.query_row(
                "SELECT original_request_json,effective_request_json FROM operations WHERE operation_id=?1 AND state='queued'",
                [&operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let original: Value = serde_json::from_str(&original_json)?;
            let effective: Value = serde_json::from_str(&effective_json)?;
            let admission = &effective["receipt"]["value"];
            let now_ms = model::now_ms()?;
            let result = json!({
                "operation_id":operation_id,
                "source_id":admission["source_id"],
                "project_id":admission["project_id"],
                "task_id":admission["task_id"],
                "task_revision_at_observation":admission["task_revision"],
                "expected_task_revision":original["expected_task_revision"],
                "repository_id":REPOSITORY_ID,
                "issue_id":admission["issue_id"],
                "issue_number":admission["issue_number"],
                "source_revision":admission["source_revision"],
                "label":original["label"],
                "desired_present":original["present"],
                "observed_present":false,
                "write_attempted":true,
                "readback":"not_confirmed",
                "outcome":"desired_state_not_observed_after_write",
                "error":{"code":"FIXTURE_AMBIGUOUS","message":"fixture starts from an ambiguous retained write"},
                "current_state_read_method":"operation.get"
            });
            let changed = tx.execute(
                "UPDATE operations SET state='outcome_unknown',sent_at_ms=?2,settled_at_ms=NULL,result_json=?3,updated_at_ms=?2 \
                 WHERE operation_id=?1 AND method='github.effect.managed_label' AND state='queued'",
                params![operation_id, now_ms, model::canonical(&result)?],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "fixture could not retain the ambiguous managed-label Operation",
                ));
            }
            crate::store::capacity::sync_operation(&tx, &operation_id, now_ms)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

fn projection_entry(pass: &Value) -> &Value {
    let entries = pass["entries"]
        .as_array()
        .expect("projection entries array");
    let matching = entries
        .iter()
        .filter(|entry| entry["automation_id"] == AUTOMATION_ID)
        .collect::<Vec<_>>();
    assert_eq!(matching.len(), 1, "target projection entry missing: {pass}");
    matching[0]
}

async fn projection_pass_for_fixture_entry(store: &Store) -> Value {
    for _ in 0..MAX_FIXTURE_ENTRY_PAGES {
        let pass = projection_reconcile_once(store).await;
        let contains_target = pass["entries"].as_array().is_some_and(|entries| {
            entries
                .iter()
                .any(|entry| entry["automation_id"] == AUTOMATION_ID)
        });
        if contains_target {
            return pass;
        }
    }
    panic!("bounded fixture pages did not reach {AUTOMATION_ID}");
}

#[tokio::test]
async fn projection_pending_does_not_hot_loop_or_advance_cursor() {
    projection_lifecycle_case(None).await;
}

#[tokio::test]
async fn cancelled_label_effect_is_an_explicit_terminal_skip_without_replay() {
    projection_lifecycle_case(Some("cancelled")).await;
}

#[tokio::test]
async fn rejected_label_effect_retains_explicit_terminal_disposition() {
    projection_lifecycle_case(Some("rejected")).await;
}

#[tokio::test]
async fn settled_no_write_does_not_advance_as_success() {
    projection_lifecycle_case(Some("settled")).await;
}

async fn projection_lifecycle_case(terminal_state: Option<&'static str>) {
    let (owner, data_directory, bootstrap_credential) =
        start_store("github-projection-lifecycle").await;
    let operator = owner
        .store
        .authenticate(bootstrap_credential)
        .await
        .unwrap();
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "github-projection-lifecycle",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    configure_acceptance_fixture(&owner.store, "review-gm", std::slice::from_ref(&subject)).await;

    let candidate_ref = subject.candidate_ref.clone();
    let submission_ref = subject.submission_ref.clone();
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    let (candidate, submission, submission_bytes) = owner
        .store
        .run(move |db| {
            let mut metadata = results::get(db, &candidate_ref)?.metadata;
            metadata["task_id"] = json!(task_id);
            metadata["attempt_id"] = json!(attempt_id);
            metadata["task_revision"] = json!(1);
            metadata["coverage"] = json!("complete");
            metadata["commit"] = json!("1".repeat(40));
            metadata["tree"] = json!("2".repeat(40));
            db.execute(
                "UPDATE artifacts SET metadata_json=?2 WHERE artifact_id=?1 AND kind='source_snapshot'",
                params![candidate_ref, model::canonical(&metadata)?],
            )?;
            let candidate = results::get(db, &candidate_ref)?;
            let submission = results::get(db, &submission_ref)?;
            let document = submissions::document(db, &submission_ref)?;
            Ok((
                candidate,
                submission,
                model::canonical(&document)?.into_bytes(),
            ))
        })
        .await
        .unwrap();
    let candidate_bytes = b"explicit source snapshot fixture".to_vec();
    owner
        .store
        .file_io(move |files| {
            files.publish(&candidate, &candidate_bytes)?;
            files.publish(&submission, &submission_bytes)?;
            Ok(())
        })
        .await
        .unwrap();

    let gm = principal("review-gm", Role::Manager);
    let accepted = owner
        .store
        .call(
            gm.clone(),
            "task.accept".into(),
            json!({
                "client_request_id":"accept-github-projection-lifecycle",
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "expected_feedback_observation_id":0,
                "reason":"The connected fixture accepts this exact retained candidate.",
                "reviews":[{
                    "requirement_id":"R1",
                    "rationale":"The retained candidate satisfies the fixture requirement.",
                    "evidence":[subject.candidate_ref]
                }],
                "check_ids":[]
            }),
        )
        .await
        .unwrap();
    let accepted_operation_id = accepted["operation_id"].as_str().unwrap().to_owned();
    let accepted_readback = owner
        .store
        .call(
            gm.clone(),
            "operation.get".into(),
            json!({"operation_id":accepted_operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(accepted_readback["method"], "task.accept");
    assert_eq!(accepted_readback["state"], "settled");
    assert_eq!(accepted_readback["result"]["outcome"], "applied");
    let acceptance_observation_id = owner
        .store
        .run({
            let accepted_operation_id = accepted_operation_id.clone();
            move |db| {
                db.query_row(
                    "SELECT observation_id FROM observations WHERE source_stream_id='controller:acceptance' AND source_event_key=?1 AND operation_id=?2 AND kind='task.acceptance'",
                    params![format!("accept:{accepted_operation_id}"), accepted_operation_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();

    insert_registered_source_fixture(&owner.store, &subject.task_id, &operator.client_id).await;
    let configured = owner
        .store
        .call(
            gm.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"configure-github-projection-lifecycle",
                "project_id":"fixture",
                "changes":[{
                    "automation_id":AUTOMATION_ID,
                    "expected_revision":0,
                    "include_existing":true,
                    "patch":{
                        "enabled":true,
                        "scope":{"work_pool_id":null},
                        "steps":["github_projection"],
                        "github_projection":{
                            "source_id":SOURCE_ID,
                            "label":LABEL,
                            "present":true
                        }
                    }
                }]
            }),
        )
        .await
        .unwrap();
    assert_eq!(configured["applied"], true);

    // Call the real production projection reconciliation transaction directly.
    // Store.reconcile_automations_once also drains queued effects through GhCli,
    // so the domain entry point keeps this fixture free of external I/O.
    let first_pass = projection_pass_for_fixture_entry(&owner.store).await;
    let first_entry = projection_entry(&first_pass);
    assert_eq!(first_entry["automation_id"], AUTOMATION_ID);
    assert_eq!(first_entry["processed"], 0);
    assert_eq!(first_entry["cursor"], 0);
    let first_rows = retained_label_rows(&owner.store, &subject.task_id).await;
    assert_eq!(
        first_rows.len(),
        1,
        "one accepted cause reserves one effect"
    );
    let (operation_id, state, semantic_id, original_json, effective_json, _, sent, settled) =
        &first_rows[0];
    assert_eq!(state, "queued");
    assert_eq!(sent, &None);
    assert_eq!(settled, &None);
    let original: Value = serde_json::from_str(original_json).unwrap();
    let effective: Value = serde_json::from_str(effective_json).unwrap();
    let admission = &effective["receipt"]["value"];
    assert_eq!(original["client_request_id"], semantic_id.as_str());
    assert_eq!(original["source_id"], SOURCE_ID);
    assert_eq!(original["task_id"], subject.task_id);
    assert_eq!(original["expected_task_revision"], 1);
    assert_eq!(original["label"], LABEL);
    assert_eq!(original["present"], true);
    assert_eq!(effective["receipt"]["ok"], true);
    assert_eq!(
        effective["automation_on_behalf"]["action"],
        "github.effect.managed_label"
    );
    assert_eq!(admission["operation_id"], operation_id.as_str());
    assert_eq!(admission["source_id"], SOURCE_ID);
    assert_eq!(admission["project_id"], "fixture");
    assert_eq!(admission["task_id"], subject.task_id);
    assert_eq!(admission["task_revision"], 1);
    assert_eq!(admission["source_revision"], SOURCE_REVISION);
    assert_eq!(admission["issue_id"], ISSUE_ID);
    assert_eq!(admission["issue_number"], ISSUE_NUMBER);
    assert_eq!(admission["label"], LABEL);
    assert_eq!(admission["desired_present"], true);

    if let Some(terminal_state) = terminal_state {
        if terminal_state == "cancelled" {
            let cancelled = owner
                .store
                .call(
                    operator.clone(),
                    "operation.cancel".into(),
                    json!({
                        "client_request_id":"cancel-projection-before-effect",
                        "operation_id":operation_id,
                        "reason":"Do not apply this queued label effect."
                    }),
                )
                .await
                .unwrap();
            assert_eq!(cancelled["cancelled_operation_id"], operation_id.as_str());
            assert_eq!(cancelled["native_cancel_sent"], false);
        } else {
            // This retained-state boundary follows real Store admission. It
            // exercises the automatic consumer, not an external label worker.
            let mut result = admission.clone();
            result["state"] = json!(terminal_state);
            result["outcome"] = json!("rejected_before_write");
            result["write_attempted"] = json!(false);
            result["error"] = json!({
                "code":"FORBIDDEN",
                "message":"The admitted label effect was refused before a write."
            });
            let id = operation_id.clone();
            owner.store.run(move |db| {
                let now_ms = model::now_ms()?;
                let changed = db.execute(
                    "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?4 \
                     WHERE operation_id=?1 AND method='github.effect.managed_label' \
                       AND state='queued' AND sent_at_ms IS NULL AND settled_at_ms IS NULL",
                    params![id, terminal_state, model::canonical(&result)?, now_ms],
                )?;
                if changed != 1 {
                    return Err(Error::conflict("exact queued projection fixture changed"));
                }
                Ok(())
            }).await.unwrap();
        }
        let terminal_pass = projection_pass_for_fixture_entry(&owner.store).await;
        let entry = projection_entry(&terminal_pass);
        assert_eq!(entry["processed"], 1);
        assert!(entry["cursor"].as_i64().unwrap() >= acceptance_observation_id);
        let recent = entry["recent"].as_array().unwrap().last().unwrap();
        assert_eq!(recent["status"], "skipped", "terminal projection: {recent}");
        assert_eq!(
            recent["effect_status"],
            match terminal_state {
                "cancelled" => "projection_cancelled",
                "rejected" => "projection_rejected",
                "settled" => "projection_not_applied",
                _ => unreachable!(),
            }
        );
        assert_eq!(recent["effect_operation_id"], operation_id.as_str());
        assert_eq!(recent["automation_revision"], 1);
        assert!(!recent["code"].as_str().unwrap().is_empty());
        assert!(!recent["reason"].as_str().unwrap().is_empty());
        let after = retained_label_rows(&owner.store, &subject.task_id).await;
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].0, *operation_id);
        assert_eq!(after[0].1, terminal_state);
        assert_eq!(after[0].2, *semantic_id);
        assert_eq!(after[0].3, *original_json);
        assert_eq!(after[0].4, *effective_json);
        assert_eq!(after[0].6, None);
        assert!(after[0].7.is_some());
        let again = projection_pass_for_fixture_entry(&owner.store).await;
        assert_eq!(projection_entry(&again)["processed"], 0);
        assert_eq!(
            retained_label_rows(&owner.store, &subject.task_id)
                .await
                .len(),
            1
        );
        owner.close().await.unwrap();
        std::fs::remove_dir_all(data_directory).unwrap();
        return;
    }

    // Simulate the exact producer-shaped retained ambiguity after dispatch;
    // no write is sent. The second real reconcile must only request readback.
    retain_ambiguous_effect_fixture(&owner.store, operation_id).await;
    let second_pass = projection_pass_for_fixture_entry(&owner.store).await;
    let second_entry = projection_entry(&second_pass);
    assert_eq!(second_entry["processed"], 0);
    assert_eq!(second_entry["cursor"], first_entry["cursor"]);
    let second_rows = retained_label_rows(&owner.store, &subject.task_id).await;
    assert_eq!(second_rows.len(), 1, "unknown effect is not duplicated");
    assert_eq!(second_rows[0].0, *operation_id);
    assert_eq!(second_rows[0].1, "outcome_unknown");
    assert_eq!(second_rows[0].2, *semantic_id);
    assert_eq!(second_rows[0].3, *original_json);
    assert_eq!(second_rows[0].4, *effective_json);
    assert!(second_rows[0].6.is_some());
    assert_eq!(second_rows[0].7, None);
    assert_eq!(
        second_entry["recent"].as_array().unwrap().last().unwrap()["effect_status"],
        "projection_readback_required"
    );

    // Wrong immutable Issue identity is rejected by the actual readback API
    // path, leaving the same effect unknown and the projection cursor parked.
    let wrong_target = crate::store::github_effects::reconcile_call_with_api(
        &owner.store,
        gm.clone(),
        json!({
            "client_request_id":"projection-readback-wrong-target",
            "operation_id":operation_id
        }),
        &FixtureLabelReadback {
            issue_id: ISSUE_ID + 1,
        },
    )
    .await
    .unwrap();
    assert_eq!(wrong_target["outcome"], "readback_unavailable");
    assert_eq!(
        wrong_target["error"]["code"],
        "GITHUB_ISSUE_IDENTITY_CHANGED"
    );
    assert_eq!(wrong_target["original_operation_state"], "outcome_unknown");
    let still_unknown = retained_label_rows(&owner.store, &subject.task_id).await;
    assert_eq!(still_unknown.len(), 1);
    assert_eq!(still_unknown[0].0, *operation_id);
    assert_eq!(still_unknown[0].1, "outcome_unknown");
    assert_eq!(still_unknown[0].3, *original_json);
    assert_eq!(still_unknown[0].4, *effective_json);
    let after_wrong_target_pass = projection_pass_for_fixture_entry(&owner.store).await;
    let after_wrong_target_entry = projection_entry(&after_wrong_target_pass);
    assert_eq!(after_wrong_target_entry["processed"], 0);
    assert_eq!(after_wrong_target_entry["cursor"], second_entry["cursor"]);
    assert_eq!(after_wrong_target_entry["cursor"], 0);
    let still_one_effect = retained_label_rows(&owner.store, &subject.task_id).await;
    assert_eq!(still_one_effect.len(), 1);
    assert_eq!(still_one_effect[0].0, *operation_id);
    assert_eq!(still_one_effect[0].1, "outcome_unknown");

    let confirmed = crate::store::github_effects::reconcile_call_with_api(
        &owner.store,
        gm,
        json!({
            "client_request_id":"projection-readback-confirmed",
            "operation_id":operation_id
        }),
        &FixtureLabelReadback { issue_id: ISSUE_ID },
    )
    .await
    .unwrap();
    assert_eq!(confirmed["original_operation_id"], operation_id.as_str());
    assert_eq!(confirmed["outcome"], "reconciled_from_readback");
    assert_eq!(confirmed["original_operation_state"], "settled");
    assert_eq!(confirmed["observed_issue_id"], ISSUE_ID);
    assert_eq!(confirmed["observed_issue_number"], ISSUE_NUMBER);
    assert_eq!(confirmed["observed_present"], true);
    assert_eq!(confirmed["readback"], "repository_and_issue_confirmed");

    let settled_rows = retained_label_rows(&owner.store, &subject.task_id).await;
    assert_eq!(settled_rows.len(), 1);
    assert_eq!(settled_rows[0].0, *operation_id);
    assert_eq!(settled_rows[0].1, "settled");
    assert_eq!(settled_rows[0].2, *semantic_id);
    assert_eq!(
        settled_rows[0].3, *original_json,
        "original request is immutable"
    );
    assert_eq!(
        settled_rows[0].4, *effective_json,
        "admission receipt is immutable"
    );
    assert!(settled_rows[0].7.is_some());
    let settled_result: Value =
        serde_json::from_str(settled_rows[0].5.as_deref().unwrap()).unwrap();
    assert_eq!(settled_result["operation_id"], operation_id.as_str());
    assert_eq!(settled_result["outcome"], "reconciled_from_readback");
    assert_eq!(settled_result["readback"], "confirmed");
    assert_eq!(settled_result["desired_present"], true);
    assert_eq!(settled_result["observed_present"], true);

    let final_pass = projection_pass_for_fixture_entry(&owner.store).await;
    let final_entry = projection_entry(&final_pass);
    assert_eq!(final_entry["processed"], 1);
    assert!(final_entry["cursor"].as_i64().unwrap() >= acceptance_observation_id);
    assert!(final_entry["cursor"].as_i64().unwrap() > second_entry["cursor"].as_i64().unwrap());
    assert_eq!(final_entry["catch_up_until"], Value::Null);
    let final_recent = final_entry["recent"].as_array().unwrap().last().unwrap();
    assert_eq!(final_recent["effect_operation_id"], operation_id.as_str());
    assert_eq!(final_recent["effect_status"], "projection_applied");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(data_directory).unwrap();
}
