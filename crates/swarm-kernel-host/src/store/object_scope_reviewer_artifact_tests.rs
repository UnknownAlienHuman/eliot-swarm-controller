use crate::{
    config::Config,
    model::{self, Credential, Principal, Role},
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner},
};
use serde_json::{Value, json};
use std::{path::Path, process::Command, sync::Arc, time::Duration};

async fn start_store() -> (StoreOwner, std::path::PathBuf, Principal, Principal) {
    let directory =
        std::env::temp_dir().join(format!("swarm-reviewer-artifact-scope-{}", model::new_id()));
    let store_directory = directory.join("store");
    std::fs::create_dir_all(&store_directory).unwrap();
    let root = DataRoot::acquire(&store_directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    let manager = register_client(
        &owner.store,
        &operator,
        "reviewer-artifact-manager",
        Role::Manager,
    )
    .await;
    (owner, directory, operator, manager)
}

async fn register_client(
    store: &Store,
    operator: &Principal,
    client_id: &str,
    role: Role,
) -> Principal {
    let token = format!("reviewer-artifact-token-{}", model::new_id());
    store
        .call(
            operator.clone(),
            "client.register".into(),
            json!({
                "client_request_id":model::new_id(),
                "client_id":client_id,
                "role":role,
                "token_hash":model::digest(token.as_bytes()),
            }),
        )
        .await
        .unwrap();
    store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

async fn register_pending_reviewer(
    store: &Store,
    sponsor: &Principal,
    client_id: &str,
    task_id: &str,
    attempt_id: &str,
    submission_ref: &str,
    candidate_ref: &str,
) -> Principal {
    let token = format!("reviewer-artifact-token-{}", model::new_id());
    let scope = json!({
        "review_assignment_id":null,
        "task_id":task_id,
        "attempt_id":attempt_id,
        "task_revision":1,
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref,
    });
    store
        .call(
            sponsor.clone(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":model::new_id(),
                "client_id":client_id,
                "token_hash":model::digest(token.as_bytes()),
                "task_id":task_id,
                "task_revision":1,
                "attempt_id":attempt_id,
                "participation_basis":{
                    "kind":"sponsored_reviewer",
                    "review_scope":scope,
                },
            }),
        )
        .await
        .unwrap();
    store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

async fn call(store: &Store, principal: &Principal, method: &str, request: Value) -> Value {
    store
        .call(principal.clone(), method.into(), request)
        .await
        .unwrap_or_else(|error| panic!("{method}: {error:?}"))
}

async fn write(store: &Store, principal: &Principal, method: &str, mut request: Value) -> Value {
    request["client_request_id"] = json!(model::new_id());
    call(store, principal, method, request).await
}

async fn denied(store: &Store, principal: &Principal, method: &str, request: Value) {
    let error = store
        .call(principal.clone(), method.into(), request)
        .await
        .unwrap_err();
    assert_eq!(error.code, "FORBIDDEN", "{method}: {error:?}");
}

async fn denied_review_scope(store: &Store, principal: &Principal, request: Value) {
    let error = store
        .call(principal.clone(), "swarm.review.context".into(), request)
        .await
        .unwrap_err();
    assert!(
        matches!(
            error.code.as_str(),
            "FORBIDDEN"
                | "NOT_FOUND"
                | "UNAUTHORIZED"
                | "STALE_PARTICIPANT"
                | "PARTICIPANT_NOT_ASSIGNED"
                | "STALE_REVIEW_ASSIGNMENT"
                | "REVIEW_ASSIGNMENT_NOT_FOUND"
        ),
        "historical/foreign context was not denied: {error:?}"
    );
}

fn git(repository: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repository)
        .output()
        .expect("run local source-capture fixture Git command");
    assert!(
        output.status.success(),
        "Git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Git output is UTF-8")
        .trim()
        .to_owned()
}

async fn settled_operation(store: &Store, caller: &Principal, operation_id: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let operation = call(
                store,
                caller,
                "operation.get",
                json!({"operation_id":operation_id}),
            )
            .await;
            match operation["state"].as_str() {
                Some("settled") => return operation,
                Some("queued" | "sending" | "native_accepted" | "outcome_unknown") => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                state => panic!("source.capture ended in unexpected state {state:?}: {operation}"),
            }
        }
    })
    .await
    .expect("source.capture settles within the fixture bound")
}

#[tokio::test]
async fn assigned_candidate_artifact_is_current_only_and_review_receipts_survive_release() {
    let (owner, directory, operator, manager) = start_store().await;
    let store = &owner.store;

    let mut spec: Value =
        serde_json::from_str(include_str!("../../config/task.example.json")).unwrap();
    spec["objective"] = json!("Assigned reviewer artifact-scope fixture");
    let task = write(
        store,
        &manager,
        "task.create",
        json!({
            "project_id":"reviewer-artifact-scope",
            "origin_key":model::new_id(),
            "spec":spec,
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let attempt = write(
        store,
        &manager,
        "task.claim",
        json!({"task_id":task_id,"expected_revision":1}),
    )
    .await;
    let attempt_id = attempt["attempt_id"].as_str().unwrap().to_owned();

    let repository = directory.join("candidate-repository");
    std::fs::create_dir_all(&repository).unwrap();
    git(&repository, &["init", "--quiet"]);
    git(&repository, &["config", "user.name", "Fixture Author"]);
    git(
        &repository,
        &["config", "user.email", "fixture@example.invalid"],
    );
    std::fs::write(
        repository.join("candidate.txt"),
        b"assigned candidate bytes\n",
    )
    .unwrap();
    git(&repository, &["add", "candidate.txt"]);
    git(
        &repository,
        &["commit", "--quiet", "-m", "review artifact fixture"],
    );
    let commit = git(&repository, &["rev-parse", "HEAD"]);
    let captured = write(
        store,
        &manager,
        "source.capture",
        json!({
            "attempt_id":attempt_id,
            "expected_revision":1,
            "repository":repository,
            "commit":commit,
        }),
    )
    .await;
    let capture_operation_id = captured["operation_id"].as_str().unwrap().to_owned();
    let capture_operation = settled_operation(store, &manager, &capture_operation_id).await;
    assert_eq!(capture_operation["state"], "settled");
    assert_eq!(capture_operation["method"], "source.capture");
    let candidate_ref = format!("source-{}", model::digest(capture_operation_id.as_bytes()));
    let candidate_metadata = call(
        store,
        &manager,
        "artifact.get",
        json!({"artifact_id":candidate_ref}),
    )
    .await;
    assert_eq!(candidate_metadata["kind"], "source_snapshot");
    assert!(candidate_metadata["byte_length"].as_u64().unwrap() > 0);
    let read_length = 65_536u64;

    let _submission = write(
        store,
        &manager,
        "task.submit",
        json!({
            "attempt_id":attempt_id,
            "expected_revision":1,
            "expected_submission_ref":null,
            "candidate_ref":candidate_ref,
            "summary":"Submit the exact source.capture candidate.",
            "claims":[],
        }),
    )
    .await;
    let submitted_attempt = call(
        store,
        &manager,
        "attempt.get",
        json!({"attempt_id":attempt_id}),
    )
    .await;
    let submission_ref = submitted_attempt["submission_ref"]
        .as_str()
        .unwrap()
        .to_owned();

    let reviewer_id = "reviewer-artifact-current";
    let reviewer = register_pending_reviewer(
        store,
        &manager,
        reviewer_id,
        &task_id,
        &attempt_id,
        &submission_ref,
        &candidate_ref,
    )
    .await;
    let foreign_reviewer = register_pending_reviewer(
        store,
        &manager,
        "reviewer-artifact-pending",
        &task_id,
        &attempt_id,
        &submission_ref,
        &candidate_ref,
    )
    .await;
    let assignment = write(
        store,
        &manager,
        "review.assign",
        json!({
            "attempt_id":attempt_id,
            "expected_revision":1,
            "submission_ref":submission_ref,
            "candidate_ref":candidate_ref,
            "reviewer_client_id":reviewer_id,
        }),
    )
    .await;
    let assignment_id = assignment["review_assignment_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let assignment_operation_id = assignment["operation_id"].as_str().unwrap().to_owned();

    let context = call(
        store,
        &reviewer,
        "swarm.review.context",
        json!({"review_assignment_id":assignment_id}),
    )
    .await;
    assert_eq!(
        context["context"]["candidate"]["artifact_id"],
        candidate_ref
    );
    denied(
        store,
        &reviewer,
        "artifact.get",
        json!({"artifact_id":candidate_ref}),
    )
    .await;
    let bytes = call(
        store,
        &reviewer,
        "artifact.read",
        json!({
            "artifact_id":candidate_ref,
            "offset_bytes":0,
            "length_bytes":read_length,
        }),
    )
    .await;
    assert!(
        bytes["byte_length"]
            .as_u64()
            .is_some_and(|length| length > 0)
    );
    assert!(
        bytes["content"]
            .as_str()
            .is_some_and(|content| !content.is_empty())
    );

    denied_review_scope(
        store,
        &foreign_reviewer,
        json!({"review_assignment_id":assignment_id}),
    )
    .await;
    denied(
        store,
        &foreign_reviewer,
        "artifact.read",
        json!({
            "artifact_id":candidate_ref,
            "offset_bytes":0,
            "length_bytes":read_length,
        }),
    )
    .await;

    let review_result = write(
        store,
        &reviewer,
        "review.submit",
        json!({
            "review_assignment_id":assignment_id,
            "submission_ref":submission_ref,
            "candidate_ref":candidate_ref,
            "verdict":"pass",
            "coverage":"complete",
            "findings":[],
            "evidence_refs":["evidence://review/assigned-candidate"],
        }),
    )
    .await;
    assert_eq!(review_result["verdict"], "pass");
    let result_operation_id = review_result["operation_id"].as_str().unwrap().to_owned();

    let released = write(
        store,
        &manager,
        "attempt.release",
        json!({
            "attempt_id":attempt_id,
            "outcome":"superseded",
            "reason":"Close the exact reviewer artifact-scope fixture.",
            "assignment_closed":true,
        }),
    )
    .await;
    assert_eq!(released["released"], true);
    let mut revised_spec: Value =
        serde_json::from_str(include_str!("../../config/task.example.json")).unwrap();
    revised_spec["objective"] = json!("Historical reviewer artifact-scope fixture");
    let revised = write(
        store,
        &operator,
        "task.revise",
        json!({
            "task_id":task_id,
            "expected_revision":1,
            "spec":revised_spec,
        }),
    )
    .await;
    assert_eq!(revised["revision"], 2);

    denied_review_scope(
        store,
        &reviewer,
        json!({"review_assignment_id":assignment_id}),
    )
    .await;
    denied(
        store,
        &reviewer,
        "artifact.read",
        json!({
            "artifact_id":candidate_ref,
            "offset_bytes":0,
            "length_bytes":read_length,
        }),
    )
    .await;

    let historical_review = call(
        store,
        &reviewer,
        "review.get",
        json!({"review_assignment_id":assignment_id}),
    )
    .await;
    assert_eq!(historical_review["result"]["verdict"], "pass");
    assert_eq!(
        historical_review["result"]["operation_id"],
        result_operation_id
    );
    let assignment_receipt = call(
        store,
        &reviewer,
        "operation.get",
        json!({"operation_id":assignment_operation_id}),
    )
    .await;
    assert_eq!(assignment_receipt["method"], "review.assign");
    let result_receipt = call(
        store,
        &reviewer,
        "operation.get",
        json!({"operation_id":result_operation_id}),
    )
    .await;
    assert_eq!(result_receipt["method"], "review.submit");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
