//! Deterministic transport fixture for the exact PR identity gate. No live
//! GitHub account or credential is used by these tests.

use super::*;
use crate::github::client::{
    GitHubPullRequestApi, PullRequestReadback, PullRequestRefReadback,
    PullRequestRepositoryReadback, RepositoryReadback,
};
use crate::store::{StoreOwner, github_pr_effects, gm, tasks};
use crate::{
    config::Config,
    error::{Error, Result},
    forge::ForgeProject,
    model::{self, Credential},
    platform::{DataRoot, bootstrap_credential},
};
use rusqlite::params;
use serde_json::json;
use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};
use tokio::sync::Mutex;

struct ExactPullRequestFixture {
    repository: RepositoryReadback,
    pull_request: PullRequestReadback,
}

#[derive(Debug)]
struct RemoteState {
    title: String,
    body: String,
    writes: usize,
    pull_reads: usize,
}

#[derive(Clone)]
struct AmbiguousWriteFixture {
    state: Arc<Mutex<RemoteState>>,
}

impl GitHubPullRequestApi for AmbiguousWriteFixture {
    fn repository<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        Box::pin(async {
            Ok(RepositoryReadback {
                id: 44,
                full_name: "owner/repo".into(),
                html_url: "https://github.com/owner/repo".into(),
            })
        })
    }

    fn pull_request<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<PullRequestReadback>> + Send + 'a>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state.lock().await;
            state.pull_reads += 1;
            Ok(PullRequestReadback {
                id: 55,
                number: 7,
                state: "open".into(),
                title: state.title.clone(),
                body: Some(state.body.clone()),
                updated_at: "2026-10-04T00:00:00Z".into(),
                html_url: "https://github.com/owner/repo/pull/7".into(),
                draft: false,
                merged: false,
                merge_commit_sha: None,
                head: PullRequestRefReadback {
                    sha: "a".repeat(40),
                    ref_name: Some("main".into()),
                    repo: Some(PullRequestRepositoryReadback { id: 44 }),
                },
                base: PullRequestRefReadback {
                    sha: "b".repeat(40),
                    ref_name: Some("release".into()),
                    repo: Some(PullRequestRepositoryReadback { id: 44 }),
                },
            })
        })
    }

    fn update_description<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
        _title: &'a str,
        _body: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let mut state = self.state.lock().await;
            state.writes += 1;
            Err(Error::new(
                "FIXTURE_RESPONSE_LOST",
                "fixture reports an ambiguous PATCH without exposing immediate remote state",
            ))
        })
    }
}

#[derive(Clone)]
struct TaskRevisionDuringReadFixture {
    store: Store,
    operator: Principal,
}

impl GitHubPullRequestApi for TaskRevisionDuringReadFixture {
    fn repository<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        Box::pin(async {
            Ok(RepositoryReadback {
                id: 44,
                full_name: "owner/repo".into(),
                html_url: "https://github.com/owner/repo".into(),
            })
        })
    }

    fn pull_request<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<PullRequestReadback>> + Send + 'a>> {
        let store = self.store.clone();
        let operator = self.operator.clone();
        Box::pin(async move {
            store
                .call(
                    operator,
                    "task.revise".to_owned(),
                    json!({
                        "client_request_id":"o4-pr-revise-during-preflight-get",
                        "task_id":"task-1",
                        "expected_revision":2,
                        "spec":{
                            "objective":"Invalidate the accepted candidate during PR preflight",
                            "phase":"implementation",
                            "requirements":[{"id":"R1","statement":"No write starts after the candidate changes."}],
                            "owner_policy_id":"policy-1"
                        }
                    }),
                )
                .await?;
            Ok(PullRequestReadback {
                id: 57,
                number: 9,
                state: "open".into(),
                title: "old title".into(),
                body: Some("old body".into()),
                updated_at: "2026-10-04T00:00:00Z".into(),
                html_url: "https://github.com/owner/repo/pull/9".into(),
                draft: false,
                merged: false,
                merge_commit_sha: None,
                head: PullRequestRefReadback {
                    sha: "c".repeat(40),
                    ref_name: Some("main".into()),
                    repo: Some(PullRequestRepositoryReadback { id: 44 }),
                },
                base: PullRequestRefReadback {
                    sha: "d".repeat(40),
                    ref_name: Some("release".into()),
                    repo: Some(PullRequestRepositoryReadback { id: 44 }),
                },
            })
        })
    }

    fn update_description<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
        _title: &'a str,
        _body: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { panic!("preflight invalidation must not dispatch a PR write") })
    }
}

#[derive(Clone)]
struct ManagerHandoverDuringReadFixture {
    store: Store,
    current_manager: Principal,
    next_manager_id: String,
    state: Arc<Mutex<RemoteState>>,
}

impl GitHubPullRequestApi for ManagerHandoverDuringReadFixture {
    fn repository<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        Box::pin(async {
            Ok(RepositoryReadback {
                id: 44,
                full_name: "owner/repo".into(),
                html_url: "https://github.com/owner/repo".into(),
            })
        })
    }

    fn pull_request<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<PullRequestReadback>> + Send + 'a>> {
        let store = self.store.clone();
        let current_manager = self.current_manager.clone();
        let next_manager_id = self.next_manager_id.clone();
        let state = self.state.clone();
        Box::pin(async move {
            let (title, body) = {
                let mut state = state.lock().await;
                state.pull_reads += 1;
                (state.title.clone(), state.body.clone())
            };
            store
                .call(
                    current_manager,
                    "gm.handover".to_owned(),
                    json!({
                        "client_request_id":"o4-pr-handover-during-reconcile-get",
                        "client_id":next_manager_id
                    }),
                )
                .await?;
            Ok(PullRequestReadback {
                id: 55,
                number: 7,
                state: "open".into(),
                title,
                body: Some(body),
                updated_at: "2026-10-04T00:00:00Z".into(),
                html_url: "https://github.com/owner/repo/pull/7".into(),
                draft: false,
                merged: false,
                merge_commit_sha: None,
                head: PullRequestRefReadback {
                    sha: "a".repeat(40),
                    ref_name: Some("main".into()),
                    repo: Some(PullRequestRepositoryReadback { id: 44 }),
                },
                base: PullRequestRefReadback {
                    sha: "b".repeat(40),
                    ref_name: Some("release".into()),
                    repo: Some(PullRequestRepositoryReadback { id: 44 }),
                },
            })
        })
    }

    fn update_description<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
        _title: &'a str,
        _body: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { panic!("PR reconciliation is readback-only") })
    }
}

impl GitHubPullRequestApi for ExactPullRequestFixture {
    fn repository<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        Box::pin(async move { Ok(self.repository.clone()) })
    }

    fn pull_request<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<PullRequestReadback>> + Send + 'a>> {
        Box::pin(async move { Ok(self.pull_request.clone()) })
    }

    fn update_description<'a>(
        &'a self,
        _repository: &'a RepositoryRef,
        _number: i64,
        _title: &'a str,
        _body: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async { panic!("identity fixture must never dispatch a write") })
    }
}

fn target() -> Target {
    Target {
        publication_operation_id: "publish-1".into(),
        task_id: "task-1".into(),
        attempt_id: "attempt-1".into(),
        project_id: "project-1".into(),
        source_id: "source-1".into(),
        repository_id: 44,
        host: "github.com".into(),
        owner: "owner".into(),
        repo: "repo".into(),
        pull_request_id: 55,
        pull_request_number: 7,
        head_sha: "a".repeat(40),
        target_ref: "refs/heads/main".into(),
        base_ref: "refs/heads/release".into(),
    }
}

fn fixture() -> ExactPullRequestFixture {
    ExactPullRequestFixture {
        repository: RepositoryReadback {
            id: 44,
            full_name: "owner/repo".into(),
            html_url: "https://github.com/owner/repo".into(),
        },
        pull_request: PullRequestReadback {
            id: 55,
            number: 7,
            state: "open".into(),
            title: "old title".into(),
            body: Some("old body".into()),
            updated_at: "2026-10-04T00:00:00Z".into(),
            html_url: "https://github.com/owner/repo/pull/7".into(),
            draft: false,
            merged: false,
            merge_commit_sha: None,
            head: PullRequestRefReadback {
                sha: "a".repeat(40),
                ref_name: Some("main".into()),
                repo: Some(PullRequestRepositoryReadback { id: 44 }),
            },
            base: PullRequestRefReadback {
                sha: "b".repeat(40),
                ref_name: Some("release".into()),
                repo: Some(PullRequestRepositoryReadback { id: 44 }),
            },
        },
    }
}

fn seed_accepted_candidate(db: &rusqlite::Connection, caller: &str) -> Result<()> {
    let now = 1_i64;
    let candidate_digest = "c".repeat(64);
    let commit = "a".repeat(40);
    let tree = "b".repeat(40);
    let submission_document = json!({
        "attempt_id":"attempt-1",
        "task_revision":1,
        "candidate_ref":"candidate-1",
        "candidate_sha256":candidate_digest.clone(),
        "claims":[]
    });
    let submission_digest = model::digest(model::canonical(&submission_document)?.as_bytes());
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES('submission-1','fixture/submission.json','task_submission',0,?1,?2,?3)",
        params![submission_digest, now, json!({"operation_id":"submit-1"}).to_string()],
    )?;
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES('candidate-1','fixture/candidate.json','source_snapshot',0,?1,?2,?3)",
        params![
            candidate_digest,
            now,
            model::canonical(&json!({"attempt_id":"attempt-1","task_revision":1,"commit":commit.clone(),"tree":tree.clone()}))?
        ],
    )?;
    let spec = json!({"owner_policy_id":"policy-1"});
    let snapshot =
        json!({"spec":spec.clone(),"owner_policy":{"status":"legacy_unknown"},"brief":{}});
    db.execute(
        "INSERT INTO tasks(task_id,project_id,origin_key,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('task-1','project-1',NULL,1,'open',?1,?2,?2)",
        params![model::canonical(&spec)?, now],
    )?;
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES('attempt-1','task-1',1,?1,?2,'controller','accepted','submission-1','candidate-1',?3,?3)",
        params![model::canonical(&snapshot)?, caller, now],
    )?;
    let acceptance = json!({
        "outcome":"applied",
        "acceptance_operation_id":"accept-1",
        "task_id":"task-1",
        "attempt_id":"attempt-1",
        "task_revision":1,
        "phase":"complete",
        "submission_ref":"submission-1",
        "candidate_ref":"candidate-1"
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES('accept-1',?1,'accept-request','task.accept','{}','{}','task-1','attempt-1','settled',?2,0,?3,?3,?3)",
        params![caller, model::canonical(&acceptance)?, now],
    )?;
    db.execute(
        "UPDATE tasks SET state='accepted',accepted_attempt_id='attempt-1',accepted_operation_id='accept-1',accepted_revision=1,accepted_phase='complete',accepted_candidate_ref='candidate-1',updated_at_ms=?1 WHERE task_id='task-1'",
        [now],
    )?;
    let submission_result =
        json!({"outcome":"applied","submission_ref":"submission-1","candidate_ref":"candidate-1"});
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES('submit-1',?1,'submit-request','task.submit','{}',?2,'task-1','attempt-1','settled',?3,0,?4,?4,?4)",
        params![
            caller,
            model::canonical(&json!({"submission_document":submission_document}))?,
            model::canonical(&submission_result)?,
            now
        ],
    )?;
    db.execute(
        "INSERT INTO github_sources(source_id,project_id,host,owner,repository_name,repository_id,next_page,poll_generation,last_poll_status,last_coverage_json,created_by,created_at_ms,updated_at_ms) VALUES('source-1','project-1','github.com','owner','repo',44,1,0,'never','{}',?1,?2,?2)",
        params![caller, now],
    )?;
    Ok(())
}

fn seed_accepted_candidate_revision_two(db: &rusqlite::Connection, caller: &str) -> Result<()> {
    let now = 3_i64;
    let attempt_id = "attempt-2";
    let submission_ref = "submission-2";
    let candidate_ref = "candidate-2";
    let acceptance_operation_id = "accept-2";
    let submit_operation_id = "submit-2";
    let candidate_digest = "e".repeat(64);
    let commit = "c".repeat(40);
    let tree = "d".repeat(40);
    let submission_document = json!({
        "task_id":"task-1",
        "attempt_id":attempt_id,
        "task_revision":2,
        "candidate_ref":candidate_ref,
        "candidate_sha256":candidate_digest,
        "claims":[]
    });
    let submission_digest = model::digest(model::canonical(&submission_document)?.as_bytes());
    let task = tasks::get_task(db, "task-1")?;
    let snapshot = json!({
        "spec":task["spec"],
        "owner_policy":{"status":"legacy_unknown"},
        "brief":{}
    });
    db.execute(
        "UPDATE attempts SET state='superseded',released_at_ms=?1,updated_at_ms=?1 WHERE attempt_id='attempt-1' AND state='accepted'",
        params![now],
    )?;
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'task_submission',0,?3,?4,?5)",
        params![
            submission_ref,
            "fixture/submission-2.json",
            submission_digest,
            now,
            json!({"operation_id":submit_operation_id}).to_string()
        ],
    )?;
    db.execute(
        "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES(?1,?2,'source_snapshot',0,?3,?4,?5)",
        params![
            candidate_ref,
            "fixture/candidate-2.json",
            candidate_digest,
            now,
            model::canonical(&json!({
                "attempt_id":attempt_id,
                "task_revision":2,
                "commit":commit,
                "tree":tree
            }))?
        ],
    )?;
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES(?1,'task-1',2,?2,?3,'controller','accepted',?4,?5,?6,?6)",
        params![
            attempt_id,
            model::canonical(&snapshot)?,
            caller,
            submission_ref,
            candidate_ref,
            now
        ],
    )?;
    let acceptance = json!({
        "outcome":"applied",
        "acceptance_operation_id":acceptance_operation_id,
        "task_id":"task-1",
        "attempt_id":attempt_id,
        "task_revision":2,
        "phase":"complete",
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,'accept-request-2','task.accept','{}','{}','task-1',?3,'settled',?4,0,?5,?5,?5)",
        params![
            acceptance_operation_id,
            caller,
            attempt_id,
            model::canonical(&acceptance)?,
            now
        ],
    )?;
    let submission_result = json!({
        "outcome":"applied",
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,'submit-request-2','task.submit','{}',?3,'task-1',?4,'settled',?5,0,?6,?6,?6)",
        params![
            submit_operation_id,
            caller,
            model::canonical(&json!({"submission_document":submission_document}))?,
            attempt_id,
            model::canonical(&submission_result)?,
            now
        ],
    )?;
    db.execute(
        "UPDATE tasks SET state='accepted',accepted_attempt_id=?1,accepted_operation_id=?2,accepted_revision=2,accepted_phase='complete',accepted_candidate_ref=?3,updated_at_ms=?4 WHERE task_id='task-1' AND revision=2 AND state='open'",
        params![attempt_id, acceptance_operation_id, candidate_ref, now],
    )?;
    Ok(())
}

async fn register_manager(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    let token = format!("o4-pr-{client_id}-{}", model::new_id());
    store
        .call(
            operator.clone(),
            "client.register".to_owned(),
            json!({
                "client_request_id":format!("o4-pr-register-{client_id}"),
                "client_id":client_id,
                "role":"manager",
                "token_hash":model::digest(token.as_bytes())
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

#[tokio::test]
async fn exact_pr_fixture_rejects_a_different_published_head_without_writing() {
    let repository = RepositoryRef::new("github.com", "owner", "repo").unwrap();
    let api = fixture();
    let checked = read_remote(&api, &repository, &target())
        .await
        .expect("exact repository, PR, branch and published SHA are accepted");
    assert_eq!(checked.id, 55);

    let mut wrong_head = fixture();
    wrong_head.pull_request.head.sha = "c".repeat(40);
    let error = read_remote(&wrong_head, &repository, &target())
        .await
        .expect_err("a PR with a different head commit must be rejected");
    assert_eq!(error.code, "GITHUB_PR_IDENTITY_CHANGED");
}

#[tokio::test]
async fn successor_gm_reconciles_predecessor_unknown_pr_effect_readback_only() {
    let directory = std::env::temp_dir().join(format!("eliot-gh-pr-test-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.forge.enabled = true;
    config.forge.git_executable = std::env::current_exe().unwrap();
    config.forge.projects.insert(
        "project-1".into(),
        ForgeProject {
            canonical_repository: "github.com/owner/repo".into(),
            repository_path: PathBuf::from(&directory),
            remote_name: "origin".into(),
            policy_revision: "policy-1".into(),
            target_refs: vec!["refs/heads/main".into()],
        },
    );
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    let predecessor = register_manager(&owner.store, &operator, "o4-pr-predecessor").await;
    let successor = register_manager(&owner.store, &operator, "o4-pr-successor").await;
    let final_manager = register_manager(&owner.store, &operator, "o4-pr-final-manager").await;
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".to_owned(),
            json!({"client_request_id":"o4-pr-designate-predecessor","client_id":predecessor.client_id.clone()}),
        )
        .await
        .unwrap();

    let caller_id = predecessor.client_id.clone();
    owner
        .store
        .run(move |db| seed_accepted_candidate(db, &caller_id))
        .await
        .unwrap();

    // Admit through the real Store mutation path, including current
    // predecessor-GM rights, accepted Task/Attempt, submission digest,
    // candidate provenance,
    // configured project and normal durable forge Operation creation. The
    // fixture marks the already-admitted publication as remotely confirmed;
    // it never runs Git or contacts a remote.
    let publication_request = json!({
        "client_request_id":"publish-request-1",
        "attempt_id":"attempt-1",
        "expected_revision":1,
        "submission_ref":"submission-1",
        "accepted_operation_id":"accept-1",
        "candidate_ref":"candidate-1",
        "expected_policy_revision":"policy-1",
        "target_ref":"refs/heads/main",
        "expected_old_ref":null,
        "expected_create":true
    });
    let publication_config = owner.store.config.clone();
    let publication_principal = predecessor.clone();
    let publication_receipt = owner
        .store
        .run(move |db| {
            let current = current_principal(db, publication_principal)?;
            mutate(
                db,
                &current,
                "forge.publish_ref",
                &publication_request,
                publication_config.as_ref(),
            )
        })
        .await
        .unwrap();

    let publication_operation_id = model::text(&publication_receipt, "operation_id")
        .unwrap()
        .to_owned();
    let publication_id_for_seed = publication_operation_id.clone();
    owner
        .store
        .run(move |db| {
            let result = json!({
                "operation_id":publication_id_for_seed,
                "outcome":"applied",
                "publication":"confirmed_by_remote_readback",
                "acceptance_current_at_finish":true,
                "remote_ref":{"present":true,"commit":"a".repeat(40)}
            });
            db.execute(
                "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=2,updated_at_ms=2 WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued'",
                params![publication_id_for_seed, model::canonical(&result)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let state = Arc::new(Mutex::new(RemoteState {
        title: "before".into(),
        body: "before body".into(),
        writes: 0,
        pull_reads: 0,
    }));
    let api = AmbiguousWriteFixture {
        state: state.clone(),
    };
    let request = json!({
        "client_request_id":"pr-update-predecessor-unknown",
        "publication_operation_id":publication_operation_id,
        "pull_request_id":55,
        "pull_request_number":7,
        "base_ref":"refs/heads/release",
        "title":"Published candidate",
        "body":"Reviewed candidate body"
    });
    // The predecessor performs the authorized PATCH while current. Its
    // response is ambiguous and the first exact readback still shows old data.
    let first =
        github_pr_effects::call_with_api(&owner.store, predecessor.clone(), request.clone(), &api)
            .await
            .unwrap();
    assert_eq!(first["operation_state"], "outcome_unknown");
    assert_eq!(
        first["outcome"],
        "desired_description_not_observed_after_write"
    );
    assert_eq!(first["write_attempted"], true);
    assert_eq!(first["task_id"], "task-1");
    assert_eq!(first["attempt_id"], "attempt-1");
    assert_eq!(first["project_id"], "project-1");
    assert_eq!(first["source_id"], "source-1");
    assert_eq!(first["repository_id"], 44);
    assert_eq!(first["pull_request_id"], 55);
    assert_eq!(
        first["publication_operation_id"],
        publication_receipt["operation_id"]
    );
    assert_eq!(state.lock().await.writes, 1);

    // Admit a second PR update while the predecessor is current, but leave it
    // before any GitHub I/O. A later current GM may cancel this precisely
    // scoped queued Attempt Operation through ordinary operation.cancel.
    let queued_cancel_request = json!({
        "client_request_id":"pr-update-queued-for-successor-cancel",
        "publication_operation_id":publication_receipt["operation_id"],
        "pull_request_id":56,
        "pull_request_number":8,
        "base_ref":"refs/heads/release",
        "title":"queued title",
        "body":"queued body"
    });
    let queued_cancel_config = owner.store.config.clone();
    let queued_cancel_principal = predecessor.clone();
    let queued_cancel_receipt = owner
        .store
        .run(move |db| {
            let current = current_principal(db, queued_cancel_principal)?;
            mutate(
                db,
                &current,
                METHOD,
                &queued_cancel_request,
                queued_cancel_config.as_ref(),
            )
        })
        .await
        .unwrap();
    let queued_cancel_operation_id = model::text(&queued_cancel_receipt, "operation_id")
        .unwrap()
        .to_owned();
    let queued_cancel_state = state.lock().await.pull_reads;

    // Startup can convert a sending row to outcome_unknown while leaving its
    // original queued receipt in place. Simulate that retained shape so
    // reconciliation derives the desired title/body from original_request_json.
    let restart_receipt = json!({
        "operation_id":first["operation_id"],
        "publication_operation_id":first["publication_operation_id"],
        "project_id":first["project_id"],
        "source_id":first["source_id"],
        "task_id":first["task_id"],
        "attempt_id":first["attempt_id"],
        "repository_id":first["repository_id"],
        "pull_request_id":first["pull_request_id"],
        "pull_request_number":first["pull_request_number"],
        "head_sha":first["head_sha"],
        "target_ref":first["target_ref"],
        "base_ref":first["base_ref"],
        "outcome":"outcome_unknown",
        "readback":"required",
        "write_attempted":true,
        "current_state_read_method":"operation.get"
    });
    let restart_operation_id = first["operation_id"].as_str().unwrap().to_owned();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE operations SET result_json=?2 WHERE operation_id=?1 AND method='github.pull_request.update_description' AND state='outcome_unknown'",
                params![restart_operation_id, model::canonical(&restart_receipt)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let target_operation_id = first["operation_id"].as_str().unwrap().to_owned();
    let original_history_before: (String, String, String, String) = owner
        .store
        .run({
            let operation_id = target_operation_id.clone();
            move |db| {
                db.query_row(
                    "SELECT caller_id,client_request_id,original_request_json,effective_request_json FROM operations WHERE operation_id=?1 AND method='github.pull_request.update_description'",
                    [operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();

    // Transfer after the ambiguous write. The target's historical caller and
    // request remain fixed; a later Task revision must not prevent readback.
    owner
        .store
        .call(
            predecessor.clone(),
            "gm.handover".to_owned(),
            json!({
                "client_request_id":"o4-pr-handover-successor",
                "client_id":successor.client_id.clone()
            }),
        )
        .await
        .unwrap();
    let revised = owner
        .store
        .call(
            operator.clone(),
            "task.revise".to_owned(),
            json!({
                "client_request_id":"o4-pr-revise-after-unknown",
                "task_id":"task-1",
                "expected_revision":1,
                "spec":{
                    "objective":"Read back the exact PR effect",
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Preserve the exact original PR Operation."}],
                    "owner_policy_id":"policy-1"
                }
            }),
        )
        .await
        .unwrap();
    assert_eq!(revised["revision"], 2);

    // The queued effect remains cancellable after its Task/Attempt is stale.
    let cancelled = owner
        .store
        .call(
            successor.clone(),
            "operation.cancel".to_owned(),
            json!({
                "client_request_id":"pr-update-successor-cancels-queued",
                "operation_id":queued_cancel_operation_id,
                "reason":"current GM cancelled an unstarted PR description effect"
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        cancelled["cancelled_operation_id"],
        queued_cancel_receipt["operation_id"]
    );
    let cancelled_target = owner
        .store
        .run({
            let operation_id = queued_cancel_receipt["operation_id"]
                .as_str()
                .unwrap()
                .to_owned();
            move |db| operations::get_operation(db, &operation_id)
        })
        .await
        .unwrap();
    assert_eq!(cancelled_target["state"], "cancelled");
    assert_eq!(state.lock().await.pull_reads, queued_cancel_state);
    assert_eq!(state.lock().await.writes, 1);

    let successor_id = successor.client_id.clone();
    owner
        .store
        .run(move |db| seed_accepted_candidate_revision_two(db, &successor_id))
        .await
        .unwrap();
    let new_publication_request = json!({
        "client_request_id":"publish-request-2",
        "attempt_id":"attempt-2",
        "expected_revision":2,
        "submission_ref":"submission-2",
        "accepted_operation_id":"accept-2",
        "candidate_ref":"candidate-2",
        "expected_policy_revision":"policy-1",
        "target_ref":"refs/heads/main",
        "expected_old_ref":null,
        "expected_create":true
    });
    let new_publication_config = owner.store.config.clone();
    let new_publication_principal = successor.clone();
    let new_publication_receipt = owner
        .store
        .run(move |db| {
            let current = current_principal(db, new_publication_principal)?;
            mutate(
                db,
                &current,
                "forge.publish_ref",
                &new_publication_request,
                new_publication_config.as_ref(),
            )
        })
        .await
        .unwrap();
    let new_publication_operation_id = model::text(&new_publication_receipt, "operation_id")
        .unwrap()
        .to_owned();
    let new_publication_id_for_seed = new_publication_operation_id.clone();
    owner
        .store
        .run(move |db| {
            let result = json!({
                "operation_id":new_publication_id_for_seed,
                "outcome":"applied",
                "publication":"confirmed_by_remote_readback",
                "acceptance_current_at_finish":true,
                "remote_ref":{"present":true,"commit":"c".repeat(40)}
            });
            db.execute(
                "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=3,updated_at_ms=3 WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued'",
                params![new_publication_id_for_seed, model::canonical(&result)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    // A new accepted commit for the same PR must not bypass the unresolved
    // description effect by creating a second head-keyed slot.
    let before_new_head_attempt = state.lock().await.pull_reads;
    let new_head_error = github_pr_effects::call_with_api(
        &owner.store,
        successor.clone(),
        json!({
            "client_request_id":"pr-update-new-head-blocked",
            "publication_operation_id":new_publication_operation_id,
            "pull_request_id":55,
            "pull_request_number":7,
            "base_ref":"refs/heads/release",
            "title":"new candidate title",
            "body":"new candidate body"
        }),
        &api,
    )
    .await
    .expect_err("the old PR resource slot remains held across a head advance");
    assert_eq!(new_head_error.code, "CONFLICT");
    assert_eq!(state.lock().await.pull_reads, before_new_head_attempt);
    assert_eq!(state.lock().await.writes, 1);

    // Change the Task during the preflight GET. The second authorization cut
    // must reject the still-queued Operation before PATCH, rather than leaving
    // a durable slot and queued receipt orphaned.
    let revision_during_get = TaskRevisionDuringReadFixture {
        store: owner.store.clone(),
        operator: operator.clone(),
    };
    let prewrite_request = json!({
        "client_request_id":"pr-update-candidate-changed-during-get",
        "publication_operation_id":new_publication_operation_id,
        "pull_request_id":57,
        "pull_request_number":9,
        "base_ref":"refs/heads/release",
        "title":"candidate title",
        "body":"candidate body"
    });
    let prewrite_error = github_pr_effects::call_with_api(
        &owner.store,
        successor.clone(),
        prewrite_request,
        &revision_during_get,
    )
    .await
    .expect_err("a Task revision change during GET must stop the write");
    assert_eq!(prewrite_error.code, "FORGE_ACCEPTANCE_STALE");
    assert_eq!(state.lock().await.writes, 1);
    let prewrite_operation = owner
        .store
        .run({
            let caller = successor.client_id.clone();
            move |db| {
                let raw: String = db.query_row(
                    "SELECT operation_id FROM operations WHERE caller_id=?1 AND client_request_id='pr-update-candidate-changed-during-get' AND method='github.pull_request.update_description'",
                    [caller],
                    |row| row.get(0),
                )?;
                operations::get_operation(db, &raw)
            }
        })
        .await
        .unwrap();
    assert_eq!(prewrite_operation["state"], "rejected");
    assert_eq!(
        prewrite_operation["result"]["outcome"],
        "rejected_before_write"
    );
    assert_eq!(prewrite_operation["result"]["write_attempted"], false);

    let reads_before = state.lock().await.pull_reads;
    let former_reconcile = json!({
        "client_request_id":"pr-reconcile-former-denied",
        "operation_id":target_operation_id
    });
    let former_error = github_pr_effects::reconcile_call_with_api(
        &owner.store,
        predecessor.clone(),
        former_reconcile,
        &api,
    )
    .await
    .expect_err("former manager cannot reconcile after losing current GM authority");
    assert_eq!(former_error.code, "FORBIDDEN");
    assert_eq!(state.lock().await.pull_reads, reads_before);
    assert_eq!(state.lock().await.writes, 1);

    // Simulate delayed remote application after a lost response. The retained
    // unknown Operation is reconciled by the successor through one exact GET
    // sequence only. No accepted-candidate current-revision gate is applied to
    // this readback of the historical effect.
    {
        let mut remote = state.lock().await;
        remote.title = "Published candidate".into();
        remote.body = "Reviewed candidate body".into();
    }
    let handover_api = ManagerHandoverDuringReadFixture {
        store: owner.store.clone(),
        current_manager: successor.clone(),
        next_manager_id: final_manager.client_id.clone(),
        state: state.clone(),
    };
    let reconciled = github_pr_effects::reconcile_call_with_api(
        &owner.store,
        successor.clone(),
        json!({
            "client_request_id":"pr-reconcile-successor",
            "operation_id":target_operation_id
        }),
        &handover_api,
    )
    .await
    .unwrap();
    assert_eq!(reconciled["target_operation_id"], first["operation_id"]);
    assert_eq!(reconciled["operation_state"], "settled");
    assert_eq!(reconciled["target_operation_state"], "settled");
    assert_eq!(reconciled["readback"], "confirmed");
    assert_eq!(reconciled["readback_only"], true);
    assert_eq!(reconciled["write_attempted"], false);
    assert_ne!(reconciled["operation_id"], first["operation_id"]);
    let reconciliation_identity: (String, String, Option<String>, Option<String>, String) = owner
        .store
        .run({
            let operation_id = reconciled["operation_id"].as_str().unwrap().to_owned();
            move |db| {
                db.query_row(
                    "SELECT method,caller_id,task_id,attempt_id,state FROM operations WHERE operation_id=?1",
                    [operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    assert_eq!(
        reconciliation_identity.0,
        "github.pull_request.reconcile_description"
    );
    assert_eq!(reconciliation_identity.1, successor.client_id);
    assert_eq!(reconciliation_identity.2.as_deref(), Some("task-1"));
    assert_eq!(reconciliation_identity.3.as_deref(), Some("attempt-1"));
    assert_eq!(reconciliation_identity.4, "settled");
    assert_eq!(state.lock().await.pull_reads, reads_before + 1);
    assert_eq!(state.lock().await.writes, 1);
    let current_gm = owner
        .store
        .run(|db| gm::record(db))
        .await
        .unwrap()
        .expect("the handover during readback retained a current GM");
    assert_eq!(current_gm["client_id"], final_manager.client_id);

    let original_history_after: (String, String, String, String) = owner
        .store
        .run({
            let operation_id = target_operation_id.clone();
            move |db| {
                db.query_row(
                    "SELECT caller_id,client_request_id,original_request_json,effective_request_json FROM operations WHERE operation_id=?1 AND method='github.pull_request.update_description'",
                    [operation_id],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    assert_eq!(original_history_after, original_history_before);
    let original_operation = owner
        .store
        .run({
            let operation_id = target_operation_id.clone();
            move |db| operations::get_operation(db, &operation_id)
        })
        .await
        .unwrap();
    assert_eq!(original_operation["state"], "settled");
    assert_eq!(
        original_operation["result"]["readback_reconciliation"]["reconciler_client_id"],
        successor.client_id
    );
    assert_eq!(
        original_operation["result"]["readback_reconciliation"]["operation_id"],
        reconciled["operation_id"]
    );
    let reads_after_reconcile = state.lock().await.pull_reads;
    let forged_reconcile = github_pr_effects::reconcile_call_with_api(
        &owner.store,
        final_manager.clone(),
        json!({
            "client_request_id":"pr-reconcile-forged-target",
            "operation_id":"not-a-pr-update-operation"
        }),
        &api,
    )
    .await
    .expect_err("a forged target Operation is rejected before GitHub readback");
    assert_eq!(forged_reconcile.code, "NOT_FOUND");
    assert_eq!(state.lock().await.pull_reads, reads_after_reconcile);
    assert_eq!(state.lock().await.writes, 1);

    let publication_id_for_audit = publication_operation_id.clone();
    let publication_history: (String, String, String) = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT caller_id,effective_request_json,state FROM operations WHERE operation_id=?1 AND method='forge.publish_ref'",
                [publication_id_for_audit],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    let publication_effective: serde_json::Value =
        serde_json::from_str(&publication_history.1).unwrap();
    assert_eq!(publication_history.0, predecessor.client_id);
    assert_eq!(
        publication_effective["publication_intent"]["admitted_gm_epoch"],
        1
    );
    assert_eq!(publication_history.2, "settled");

    let pr_operation_id = first["operation_id"].as_str().unwrap().to_owned();
    let pr_operation_caller = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT caller_id FROM operations WHERE operation_id=?1 AND method='github.pull_request.update_description'",
                [pr_operation_id],
                |row| row.get::<_, String>(0),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    assert_eq!(pr_operation_caller, predecessor.client_id);

    let operation_id = first["operation_id"].as_str().unwrap().to_owned();
    let slot: (i64, i64, String, String) = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT repository_id,pull_request_id,head_sha,operation_id FROM github_pr_effect_slots WHERE operation_id=?1",
                [operation_id],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    assert_eq!(
        slot,
        (
            44,
            55,
            "a".repeat(40),
            first["operation_id"].as_str().unwrap().to_owned()
        )
    );

    // A different/forged publication link is rejected at durable admission;
    // it cannot reach the transport.
    let forged = json!({
        "client_request_id":"pr-update-request-forged",
        "publication_operation_id":"not-a-publication",
        "pull_request_id":55,
        "pull_request_number":7,
        "base_ref":"refs/heads/release",
        "title":"forged",
        "body":"forged"
    });
    let forged_error = github_pr_effects::call_with_api(&owner.store, operator, forged, &api)
        .await
        .expect_err("forged publication identity is not admitted");
    assert_eq!(forged_error.code, "NOT_FOUND");
    assert_eq!(state.lock().await.writes, 1);
}

#[test]
fn pr_reconciliation_request_is_closed_and_bounded() {
    let valid = json!({
        "client_request_id":"pr-reconcile-1",
        "operation_id":"op-pr-1"
    });
    assert!(protocol::PullRequestDescriptionReconcileRequest::parse(&valid).is_ok());
    let with_write_intent = json!({
        "client_request_id":"pr-reconcile-1",
        "operation_id":"op-pr-1",
        "title":"must not be writable"
    });
    assert!(protocol::PullRequestDescriptionReconcileRequest::parse(&with_write_intent).is_err());
    let empty_target = json!({
        "client_request_id":"pr-reconcile-1",
        "operation_id":"  "
    });
    assert!(protocol::PullRequestDescriptionReconcileRequest::parse(&empty_target).is_err());
}

#[test]
fn pr_description_request_is_closed_and_bounded() {
    let valid = json!({
        "client_request_id":"pr-update-1",
        "publication_operation_id":"publish-1",
        "pull_request_id":55,
        "pull_request_number":7,
        "base_ref":"refs/heads/release",
        "title":"Published candidate",
        "body":"Accepted SHA is being reviewed."
    });
    assert!(protocol::PullRequestDescriptionUpdateRequest::parse(&valid).is_ok());
    let with_state = json!({"client_request_id":"pr-update-1","publication_operation_id":"publish-1","pull_request_id":55,"pull_request_number":7,"base_ref":"refs/heads/release","title":"x","body":"y","state":"closed"});
    assert!(protocol::PullRequestDescriptionUpdateRequest::parse(&with_state).is_err());
}
