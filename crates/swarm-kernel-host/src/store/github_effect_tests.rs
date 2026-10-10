use super::*;
use crate::{
    config::Config,
    github::client::{GitHubLabelApi, IssueLabelSnapshot, RepositoryReadback, RepositoryRef},
    platform::{DataRoot, bootstrap_credential},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Mutex,
    task::JoinHandle,
};

#[derive(Default)]
struct RemoteState {
    label_present: bool,
    drop_next_write: bool,
    write_count: usize,
    read_count: usize,
}

struct FixtureServer {
    address: SocketAddr,
    state: Arc<Mutex<RemoteState>>,
    task: JoinHandle<()>,
}

impl FixtureServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(RemoteState {
            drop_next_write: true,
            ..RemoteState::default()
        }));
        let server_state = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let state = server_state.clone();
                tokio::spawn(async move {
                    serve_request(stream, state).await;
                });
            }
        });
        Self {
            address,
            state,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_request(mut stream: TcpStream, state: Arc<Mutex<RemoteState>>) {
    let Some((method, path, body)) = read_request(&mut stream).await else {
        return;
    };
    let response = match (method.as_str(), path.as_str()) {
        ("GET", "/repos/owner/repo") => {
            state.lock().await.read_count += 1;
            Some((
                200,
                json!({
                    "id":44,"full_name":"owner/repo","html_url":"https://github.com/owner/repo"
                }),
            ))
        }
        ("GET", "/repos/owner/repo/issues/7") => {
            let mut current = state.lock().await;
            current.read_count += 1;
            let labels = if current.label_present {
                json!([{"name":"eliot-ready"}])
            } else {
                json!([])
            };
            Some((200, json!({"id":77,"number":7,"labels":labels})))
        }
        ("POST", "/repos/owner/repo/issues/7/labels") => {
            let mut current = state.lock().await;
            current.write_count += 1;
            if current.drop_next_write {
                current.drop_next_write = false;
                None
            } else {
                let posted: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                if posted["labels"]
                    .as_array()
                    .is_some_and(|labels| labels.iter().any(|label| label == "eliot-ready"))
                {
                    current.label_present = true;
                }
                Some((200, json!([{"name":"eliot-ready"}])))
            }
        }
        ("DELETE", "/repos/owner/repo/issues/7/labels/eliot-ready") => {
            let mut current = state.lock().await;
            current.write_count += 1;
            current.label_present = false;
            Some((200, json!({"name":"eliot-ready"})))
        }
        _ => Some((404, json!({"message":"fixture route not found"}))),
    };
    let Some((status, body)) = response else {
        // Model an ambiguous transport outcome before the server changes state.
        let _ = stream.shutdown().await;
        return;
    };
    let body = serde_json::to_vec(&body).unwrap();
    let reason = if status == 200 { "OK" } else { "Not Found" };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes()).await;
    let _ = stream.write_all(&body).await;
    let _ = stream.shutdown().await;
}

async fn read_request(stream: &mut TcpStream) -> Option<(String, String, Vec<u8>)> {
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 4096];
    let (header_end, content_length) = loop {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.len() > 64 * 1024 {
            return None;
        }
        if let Some(offset) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            let header_end = offset + 4;
            let headers = std::str::from_utf8(&bytes[..offset]).ok()?;
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            break (header_end, content_length);
        }
    };
    while bytes.len() < header_end.saturating_add(content_length) {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 || bytes.len().saturating_add(read) > 64 * 1024 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    let headers = std::str::from_utf8(&bytes[..header_end - 4]).ok()?;
    let first = headers.lines().next()?;
    let mut words = first.split_ascii_whitespace();
    let method = words.next()?.to_owned();
    let path = words.next()?.to_owned();
    let body = bytes[header_end..header_end + content_length].to_vec();
    Some((method, path, body))
}

struct HttpFixtureApi {
    base_url: String,
    client: reqwest::Client,
}

impl HttpFixtureApi {
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .build()
                .unwrap(),
        }
    }
}

#[derive(Deserialize)]
struct FixtureIssue {
    id: i64,
    number: i64,
    labels: Vec<FixtureLabel>,
}

#[derive(Deserialize)]
struct FixtureLabel {
    name: String,
}

impl GitHubLabelApi for HttpFixtureApi {
    fn repository<'a>(
        &'a self,
        repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        Box::pin(async move {
            self.client
                .get(format!(
                    "{}/repos/{}/{}",
                    self.base_url, repository.owner, repository.name
                ))
                .send()
                .await
                .map_err(|_| Error::new("FIXTURE_HTTP", "repository fixture request failed"))?
                .error_for_status()
                .map_err(|_| Error::new("FIXTURE_HTTP", "repository fixture status failed"))?
                .json()
                .await
                .map_err(|_| Error::new("FIXTURE_HTTP", "repository fixture JSON failed"))
        })
    }

    fn issue_labels<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<IssueLabelSnapshot>> + Send + 'a>> {
        Box::pin(async move {
            let issue: FixtureIssue = self
                .client
                .get(format!(
                    "{}/repos/{}/{}/issues/{number}",
                    self.base_url, repository.owner, repository.name
                ))
                .send()
                .await
                .map_err(|_| Error::new("FIXTURE_HTTP", "Issue fixture request failed"))?
                .error_for_status()
                .map_err(|_| Error::new("FIXTURE_HTTP", "Issue fixture status failed"))?
                .json()
                .await
                .map_err(|_| Error::new("FIXTURE_HTTP", "Issue fixture JSON failed"))?;
            Ok(IssueLabelSnapshot {
                id: issue.id,
                number: issue.number,
                labels: issue.labels.into_iter().map(|label| label.name).collect(),
            })
        })
    }

    fn set_label<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
        label: &'a str,
        present: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let endpoint = if present {
                format!(
                    "{}/repos/{}/{}/issues/{number}/labels",
                    self.base_url, repository.owner, repository.name
                )
            } else {
                format!(
                    "{}/repos/{}/{}/issues/{number}/labels/{label}",
                    self.base_url, repository.owner, repository.name
                )
            };
            let request = if present {
                self.client.post(endpoint).json(&json!({"labels":[label]}))
            } else {
                self.client.delete(endpoint)
            };
            request
                .send()
                .await
                .map_err(|_| Error::new("FIXTURE_HTTP", "effect response was lost"))?
                .error_for_status()
                .map_err(|_| Error::new("FIXTURE_HTTP", "effect fixture status failed"))?;
            Ok(())
        })
    }
}

struct HandoverDuringWriteApi {
    inner: HttpFixtureApi,
    store: Store,
    operator: Principal,
    successor_id: String,
}

impl GitHubLabelApi for HandoverDuringWriteApi {
    fn repository<'a>(
        &'a self,
        repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        self.inner.repository(repository)
    }

    fn issue_labels<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<IssueLabelSnapshot>> + Send + 'a>> {
        self.inner.issue_labels(repository, number)
    }

    fn set_label<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
        label: &'a str,
        present: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        let write = self.inner.set_label(repository, number, label, present);
        let store = self.store.clone();
        let operator = self.operator.clone();
        let successor_id = self.successor_id.clone();
        Box::pin(async move {
            let result = write.await;
            if result.is_err() {
                // The server lost the response. Rotate authority and revoke
                // selection before the caller's exact follow-up GET; the test
                // applies the delayed remote commit only after that GET keeps
                // the original Operation genuinely unknown.
                store
                    .call(
                        operator,
                        "gm.handover".into(),
                        json!({
                            "client_request_id":"handover-during-label-write",
                            "client_id":successor_id
                        }),
                    )
                    .await?;
                store
                    .run(|db| {
                        db.execute("UPDATE tasks SET revision=4 WHERE task_id='task-1'", [])?;
                        db.execute(
                            "UPDATE github_work_pool_members SET selected=0,selection_order=NULL WHERE source_id='source-1' AND task_id='task-1'",
                            [],
                        )?;
                        Ok(())
                    })
                    .await?;
            }
            result
        })
    }
}

struct HandoverDuringReadbackApi {
    inner: HttpFixtureApi,
    store: Store,
    operator: Principal,
    successor_id: String,
}

impl GitHubLabelApi for HandoverDuringReadbackApi {
    fn repository<'a>(
        &'a self,
        repository: &'a RepositoryRef,
    ) -> Pin<Box<dyn Future<Output = Result<RepositoryReadback>> + Send + 'a>> {
        self.inner.repository(repository)
    }

    fn issue_labels<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
    ) -> Pin<Box<dyn Future<Output = Result<IssueLabelSnapshot>> + Send + 'a>> {
        let read = self.inner.issue_labels(repository, number);
        let store = self.store.clone();
        let operator = self.operator.clone();
        let successor_id = self.successor_id.clone();
        Box::pin(async move {
            let snapshot = read.await?;
            // The GET has returned its exact Issue snapshot. Rotate GM before
            // the Store's final transaction to prove result ingestion does
            // not discard an already-authorized read solely on handover.
            store
                .call(
                    operator,
                    "gm.handover".into(),
                    json!({
                        "client_request_id":"handover-during-label-readback",
                        "client_id":successor_id
                    }),
                )
                .await?;
            store
                .run(|db| {
                    db.execute("UPDATE tasks SET revision=5 WHERE task_id='task-1'", [])?;
                    db.execute(
                        "UPDATE github_work_pool_members SET selected=0,selection_order=NULL WHERE source_id='source-1' AND task_id='task-1'",
                        [],
                    )?;
                    Ok(())
                })
                .await?;
            Ok(snapshot)
        })
    }

    fn set_label<'a>(
        &'a self,
        repository: &'a RepositoryRef,
        number: i64,
        label: &'a str,
        present: bool,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        self.inner.set_label(repository, number, label, present)
    }
}

#[tokio::test]
async fn ambiguous_write_is_read_back_on_exact_retry_without_resending() {
    let directory = std::env::temp_dir().join(format!("eliot-gh-label-test-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory;
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    owner
        .store
        .run(|db| {
            db.execute(
                "INSERT INTO github_sources(source_id,project_id,host,owner,repository_name,repository_id,next_page,poll_generation,last_poll_status,last_coverage_json,created_by,created_at_ms,updated_at_ms) VALUES('source-1','project-1','github.com','owner','repo',44,1,0,'never','{}','operator',1,1)",
                [],
            )?;
            db.execute(
                "INSERT INTO tasks(task_id,project_id,origin_key,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('task-1','project-1','origin-1',3,'open','{}',1,1)",
                [],
            )?;
            db.execute(
                "INSERT INTO github_issue_items(source_id,issue_id,issue_number,current_fact_digest,current_event_key,source_revision,payload_json,task_id,mapping_status,observed_generation,last_seen_at_ms) VALUES('source-1',77,7,'fact-1','event-1','revision-1','{\"state\":\"open\"}','task-1','mapped',1,1)",
                [],
            )?;
            db.execute(
                "INSERT INTO github_work_pool_members(source_id,task_id,issue_id,selected,selection_order,discovered_at_ms,updated_at_ms) VALUES('source-1','task-1',77,1,0,1,1)",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let fixture = FixtureServer::start().await;
    let api = HttpFixtureApi::new(fixture.base_url());
    let request = json!({
        "client_request_id":"effect-request-1",
        "source_id":"source-1",
        "task_id":"task-1",
        "expected_task_revision":3,
        "label":"eliot-ready",
        "present":true
    });
    let first =
        github_effects::call_with_api(&owner.store, operator.clone(), request.clone(), &api)
            .await
            .unwrap();
    assert_eq!(first["operation_state"], "outcome_unknown");
    assert_eq!(first["outcome"], "desired_state_not_observed_after_write");
    assert_eq!(fixture.state.lock().await.write_count, 1);

    // The fixture reports the desired state on readback. Replaying the exact
    // request must reconcile its retained Operation and must not POST again.
    fixture.state.lock().await.label_present = true;
    let retried = github_effects::call_with_api(&owner.store, operator, request, &api)
        .await
        .unwrap();
    assert_eq!(retried["operation_id"], first["operation_id"]);
    assert_eq!(retried["operation_state"], "settled");
    assert_eq!(retried["outcome"], "reconciled_from_readback");
    assert_eq!(retried["observed_present"], true);
    assert_eq!(fixture.state.lock().await.write_count, 1);
}

#[tokio::test]
async fn successor_gm_reconciles_unknown_label_by_exact_readback_only() {
    let directory =
        std::env::temp_dir().join(format!("eliot-gh-label-transfer-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    owner
        .store
        .run(|db| {
            set_meta(
                db,
                "client:label-former-gm",
                &json!({"role":"manager","disabled":false}),
            )?;
            set_meta(
                db,
                "client:label-successor-gm",
                &json!({"role":"manager","disabled":false}),
            )?;
            set_meta(
                db,
                "client:label-next-successor-gm",
                &json!({"role":"manager","disabled":false}),
            )?;
            set_meta(
                db,
                "gm",
                &json!({"client_id":"label-former-gm","epoch":1}),
            )?;
            db.execute(
                "INSERT INTO github_sources(source_id,project_id,host,owner,repository_name,repository_id,next_page,poll_generation,last_poll_status,last_coverage_json,created_by,created_at_ms,updated_at_ms) VALUES('source-1','project-1','github.com','owner','repo',44,1,0,'never','{}','operator',1,1)",
                [],
            )?;
            db.execute(
                "INSERT INTO tasks(task_id,project_id,origin_key,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('task-1','project-1','origin-1',3,'open','{}',1,1)",
                [],
            )?;
            db.execute(
                "INSERT INTO github_issue_items(source_id,issue_id,issue_number,current_fact_digest,current_event_key,source_revision,payload_json,task_id,mapping_status,observed_generation,last_seen_at_ms) VALUES('source-1',77,7,'fact-1','event-1','revision-1','{\"state\":\"open\"}','task-1','mapped',1,1)",
                [],
            )?;
            db.execute(
                "INSERT INTO github_work_pool_members(source_id,task_id,issue_id,selected,selection_order,discovered_at_ms,updated_at_ms) VALUES('source-1','task-1',77,1,0,1,1)",
                [],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let former = Principal {
        link_id: model::new_id(),
        client_id: "label-former-gm".to_owned(),
        role: Role::Manager,
    };
    let successor = Principal {
        link_id: model::new_id(),
        client_id: "label-successor-gm".to_owned(),
        role: Role::Manager,
    };
    let fixture = FixtureServer::start().await;
    let api = HandoverDuringWriteApi {
        inner: HttpFixtureApi::new(fixture.base_url()),
        store: owner.store.clone(),
        operator: operator.clone(),
        successor_id: successor.client_id.clone(),
    };
    let original_request = json!({
        "client_request_id":"label-former-effect",
        "source_id":"source-1",
        "task_id":"task-1",
        "expected_task_revision":3,
        "label":"eliot-ready",
        "present":true
    });
    let unknown =
        github_effects::call_with_api(&owner.store, former.clone(), original_request.clone(), &api)
            .await
            .unwrap();
    assert_eq!(unknown["operation_state"], "outcome_unknown");
    assert_eq!(unknown["outcome"], "desired_state_not_observed_after_write");
    assert_eq!(fixture.state.lock().await.write_count, 1);

    let original_id = unknown["operation_id"].as_str().unwrap().to_owned();
    let original_before = owner
        .store
        .run({
            let original_id = original_id.clone();
            move |db| {
                db.query_row(
                    "SELECT caller_id,client_request_id,original_request_json,effective_request_json,result_json FROM operations WHERE operation_id=?1",
                    [&original_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                        ))
                    },
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    let prior_unknown_result: Value = serde_json::from_str(&original_before.4).unwrap();
    let reads_before_former_retry = fixture.state.lock().await.read_count;
    let former_retry = github_effects::call_with_api(&owner.store, former, original_request, &api)
        .await
        .unwrap_err();
    assert_eq!(former_retry.code, "FORBIDDEN");
    assert_eq!(
        fixture.state.lock().await.read_count,
        reads_before_former_retry,
        "the former GM is denied before any GitHub read"
    );

    let reconcile_request = json!({
        "client_request_id":"successor-label-reconcile-mismatch",
        "operation_id":original_id
    });
    let mismatch = github_effects::reconcile_call_with_api(
        &owner.store,
        successor.clone(),
        reconcile_request.clone(),
        &api,
    )
    .await
    .unwrap();
    assert_eq!(mismatch["operation_state"], "settled");
    assert_eq!(mismatch["original_operation_state"], "outcome_unknown");
    assert_eq!(mismatch["outcome"], "desired_state_not_observed");
    let reads_after_mismatch = fixture.state.lock().await.read_count;
    let replay = github_effects::reconcile_call_with_api(
        &owner.store,
        successor.clone(),
        reconcile_request,
        &api,
    )
    .await
    .unwrap();
    assert_eq!(replay["operation_id"], mismatch["operation_id"]);
    assert_eq!(
        fixture.state.lock().await.read_count,
        reads_after_mismatch,
        "an exact reconcile retry returns its ordinary Operation receipt without another read"
    );

    // Model delayed application after the earlier exact read observed the
    // effect absent, without sending a second write.
    fixture.state.lock().await.label_present = true;
    let handover_api = HandoverDuringReadbackApi {
        inner: HttpFixtureApi::new(fixture.base_url()),
        store: owner.store.clone(),
        operator: operator.clone(),
        successor_id: "label-next-successor-gm".to_owned(),
    };
    let final_reconcile_request = json!({
        "client_request_id":"successor-label-reconcile-exact",
        "operation_id":original_id
    });
    let reconciled = github_effects::reconcile_call_with_api(
        &owner.store,
        successor.clone(),
        final_reconcile_request,
        &handover_api,
    )
    .await
    .unwrap();
    assert_eq!(reconciled["operation_state"], "settled");
    assert_eq!(reconciled["original_operation_state"], "settled");
    assert_eq!(reconciled["outcome"], "reconciled_from_readback");
    assert_eq!(reconciled["observed_present"], true);
    assert_eq!(reconciled["write_attempted"], false);
    assert_eq!(fixture.state.lock().await.write_count, 1);

    let original_after = owner
        .store
        .run({
            let original_id = original_id.clone();
            move |db| {
                db.query_row(
                    "SELECT caller_id,client_request_id,original_request_json,effective_request_json,state,result_json FROM operations WHERE operation_id=?1",
                    [&original_id],
                    |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, String>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, String>(5)?,
                        ))
                    },
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    assert_eq!(original_after.0, original_before.0);
    assert_eq!(original_after.1, original_before.1);
    assert_eq!(original_after.2, original_before.2);
    assert_eq!(original_after.3, original_before.3);
    assert_eq!(original_after.4, "settled");
    let settled_result: Value = serde_json::from_str(&original_after.5).unwrap();
    assert_eq!(
        settled_result["reconciliation"]["prior_unknown_result"],
        prior_unknown_result
    );
    assert_eq!(
        settled_result["reconciliation"]["original_caller_id"],
        "label-former-gm"
    );
    assert_eq!(
        settled_result["reconciliation"]["reconciler_client_id"],
        "label-successor-gm"
    );

    assert_eq!(fixture.state.lock().await.write_count, 1);

    let original_readback = owner
        .store
        .call(
            operator,
            "operation.get".into(),
            json!({"operation_id":original_id}),
        )
        .await
        .unwrap();
    assert_eq!(original_readback["method"], "github.effect.managed_label");
    assert_eq!(original_readback["state"], "settled");
    assert_eq!(
        original_readback["diagnostic"]["caller_id"],
        "label-former-gm"
    );
}
