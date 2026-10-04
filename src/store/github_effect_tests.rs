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
        ("GET", "/repos/owner/repo") => Some((
            200,
            json!({
                "id":44,"full_name":"owner/repo","html_url":"https://github.com/owner/repo"
            }),
        )),
        ("GET", "/repos/owner/repo/issues/7") => {
            let current = state.lock().await;
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
