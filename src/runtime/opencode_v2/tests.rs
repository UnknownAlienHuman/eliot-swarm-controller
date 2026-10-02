//! Local protocol fixtures only: no vendor executable, native agent, credential
//! discovery, user configuration, provider request or billable model call.
use super::*;
use crate::{
    model,
    runtime::{EffectOutcome, RuntimeCommand},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

#[derive(Clone)]
pub(crate) struct Request {
    pub method: String,
    pub path: String,
    pub target: String,
    pub body: Value,
    pub authorization: String,
}
#[derive(Clone)]
pub(crate) enum Reply {
    Json(u16, Value),
    Empty(u16),
    Redirect(String),
    Drop,
    Sse(String, bool),
}
#[derive(Default)]
pub(crate) struct World {
    pub requests: Vec<Request>,
    pub sessions: BTreeMap<String, Value>,
    pub inbox: BTreeMap<String, Value>,
    pub messages: BTreeMap<String, Value>,
    pub forms: BTreeMap<String, Vec<Value>>,
    pub permissions: BTreeMap<String, Vec<Value>>,
    pub logs: BTreeMap<String, Vec<Value>>,
    pub active: BTreeMap<String, Value>,
    pub overrides: BTreeMap<(String, String), Reply>,
    pub entries: BTreeMap<String, BTreeMap<String, Value>>,
    pub lose_create: bool,
    pub lose_prompt: bool,
    pub lose_entry_put: bool,
    pub consume_prompt: bool,
    pub event_connections: usize,
}
pub(crate) struct Fixture {
    pub options: Options,
    pub world: Arc<Mutex<World>>,
    pub dir: PathBuf,
    task: JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
impl Fixture {
    pub(crate) async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let dir = std::env::temp_dir().join(format!("eliot-oc-fixture-{}", model::new_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let connection_file = dir.join("service.credential.json");
        std::fs::write(&connection_file,json!({"schema_version":1,"endpoint":origin,"pid":424242,"username":"fixture","password":"fixture-password-only"}).to_string()).unwrap();
        let options = Options {
            service_id: "fixture_service".into(),
            connection_file,
            expected_version: "2.0.fixture".into(),
            directory: dir.join("workspace"),
            model: ModelRef {
                id: "fixture-model".into(),
                provider_id: "fixture-provider".into(),
                variant: "explicit-variant".into(),
            },
        };
        let world = Arc::new(Mutex::new(World::default()));
        let shared = world.clone();
        let selected = options.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted=listener.accept()=>{
                        let Ok((stream,_))=accepted else{break;};let world=shared.clone();let options=selected.clone();let origin=origin.clone();
                        connections.spawn(async move {
                            let Some((mut stream,request))=read_request(stream).await else{return;};
                            let reply={let mut w=world.lock().unwrap();w.requests.push(request.clone());respond(&mut w,&options,&origin,&request)};
                            write_reply(&mut stream,reply).await;
                        });
                    },
                    Some(_)=connections.join_next(),if !connections.is_empty()=>{},
                }
            }
        });
        Self {
            options,
            world,
            dir,
            task,
        }
    }
    pub(crate) async fn service(&self) -> Service {
        Service::connect(&self.options).await.unwrap()
    }
    pub(crate) fn command(&self, method: &str) -> RuntimeCommand {
        RuntimeCommand {
            operation_id: model::new_id(),
            method: method.into(),
            created_at_ms: model::now_ms().unwrap(),
            binding_id: "fixture-binding".into(),
            generation: 1,
            native_root_id: if method == "agent.open" {
                None
            } else {
                Some(root_id("fixture-binding", 1))
            },
            route: json!({"runtime":RUNTIME,"module_artifact_id":ARTIFACT_ID,"native_options":self.options}),
            input: json!({"text":"fixture instruction","delivery":"next_turn"}),
        }
    }
    pub(crate) fn posts(&self, path: &str) -> usize {
        self.world
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method == "POST" && r.path == path)
            .count()
    }
    pub(crate) fn puts(&self, path: &str) -> usize {
        self.world
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method == "PUT" && r.path == path)
            .count()
    }
    pub(crate) fn deletes(&self, path: &str) -> usize {
        self.world
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| r.method == "DELETE" && r.path == path)
            .count()
    }
    pub(crate) fn goal_command(&self, input: Value) -> RuntimeCommand {
        let mut c = self.command("agent.goal");
        c.input = input;
        c
    }
    pub(crate) fn override_get(&self, path: &str, reply: Reply) {
        self.world
            .lock()
            .unwrap()
            .overrides
            .insert(("GET".into(), path.into()), reply);
    }
    pub(crate) async fn open(&self, service: &Service) -> RuntimeCommand {
        let c = self.command("agent.open");
        assert_applied(service.execute(&c, &self.options).await);
        c
    }
}
fn respond(w: &mut World, o: &Options, origin: &str, r: &Request) -> Reply {
    if let Some(reply) = w
        .overrides
        .get(&(r.method.clone(), r.target.clone()))
        .or_else(|| w.overrides.get(&(r.method.clone(), r.path.clone())))
    {
        return reply.clone();
    }
    if r.authorization != "Basic Zml4dHVyZTpmaXh0dXJlLXBhc3N3b3JkLW9ubHk=" {
        return Reply::Json(401, json!({"unauthorized":true}));
    }
    match (r.method.as_str(), r.path.as_str()) {
        ("GET", "/api/info") => {
            return Reply::Json(
                200,
                json!({"version":o.expected_version,"pid":424242,"urls":[origin],"paths":{"tmp":"fixture"}}),
            );
        }
        ("GET", "/api/location") => {
            return Reply::Json(
                200,
                json!({"directory":o.directory,"project":{"id":"prj_fixture","directory":o.directory,"canonical":o.directory}}),
            );
        }
        ("GET", "/api/model") => {
            return Reply::Json(
                200,
                json!({"location":{"directory":o.directory},"data":[{"id":o.model.id,"modelID":o.model.id,"providerID":o.model.provider_id,"name":"Fixture Model","capabilities":{"tools":true,"input":["text"],"output":["text"]},"variants":[{"id":o.model.variant,"settings":{}}],"time":{"released":1},"cost":[],"status":"active","enabled":true,"limit":{"context":1000,"output":100}}]}),
            );
        }
        ("GET", "/api/agent") => {
            return Reply::Json(200, json!({"location":{"directory":o.directory},"data":[]}));
        }
        ("GET", "/api/event") => {
            w.event_connections += 1;
            return Reply::Sse("data: {\"fixture\":true}\n\n".to_owned(), true);
        }
        ("GET", "/api/session/active") => return Reply::Json(200, json!({"data":w.active})),
        ("POST", "/api/session") => {
            let mut session = r.body.clone();
            session["projectID"] = json!("prj_fixture");
            session["time"] = json!({"created":1,"updated":2});
            w.sessions
                .insert(session["id"].as_str().unwrap().into(), session.clone());
            if std::mem::take(&mut w.lose_create) {
                return Reply::Drop;
            }
            return Reply::Json(200, json!({"data":session}));
        }
        ("GET", "/api/session") => {
            let url = reqwest::Url::parse(&format!("http://localhost{}", r.target)).unwrap();
            let parent = url
                .query_pairs()
                .find(|(k, _)| k == "parentID")
                .map(|(_, v)| v.into_owned())
                .unwrap_or_default();
            return Reply::Json(
                200,
                json!({"data":w.sessions.values().filter(|s|s["parentID"]==parent).cloned().collect::<Vec<_>>(),"cursor":{"next":null,"previous":null}}),
            );
        }
        _ => {}
    }
    let parts = r
        .path
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    if parts.len() >= 4 && parts[0] == "api" && parts[1] == "experimental" && parts[2] == "session"
    {
        let id = parts[3];
        if parts.len() == 6
            && parts[4] == "instructions"
            && parts[5] == "entries"
            && r.method == "GET"
        {
            let entries = w
                .entries
                .get(id)
                .map(|entries| {
                    entries
                        .iter()
                        .map(|(key, value)| json!({"key":key,"value":value}))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            return Reply::Json(200, json!({"data":entries}));
        }
        if parts.len() == 7 && parts[4] == "instructions" && parts[5] == "entries" {
            let key = parts[6];
            match r.method.as_str() {
                "PUT" => {
                    w.entries
                        .entry(id.into())
                        .or_default()
                        .insert(key.into(), r.body["value"].clone());
                    if std::mem::take(&mut w.lose_entry_put) {
                        return Reply::Drop;
                    }
                    return Reply::Empty(204);
                }
                "DELETE" => {
                    if let Some(entries) = w.entries.get_mut(id) {
                        entries.remove(key);
                    }
                    return Reply::Empty(204);
                }
                _ => {}
            }
        }
    }
    if parts.len() == 5
        && parts[0] == "api"
        && parts[1] == "experimental"
        && parts[2] == "session"
        && parts[4] == "log"
        && r.method == "GET"
    {
        let Some(events) = w.logs.get(parts[3]) else {
            return Reply::Json(404, json!({"missing":true}));
        };
        let url = reqwest::Url::parse(&format!("http://localhost{}", r.target)).unwrap();
        let after = url
            .query_pairs()
            .find(|(k, _)| k == "after")
            .and_then(|(_, v)| v.parse::<u64>().ok());
        let mut body = String::new();
        let mut max = 0u64;
        for e in events {
            let seq = e["durable"]["seq"].as_u64().unwrap_or(0);
            max = max.max(seq);
            if after.is_none_or(|a| seq > a) {
                body.push_str(&format!("data: {e}\n\n"));
            }
        }
        body.push_str(&format!(
            "data: {}\n\n",
            json!({"type":"log.synced","aggregateID":parts[3],"seq":max})
        ));
        return Reply::Sse(body, false);
    }
    if parts.len() >= 3 && parts[0] == "api" && parts[1] == "session" {
        let id = parts[2];
        if parts.len() == 3 && r.method == "GET" {
            return w
                .sessions
                .get(id)
                .map(|s| Reply::Json(200, json!({"data":s})))
                .unwrap_or_else(|| Reply::Json(404, json!({"missing":true})));
        }
        if parts.len() == 4 {
            match (r.method.as_str(), parts[3]) {
                ("POST", "prompt") => {
                    let input = r.body["id"].as_str().unwrap().to_owned();
                    let item = json!({"id":input,"sessionID":id,"type":"user","time":{"created":1},"delivery":r.body["delivery"],"payload":{"text":r.body["text"],"metadata":r.body["metadata"]}});
                    if w.consume_prompt {
                        w.messages.insert(input.clone(),json!({"id":input,"type":"user","time":{"created":1},"text":r.body["text"],"metadata":r.body["metadata"]}));
                    } else {
                        w.inbox.insert(input, item.clone());
                    }
                    if std::mem::take(&mut w.lose_prompt) {
                        return Reply::Drop;
                    }
                    return Reply::Json(200, json!({"data":item}));
                }
                ("GET", "inbox") => {
                    return Reply::Json(
                        200,
                        json!({"data":w.inbox.values().filter(|i|i["sessionID"]==id).cloned().collect::<Vec<_>>()}),
                    );
                }
                ("GET", "form") => {
                    return Reply::Json(
                        200,
                        json!({"data":w.forms.get(id).cloned().unwrap_or_default()}),
                    );
                }
                ("GET", "permission") => {
                    return Reply::Json(
                        200,
                        json!({"data":w.permissions.get(id).cloned().unwrap_or_default()}),
                    );
                }
                _ => {}
            }
        }
        if parts.len() == 5 && parts[3] == "message" && r.method == "GET" {
            return w
                .messages
                .get(parts[4])
                .map(|s| Reply::Json(200, json!({"data":s})))
                .unwrap_or_else(|| Reply::Json(404, json!({"missing":true})));
        }
        if parts.len() == 6 && parts[5] == "reply" && r.method == "POST" {
            return Reply::Empty(204);
        }
    }
    Reply::Json(404, json!({"fixture_unexpected_path":true}))
}
async fn read_request(mut stream: TcpStream) -> Option<(TcpStream, Request)> {
    let mut bytes = Vec::new();
    let end = loop {
        if bytes.len() > 16384 {
            return None;
        }
        let mut block = [0; 1024];
        let n = stream.read(&mut block).await.ok()?;
        if n == 0 {
            return None;
        }
        bytes.extend_from_slice(&block[..n]);
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).ok()?;
    let mut lines = headers.lines();
    let mut first = lines.next()?.split_whitespace();
    let method = first.next()?.to_string();
    let target = first.next()?.to_string();
    let mut length = 0usize;
    let mut authorization = String::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            if key.eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().ok()?;
            } else if key.eq_ignore_ascii_case("authorization") {
                authorization = value.trim().into();
            }
        }
    }
    if length > 4 * 1024 * 1024 {
        return None;
    }
    while bytes.len() < end + length {
        let mut block = [0; 8192];
        let n = stream.read(&mut block).await.ok()?;
        if n == 0 {
            return None;
        }
        bytes.extend_from_slice(&block[..n]);
    }
    let body = if length == 0 {
        Value::Null
    } else {
        serde_json::from_slice(&bytes[end..end + length]).ok()?
    };
    let path = target.split('?').next()?.to_string();
    Some((
        stream,
        Request {
            method,
            path,
            target,
            body,
            authorization,
        },
    ))
}
async fn write_reply(stream: &mut TcpStream, reply: Reply) {
    let (status, headers, body, hold) = match reply {
        Reply::Drop => return,
        Reply::Json(status, body) => (
            status,
            "Content-Type: application/json\r\n".to_owned(),
            body.to_string(),
            false,
        ),
        Reply::Empty(status) => (status, String::new(), String::new(), false),
        Reply::Redirect(url) => (307, format!("Location: {url}\r\n"), String::new(), false),
        Reply::Sse(body, hold) => (
            200,
            "Content-Type: text/event-stream\r\n".to_owned(),
            body,
            hold,
        ),
    };
    let length = if hold {
        String::new()
    } else {
        format!("Content-Length: {}\r\n", body.len())
    };
    let text =
        format!("HTTP/1.1 {status} Fixture\r\n{headers}{length}Connection: close\r\n\r\n{body}");
    if stream.write_all(text.as_bytes()).await.is_ok() && hold {
        std::future::pending::<()>().await;
    }
}
fn assert_applied(r: crate::runtime::RuntimeOutcome) {
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
}

#[test]
fn endpoint_and_path_boundaries() {
    for good in [
        "http://127.0.0.1:1234",
        "http://[::1]:1234",
        "http://localhost:1234/",
    ] {
        assert!(http::endpoint(good).is_ok(), "{good}");
    }
    for bad in [
        "https://localhost:1234",
        "http://localhost.evil:1234",
        "http://127.0.0.1:0",
        "http://127.0.0.1/a",
        "http://u:p@127.0.0.1/",
        "http://127.0.0.1/?x=1",
        "http://127.0.0.1/#fragment",
        "http://192.0.2.1/",
    ] {
        assert!(http::endpoint(bad).is_err(), "{bad}");
    }
    for bad in ["ses", "ses/../foo", "ses?x", "ses%2fabc", "ses space"] {
        assert!(valid_id(bad, "ses").is_err());
    }
    assert_ne!(root_id("x", 1), root_id("x", 2));
    assert_eq!(input_id("op"), input_id("op"));
    assert_ne!(input_id("a"), input_id("b"));
}
#[tokio::test]
async fn explicit_route_and_connection_contract() {
    let f = Fixture::new().await;
    let mut route = json!(f.options);
    assert!(Options::parse(&route).is_ok());
    route["model"].as_object_mut().unwrap().remove("variant");
    assert!(Options::parse(&route).is_err());
    let mut record: Value =
        serde_json::from_slice(&std::fs::read(&f.options.connection_file).unwrap()).unwrap();
    record["pid"] = json!(555);
    std::fs::write(&f.options.connection_file, record.to_string()).unwrap();
    let error = Service::connect(&f.options).await.err().unwrap();
    assert_eq!(error.code, "NATIVE_INSTANCE_CHANGED");
}
#[tokio::test]
async fn exact_model_creation_and_input_admission_are_not_turn_completion() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let mut c = f.command("task.dispatch");
    c.input["task_snapshot"] = json!({"objective":"immutable-fixture"});
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert!(r.turn_id.is_none());
    assert_eq!(
        r.native_input_id.as_deref(),
        Some(input_id(&c.operation_id).as_str())
    );
    assert_eq!(r.details["execution_complete"], false);
    let w = f.world.lock().unwrap();
    let post = w
        .requests
        .iter()
        .find(|r| r.method == "POST" && r.path.ends_with("/prompt"))
        .unwrap();
    assert!(
        post.body["text"]
            .as_str()
            .unwrap()
            .contains("immutable-fixture")
    );
    assert_eq!(post.body["delivery"], "queue");
    assert_eq!(
        w.sessions.values().next().unwrap()["model"],
        json!(f.options.model)
    );
}
#[tokio::test]
async fn lost_creation_is_resolved_by_exact_read_without_create_replay() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.world.lock().unwrap().lose_create = true;
    let c = f.command("agent.open");
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Unknown));
    assert_eq!(
        r.native_root_id.as_deref(),
        Some(root_id(&c.binding_id, c.generation).as_str())
    );
    assert_applied(s.reconcile(&c, &f.options).await);
    assert_eq!(f.posts("/api/session"), 1);
}
#[tokio::test]
async fn lost_prompt_is_read_back_from_inbox_without_resend() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    f.world.lock().unwrap().lose_prompt = true;
    let c = f.command("agent.send");
    assert!(matches!(
        s.execute(&c, &f.options).await.outcome,
        EffectOutcome::Unknown
    ));
    assert_applied(s.reconcile(&c, &f.options).await);
    assert_eq!(
        f.posts(&format!(
            "/api/session/{}/prompt",
            c.native_root_id.unwrap()
        )),
        1
    );
}
#[tokio::test]
async fn consumed_prompt_readback_is_not_a_second_delivery() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    {
        let mut w = f.world.lock().unwrap();
        w.lose_prompt = true;
        w.consume_prompt = true;
    }
    let c = f.command("agent.send");
    assert!(matches!(
        s.execute(&c, &f.options).await.outcome,
        EffectOutcome::Unknown
    ));
    let r = s.reconcile(&c, &f.options).await;
    assert_eq!(r.details["evidence"], "projected_message_readback");
    assert_applied(r);
    assert_eq!(
        f.posts(&format!(
            "/api/session/{}/prompt",
            c.native_root_id.unwrap()
        )),
        1
    );
}
#[tokio::test]
async fn unsupported_exact_turn_steering_never_posts() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let mut c = f.command("agent.send");
    c.input["delivery"] = json!("steer");
    c.input["expected_turn_id"] = json!("turn_fixture");
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Rejected));
    assert_eq!(r.details["code"], "UNSUPPORTED_EXACT_TURN_STEER");
    assert_eq!(
        f.posts(&format!(
            "/api/session/{}/prompt",
            c.native_root_id.unwrap()
        )),
        0
    );
}
#[tokio::test]
async fn changed_binding_or_model_cannot_receive_work() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let c = f.command("agent.send");
    let root = c.native_root_id.clone().unwrap();
    f.world.lock().unwrap().sessions.get_mut(&root).unwrap()["metadata"]["eliot"]["binding"] =
        json!("stranger");
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Rejected));
    assert_eq!(r.details["code"], "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(f.posts(&format!("/api/session/{root}/prompt")), 0);
}
#[tokio::test]
async fn redirect_does_not_forward_credentials_or_replay_post() {
    let f = Fixture::new().await;
    let sink = Fixture::new().await;
    let s = f.service().await;
    let endpoint: Value =
        serde_json::from_slice(&std::fs::read(&sink.options.connection_file).unwrap()).unwrap();
    f.world.lock().unwrap().overrides.insert(
        ("POST".into(), "/api/session".into()),
        Reply::Redirect(format!("{}/stolen", endpoint["endpoint"].as_str().unwrap())),
    );
    assert!(matches!(
        s.execute(&f.command("agent.open"), &f.options)
            .await
            .outcome,
        EffectOutcome::Unknown
    ));
    assert_eq!(f.posts("/api/session"), 1);
    assert!(sink.world.lock().unwrap().requests.is_empty());
}
#[tokio::test]
async fn active_child_and_stale_retained_child_prevent_false_family_idle() {
    let f = Fixture::new().await;
    let s = f.service().await;
    let c = f.open(&s).await;
    let root = root_id(&c.binding_id, c.generation);
    {
        let mut w = f.world.lock().unwrap();
        w.sessions.insert("ses_child".into(),json!({"id":"ses_child","parentID":root,"projectID":"prj_fixture","time":{"created":1,"updated":2}}));
        w.active
            .insert("ses_child".into(), json!({"type":"running"}));
    }
    let first = s
        .snapshot(&root, &Value::Null, &Default::default())
        .await
        .unwrap();
    assert_eq!(first.state["execution"], "observed_active");
    assert_eq!(first.state["family_completeness"], "partial");
    f.world.lock().unwrap().sessions.remove("ses_child");
    let second = s
        .snapshot(&root, &first.state, &Default::default())
        .await
        .unwrap();
    assert_eq!(second.state["execution"], "observed_active");
    assert_eq!(second.state["observed_children"][0]["observed_now"], false);
}
#[tokio::test]
async fn malformed_inventory_and_unknown_active_schema_are_not_empty_healthy() {
    let f = Fixture::new().await;
    let s = f.service().await;
    let c = f.open(&s).await;
    let root = root_id(&c.binding_id, c.generation);
    f.override_get(
        "/api/session",
        Reply::Json(200, json!({"data":[],"cursor":{"has_next":true}})),
    );
    f.override_get(
        "/api/session/active",
        Reply::Json(200, json!({"data":{"strange":{}}})),
    );
    let result = s
        .snapshot(
            &root,
            &json!({"observed_children":[{"sessionId":"ses_old"}]}),
            &Default::default(),
        )
        .await
        .unwrap();
    assert_eq!(result.state["enumeration_complete"], false);
    assert_eq!(result.state["execution"], "unknown");
    assert_eq!(result.state["observed_children"][0]["sessionId"], "ses_old");
    assert!(result.state["gaps"].as_u64().unwrap() > 0);
}
#[tokio::test]
async fn pending_questions_use_the_whole_atlas_donor_before_persistence() {
    let f = Fixture::new().await;
    let s = f.service().await;
    let c = f.open(&s).await;
    let root = root_id(&c.binding_id, c.generation);
    let request = json!({"id":"frm_fixture","sessionID":root,"title":"DB_PASSWORD=sensitive-local-password-94857","fields":[]});
    f.world
        .lock()
        .unwrap()
        .forms
        .insert(root.clone(), vec![request.clone()]);
    let result = s
        .snapshot(&root, &Value::Null, &Default::default())
        .await
        .unwrap();
    let p = &result.state["pending_requests"][0];
    assert_eq!(p["request_id"], "frm_fixture");
    assert_eq!(
        p["fingerprint"],
        model::digest(model::canonical(&request).unwrap().as_bytes())
    );
    assert!(
        !result
            .state
            .to_string()
            .contains("sensitive-local-password-94857")
    );
    assert!(result.state.to_string().contains("REDACTED"));
}
#[tokio::test]
async fn reply_targets_the_owned_pending_request_not_a_foreign_family() {
    let f = Fixture::new().await;
    let s = f.service().await;
    let open = f.open(&s).await;
    let root = root_id(&open.binding_id, open.generation);
    let request =
        json!({"id":"per_fixture","sessionID":root,"action":"shell","resources":["fixture"]});
    f.world
        .lock()
        .unwrap()
        .permissions
        .insert(root.clone(), vec![request.clone()]);
    let mut c = f.command("agent.reply");
    c.input = json!({"reply":{"kind":"permission","session_id":root,"request_id":"per_fixture","fingerprint":model::digest(model::canonical(&request).unwrap().as_bytes()),"body":{"decision":"once"}}});
    assert_applied(s.execute(&c, &f.options).await);
    c.input["reply"]["session_id"] = json!("ses_foreign");
    f.world.lock().unwrap().sessions.insert(
        "ses_foreign".into(),
        json!({"id":"ses_foreign","projectID":"prj_foreign","time":{"created":1,"updated":1}}),
    );
    assert!(matches!(
        s.execute(&c, &f.options).await.outcome,
        EffectOutcome::Rejected
    ));
    assert_eq!(
        f.posts("/api/session/ses_foreign/permission/per_fixture/reply"),
        0
    );
}
#[tokio::test]
async fn sse_failure_records_a_gap_without_native_mutation() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.override_get(
        "/api/event",
        Reply::Sse(
            "event: effect/httpapi/stream/failure\ndata: []\n\n".to_owned(),
            false,
        ),
    );
    let (stop, receiver) = tokio::sync::watch::channel(false);
    let mut reader = s.events(receiver);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if reader.state.borrow().gaps > 0 {
                break;
            }
            reader.state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(reader.state.borrow().last_gap, Some("SSE_NATIVE_FAILURE"));
    assert!(!reader.state.borrow().connected);
    stop.send(true).unwrap();
    assert!(
        f.world
            .lock()
            .unwrap()
            .requests
            .iter()
            .all(|r| r.method == "GET")
    );
}

fn goal_entry_path(root: &str) -> String {
    format!("/api/experimental/session/{root}/instructions/entries/eliot.goal")
}
fn goal_prompt_path(root: &str) -> String {
    format!("/api/session/{root}/prompt")
}
fn stored_goal(f: &Fixture, root: &str) -> Option<Value> {
    f.world
        .lock()
        .unwrap()
        .entries
        .get(root)
        .and_then(|entries| entries.get("eliot.goal"))
        .cloned()
}

#[tokio::test]
async fn goal_set_records_entry_and_admits_one_activation_prompt() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let c = f.goal_command(json!({"action":"set","objective":"fixture goal objective"}));
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["completion_condition"], "native_goal_recorded");
    assert_eq!(r.details["goal"]["action"], "set");
    assert_eq!(r.details["goal"]["status"], "active");
    assert_eq!(r.details["goal"]["revision"], 1);
    assert_eq!(r.details["goal"]["native_goal_api"], false);
    assert_eq!(r.details["goal"]["mutation_sent"], true);
    assert_eq!(r.details["goal"]["model_work_started"], true);
    assert_eq!(
        r.native_input_id.as_deref(),
        Some(input_id(&c.operation_id).as_str())
    );
    assert_eq!(f.puts(&goal_entry_path(&root)), 1);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 1);
    let stored = stored_goal(&f, &root).unwrap();
    assert_eq!(stored["objective"], "fixture goal objective");
    assert_eq!(stored["status"], "active");
    assert_eq!(stored["revision"], 1);
    assert_eq!(stored["updated_by_operation_id"], c.operation_id);
}

#[tokio::test]
async fn goal_set_with_same_objective_is_noop_without_second_prompt() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let first = f.goal_command(json!({"action":"set","objective":"same objective"}));
    assert_applied(s.execute(&first, &f.options).await);
    let second = f.goal_command(json!({"action":"set","objective":"same objective"}));
    let r = s.execute(&second, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["goal"]["evidence"], "preexisting_exact_readback");
    assert_eq!(r.details["goal"]["mutation_sent"], false);
    assert_eq!(r.details["goal"]["revision"], 1);
    assert!(r.native_input_id.is_none());
    assert_eq!(f.puts(&goal_entry_path(&root)), 1);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 1);
}

#[tokio::test]
async fn goal_pause_changes_only_status_and_never_prompts_or_interrupts() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let set = f.goal_command(json!({"action":"set","objective":"pausable objective"}));
    assert_applied(s.execute(&set, &f.options).await);
    let pause = f.goal_command(json!({"action":"pause"}));
    let r = s.execute(&pause, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["goal"]["status"], "paused");
    assert_eq!(r.details["goal"]["revision"], 2);
    assert_eq!(r.details["goal"]["model_work_started"], false);
    assert_eq!(f.puts(&goal_entry_path(&root)), 2);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 1);
    assert!(
        f.world
            .lock()
            .unwrap()
            .requests
            .iter()
            .all(|request| !request.path.contains("interrupt"))
    );
    // A second pause is an exact no-op: no further PUT.
    let pause_again = f.goal_command(json!({"action":"pause"}));
    assert_applied(s.execute(&pause_again, &f.options).await);
    assert_eq!(f.puts(&goal_entry_path(&root)), 2);
}

#[tokio::test]
async fn goal_resume_reactivates_and_admits_one_prompt() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let set = f.goal_command(json!({"action":"set","objective":"resumable objective"}));
    assert_applied(s.execute(&set, &f.options).await);
    let pause = f.goal_command(json!({"action":"pause"}));
    assert_applied(s.execute(&pause, &f.options).await);
    let resume = f.goal_command(json!({"action":"resume"}));
    let r = s.execute(&resume, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["goal"]["status"], "active");
    assert_eq!(r.details["goal"]["revision"], 3);
    assert_eq!(
        r.native_input_id.as_deref(),
        Some(input_id(&resume.operation_id).as_str())
    );
    assert_eq!(f.posts(&goal_prompt_path(&root)), 2);
    // Resuming an already active goal is a no-op without another prompt.
    let resume_again = f.goal_command(json!({"action":"resume"}));
    assert_applied(s.execute(&resume_again, &f.options).await);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 2);
    assert_eq!(f.puts(&goal_entry_path(&root)), 3);
}

#[tokio::test]
async fn goal_edit_replaces_objective_and_bumps_revision() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let set = f.goal_command(json!({"action":"set","objective":"first objective"}));
    assert_applied(s.execute(&set, &f.options).await);
    let edit = f.goal_command(json!({"action":"edit","objective":"second objective"}));
    let r = s.execute(&edit, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["goal"]["revision"], 2);
    assert_eq!(
        stored_goal(&f, &root).unwrap()["objective"],
        "second objective"
    );
    assert_eq!(f.posts(&goal_prompt_path(&root)), 2);
    // Editing to the same objective is a no-op.
    let edit_same = f.goal_command(json!({"action":"edit","objective":"second objective"}));
    let r = s.execute(&edit_same, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["goal"]["evidence"], "preexisting_exact_readback");
    assert_eq!(f.puts(&goal_entry_path(&root)), 2);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 2);
}

#[tokio::test]
async fn goal_clear_removes_entry_and_clear_when_absent_is_noop() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let set = f.goal_command(json!({"action":"set","objective":"clearable objective"}));
    assert_applied(s.execute(&set, &f.options).await);
    let clear = f.goal_command(json!({"action":"clear"}));
    let r = s.execute(&clear, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["goal"]["present"], false);
    assert_eq!(f.deletes(&goal_entry_path(&root)), 1);
    assert!(stored_goal(&f, &root).is_none());
    let clear_again = f.goal_command(json!({"action":"clear"}));
    let r = s.execute(&clear_again, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["goal"]["evidence"], "preexisting_exact_readback");
    assert_eq!(f.deletes(&goal_entry_path(&root)), 1);
}

#[tokio::test]
async fn goal_edit_pause_resume_without_record_are_rejected_without_mutation() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    for input in [
        json!({"action":"edit","objective":"nothing to edit"}),
        json!({"action":"pause"}),
        json!({"action":"resume"}),
    ] {
        let c = f.goal_command(input);
        let r = s.execute(&c, &f.options).await;
        assert!(matches!(r.outcome, EffectOutcome::Rejected), "{r:?}");
        assert_eq!(r.details["code"], "NATIVE_GOAL_ABSENT");
    }
    assert_eq!(f.puts(&goal_entry_path(&root)), 0);
    assert_eq!(f.deletes(&goal_entry_path(&root)), 0);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 0);
}

#[tokio::test]
async fn lost_goal_put_reconciles_by_readback_only() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    f.world.lock().unwrap().lose_entry_put = true;
    let c = f.goal_command(json!({"action":"set","objective":"lost put objective"}));
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Unknown), "{r:?}");
    // The lost response stopped execution before any activation prompt.
    assert_eq!(f.posts(&goal_prompt_path(&root)), 0);
    let r = s.reconcile(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(r.details["completion_condition"], "native_goal_recorded");
    assert_eq!(r.details["goal"]["evidence"], "exact_state_reconciliation");
    // The record is the completion condition; the never-sent activation is
    // honestly reported as absent instead of being replayed.
    assert_eq!(r.details["goal"]["activation_input_id"], Value::Null);
    assert_eq!(r.details["goal"]["model_work_started"], false);
    assert_eq!(f.puts(&goal_entry_path(&root)), 1);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 0);
}

#[tokio::test]
async fn lost_activation_prompt_reconciles_from_inbox_without_resend() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    f.world.lock().unwrap().lose_prompt = true;
    let c = f.goal_command(json!({"action":"set","objective":"lost prompt objective"}));
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Unknown), "{r:?}");
    let r = s.reconcile(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Applied), "{r:?}");
    assert_eq!(
        r.native_input_id.as_deref(),
        Some(input_id(&c.operation_id).as_str())
    );
    assert_eq!(r.details["goal"]["model_work_started"], true);
    assert_eq!(f.puts(&goal_entry_path(&root)), 1);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 1);
}

#[tokio::test]
async fn goal_reconcile_stays_unknown_when_entry_was_changed_externally() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    f.world.lock().unwrap().lose_entry_put = true;
    let c = f.goal_command(json!({"action":"set","objective":"original objective"}));
    assert!(matches!(
        s.execute(&c, &f.options).await.outcome,
        EffectOutcome::Unknown
    ));
    // An external writer replaced the record between execute and reconcile.
    f.world.lock().unwrap().entries.get_mut(&root).unwrap().insert(
        "eliot.goal".into(),
        json!({"objective":"external objective","status":"active","revision":9,"updated_by_operation_id":"external-writer"}),
    );
    let r = s.reconcile(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Unknown), "{r:?}");
    assert_eq!(f.puts(&goal_entry_path(&root)), 1);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 0);
}

#[tokio::test]
async fn changed_binding_or_model_blocks_goal_before_any_mutation() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let c = f.goal_command(json!({"action":"set","objective":"blocked objective"}));
    f.world.lock().unwrap().sessions.get_mut(&root).unwrap()["metadata"]["eliot"]["binding"] =
        json!("stranger");
    let r = s.execute(&c, &f.options).await;
    assert!(matches!(r.outcome, EffectOutcome::Rejected), "{r:?}");
    assert_eq!(r.details["code"], "NATIVE_IDENTITY_MISMATCH");
    assert_eq!(f.puts(&goal_entry_path(&root)), 0);
    assert_eq!(f.posts(&goal_prompt_path(&root)), 0);
}

#[tokio::test]
async fn snapshot_exposes_goal_axis_with_digest_only() {
    let f = Fixture::new().await;
    let s = f.service().await;
    f.open(&s).await;
    let root = root_id("fixture-binding", 1);
    let c = f.goal_command(json!({"action":"set","objective":"snapshot secret objective"}));
    assert_applied(s.execute(&c, &f.options).await);
    let snapshot = s
        .snapshot(&root, &Value::Null, &Default::default())
        .await
        .unwrap();
    let goal = &snapshot.state["goal_configuration"];
    assert_eq!(goal["complete"], true);
    assert_eq!(goal["present"], true);
    assert_eq!(goal["status"], "active");
    assert_eq!(goal["revision"], 1);
    assert_eq!(goal["objective_content_persisted"], false);
    assert_eq!(goal["native_goal_api"], false);
    assert_eq!(goal["continuation_owner"], "controller_record");
    assert!(
        goal["objective_digest"]
            .as_str()
            .is_some_and(|digest| digest.starts_with("sha256:"))
    );
    assert!(
        !snapshot
            .state
            .to_string()
            .contains("snapshot secret objective")
    );
}

pub(crate) fn child_events(child: &str, root: &str, runs: &[(&str, Option<&str>)]) -> Vec<Value> {
    // Events carry the full native envelope the log reader verifies:
    // durable version, per-event sessionID and a finite creation time.
    let mut events = vec![
        json!({"id":format!("evt_{child}_created"),"type":"session.created","version":1,
        "created":1.0,
        "durable":{"aggregateID":child,"seq":1,"version":1},
        "data":{"sessionID":child,"parentID":root}}),
    ];
    let mut seq = 1u64;
    for (run, terminal) in runs {
        seq += 1;
        events.push(
            json!({"id":format!("evt_{run}_started"),"type":"session.execution.started","version":1,
            "created":1.0,
            "durable":{"aggregateID":child,"seq":seq,"version":1},"data":{"sessionID":child}}),
        );
        if let Some(outcome) = terminal {
            seq += 1;
            let (kind, mut data) = match *outcome {
                "completed" => ("session.execution.succeeded", json!({})),
                "failed" => ("session.execution.failed", json!({"error":{}})),
                _ => ("session.execution.interrupted", json!({"reason":"user"})),
            };
            data["sessionID"] = json!(child);
            events.push(
                json!({"id":format!("evt_{run}_terminal"),"type":kind,"version":1,
                "created":1.0,
                "durable":{"aggregateID":child,"seq":seq,"version":1},"data":data}),
            );
        }
    }
    events
}

#[test]
fn session_scan_records_periods_and_refuses_gap_terminals() {
    let mut scan = SessionScan::restore("ses_kid", "ses_root", None).unwrap();
    for e in child_events("ses_kid", "ses_root", &[("run_1", Some("failed"))]) {
        scan.consume(&e).unwrap();
    }
    assert!(scan.is_terminal());
    assert_eq!(scan.disposition(), "failed");
    let turn = scan.last_turn().unwrap();
    assert_eq!(turn["sessionId"], "ses_kid");
    assert_eq!(turn["turnId"], "evt_run_1_started");
    assert_eq!(turn["terminal"], "failed");
    // A second start while one is open is a restart gap, never a terminal.
    let mut scan = SessionScan::restore("ses_kid", "ses_root", None).unwrap();
    for e in child_events(
        "ses_kid",
        "ses_root",
        &[("run_1", None), ("run_2", Some("completed"))],
    ) {
        scan.consume(&e).unwrap();
    }
    assert_eq!(scan.disposition(), "unknown");
    assert!(scan.last_turn().is_none());
    assert!(scan.turns().is_empty());
    // The checkpoint round-trips through JSON with its uncertainty intact.
    let saved = serde_json::to_value(&scan).unwrap();
    let restored = SessionScan::restore("ses_kid", "ses_root", Some(&saved)).unwrap();
    assert_eq!(restored.disposition(), "unknown");
    // A scan never migrates across sessions or parents.
    assert!(SessionScan::restore("ses_other", "ses_root", Some(&saved)).is_err());
    assert!(SessionScan::restore("ses_kid", "ses_other", Some(&saved)).is_err());
}

#[tokio::test]
async fn child_logs_bind_terminal_and_coverage_without_completeness() {
    let f = Fixture::new().await;
    let s = f.service().await;
    let c = f.open(&s).await;
    let root = root_id(&c.binding_id, c.generation);
    {
        let mut w = f.world.lock().unwrap();
        for id in ["ses_a", "ses_b", "ses_c"] {
            w.sessions.insert(
                id.into(),
                json!({"id":id,"parentID":root,"projectID":"prj_fixture","time":{"created":1,"updated":2}}),
            );
        }
        w.active.insert("ses_b".into(), json!({"type":"running"}));
        w.logs.insert(
            "ses_a".into(),
            child_events("ses_a", &root, &[("run_a", Some("completed"))]),
        );
        w.logs.insert(
            "ses_b".into(),
            child_events("ses_b", &root, &[("run_b", None)]),
        );
    }
    let bound: BTreeSet<String> = ["ses_a".to_owned(), "ses_b".to_owned()]
        .into_iter()
        .collect();
    let first = s.snapshot(&root, &Value::Null, &bound).await.unwrap();
    let st = &first.state;
    // Evidence grows per axis; completeness stays partial by design.
    assert_eq!(st["family_completeness"], "partial");
    assert_eq!(st["family_coverage"]["members_total"], 3);
    assert_eq!(st["family_coverage"]["members_observed_now"], 3);
    assert_eq!(st["family_coverage"]["members_active_verified"], 1);
    assert_eq!(st["family_coverage"]["members_with_execution_evidence"], 2);
    assert_eq!(st["family_coverage"]["members_with_terminal_evidence"], 1);
    let turns = st["turns"].as_array().unwrap();
    assert!(turns.iter().any(|t| t["sessionId"] == "ses_a"
        && t["turnId"] == "evt_run_a_started"
        && t["terminal"] == "completed"));
    assert!(turns.iter().any(|t| t["sessionId"] == "ses_b"
        && t["turnId"] == "evt_run_b_started"
        && t["terminal"].is_null()));
    let children = st["observed_children"].as_array().unwrap();
    let child_a = children.iter().find(|c| c["sessionId"] == "ses_a").unwrap();
    assert_eq!(child_a["last_turn"]["terminal"], "completed");
    assert_eq!(child_a["execution_disposition"], "completed");
    // The untracked child is never log-read: it stays without evidence and
    // without a read failure, and the partial snapshot still succeeds.
    let child_c = children.iter().find(|c| c["sessionId"] == "ses_c").unwrap();
    assert!(child_c["last_turn"].is_null());
    let failures = st["failures"].as_array().unwrap();
    assert!(
        failures
            .iter()
            .all(|x| !(x["session_id"] == "ses_c" && x["source"] == "child_execution_log")),
        "untracked children are never log-read"
    );
    // Second snapshot: B's log gains a cancellation consumed through the saved
    // anchor; A's recorded terminal persists although A is no longer tracked.
    {
        let mut w = f.world.lock().unwrap();
        w.logs.get_mut("ses_b").unwrap().push(
            json!({"id":"evt_run_b_terminal","type":"session.execution.interrupted","version":1,
            "created":1.0,
            "durable":{"aggregateID":"ses_b","seq":3,"version":1},"data":{"sessionID":"ses_b","reason":"user"}}),
        );
        w.active.remove("ses_b");
    }
    let second = s
        .snapshot(&root, &first.state, &BTreeSet::new())
        .await
        .unwrap();
    let st = &second.state;
    assert_eq!(st["family_completeness"], "partial");
    assert_eq!(st["family_coverage"]["members_with_terminal_evidence"], 2);
    let children = st["observed_children"].as_array().unwrap();
    assert_eq!(
        children.iter().find(|c| c["sessionId"] == "ses_a").unwrap()["last_turn"]["terminal"],
        "completed"
    );
    assert_eq!(
        children.iter().find(|c| c["sessionId"] == "ses_b").unwrap()["last_turn"]["terminal"],
        "cancelled"
    );
    assert_eq!(st["execution"], "not_observed_active");
}
