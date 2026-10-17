//! Shared ephemeral rings. Database access below is authorization/readback only;
//! publication never writes Observations, Operations, artifacts or bus events.
use super::{Store, current_principal, meta, object_scope, operations, runtime};
use crate::{error::{Error, Result}, model::{self, Principal}};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::{collections::{HashMap, VecDeque}, sync::Arc};
use swarm_contracts::native_stream::*;

#[derive(Default)]
pub(super) struct NativeStreams {
    rings: HashMap<StreamScope, Ring>,
    viewers: HashMap<String, Viewer>,
}

struct Ring {
    epoch: String,
    publisher_link: String,
    records: VecDeque<Record>,
    bytes: usize,
    cursor: u64,
    source_sequence: u64,
    source_gap: bool,
    lost_through: u64,
    updated_at_ms: i64,
}

#[derive(Clone, serde::Serialize)]
struct Record {
    cursor: u64,
    frame: StreamPublish,
    #[serde(skip)]
    bytes: usize,
}

struct Viewer {
    client_id: String,
    link_id: String,
    scope: StreamScope,
    epoch: String,
    expires_at_ms: i64,
}

/// The wire scope plus sealed Store-derived content identity. No grant is
/// derived from module-supplied Task/project/owner JSON.
struct NativeStreamSource {
    scope: StreamScope,
    content: object_scope::NativeStreamContentSource,
    module_client_id: String,
    module_link_id: String,
}

fn denied() -> Error {
    Error::new("NOT_FOUND", "authorized native content stream is unavailable")
}
fn stale() -> Error {
    Error::new("STREAM_STALE", "reopen against current authorized native content")
}
fn bounded_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

pub(super) async fn call(
    store: &Store, principal: Principal, method: String, params: Value,
) -> Result<Value> {
    let shared = Arc::clone(&store.native_streams);
    store.run(move |db| {
        // Every read re-resolves revocation, role and exact object rights in
        // the same serialized Store job that selects the ring records.
        let principal = current_principal(db, principal)?;
        let now = model::now_ms()?;
        let mut streams = shared.lock()
            .map_err(|_| Error::new("STREAM_UNAVAILABLE", "stream state unavailable"))?;
        streams.viewers.retain(|_, v| v.expires_at_ms >= now);
        streams.rings.retain(|_, r| now.saturating_sub(r.updated_at_ms) <= STALE_AFTER_MS);
        match method.as_str() {
            PUBLISH_METHOD => {
                let frame: StreamPublish = serde_json::from_value(params)
                    .map_err(|_| Error::invalid("invalid native stream publication"))?;
                let (id, generation, binding) = runtime::scope(db, &principal, true)?;
                if frame.scope.binding_id != id || frame.scope.generation != generation
                    || binding["observation"]["bridge_boot_id"] != frame.scope.boot_id
                {
                    return Err(stale());
                }
                let source = validate_scope(db, &frame.scope, Some(&principal.link_id))?;
                if source.module_client_id != principal.client_id {
                    return Err(stale());
                }
                streams.publish(frame, principal.link_id, now)
            }
            "stream.open" => {
                let request: StreamOpen = serde_json::from_value(params)
                    .map_err(|_| Error::invalid("invalid stream.open parameters"))?;
                if !bounded_id(&request.operation_id) {
                    return Err(Error::invalid("stream identifiers exceed bounds"));
                }
                let source = source_for_operation(db, &request.operation_id)?;
                let grant = authorize_content(db, &principal, &source)?;
                let scope = source.scope;
                let ring = streams.rings.get(&scope).ok_or_else(denied)?;
                if ring.publisher_link != source.module_link_id {
                    return Err(stale());
                }
                if streams.viewers.len() >= MAX_VIEWERS {
                    return Err(Error::new("STREAM_CAPACITY", "close an existing stream view"));
                }
                let stream_id = model::new_id();
                let epoch = ring.epoch.clone();
                let cursor = ring.cursor;
                let lost_through = ring.lost_through;
                streams.viewers.insert(stream_id.clone(), Viewer {
                    client_id: principal.client_id,
                    link_id: principal.link_id,
                    scope: scope.clone(),
                    epoch: epoch.clone(),
                    expires_at_ms: now.saturating_add(STALE_AFTER_MS),
                });
                Ok(json!({"stream_id":stream_id,"epoch":epoch,"scope":scope,
                    "cursor":cursor,"after":0,"lost_through":lost_through,
                    "ephemeral":true,"text_delivery":"complete_logical_records_only",
                    "content_grant":{"level":grant.level.as_str(),"basis":grant.basis.as_str()},
                    "hidden_reasoning":"unavailable"}))
            }
            "stream.read" => {
                let request: StreamRead = serde_json::from_value(params)
                    .map_err(|_| Error::invalid("invalid stream.read parameters"))?;
                if !bounded_id(&request.stream_id) {
                    return Err(Error::invalid("stream identifier exceeds bounds"));
                }
                let limit = request.limit.unwrap_or(32);
                if !(1..=MAX_READ_RECORDS).contains(&limit) {
                    return Err(Error::invalid("stream read limit outside bounds"));
                }
                let viewer = streams.viewers.get(&request.stream_id).ok_or_else(denied)?;
                if viewer.client_id != principal.client_id || viewer.link_id != principal.link_id {
                    return Err(denied());
                }
                let source = validate_scope(db, &viewer.scope, None)?;
                let grant = authorize_content(db, &principal, &source)?;
                let ring = streams.rings.get(&viewer.scope).ok_or_else(stale)?;
                if ring.epoch != viewer.epoch {
                    return Err(stale());
                }
                if ring.publisher_link != source.module_link_id {
                    return Err(stale());
                }
                if request.after > ring.cursor {
                    return Err(Error::invalid("cursor is ahead of this ring"));
                }
                let mut selected = Vec::new();
                let mut bytes = 0;
                let mut next = request.after.max(ring.lost_through);
                for record in ring.records.iter().filter(|r| r.cursor > request.after) {
                    if selected.len() >= limit || bytes + record.bytes > MAX_READ_BYTES - 4096 {
                        break;
                    }
                    bytes += record.bytes;
                    next = record.cursor;
                    selected.push(record.clone());
                }
                // Page admission follows host cursors; rendering order follows
                // native item/part facts even when a late final replaces an
                // earlier item's deltas. next remains the selected cursor cut.
                selected.sort_by_key(|record| (
                    record.frame.item_order, record.frame.part_index, record.cursor,
                ));
                let value = json!({"stream_id":request.stream_id,"epoch":ring.epoch,
                    "records":selected,"next":next,"head":ring.cursor,
                    "cursor_loss":request.after < ring.lost_through,
                    "lost_through":ring.lost_through,"source_gap":ring.source_gap,
                    "has_more":next < ring.cursor,"ephemeral":true,
                    "ordering":"item_order_part_index_then_cursor",
                    "content_grant":{"level":grant.level.as_str(),"basis":grant.basis.as_str()},
                    "text_delivery":"complete_logical_records_only","hidden_reasoning":"unavailable"});
                streams.viewers.get_mut(&request.stream_id).expect("retained viewer")
                    .expires_at_ms = now.saturating_add(STALE_AFTER_MS);
                Ok(value)
            }
            "stream.close" => {
                let request: StreamClose = serde_json::from_value(params)
                    .map_err(|_| Error::invalid("invalid stream.close parameters"))?;
                if !bounded_id(&request.stream_id) {
                    return Err(Error::invalid("stream identifier exceeds bounds"));
                }
                let viewer = streams.viewers.get(&request.stream_id).ok_or_else(denied)?;
                if viewer.client_id != principal.client_id || viewer.link_id != principal.link_id {
                    return Err(denied());
                }
                streams.viewers.remove(&request.stream_id);
                Ok(json!({"closed":true}))
            }
            _ => Err(Error::invalid("unknown native stream method")),
        }
    }).await
}

fn source_for_operation(db: &Connection, operation_id: &str) -> Result<NativeStreamSource> {
    if !bounded_id(operation_id) {
        return Err(denied());
    }
    let content = object_scope::native_stream_content_source(db, operation_id)?
        .ok_or_else(denied)?;
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "task.dispatch"
        || !matches!(operation["state"].as_str(), Some("native_accepted" | "settled" | "outcome_unknown"))
    {
        return Err(denied());
    }
    let binding_id = model::text(&operation, "binding_id")?.to_owned();
    let generation = model::positive(&operation, "binding_generation")?;
    let binding = operations::get_binding(db, &binding_id, generation)?;
    let scope = StreamScope {
        binding_id, generation,
        boot_id: model::text(&binding["observation"], "bridge_boot_id")?.to_owned(),
        task_id: content.identity.task_id.clone(),
        task_revision: content.identity.task_revision.ok_or_else(denied)?,
        attempt_id: content.identity.attempt_id.clone().ok_or_else(denied)?,
        operation_id: operation_id.to_owned(),
        native_session_id: model::text(&operation["native_refs"], "session_id")?.to_owned(),
        turn_id: model::text(&operation["native_refs"], "turn_id")?.to_owned(),
    };
    if content.identity.binding_id.as_deref() != Some(scope.binding_id.as_str())
        || content.identity.binding_generation != Some(scope.generation)
    {
        return Err(denied());
    }
    let newest: i64 = db.query_row(
        "SELECT MAX(generation) FROM bindings WHERE binding_id=?1",
        [&scope.binding_id], |r| r.get(0),
    )?;
    let observation = &binding["observation"];
    if newest != scope.generation || !binding["released_at_ms"].is_null()
        || binding["native_root_id"] != scope.native_session_id
        || observation["connection"] != "connected"
    {
        return Err(stale());
    }
    let module_id = model::text(observation, "module_client_id")?;
    let client = meta(db, &format!("client:{module_id}"))?.ok_or_else(stale)?;
    if client["role"] != "module" || client["disabled"] == true
        || client["binding_id"] != scope.binding_id
        || client["binding_generation"] != scope.generation
    {
        return Err(stale());
    }
    Ok(NativeStreamSource {
        module_client_id: module_id.to_owned(),
        module_link_id: model::text(observation, "module_link_id")?.to_owned(),
        scope, content,
    })
}

fn validate_scope(
    db: &Connection, scope: &StreamScope, publisher_link: Option<&str>,
) -> Result<NativeStreamSource> {
    let source = source_for_operation(db, &scope.operation_id)?;
    if source.scope != *scope
        || publisher_link.is_some_and(|link| source.module_link_id != link)
    {
        return Err(stale());
    }
    Ok(source)
}

fn authorize_content(
    db: &Connection, principal: &Principal, source: &NativeStreamSource,
) -> Result<object_scope::StreamContentGrant> {
    match object_scope::resolve_native_stream_content(db, principal, &source.content)? {
        Some(grant) if grant.level == object_scope::ArtifactReadLevel::Bytes => Ok(grant),
        _ => Err(denied()),
    }
}

impl NativeStreams {
    fn publish(&mut self, mut frame: StreamPublish, link: String, now: i64) -> Result<Value> {
        let encoded = serde_json::to_vec(&frame)?;
        if encoded.len() > MAX_FRAME_BYTES || frame.source_sequence == 0
            || frame.scope.generation <= 0 || frame.scope.task_revision <= 0 || frame.part_index > 65_535
            || frame.item_order > 1_000_000 || frame.native_cursor.len() > 4096
            || [&frame.scope.binding_id, &frame.scope.boot_id, &frame.scope.task_id,
                &frame.scope.operation_id, &frame.scope.attempt_id, &frame.scope.native_session_id, &frame.scope.turn_id,
                &frame.item_id].iter().any(|s| s.is_empty() || s.len() > 256)
        {
            return Err(Error::invalid("native stream frame exceeds closed bounds"));
        }
        // No streaming redactor has been qualified. Discard partial text at
        // ingress; only full native logical records reach the shared ring.
        // This also protects secrets split across arbitrary delta boundaries.
        frame.payload = match frame.payload {
            StreamPayload::Text { class, text, final_record: true, revision } => {
                if revision.is_none_or(|r| r == 0) {
                    return Err(Error::invalid("final native record needs its revision"));
                }
                StreamPayload::Text { class,
                    text: crate::redaction::diagnostic_text(&text)
                        .ok_or_else(|| Error::new("STREAM_REDACTION", "content omitted"))?,
                    final_record: true, revision }
            }
            StreamPayload::Text { .. } => StreamPayload::Unavailable {
                reason: StreamUnavailable::LogicalRecordPending,
            },
            other => other,
        };
        if serde_json::to_vec(&frame)?.len() > MAX_FRAME_BYTES {
            return Err(Error::invalid("redacted logical record exceeds stream bound"));
        }
        // A verified reconnect replaces this ephemeral incarnation; old
        // viewers retain the old epoch and must reopen. Never mix two links.
        if self.rings.get(&frame.scope).is_some_and(|ring| ring.publisher_link != link) {
            self.rings.remove(&frame.scope);
        }
        if !self.rings.contains_key(&frame.scope) && self.rings.len() >= MAX_RINGS {
            let oldest = self.rings.iter().min_by_key(|(_, r)| r.updated_at_ms)
                .map(|(key, _)| key.clone()).expect("nonempty bounded rings");
            self.rings.remove(&oldest);
        }
        let ring = self.rings.entry(frame.scope.clone()).or_insert_with(|| Ring {
            epoch: model::new_id(), publisher_link: link.clone(), records: VecDeque::new(),
            bytes: 0, cursor: 0, source_sequence: 0, source_gap: false,
            lost_through: 0, updated_at_ms: now,
        });
        if ring.publisher_link != link || frame.source_sequence <= ring.source_sequence {
            return Err(stale());
        }
        if ring.records.iter().any(|old| {
            (old.frame.item_id == frame.item_id && old.frame.item_order != frame.item_order)
                || (!old.frame.item_id.starts_with('@') && !frame.item_id.starts_with('@')
                    && old.frame.item_id != frame.item_id && old.frame.item_order == frame.item_order)
        }) {
            return Err(Error::invalid("native item ordering facts changed"));
        }
        let replacement = matches!(&frame.payload, StreamPayload::Text { final_record: true, .. }
            | StreamPayload::CumulativeUsage { .. });
        for old in ring.records.iter().filter(|old| same_part(&old.frame, &frame)) {
            if let StreamPayload::Text { final_record: true, revision: old_revision, .. } = &old.frame.payload {
                match &frame.payload {
                    StreamPayload::Text { revision, .. } if revision > old_revision => {}
                    _ => return Err(stale()),
                }
            }
        }
        if replacement {
            ring.records.retain(|old| !same_part(&old.frame, &frame));
            ring.bytes = ring.records.iter().map(|r| r.bytes).sum();
        }
        ring.source_gap |= ring.source_sequence > 0
            && frame.source_sequence != ring.source_sequence + 1;
        ring.source_sequence = frame.source_sequence;
        ring.cursor = ring.cursor.checked_add(1).ok_or_else(stale)?;
        let mut record = Record { cursor: ring.cursor, frame, bytes: 0 };
        record.bytes = serde_json::to_vec(&record)?.len();
        ring.bytes += record.bytes;
        ring.records.push_back(record);
        while ring.records.len() > MAX_RING_RECORDS || ring.bytes > MAX_RING_BYTES {
            let removed = ring.records.pop_front().expect("bounded ring has an oldest record");
            ring.bytes -= removed.bytes;
            ring.lost_through = removed.cursor;
        }
        ring.updated_at_ms = now;
        Ok(json!({"published":true,"ephemeral":true,"cursor":ring.cursor}))
    }
}

fn same_part(a: &StreamPublish, b: &StreamPublish) -> bool {
    match (&a.payload, &b.payload) {
        (StreamPayload::CumulativeUsage { .. }, StreamPayload::CumulativeUsage { .. }) => true,
        _ => a.item_id == b.item_id && a.part_index == b.part_index,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config, model::Credential,
        platform::{DataRoot, bootstrap_credential}, store::StoreOwner,
    };
    use rusqlite::params;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, ReadHalf, WriteHalf},
        sync::watch, task::JoinHandle,
    };

    struct Peer {
        input: BufReader<ReadHalf<DuplexStream>>,
        output: WriteHalf<DuplexStream>,
        stop: watch::Sender<bool>,
        task: JoinHandle<Result<()>>,
    }

    impl Peer {
        async fn connect(store: &Store, credential: Credential, config: &Config) -> Self {
            let (server, client) = tokio::io::duplex(128 * 1024);
            let (stop, stopping) = watch::channel(false);
            let task = tokio::spawn(crate::ipc::serve(
                Box::new(server), store.clone(), Arc::new(config.ipc.clone()), stopping,
            ));
            let (input, output) = tokio::io::split(client);
            let mut peer = Self { input: BufReader::new(input), output, stop, task };
            let hello = peer.rpc("client.hello", serde_json::to_value(credential).unwrap()).await;
            assert!(hello.get("result").is_some(), "{hello}");
            peer
        }
        async fn rpc(&mut self, method: &str, params: Value) -> Value {
            let request = json!({"jsonrpc":"2.0","id":model::new_id(),"method":method,"params":params});
            self.output.write_all(format!("{request}\n").as_bytes()).await.unwrap();
            self.output.flush().await.unwrap();
            let mut line = String::new();
            assert!(self.input.read_line(&mut line).await.unwrap() > 0);
            serde_json::from_str(&line).unwrap()
        }
        async fn close(self) {
            self.stop.send(true).unwrap();
            drop(self.output);
            self.task.await.unwrap().unwrap();
        }
    }

    fn scope() -> StreamScope {
        StreamScope { binding_id:"stream-binding".into(), generation:1, boot_id:"stream-boot".into(),
            task_id:"stream-task".into(), task_revision:1, attempt_id:"stream-attempt".into(),
            operation_id:"stream-dispatch".into(),
            native_session_id:"stream-root".into(), turn_id:"stream-turn".into() }
    }
    fn frame(sequence: u64, item: &str, order: u32, part: u32, payload: StreamPayload) -> StreamPublish {
        StreamPublish { scope:scope(), source_sequence:sequence,
            native_cursor:format!("opaque:{sequence}"), item_id:item.into(),
            item_order:order, part_index:part, payload }
    }
    async fn counts(store: &Store) -> (i64, i64, i64) {
        store.run_fixture(|db| Ok((
            db.query_row("SELECT COUNT(*) FROM observations", [], |r| r.get(0))?,
            db.query_row("SELECT COUNT(*) FROM operations", [], |r| r.get(0))?,
            db.query_row("SELECT COUNT(*) FROM operations WHERE state IN ('queued','sending')", [], |r| r.get(0))?,
        ))).await.unwrap()
    }
    async fn publish(module: &mut Peer, frame: StreamPublish) {
        let reply = module.rpc(PUBLISH_METHOD, serde_json::to_value(frame).unwrap()).await;
        assert_eq!(reply["result"]["ephemeral"], true, "{reply}");
    }

    #[tokio::test]
    async fn real_store_ipc_stream_content_rights_replacement_gap_revocation_and_no_observations() {
        swarm_contracts::mcp_catalog::validate_registry_metadata().unwrap();
        for method in ["stream.open", "stream.read", "stream.close"] {
            assert!(swarm_contracts::mcp_catalog::find_tool_spec(method).unwrap().0);
        }
        assert!(swarm_contracts::mcp_catalog::find_tool_spec(PUBLISH_METHOD).is_none());
        let directory = std::env::temp_dir().join(format!("swarm-native-stream-{}", model::new_id()));
        std::fs::create_dir_all(&directory).unwrap();
        let root = DataRoot::acquire(&directory).unwrap();
        let operator = bootstrap_credential(&root.path).unwrap();
        let mut config = Config::default();
        config.storage.data_dir = root.path.clone();
        let owner = StoreOwner::start(root, Arc::new(config.clone()), operator).await.unwrap();
        let store = owner.store.clone();
        // Seed retained synthetic native facts through the real Store writer.
        // No provider, model, module process or worker is started.
        store.run_fixture(|db| {
            for (id, role) in [("stream-reader","manager"), ("stream-unrelated","manager"),
                ("stream-exact-caller","manager"),
                ("stream-observer","observer"), ("stream-module","module"),
                ("stream-reviewer","participant")] {
                let mut registration = json!({"role":role,"disabled":false,
                    "token_hash":model::digest(format!("{id}-token").as_bytes())});
                if role == "module" {
                    registration["binding_id"] = json!("stream-binding");
                    registration["binding_generation"] = json!(1);
                }
                if id == "stream-reviewer" {
                    registration["participation_basis"] = json!({"kind":"sponsored_reviewer"});
                }
                super::super::set_meta(db, &format!("client:{id}"), &registration)?;
            }
            db.execute("INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES('stream-task','stream-project',1,'open','{}',1,1)", [])?;
            db.execute("INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) VALUES('stream-binding',1,'stream-lane','stream-instance','stream-artifact','ready','stream-scope','stream-root','{\"module_artifact_id\":\"stream-artifact\",\"runtime\":\"module\"}',?1,1)",
                [json!({"module_client_id":"stream-module","bridge_boot_id":"stream-boot","connection":"connected"}).to_string()])?;
            db.execute("INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,created_at_ms,updated_at_ms) VALUES('stream-attempt','stream-task',1,'{\"revision\":1}','stream-reader','controller','stream-binding',1,'running',1,1)", [])?;
            db.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES('stream-dispatch','stream-exact-caller','stream-request','task.dispatch','{}','{}','stream-task','stream-attempt','stream-binding',1,'settled',?1,?2,1,1,1,1)",
                params![json!({"session_id":"stream-root","turn_id":"stream-turn"}).to_string(),
                    json!({"outcome":"applied","details":{"artifact_refs":[]}}).to_string()])?;
            db.execute("INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,released_at_ms,created_at_ms,updated_at_ms) VALUES('stream-old-attempt','stream-task',1,'{\"revision\":1}','stream-exact-caller','controller','stream-binding',1,'superseded',1,1,1)", [])?;
            db.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES('stream-old-dispatch','stream-exact-caller','stream-old-request','task.dispatch','{}','{}','stream-task','stream-old-attempt','stream-binding',1,'settled','{\"session_id\":\"stream-root\",\"turn_id\":\"old-turn\"}','{\"outcome\":\"applied\",\"details\":{}}',1,1,1,1)", [])?;
            Ok(())
        }).await.unwrap();
        let connect = |id: &str| Credential { client_id:id.into(), token:format!("{id}-token") };
        let mut module = Peer::connect(&store, connect("stream-module"), &config).await;
        // Existing legacy synthetic handshake establishes this actual IPC
        // link. It is not native MCP proof or a versioned adapter claim.
        let hello = module.rpc("module.hello", json!({"boot_id":"stream-boot",
            "module_artifact_id":"stream-artifact","native_ready":true})).await;
        assert!(hello.get("result").is_some(), "{hello}");
        let mut reader = Peer::connect(&store, connect("stream-reader"), &config).await;
        let mut unrelated = Peer::connect(&store, connect("stream-unrelated"), &config).await;
        let mut receipt_reader = Peer::connect(&store, connect("stream-exact-caller"), &config).await;
        let mut observer = Peer::connect(&store, connect("stream-observer"), &config).await;
        let mut reviewer = Peer::connect(&store, connect("stream-reviewer"), &config).await;
        let registration = reader.rpc("coordination.participant.register", json!({
            "client_request_id":model::new_id(),"client_id":"stream-participant",
            "token_hash":model::digest(b"stream-participant-token"),
            "task_id":"stream-task","task_revision":1,"attempt_id":"stream-attempt",
            "binding_id":"stream-binding","binding_generation":1,
            "participation_basis":{"kind":"attempt_owner"},
        })).await;
        assert!(registration.get("result").is_some(), "{registration}");
        let mut participant = Peer::connect(&store, connect("stream-participant"), &config).await;
        let before = counts(&store).await;
        publish(&mut module, frame(1, "assistant-item", 0, 0, StreamPayload::Text {
            class:TextClass::Assistant, text:"secret split-prefix".into(),
            final_record:false, revision:None,
        })).await;
        let selector = json!({"operation_id":"stream-dispatch"});
        for peer in [&mut unrelated, &mut observer, &mut reviewer, &mut module] {
            let denied = peer.rpc("stream.open", selector.clone()).await;
            assert!(denied.get("error").is_some(), "{denied}");
        }
        let participant_view = participant.rpc("stream.open", selector.clone()).await;
        let participant_id = participant_view["result"]["stream_id"].as_str()
            .expect("current exact Participant content branch");
        assert_eq!(participant_view["result"]["content_grant"]["basis"], "current_participant_candidate");
        assert!(participant.rpc("stream.read", json!({"stream_id":participant_id,"after":0})).await.get("result").is_some());
        assert_eq!(participant.rpc("stream.close", json!({"stream_id":participant_id})).await["result"]["closed"], true);
        let receipt = receipt_reader.rpc("operation.get", json!({"operation_id":"stream-old-dispatch"})).await;
        assert!(receipt.get("result").is_some(), "old exact caller retains a Receipt: {receipt}");
        let receipt_denied = receipt_reader.rpc("stream.open", json!({"operation_id":"stream-old-dispatch"})).await;
        assert!(receipt_denied.get("error").is_some(), "Receipt does not bypass the live Attempt fence");
        let byte_authorized = receipt_reader.rpc("stream.open",
            selector.clone()).await;
        let byte_view = byte_authorized["result"]["stream_id"].as_str()
            .expect("existing exact native-source caller content branch");
        assert_eq!(byte_authorized["result"]["content_grant"]["basis"], "exact_result_operation_caller");
        let anchored_read = receipt_reader.rpc("stream.read",
            json!({"stream_id":byte_view,"after":0})).await;
        assert!(anchored_read.get("result").is_some());
        assert_eq!(receipt_reader.rpc("stream.close", json!({"stream_id":byte_view})).await["result"]["closed"], true);
        let forbidden_publish = reader.rpc(PUBLISH_METHOD, serde_json::to_value(frame(
            2, "fake", 0, 0, StreamPayload::Lifecycle { state:StreamLifecycle::Started },
        )).unwrap()).await;
        assert!(forbidden_publish.get("error").is_some(), "publication is module-only");
        let mut foreign = frame(2, "fake", 0, 0,
            StreamPayload::Lifecycle { state:StreamLifecycle::Started });
        foreign.scope.turn_id = "foreign-turn".into();
        let foreign_turn = module.rpc(PUBLISH_METHOD, serde_json::to_value(foreign).unwrap()).await;
        assert!(foreign_turn.get("error").is_some(), "native turn must match retained Operation");
        let open = reader.rpc("stream.open", selector.clone()).await;
        assert_eq!(open["result"]["content_grant"]["basis"], "current_task_manager");
        let stream_id = open["result"]["stream_id"].as_str().expect("authorized Task reader").to_owned();
        let incomplete = reader.rpc("stream.read", json!({"stream_id":stream_id,"after":0})).await;
        assert_eq!(incomplete["result"]["records"][0]["frame"]["payload"]["reason"], "logical_record_pending");
        assert!(!incomplete.to_string().contains("secret split-prefix"));
        publish(&mut module, frame(2, "assistant-item", 0, 0, StreamPayload::Text {
            class:TextClass::Assistant, text:"final assistant".into(),
            final_record:true, revision:Some(2),
        })).await;
        for (sequence, part) in [(3,1), (4,0)] {
            publish(&mut module, frame(sequence, "reasoning-item", 1, part, StreamPayload::Text {
                class:TextClass::ReasoningSummary, text:format!("summary {part}"),
                final_record:true, revision:Some(2),
            })).await;
        }
        for (sequence, total) in [(5,50), (6,77)] {
            publish(&mut module, frame(sequence, "@usage", 1_000_000, 0,
                StreamPayload::CumulativeUsage { prompt_tokens:40, output_tokens:10, total_tokens:total })).await;
        }
        let page = reader.rpc("stream.read", json!({"stream_id":stream_id,"after":0})).await;
        let records = page["result"]["records"].as_array().unwrap();
        assert_eq!(records.len(), 4, "final replaces delta; cumulative usage replaces earlier usage");
        assert_eq!(records[0]["frame"]["payload"]["text"], "final assistant");
        assert_eq!(records[1]["frame"]["part_index"], 0);
        assert_eq!(records[2]["frame"]["part_index"], 1);
        assert_eq!(records[3]["frame"]["payload"]["total_tokens"], 77);
        assert!(serde_json::to_vec(&page).unwrap().len() <= MAX_READ_BYTES);
        // Changing the Task's current content relation revokes an already
        // open view even though its credential and handle remain valid.
        store.run_fixture(|db| {
            db.execute("UPDATE attempts SET owner_id='stream-unrelated' WHERE attempt_id='stream-attempt'", [])?;
            Ok(())
        }).await.unwrap();
        let scope_revoked = reader.rpc("stream.read", json!({"stream_id":stream_id,"after":0})).await;
        assert!(scope_revoked.get("error").is_some(), "current Task byte rights must be re-resolved");
        store.run_fixture(|db| {
            db.execute("UPDATE attempts SET owner_id='stream-reader' WHERE attempt_id='stream-attempt'", [])?;
            Ok(())
        }).await.unwrap();
        let stolen = unrelated.rpc("stream.read", json!({"stream_id":stream_id,"after":0})).await;
        assert!(stolen.get("error").is_some());
        let extra = reader.rpc("stream.open", selector).await;
        let extra_id = extra["result"]["stream_id"].as_str().unwrap();
        let closed = reader.rpc("stream.close", json!({"stream_id":extra_id})).await;
        assert_eq!(closed["result"]["closed"], true);
        for offset in 0..MAX_RING_RECORDS + 4 {
            publish(&mut module, frame(7 + offset as u64, &format!("item-{offset}"),
                10 + offset as u32, 0, StreamPayload::Lifecycle { state:StreamLifecycle::InProgress })).await;
        }
        let lost = reader.rpc("stream.read", json!({"stream_id":stream_id,"after":0,"limit":64})).await;
        assert_eq!(lost["result"]["cursor_loss"], true);
        assert!(lost["result"]["lost_through"].as_u64().unwrap() > 0);
        {
            let shared = store.native_streams.lock().unwrap();
            let ring = shared.rings.get(&scope()).unwrap();
            assert!(ring.records.len() <= MAX_RING_RECORDS);
            assert!(ring.bytes <= MAX_RING_BYTES);
            assert_eq!(shared.rings.len(), 1, "viewers do not copy transcript/ring");
        }
        store.run_fixture(|db| {
            let mut registration = meta(db, "client:stream-reader")?.unwrap();
            registration["disabled"] = json!(true);
            super::super::set_meta(db, "client:stream-reader", &registration)?;
            Ok(())
        }).await.unwrap();
        let revoked = reader.rpc("stream.read", json!({"stream_id":stream_id,"after":0})).await;
        assert!(revoked.get("error").is_some(), "connected reader must revalidate current rights");
        assert_eq!(counts(&store).await, before,
            "publish/open/read add zero Observations and zero native command Operations");
        store.run_fixture(|db| {
            let mut registration = meta(db, "client:stream-reader")?.unwrap();
            registration["disabled"] = json!(false);
            super::super::set_meta(db, "client:stream-reader", &registration)?;
            db.execute("UPDATE bindings SET state_json=json_set(state_json,'$.bridge_boot_id','replacement-boot') WHERE binding_id='stream-binding' AND generation=1", [])?;
            Ok(())
        }).await.unwrap();
        let stale_view = reader.rpc("stream.read", json!({"stream_id":stream_id,"after":0})).await;
        assert_eq!(stale_view["error"]["data"]["code"], "STREAM_STALE");
        let stale_publication = module.rpc(PUBLISH_METHOD,
            serde_json::to_value(frame(1000, "old-item", 1000, 0,
                StreamPayload::Lifecycle { state:StreamLifecycle::Completed })).unwrap()).await;
        assert!(stale_publication.get("error").is_some(), "old boot cannot publish");
        // Closing another view has no native effect either.
        let closed = observer.rpc("stream.close", json!({"stream_id":stream_id})).await;
        assert!(closed.get("error").is_some());
        reader.close().await;
        unrelated.close().await;
        receipt_reader.close().await;
        observer.close().await;
        reviewer.close().await;
        participant.close().await;
        module.close().await;
        drop(store);
        owner.close().await.unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rings_bound_bytes_count_sources_and_reconnect_incarnations() {
        let mut streams = NativeStreams::default();
        for sequence in 1..=100_u64 {
            streams.publish(frame(sequence, &format!("large-{sequence}"), sequence as u32, 0,
                StreamPayload::Text { class:TextClass::Assistant, text:"a".repeat(20_000),
                    final_record:true, revision:Some(1) }), "link".into(), sequence as i64).unwrap();
        }
        let ring = streams.rings.get(&scope()).unwrap();
        assert!(ring.bytes <= MAX_RING_BYTES);
        assert!(ring.records.len() < MAX_RING_RECORDS, "byte bound must evict before count");
        assert!(ring.lost_through > 0);
        let old_epoch = ring.epoch.clone();
        streams.publish(frame(1, "replacement", 0, 0,
            StreamPayload::Lifecycle { state:StreamLifecycle::Started }), "new-link".into(), 200).unwrap();
        assert_ne!(streams.rings[&scope()].epoch, old_epoch);
        streams.publish(frame(3, "replacement", 0, 1,
            StreamPayload::Lifecycle { state:StreamLifecycle::Completed }), "new-link".into(), 201).unwrap();
        assert!(streams.rings[&scope()].source_gap);
        for index in 0..MAX_RINGS + 5 {
            let mut next = frame(1, "other", 0, 0,
                StreamPayload::Lifecycle { state:StreamLifecycle::Started });
            next.scope.operation_id = format!("other-operation-{index}");
            streams.publish(next, "link".into(), 300 + index as i64).unwrap();
        }
        assert_eq!(streams.rings.len(), MAX_RINGS);
    }
}
