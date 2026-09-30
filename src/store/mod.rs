//! A single database owner. The async facade never holds a SQLite connection.
mod operations;
mod producers;
mod runtime;
mod tasks;
use crate::{
    config::Config,
    error::{Error, Result},
    model::{self, Credential, Principal, Role},
    platform::DataRoot,
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{fs::File, path::Path, sync::Arc, thread::JoinHandle};
use tokio::sync::{mpsc, oneshot, watch};

const SCHEMA: &str = include_str!("../../migrations/001_core.sql");
const APPLICATION_ID: i64 = 0x45534331;
type Job = Box<dyn FnOnce(&mut Connection) + Send>;
#[derive(Clone)]
pub struct Store {
    tx: mpsc::Sender<Job>,
    config: Arc<Config>,
    changed: watch::Sender<u64>,
}
pub struct StoreOwner {
    thread: JoinHandle<()>,
    pub store: Store,
}

impl StoreOwner {
    pub async fn start(
        root: DataRoot,
        config: Arc<Config>,
        credential: Credential,
    ) -> Result<Self> {
        let (tx, mut rx) = mpsc::channel::<Job>(config.storage.queue_capacity);
        let (ready_tx, ready_rx) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("swarm-store".into())
            .spawn(move || {
                let _lock: File = root.lock;
                match open_database(&root.path, &credential) {
                    Ok(mut db) => {
                        if ready_tx.send(Ok(())).is_ok() {
                            while let Some(job) = rx.blocking_recv() {
                                job(&mut db);
                            }
                        }
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                }
            })?;
        ready_rx
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "initialization thread ended"))??;
        Ok(Self {
            thread,
            store: Store {
                tx,
                config,
                changed: watch::channel(0).0,
            },
        })
    }
    pub async fn close(self) -> Result<()> {
        drop(self.store);
        tokio::task::spawn_blocking(move || self.thread.join())
            .await
            .map_err(|e| Error::new("STORE_CLOSED", e.to_string()))?
            .map_err(|_| Error::new("STORE_PANIC", "database owner panicked"))
    }
}
impl Store {
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Box::new(move |db| {
                let _ = tx.send(f(db));
            }))
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "database owner stopped"))?;
        rx.await
            .map_err(|_| Error::new("STORE_CLOSED", "database operation lost its response"))?
    }
    pub async fn authenticate(&self, credential: Credential) -> Result<Principal> {
        self.run(move |db| {
            let value = meta(db, &format!("client:{}", credential.client_id))?
                .ok_or_else(|| Error::new("UNAUTHORIZED", "unknown client or credential"))?;
            let expected = value["token_hash"].as_str().unwrap_or("");
            if expected != model::digest(credential.token.as_bytes()) || value["disabled"] == true {
                return Err(Error::new("UNAUTHORIZED", "unknown client or credential"));
            }
            Ok(Principal {
                link_id: model::new_id(),
                client_id: credential.client_id,
                role: serde_json::from_value(value["role"].clone())?,
            })
        })
        .await
    }
    pub async fn call(&self, principal: Principal, method: String, params: Value) -> Result<Value> {
        if method == "module.next" {
            model::fields(&params, &[])?;
            let mut changed = self.changed.subscribe();
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
            loop {
                let p = principal.clone();
                let result = self.run(move |db| runtime::next(db, &p)).await?;
                if !result["command"].is_null() || result.get("rejected_operation_id").is_some() {
                    return Ok(result);
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => return Ok(result),
                    r = changed.changed() => { if r.is_err() {return Err(Error::new("STORE_CLOSED","host stopped"));} }
                }
            }
        }
        let wake_dispatch = matches!(
            method.as_str(),
            "agent.open"
                | "task.dispatch"
                | "agent.send"
                | "agent.reply"
                | "agent.configure"
                | "agent.goal"
                | "agent.refresh"
                | "agent.reconcile"
                | "host.mode"
                | "module.outcome"
        );
        let config = self.config.clone();
        let result = self
            .run(move |db| {
                let current = meta(db, &format!("client:{}", principal.client_id))?
                    .ok_or_else(|| Error::new("UNAUTHORIZED", "client no longer registered"))?;
                if current["disabled"] == true {
                    return Err(Error::new("UNAUTHORIZED", "client disabled"));
                }
                let principal = Principal {
                    link_id: principal.link_id,
                    client_id: principal.client_id,
                    role: serde_json::from_value(current["role"].clone())?,
                };
                if principal.role == Role::Module {
                    return match method.as_str() {
                        "module.hello" => runtime::hello(db, &principal, &params),
                        "module.outcome" => runtime::outcome(db, &principal, &params),
                        "module.observe" => runtime::observe(db, &principal, &params),
                        _ => Err(Error::new(
                            "FORBIDDEN",
                            "module credentials serve only their native binding",
                        )),
                    };
                }
                if is_read(&method) {
                    return read(db, &principal, &method, &params, &config);
                }
                principal.require_writer()?;
                mutate(db, &principal, &method, &params, &config)
            })
            .await;
        if result.is_ok() && wake_dispatch {
            self.changed.send_modify(|n| *n = n.wrapping_add(1));
        }
        result
    }
    pub async fn disconnected(&self, principal: Principal) {
        let _ = self
            .run(move |db| runtime::disconnected(db, &principal))
            .await;
    }
}
fn open_database(root: &Path, credential: &Credential) -> Result<Connection> {
    if rusqlite::version_number() < 3_051_003 {
        return Err(Error::new(
            "SQLITE_VERSION",
            "bundled SQLite >= 3.51.3 is required",
        ));
    }
    let mut db = Connection::open(root.join("swarm.db"))?;
    let version: i64 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
    let app: i64 = db.pragma_query_value(None, "application_id", |r| r.get(0))?;
    let schema_hash = model::digest(SCHEMA.as_bytes());
    let empty: bool = db.query_row(
        "SELECT COUNT(*) = 0 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
        [],
        |r| r.get(0),
    )?;
    if !(empty && version == 0 && app == 0) && (app != APPLICATION_ID || version != 1) {
        return Err(Error::new(
            "SCHEMA_MISMATCH",
            "not this prototype's version-1 database; no automatic overwrite or downgrade",
        ));
    }
    db.pragma_update(None, "foreign_keys", "ON")?;
    db.pragma_update(None, "journal_mode", "WAL")?;
    db.pragma_update(None, "synchronous", "FULL")?;
    db.busy_timeout(std::time::Duration::from_secs(5))?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if empty {
        tx.execute_batch(SCHEMA)?;
        tx.pragma_update(None, "application_id", APPLICATION_ID)?;
        tx.pragma_update(None, "user_version", 1)?;
        set_meta(&tx, "schema_digest", &json!(schema_hash))?;
        set_meta(&tx, "controller_id", &json!(model::new_id()))?;
        set_meta(&tx, "host_epoch", &json!(0))?;
        set_meta(&tx, "execution_mode", &json!({"new_work":"enabled"}))?;
        set_meta(
            &tx,
            &format!("client:{}", credential.client_id),
            &json!({"role":"operator","token_hash":model::digest(credential.token.as_bytes()),"disabled":false}),
        )?;
    } else {
        if meta(&tx, "schema_digest")? != Some(json!(schema_hash)) {
            return Err(Error::new(
                "SCHEMA_MISMATCH",
                "migration content differs; refusing to open a draft/reference database",
            ));
        }
        let record = meta(&tx, &format!("client:{}", credential.client_id))?.ok_or_else(|| {
            Error::new(
                "UNAUTHORIZED",
                "operator credential does not match database",
            )
        })?;
        if record["role"] != "operator"
            || record["token_hash"] != model::digest(credential.token.as_bytes())
            || record["disabled"] == true
        {
            return Err(Error::new(
                "UNAUTHORIZED",
                "operator credential does not match database",
            ));
        }
    }
    let epoch = meta(&tx, "host_epoch")?
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::new("EPOCH_OVERFLOW", "host epoch exhausted"))?;
    set_meta(&tx, "host_epoch", &json!(epoch))?;
    tx.execute("UPDATE operations SET state='outcome_unknown', updated_at_ms=?1 WHERE state IN ('sending','native_accepted')", [model::now_ms()?])?;
    tx.execute(
        "UPDATE bindings SET state='reconciling',state_json=json_set(state_json,'$.connection','disconnected') WHERE state='ready' AND released_at_ms IS NULL",
        [],
    )?;
    tx.commit()?;
    let fk: i64 = db.pragma_query_value(None, "foreign_keys", |r| r.get(0))?;
    let mode: String = db.pragma_query_value(None, "journal_mode", |r| r.get(0))?;
    let sync: i64 = db.pragma_query_value(None, "synchronous", |r| r.get(0))?;
    if fk != 1 || mode != "wal" || sync != 2 {
        return Err(Error::new(
            "STORE_CONFIGURATION",
            "foreign_keys/WAL/FULL were not applied",
        ));
    }
    Ok(db)
}
fn meta(db: &Connection, key: &str) -> Result<Option<Value>> {
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |r| {
            r.get(0)
        })
        .optional()?;
    raw.map(|s| serde_json::from_str(&s).map_err(Into::into))
        .transpose()
}
fn set_meta(db: &Connection, key: &str, value: &Value) -> Result<()> {
    db.execute("INSERT INTO meta(key,value_json) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json", params![key,model::canonical(value)?])?;
    Ok(())
}
fn is_read(method: &str) -> bool {
    matches!(
        method,
        "host.status"
            | "task.get"
            | "task.list"
            | "attempt.get"
            | "operation.get"
            | "operation.list"
            | "agent.family"
            | "agent.state"
            | "agent.list"
            | "route.list"
            | "report.delta"
            | "message.read"
            | "client.list"
    )
}
fn page(params: &Value) -> Result<(i64, i64)> {
    let integer = |name, default| -> Result<i64> {
        match params.get(name) {
            None => Ok(default),
            Some(value) => value
                .as_i64()
                .ok_or_else(|| Error::invalid(format!("{name} must be an integer"))),
        }
    };
    let limit = integer("limit", 50)?;
    let after = integer("after", 0)?;
    if !(1..=200).contains(&limit) || after < 0 {
        return Err(Error::invalid("limit must be 1..200 and after nonnegative"));
    }
    Ok((limit, after))
}
fn read(db: &Connection, p: &Principal, method: &str, v: &Value, config: &Config) -> Result<Value> {
    match method {
        "host.status" => {
            model::fields(v, &[])?;
            let tasks: i64 = db.query_row("SELECT count(*) FROM tasks", [], |r| r.get(0))?;
            let owners: i64 = db.query_row(
                "SELECT count(*) FROM attempts WHERE released_at_ms IS NULL",
                [],
                |r| r.get(0),
            )?;
            let queued: i64 = db.query_row(
                "SELECT count(*) FROM operations WHERE state='queued'",
                [],
                |r| r.get(0),
            )?;
            Ok(
                json!({"version":env!("CARGO_PKG_VERSION"),"controller_id":meta(db,"controller_id")?,"host_epoch":meta(db,"host_epoch")?,"sqlite":rusqlite::version(),"tasks":tasks,"unreleased_attempts":owners,"queued_operations":queued,"execution_mode":meta(db,"execution_mode")?,"native_modules_connected":db.query_row("SELECT count(*) FROM bindings WHERE released_at_ms IS NULL AND json_extract(state_json, '$.connection')='connected'",[],|r|r.get::<_,i64>(0))?,"native_execution":"external_module_protocol"}),
            )
        }
        "agent.family" => producers::family(db, v),
        "task.get" => {
            model::fields(v, &["task_id"])?;
            tasks::get_task(db, model::text(v, "task_id")?)
        }
        "attempt.get" => {
            model::fields(v, &["attempt_id"])?;
            tasks::get_attempt(db, model::text(v, "attempt_id")?)
        }
        "task.list" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            let mut s = db.prepare(
                "SELECT task_id FROM tasks ORDER BY created_at_ms,task_id LIMIT ?1 OFFSET ?2",
            )?;
            let ids = s
                .query_map(params![limit, after], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let values = ids
                .iter()
                .map(|id| tasks::get_task(db, id))
                .collect::<Result<Vec<_>>>()?;
            Ok(
                json!({"items":values,"next_after":after+ids.len() as i64,"pagination":"offset_snapshot_not_inventory_proof"}),
            )
        }
        "operation.get" => {
            model::fields(v, &["operation_id"])?;
            operations::get_operation(db, model::text(v, "operation_id")?)
        }
        "agent.state" => {
            model::fields(v, &["binding_id", "generation"])?;
            operations::get_binding(
                db,
                model::text(v, "binding_id")?,
                model::positive(v, "generation")?,
            )
        }
        "route.list" => {
            model::fields(v, &[])?;
            Ok(json!({"routes":config.routes,"live_qualification":false}))
        }
        "client.list" => {
            p.require_operator()?;
            model::fields(v, &[])?;
            let mut s = db.prepare(
                "SELECT key,value_json FROM meta WHERE key LIKE 'client:%' ORDER BY key",
            )?;
            let items = s
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut result = Vec::new();
            for (key, raw) in items {
                let v: Value = serde_json::from_str(&raw)?;
                result
                    .push(json!({"client_id":&key[7..],"role":v["role"],"disabled":v["disabled"]}));
            }
            Ok(json!({"items":result}))
        }
        "agent.list" => {
            model::fields(v, &["limit", "after"])?;
            let (limit, after) = page(v)?;
            let mut s=db.prepare("SELECT binding_id,generation FROM bindings ORDER BY created_at_ms,binding_id,generation LIMIT ?1 OFFSET ?2")?;
            let ids = s
                .query_map(params![limit, after], |r| {
                    Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items = ids
                .iter()
                .map(|(id, g)| operations::get_binding(db, id, *g))
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"items":items,"next_after":after+ids.len() as i64}))
        }
        "operation.list" => {
            model::fields(v, &["after", "limit", "state"])?;
            let (limit, after) = page(v)?;
            let state = v.get("state").and_then(Value::as_str);
            let mut s=db.prepare("SELECT operation_id FROM operations WHERE (?1 IS NULL OR state=?1) ORDER BY created_at_ms,operation_id LIMIT ?2 OFFSET ?3")?;
            let ids = s
                .query_map(params![state, limit, after], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items = ids
                .iter()
                .map(|id| operations::get_operation(db, id))
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"items":items,"next_after":after+ids.len() as i64}))
        }
        "report.delta" | "message.read" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            let only_mail = method == "message.read";
            let mut s=db.prepare("SELECT observation_id,kind,payload_json,recorded_at_ms FROM observations WHERE observation_id>?1 AND (?2=0 OR (kind='message.send' AND json_extract(payload_json,'$.recipient')=?3)) ORDER BY observation_id LIMIT ?4")?;
            let rows = s
                .query_map(params![after, only_mail, p.client_id, limit], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut next = after;
            let mut items = Vec::new();
            for (id, kind, raw, time) in rows {
                next = id;
                items.push(json!({"cursor":id,"kind":kind,"payload":serde_json::from_str::<Value>(&raw)?,"recorded_at_ms":time}));
            }
            Ok(json!({"items":items,"next_cursor":next}))
        }
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

fn mutate(
    db: &mut Connection,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
) -> Result<Value> {
    model::validate_mutation(method, v)?;
    let request_id = model::text(v, "client_request_id")?;
    let original = model::canonical(v)?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let old:Option<(String,String,String)> = tx.query_row("SELECT method,original_request_json,effective_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2", params![p.client_id,request_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    if let Some((old_method, body, effective)) = old {
        if old_method != method || body != original {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "request ID was used with a different method or payload",
            ));
        }
        let receipt: Value = serde_json::from_str(&effective)?;
        return receipt_result(&receipt["receipt"]);
    }
    let id = model::new_id();
    let now = model::now_ms()?;
    tx.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,'{}','queued',?6,?6,?6)",params![id,p.client_id,request_id,method,original,now])?;
    tx.execute_batch("SAVEPOINT mutation_effect")?;
    let result = apply(&tx, p, method, v, config, &id, now);
    let receipt = match &result {
        Ok((value, queued)) => {
            tx.execute_batch("RELEASE mutation_effect")?;
            let state = if *queued { "queued" } else { "settled" };
            tx.execute("UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1",params![id,state,model::canonical(value)?,if *queued{None}else{Some(now)},now])?;
            tx.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller',?1,?1,?2,?3,?4)",params![id,method,model::canonical(value)?,now])?;
            json!({"ok":true,"value":value})
        }
        Err(error) => {
            tx.execute_batch("ROLLBACK TO mutation_effect; RELEASE mutation_effect")?;
            tx.execute("UPDATE operations SET state='rejected',result_json=?2,settled_at_ms=?3 WHERE operation_id=?1",params![id,model::canonical(&json!(error))?,now])?;
            json!({"ok":false,"error":error})
        }
    };
    tx.execute("UPDATE operations SET effective_request_json=json_set(effective_request_json,'$.receipt',json(?2)) WHERE operation_id=?1",params![id,model::canonical(&receipt)?])?;
    tx.commit()?;
    result.map(|(v, _)| v)
}
fn receipt_result(value: &Value) -> Result<Value> {
    if value["ok"] == true {
        Ok(value["value"].clone())
    } else {
        Err(Error::new(
            value["error"]["code"].as_str().unwrap_or("INVALID_RECEIPT"),
            value["error"]["message"]
                .as_str()
                .unwrap_or("stored request has no valid receipt"),
        ))
    }
}
fn apply(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    match method {
        "task.create" => tasks::create(tx, p, v, id, now).map(|v| (v, false)),
        "task.revise" => tasks::revise(tx, p, v, id, now).map(|v| (v, false)),
        "task.claim" => tasks::claim(tx, p, v, id, now).map(|v| (v, false)),
        "attempt.bind_producer" => producers::bind(tx, p, v, id, now).map(|v| (v, false)),
        "attempt.release" => tasks::release(tx, p, v, id, now).map(|v| (v, false)),
        "task.dispatch" => operations::dispatch(tx, p, v, id, now),
        "agent.send" | "agent.reply" | "agent.configure" | "agent.goal" | "agent.refresh"
        | "agent.reconcile" => runtime::user_command(tx, p, method, v, id).map(|v| (v, true)),
        "agent.open" => operations::open(tx, p, v, config, id, now).map(|v| (v, true)),
        "operation.cancel" => operations::cancel(tx, p, v, id, now).map(|v| (v, false)),
        "host.mode" => {
            p.require_operator()?;
            model::fields(v, &["client_request_id", "new_work"])?;
            let mode = model::text(v, "new_work")?;
            if !["enabled", "disabled"].contains(&mode) {
                return Err(Error::invalid("new_work must be enabled or disabled"));
            }
            set_meta(tx, "execution_mode", &json!({"new_work":mode}))?;
            Ok((
                json!({"operation_id":id,"new_work":mode,"running_work_cancelled":false}),
                false,
            ))
        }
        "client.register" => {
            p.require_operator()?;
            model::fields(
                v,
                &[
                    "client_request_id",
                    "client_id",
                    "role",
                    "token_hash",
                    "binding_id",
                    "binding_generation",
                ],
            )?;
            let client = model::text(v, "client_id")?;
            let role: Role = serde_json::from_value(v["role"].clone())?;
            let hash = model::text(v, "token_hash")?;
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(Error::invalid("token_hash must be SHA-256 hex"));
            }
            if meta(tx, &format!("client:{client}"))?.is_some() {
                return Err(Error::conflict(
                    "client already registered; no implicit credential rotation",
                ));
            }
            let scope = if role == Role::Module {
                runtime::register(tx, v, client)?
            } else {
                if v.get("binding_id").is_some() || v.get("binding_generation").is_some() {
                    return Err(Error::invalid(
                        "binding scope only belongs to module credentials",
                    ));
                }
                json!({})
            };
            let mut registration =
                json!({"role":role,"token_hash":hash.to_lowercase(),"disabled":false});
            if let Some(fields) = scope.as_object() {
                for (k, v) in fields {
                    registration[k] = v.clone();
                }
            }
            set_meta(tx, &format!("client:{client}"), &registration)?;
            Ok((
                json!({"operation_id":id,"client_id":client,"role":role}),
                false,
            ))
        }
        "message.send" => {
            model::fields(
                v,
                &["client_request_id", "recipient", "text", "in_reply_to"],
            )?;
            let recipient = model::text(v, "recipient")?;
            let body = model::text(v, "text")?;
            if meta(tx, &format!("client:{recipient}"))?.is_none() {
                return Err(Error::new("NOT_FOUND", "recipient is not registered"));
            }
            if let Some(reply) = v.get("in_reply_to").and_then(Value::as_str) {
                let prior = operations::get_operation(tx, reply)?;
                if prior["method"] != "message.send"
                    || prior["result"]["recipient"] != p.client_id
                    || prior["result"]["sender"] != recipient
                {
                    return Err(Error::invalid(
                        "reply does not match the sender and recipient of that message",
                    ));
                }
            }
            Ok((
                json!({"operation_id":id,"message_id":id,"sender":p.client_id,"recipient":recipient,"text":body,"in_reply_to":v.get("in_reply_to"),"delivery":"durable_mailbox_only"}),
                false,
            ))
        }
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not implemented; no native effect was attempted"),
        )),
    }
}
