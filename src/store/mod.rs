//! A single database owner. The async facade never holds a SQLite connection.
mod acceptance;
mod assembly;
pub(crate) mod capacity;
mod checks;
mod forge;
mod gm;
mod message_batch;
mod opencode;
mod operations;
mod prerequisites;
mod producers;
mod projection;
mod results;
mod runtime;
mod schedules;
mod status_reader;
mod submissions;
mod tasks;
use crate::{
    artifacts::{ArtifactFiles, MAX_PAGE_BYTES, ResultPage},
    config::Config,
    error::{Error, Result},
    model::{self, Credential, Principal, Role},
    platform::DataRoot,
};
use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, named_params, params,
};
use serde_json::{Value, json};
use std::{fs::File, path::Path, sync::Arc, thread::JoinHandle};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};

const SCHEMA: &str = include_str!("../../migrations/001_core.sql");
const APPLICATION_ID: i64 = 0x45534331;
const LOCAL_OPERATOR_CLIENT_ID_KEY: &str = "local_operator_client_id";
type RunJob = Box<dyn FnOnce(&mut Connection) + Send>;
enum Job {
    Run(RunJob),
    MessageSend(message_batch::Request),
}
#[derive(Clone)]
pub struct Store {
    tx: mpsc::Sender<Job>,
    status_reader: status_reader::Sender,
    config: Arc<Config>,
    changed: watch::Sender<u64>,
    artifacts: ArtifactFiles,
    artifact_io: Arc<Semaphore>,
    data_dir: std::path::PathBuf,
}
pub struct StoreOwner {
    thread: JoinHandle<()>,
    status_thread: JoinHandle<()>,
    pub store: Store,
}

impl StoreOwner {
    pub async fn start(
        root: DataRoot,
        config: Arc<Config>,
        credential: Credential,
    ) -> Result<Self> {
        let artifacts = ArtifactFiles::new(&root.path)?;
        let data_dir = root.path.clone();
        let (tx, mut rx) = mpsc::channel::<Job>(config.storage.queue_capacity);
        let (ready_tx, ready_rx) = oneshot::channel();
        let writer_config = config.clone();
        let thread = std::thread::Builder::new()
            .name("swarm-store".into())
            .spawn(move || {
                let _lock: File = root.lock;
                match open_database(&root.path, &credential) {
                    Ok(mut db) => {
                        if ready_tx.send(Ok(())).is_ok() {
                            let mut pending = None;
                            loop {
                                let job = match pending.take() {
                                    Some(job) => Some(job),
                                    None => rx.blocking_recv(),
                                };
                                let Some(job) = job else { break };
                                match job {
                                    Job::Run(job) => job(&mut db),
                                    Job::MessageSend(first) => {
                                        let mut batch =
                                            Vec::with_capacity(message_batch::MAX_BATCH_SIZE);
                                        batch.push(first);
                                        while batch.len() < message_batch::MAX_BATCH_SIZE {
                                            match rx.try_recv() {
                                                Ok(Job::MessageSend(request)) => {
                                                    batch.push(request);
                                                }
                                                Ok(other) => {
                                                    // Preserve the single queue's FIFO order:
                                                    // a non-send job ends this batch and runs next.
                                                    pending = Some(other);
                                                    break;
                                                }
                                                Err(_) => break,
                                            }
                                        }
                                        message_batch::process(&mut db, batch, &writer_config);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                    }
                }
            })?;
        match ready_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                join_store_thread(thread, "database owner").await?;
                return Err(error);
            }
            Err(_) => {
                join_store_thread(thread, "database owner").await?;
                return Err(Error::new("STORE_CLOSED", "initialization thread ended"));
            }
        }
        let (status_reader, status_thread) = match status_reader::start(
            data_dir.join("swarm.db"),
            config.storage.queue_capacity,
            config.clone(),
        )
        .await
        {
            Ok(reader) => reader,
            Err(error) => {
                drop(tx);
                join_store_thread(thread, "database owner").await?;
                return Err(error);
            }
        };
        Ok(Self {
            thread,
            status_thread,
            store: Store {
                tx,
                status_reader,
                config,
                changed: watch::channel(0).0,
                artifacts,
                data_dir,
                artifact_io: Arc::new(Semaphore::new(4)),
            },
        })
    }
    pub async fn close(self) -> Result<()> {
        let StoreOwner {
            thread,
            status_thread,
            store,
        } = self;
        drop(store);
        join_store_threads(status_thread, thread).await
    }
}
async fn join_store_threads(
    status_thread: JoinHandle<()>,
    database_thread: JoinHandle<()>,
) -> Result<()> {
    let status_result = join_store_thread(status_thread, "status reader").await;
    let database_result = join_store_thread(database_thread, "database owner").await;
    status_result?;
    database_result
}
async fn join_store_thread(thread: JoinHandle<()>, name: &'static str) -> Result<()> {
    tokio::task::spawn_blocking(move || thread.join())
        .await
        .map_err(|error| Error::new("STORE_CLOSED", format!("{name} join failed: {error}")))?
        .map_err(|_| Error::new("STORE_PANIC", format!("{name} panicked")))
}
impl Store {
    async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Job::Run(Box::new(move |db| {
                let _ = tx.send(f(db));
            })))
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "database owner stopped"))?;
        rx.await
            .map_err(|_| Error::new("STORE_CLOSED", "database operation lost its response"))?
    }
    async fn message_send(&self, principal: Principal, params: Value) -> Result<Value> {
        let (response, receive) = oneshot::channel();
        self.tx
            .send(Job::MessageSend(message_batch::Request {
                principal,
                params,
                response,
            }))
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "database owner stopped"))?;
        receive
            .await
            .map_err(|_| Error::new("STORE_CLOSED", "database operation lost its response"))?
    }
    pub async fn authenticate(&self, credential: Credential) -> Result<Principal> {
        self.run(move |db| {
            let value = meta(db, &format!("client:{}", credential.client_id))?
                .ok_or_else(|| Error::new("UNAUTHORIZED", "unknown client or credential"))?;
            if value["internal_only"] == true {
                return Err(Error::new(
                    "UNAUTHORIZED",
                    "internal principals have no transport credentials",
                ));
            }
            let expected = value["token_hash"].as_str().unwrap_or("");
            if expected != model::digest(credential.token.as_bytes()) || value["disabled"] == true {
                return Err(Error::new("UNAUTHORIZED", "unknown client or credential"));
            }
            let role: Role = serde_json::from_value(value["role"].clone())?;
            if role == Role::Operator {
                require_local_operator(db, &credential.client_id)?;
            }
            Ok(Principal {
                link_id: model::new_id(),
                client_id: credential.client_id,
                role,
            })
        })
        .await
    }
    pub async fn call(&self, principal: Principal, method: String, params: Value) -> Result<Value> {
        if method == "host.status" {
            return self.status_reader.host_status(principal, params).await;
        }
        if method == "message.send" {
            return self.message_send(principal, params).await;
        }
        if method == "forge.publish_ref" {
            return self.publish_ref(principal, params).await;
        }
        if method == "module.hello" {
            let p = principal.clone();
            let v = params.clone();
            let mut plan = self.run(move |db| runtime::hello_plan(db, &p, &v)).await?;
            let inspect = plan.clone();
            let new_owner = params.get("managed_owner").cloned();
            self.file_io(move |_| {
                if let Some(owner) = new_owner {
                    let token = model::text(&owner, "token")?;
                    if uuid::Uuid::parse_str(token).is_err()
                        || owner["process"]["purpose"] != "module"
                        || crate::platform::process_group::departed_empty(&owner["process"], token)?
                    {
                        return Err(Error::invalid(
                            "managed module owner must identify a live local process group",
                        ));
                    }
                }
                if inspect["changed"] == true && !inspect["owner"].is_null() {
                    crate::runtime::owner::verify_departed(&inspect["owner"])?;
                }
                Ok(())
            })
            .await?;
            plan["departed"] = json!(plan["changed"] == true && !plan["owner"].is_null());
            return self
                .run(move |db| runtime::hello(db, &principal, &params, &plan))
                .await;
        }
        if method == "source.capture" {
            return self.capture_source(principal, params).await;
        }
        if method == "check.run" {
            return self.check_run(principal, params).await;
        }
        if method == "task.accept" {
            return self.accept_task(principal, params).await;
        }
        if method == "task.submit" {
            return self.submit_task(principal, params).await;
        }
        if method == "module.result" {
            return self.persist_result(principal, params).await;
        }
        if method == "artifact.assemble" {
            return self.assemble_artifact(principal, params).await;
        }
        if method == "artifact.read" {
            return self.read_artifact(principal, params).await;
        }
        if method == "doctor.inspect" {
            // Read-only diagnostics over already recorded facts. The database
            // side runs on the DB thread like every read; the filesystem side
            // is metadata-only and is attached here, where the data directory
            // is known. Doctor performs no mutation or repair.
            model::fields(&params, &[])?;
            let config = self.config.clone();
            let mut inspection = self
                .run(move |db| {
                    let p = current_principal(db, principal)?;
                    if p.role == Role::Module {
                        return Err(Error::new(
                            "FORBIDDEN",
                            "module credentials serve only their native binding",
                        ));
                    }
                    crate::doctor::inspect(db, &config)
                })
                .await?;
            crate::doctor::attach_filesystem(&mut inspection, &self.data_dir);
            return Ok(inspection.report);
        }

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
            "check.run"
                | "check.cancel"
                | "operation.cancel"
                | "agent.open"
                | "task.dispatch"
                | "agent.send"
                | "agent.reply"
                | "agent.configure"
                | "agent.goal"
                | "agent.background"
                | "agent.refresh"
                | "agent.reconcile"
                | "agent.result"
                | "agent.recover"
                | "host.mode"
                | "module.outcome"
        );
        let config = self.config.clone();
        let result = self
            .run(move |db| {
                let principal = current_principal(db, principal)?;
                if principal.role == Role::Module {
                    return match method.as_str() {
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
    async fn file_io<T: Send + 'static>(
        &self,
        f: impl FnOnce(ArtifactFiles) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let permit = self
            .artifact_io
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new("ARTIFACT_IO_CLOSED", "artifact writer stopped"))?;
        let files = self.artifacts.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            f(files)
        })
        .await
        .map_err(|e| Error::new("ARTIFACT_IO_ERROR", e.to_string()))?
    }
    async fn persist_result(&self, principal: Principal, params: Value) -> Result<Value> {
        model::fields(&params, &["operation_id", "page"])?;
        let op = model::text(&params, "operation_id")?.to_string();
        let page: ResultPage = serde_json::from_value(params["page"].clone())?;
        let p = principal.clone();
        let mut metadata = self.run(move |db| results::prepare(db, &p, &op)).await?;
        let bytes = page.decode()?;
        if metadata["requested_offset"].as_u64() != Some(page.offset_bytes)
            || page.byte_length > metadata["requested_length"].as_u64().unwrap_or(0)
        {
            return Err(Error::invalid(
                "result page differs from the admitted byte range",
            ));
        }
        if let Value::Object(fields) = page.metadata() {
            for (key, value) in fields {
                metadata[key] = value;
            }
        }
        let record = ArtifactFiles::record(
            model::text(&metadata, "operation_id")?,
            &bytes,
            metadata.clone(),
        );
        let saved = record.clone();
        self.file_io(move |files| files.publish(&saved, &bytes))
            .await?;
        let result = self
            .run(move |db| results::record(db, &principal, &record))
            .await?;
        self.changed.send_modify(|n| *n = n.wrapping_add(1));
        Ok(result)
    }
    async fn submit_task(&self, principal: Principal, params: Value) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                if !matches!(p.role, Role::Operator | Role::Manager) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "submission requires a manager or operator",
                    ));
                }
                mutate(db, &p, "task.submit", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_string();
        let start_id = id.clone();
        let p = principal.clone();
        if let Some((candidate, document)) = self
            .run(move |db| submissions::begin(db, p, &start_id))
            .await?
        {
            let file_id = id.clone();
            let outcome = self
                .file_io(move |files| {
                    files.verify(&candidate)?;
                    let (record, bytes) = ArtifactFiles::submission(&file_id, &document)?;
                    files.publish(&record, &bytes)?;
                    Ok(record)
                })
                .await;
            self.run(move |db| submissions::finish(db, principal, &id, outcome))
                .await?;
        }
        Ok(receipt)
    }
    async fn accept_task(&self, principal: Principal, params: Value) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                gm::require_authority(db, &p)?;
                mutate(db, &p, "task.accept", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_owned();
        let start_id = id.clone();
        let p = principal.clone();
        if let Some(work) = self
            .run(move |db| acceptance::begin(db, p, &start_id))
            .await?
        {
            let outcome = match work {
                Ok(records) => {
                    self.file_io(move |files| {
                        for record in &records {
                            files.verify(record)?;
                        }
                        Ok(())
                    })
                    .await
                }
                Err(error) => Err(error),
            };
            self.run(move |db| acceptance::finish(db, principal, &id, outcome))
                .await?;
        }
        // Like submission, acceptance keeps its original admission receipt. The
        // current decision outcome is available via operation.get/task.acceptance.
        Ok(receipt)
    }
    async fn assemble_artifact(&self, principal: Principal, params: Value) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                if !matches!(p.role, Role::Operator | Role::Manager) {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "assembly requires a manager or operator",
                    ));
                }
                mutate(db, &p, "artifact.assemble", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_string();
        let start_id = id.clone();
        if let Some((request, pages)) = self
            .run(move |db| assembly::begin(db, principal, &start_id))
            .await?
        {
            let file_id = id.clone();
            let outcome = self
                .file_io(move |files| {
                    files.assemble(&file_id, &pages, request.expected_sha256.as_deref())
                })
                .await;
            self.run(move |db| assembly::finish(db, &id, outcome))
                .await?;
        }
        // Always return the same admission receipt, including after reconnect.
        // operation.get provides the current file-processing result.
        Ok(receipt)
    }
    async fn read_artifact(&self, principal: Principal, params: Value) -> Result<Value> {
        model::fields(&params, &["artifact_id", "offset_bytes", "length_bytes"])?;
        let id = model::text(&params, "artifact_id")?.to_string();
        let integer = |name: &str, fallback| -> Result<u64> {
            params.get(name).map_or(Ok(fallback), |v| {
                v.as_u64()
                    .ok_or_else(|| Error::invalid(format!("{name} must be a nonnegative integer")))
            })
        };
        let offset = integer("offset_bytes", 0)?;
        let length = integer("length_bytes", MAX_PAGE_BYTES as u64)?;
        if length == 0 || length > MAX_PAGE_BYTES as u64 {
            return Err(Error::invalid("length_bytes must be 1..65536"));
        }
        let record = self
            .run(move |db| {
                let p = current_principal(db, principal)?;
                if p.role == Role::Module {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "module cannot inspect other results",
                    ));
                }
                results::get(db, &id)
            })
            .await?;
        self.file_io(move |files| files.read(&record, offset, length as usize))
            .await
    }
    pub async fn disconnected(&self, principal: Principal) {
        let _ = self
            .run(move |db| runtime::disconnected(db, &principal))
            .await;
    }
}
fn current_principal(db: &Connection, principal: Principal) -> Result<Principal> {
    if principal.role == Role::Scheduler
        && principal.client_id == model::INTERNAL_SCHEDULER_CLIENT_ID
    {
        return Ok(principal);
    }
    let current = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "client no longer registered"))?;
    if current["disabled"] == true {
        return Err(Error::new("UNAUTHORIZED", "client disabled"));
    }
    let role: Role = serde_json::from_value(current["role"].clone())?;
    if role == Role::Operator {
        require_local_operator(db, &principal.client_id)?;
    }
    Ok(Principal { role, ..principal })
}

fn require_local_operator(db: &Connection, client_id: &str) -> Result<()> {
    let local_operator = meta(db, LOCAL_OPERATOR_CLIENT_ID_KEY)?;
    if local_operator.as_ref().and_then(Value::as_str) != Some(client_id) {
        return Err(Error::new(
            "LOCAL_OPERATOR_MISMATCH",
            "operator identity does not match the bootstrap credential",
        ));
    }
    Ok(())
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
            LOCAL_OPERATOR_CLIENT_ID_KEY,
            &json!(credential.client_id),
        )?;
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
        let local_operator = meta(&tx, LOCAL_OPERATOR_CLIENT_ID_KEY)?;
        if let Some(local_operator) = &local_operator
            && local_operator.as_str() != Some(credential.client_id.as_str())
        {
            return Err(Error::new(
                "LOCAL_OPERATOR_MISMATCH",
                "bootstrap credential does not match the pinned local operator identity",
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
        if local_operator.is_none() {
            set_meta(
                &tx,
                LOCAL_OPERATOR_CLIENT_ID_KEY,
                &json!(credential.client_id),
            )?;
        }
    }
    let scheduler_key = format!("client:{}", model::INTERNAL_SCHEDULER_CLIENT_ID);
    match meta(&tx, &scheduler_key)? {
        None => set_meta(
            &tx,
            &scheduler_key,
            &json!({
                "role": "scheduler", "internal_only": true, "disabled": false
            }),
        )?,
        Some(record) if record["role"] == "scheduler" && record["internal_only"] == true => {}
        Some(_) => {
            return Err(Error::new(
                "INTERNAL_CLIENT_CONFLICT",
                "reserved scheduler identity already has a transport registration",
            ));
        }
    }
    let epoch = meta(&tx, "host_epoch")?
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| Error::new("EPOCH_OVERFLOW", "host epoch exhausted"))?;
    set_meta(&tx, "host_epoch", &json!(epoch))?;
    tx.execute(
        "UPDATE operations SET state='outcome_unknown',result_json=CASE \
         WHEN method='forge.publish_ref' AND state='sending' THEN \
           json_set(COALESCE(result_json,'{}'), \
             '$.outcome','unknown', \
             '$.publication','operator_intervention_required', \
             '$.process_tree_unconfirmed',json('true'), \
             '$.process_tree_status','unconfirmed_after_restart', \
             '$.process_tree_cleanup','host_lifecycle_interrupted_before_confirmation', \
             '$.resolution','manual_operator_intervention_required', \
             '$.reason','process_tree_unconfirmed') \
         ELSE result_json END,updated_at_ms=?1 \
         WHERE state IN ('sending','native_accepted')",
        [model::now_ms()?],
    )?;
    tx.execute(
        "UPDATE bindings SET state='reconciling',state_json=json_set(state_json,'$.connection','disconnected') WHERE state='ready' AND released_at_ms IS NULL",
        [],
    )?;
    tx.execute(
        "UPDATE check_runs SET state='reconciling' WHERE state='running'",
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
        "check.get"
            | "check.profiles"
            | "artifact.get"
            | "artifact.parts"
            | "task.submission"
            | "task.acceptance"
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
            | "report.capacity"
            | "report.attention"
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

// Public Operation/report reads share this visibility rule for directed
// message receipts. The caller and the exact original recipient may read a
// send; cancellation recipients are resolved against the immutable settled
// send identified by both delivery ID and payload digest. Only the verified
// local operator receives the global diagnostic view.
const OPERATION_VISIBILITY_SQL: &str = r#"(
    :operator = 1
    OR op.method NOT IN ('message.send', 'message.cancel')
    OR op.caller_id = :client
    OR (
        op.method = 'message.send'
        AND op.state = 'settled'
        AND json_type(op.result_json, '$.sender') = 'text'
        AND json_extract(op.result_json, '$.sender') = op.caller_id
        AND json_type(op.result_json, '$.recipient') = 'text'
        AND length(json_extract(op.result_json, '$.recipient')) > 0
        AND json_extract(op.result_json, '$.recipient') = :client
    )
    OR (
        op.method = 'message.cancel'
        AND op.state = 'settled'
        AND EXISTS (
            SELECT 1 FROM operations AS original
            WHERE original.method = 'message.send'
              AND original.state = 'settled'
              AND json_extract(original.result_json, '$.delivery_id') =
                  json_extract(op.result_json, '$.cancellation.delivery_id')
              AND json_extract(original.result_json, '$.payload_digest') =
                  json_extract(op.result_json, '$.cancellation.payload_digest')
              AND original.caller_id = op.caller_id
              AND json_type(original.result_json, '$.sender') = 'text'
              AND json_extract(original.result_json, '$.sender') = op.caller_id
              AND json_type(original.result_json, '$.recipient') = 'text'
              AND length(json_extract(original.result_json, '$.recipient')) > 0
              AND json_extract(original.result_json, '$.recipient') = :client
              AND (
                  SELECT count(*) FROM operations AS same_identity
                  WHERE same_identity.method = 'message.send'
                    AND same_identity.state = 'settled'
                    AND json_extract(same_identity.result_json, '$.delivery_id') =
                        json_extract(op.result_json, '$.cancellation.delivery_id')
                    AND json_extract(same_identity.result_json, '$.payload_digest') =
                        json_extract(op.result_json, '$.cancellation.payload_digest')
              ) = 1
        )
    )
)"#;

fn timeline_visibility_sql() -> String {
    format!(
        r#"(
            (
                :mailbox_only = 1
                AND o.kind IN ('message.send', 'task.feedback', 'check.completed')
                AND json_extract(o.payload_json, '$.recipient') = :client
            )
            OR (
                :mailbox_only = 0
                AND (
                    o.kind NOT IN ('message.send', 'message.cancel')
                    OR (:operator = 1 AND o.kind IN ('message.send', 'message.cancel'))
                    OR (
                        o.kind IN ('message.send', 'message.cancel')
                        AND EXISTS (
                            SELECT 1 FROM operations AS op
                            WHERE op.operation_id = o.operation_id
                              AND op.method = o.kind
                              AND {OPERATION_VISIBILITY_SQL}
                        )
                    )
                )
            )
        )"#
    )
}

fn operation_visible_to(db: &Connection, p: &Principal, id: &str) -> Result<bool> {
    let sql = format!(
        "SELECT EXISTS(SELECT 1 FROM operations AS op WHERE op.operation_id=:operation_id AND {OPERATION_VISIBILITY_SQL})"
    );
    Ok(db.query_row(
        &sql,
        named_params! {
            ":operation_id": id,
            ":operator": p.role == Role::Operator,
            ":client": &p.client_id,
        },
        |row| row.get(0),
    )?)
}

fn read(db: &Connection, p: &Principal, method: &str, v: &Value, config: &Config) -> Result<Value> {
    match method {
        "check.get" => checks::describe(db, v),
        "check.profiles" => {
            model::fields(v, &[])?;
            let profiles = config
                .checks
                .profiles
                .iter()
                .map(|profile| {
                    json!({
                        "profile_id":profile.profile_id,
                        "profile_revision":profile.profile_revision,
                        "parser":profile.parser,
                        "resource":profile.resource,
                        "reproducible_opt_in":profile.reproducible,
                        "configured_environment_names":profile.environment.keys().collect::<Vec<_>>(),
                        "inherited_environment_names":profile.inherit_env,
                        "versioned_input_names":profile.versioned_inputs.keys().collect::<Vec<_>>(),
                    })
                })
                .collect::<Vec<_>>();
            Ok(json!({
                "enabled":config.checks.enabled,
                "profiles":profiles,
                "cache_reuse":"conditional",
                "cache_reuse_policy":"per_check_requires_verified_versioned_inputs"
            }))
        }

        "artifact.get" => results::describe(db, p, v),
        "artifact.parts" => assembly::parts(db, v),
        "task.acceptance" => acceptance::describe(db, v),
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
                json!({"version":env!("CARGO_PKG_VERSION"),"controller_id":meta(db,"controller_id")?,"host_epoch":meta(db,"host_epoch")?,"gm":gm::record(db)?,"gm_wake_mode":gm::WAKE_MODE,"sqlite":rusqlite::version(),"tasks":tasks,"unreleased_attempts":owners,"queued_operations":queued,"execution_mode":meta(db,"execution_mode")?,"native_modules_connected":db.query_row("SELECT count(*) FROM bindings WHERE released_at_ms IS NULL AND json_extract(state_json, '$.connection')='connected'",[],|r|r.get::<_,i64>(0))?,"native_execution":"scoped_runtime_protocol","schedules":schedules::status(db,&config.schedules,model::now_ms()?)?}),
            )
        }
        "agent.family" => producers::family(db, v),
        "task.submission" => submissions::describe(db, v),
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
            let id = model::text(v, "operation_id")?;
            if !operation_visible_to(db, p, id)? {
                return Err(Error::new("NOT_FOUND", format!("Operation {id}")));
            }
            operations::get_operation(db, id)
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
            gm::require_authority(db, p)?;
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
            let sql = format!(
                "SELECT op.operation_id FROM operations AS op WHERE (:state IS NULL OR op.state=:state) AND {OPERATION_VISIBILITY_SQL} ORDER BY op.created_at_ms,op.operation_id LIMIT :limit OFFSET :after"
            );
            let mut s = db.prepare(&sql)?;
            let ids = s
                .query_map(
                    named_params! {
                        ":state": state,
                        ":limit": limit,
                        ":after": after,
                        ":operator": p.role == Role::Operator,
                        ":client": &p.client_id,
                    },
                    |r| r.get::<_, String>(0),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let items = ids
                .iter()
                .map(|id| operations::get_operation(db, id))
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"items":items,"next_after":after+ids.len() as i64}))
        }
        "report.capacity" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            capacity::capacity_report(db, limit, after)
        }
        "report.attention" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            capacity::attention_report(db, limit, after)
        }
        "report.delta" | "message.read" => {
            model::fields(v, &["after", "limit"])?;
            let (limit, after) = page(v)?;
            let mailbox_only = method == "message.read";
            // Reports subscriptions read this same scoped cursor, so hidden
            // mail cannot leak through either payloads or pagination flags.
            let visibility = timeline_visibility_sql();
            let mut s = db.prepare(&format!(
                "SELECT o.observation_id,o.kind,o.payload_json,o.recorded_at_ms,o.operation_id FROM observations AS o WHERE o.observation_id>:after AND {visibility} ORDER BY o.observation_id LIMIT :limit"
            ))?;
            let rows = s
                .query_map(
                    named_params! {
                        ":after": after,
                        ":mailbox_only": mailbox_only,
                        ":operator": p.role == Role::Operator,
                        ":client": &p.client_id,
                        ":limit": limit,
                    },
                    |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, i64>(3)?,
                            r.get::<_, Option<String>>(4)?,
                        ))
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            // Projection first, limits after projection (§8.1): an
            // oversized item becomes an explicit gap reference at its
            // own cursor; the cursor below advances only over entries
            // actually returned, so a limited page never replays or
            // silently skips a source row.
            let mut projected = Vec::with_capacity(rows.len());
            for (id, kind, raw, time, operation_id) in rows {
                projected.push(json!({"cursor":id,"kind":kind,"payload":serde_json::from_str::<Value>(&raw)?,"recorded_at_ms":time,"operation_id":operation_id}));
            }
            let limited = projection::limit_items(projected, projection::timeline_gap_reference)?;
            let next = limited
                .items
                .last()
                .and_then(|item| item["cursor"].as_i64())
                .unwrap_or(after);
            // Newer source rows exist when a budget stopped this page
            // early (fetched rows were left unemitted) or when the
            // source itself continues past the last returned cursor.
            let has_newer: bool = limited.stopped_early
                || db.query_row(
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM observations AS o WHERE o.observation_id>:after AND {visibility})"
                    ),
                    named_params! {
                        ":after": next,
                        ":mailbox_only": mailbox_only,
                        ":operator": p.role == Role::Operator,
                        ":client": &p.client_id,
                    },
                    |r| r.get::<_, bool>(0),
                )?;
            let has_older: bool = db.query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM observations AS o WHERE o.observation_id<=:after AND {visibility})"
                ),
                named_params! {
                    ":after": after,
                    ":mailbox_only": mailbox_only,
                    ":operator": p.role == Role::Operator,
                    ":client": &p.client_id,
                },
                |r| r.get(0),
            )?;
            let frame = projection::frame(
                if mailbox_only {
                    "mailbox"
                } else {
                    "observation_timeline"
                },
                json!({"after": after, "next_cursor": next}),
                &limited,
                limit,
                has_older,
                has_newer,
                limited.gap_count == 0,
                Vec::new(),
            )?;
            Ok(json!({"items":limited.items,"next_cursor":next,"projection":frame}))
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
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let result = mutate_in_transaction(&tx, p, method, v, config, model::now_ms()?)?;
    // Rejected requests retain the same durable receipt as successful requests.
    tx.commit()?;
    result
}

fn mutate_in_transaction(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
) -> Result<Result<Value>> {
    mutate_in_transaction_with_check_plan(tx, p, method, v, config, now, None)
}

fn mutate_in_transaction_with_check_plan(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    now: i64,
    check_plan: Option<&checks::CheckPlanResolution>,
) -> Result<Result<Value>> {
    if p.role == Role::Scheduler
        && (p.client_id != model::INTERNAL_SCHEDULER_CLIENT_ID || method != "check.run")
    {
        return Err(Error::new(
            "FORBIDDEN",
            "scheduler may admit only configured checks",
        ));
    }
    model::validate_mutation(method, v)?;
    let request_id = model::text(v, "client_request_id")?;
    let original = model::canonical(v)?;
    let old:Option<(String,String,String)> = tx.query_row("SELECT method,original_request_json,effective_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2", params![p.client_id,request_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    if let Some((old_method, body, effective)) = old {
        if old_method != method || body != original {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "request ID was used with a different method or payload",
            ));
        }
        let receipt: Value = serde_json::from_str(&effective)?;
        return Ok(receipt_result(&receipt["receipt"]));
    }
    let id = model::new_id();
    tx.execute("INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,'{}','queued',?6,?6,?6)",params![id,p.client_id,request_id,method,original,now])?;
    tx.execute_batch("SAVEPOINT mutation_effect")?;
    let result = apply(
        tx,
        p,
        method,
        v,
        config,
        ApplyContext {
            operation_id: &id,
            now,
            check_plan,
        },
    );
    let receipt = match &result {
        Ok((value, queued)) => {
            tx.execute_batch("RELEASE mutation_effect")?;
            let state = if *queued { "queued" } else { "settled" };
            tx.execute("UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5 WHERE operation_id=?1",params![id,state,model::canonical(value)?,if *queued{None}else{Some(now)},now])?;
            // The admitted operation now holds (or releases) native
            // capacity; record that in the durable ledger (R23).
            capacity::sync_operation(tx, &id, now)?;
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
    Ok(result.map(|(v, _)| v))
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
struct ApplyContext<'a> {
    operation_id: &'a str,
    now: i64,
    check_plan: Option<&'a checks::CheckPlanResolution>,
}

fn apply(
    tx: &Transaction<'_>,
    p: &Principal,
    method: &str,
    v: &Value,
    config: &Config,
    context: ApplyContext<'_>,
) -> Result<(Value, bool)> {
    let ApplyContext {
        operation_id: id,
        now,
        check_plan,
    } = context;
    match method {
        "forge.publish_ref" => forge::reserve(tx, p, v, id, config).map(|v| (v, true)),
        "source.capture" => checks::reserve_source(tx, p, v, id, config).map(|v| (v, true)),
        "check.run" => checks::reserve(tx, p, v, id, config, check_plan),
        "check.cancel" => checks::cancel(tx, p, v, id).map(|v| (v, false)),

        "artifact.assemble" => assembly::reserve(tx, p, v, id).map(|v| (v, true)),
        "task.submit" => submissions::reserve(tx, p, v, id).map(|v| (v, true)),
        "task.accept" => acceptance::reserve(tx, p, v, id).map(|v| {
            let queued = v.get("coalesced") != Some(&Value::Bool(true));
            (v, queued)
        }),
        "task.invalidate_acceptance" => {
            acceptance::invalidate(tx, p, v, id, now).map(|v| (v, false))
        }
        "task.request_changes" => {
            submissions::request_changes(tx, p, v, id, now).map(|v| (v, false))
        }
        "task.create" => tasks::create(tx, p, v, id, now).map(|v| (v, false)),
        "task.revise" => tasks::revise(tx, p, v, id, now).map(|v| (v, false)),
        "task.claim" => tasks::claim(tx, p, v, id, now).map(|v| (v, false)),
        "attempt.bind_producer" => producers::bind(tx, p, v, id, now).map(|v| (v, false)),
        "attempt.release" => tasks::release(tx, p, v, id, now).map(|v| (v, false)),
        "task.dispatch" => operations::dispatch(tx, p, v, id, now),
        "agent.send" | "agent.reply" | "agent.configure" | "agent.goal" | "agent.background"
        | "agent.refresh" | "agent.reconcile" | "agent.result" | "agent.recover" => {
            runtime::user_command(tx, p, method, v, id).map(|v| (v, true))
        }
        "agent.open" => operations::open(tx, p, v, config, id, now).map(|v| (v, true)),
        "operation.cancel" => operations::cancel(tx, p, v, id, now).map(|v| (v, false)),
        "host.mode" => {
            gm::require_authority(tx, p)?;
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
        "gm.handover" => gm::handover(tx, p, v, id).map(|v| (v, false)),
        "client.register" => {
            gm::require_authority(tx, p)?;
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
            if role == Role::Operator {
                return Err(Error::new(
                    "FORBIDDEN",
                    "operator role is reserved for the bootstrap credential",
                ));
            }
            if role == Role::Scheduler || client == model::INTERNAL_SCHEDULER_CLIENT_ID {
                return Err(Error::new(
                    "FORBIDDEN",
                    "scheduler identity is reserved for in-process admission",
                ));
            }
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
                &[
                    "client_request_id",
                    "recipient",
                    "text",
                    "in_reply_to",
                    "in_reply_to_digest",
                    "admission_deadline_ms",
                    "delivery_deadline_ms",
                    "reply_deadline_ms",
                ],
            )?;
            let recipient = model::text(v, "recipient")?;
            let body = model::text(v, "text")?;
            let recipient_registration = meta(tx, &format!("client:{recipient}"))?
                .ok_or_else(|| Error::new("NOT_FOUND", "recipient is not registered"))?;
            let sender_registration = meta(tx, &format!("client:{}", p.client_id))?
                .ok_or_else(|| Error::new("UNAUTHORIZED", "sender is not registered"))?;
            let payload_digest = model::message_payload_digest(&p.client_id, recipient, body)?;
            let mut reply_to = Value::Null;
            if let Some(reply) = v.get("in_reply_to").and_then(Value::as_str) {
                // A reply addresses the original delivery: by its own
                // delivery_id when it carries one, otherwise by the
                // historical operation id.
                let prior = match find_delivery(tx, reply)? {
                    Some(original) => original,
                    None => operations::get_operation(tx, reply)?,
                };
                if !(prior["method"] == "message.send"
                    || (prior["method"] == "task.request_changes"
                        && prior["result"]["applied"] == true)
                    || (prior["method"] == "task.invalidate_acceptance"
                        && prior["result"]["message_id"] == reply))
                    || prior["result"]["recipient"] != p.client_id
                    || prior["result"]["sender"] != recipient
                {
                    return Err(Error::invalid(
                        "reply does not match the sender and recipient of that message",
                    ));
                }
                model::verify_payload_digest_claim(
                    prior["result"]["payload_digest"].as_str(),
                    v.get("in_reply_to_digest").and_then(Value::as_str),
                )?;
                reply_to = model::message_reply_reference(&prior["result"]);
            }
            Ok((
                json!({"operation_id":id,"message_id":id,"delivery_id":model::new_id(),"sender":p.client_id,"recipient":recipient,"source_scope":model::message_scope(&sender_registration,&p.client_id),"target_scope":model::message_scope(&recipient_registration,recipient),"actor":model::message_actor(&sender_registration,&p.client_id),"payload_digest":payload_digest,"admission_deadline_ms":model::deadline(v,"admission_deadline_ms")?,"delivery_deadline_ms":model::deadline(v,"delivery_deadline_ms")?,"reply_deadline_ms":model::deadline(v,"reply_deadline_ms")?,"text":body,"in_reply_to":v.get("in_reply_to"),"reply_to":reply_to,"cancellation":Value::Null,"delivery":"durable_mailbox_only"}),
                false,
            ))
        }
        "message.cancel" => cancel_message(tx, p, v, id).map(|v| (v, false)),
        _ => Err(Error::new(
            "METHOD_NOT_FOUND",
            format!("{method} is not implemented; no native effect was attempted"),
        )),
    }
}

/// Finds a settled mailbox delivery by its own delivery identity (R22).
/// Records written before delivery identity existed have no `delivery_id`
/// and stay addressable only by their historical operation id.
fn find_delivery(db: &Connection, delivery_id: &str) -> Result<Option<Value>> {
    let operation_id: Option<String> = db
        .query_row(
            "SELECT operation_id FROM operations WHERE method='message.send' AND state='settled' AND json_extract(result_json,'$.delivery_id')=?1",
            [delivery_id],
            |r| r.get(0),
        )
        .optional()?;
    match operation_id {
        Some(id) => Ok(Some(operations::get_operation(db, &id)?)),
        None => Ok(None),
    }
}

/// Records the cancellation of one mailbox delivery. A cancellation is its
/// own durable record referencing the original delivery by identity and
/// digest; the original record is evidence and is never rewritten, and no
/// workflow state changes because a delivery was cancelled — this text, like
/// any message text, is not a workflow transition.
fn cancel_message(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    model::fields(
        v,
        &[
            "client_request_id",
            "delivery_id",
            "payload_digest",
            "reason",
        ],
    )?;
    let delivery_id = model::text(v, "delivery_id")?;
    let claimed = model::text(v, "payload_digest")?;
    let original = find_delivery(tx, delivery_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Delivery {delivery_id}")))?;
    if original["result"]["sender"] != p.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "only the original sender can cancel a delivery",
        ));
    }
    model::verify_payload_digest_claim(
        original["result"]["payload_digest"].as_str(),
        Some(claimed),
    )?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM operations WHERE method='message.cancel' AND state='settled' AND json_extract(result_json,'$.cancellation.delivery_id')=?1",
            [delivery_id],
            |r| r.get(0),
        )
        .optional()?;
    if existing.is_some() {
        return Err(Error::conflict("delivery is already cancelled"));
    }
    let sender_registration = meta(tx, &format!("client:{}", p.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "sender is not registered"))?;
    Ok(
        json!({"operation_id":id,"cancellation":{"delivery_id":delivery_id,"payload_digest":claimed},"cancelled_by":model::message_actor(&sender_registration,&p.client_id),"reason":v.get("reason").cloned().unwrap_or(Value::Null),"original_record_changed":false,"delivery":"durable_mailbox_only"}),
    )
}

#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
mod mailbox_tests;
#[cfg(test)]
mod receipt_tests;
#[cfg(test)]
mod security_tests;
