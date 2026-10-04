//! Store-side registry and direct-run admission for trusted-local scripts.
//!
//! Script execution is deliberately a closed consumer: no script receives a
//! Store handle, Manager credential, or controller API capability. Filesystem
//! work runs outside SQLite; the database records the immutable admitted
//! bundle, Task/Attempt, work receipt digest and Operation lifecycle.
use super::{Store, current_principal, gm, meta, results, tasks};
use crate::{
    artifacts::{ArtifactFiles, ArtifactRecord},
    error::{Error, Result},
    model::{self, Principal, Role},
    scripts::{manifest, protocol, registry, runner},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::watch;

const RECONCILE_INTERVAL: Duration = Duration::from_millis(200);
const STARTING_WORKER_TIMEOUT_MS: i64 = 120_000;

#[derive(Debug, Clone)]
struct RevisionSnapshot {
    record: ArtifactRecord,
    interpreter: manifest::InterpreterIdentity,
    validated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct PendingRun {
    run_id: String,
    operation_id: String,
    operation_state: String,
    work_digest: String,
    sent_at_ms: Option<i64>,
}

struct OperationRecord<'a> {
    method: &'a str,
    request_id: &'a str,
    original_request: &'a str,
    operation_id: &'a str,
    result: &'a Value,
    context: Value,
    settled: bool,
    now_ms: i64,
}

struct ScriptRunArtifactLinks {
    run_id: String,
    operation_id: String,
    state: String,
    result_ref: Option<String>,
    stdout_ref: Option<String>,
    stderr_ref: Option<String>,
    method: String,
}

struct RunnerObservation {
    completion: Option<runner::Completion>,
    ready: Option<Value>,
    launch: Option<Value>,
    launch_departed: bool,
    has_go: bool,
}

impl Store {
    pub(super) async fn script_call(
        &self,
        principal: Principal,
        method: String,
        params: Value,
    ) -> Result<Value> {
        match method.as_str() {
            "script.register" => self.register_script(principal, params).await,
            "script.revise" => self.revise_script(principal, params).await,
            "script.validate" => self.validate_script(principal, params).await,
            "script.activate" => self.activate_script(principal, params).await,
            "script.run" => self.run_script(principal, params).await,
            "script.get" => self.get_script(principal, params).await,
            "script.list" => self.list_scripts(principal, params).await,
            _ => Err(Error::new("METHOD_NOT_FOUND", method)),
        }
    }

    async fn register_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::RegisterRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if let Some(value) = self.replay(&principal, "script.register", &params).await? {
            return Ok(value);
        }
        let caller = principal.client_id.clone();
        let bundle_request = request.bundle;
        let bundle = tokio::task::spawn_blocking(move || bundle_request.capture())
            .await
            .map_err(|error| Error::new("SCRIPT_PREPARATION", error.to_string()))??;
        let bundle_bytes = model::canonical(&json!(bundle))?.into_bytes();
        if bundle_bytes.len() > manifest::MAX_BUNDLE_REQUEST_BYTES + manifest::MAX_SCHEMA_BYTES {
            return Err(Error::invalid(
                "captured script bundle exceeds the 932 KiB stored manifest limit",
            ));
        }
        let bundle_sha256 = model::digest(&bundle_bytes);
        let revision = 1_i64;
        let artifact_id = bundle_artifact_id(
            &bundle.script_id,
            revision,
            &caller,
            &request.client_request_id,
            &bundle_sha256,
        )?;
        let metadata = json!({
            "script_id":bundle.script_id,
            "revision":revision,
            "bundle_sha256":bundle_sha256,
            "interpreter_kind":bundle.interpreter.kind,
            "interpreter_sha256":bundle.interpreter.sha256,
        });
        let (record, bytes) = ArtifactFiles::document(
            registry::BUNDLE_KIND,
            &artifact_id,
            &json!(bundle),
            metadata,
        )?;
        let publish_record = record.clone();
        self.file_io(move |files| files.publish(&publish_record, &bytes))
            .await?;
        let request_json = model::canonical(&params)?;
        let owner = principal.clone();
        let caller = principal.client_id.clone();
        let request_id = request.client_request_id;
        let bundle_for_tx = bundle.clone();
        let record_for_tx = record.clone();
        let value = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = require_script_authority(&tx, &owner)?;
                if let Some(receipt) = replay_tx(
                    &tx,
                    &caller,
                    "script.register",
                    &request_id,
                    &request_json,
                )? {
                    tx.commit()?;
                    return Ok(receipt);
                }
                if script_exists(&tx, &bundle_for_tx.script_id)? {
                    return Err(Error::conflict("script ID is already registered"));
                }
                let now = model::now_ms()?;
                registry::register_artifact(&tx, &record_for_tx, now)?;
                tx.execute(
                    "INSERT INTO scripts(script_id,owner_id,active_revision,created_at_ms,updated_at_ms) VALUES(?1,?2,NULL,?3,?3)",
                    params![bundle_for_tx.script_id, current.client_id, now],
                )?;
                insert_revision(&tx, &bundle_for_tx, &record_for_tx, &current.client_id, 1, now)?;
                let operation_id = model::new_id();
                let result = json!({
                    "operation_id":operation_id,
                    "script_id":bundle_for_tx.script_id,
                    "revision":1,
                    "bundle_ref":record_for_tx.artifact_id,
                    "state":"registered",
                    "validated":true,
                    "activation":"not_selected",
                });
                record_operation(
                    &tx,
                    &current,
                    OperationRecord {
                        method: "script.register",
                        request_id: &request_id,
                        original_request: &request_json,
                        operation_id: &operation_id,
                        result: &result,
                        context: json!({"script_id":bundle_for_tx.script_id,"revision":1,"bundle_ref":record_for_tx.artifact_id}),
                        settled: true,
                        now_ms: now,
                    },
                )?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(value)
    }

    async fn revise_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ReviseRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if let Some(value) = self.replay(&principal, "script.revise", &params).await? {
            return Ok(value);
        }
        let request_json = model::canonical(&params)?;
        let caller = principal.client_id.clone();
        let principal_for_head = principal.clone();
        let id = request.script_id.clone();
        let expected_revision = request.expected_revision;
        let head = self
            .run(move |db| {
                let current = require_script_authority(db, &principal_for_head)?;
                let owner: Option<String> = db
                    .query_row(
                        "SELECT owner_id FROM scripts WHERE script_id=?1",
                        [&id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let owner =
                    owner.ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))?;
                if current.role == Role::Manager && owner != current.client_id {
                    return Err(Error::new(
                        "FORBIDDEN",
                        "only the registered script owner may revise it",
                    ));
                }
                let latest: i64 = db.query_row(
                    "SELECT COALESCE(MAX(revision),0) FROM script_revisions WHERE script_id=?1",
                    [&id],
                    |row| row.get(0),
                )?;
                if latest != expected_revision {
                    return Err(Error::new(
                        "STALE_REVISION",
                        "script revision changed before revise",
                    ));
                }
                latest
                    .checked_add(1)
                    .ok_or_else(|| Error::invalid("script revision overflow"))
            })
            .await?;
        let bundle_request = request.bundle;
        let bundle = tokio::task::spawn_blocking(move || bundle_request.capture())
            .await
            .map_err(|error| Error::new("SCRIPT_PREPARATION", error.to_string()))??;
        let bundle_bytes = model::canonical(&json!(bundle))?.into_bytes();
        if bundle_bytes.len() > manifest::MAX_BUNDLE_REQUEST_BYTES + manifest::MAX_SCHEMA_BYTES {
            return Err(Error::invalid(
                "captured script bundle exceeds the 932 KiB stored manifest limit",
            ));
        }
        let bundle_sha256 = model::digest(&bundle_bytes);
        let artifact_id = bundle_artifact_id(
            &bundle.script_id,
            head,
            &caller,
            &request.client_request_id,
            &bundle_sha256,
        )?;
        let metadata = json!({
            "script_id":bundle.script_id,
            "revision":head,
            "bundle_sha256":bundle_sha256,
            "interpreter_kind":bundle.interpreter.kind,
            "interpreter_sha256":bundle.interpreter.sha256,
        });
        let (record, bytes) = ArtifactFiles::document(
            registry::BUNDLE_KIND,
            &artifact_id,
            &json!(bundle),
            metadata,
        )?;
        let publish_record = record.clone();
        self.file_io(move |files| files.publish(&publish_record, &bytes))
            .await?;
        let principal_for_tx = principal.clone();
        let request_id = request.client_request_id;
        let bundle_for_tx = bundle.clone();
        let record_for_tx = record.clone();
        let script_id = request.script_id;
        let value = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = require_script_authority(&tx, &principal_for_tx)?;
                if let Some(receipt) = replay_tx(
                    &tx,
                    &caller,
                    "script.revise",
                    &request_id,
                    &request_json,
                )? {
                    tx.commit()?;
                    return Ok(receipt);
                }
                let owner: Option<String> = tx
                    .query_row("SELECT owner_id FROM scripts WHERE script_id=?1", [&script_id], |row| row.get(0))
                    .optional()?;
                let owner = owner.ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))?;
                if current.role == Role::Manager && owner != current.client_id {
                    return Err(Error::new("FORBIDDEN", "only the registered script owner may revise it"));
                }
                let latest: i64 = tx.query_row(
                    "SELECT COALESCE(MAX(revision),0) FROM script_revisions WHERE script_id=?1",
                    [&script_id],
                    |row| row.get(0),
                )?;
                if latest != expected_revision || head != expected_revision + 1 {
                    return Err(Error::new("STALE_REVISION", "script revision changed before revise"));
                }
                let now = model::now_ms()?;
                registry::register_artifact(&tx, &record_for_tx, now)?;
                insert_revision(&tx, &bundle_for_tx, &record_for_tx, &current.client_id, head, now)?;
                tx.execute("UPDATE scripts SET updated_at_ms=?2 WHERE script_id=?1", params![script_id, now])?;
                let operation_id = model::new_id();
                let result = json!({
                    "operation_id":operation_id,
                    "script_id":script_id,
                    "revision":head,
                    "previous_revision":expected_revision,
                    "bundle_ref":record_for_tx.artifact_id,
                    "state":"registered",
                    "validated":true,
                    "activation":"not_selected",
                });
                record_operation(
                    &tx,
                    &current,
                    OperationRecord {
                        method: "script.revise",
                        request_id: &request_id,
                        original_request: &request_json,
                        operation_id: &operation_id,
                        result: &result,
                        context: json!({"script_id":script_id,"revision":head,"bundle_ref":record_for_tx.artifact_id}),
                        settled: true,
                        now_ms: now,
                    },
                )?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(value)
    }

    async fn validate_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ValidateRequest::parse(&params)?;
        let id = request.script_id.clone();
        let revision = request.revision;
        let snapshot = self
            .run(move |db| {
                require_script_authority(db, &principal)?;
                revision_snapshot(db, &id, revision)
            })
            .await?;
        let record = snapshot.record.clone();
        let interpreter = snapshot.interpreter.clone();
        let checked = self
            .file_io(move |files| verify_revision_files(&files, &record, &interpreter))
            .await?;
        Ok(json!({
            "script_id":request.script_id,
            "revision":request.revision,
            "valid":true,
            "bundle_ref":snapshot.record.artifact_id,
            "bundle_sha256":snapshot.record.content_digest,
            "interpreter":checked.interpreter,
            "validated_at_ms":snapshot.validated_at_ms,
            "checked_at_ms":model::now_ms()?,
            "execution_started":false,
            "controller_effects":[],
        }))
    }

    async fn activate_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ActivateRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if let Some(value) = self.replay(&principal, "script.activate", &params).await? {
            return Ok(value);
        }
        let id = request.script_id.clone();
        let revision = request.revision;
        let principal_for_snapshot = principal.clone();
        let snapshot = self
            .run(move |db| {
                require_script_authority(db, &principal_for_snapshot)?;
                revision_snapshot(db, &id, revision)
            })
            .await?;
        let record = snapshot.record.clone();
        let interpreter = snapshot.interpreter.clone();
        self.file_io(move |files| verify_revision_files(&files, &record, &interpreter))
            .await?;
        let request_json = model::canonical(&params)?;
        let caller = principal.client_id.clone();
        let p = principal.clone();
        let request_id = request.client_request_id;
        let script_id = request.script_id;
        let revision = request.revision;
        let value = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = require_script_authority(&tx, &p)?;
                if let Some(receipt) =
                    replay_tx(&tx, &caller, "script.activate", &request_id, &request_json)?
                {
                    tx.commit()?;
                    return Ok(receipt);
                }
                let current_snapshot = revision_snapshot(&tx, &script_id, revision)?;
                if current_snapshot.record.artifact_id != snapshot.record.artifact_id
                    || current_snapshot.record.content_digest != snapshot.record.content_digest
                    || current_snapshot.interpreter != snapshot.interpreter
                    || current_snapshot.validated_at_ms <= 0
                {
                    return Err(Error::new(
                        "SCRIPT_REVISION_CHANGED",
                        "script revision changed before activation",
                    ));
                }
                let now = model::now_ms()?;
                tx.execute(
                    "UPDATE scripts SET active_revision=?2,updated_at_ms=?3 WHERE script_id=?1",
                    params![script_id, revision, now],
                )?;
                let operation_id = model::new_id();
                let result = json!({
                    "operation_id":operation_id,
                    "script_id":script_id,
                    "active_revision":revision,
                    "bundle_ref":current_snapshot.record.artifact_id,
                    "state":"activated",
                    "execution_started":false,
                });
                record_operation(
                    &tx,
                    &current,
                    OperationRecord {
                        method: "script.activate",
                        request_id: &request_id,
                        original_request: &request_json,
                        operation_id: &operation_id,
                        result: &result,
                        context: json!({"script_id":script_id,"revision":revision}),
                        settled: true,
                        now_ms: now,
                    },
                )?;
                tx.commit()?;
                Ok(result)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(value)
    }

    async fn run_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::RunRequest::parse(&params)?;
        validate_client_request_id(&request.client_request_id)?;
        if let Some(value) = self.replay(&principal, "script.run", &params).await? {
            return Ok(value);
        }
        let script_id = request.script_id.clone();
        let expected_revision = request.expected_script_revision;
        let attempt_id = request.attempt_id.clone();
        let expected_task_revision = request.expected_task_revision;
        let principal_for_snapshot = principal.clone();
        let snapshot = self
            .run(move |db| {
                let current = require_script_authority(db, &principal_for_snapshot)?;
                let (task, attempt) =
                    require_run_scope(db, &current, &attempt_id, expected_task_revision)?;
                let (active, _) = script_head(db, &script_id)?;
                if active != Some(expected_revision) {
                    return Err(Error::new(
                        "SCRIPT_REVISION_NOT_ACTIVE",
                        "requested script revision is not active",
                    ));
                }
                let revision = revision_snapshot(db, &script_id, expected_revision)?;
                Ok((task, attempt, revision))
            })
            .await?;
        let task = snapshot.0;
        let attempt = snapshot.1;
        let revision = snapshot.2;
        let record = revision.record.clone();
        let interpreter = revision.interpreter.clone();
        let input = request.input.clone();
        let (bundle, environment) = self
            .file_io(move |files| {
                let bundle = verify_revision_files(&files, &record, &interpreter)?;
                protocol::validate_input(&bundle.input_schema, &input)?;
                let environment = runner::capture_environment(&bundle)?;
                Ok((bundle, environment))
            })
            .await?;
        let operation_id = model::new_id();
        let run_id = model::new_id();
        let token = model::new_id();
        let invocation = protocol::ScriptInvocation {
            protocol_version: 1,
            operation_id: operation_id.clone(),
            run_id: run_id.clone(),
            script_id: request.script_id.clone(),
            script_revision: request.expected_script_revision,
            task_id: model::text(&task, "task_id")?.to_owned(),
            task_revision: request.expected_task_revision,
            attempt_id: request.attempt_id.clone(),
            input: request.input.clone(),
            controller_effects: Vec::new(),
        };
        let work = runner::Work {
            run_id: run_id.clone(),
            operation_id: operation_id.clone(),
            token,
            data_dir: self.data_dir.clone(),
            bundle_record: revision.record.clone(),
            interpreter: bundle.interpreter.clone(),
            bundle: bundle.clone(),
            environment_sha256: runner::environment_sha256(&environment)?,
            environment,
            invocation,
        };
        let work_for_write = work.clone();
        let (receipt_path, work_digest) = self
            .file_io(move |_| runner::write_work(&work_for_write))
            .await?;
        if receipt_path != runner::directory(&self.data_dir, &run_id)?.join("receipt.json") {
            return Err(Error::new(
                "SCRIPT_WORK_DAMAGED",
                "stored script receipt path differs from its run",
            ));
        }
        let request_json = model::canonical(&params)?;
        let caller = principal.client_id.clone();
        let p = principal.clone();
        let request_id = request.client_request_id;
        let script_id = request.script_id;
        let requested_revision = request.expected_script_revision;
        let expected_task_revision = request.expected_task_revision;
        let attempt_id = request.attempt_id;
        let stored_task = task.clone();
        let stored_attempt = attempt.clone();
        let stored_revision = revision.clone();
        let digest_for_tx = work_digest.clone();
        let work_for_tx = work.clone();
        let result = self
            .run(move |db| {
                let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let current = require_script_authority(&tx, &p)?;
                if let Some(receipt) = replay_tx(&tx, &caller, "script.run", &request_id, &request_json)? {
                    tx.commit()?;
                    return Ok(receipt);
                }
                let (current_task, current_attempt) =
                    require_run_scope(&tx, &current, &attempt_id, expected_task_revision)?;
                if current_task["task_id"] != stored_task["task_id"]
                    || current_attempt["attempt_id"] != stored_attempt["attempt_id"]
                {
                    return Err(Error::new("SCRIPT_SCOPE_CHANGED", "Task or Attempt changed during run preparation"));
                }
                let (active, _) = script_head(&tx, &script_id)?;
                if active != Some(requested_revision) {
                    return Err(Error::new("SCRIPT_REVISION_NOT_ACTIVE", "requested script revision is no longer active"));
                }
                let current_revision = revision_snapshot(&tx, &script_id, requested_revision)?;
                if current_revision.record.artifact_id != stored_revision.record.artifact_id
                    || current_revision.record.content_digest != stored_revision.record.content_digest
                    || current_revision.interpreter != stored_revision.interpreter
                {
                    return Err(Error::new("SCRIPT_REVISION_CHANGED", "retained script revision changed during run preparation"));
                }
                if work_for_tx.invocation.task_id != current_task["task_id"]
                    || work_for_tx.invocation.task_revision != current_task["revision"]
                    || work_for_tx.invocation.attempt_id != current_attempt["attempt_id"]
                {
                    return Err(Error::new("SCRIPT_SCOPE_CHANGED", "script invocation identity differs from the current Task"));
                }
                let now = model::now_ms()?;
                let admitted = json!({
                    "operation_id":operation_id,
                    "run_id":run_id,
                    "script_id":script_id,
                    "revision":requested_revision,
                    "bundle_ref":current_revision.record.artifact_id,
                    "task_id":current_task["task_id"],
                    "task_revision":current_task["revision"],
                    "attempt_id":current_attempt["attempt_id"],
                    "state":"queued",
                    "admission":"durable_local",
                    "controller_effects":[],
                });
                tx.execute(
                    "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,'script.run',?4,?5,?6,?7,'queued',?8,?9,NULL,?9,?9)",
                    params![
                        operation_id,
                        current.client_id,
                        request_id,
                        request_json,
                        model::canonical(&json!({
                            "script_run":{"run_id":run_id,"bundle_ref":current_revision.record.artifact_id,"work_digest":digest_for_tx,"environment_sha256":work_for_tx.environment_sha256,"controller_effects":[]},
                            "receipt":{"ok":true,"value":admitted},
                        }))?,
                        current_task["task_id"].as_str(),
                        current_attempt["attempt_id"].as_str(),
                        model::canonical(&admitted)?,
                        now,
                    ],
                )?;
                tx.execute(
                    "INSERT INTO script_runs(run_id,operation_id,script_id,revision,bundle_ref,task_id,task_revision,attempt_id,work_digest,spec_json,state,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,'queued',?11)",
                    params![
                        run_id,
                        operation_id,
                        script_id,
                        requested_revision,
                        current_revision.record.artifact_id,
                        current_task["task_id"].as_str(),
                        current_task["revision"].as_i64(),
                        current_attempt["attempt_id"].as_str(),
                        digest_for_tx,
                        model::canonical(&json!({
                            "environment_sha256":work_for_tx.environment_sha256,
                            "input_sha256":model::digest(model::canonical(&work_for_tx.invocation.input)?.as_bytes()),
                            "capabilities":[],
                            "trust":"trusted_local",
                        }))?,
                        now,
                    ],
                )?;
                tx.execute(
                    "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.run',?3,?4)",
                    params![format!("admitted:{operation_id}"), operation_id, model::canonical(&admitted)?, now],
                )?;
                tx.commit()?;
                Ok(admitted)
            })
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(result)
    }

    async fn get_script(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::GetRequest::parse(&params)?;
        self.run(move |db| {
            require_script_authority(db, &principal)?;
            registry::describe(db, &request.script_id, request.revision)
        })
        .await
    }

    async fn list_scripts(&self, principal: Principal, params: Value) -> Result<Value> {
        let request = protocol::ListRequest::parse(&params)?;
        let after = request.after.unwrap_or(0);
        let limit = request.limit.unwrap_or(50);
        self.run(move |db| {
            require_script_authority(db, &principal)?;
            registry::list(db, after, limit)
        })
        .await
    }

    async fn replay(
        &self,
        principal: &Principal,
        method: &'static str,
        params: &Value,
    ) -> Result<Option<Value>> {
        let current = principal.clone();
        let caller = principal.client_id.clone();
        let request_id = model::text(params, "client_request_id")?.to_owned();
        let original = model::canonical(params)?;
        self.run(move |db| {
            require_script_authority(db, &current)?;
            replay_tx(db, &caller, method, &request_id, &original)
        })
        .await
    }

    pub async fn supervise_scripts(&self, mut stop: watch::Receiver<bool>) -> Result<()> {
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            self.reconcile_scripts_once().await?;
            tokio::select! {
                _ = tokio::time::sleep(RECONCILE_INTERVAL) => {},
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { return Ok(()); }
                }
            }
        }
    }

    pub(crate) async fn reconcile_scripts_once(&self) -> Result<()> {
        let rows = self.run(|db| pending_runs(db)).await?;
        for pending in rows {
            let run_id = pending.run_id.clone();
            let digest = pending.work_digest.clone();
            let data_dir = self.data_dir.clone();
            let loaded = self
                .file_io(move |_| {
                    let receipt = runner::directory(&data_dir, &run_id)?.join("receipt.json");
                    let (work, actual_digest) = runner::read_work_record(&receipt)?;
                    if actual_digest != digest {
                        return Err(Error::new(
                            "SCRIPT_WORK_DAMAGED",
                            "retained work digest differs from SQLite",
                        ));
                    }
                    Ok(work)
                })
                .await;
            let work = match loaded {
                Ok(work) => work,
                Err(error) => {
                    self.record_run_error(&pending, error.clone()).await?;
                    let run_id = pending.run_id.clone();
                    let data_dir = self.data_dir.clone();
                    let terminal = self
                        .file_io(move |_| runner::terminal_receipt_exists(&data_dir, &run_id))
                        .await
                        .unwrap_or(true);
                    if terminal {
                        let run_id = pending.run_id.clone();
                        let code = error.code.clone();
                        let data_dir = self.data_dir.clone();
                        let started = self
                            .file_io(move |_| runner::started_receipt_exists(&data_dir, &run_id))
                            .await
                            .unwrap_or(true);
                        self.settle_incomplete(&pending, &code, started).await?;
                    } else if pending.operation_state == "queued" {
                        self.fail_before_start(&pending, &error.code).await?;
                    } else if pending.operation_state == "sending" {
                        let data_dir = self.data_dir.clone();
                        let run_id = pending.run_id.clone();
                        let operation_id = pending.operation_id.clone();
                        let code = error.code.clone();
                        let denied = self
                            .file_io(move |_| {
                                runner::deny_unstarted(&data_dir, &run_id, &operation_id, &code)
                            })
                            .await
                            .unwrap_or(false);
                        if denied {
                            self.fail_before_start(&pending, &error.code).await?;
                        } else {
                            self.mark_unknown(&pending, &error.code).await?;
                        }
                    } else if matches!(
                        pending.operation_state.as_str(),
                        "native_accepted" | "outcome_unknown"
                    ) {
                        self.mark_unknown(&pending, &error.code).await?;
                    }
                    continue;
                }
            };
            let observe_work = work.clone();
            let observed = self
                .file_io(move |files| observe(&observe_work, &files))
                .await;
            let observed = match observed {
                Ok(observed) => observed,
                Err(error) => {
                    self.record_run_error(&pending, error.clone()).await?;
                    let run_id = pending.run_id.clone();
                    let data_dir = self.data_dir.clone();
                    let terminal = self
                        .file_io(move |_| runner::terminal_receipt_exists(&data_dir, &run_id))
                        .await
                        .unwrap_or(true);
                    if terminal {
                        let run_id = pending.run_id.clone();
                        let code = error.code.clone();
                        let data_dir = self.data_dir.clone();
                        let started = self
                            .file_io(move |_| runner::started_receipt_exists(&data_dir, &run_id))
                            .await
                            .unwrap_or(true);
                        self.settle_incomplete(&pending, &code, started).await?;
                    } else if pending.operation_state == "queued" {
                        self.fail_before_start(&pending, &error.code).await?;
                    } else if pending.operation_state == "sending"
                        && !self.start_gate_exists(&work).await
                    {
                        self.deny_work(&work, &error.code).await?;
                        self.fail_before_start(&pending, &error.code).await?;
                    } else {
                        self.mark_unknown(&pending, &error.code).await?;
                    }
                    continue;
                }
            };
            if let Some(completion) = observed.completion {
                let execution_may_have_started =
                    observed.has_go || completion.started_at_ms.is_some();
                let id = pending.run_id.clone();
                let result = self.run(move |db| finish(db, &id, completion)).await;
                if let Err(error) = result {
                    if completion_failure_is_terminal(&error) {
                        self.record_run_error(&pending, error.clone()).await?;
                        self.settle_incomplete(&pending, &error.code, execution_may_have_started)
                            .await?;
                    } else {
                        return Err(error);
                    }
                } else {
                    self.changed
                        .send_modify(|revision| *revision = revision.wrapping_add(1));
                }
                continue;
            }
            match pending.operation_state.as_str() {
                "queued" => {
                    let id = pending.run_id.clone();
                    let operation_id = pending.operation_id.clone();
                    let start_work = work.clone();
                    let started = self.run(move |db| begin_run(db, &id, &operation_id)).await;
                    match started {
                        Ok(true) => {
                            let spawn_work = start_work.clone();
                            let digest = pending.work_digest.clone();
                            let launch = self
                                .file_io(move |_| runner::prepare_and_spawn(&spawn_work, &digest))
                                .await;
                            if let Err(error) = launch {
                                self.record_run_error(&pending, error.clone()).await?;
                                self.deny_work(&start_work, &error.code).await?;
                                self.fail_before_start(&pending, &error.code).await?;
                            }
                        }
                        Ok(false) => {}
                        Err(error) => {
                            self.record_run_error(&pending, error).await?;
                        }
                    }
                }
                "sending" => {
                    if let Some(identity) = observed.ready {
                        let id = pending.run_id.clone();
                        let accepted = self
                            .run(move |db| acknowledge_worker(db, &id, &identity))
                            .await;
                        match accepted {
                            Ok(true) => {
                                let allow_work = work.clone();
                                let result =
                                    self.file_io(move |_| runner::allow(&allow_work)).await;
                                if let Err(error) = result {
                                    self.record_run_error(&pending, error.clone()).await?;
                                    if !self.start_gate_exists(&work).await {
                                        self.deny_work(&work, &error.code).await?;
                                        self.fail_before_start(&pending, &error.code).await?;
                                    } else {
                                        self.mark_unknown(&pending, &error.code).await?;
                                    }
                                } else {
                                    self.changed.send_modify(|revision| {
                                        *revision = revision.wrapping_add(1)
                                    });
                                }
                            }
                            Ok(false) => {}
                            Err(error) => {
                                self.record_run_error(&pending, error.clone()).await?;
                                self.deny_work(&work, &error.code).await?;
                                self.fail_before_start(&pending, &error.code).await?;
                            }
                        }
                    } else if observed.launch_departed {
                        let code = "SCRIPT_WORKER_LOST_BEFORE_START";
                        self.record_run_error(
                            &pending,
                            Error::new(code, "worker exited before its start gate"),
                        )
                        .await?;
                        if observed.has_go {
                            self.mark_unknown(&pending, code).await?;
                        } else {
                            self.deny_work(&work, code).await?;
                            self.fail_before_start(&pending, code).await?;
                        }
                    } else if pending.sent_at_ms.is_some_and(|sent| {
                        model::now_ms()
                            .is_ok_and(|now| now.saturating_sub(sent) > STARTING_WORKER_TIMEOUT_MS)
                    }) {
                        let code = "SCRIPT_WORKER_START_TIMEOUT";
                        self.record_run_error(
                            &pending,
                            Error::new(code, "worker did not publish its ready receipt"),
                        )
                        .await?;
                        self.deny_work(&work, code).await?;
                        self.fail_before_start(&pending, code).await?;
                    }
                }
                "native_accepted"
                    if observed.launch_departed
                        || (observed.ready.is_none() && observed.launch.is_none()) =>
                {
                    let code = "SCRIPT_OUTCOME_UNOBSERVED";
                    self.record_run_error(
                        &pending,
                        Error::new(
                            code,
                            "started worker disappeared without a completion receipt",
                        ),
                    )
                    .await?;
                    self.mark_unknown(&pending, code).await?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn deny_work(&self, work: &runner::Work, code: &str) -> Result<()> {
        let work = work.clone();
        let code = code.to_owned();
        // A missing denial file cannot authorize execution: the worker still
        // requires the separate go receipt, and will time out if it never
        // arrives. Keep this optional run failure local to its Operation.
        let _ = self.file_io(move |_| runner::deny(&work, &code)).await;
        Ok(())
    }

    async fn start_gate_exists(&self, work: &runner::Work) -> bool {
        let work = work.clone();
        self.file_io(move |_| runner::has_start_gate(&work))
            .await
            .unwrap_or(true)
    }

    async fn fail_before_start(&self, pending: &PendingRun, code: &str) -> Result<()> {
        let run_id = pending.run_id.clone();
        let code = code.to_owned();
        self.run(move |db| fail_before_start(db, &run_id, &code))
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn settle_incomplete(
        &self,
        pending: &PendingRun,
        code: &str,
        execution_may_have_started: bool,
    ) -> Result<()> {
        let run_id = pending.run_id.clone();
        let code = code.to_owned();
        self.run(move |db| settle_incomplete(db, &run_id, &code, execution_may_have_started))
            .await?;
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(())
    }

    async fn mark_unknown(&self, pending: &PendingRun, code: &str) -> Result<()> {
        let run_id = pending.run_id.clone();
        let code = code.to_owned();
        self.run(move |db| mark_unknown(db, &run_id, &code)).await
    }

    async fn record_run_error(&self, pending: &PendingRun, error: Error) -> Result<()> {
        let run_id = pending.run_id.clone();
        let operation_id = pending.operation_id.clone();
        self.run(move |db| record_incident(db, &run_id, &operation_id, error))
            .await
    }
}

fn require_script_authority(db: &Connection, principal: &Principal) -> Result<Principal> {
    let current = current_principal(db, principal.clone())?;
    if !matches!(current.role, Role::Manager | Role::Operator) {
        return Err(Error::new(
            "FORBIDDEN",
            "script methods require current GM or local Operator authority",
        ));
    }
    gm::require_authority(db, &current)?;
    Ok(current)
}

fn validate_client_request_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 128
        || id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(
            "client_request_id must be 1..=128 bytes without whitespace",
        ));
    }
    Ok(())
}

fn replay_tx(
    db: &Connection,
    caller: &str,
    method: &str,
    request_id: &str,
    original: &str,
) -> Result<Option<Value>> {
    let row: Option<(String, String, String)> = db
        .query_row(
            "SELECT method,original_request_json,effective_request_json FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![caller, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((old_method, old_original, effective)) = row else {
        return Ok(None);
    };
    if old_method != method || old_original != original {
        return Err(Error::new(
            "REQUEST_ID_CONFLICT",
            "script request ID was used with another method or payload",
        ));
    }
    let effective: Value = serde_json::from_str(&effective)?;
    let receipt = effective
        .get("receipt")
        .ok_or_else(|| Error::new("INVALID_RECEIPT", "script Operation has no receipt"))?;
    super::receipt_result(receipt).map(Some)
}

fn record_operation(
    tx: &Transaction<'_>,
    principal: &Principal,
    record: OperationRecord<'_>,
) -> Result<()> {
    let state = if record.settled { "settled" } else { "queued" };
    let effective = json!({"script":record.context,"receipt":{"ok":true,"value":record.result}});
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?9,?9)",
        params![
            record.operation_id,
            principal.client_id,
            record.request_id,
            record.method,
            record.original_request,
            model::canonical(&effective)?,
            state,
            model::canonical(record.result)?,
            record.now_ms,
            if record.settled { Some(record.now_ms) } else { None },
        ],
    )?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,?3,?4,?5)",
        params![format!("admitted:{}", record.operation_id), record.operation_id, record.method, model::canonical(record.result)?, record.now_ms],
    )?;
    Ok(())
}

fn script_exists(db: &Connection, script_id: &str) -> Result<bool> {
    Ok(db.query_row(
        "SELECT EXISTS(SELECT 1 FROM scripts WHERE script_id=?1)",
        [script_id],
        |row| row.get(0),
    )?)
}

fn script_head(db: &Connection, script_id: &str) -> Result<(Option<i64>, i64)> {
    db.query_row(
        "SELECT active_revision,(SELECT COALESCE(MAX(revision),0) FROM script_revisions WHERE script_id=s.script_id) FROM scripts s WHERE script_id=?1",
        [script_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()?
    .ok_or_else(|| Error::new("NOT_FOUND", "script is not registered"))
}

fn revision_snapshot(db: &Connection, script_id: &str, revision: i64) -> Result<RevisionSnapshot> {
    let record = registry::bundle_record(db, script_id, revision)?;
    let (bundle_sha256, interpreter_json, validated_at_ms): (String, String, i64) = db.query_row(
        "SELECT bundle_sha256,interpreter_json,validated_at_ms FROM script_revisions WHERE script_id=?1 AND revision=?2",
        params![script_id, revision],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    if bundle_sha256 != record.content_digest || validated_at_ms <= 0 {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "script revision digest or validation receipt is invalid",
        ));
    }
    let interpreter: manifest::InterpreterIdentity = serde_json::from_str(&interpreter_json)
        .map_err(|_| {
            Error::new(
                "SCRIPT_REGISTRY_DAMAGED",
                "captured interpreter identity cannot be parsed",
            )
        })?;
    if record.metadata["script_id"] != script_id
        || record.metadata["revision"] != revision
        || record.metadata["bundle_sha256"] != bundle_sha256
        || record.metadata["interpreter_sha256"] != interpreter.sha256
    {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "bundle artifact metadata differs from its revision",
        ));
    }
    Ok(RevisionSnapshot {
        record,
        interpreter,
        validated_at_ms,
    })
}

fn insert_revision(
    tx: &Transaction<'_>,
    bundle: &manifest::ScriptBundle,
    record: &ArtifactRecord,
    created_by: &str,
    revision: i64,
    now: i64,
) -> Result<()> {
    if bundle.script_id != record.metadata["script_id"]
        || record.metadata["revision"] != revision
        || record.content_digest != record.metadata["bundle_sha256"].as_str().unwrap_or("")
    {
        return Err(Error::new(
            "SCRIPT_BUNDLE_DAMAGED",
            "bundle artifact differs from its revision metadata",
        ));
    }
    tx.execute(
        "INSERT INTO script_revisions(script_id,revision,bundle_ref,bundle_sha256,interpreter_json,validated_at_ms,created_by,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?6)",
        params![
            bundle.script_id,
            revision,
            record.artifact_id,
            record.content_digest,
            model::canonical(&json!(bundle.interpreter))?,
            now,
            created_by,
        ],
    )?;
    Ok(())
}

fn bundle_artifact_id(
    script_id: &str,
    revision: i64,
    caller: &str,
    request_id: &str,
    sha256: &str,
) -> Result<String> {
    let identity = json!({"script_id":script_id,"revision":revision,"caller":caller,"request_id":request_id,"bundle_sha256":sha256});
    Ok(format!(
        "script-{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn verify_revision_files(
    files: &ArtifactFiles,
    record: &ArtifactRecord,
    expected_interpreter: &manifest::InterpreterIdentity,
) -> Result<manifest::ScriptBundle> {
    let bytes = files.document_bytes(record)?;
    let bundle = registry::parse_bundle(&bytes, record)?;
    if bundle.interpreter != *expected_interpreter {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "bundle interpreter differs from the revision's captured identity",
        ));
    }
    let actual = manifest::capture_interpreter(
        &expected_interpreter.canonical_path,
        expected_interpreter.kind,
    )?;
    if actual != *expected_interpreter {
        return Err(Error::new(
            "SCRIPT_INTERPRETER_CHANGED",
            "installed interpreter differs from the captured revision",
        ));
    }
    Ok(bundle)
}

fn require_run_scope(
    db: &Connection,
    principal: &Principal,
    attempt_id: &str,
    expected_task_revision: i64,
) -> Result<(Value, Value)> {
    let current = require_script_authority(db, principal)?;
    if current.role == Role::Manager {
        gm::require_authority(db, &current)?;
    } else {
        current_principal(db, current.clone())?;
    }
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let task = tasks::get_task(db, model::text(&attempt, "task_id")?)?;
    if attempt["released_at_ms"] != Value::Null
        || task["state"] != "open"
        || task["current_attempt_id"] != attempt_id
        || task["revision"] != expected_task_revision
        || attempt["task_revision"] != expected_task_revision
    {
        return Err(Error::new(
            "STALE_ATTEMPT",
            "script run requires the exact current unreleased Attempt and Task revision",
        ));
    }
    gm::require_attempt_control(db, &current, &attempt)?;
    Ok((task, attempt))
}

fn pending_runs(db: &Connection) -> Result<Vec<PendingRun>> {
    let mut statement = db.prepare(
        "SELECT r.run_id,r.operation_id,o.state,r.work_digest,o.sent_at_ms \
         FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id \
         WHERE r.state IN ('queued','sending','running','reconciling','outcome_unknown') \
         ORDER BY r.created_at_ms,r.run_id LIMIT 64",
    )?;
    statement
        .query_map([], |row| {
            Ok(PendingRun {
                run_id: row.get(0)?,
                operation_id: row.get(1)?,
                operation_state: row.get(2)?,
                work_digest: row.get(3)?,
                sent_at_ms: row.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(Into::into)
}

fn observe(work: &runner::Work, files: &ArtifactFiles) -> Result<RunnerObservation> {
    if let Some(completion) = runner::completion(work, files)? {
        return Ok(RunnerObservation {
            completion: Some(completion),
            ready: None,
            launch: None,
            launch_departed: false,
            has_go: runner::has_start_gate(work)?,
        });
    }
    let ready = runner::ready(work)?;
    let launch = runner::launch_record(work)?;
    let launch_departed = match launch.as_ref() {
        Some(launch) => runner::worker_departed(work, launch)?,
        None => false,
    };
    Ok(RunnerObservation {
        completion: None,
        ready,
        launch,
        launch_departed,
        has_go: runner::has_start_gate(work)?,
    })
}

fn begin_run(db: &mut Connection, run_id: &str, operation_id: &str) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let state: Option<(String, String)> = tx
        .query_row(
            "SELECT o.state,r.state FROM operations o JOIN script_runs r ON r.operation_id=o.operation_id WHERE r.run_id=?1 AND o.operation_id=?2",
            params![run_id, operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((operation_state, run_state)) = state else {
        return Err(Error::new("NOT_FOUND", "script run is not registered"));
    };
    if operation_state != "queued" || run_state != "queued" {
        tx.commit()?;
        return Ok(false);
    }
    let caller: String = tx.query_row(
        "SELECT caller_id FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| row.get(0),
    )?;
    let actor = registered_actor(&tx, &caller)?;
    let script_scope: Option<(String, i64, String, i64, String)> = tx
        .query_row(
            "SELECT r.script_id,r.revision,r.task_id,r.task_revision,o.attempt_id FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        )
        .optional()?;
    let (script_id, revision, task_id, task_revision, attempt_id) =
        script_scope.ok_or_else(|| Error::new("NOT_FOUND", "script run is not registered"))?;
    let preflight = (|| -> Result<()> {
        require_script_authority(&tx, &actor)?;
        let (task, attempt) = require_run_scope(&tx, &actor, &attempt_id, task_revision)?;
        if task["task_id"] != task_id
            || task["revision"] != task_revision
            || attempt["attempt_id"] != attempt_id
        {
            return Err(Error::new(
                "SCRIPT_SCOPE_CHANGED",
                "queued script Task/Attempt scope changed",
            ));
        }
        let (active, _) = script_head(&tx, &script_id)?;
        if active != Some(revision) {
            return Err(Error::new(
                "SCRIPT_REVISION_NOT_ACTIVE",
                "queued script revision is no longer active",
            ));
        }
        Ok(())
    })();
    if let Err(error) = preflight {
        fail_before_start_tx(&tx, run_id, &error.code, model::now_ms()?)?;
        tx.commit()?;
        return Ok(false);
    }
    let now = model::now_ms()?;
    tx.execute(
        "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",
        params![operation_id, now],
    )?;
    tx.execute(
        "UPDATE script_runs SET state='sending' WHERE run_id=?1 AND state='queued'",
        [run_id],
    )?;
    tx.commit()?;
    Ok(true)
}

fn registered_actor(db: &Connection, caller: &str) -> Result<Principal> {
    let registration = meta(db, &format!("client:{caller}"))?.ok_or_else(|| {
        Error::new(
            "UNAUTHORIZED",
            "script Operation caller is no longer registered",
        )
    })?;
    let role: Role = serde_json::from_value(registration["role"].clone())?;
    Ok(Principal {
        client_id: caller.to_owned(),
        role,
        link_id: String::new(),
    })
}

fn acknowledge_worker(db: &mut Connection, run_id: &str, identity: &Value) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(String, String, Option<String>, String)> = tx
        .query_row(
            "SELECT o.operation_id,o.state,r.process_identity_json,o.caller_id FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((operation_id, operation_state, prior_identity, caller)) = row else {
        return Err(Error::new("NOT_FOUND", "script run is not registered"));
    };
    if operation_state != "sending" {
        tx.commit()?;
        return Ok(false);
    }
    if identity["run_id"] != run_id
        || identity["operation_id"] != operation_id
        || identity["token"].as_str().is_none_or(str::is_empty)
        || identity["process"]["purpose"] != "script"
    {
        return Err(Error::new(
            "SCRIPT_WORKER_DAMAGED",
            "worker readiness identity differs from its Operation",
        ));
    }
    if let Some(prior) = prior_identity {
        if serde_json::from_str::<Value>(&prior)? != *identity {
            return Err(Error::new(
                "SCRIPT_WORKER_DAMAGED",
                "script worker identity cannot be replaced",
            ));
        }
        tx.commit()?;
        return Ok(true);
    }
    let actor = registered_actor(&tx, &caller)?;
    let scope: (String, i64, String) = tx.query_row(
        "SELECT task_id,task_revision,attempt_id FROM script_runs WHERE run_id=?1",
        [run_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    let current = require_script_authority(&tx, &actor)?;
    require_run_scope(&tx, &current, &scope.2, scope.1)?;
    let task_id: String = tx.query_row(
        "SELECT task_id FROM script_runs WHERE run_id=?1",
        [run_id],
        |row| row.get(0),
    )?;
    if task_id != scope.0 {
        return Err(Error::new(
            "SCRIPT_SCOPE_CHANGED",
            "script Task changed before the start gate",
        ));
    }
    let now = model::now_ms()?;
    tx.execute(
        "UPDATE script_runs SET state='running',started_at_ms=?2,process_identity_json=?3 WHERE run_id=?1 AND state='sending'",
        params![run_id, now, model::canonical(identity)?],
    )?;
    tx.execute(
        "UPDATE operations SET state='native_accepted',updated_at_ms=?2 WHERE operation_id=?1 AND state='sending'",
        params![operation_id, now],
    )?;
    tx.commit()?;
    Ok(true)
}

fn fail_before_start(db: &mut Connection, run_id: &str, code: &str) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    fail_before_start_tx(&tx, run_id, code, model::now_ms()?)?;
    tx.commit()?;
    Ok(())
}

fn fail_before_start_tx(tx: &Transaction<'_>, run_id: &str, code: &str, now: i64) -> Result<()> {
    let operation_id: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM script_runs WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(operation_id) = operation_id else {
        return Ok(());
    };
    let op_state: String = tx.query_row(
        "SELECT state FROM operations WHERE operation_id=?1",
        [&operation_id],
        |row| row.get(0),
    )?;
    if matches!(op_state.as_str(), "settled" | "rejected" | "cancelled") {
        return Ok(());
    }
    if matches!(op_state.as_str(), "native_accepted" | "outcome_unknown") {
        return mark_unknown_tx(tx, run_id, code, now);
    }
    let result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":"failed",
        "state":"failed",
        "error_code":code,
        "execution_started":false,
        "controller_effects":[],
    });
    tx.execute(
        "UPDATE script_runs SET state='failed',finished_at_ms=?2 WHERE run_id=?1 AND state IN ('queued','sending','reconciling')",
        params![run_id, now],
    )?;
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('queued','sending')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.failed',?3,?4)",
        params![format!("terminal:{operation_id}"), operation_id, model::canonical(&result)?, now],
    )?;
    Ok(())
}

fn settle_incomplete(
    db: &mut Connection,
    run_id: &str,
    code: &str,
    execution_may_have_started: bool,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(String, String, String)> = tx
        .query_row(
            "SELECT r.operation_id,r.state,o.state FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((operation_id, run_state, operation_state)) = row else {
        tx.commit()?;
        return Ok(());
    };
    if matches!(run_state.as_str(), "completed" | "failed" | "incomplete") {
        tx.commit()?;
        return Ok(());
    }
    let now = model::now_ms()?;
    let result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":"incomplete",
        "state":"incomplete",
        "diagnostic_code":code,
        "execution_may_have_started":execution_may_have_started,
        "result_read_method":"operation.get",
        "controller_effects":[],
    });
    tx.execute(
        "UPDATE script_runs SET state='incomplete',finished_at_ms=?2 WHERE run_id=?1 AND state IN ('queued','sending','running','reconciling','outcome_unknown')",
        params![run_id, now],
    )?;
    if !matches!(
        operation_state.as_str(),
        "settled" | "rejected" | "cancelled"
    ) {
        tx.execute(
            "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('queued','sending','native_accepted','outcome_unknown')",
            params![operation_id, model::canonical(&result)?, now],
        )?;
    }
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.incomplete',?3,?4)",
        params![format!("terminal:{operation_id}"), operation_id, model::canonical(&result)?, now],
    )?;
    tx.commit()?;
    Ok(())
}

fn mark_unknown(db: &mut Connection, run_id: &str, code: &str) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    mark_unknown_tx(&tx, run_id, code, model::now_ms()?)?;
    tx.commit()?;
    Ok(())
}

fn mark_unknown_tx(tx: &Transaction<'_>, run_id: &str, code: &str, now: i64) -> Result<()> {
    let operation_id: Option<String> = tx
        .query_row(
            "SELECT operation_id FROM script_runs WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(operation_id) = operation_id else {
        return Ok(());
    };
    let result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":"unknown",
        "state":"outcome_unknown",
        "diagnostic_code":code,
        "execution_may_have_started":true,
        "controller_effects":[],
    });
    tx.execute(
        "UPDATE script_runs SET state='outcome_unknown' WHERE run_id=?1 AND state IN ('sending','running','reconciling','outcome_unknown')",
        [run_id],
    )?;
    tx.execute(
        "UPDATE operations SET state='outcome_unknown',result_json=?2,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('sending','native_accepted','outcome_unknown')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    Ok(())
}

fn completion_failure_is_terminal(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "SCRIPT_COMPLETION_DAMAGED"
            | "SCRIPT_REGISTRY_DAMAGED"
            | "ARTIFACT_DAMAGED"
            | "NOT_FOUND"
            | "CONFLICT"
    )
}

fn finish(db: &mut Connection, run_id: &str, completion: runner::Completion) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row: Option<(String, String, i64, i64, String, String)> = tx
        .query_row(
            "SELECT r.operation_id,r.script_id,r.revision,r.task_revision,r.state,o.state FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .optional()?;
    let Some((operation_id, script_id, revision, task_revision, run_state, operation_state)) = row
    else {
        return Err(Error::new("NOT_FOUND", "script run is not registered"));
    };
    if completion.run_id != run_id || completion.operation_id != operation_id {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "script completion identifies another Operation",
        ));
    }
    if matches!(run_state.as_str(), "completed" | "failed" | "incomplete") {
        let result_ref: Option<String> = tx.query_row(
            "SELECT result_ref FROM script_runs WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )?;
        if result_ref.as_deref() == Some(completion.result.artifact_id.as_str()) {
            tx.commit()?;
            return Ok(());
        }
        return Err(Error::conflict("completed script run cannot be replaced"));
    }
    if !matches!(
        operation_state.as_str(),
        "sending" | "native_accepted" | "outcome_unknown"
    ) {
        return Err(Error::conflict(
            "script completion arrived outside its retained run",
        ));
    }
    let revision_info = revision_snapshot(&tx, &script_id, revision).map_err(|error| {
        if error.code == "INVALID_PARAMS" {
            Error::new(
                "SCRIPT_REGISTRY_DAMAGED",
                "retained script revision metadata cannot be parsed",
            )
        } else {
            error
        }
    })?;
    let work_record = registry::bundle_record(&tx, &script_id, revision).map_err(|error| {
        if error.code == "INVALID_PARAMS" {
            Error::new(
                "SCRIPT_REGISTRY_DAMAGED",
                "retained script bundle metadata cannot be parsed",
            )
        } else {
            error
        }
    })?;
    if work_record.artifact_id != revision_info.record.artifact_id {
        return Err(Error::new(
            "SCRIPT_REGISTRY_DAMAGED",
            "completion revision no longer names its retained bundle",
        ));
    }
    let now = model::now_ms()?;
    let artifacts = [&completion.result, &completion.stdout, &completion.stderr];
    for artifact in artifacts {
        register_output_artifact(&tx, artifact, now)?;
    }
    let outcome = match completion.state.as_str() {
        "completed" => "applied",
        "incomplete" => "incomplete",
        _ => "failed",
    };
    let result = json!({
        "operation_id":operation_id,
        "run_id":run_id,
        "outcome":outcome,
        "state":completion.state,
        "started_at_ms":completion.started_at_ms,
        "exit_code":completion.exit_code,
        "result_ref":completion.result.artifact_id,
        "stdout_ref":completion.stdout.artifact_id,
        "stderr_ref":completion.stderr.artifact_id,
        "result":completion.result_value,
        "error_code":completion.error_code,
        "task_revision":task_revision,
        "controller_effects":[],
    });
    tx.execute(
        "UPDATE script_runs SET state=?2,result_ref=?3,stdout_ref=?4,stderr_ref=?5,exit_code=?6,started_at_ms=COALESCE(?7,started_at_ms),finished_at_ms=?8 WHERE run_id=?1",
        params![run_id, completion.state, completion.result.artifact_id, completion.stdout.artifact_id, completion.stderr.artifact_id, completion.exit_code, completion.started_at_ms, now],
    )?;
    tx.execute(
        "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1 AND state IN ('sending','native_accepted','outcome_unknown')",
        params![operation_id, model::canonical(&result)?, now],
    )?;
    tx.execute(
        "INSERT OR IGNORE INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:scripts',?1,?2,'script.completed',?3,?4)",
        params![format!("terminal:{operation_id}"), operation_id, model::canonical(&result)?, now],
    )?;
    tx.commit()?;
    Ok(())
}

fn register_output_artifact(
    tx: &Transaction<'_>,
    artifact: &ArtifactRecord,
    now: i64,
) -> Result<()> {
    let limit = match artifact.kind.as_str() {
        "script_result" => manifest::MAX_RESULT_BYTES as u64 + 16 * 1024,
        "script_output" if artifact.metadata["stream"] == "stdout" => {
            manifest::MAX_RESULT_BYTES as u64
        }
        "script_output" if artifact.metadata["stream"] == "stderr" => {
            manifest::MAX_STDERR_BYTES as u64
        }
        _ => {
            return Err(Error::new(
                "SCRIPT_COMPLETION_DAMAGED",
                "script completion names an unsupported artifact",
            ));
        }
    };
    if artifact.byte_length > limit
        || artifact.content_digest.len() != 64
        || !artifact
            .content_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
        || artifact.relative_path != format!("artifacts/{}.bin", artifact.artifact_id)
    {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "script artifact exceeds its recorded bound",
        ));
    }
    let byte_length = i64::try_from(artifact.byte_length)
        .map_err(|_| Error::invalid("script artifact length exceeds SQLite range"))?;
    tx.execute(
        "INSERT OR IGNORE INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,metadata_json,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![artifact.artifact_id, artifact.relative_path, artifact.kind, byte_length, artifact.content_digest, model::canonical(&artifact.metadata)?, now],
    )?;
    let existing = results::get(tx, &artifact.artifact_id).map_err(|error| {
        if error.code == "INVALID_PARAMS" {
            Error::new(
                "SCRIPT_COMPLETION_DAMAGED",
                "registered script artifact metadata cannot be parsed",
            )
        } else {
            error
        }
    })?;
    if existing.kind != artifact.kind
        || existing.relative_path != artifact.relative_path
        || existing.byte_length != artifact.byte_length
        || existing.content_digest != artifact.content_digest
        || existing.metadata != artifact.metadata
    {
        return Err(Error::new(
            "SCRIPT_COMPLETION_DAMAGED",
            "registered script artifact differs from its immutable completion",
        ));
    }
    Ok(())
}

fn record_incident(db: &Connection, run_id: &str, operation_id: &str, error: Error) -> Result<()> {
    let now = model::now_ms()?;
    let key = format!("script:{run_id}:{}", error.code);
    let evidence = json!({"run_id":run_id,"operation_id":operation_id,"code":error.code});
    db.execute(
        "INSERT INTO incidents(incident_id,dedup_key,state,occurrences,action_operation_id,details_json,opened_at_ms,last_seen_at_ms) VALUES(?1,?2,'open',1,?3,?4,?5,?5) ON CONFLICT(dedup_key) WHERE state='open' DO NOTHING",
        params![model::new_id(), key, operation_id, model::canonical(&evidence)?, now],
    )?;
    Ok(())
}

pub(super) fn authorize_artifact_read(
    db: &Connection,
    principal: &Principal,
    artifact: &ArtifactRecord,
) -> Result<()> {
    require_script_authority(db, principal)?;
    match artifact.kind.as_str() {
        "script_bundle" => {
            let (script_id, revision): (String, i64) = db
                .query_row(
                    "SELECT script_id,revision FROM script_revisions WHERE bundle_ref=?1",
                    [&artifact.artifact_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?
                .ok_or_else(|| {
                    Error::new(
                        "NOT_FOUND",
                        "script bundle is not linked to a retained revision",
                    )
                })?;
            let linked = registry::bundle_record(db, &script_id, revision)?;
            if linked.kind != artifact.kind
                || linked.artifact_id != artifact.artifact_id
                || linked.content_digest != artifact.content_digest
                || linked.byte_length != artifact.byte_length
                || linked.relative_path != artifact.relative_path
                || linked.metadata != artifact.metadata
            {
                return Err(Error::new(
                    "ARTIFACT_DAMAGED",
                    "script bundle artifact differs from its retained revision",
                ));
            }
        }
        "script_result" | "script_output" => {
            let run_id = model::text(&artifact.metadata, "run_id")?;
            let operation_id = model::text(&artifact.metadata, "operation_id")?;
            let row: Option<ScriptRunArtifactLinks> = db
                .query_row(
                    "SELECT r.run_id,r.operation_id,r.state,r.result_ref,r.stdout_ref,r.stderr_ref,o.method \
                     FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id WHERE r.run_id=?1 AND r.operation_id=?2",
                    params![run_id, operation_id],
                    |row| {
                        Ok(ScriptRunArtifactLinks {
                            run_id: row.get(0)?,
                            operation_id: row.get(1)?,
                            state: row.get(2)?,
                            result_ref: row.get(3)?,
                            stdout_ref: row.get(4)?,
                            stderr_ref: row.get(5)?,
                            method: row.get(6)?,
                        })
                    },
                )
                .optional()?;
            let Some(stored) = row else {
                return Err(Error::new(
                    "NOT_FOUND",
                    "script artifact has no retained run",
                ));
            };
            if stored.run_id != run_id
                || stored.operation_id != operation_id
                || stored.method != "script.run"
                || !matches!(
                    stored.state.as_str(),
                    "completed" | "failed" | "incomplete" | "outcome_unknown"
                )
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "script artifact is outside its retained run",
                ));
            }
            let expected_ref = match artifact.kind.as_str() {
                "script_result" => stored.result_ref,
                "script_output" if artifact.metadata["stream"] == "stdout" => stored.stdout_ref,
                "script_output" if artifact.metadata["stream"] == "stderr" => stored.stderr_ref,
                _ => None,
            };
            if expected_ref.as_deref() != Some(artifact.artifact_id.as_str()) {
                return Err(Error::new(
                    "NOT_FOUND",
                    "script artifact is not referenced by its run",
                ));
            }
            let registered = results::get(db, &artifact.artifact_id)?;
            if registered.kind != artifact.kind
                || registered.relative_path != artifact.relative_path
                || registered.byte_length != artifact.byte_length
                || registered.content_digest != artifact.content_digest
                || registered.metadata != artifact.metadata
            {
                return Err(Error::new(
                    "ARTIFACT_DAMAGED",
                    "script artifact identity changed",
                ));
            }
        }
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "script artifact read requires a script artifact",
            ));
        }
    }
    Ok(())
}
