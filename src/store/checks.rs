//! CheckRun transactions and one host scheduler, using the existing nine tables.
use super::{Store, current_principal, meta, operations, results, tasks};
use crate::{
    artifacts::ArtifactRecord,
    checks::{
        model::{CaptureRequest, CheckRequest},
        source,
        worker::{self, CancelRequest, Completion, Work},
    },
    config::Config,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};
use std::{path::PathBuf, time::Duration};
use tokio::sync::watch;

fn attempt(db: &Connection, p: &Principal, id: &str) -> Result<Value> {
    p.require_writer()?;
    let a = tasks::get_attempt(db, id)?;
    p.owns(model::text(&a, "owner_id")?)?;
    let t = tasks::get_task(db, model::text(&a, "task_id")?)?;
    if !a["released_at_ms"].is_null() || t["state"] != "open" || t["revision"] != a["task_revision"]
    {
        return Err(Error::new(
            "STALE_ATTEMPT",
            "source/check requires current unreleased open Task ownership",
        ));
    }
    Ok(a)
}
fn artifact(db: &Connection, r: &ArtifactRecord) -> Result<()> {
    let length = i64::try_from(r.byte_length)
        .map_err(|_| Error::invalid("artifact length exceeds SQLite range"))?;
    db.execute("INSERT OR IGNORE INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,metadata_json,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![r.artifact_id,r.relative_path,r.kind,length,r.content_digest,model::canonical(&r.metadata)?,model::now_ms()?])?;
    let old = results::get(db, &r.artifact_id)?;
    if old.content_digest != r.content_digest
        || old.byte_length != r.byte_length
        || old.relative_path != r.relative_path
        || old.metadata != r.metadata
        || old.kind != r.kind
    {
        return Err(Error::conflict(
            "artifact identity already holds other bytes",
        ));
    }
    Ok(())
}
fn settle(db: &Connection, id: &str, result: &Value) -> Result<()> {
    let now = model::now_ms()?;
    let encoded = model::canonical(result)?;
    db.execute("UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1",params![id,encoded,now])?;
    db.execute("INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('controller:checks',?1,?1,'check.completed',?2,?3)",params![id,encoded,now])?;
    Ok(())
}
pub(super) fn reserve_source(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    config: &Config,
) -> Result<Value> {
    let input = CaptureRequest::parse(v)?;
    let a = attempt(tx, p, &input.attempt_id)?;
    if a["task_revision"] != input.expected_revision {
        return Err(Error::new(
            "STALE_REVISION",
            "source capture revision changed",
        ));
    }
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",params![id,a["task_id"].as_str(),input.attempt_id,model::canonical(&json!({"capture":input,"git_executable":config.checks.git_executable,"identity":{"task_id":a["task_id"],"attempt_id":a["attempt_id"],"task_revision":a["task_revision"]}}))?])?;
    Ok(json!({"operation_id":id,"state":"queued","admission":"durable_local"}))
}
fn begin_source(
    db: &mut Connection,
    p: Principal,
    id: &str,
) -> Result<Option<(CaptureRequest, PathBuf, Value)>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let p = current_principal(&tx, p)?;
    let op = operations::get_operation(&tx, id)?;
    if op["method"] != "source.capture" || op["caller_id"] != p.client_id {
        return Err(Error::new("FORBIDDEN", "capture belongs to another caller"));
    }
    if !matches!(op["state"].as_str(), Some("queued" | "outcome_unknown")) {
        return Ok(None);
    }
    let raw: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )?;
    let v: Value = serde_json::from_str(&raw)?;
    let input: CaptureRequest = serde_json::from_value(v["capture"].clone())?;
    attempt(&tx, &p, &input.attempt_id)?;
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1",params![id,model::now_ms()?])?;
    tx.commit()?;
    Ok(Some((
        input,
        serde_json::from_value(v["git_executable"].clone())?,
        v["identity"].clone(),
    )))
}
fn finish_source(
    db: &mut Connection,
    p: Principal,
    id: &str,
    outcome: Result<ArtifactRecord>,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let op = operations::get_operation(&tx, id)?;
    if op["state"] != "sending" || op["method"] != "source.capture" {
        return Err(Error::conflict("capture is not executing"));
    }
    let result=outcome.and_then(|r|{
        let p=current_principal(&tx,p)?;let a=attempt(&tx,&p,model::text(&op,"attempt_id")?)?;
        if r.metadata["attempt_id"]!=a["attempt_id"]||r.metadata["task_revision"]!=a["task_revision"]{return Err(Error::new("STALE_REVISION","capture revision changed before publication"));}
        artifact(&tx,&r)?;Ok(json!({"operation_id":id,"outcome":"applied","candidate_ref":r.artifact_id,"commit":r.metadata["commit"],"tree":r.metadata["tree"],"file_count":r.metadata["file_count"],"task_accepted":false}))
    });
    let result = match result {
        Ok(v) => v,
        Err(e) => json!({"operation_id":id,"outcome":"failed","error":e}),
    };
    settle(&tx, id, &result)?;
    tx.commit()?;
    Ok(())
}
pub(super) fn reserve(
    tx: &Transaction<'_>,
    p: &Principal,
    v: &Value,
    id: &str,
    config: &Config,
) -> Result<(Value, bool)> {
    let input = CheckRequest::parse(v)?;
    let a = attempt(tx, p, &input.attempt_id)?;
    let profile = config
        .checks
        .profile(&input.profile_id, &input.profile_revision)?;
    let candidate = results::get(tx, &input.candidate_ref)?;
    if candidate.kind != "source_snapshot"
        || candidate.metadata["attempt_id"] != input.attempt_id
        || candidate.metadata["task_revision"] != a["task_revision"]
    {
        return Err(Error::new(
            "CHECK_SOURCE_REQUIRED",
            "capture the exact source for this Attempt before requesting a machine check",
        ));
    }
    let key=model::digest(model::canonical(&json!({"candidate":candidate.artifact_id,"sha256":candidate.content_digest,"profile":profile}))?.as_bytes());
    let existing:Option<(String,String)>=tx.query_row("SELECT check_id,operation_id FROM check_runs WHERE attempt_id=?1 AND cache_key=?2 AND (state IN ('queued','running','reconciling') OR (resource_claimed_at_ms IS NOT NULL AND resource_released_at_ms IS NULL))",params![input.attempt_id,key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
    if let Some((check, op)) = existing {
        return Ok((
            json!({"operation_id":op,"check_id":check,"coalesced":true}),
            false,
        ));
    }
    let check = model::new_id();
    let token = model::new_id();
    let spec = json!({"profile_id":profile.profile_id,"profile_revision":profile.profile_revision,"profile":profile,"candidate":candidate,"token":token,"task_revision":a["task_revision"],"cache_policy":"disabled_unversioned_environment"});
    let resource = format!("check-target:{}", profile.resource.to_lowercase());
    tx.execute("INSERT INTO check_runs(check_id,operation_id,attempt_id,candidate_ref,cache_key,resource_key,spec_json,state,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,'queued',?8)",params![check,id,input.attempt_id,input.candidate_ref,key,resource,model::canonical(&spec)?,model::now_ms()?])?;
    tx.execute("UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=json_object('check_id',?4) WHERE operation_id=?1",params![id,a["task_id"].as_str(),input.attempt_id,check])?;
    Ok((
        json!({"operation_id":id,"check_id":check,"state":"queued","admission":"durable_local"}),
        true,
    ))
}
pub(super) fn cancel(tx: &Transaction<'_>, p: &Principal, v: &Value, id: &str) -> Result<Value> {
    model::fields(v, &["client_request_id", "check_id", "reason"])?;
    let check = model::text(v, "check_id")?;
    let reason = model::text(v, "reason")?;
    let c = describe(tx, &json!({"check_id":check}))?;
    let a = tasks::get_attempt(tx, model::text(&c, "attempt_id")?)?;
    p.owns(model::text(&a, "owner_id")?)?;
    if !matches!(
        c["state"].as_str(),
        Some("queued" | "running" | "reconciling")
    ) {
        return Ok(
            json!({"operation_id":id,"check_id":check,"cancellation_requested":false,"state":c["state"],"disposition":"already_terminal"}),
        );
    }
    if let Some(previous) = c.get("cancel_request").filter(|v| !v.is_null()) {
        return Ok(
            json!({"operation_id":id,"check_id":check,"cancellation_operation_id":previous["operation_id"],"cancellation_requested":true,"coalesced":true,"process_killed":false}),
        );
    }
    // An older independently running binary cannot be hot-upgraded into a new protocol.
    if !c["process"].is_null() && c["process"]["control_version"] != 2 {
        return Err(Error::new(
            "CHECK_CANCEL_UNSUPPORTED",
            "this running worker predates active cancellation; its normal result is still collected",
        ));
    }
    let request = CancelRequest {
        operation_id: id.into(),
        reason: reason.into(),
    };
    tx.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.cancel_requested',?2,'$.cancel_request',json(?3)) WHERE check_id=?1",
        params![check,reason,model::canonical(&json!(request))?])?;
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![id, a["task_id"].as_str(), a["attempt_id"].as_str()],
    )?;
    Ok(
        json!({"operation_id":id,"check_id":check,"cancellation_operation_id":id,"cancellation_requested":true,"admission":"durable_request","process_killed":false}),
    )
}
pub(super) fn describe(db: &Connection, v: &Value) -> Result<Value> {
    model::fields(v, &["check_id"])?;
    let id = model::text(v, "check_id")?;
    let raw:Option<String>=db.query_row("SELECT json_object('check_id',check_id,'operation_id',operation_id,'attempt_id',attempt_id,'candidate_ref',candidate_ref,'state',state,'resource_key',resource_key,'resource_claimed_at_ms',resource_claimed_at_ms,'resource_released_at_ms',resource_released_at_ms,'process',json(process_identity_json),'coverage',json(coverage_json),'result_ref',result_ref,'exit_code',exit_code,'profile_id',json_extract(spec_json,'$.profile_id'),'profile_revision',json_extract(spec_json,'$.profile_revision'),'cancel_request',json_extract(spec_json,'$.cancel_request'),'cancellation',json_extract(spec_json,'$.cancellation')) FROM check_runs WHERE check_id=?1",[id],|r|r.get(0)).optional()?;
    Ok(serde_json::from_str(&raw.ok_or_else(|| {
        Error::new("NOT_FOUND", "unknown CheckRun")
    })?)?)
}
fn work(db: &Connection, id: &str, root: PathBuf) -> Result<Work> {
    let (op, raw, identity): (String, String, Option<String>) = db.query_row(
        "SELECT operation_id,spec_json,process_identity_json FROM check_runs WHERE check_id=?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let spec: Value = serde_json::from_str(&raw)?;
    Ok(Work {
        check_id: id.into(),
        operation_id: op,
        token: model::text(&spec, "token")?.into(),
        preflight_error: spec
            .get("preflight_error")
            .filter(|v| !v.is_null())
            .cloned(),
        data_dir: root,
        candidate: serde_json::from_value(spec["candidate"].clone())?,
        profile: serde_json::from_value(spec["profile"].clone())?,
        cancel_request: spec
            .get("cancel_request")
            .filter(|v| !v.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose()?,
        expected_worker: identity.map(|raw| serde_json::from_str(&raw)).transpose()?,
    })
}
fn next(db: &mut Connection, config: &Config, root: PathBuf) -> Result<Option<Work>> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let enabled = config.checks.enabled
        && meta(&tx, "execution_mode")?.unwrap_or(Value::Null)["new_work"] == "enabled";
    let running: i64 = tx.query_row(
        "SELECT count(*) FROM check_runs WHERE state='running'",
        [],
        |r| r.get(0),
    )?;
    let row:Option<(String,String,String)>=tx.query_row("SELECT c.check_id,o.caller_id,c.attempt_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE c.state='queued' AND o.state='queued' AND (json_extract(c.spec_json,'$.cancel_requested') IS NOT NULL OR (?1 AND ?2 AND NOT EXISTS(SELECT 1 FROM check_runs active WHERE active.resource_key=c.resource_key AND active.resource_claimed_at_ms IS NOT NULL AND active.resource_released_at_ms IS NULL))) ORDER BY c.created_at_ms,c.check_id LIMIT 1",params![enabled,running < config.checks.max_running as i64],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    let Some((id, caller, attempt_id)) = row else {
        return Ok(None);
    };
    let mut w = work(&tx, &id, root)?;
    let preflight = (|| -> Result<()> {
        let cancelled:bool=tx.query_row("SELECT json_extract(spec_json,'$.cancel_requested') IS NOT NULL FROM check_runs WHERE check_id=?1",[&id],|r|r.get(0))?;
        if cancelled {
            return Err(Error::new(
                "CHECK_CANCELLED",
                "queued check cancelled before command execution",
            ));
        }
        let p = current_principal(
            &tx,
            Principal {
                client_id: caller,
                link_id: String::new(),
                role: Role::Manager,
            },
        )?;
        attempt(&tx, &p, &attempt_id)?;
        let current = config
            .checks
            .profile(&w.profile.profile_id, &w.profile.profile_revision)?;
        if model::canonical(&json!(current))? != model::canonical(&json!(w.profile))? {
            return Err(Error::new(
                "CHECK_PROFILE_CHANGED",
                "queued profile revision contents changed",
            ));
        }
        Ok(())
    })();
    if let Err(e) = preflight {
        w.preflight_error = Some(json!(e));
        tx.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.preflight_error',json(?2)) WHERE check_id=?1",params![id,model::canonical(&json!(e))?])?;
    }
    let now = model::now_ms()?;
    if w.preflight_error.is_none() {
        tx.execute("UPDATE check_runs SET state='running',resource_claimed_at_ms=?2,started_at_ms=?2 WHERE check_id=?1 AND state='queued'",params![id,now])?;
    }
    tx.execute("UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='queued'",params![w.operation_id,now])?;
    tx.commit()?;
    Ok(Some(w))
}
fn pending(db: &Connection, root: PathBuf) -> Result<Vec<Work>> {
    let mut s =
        db.prepare("SELECT c.check_id FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id WHERE c.state IN ('running','reconciling') OR (c.state='queued' AND o.state IN ('sending','outcome_unknown'))")?;
    let ids = s
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ids.iter().map(|id| work(db, id, root.clone())).collect()
}
fn ready(db: &mut Connection, w: &Work, identity: Value) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let status = describe(&tx, &json!({"check_id":w.check_id}))?;
    if !matches!(status["state"].as_str(), Some("running" | "reconciling")) {
        return Ok(false);
    }
    if !status["process"].is_null() && status["process"] != identity {
        return Err(Error::new(
            "CHECK_OWNER_CHANGED",
            "worker identity cannot be replaced",
        ));
    }
    if status["state"] == "running" && status["process"] == identity {
        let op = operations::get_operation(&tx, &w.operation_id)?;
        if op["state"] == "native_accepted" {
            return Ok(true);
        }
    }
    tx.execute(
        "UPDATE check_runs SET state='running',process_identity_json=?2 WHERE check_id=?1",
        params![w.check_id, model::canonical(&identity)?],
    )?;
    tx.execute("UPDATE operations SET state='native_accepted',updated_at_ms=?2 WHERE operation_id=?1 AND state IN ('sending','outcome_unknown')",params![w.operation_id,model::now_ms()?])?;
    tx.commit()?;
    Ok(true)
}
fn finish(db: &mut Connection, w: &Work, c: Completion) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let status = describe(&tx, &json!({"check_id":w.check_id}))?;
    if !matches!(status["state"].as_str(), Some("running" | "reconciling"))
        && !(status["state"] == "queued" && w.preflight_error.is_some())
    {
        return Ok(());
    }
    if c.check_id != w.check_id
        || c.operation_id != w.operation_id
        || c.token != w.token
        || !c.resource_released
    {
        return Err(Error::conflict("check completion mismatched"));
    }
    artifact(&tx, &c.result)?;
    for out in &c.outputs {
        artifact(&tx, out)?;
    }
    let now = model::now_ms()?;
    tx.execute("UPDATE check_runs SET spec_json=json_set(spec_json,'$.cancellation',json(?2)) WHERE check_id=?1",params![w.check_id,model::canonical(&json!(c.cancellation))?])?;
    tx.execute("UPDATE check_runs SET state=?2,resource_released_at_ms=CASE WHEN resource_claimed_at_ms IS NOT NULL THEN ?3 ELSE NULL END,finished_at_ms=?3,exit_code=?4,result_ref=?5,coverage_json=?6 WHERE check_id=?1",params![w.check_id,c.state,now,c.exit_code,c.result.artifact_id,model::canonical(&c.coverage)?])?;
    let owner:String=tx.query_row("SELECT a.owner_id FROM attempts a JOIN check_runs c ON c.attempt_id=a.attempt_id WHERE c.check_id=?1",[&w.check_id],|r|r.get(0))?;
    let report = json!({"operation_id":w.operation_id,"outcome":"applied","check_id":w.check_id,"state":c.state,"exit_code":c.exit_code,"result_ref":c.result.artifact_id,"recipient":owner,"source_checkout_verified":c.state=="passed","output_refs":c.outputs.iter().map(|o|&o.artifact_id).collect::<Vec<_>>(),"task_accepted":false});
    settle(&tx, &w.operation_id, &report)?;
    tx.execute("UPDATE incidents SET state='resolved',last_seen_at_ms=?2 WHERE state='open' AND dedup_key LIKE ?1", params![format!("check:{}:%", w.check_id), now])?;
    tx.commit()?;
    Ok(())
}
fn incident(db: &Connection, key: &str, error: Error) -> Result<()> {
    // One durable incident, not a log line or model nudge on every scheduler tick.
    let now = model::now_ms()?;
    db.execute("INSERT INTO incidents(incident_id,dedup_key,state,occurrences,details_json,opened_at_ms,last_seen_at_ms) VALUES(?1,?2,'open',1,?3,?4,?4) ON CONFLICT(dedup_key) WHERE state='open' DO NOTHING",params![model::new_id(),key,model::canonical(&json!({"error":error}))?,now])?;
    Ok(())
}
impl Store {
    pub(super) async fn capture_source(
        &self,
        principal: Principal,
        params: Value,
    ) -> Result<Value> {
        let p = principal.clone();
        let config = self.config.clone();
        let receipt = self
            .run(move |db| {
                let p = current_principal(db, p)?;
                p.require_writer()?;
                super::mutate(db, &p, "source.capture", &params, &config)
            })
            .await?;
        let id = model::text(&receipt, "operation_id")?.to_owned();
        let start = id.clone();
        let p = principal.clone();
        if let Some((input, git, identity)) =
            self.run(move |db| begin_source(db, p, &start)).await?
        {
            let root = self.data_dir.clone();
            let op = id.clone();
            let outcome = self
                .file_io(move |files| source::capture(&root, &files, &input, &op, &git, identity))
                .await;
            self.run(move |db| finish_source(db, principal, &id, outcome))
                .await?;
        }
        Ok(receipt)
    }
    pub async fn supervise_checks(self, mut stopping: watch::Receiver<bool>) {
        let mut changed = self.changed.subscribe();
        let mut tick = tokio::time::interval(Duration::from_millis(500));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *stopping.borrow() {
                break;
            }
            let root = self.data_dir.clone();
            if let Ok(items) = self.run(move |db| pending(db, root)).await {
                for w in items {
                    let result = async {
                        if let Some(e) = w.preflight_error.clone() {
                            let failed = w.clone();
                            let c = self
                                .file_io(move |files| worker::failure(&failed, &files, e))
                                .await?;
                            let done = w.clone();
                            return self.run(move |db| finish(db, &done, c)).await;
                        }
                        let scan = w.clone();
                        let complete = self
                            .file_io(move |files| worker::completion(&scan, &files))
                            .await?;
                        if let Some(c) = complete {
                            let done = w.clone();
                            return self.run(move |db| finish(db, &done, c)).await;
                        }
                        let scan = w.clone();
                        match self.file_io(move |_| worker::ready(&scan)).await {
                            Ok(Some(identity)) => {
                                let active = w.clone();
                                if self.run(move |db| ready(db, &active, identity)).await? {
                                    let allow = w.clone();
                                    // Persisted cancellation is delivered before go-ahead when both are pending.
                                    self.file_io(move |_| { worker::deliver_cancel(&allow)?; worker::allow(&allow) }).await?;
                                }
                            }
                            Ok(None) => {},
                            Err(e) if e.code == "CHECK_WORKER_LOST" => {
                                let id = w.check_id.clone();
                                self.run(move |db| { db.execute("UPDATE check_runs SET state='reconciling' WHERE check_id=?1 AND state='running'",[id])?; Ok(()) }).await?;
                                let scan = w.clone();
                                if let Some(c) = self.file_io(move |files| worker::recover(&scan, &files)).await? {
                                    let done = w.clone();
                                    return self.run(move |db| finish(db, &done, c)).await;
                                }
                                return Err(e);
                            }
                            Err(e) => return Err(e),
                        }
                        Ok(())
                    }
                    .await;
                    if let Err(e) = result {
                        if e.code == "CHECK_WORKER_LOST" {
                            let id = w.check_id.clone();
                            let _=self.run(move|db|{db.execute("UPDATE check_runs SET state='reconciling' WHERE check_id=?1 AND state='running'",[id])?;Ok(())}).await;
                        }
                        let key = format!("check:{}:{}", w.check_id, e.code);
                        let _ = self.run(move |db| incident(db, &key, e)).await;
                    }
                }
            }
            let root = self.data_dir.clone();
            let config = self.config.clone();
            match self.run(move |db| next(db, &config, root)).await {
                Ok(Some(w)) => {
                    let launch = w.clone();
                    let error = if let Some(e) = w.preflight_error.clone() {
                        Some(e)
                    } else {
                        match self
                            .file_io(move |_| worker::prepare_and_spawn(&launch))
                            .await
                        {
                            Ok(()) => None,
                            Err(e) if e.code == "CHECK_LAUNCH_UNKNOWN" => {
                                let key = format!("check-launch:{}", w.check_id);
                                let _ = self.run(move |db| incident(db, &key, e)).await;
                                None
                            }
                            Err(e) => Some(json!(e)),
                        }
                    };
                    if let Some(error) = error {
                        let failed = w.clone();
                        if let Ok(c) = self
                            .file_io(move |files| worker::failure(&failed, &files, error))
                            .await
                        {
                            let _ = self.run(move |db| finish(db, &w, c)).await;
                        }
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    let key = format!("check-admission:{}", e.code);
                    let _ = self.run(move |db| incident(db, &key, e)).await;
                }
            }
            tokio::select! {_=stopping.changed()=>{},_=changed.changed()=>{},_=tick.tick()=>{}}
        }
    }
}
