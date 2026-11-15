//! Memory call admission shares the writer and existing global writes namespace.
mod continuity;

use super::*;
use eliot_core::api::inference::*;

fn json_error(_: serde_json::Error) -> Error {
    Error::new("source_integrity", "invalid inference metadata encoding")
}

fn owner(client: &str) -> String {
    format!("memory_inference:{client}")
}

fn replay(
    tx: &Connection,
    id: &str,
    principal: &str,
    method: &str,
    payload: &Value,
) -> Result<Option<Value>> {
    validate_ulid(id)?;
    if command_binding(tx, id)?.is_some() {
        return Err(Error::new("id_conflict", "id is bound to a command"));
    }
    if let Some((p, m, digest, result)) = write_binding(tx, id)? {
        if p != principal || m != method || digest != canonical::digest(payload)? {
            return Err(Error::new(
                "id_conflict",
                "id is bound to a different mutation",
            ));
        }
        return decode_json(&result).map(Some);
    }
    Ok(None)
}

fn write(
    tx: &Connection,
    id: &str,
    principal: &str,
    method: &str,
    payload: &Value,
    result: &Value,
    now: i64,
) -> Result<()> {
    let sequence = if let Some(client) = principal.strip_prefix("memory_inference:") {
        let last: i64 = tx
            .query_row(
                "SELECT last_write_seq FROM inference_clients WHERE client_id=?1",
                [client],
                |r| r.get(0),
            )
            .map_err(sql_error)?;
        let next = last
            .checked_add(1)
            .ok_or_else(|| Error::new("capacity", "inference write sequence exhausted"))?;
        db!(tx.execute(
            "UPDATE inference_clients SET last_write_seq=?2 WHERE client_id=?1",
            params![client, next]
        ));
        Some(next)
    } else {
        None
    };
    db!(tx.execute("INSERT INTO writes(id,principal,method,payload_digest,result,created_at,inference_seq) VALUES (?1,?2,?3,?4,?5,?6,?7)",
        params![id, principal, method, canonical::digest(payload)?, encode_json(result)?, now, sequence]));
    Ok(())
}

fn auxiliary_unbound(tx: &Connection, call: &str, cancel: bool) -> Result<()> {
    let sql = if cancel {
        "SELECT cancel_id FROM inference_calls WHERE id=?1"
    } else {
        "SELECT release_id FROM inference_calls WHERE id=?1"
    };
    let bound: Option<String> = tx.query_row(sql, [call], |r| r.get(0)).map_err(sql_error)?;
    if bound.is_some() {
        return Err(Error::new(
            "already_bound",
            "call already has this auxiliary mutation; reconcile its original write id",
        ));
    }
    Ok(())
}

fn current(tx: &Connection, client: &str, version: i64) -> Result<()> {
    let ok: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM inference_clients WHERE client_id=?1 AND current_version=?2)", params![client,version], |r| r.get(0)).map_err(sql_error)?;
    if !ok {
        return Err(Error::new(
            "not_allowed",
            "new mutations require the current credential",
        ));
    }
    Ok(())
}

fn validate_inference_id_age(tx: &Connection, client: &str, id: &str, now: i64) -> Result<()> {
    inference_id(id)?;
    let expired_through: i64 = tx
        .query_row(
            "SELECT expired_id_through_ms FROM inference_clients WHERE client_id=?1",
            [client],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if ulid_timestamp_ms(id).is_some_and(|timestamp| timestamp <= expired_through) {
        return Err(Error::new(
            "id_expired",
            "mutation id is at or below the client's durable expiry watermark",
        ));
    }
    validate_id_age(id, now)
}

impl Store {
    pub fn inference_controller_id(&self) -> Result<String> {
        self.custody_namespace()
    }

    pub fn inference_token_hash(&self, client: &str, version: i64) -> Result<Option<String>> {
        self.connection.query_row("SELECT CASE WHEN current_version=?2 THEN current_hash WHEN pending_version=?2 THEN pending_hash WHEN previous_version=?2 THEN previous_hash END FROM inference_clients WHERE client_id=?1", params![client,version], |r| r.get(0)).optional().map_err(sql_error).map(Option::flatten)
    }

    pub fn inference_pending_file(&self, client: &str) -> Result<String> {
        self.connection.query_row("SELECT pending_file FROM inference_clients WHERE client_id=?1 AND pending_version IS NOT NULL",[client],|r|r.get(0)).map_err(sql_error)
    }

    pub fn inference_credential_preflight(
        &self,
        method: &str,
        request: &InferenceCredentialRequest,
    ) -> Result<()> {
        self.ensure_writer_healthy()?;
        if method == "inference.client.install" {
            if request.expected_version != 0 {
                return Err(Error::new(
                    "invalid",
                    "install requires expected version zero",
                ));
            }
            let count: i64 = self
                .connection
                .query_row("SELECT COUNT(*) FROM inference_clients", [], |r| r.get(0))
                .map_err(sql_error)?;
            let exists: bool = self
                .connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM inference_clients WHERE client_id=?1)",
                    [&request.client_id],
                    |r| r.get(0),
                )
                .map_err(sql_error)?;
            if exists {
                return Err(Error::new(
                    "version_conflict",
                    "client is already installed",
                ));
            }
            if count >= MAX_INFERENCE_CLIENTS {
                return Err(Error::new("capacity", "inference client capacity reached"));
            }
        } else if method == "inference.client.rotate" {
            let allowed:bool=self.connection.query_row("SELECT EXISTS(SELECT 1 FROM inference_clients c WHERE c.client_id=?1 AND c.current_version=?2 AND c.pending_version IS NULL AND c.previous_version IS NULL AND c.last_admission_seq>0 AND EXISTS(SELECT 1 FROM inference_calls i WHERE i.client_id=c.client_id))",params![request.client_id,request.expected_version],|r|r.get(0)).map_err(sql_error)?;
            if !allowed {
                return Err(Error::new(
                    "version_conflict",
                    "rotation needs an existing call and no pending/previous credential",
                ));
            }
        } else if method == "inference.client.retire" {
            let allowed: bool = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM inference_clients c JOIN inference_calls i ON i.id=c.previous_read_call_id AND i.client_id=c.client_id WHERE c.client_id=?1 AND c.current_version=?2 AND c.previous_version IS NOT NULL AND c.previous_hash IS NOT NULL AND c.previous_file IS NOT NULL AND c.previous_retirement_id IS NULL AND c.previous_restarted=1 AND c.previous_read_call_id IS NOT NULL AND c.previous_release_proven=1 AND i.state='released' AND i.cleanup_complete=1)",
                params![request.client_id, request.expected_version],
                |r| r.get(0),
            ).map_err(sql_error)?;
            if !allowed {
                return Err(Error::new(
                    "not_allowed",
                    "previous credential retirement proof is incomplete",
                ));
            }
        }
        Ok(())
    }

    pub fn inference_write_get(
        &self,
        client: &str,
        id: &str,
        now: i64,
    ) -> Result<InferenceWriteResponse> {
        self.ensure_writer_healthy()?;
        inference_id(id)?;
        if command_binding(&self.connection, id)?.is_some() {
            return Err(Error::new(
                "not_allowed",
                "mutation belongs to another scope",
            ));
        }
        if let Some((p, m, digest, result)) = write_binding(&self.connection, id)? {
            if p != owner(client) {
                return Err(Error::new(
                    "not_allowed",
                    "mutation belongs to another scope",
                ));
            }
            return Ok(InferenceWriteResponse {
                id: id.to_owned(),
                status: InferenceWriteStatus::Retained,
                method: Some(m),
                payload_sha256: Some(digest.iter().map(|b| format!("{b:02x}")).collect()),
                result: Some(serde_json::from_str(&result).map_err(json_error)?),
            });
        }
        let status = match validate_inference_id_age(&self.connection, client, id, now) {
            Ok(()) => InferenceWriteStatus::Absent,
            Err(e) if e.code == "id_expired" => InferenceWriteStatus::HistoryGap,
            Err(e) => return Err(e),
        };
        Ok(InferenceWriteResponse {
            id: id.to_owned(),
            status,
            method: None,
            payload_sha256: None,
            result: None,
        })
    }

    /// Execute all inference mutations with the ordinary writer fault latch.
    fn inference_transaction<T>(
        &mut self,
        f: impl FnOnce(&Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        self.ensure_writer_healthy()?;
        let outcome = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let result = f(&tx)?;
            commit_transaction(tx, &self.write_faulted)?;
            Ok(result)
        })();
        self.fault_on_persistence_error(&outcome);
        outcome
    }

    pub fn inference_credential(
        &mut self,
        method: &str,
        request: &InferenceCredentialRequest,
        hash: &str,
        result: &InferenceCredentialResponse,
        now: i64,
    ) -> Result<Value> {
        request.validate()?;
        let payload = serde_json::to_value(request).map_err(json_error)?;
        let result = serde_json::to_value(result).map_err(json_error)?;
        self.inference_transaction(|tx| {
            if let Some(value)=replay(tx,&request.id,"owner",method,&payload)? { return Ok(value); }
            validate_id_age(&request.id,now)?;
            if method == "inference.client.install" {
                if request.expected_version != 0 { return Err(Error::new("invalid", "install starts at version zero")); }
                let count:i64=tx.query_row("SELECT COUNT(*) FROM inference_clients",[],|r|r.get(0)).map_err(sql_error)?;
                if count >= MAX_INFERENCE_CLIENTS {return Err(Error::new("capacity", "inference client capacity reached"));}
                let exists:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM inference_clients WHERE client_id=?1)",[&request.client_id],|r|r.get(0)).map_err(sql_error)?;
                if exists {return Err(Error::new("version_conflict", "client is already installed"));}
                db!(tx.execute("INSERT INTO inference_clients(client_id,current_version,current_hash,current_file) VALUES (?1,1,?2,?3)",params![request.client_id,hash,result["credential_file"].as_str()]));
            } else if method == "inference.client.rotate" {
                let changed=db!(tx.execute("UPDATE inference_clients SET pending_version=current_version+1,pending_hash=?3,pending_file=?4,rotation_seq=last_admission_seq,pending_proven=0 WHERE client_id=?1 AND current_version=?2 AND pending_version IS NULL AND previous_version IS NULL AND last_admission_seq>0 AND EXISTS(SELECT 1 FROM inference_calls WHERE inference_calls.client_id=inference_clients.client_id)",params![request.client_id,request.expected_version,hash,result["credential_file"].as_str()]));
                if changed != 1 {return Err(Error::new("version_conflict", "rotation is pending or a previous credential remains"));}
            } else if method == "inference.client.promote" {
                let changed=db!(tx.execute("UPDATE inference_clients SET previous_version=current_version,previous_hash=current_hash,previous_file=current_file,current_version=pending_version,current_hash=pending_hash,current_file=pending_file,pending_version=NULL,pending_hash=NULL,pending_file=NULL,previous_retirement_id=NULL,previous_retirement_complete=0,previous_read_call_id=NULL,previous_release_proven=0,previous_restarted=0 WHERE client_id=?1 AND current_version=?2 AND pending_version IS NOT NULL AND pending_proven=1",params![request.client_id,request.expected_version]));
                if changed != 1 {return Err(Error::new("not_allowed", "pending credential must read a pre-rotation call before promotion"));}
            } else if method == "inference.client.retire" {
                let changed = db!(tx.execute("UPDATE inference_clients SET previous_hash=NULL,previous_retirement_id=?3,previous_retirement_complete=0 WHERE client_id=?1 AND current_version=?2 AND previous_version IS NOT NULL AND previous_hash IS NOT NULL AND previous_file IS NOT NULL AND previous_retirement_id IS NULL AND previous_restarted=1 AND previous_read_call_id IS NOT NULL AND previous_release_proven=1 AND EXISTS(SELECT 1 FROM inference_calls i WHERE i.id=inference_clients.previous_read_call_id AND i.client_id=inference_clients.client_id AND i.state='released' AND i.cleanup_complete=1)", params![request.client_id, request.expected_version, request.id]));
                if changed != 1 { return Err(Error::new("not_allowed", "previous credential retirement proof is incomplete")); }
            } else {return Err(Error::new("unsupported", "credential operation is unsupported"));}
            write(tx,&request.id,"owner",method,&payload,&result,now)?;
            Ok(result)
        })
    }

    /// Probe an owner write before touching credential files or config on retry.
    pub fn inference_owner_replay(
        &self,
        method: &str,
        request: &InferenceCredentialRequest,
    ) -> Result<Option<Value>> {
        self.ensure_committed_reads()?;
        replay(
            &self.connection,
            &request.id,
            "owner",
            method,
            &serde_json::to_value(request).map_err(json_error)?,
        )
    }

    /// The fixed daemon-owned previous credential path for the retirement proof.
    pub fn inference_previous_credential(
        &self,
        client: &str,
        current_version: i64,
    ) -> Result<(i64, String)> {
        self.ensure_committed_reads()?;
        self.connection.query_row(
            "SELECT previous_version,previous_file FROM inference_clients WHERE client_id=?1 AND current_version=?2 AND previous_version IS NOT NULL AND previous_hash IS NOT NULL AND previous_file IS NOT NULL",
            params![client, current_version],
            |r| Ok((r.get(0)?, r.get(1)?)),
        ).map_err(sql_error)
    }

    /// Durable retirement intent remains queryable until file deletion is read back.
    pub fn inference_retirement_file(&self, client: &str, id: &str) -> Result<Option<String>> {
        self.ensure_committed_reads()?;
        self.connection.query_row(
            "SELECT previous_file FROM inference_clients WHERE client_id=?1 AND previous_retirement_id=?2 AND previous_retirement_complete=0",
            params![client, id],
            |r| r.get(0),
        ).optional().map_err(sql_error)
    }

    /// Complete local retirement only after the Router verified the file is absent.
    pub fn inference_retirement_complete(&mut self, client: &str, id: &str) -> Result<()> {
        self.inference_transaction(|tx| {
            db!(tx.execute("UPDATE inference_clients SET previous_retirement_complete=1,previous_version=NULL,previous_file=NULL,previous_read_call_id=NULL,previous_release_proven=0,previous_restarted=0 WHERE client_id=?1 AND previous_retirement_id=?2 AND previous_retirement_complete=0 AND previous_hash IS NULL", params![client, id]));
            Ok(())
        })
    }

    pub fn inference_text(
        &mut self,
        client: &str,
        version: i64,
        request: &InferenceTextRequest,
        route: &Value,
        now: i64,
    ) -> Result<(InferenceReceipt, bool)> {
        request.validate()?;
        let mut metadata = serde_json::to_value(request).map_err(json_error)?;
        metadata.as_object_mut().expect("DTO object").remove("text");
        self.inference_transaction(|tx| {
            if let Some(value)=replay(tx,&request.id,&owner(client),"inference.text",&metadata)? {return Ok((serde_json::from_value(value).map_err(json_error)?,false));}
            current(tx,client,version)?;
            validate_inference_id_age(tx,client,&request.id,now)?;
            let count:i64=tx.query_row("SELECT COUNT(*) FROM inference_calls",[],|r|r.get(0)).map_err(sql_error)?;
            if count>=MAX_INFERENCE_CALLS {return Err(Error::new("capacity", "inference call capacity reached; reconcile release and retention"));}
            let seq:i64=tx.query_row("SELECT last_admission_seq FROM inference_clients WHERE client_id=?1",[client],|r|r.get(0)).map_err(sql_error)?;
            let seq=seq.checked_add(1).ok_or_else(||Error::new("capacity","inference sequence exhausted"))?;
            let receipt=InferenceReceipt{call_id:request.id.clone(),admission_seq:seq};
            write(tx,&request.id,&owner(client),"inference.text",&metadata,&serde_json::to_value(&receipt).map_err(json_error)?,now)?;
            db!(tx.execute("UPDATE inference_clients SET last_admission_seq=?2 WHERE client_id=?1",params![client,seq]));
            db!(tx.execute("INSERT INTO inference_calls(id,client_id,admission_seq,state,request,route) VALUES (?1,?2,?3,'queued',?4,?5)",params![request.id,client,seq,encode_json(&metadata)?,encode_json(route)?]));
            Ok((receipt,true))
        })
    }

    pub fn inference_text_replay(
        &self,
        client: &str,
        request: &InferenceTextRequest,
    ) -> Result<Option<InferenceReceipt>> {
        self.ensure_writer_healthy()?;
        let mut payload = serde_json::to_value(request).map_err(json_error)?;
        payload.as_object_mut().expect("DTO object").remove("text");
        replay(
            &self.connection,
            &request.id,
            &owner(client),
            "inference.text",
            &payload,
        )?
        .map(|v| serde_json::from_value(v).map_err(json_error))
        .transpose()
    }

    pub fn inference_get(&self, client: &str, id: &str) -> Result<InferenceCall> {
        self.ensure_committed_reads()?;
        call(&self.connection, client, id)
    }

    /// Record pending-rotation or post-restart previous-retirement proof on an exact own-call read.
    pub fn inference_rotation_read(&mut self, client: &str, version: i64, seq: i64) -> Result<()> {
        self.inference_transaction(|tx| {
            db!(tx.execute("UPDATE inference_clients SET pending_proven=1 WHERE client_id=?1 AND pending_version=?2 AND ?3<=rotation_seq",params![client,version,seq]));
            db!(tx.execute("UPDATE inference_clients SET previous_release_proven=CASE WHEN previous_read_call_id=(SELECT id FROM inference_calls WHERE client_id=?1 AND admission_seq=?3) THEN previous_release_proven ELSE 0 END,previous_read_call_id=(SELECT id FROM inference_calls WHERE client_id=?1 AND admission_seq=?3) WHERE client_id=?1 AND current_version=?2 AND previous_version IS NOT NULL AND previous_restarted=1 AND ?3<=rotation_seq AND EXISTS(SELECT 1 FROM inference_calls WHERE client_id=?1 AND admission_seq=?3)",params![client,version,seq])); Ok(())
        })
    }

    pub fn inference_claim(&mut self, client: &str, id: &str) -> Result<bool> {
        self.inference_transaction(|tx| Ok(db!(tx.execute("UPDATE inference_calls SET state='sending',attempts=1 WHERE id=?1 AND client_id=?2 AND state='queued' AND attempts=0",params![id,client]))==1))
    }

    pub fn inference_finish(
        &mut self,
        client: &str,
        id: &str,
        state: InferenceState,
        result: Option<(&str, usize)>,
    ) -> Result<()> {
        self.inference_transaction(|tx| {
            let state=serde_json::to_value(state).map_err(json_error)?;
            db!(tx.execute("UPDATE inference_calls SET state=?3,result_sha256=?4,result_bytes=?5 WHERE id=?1 AND client_id=?2 AND state IN ('queued','sending')",params![id,client,state.as_str(),result.map(|r|r.0),result.map(|r|r.1 as i64)])); Ok(())
        })
    }

    pub fn inference_success(
        &mut self,
        client: &str,
        id: &str,
        digest: &str,
        bytes: usize,
        session: &str,
    ) -> Result<()> {
        self.inference_transaction(|tx| {
            db!(tx.execute("UPDATE inference_calls SET state='succeeded',result_sha256=?3,result_bytes=?4,native_session_id=?5,native_turns=1 WHERE id=?1 AND client_id=?2 AND state='sending'",params![id,client,digest,bytes as i64,session])); Ok(())
        })
    }

    pub fn inference_cancel(
        &mut self,
        client: &str,
        version: i64,
        request: &InferenceCancelRequest,
        now: i64,
    ) -> Result<Value> {
        request.validate()?;
        let payload = serde_json::to_value(request).map_err(json_error)?;
        self.inference_transaction(|tx| {
            if let Some(value) = replay(
                tx,
                &request.id,
                &owner(client),
                "inference.cancel",
                &payload,
            )? {
                return Ok(value);
            }
            current(tx, client, version)?;
            validate_inference_id_age(tx, client, &request.id, now)?;
            let call = call(tx, client, &request.call_id)?;
            auxiliary_unbound(tx, &request.call_id, true)?;
            let outcome = if call.state == InferenceState::Queued {
                db!(tx.execute(
                    "UPDATE inference_calls SET state='cancelled' WHERE id=?1",
                    [&request.call_id]
                ));
                "cancelled_before_sending"
            } else {
                "too_late"
            };
            let result = json!({"call_id":request.call_id,"outcome":outcome});
            write(
                tx,
                &request.id,
                &owner(client),
                "inference.cancel",
                &payload,
                &result,
                now,
            )?;
            db!(tx.execute(
                "UPDATE inference_calls SET cancel_id=?2 WHERE id=?1",
                params![request.call_id, request.id]
            ));
            Ok(result)
        })
    }

    pub fn inference_release(
        &mut self,
        client: &str,
        version: i64,
        request: &InferenceReleaseRequest,
        now: i64,
    ) -> Result<Value> {
        request.validate()?;
        let payload = serde_json::to_value(request).map_err(json_error)?;
        self.inference_transaction(|tx| {
            if let Some(value)=replay(tx,&request.id,&owner(client),"inference.release",&payload)? {
                db!(tx.execute("UPDATE inference_clients SET previous_release_proven=1 WHERE client_id=?1 AND current_version=?2 AND previous_version IS NOT NULL AND previous_restarted=1 AND previous_read_call_id=?3", params![client,version,request.call_id]));
                return Ok(value);
            }
            current(tx,client,version)?; validate_inference_id_age(tx,client,&request.id,now)?;
            if request.dependency()? == InferenceBackupDependency::ControllerDependent {
                return Err(Error::new("dependency_protected", "Memory backup or unresolved fence still depends on this call"));
            }
            let call=call(tx,client,&request.call_id)?;
            auxiliary_unbound(tx,&request.call_id,false)?;
            let allowed=match request.disposition {
                InferenceDisposition::AbandonForErasure => true,
                InferenceDisposition::MemoryCopyVerified => call.state==InferenceState::Succeeded && request.result_sha256.is_some() && request.result_sha256==call.result_sha256,
                InferenceDisposition::TerminalWithoutResult => matches!(call.state,InferenceState::Failed|InferenceState::Cancelled) && request.result_sha256.is_none(),
            };
            if !allowed {return Err(Error::new("not_allowed", "release disposition does not match exact call state/result"));}
            db!(tx.execute("UPDATE inference_calls SET state='released',result_sha256=NULL,result_bytes=NULL WHERE id=?1",[&request.call_id]));
            let result=json!({"call_id":request.call_id,"outcome":"publication_fenced"});
            write(tx,&request.id,&owner(client),"inference.release",&payload,&result,now)?;
            db!(tx.execute("UPDATE inference_calls SET release_id=?2 WHERE id=?1",params![request.call_id,request.id]));
            db!(tx.execute("UPDATE inference_clients SET previous_release_proven=1 WHERE client_id=?1 AND current_version=?2 AND previous_version IS NOT NULL AND previous_restarted=1 AND previous_read_call_id=?3", params![client,version,request.call_id]));
            Ok(result)
        })
    }

    pub fn inference_cleanup_complete(&mut self, client: &str, id: &str) -> Result<()> {
        self.inference_transaction(|tx| {db!(tx.execute("UPDATE inference_calls SET cleanup_complete=1 WHERE id=?1 AND client_id=?2 AND state='released'",params![id,client]));Ok(())})
    }

    pub fn inference_recover(&mut self) -> Result<()> {
        self.inference_transaction(|tx| {
            db!(tx.execute("UPDATE inference_calls SET state=CASE state WHEN 'queued' THEN 'failed' ELSE 'unknown' END WHERE state IN ('queued','sending')",[]));
            db!(tx.execute("UPDATE inference_clients SET previous_restarted=1 WHERE previous_version IS NOT NULL",[])); Ok(())
        })
    }
}

type CallRow = (
    String,
    i64,
    String,
    u32,
    Option<String>,
    Option<u32>,
    bool,
    Option<String>,
    Option<u32>,
);
fn call(connection: &Connection, client: &str, id: &str) -> Result<InferenceCall> {
    inference_id(id)?;
    let row:Option<CallRow>=connection.query_row("SELECT client_id,admission_seq,state,attempts,result_sha256,result_bytes,cleanup_complete,native_session_id,native_turns FROM inference_calls WHERE id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional().map_err(sql_error)?;
    let Some((actual, seq, state, attempts, digest, bytes, cleanup, session, turns)) = row else {
        return Err(Error::new("not_found", "inference call is absent"));
    };
    if actual != client {
        return Err(Error::new(
            "not_allowed",
            "inference call belongs to another principal",
        ));
    }
    Ok(InferenceCall {
        call_id: id.to_owned(),
        admission_seq: seq,
        state: serde_json::from_value(json!(state)).map_err(json_error)?,
        provider_attempts: attempts,
        native_session_id: session,
        native_turns: turns,
        result_sha256: digest,
        result_bytes: bytes.map(|b| b as usize),
        cleanup_complete: cleanup,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> String {
        Ulid::new().to_string()
    }
    fn now() -> i64 {
        i64::try_from(Ulid::new().timestamp_ms()).unwrap()
    }
    fn setup() -> (Store, String) {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let client = format!("memory:{}", id());
        let request = InferenceCredentialRequest {
            id: id(),
            client_id: client.clone(),
            expected_version: 0,
        };
        let response = InferenceCredentialResponse {
            client_id: client.clone(),
            token_version: 1,
            credential_file: "private.json".to_owned(),
            phase: "active".to_owned(),
        };
        store
            .inference_credential(
                "inference.client.install",
                &request,
                &inference_sha256(b"private-test-secret"),
                &response,
                now(),
            )
            .unwrap();
        (store, client)
    }
    fn text() -> InferenceTextRequest {
        let input = "Summarize this episode: SQLite commit preceded the reply.";
        InferenceTextRequest {
            id: id(),
            profile: INFERENCE_PROFILE.to_owned(),
            origin: "episode:1".to_owned(),
            disclosure: InferenceDisclosure::ProviderAllowed,
            text: input.to_owned(),
            input_sha256: inference_sha256(input.as_bytes()),
            input_bytes: input.len(),
            response_format: InferenceFormat::Text,
            max_result_bytes: 1024,
            deadline_ms: 30000,
        }
    }

    fn id_at(timestamp: i64, tag: u128) -> String {
        Ulid::from(((timestamp as u128) << 80) | tag).to_string()
    }

    fn retention(store: &Store, client: &str, id: String) -> InferenceRetentionRequest {
        let high = store.inference_high_water(client).unwrap();
        InferenceRetentionRequest {
            id,
            observed_call_high_water_seq: high.call_high_water_seq,
            observed_write_high_water_seq: high.write_high_water_seq,
            safe_retain_after_seq: high.call_high_water_seq,
            retain_after_seq: high.call_high_water_seq,
            safe_write_retain_after_seq: high.write_high_water_seq,
            write_retain_after_seq: high.write_high_water_seq,
            limit: MAX_INFERENCE_INVENTORY_PAGE,
        }
    }

    #[test]
    fn pruning_expiry_survives_clock_rollback_without_readmission_and_keeps_replay() {
        let (mut store, client) = setup();
        let start = now();
        let mut pruned = text();
        pruned.id = id_at(start, 1);
        store
            .inference_text(&client, 1, &pruned, &Value::Null, start)
            .unwrap();
        let release = InferenceReleaseRequest {
            id: id_at(start, 2),
            call_id: pruned.id.clone(),
            disposition: InferenceDisposition::AbandonForErasure,
            result_sha256: None,
            no_backup_dependencies: None,
            backup_dependency: Some(InferenceBackupDependency::NoBackupDependencies),
        };
        store
            .inference_release(&client, 1, &release, start)
            .unwrap();
        store
            .inference_cleanup_complete(&client, &pruned.id)
            .unwrap();
        let mut retained = text();
        retained.id = id_at(start, 3);
        let original = store
            .inference_text(&client, 1, &retained, &Value::Null, start)
            .unwrap()
            .0;
        let prune_time = start + RETRY_WINDOW_MS + 1;
        let request = retention(&store, &client, id_at(prune_time, 4));
        let result = store
            .inference_retention_advance(&client, 1, &request, prune_time)
            .unwrap();
        assert_eq!(result["pruned_calls"], 1);
        assert_eq!(result["pruned_writes"], 2);

        let rollback = prune_time - 2;
        assert!(validate_id_age(&pruned.id, rollback).is_ok());
        let high = store.inference_high_water(&client).unwrap();
        for id in [&pruned.id, &release.id, &id_at(start, 5)] {
            assert_eq!(
                store
                    .inference_write_get(&client, id, rollback)
                    .unwrap()
                    .status,
                InferenceWriteStatus::HistoryGap
            );
            let mut retry = pruned.clone();
            retry.id = id.clone();
            assert_eq!(
                store
                    .inference_text(&client, 1, &retry, &Value::Null, rollback)
                    .unwrap_err()
                    .code,
                "id_expired"
            );
        }
        assert_eq!(
            store
                .inference_cancel(
                    &client,
                    1,
                    &InferenceCancelRequest {
                        id: release.id.clone(),
                        call_id: retained.id.clone()
                    },
                    rollback
                )
                .unwrap_err()
                .code,
            "id_expired"
        );
        assert_eq!(
            store
                .inference_release(&client, 1, &release, rollback)
                .unwrap_err()
                .code,
            "id_expired"
        );
        let expired_retention = retention(&store, &client, id_at(start, 6));
        assert_eq!(
            store
                .inference_retention_advance(&client, 1, &expired_retention, rollback)
                .unwrap_err()
                .code,
            "id_expired"
        );
        let replay = store
            .inference_text(&client, 0, &retained, &Value::Null, rollback)
            .unwrap();
        assert_eq!(replay.0.admission_seq, original.admission_seq);
        assert!(!replay.1);
        assert_eq!(
            store
                .inference_write_get(&client, &retained.id, rollback)
                .unwrap()
                .status,
            InferenceWriteStatus::Retained
        );
        assert_eq!(
            store
                .inference_retention_advance(&client, 0, &request, rollback)
                .unwrap(),
            result
        );
        assert_eq!(store.inference_high_water(&client).unwrap(), high);
        assert_eq!(
            store.inference_get(&client, &pruned.id).unwrap_err().code,
            "not_found"
        );
        assert_eq!(
            store
                .inference_get(&client, &retained.id)
                .unwrap()
                .provider_attempts,
            0
        );

        // A later successful retention pass with a lower clock cannot lower expiry.
        let request = retention(&store, &client, id_at(rollback, 7));
        store
            .inference_retention_advance(&client, 1, &request, rollback)
            .unwrap();
        assert_eq!(
            store
                .inference_write_get(&client, &pruned.id, rollback)
                .unwrap()
                .status,
            InferenceWriteStatus::HistoryGap
        );
        let mut boundary = text();
        boundary.id = id_at(start + 1, 8);
        assert!(
            store
                .inference_text(&client, 1, &boundary, &Value::Null, rollback)
                .unwrap()
                .1
        );
    }

    #[test]
    fn retention_receipt_capacity_preserves_replay_cancel_release_and_post_prune_admission() {
        let (mut store, client) = setup();
        let start = now();
        let mut call = text();
        call.id = id_at(start, 3000);
        store
            .inference_text(&client, 1, &call, &Value::Null, start)
            .unwrap();
        store.inference_claim(&client, &call.id).unwrap();
        store
            .inference_finish(&client, &call.id, InferenceState::Unknown, None)
            .unwrap();
        // Fill behind a real unresolved call in one transaction; no native work.
        store
            .inference_transaction(|tx| {
                for tag in 1..MAX_INFERENCE_RETENTION_RECEIPTS {
                    write(
                        tx,
                        &id_at(start, tag as u128),
                        &owner(&client),
                        "inference.retention.advance",
                        &json!({}),
                        &json!({}),
                        start,
                    )?;
                }
                Ok(())
            })
            .unwrap();
        let last = retention(&store, &client, id_at(start, 2000));
        let result = store
            .inference_retention_advance(&client, 1, &last, start)
            .unwrap();
        assert_eq!(result["write_retain_after_seq"], 0);
        let blocked = retention(&store, &client, id_at(start, 2001));
        let high = store.inference_high_water(&client).unwrap();
        assert_eq!(
            store
                .inference_retention_advance(&client, 1, &blocked, start)
                .unwrap_err()
                .code,
            "capacity"
        );
        assert_eq!(store.inference_high_water(&client).unwrap(), high);
        assert_eq!(
            store
                .inference_write_get(&client, &blocked.id, start)
                .unwrap()
                .status,
            InferenceWriteStatus::Absent
        );
        assert_eq!(
            store
                .inference_retention_advance(&client, 0, &last, start + RETRY_WINDOW_MS + 1)
                .unwrap(),
            result
        );
        let count: i64 = store.connection.query_row("SELECT COUNT(*) FROM writes WHERE principal=?1 AND method='inference.retention.advance'", [owner(&client)], |row| row.get(0)).unwrap();
        assert_eq!(count, MAX_INFERENCE_RETENTION_RECEIPTS);
        let cancel = InferenceCancelRequest {
            id: id_at(start, 2002),
            call_id: call.id.clone(),
        };
        assert_eq!(
            store.inference_cancel(&client, 1, &cancel, start).unwrap()["outcome"],
            "too_late"
        );
        let release = InferenceReleaseRequest {
            id: id_at(start, 2003),
            call_id: call.id.clone(),
            disposition: InferenceDisposition::AbandonForErasure,
            result_sha256: None,
            no_backup_dependencies: None,
            backup_dependency: Some(InferenceBackupDependency::NoBackupDependencies),
        };
        let released = store
            .inference_release(&client, 1, &release, start)
            .unwrap();
        assert_eq!(
            store
                .inference_release(&client, 1, &release, start)
                .unwrap(),
            released
        );
        store.inference_cleanup_complete(&client, &call.id).unwrap();
        let later = start + RETRY_WINDOW_MS + 1;
        let request = retention(&store, &client, id_at(later, 2004));
        let result = store
            .inference_retention_advance(&client, 1, &request, later)
            .unwrap();
        assert_eq!(result["pruned_calls"], 1);
        assert_eq!(result["pruned_writes"], MAX_INFERENCE_INVENTORY_PAGE);
        let count: i64 = store.connection.query_row("SELECT COUNT(*) FROM writes WHERE principal=?1 AND method='inference.retention.advance'", [owner(&client)], |row| row.get(0)).unwrap();
        assert!(count < MAX_INFERENCE_RETENTION_RECEIPTS);
    }

    #[test]
    fn replay_conflict_and_namespace_are_atomic_and_content_free() {
        let (mut store, client) = setup();
        let request = text();
        let (receipt, new) = store
            .inference_text(&client, 1, &request, &json!({"native":"private"}), now())
            .unwrap();
        assert!(new);
        assert!(store.inference_claim(&client, &request.id).unwrap());
        assert!(!store.inference_claim(&client, &request.id).unwrap());
        let (again, new) = store
            .inference_text(
                &client,
                1,
                &request,
                &Value::Null,
                now() + RETRY_WINDOW_MS * 2,
            )
            .unwrap();
        assert!(!new);
        assert_eq!(receipt, again);
        let mut changed = request.clone();
        changed.deadline_ms += 1;
        assert_eq!(
            store
                .inference_text(&client, 1, &changed, &Value::Null, now())
                .unwrap_err()
                .code,
            "id_conflict"
        );
        assert_eq!(
            store
                .inference_get(&client, &request.id)
                .unwrap()
                .provider_attempts,
            1
        );
        let metadata: String = store
            .connection
            .query_row(
                "SELECT request FROM inference_calls WHERE id=?1",
                [&request.id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(!metadata.contains(&request.text));
        assert!(
            serde_json::from_str::<Value>(&metadata)
                .unwrap()
                .get("text")
                .is_none()
        );
        let generic = id();
        store
            .record_write(&generic, "owner", "other", &json!({}), &json!({}), now())
            .unwrap();
        changed.id = generic;
        assert_eq!(
            store
                .inference_text(&client, 1, &changed, &Value::Null, now())
                .unwrap_err()
                .code,
            "id_conflict"
        );
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT last_admission_seq FROM inference_clients WHERE client_id=?1",
                    [&client],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
    }

    #[test]
    fn exact_write_lookup_never_admits_and_distinguishes_history_and_foreign_scope() {
        let (mut store, client) = setup();
        let absent = id();
        assert_eq!(
            store
                .inference_write_get(&client, &absent, now())
                .unwrap()
                .status,
            InferenceWriteStatus::Absent
        );
        assert_eq!(
            store
                .inference_write_get(&client, &absent, now() + RETRY_WINDOW_MS + 1)
                .unwrap()
                .status,
            InferenceWriteStatus::HistoryGap
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM inference_calls", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            0
        );
        let request = text();
        store
            .inference_text(&client, 1, &request, &Value::Null, now())
            .unwrap();
        let retained = store
            .inference_write_get(&client, &request.id, now())
            .unwrap();
        assert_eq!(retained.status, InferenceWriteStatus::Retained);
        assert_eq!(
            store
                .inference_write_get("memory:other", &request.id, now())
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            store
                .inference_get("memory:other", &request.id)
                .unwrap_err()
                .code,
            "not_allowed"
        );
    }

    #[test]
    fn restart_and_release_never_reopen_a_possible_send() {
        let (mut store, client) = setup();
        let sent = text();
        let queued = text();
        store
            .inference_text(&client, 1, &sent, &Value::Null, now())
            .unwrap();
        store.inference_claim(&client, &sent.id).unwrap();
        store
            .inference_text(&client, 1, &queued, &Value::Null, now())
            .unwrap();
        store.inference_recover().unwrap();
        assert_eq!(
            store.inference_get(&client, &sent.id).unwrap().state,
            InferenceState::Unknown
        );
        assert_eq!(
            store.inference_get(&client, &queued.id).unwrap().state,
            InferenceState::Failed
        );
        assert!(!store.inference_claim(&client, &sent.id).unwrap());
        let release = InferenceReleaseRequest {
            id: id(),
            call_id: sent.id.clone(),
            disposition: InferenceDisposition::AbandonForErasure,
            result_sha256: None,
            no_backup_dependencies: None,
            backup_dependency: Some(InferenceBackupDependency::NoBackupDependencies),
        };
        let first = store
            .inference_release(&client, 1, &release, now())
            .unwrap();
        store
            .inference_success(
                &client,
                &sent.id,
                &inference_sha256(b"late"),
                4,
                "native-session",
            )
            .unwrap();
        let call = store.inference_get(&client, &sent.id).unwrap();
        assert_eq!(call.state, InferenceState::Released);
        assert!(call.result_sha256.is_none());
        assert!(!call.cleanup_complete);
        assert_eq!(
            store
                .inference_release(&client, 1, &release, now())
                .unwrap(),
            first
        );
        store.inference_cleanup_complete(&client, &sent.id).unwrap();
        assert!(
            store
                .inference_get(&client, &sent.id)
                .unwrap()
                .cleanup_complete
        );
    }

    #[test]
    fn rotation_replays_exact_version_and_requires_pending_pre_rotation_read() {
        let (mut store, client) = setup();
        let call = text();
        store
            .inference_text(&client, 1, &call, &Value::Null, now())
            .unwrap();
        let rotate = InferenceCredentialRequest {
            id: id(),
            client_id: client.clone(),
            expected_version: 1,
        };
        let pending = InferenceCredentialResponse {
            client_id: client.clone(),
            token_version: 2,
            credential_file: "pending.json".to_owned(),
            phase: "pending".to_owned(),
        };
        let hash = inference_sha256(b"pending-secret");
        let first = store
            .inference_credential("inference.client.rotate", &rotate, &hash, &pending, now())
            .unwrap();
        assert_eq!(
            store
                .inference_credential(
                    "inference.client.rotate",
                    &rotate,
                    "other-candidate",
                    &pending,
                    now()
                )
                .unwrap(),
            first
        );
        assert_eq!(store.inference_token_hash(&client, 2).unwrap(), Some(hash));
        let promote = InferenceCredentialRequest {
            id: id(),
            client_id: client.clone(),
            expected_version: 1,
        };
        assert!(
            store
                .inference_credential("inference.client.promote", &promote, "", &pending, now())
                .is_err()
        );
        store.inference_rotation_read(&client, 2, 1).unwrap();
        store
            .inference_credential("inference.client.promote", &promote, "", &pending, now())
            .unwrap();
        assert_eq!(
            store
                .inference_text(&client, 1, &text(), &Value::Null, now())
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert!(
            !store
                .inference_text(&client, 2, &call, &Value::Null, now())
                .unwrap()
                .1
        );
        assert!(
            store
                .inference_text(&client, 2, &text(), &Value::Null, now())
                .unwrap()
                .1
        );
    }

    #[test]
    fn rotation_requires_a_retained_call_even_when_sequence_high_water_is_nonzero() {
        let (mut store, client) = setup();
        store
            .connection
            .execute(
                "UPDATE inference_clients SET last_admission_seq=1 WHERE client_id=?1",
                [&client],
            )
            .unwrap();
        let request = InferenceCredentialRequest {
            id: id(),
            client_id: client.clone(),
            expected_version: 1,
        };
        assert!(
            store
                .inference_credential_preflight("inference.client.rotate", &request)
                .is_err()
        );
        let pending = InferenceCredentialResponse {
            client_id: client.clone(),
            token_version: 2,
            credential_file: "pending.json".to_owned(),
            phase: "pending".to_owned(),
        };
        assert!(
            store
                .inference_credential(
                    "inference.client.rotate",
                    &request,
                    &inference_sha256(b"pending-secret"),
                    &pending,
                    now(),
                )
                .is_err()
        );
        let pending_version: Option<i64> = store
            .connection
            .query_row(
                "SELECT pending_version FROM inference_clients WHERE client_id=?1",
                [&client],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(pending_version, None);
    }

    #[test]
    fn previous_credential_retirement_requires_restart_read_release_and_is_replay_safe() {
        let (mut store, client) = setup();
        let old_call = text();
        store
            .inference_text(&client, 1, &old_call, &Value::Null, now())
            .unwrap();
        let rotate = InferenceCredentialRequest {
            id: id(),
            client_id: client.clone(),
            expected_version: 1,
        };
        let pending = InferenceCredentialResponse {
            client_id: client.clone(),
            token_version: 2,
            credential_file: "pending.json".to_owned(),
            phase: "pending".to_owned(),
        };
        store
            .inference_credential(
                "inference.client.rotate",
                &rotate,
                &inference_sha256(b"pending-secret"),
                &pending,
                now(),
            )
            .unwrap();
        store.inference_rotation_read(&client, 2, 1).unwrap();
        let promote = InferenceCredentialRequest {
            id: id(),
            client_id: client.clone(),
            expected_version: 1,
        };
        store
            .inference_credential("inference.client.promote", &promote, "", &pending, now())
            .unwrap();

        let retire = InferenceCredentialRequest {
            id: id(),
            client_id: client.clone(),
            expected_version: 2,
        };
        assert!(
            store
                .inference_credential_preflight("inference.client.retire", &retire)
                .is_err()
        );
        store.inference_recover().unwrap();
        store.inference_rotation_read(&client, 2, 1).unwrap();
        let release = InferenceReleaseRequest {
            id: id(),
            call_id: old_call.id.clone(),
            disposition: InferenceDisposition::AbandonForErasure,
            result_sha256: None,
            no_backup_dependencies: None,
            backup_dependency: Some(InferenceBackupDependency::NoBackupDependencies),
        };
        store
            .inference_release(&client, 2, &release, now())
            .unwrap();
        assert!(
            store
                .inference_credential_preflight("inference.client.retire", &retire)
                .is_err()
        );
        store
            .inference_cleanup_complete(&client, &old_call.id)
            .unwrap();
        // A later exact read of the same released call must not erase its
        // committed release proof before retirement.
        store.inference_rotation_read(&client, 2, 1).unwrap();
        store
            .inference_credential_preflight("inference.client.retire", &retire)
            .unwrap();

        let result = InferenceCredentialResponse {
            client_id: client.clone(),
            token_version: 2,
            credential_file: "private.json".to_owned(),
            phase: "retired".to_owned(),
        };
        let first = store
            .inference_credential("inference.client.retire", &retire, "", &result, now())
            .unwrap();
        assert_eq!(store.inference_token_hash(&client, 1).unwrap(), None);
        assert_eq!(
            store
                .inference_retirement_file(&client, &retire.id)
                .unwrap(),
            Some("private.json".to_owned())
        );
        assert!(
            store
                .inference_credential_preflight(
                    "inference.client.rotate",
                    &InferenceCredentialRequest {
                        id: id(),
                        client_id: client.clone(),
                        expected_version: 2,
                    }
                )
                .is_err()
        );

        store
            .inference_retirement_complete(&client, &retire.id)
            .unwrap();
        assert_eq!(
            store
                .inference_retirement_file(&client, &retire.id)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .inference_owner_replay("inference.client.retire", &retire)
                .unwrap(),
            Some(first)
        );
        store
            .inference_credential_preflight(
                "inference.client.rotate",
                &InferenceCredentialRequest {
                    id: id(),
                    client_id: client,
                    expected_version: 2,
                },
            )
            .unwrap();
    }

    #[test]
    fn cancellation_and_verified_release_report_only_the_observed_outcome() {
        let (mut store, client) = setup();
        let queued = text();
        store
            .inference_text(&client, 1, &queued, &Value::Null, now())
            .unwrap();
        let cancel = InferenceCancelRequest {
            id: id(),
            call_id: queued.id.clone(),
        };
        assert_eq!(
            store.inference_cancel(&client, 1, &cancel, now()).unwrap()["outcome"],
            "cancelled_before_sending"
        );
        assert!(!store.inference_claim(&client, &queued.id).unwrap());
        let sent = text();
        store
            .inference_text(&client, 1, &sent, &Value::Null, now())
            .unwrap();
        store.inference_claim(&client, &sent.id).unwrap();
        let cancel = InferenceCancelRequest {
            id: id(),
            call_id: sent.id.clone(),
        };
        assert_eq!(
            store.inference_cancel(&client, 1, &cancel, now()).unwrap()["outcome"],
            "too_late"
        );
        store
            .inference_success(&client, &sent.id, &inference_sha256(b"ok"), 2, "session")
            .unwrap();
        let mut release = InferenceReleaseRequest {
            id: id(),
            call_id: sent.id.clone(),
            disposition: InferenceDisposition::MemoryCopyVerified,
            result_sha256: Some(inference_sha256(b"wrong")),
            no_backup_dependencies: None,
            backup_dependency: Some(InferenceBackupDependency::NoBackupDependencies),
        };
        assert!(
            store
                .inference_release(&client, 1, &release, now())
                .is_err()
        );
        release.result_sha256 = Some(inference_sha256(b"ok"));
        release.backup_dependency = Some(InferenceBackupDependency::ControllerDependent);
        assert_eq!(
            store
                .inference_release(&client, 1, &release, now())
                .unwrap_err()
                .code,
            "dependency_protected"
        );
        assert_eq!(
            store.inference_get(&client, &sent.id).unwrap().state,
            InferenceState::Succeeded
        );
        assert_eq!(
            store
                .inference_write_get(&client, &release.id, now())
                .unwrap()
                .status,
            InferenceWriteStatus::Absent
        );
        release.backup_dependency = Some(InferenceBackupDependency::ControllerClosed);
        assert!(store.inference_release(&client, 1, &release, now()).is_ok());
    }

    #[test]
    fn counter_exhaustion_rolls_back_receipt_and_call_together() {
        let (mut store, client) = setup();
        let request = text();
        store
            .connection
            .execute(
                "UPDATE inference_clients SET last_admission_seq=?2 WHERE client_id=?1",
                params![client, i64::MAX],
            )
            .unwrap();
        assert_eq!(
            store
                .inference_text(&client, 1, &request, &Value::Null, now())
                .unwrap_err()
                .code,
            "capacity"
        );
        assert_eq!(
            store
                .inference_write_get(&client, &request.id, now())
                .unwrap()
                .status,
            InferenceWriteStatus::Absent
        );
        assert!(store.inference_get(&client, &request.id).is_err());
    }

    #[test]
    fn original_release_wire_spelling_replays_after_upgrade() {
        let (mut store, client) = setup();
        let request = text();
        store
            .inference_text(&client, 1, &request, &Value::Null, now())
            .unwrap();
        let legacy = json!({"id":id(),"call_id":request.id,"disposition":"abandon_for_erasure","result_sha256":null,"no_backup_dependencies":true});
        let release: InferenceReleaseRequest = serde_json::from_value(legacy.clone()).unwrap();
        assert_eq!(serde_json::to_value(&release).unwrap(), legacy);
        let first = store
            .inference_release(&client, 1, &release, now())
            .unwrap();
        store
            .inference_cleanup_complete(&client, &request.id)
            .unwrap();
        assert_eq!(
            store
                .inference_release(&client, 999, &release, now() + RETRY_WINDOW_MS + 1)
                .unwrap(),
            first
        );
        let mut conflicting = legacy;
        conflicting["backup_dependency"] = json!("controller_closed");
        assert!(
            serde_json::from_value::<InferenceReleaseRequest>(conflicting)
                .unwrap()
                .validate()
                .is_err()
        );
    }

    #[test]
    fn call_state_read_refuses_uncommitted_writer_state() {
        let (mut store, client) = setup();
        let request = text();
        store
            .inference_text(&client, 1, &request, &json!({"native": "private"}), now())
            .unwrap();
        // A failed commit whose rollback also fails leaves the connection
        // outside autocommit with an uncommitted row visible; call state
        // must not be served from that transaction.
        store.connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        assert_eq!(
            store.inference_get(&client, &request.id).unwrap_err().code,
            "unavailable"
        );
        store.connection.execute_batch("ROLLBACK").unwrap();
        let call = store.inference_get(&client, &request.id).unwrap();
        assert_eq!(call.state, InferenceState::Queued);
    }
}
