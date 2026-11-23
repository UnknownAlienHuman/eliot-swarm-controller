//! Narrow Memory authority and call-owned artifact lifecycle. Vendor protocol is
//! in the Claude plugin; this module owns Store-before-effect and publication.

use super::{MODE_OPEN, Principal, Router, contains_known_secret_including_keys, require_owner};
use crate::{
    common::{now_ms, secure_eq},
    platform::data_dir::{
        ensure_private_file, ensure_private_root, read_private_bytes, replace_private_file,
    },
    scripts::process::{ProcessOutcome, run_owned_process},
};
use eliot_core::{
    Error, Result,
    api::{discovery::decode_params, inference::*},
};
use eliot_plugin::{sync::LockExt, text_inference};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::atomic::Ordering,
    time::Duration,
};
#[expect(
    clippy::disallowed_types,
    reason = "The Memory inference path owns the Claude child after durable admission."
)]
use tokio::process::Command;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CredentialFile {
    controller_id: String,
    client_id: String,
    token_version: i64,
    credential: String,
    operation_id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Route {
    claude_executable: PathBuf,
}

fn scope(principal: &Principal) -> Result<(&str, i64)> {
    match principal {
        Principal::MemoryInference {
            client_id,
            token_version,
        } => Ok((client_id, *token_version)),
        _ => Err(Error::new(
            "not_allowed",
            "Memory inference requires its own credential",
        )),
    }
}
fn encode(value: impl Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "inference DTO could not be encoded"))
}
fn unknown() -> Error {
    Error::new(
        "unknown",
        "private inference artifact could not be reconciled",
    )
}

fn credential_file_name_is_owned(name: &str) -> bool {
    let Some(id) = name
        .strip_prefix("inference-credential-")
        .and_then(|name| name.strip_suffix(".json"))
    else {
        return false;
    };
    Path::new(name).file_name().and_then(|value| value.to_str()) == Some(name)
        && inference_id(id).is_ok()
}

/// Completed/Failed captures are settled only after owned-custody
/// quiescence is established (Windows Job / Unix group). Unknown without
/// quiescence keeps the cleanup/dispatch fence.
fn inference_quiescent(outcome: &ProcessOutcome) -> bool {
    !matches!(
        outcome,
        ProcessOutcome::Unknown {
            quiescent: false,
            ..
        }
    )
}

impl Router {
    fn artifact(&self, id: &str, kind: &str) -> PathBuf {
        self.inner.data_root.join(format!("inference-{id}.{kind}"))
    }

    pub(super) async fn inference_identity(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let _: InferenceIdentityRequest = decode_params(params.clone())?;
        let (client, version) = scope(principal)?;
        encode(InferenceIdentityResponse {
            controller_id: self.inner.store.lock().await.inference_controller_id()?,
            client_id: client.to_owned(),
            principal: "memory_inference".to_owned(),
            token_version: version,
            contract_version: "0.19-M1.2".to_owned(),
        })
    }

    pub(super) async fn inference_credential(
        &self,
        method: &str,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: InferenceCredentialRequest = decode_params(params.clone())?;
        request.validate()?;
        let _operation = self.inner.inference_ops.lock().await;
        let mut store = self.inner.store.lock().await;
        if let Some(value) = store.inference_owner_replay(method, &request)? {
            if method == "inference.client.retire" {
                self.reconcile_credential_retirement(&mut store, &request, &value)?;
                return Ok(value);
            }
            let result: InferenceCredentialResponse =
                serde_json::from_value(value.clone()).map_err(|_| unknown())?;
            self.check_credential_file(&store, &result)?;
            return Ok(value);
        }
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new("unavailable", "daemon admission is closed"));
        }
        store.inference_credential_preflight(method, &request)?;
        let controller = store.inference_controller_id()?;
        if method == "inference.client.retire" {
            let (previous_version, previous_file) = store
                .inference_previous_credential(&request.client_id, request.expected_version)?;
            let previous = InferenceCredentialResponse {
                client_id: request.client_id.clone(),
                token_version: previous_version,
                credential_file: previous_file.clone(),
                phase: "previous".to_owned(),
            };
            // Validate and hash-check the old file before committing the durable
            // fence that removes its hash from the authentication table.
            self.check_credential_file(&store, &previous)?;
            let result = InferenceCredentialResponse {
                client_id: request.client_id.clone(),
                token_version: request.expected_version,
                credential_file: previous_file,
                phase: "retired".to_owned(),
            };
            let value = store.inference_credential(method, &request, "", &result, now_ms())?;
            self.reconcile_credential_retirement(&mut store, &request, &value)?;
            return Ok(value);
        }
        let file_name = if method == "inference.client.promote" {
            store.inference_pending_file(&request.client_id)?
        } else {
            format!("inference-credential-{}.json", request.id)
        };
        if Path::new(&file_name).file_name().and_then(|s| s.to_str()) != Some(file_name.as_str()) {
            return Err(unknown());
        }
        let path = self.inner.data_root.join(&file_name);
        let record: CredentialFile = if path.try_exists().map_err(|_| unknown())? {
            serde_json::from_slice(&read_private_bytes(&path, 4096)?).map_err(|_| unknown())?
        } else {
            if method == "inference.client.promote" {
                return Err(unknown());
            }
            let record = CredentialFile {
                controller_id: controller.clone(),
                client_id: request.client_id.clone(),
                token_version: request.expected_version + 1,
                credential: crate::scripts::auth::random_256_bit_token(),
                operation_id: request.id.clone(),
            };
            replace_private_file(
                &self.inner.data_root,
                &file_name,
                &serde_json::to_vec(&record).map_err(|_| unknown())?,
            )?;
            // Exact publication readback is required before credential installation.
            let bytes = read_private_bytes(&path, 4096)?;
            if bytes != serde_json::to_vec(&record).map_err(|_| unknown())? {
                return Err(unknown());
            }
            record
        };
        if record.controller_id != controller
            || record.client_id != request.client_id
            || record.token_version != request.expected_version + 1
            || (method != "inference.client.promote" && record.operation_id != request.id)
        {
            return Err(Error::new(
                "id_conflict",
                "credential publication has a different binding",
            ));
        }
        self.inner
            .secrets
            .lock_recover()
            .insert(record.credential.clone());
        let response = InferenceCredentialResponse {
            client_id: request.client_id.clone(),
            token_version: record.token_version,
            credential_file: file_name,
            phase: if method == "inference.client.rotate" {
                "pending"
            } else {
                "active"
            }
            .to_owned(),
        };
        store.inference_credential(
            method,
            &request,
            &sha256_hex(record.credential.as_bytes()),
            &response,
            now_ms(),
        )
    }

    /// Reconcile a committed retirement intent by deleting only its fixed,
    /// daemon-owned credential basename and recording verified absence.
    fn reconcile_credential_retirement(
        &self,
        store: &mut eliot_store::Store,
        request: &InferenceCredentialRequest,
        value: &Value,
    ) -> Result<()> {
        let result: InferenceCredentialResponse =
            serde_json::from_value(value.clone()).map_err(|_| unknown())?;
        if result.client_id != request.client_id
            || result.token_version != request.expected_version
            || result.phase != "retired"
            || !credential_file_name_is_owned(&result.credential_file)
        {
            return Err(unknown());
        }
        let retirement_file = store.inference_retirement_file(&request.client_id, &request.id)?;
        if retirement_file
            .as_deref()
            .is_some_and(|file_name| file_name != result.credential_file)
        {
            return Err(unknown());
        }
        let path = self.inner.data_root.join(&result.credential_file);
        match std::fs::symlink_metadata(&path) {
            Ok(_) => {
                if retirement_file.is_some() {
                    // Atomically overwrite the bearer with an empty private file
                    // and sync its parent before unlinking. A crash can then only
                    // leave an unusable empty marker, never restore the old token.
                    replace_private_file(&self.inner.data_root, &result.credential_file, &[])
                        .map_err(|_| unknown())?;
                }
                std::fs::remove_file(&path).map_err(|_| unknown())?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(unknown()),
        }
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if retirement_file.is_some() {
                    store.inference_retirement_complete(&request.client_id, &request.id)?;
                }
                Ok(())
            }
            _ => Err(unknown()),
        }
    }

    fn check_credential_file(
        &self,
        store: &eliot_store::Store,
        result: &InferenceCredentialResponse,
    ) -> Result<()> {
        if Path::new(&result.credential_file)
            .file_name()
            .and_then(|s| s.to_str())
            != Some(&result.credential_file)
        {
            return Err(unknown());
        }
        let record: CredentialFile = serde_json::from_slice(
            &read_private_bytes(&self.inner.data_root.join(&result.credential_file), 4096)
                .map_err(|_| unknown())?,
        )
        .map_err(|_| unknown())?;
        if record.controller_id != store.inference_controller_id()?
            || record.client_id != result.client_id
            || record.token_version != result.token_version
            || inference_id(&record.operation_id).is_err()
            || result.credential_file
                != format!("inference-credential-{}.json", record.operation_id)
            || !store
                .inference_token_hash(&record.client_id, record.token_version)?
                .is_some_and(|hash| secure_eq(&hash, &sha256_hex(record.credential.as_bytes())))
        {
            return Err(unknown());
        }
        self.inner.secrets.lock_recover().insert(record.credential);
        Ok(())
    }

    pub(super) async fn inference_text(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let (client, version) = scope(principal)?;
        let request: InferenceTextRequest = decode_params(params.clone())?;
        request.validate()?;
        if contains_known_secret_including_keys(params, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "inference request contains a known credential",
            ));
        }
        let _operation = self.inner.inference_ops.lock().await;
        // Retained binding lookup precedes shutdown, capacity and route/config reads.
        if let Some(receipt) = self
            .inner
            .store
            .lock()
            .await
            .inference_text_replay(client, &request)?
        {
            return encode(receipt);
        }
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new("unavailable", "daemon admission is closed"));
        }
        if !cfg!(windows) {
            return Err(Error::new(
                "unsupported",
                "M1 inference is qualified only on Windows",
            ));
        }
        let adapter = text_inference::configured()?;
        let route: Route = serde_json::from_slice(&read_private_bytes(
            &self.inner.data_root.join("inference.json"),
            4096,
        )?)
        .map_err(|_| Error::new("invalid", "invalid inference.json route"))?;
        if !route.claude_executable.is_absolute() || !route.claude_executable.is_file() {
            return Err(Error::new(
                "invalid",
                "inference requires the installed absolute Claude executable",
            ));
        }
        let native_home = self.inner.data_root.join("inference-native");
        ensure_private_root(&native_home)?;
        // Native OAuth refresh replaces this owned file, inheriting our private
        // directory ACL. Re-establish protected file permissions before reading;
        // the existing helper rejects reparse points and foreign ownership.
        ensure_private_file(&native_home.join(".credentials.json"))?;
        // The owner provisions a private native login here. Never mutate the
        // owner's global Claude settings or distribute their identity to a tool.
        let native: Value = serde_json::from_slice(&read_private_bytes(
            &native_home.join(".credentials.json"),
            128 * 1024,
        )?)
        .map_err(|_| Error::new("not_allowed", "private Claude login is unavailable"))?;
        for key in ["accessToken", "refreshToken"] {
            if let Some(secret) = native
                .get("claudeAiOauth")
                .and_then(|v| v.get(key))
                .and_then(Value::as_str)
            {
                self.inner.secrets.lock_recover().insert(secret.to_owned());
            }
        }
        if contains_known_secret_including_keys(params, &self.inner.secrets) {
            return Err(Error::new(
                "secret_in_payload",
                "inference request contains a known credential",
            ));
        }
        let frozen = encode(&route)?;
        let mut store = self.inner.store.lock().await;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new("unavailable", "daemon admission is closed"));
        }
        let (receipt, is_new) =
            store.inference_text(client, version, &request, &frozen, now_ms())?;
        if is_new {
            // Call-owned private input exists completely before `sending` or spawn.
            let name = format!("inference-{}.input", request.id);
            if replace_private_file(&self.inner.data_root, &name, request.text.as_bytes()).is_err()
                || read_private_bytes(&self.artifact(&request.id, "input"), MAX_INFERENCE_INPUT)
                    .ok()
                    .as_deref()
                    != Some(request.text.as_bytes())
            {
                store.inference_finish(client, &request.id, InferenceState::Failed, None)?;
            } else if self.inference_dispatch_fenced() {
                // Never dispatch native work while a previous process family
                // exit is unproven; the admitted call stays queued as debt
                // until restart recovery fails it.
            } else {
                let router = self.clone();
                let client = client.to_owned();
                let id = request.id.clone();
                let mut workers = self.inner.inference_workers.lock_recover();
                workers.retain(|_, task| !task.is_finished());
                let deadline =
                    tokio::time::Instant::now() + Duration::from_millis(request.deadline_ms);
                workers.insert(
                    id,
                    tokio::spawn(async move {
                        router
                            .run_inference(client, request, route, adapter, deadline)
                            .await;
                    }),
                );
            }
        }
        encode(receipt)
    }

    async fn run_inference(
        &self,
        client: String,
        request: InferenceTextRequest,
        route: Route,
        adapter: text_inference::TextInferenceAdapter,
        deadline: tokio::time::Instant,
    ) {
        let single = tokio::time::timeout_at(deadline, self.inner.inference_runner.lock()).await;
        // Quiescence is carried through settlement: only a proven process
        // family exit (Windows Job / Unix group) may release the cleanup
        // fence below.
        let quiescent = if let Ok(_single) = single {
            match self
                .run_inference_inner(&client, &request, route, adapter, deadline)
                .await
            {
                Ok(quiescent) => quiescent,
                Err(_) => {
                    let _ = self.inner.store.lock().await.inference_finish(
                        &client,
                        &request.id,
                        InferenceState::Unknown,
                        None,
                    );
                    // The result is uncertain, but quiescence is a recorded
                    // fact: only run_inference_inner, from the actual
                    // ProcessOutcome, ever marks a call unquiescent.
                    !self.is_inference_unquiescent(&request.id)
                }
            }
        } else {
            let _ = self.inner.store.lock().await.inference_finish(
                &client,
                &request.id,
                InferenceState::Failed,
                None,
            );
            true
        };
        // Released calls may never publish a late result. Cleanup is retriable by
        // the same release id; failures leave explicit debt in the call view.
        // Cleanup stays fenced until the process family exit is established.
        let _operation = self.inner.inference_ops.lock().await;
        if quiescent
            && self
                .inner
                .store
                .lock()
                .await
                .inference_get(&client, &request.id)
                .is_ok_and(|c| c.state == InferenceState::Released)
        {
            let _ = self.cleanup_inference(&client, &request.id).await;
        }
        self.signal_progress();
    }

    #[expect(
        clippy::disallowed_types,
        reason = "This effect runner starts the admitted Claude inference command."
    )]
    async fn run_inference_inner(
        &self,
        client: &str,
        request: &InferenceTextRequest,
        route: Route,
        adapter: text_inference::TextInferenceAdapter,
        deadline: tokio::time::Instant,
    ) -> Result<bool> {
        let cancellation = self.inner.run_cancellation.subscribe();
        {
            let _operation = self.inner.inference_ops.lock().await;
            if *cancellation.borrow() || tokio::time::Instant::now() >= deadline {
                self.inner.store.lock().await.inference_finish(
                    client,
                    &request.id,
                    InferenceState::Cancelled,
                    None,
                )?;
                return Ok(true);
            }
            // Verify exact immutable input before setting the at-most-once fence.
            let input =
                read_private_bytes(&self.artifact(&request.id, "input"), MAX_INFERENCE_INPUT)?;
            if input != request.text.as_bytes() {
                return Err(unknown());
            }
            if !self
                .inner
                .store
                .lock()
                .await
                .inference_claim(client, &request.id)?
            {
                return Ok(true);
            }
        }
        let cwd = self.inner.data_root.join("inference-work");
        ensure_private_root(&cwd)?;
        #[expect(
            clippy::disallowed_methods,
            reason = "This statement constructs a process owned by this module."
        )]
        let mut command = Command::new(route.claude_executable);
        command
            .args((adapter.arguments)())
            .current_dir(&cwd)
            .env_clear()
            .envs((adapter.environment)(
                &self.inner.data_root.join("inference-native"),
            ));
        for key in [
            "SYSTEMROOT",
            "WINDIR",
            "PATH",
            "TEMP",
            "TMP",
            "USERPROFILE",
            "HOME",
            "APPDATA",
            "LOCALAPPDATA",
        ] {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        let policy = self.secret_policy_snapshot();
        let outcome = run_owned_process(
            command,
            request.text.as_bytes().to_vec(),
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            policy,
            cancellation.clone(),
        )
        .await;
        let quiescent = inference_quiescent(&outcome);
        if quiescent {
            self.clear_inference_quiescence(&request.id);
        } else {
            self.mark_inference_unquiescent(&request.id);
        }
        let text = match outcome {
            ProcessOutcome::Completed(capture)
                if capture.status.success() && capture.stdout_complete =>
            {
                (adapter.result)(&capture.stdout, request.max_result_bytes).ok()
            }
            _ => None,
        };
        let _operation = self.inner.inference_ops.lock().await;
        if self
            .inner
            .store
            .lock()
            .await
            .inference_get(client, &request.id)?
            .state
            == InferenceState::Released
        {
            return Ok(quiescent);
        }
        if let Some(output) = text {
            let text = output.text;
            let name = format!("inference-{}.result", request.id);
            replace_private_file(&self.inner.data_root, &name, text.as_bytes())?;
            let exact =
                read_private_bytes(&self.artifact(&request.id, "result"), MAX_INFERENCE_RESULT)?;
            if exact != text.as_bytes() {
                return Err(unknown());
            }
            self.inner.store.lock().await.inference_success(
                client,
                &request.id,
                &sha256_hex(&exact),
                exact.len(),
                &output.session_id,
            )?;
        } else {
            self.inner.store.lock().await.inference_finish(
                client,
                &request.id,
                InferenceState::Unknown,
                None,
            )?;
        }
        Ok(quiescent)
    }

    pub(super) async fn inference_get(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: InferenceGetRequest = decode_params(params.clone())?;
        request.validate()?;
        let (client, version) = scope(principal)?;
        let mut store = self.inner.store.lock().await;
        let call = store.inference_get(client, &request.call_id)?;
        store.inference_rotation_read(client, version, call.admission_seq)?;
        encode(call)
    }
    pub(super) async fn inference_write_get(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: InferenceWriteGetRequest = decode_params(params.clone())?;
        request.validate()?;
        let (client, _) = scope(principal)?;
        encode(
            self.inner
                .store
                .lock()
                .await
                .inference_write_get(client, &request.id, now_ms())?,
        )
    }
    pub(super) async fn inference_high_water(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let _: InferenceHighWaterRequest = decode_params(params.clone())?;
        let (client, _) = scope(principal)?;
        encode(self.inner.store.lock().await.inference_high_water(client)?)
    }

    pub(super) async fn inference_list(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: InferenceListRequest = decode_params(params.clone())?;
        request.validate()?;
        let (client, _) = scope(principal)?;
        encode(
            self.inner
                .store
                .lock()
                .await
                .inference_list(client, &request)?,
        )
    }

    pub(super) async fn inference_retention(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: InferenceRetentionRequest = decode_params(params.clone())?;
        request.validate()?;
        let (client, version) = scope(principal)?;
        let _operation = self.inner.inference_ops.lock().await;
        self.inner.store.lock().await.inference_retention_advance(
            client,
            version,
            &request,
            now_ms(),
        )
    }

    pub(super) async fn inference_result_read(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: InferenceResultRequest = decode_params(params.clone())?;
        request.validate()?;
        let (client, _) = scope(principal)?;
        let _operation = self.inner.inference_ops.lock().await;
        let call = self
            .inner
            .store
            .lock()
            .await
            .inference_get(client, &request.call_id)?;
        if call.state != InferenceState::Succeeded
            || call.result_sha256.as_deref() != Some(&request.result_sha256)
        {
            return Err(Error::new(
                "not_allowed",
                "exact retained successful result required",
            ));
        }
        let bytes = read_private_bytes(
            &self.artifact(&request.call_id, "result"),
            MAX_INFERENCE_RESULT,
        )?;
        if call.result_bytes != Some(bytes.len()) || sha256_hex(&bytes) != request.result_sha256 {
            return Err(unknown());
        }
        if request.offset > bytes.len() {
            return Err(Error::new("invalid", "range begins beyond result"));
        }
        let end = (request.offset + request.length).min(bytes.len());
        encode(InferenceResultResponse {
            call_id: request.call_id,
            result_sha256: request.result_sha256,
            total_bytes: bytes.len(),
            offset: request.offset,
            bytes: bytes[request.offset..end].to_vec(),
            eof: end == bytes.len(),
        })
    }
    pub(super) async fn inference_cancel(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: InferenceCancelRequest = decode_params(params.clone())?;
        request.validate()?;
        let (client, version) = scope(principal)?;
        let _operation = self.inner.inference_ops.lock().await;
        self.inner
            .store
            .lock()
            .await
            .inference_cancel(client, version, &request, now_ms())
    }
    pub(super) async fn inference_release(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: InferenceReleaseRequest = decode_params(params.clone())?;
        request.validate()?;
        let (client, version) = scope(principal)?;
        let _operation = self.inner.inference_ops.lock().await;
        let result =
            self.inner
                .store
                .lock()
                .await
                .inference_release(client, version, &request, now_ms())?;
        let active = self
            .inner
            .inference_workers
            .lock_recover()
            .get(&request.call_id)
            .is_some_and(|w| !w.is_finished());
        // Cleanup stays fenced until the process family exit is established;
        // an unproven family keeps its artifacts and cleanup debt.
        if !active && !self.is_inference_unquiescent(&request.call_id) {
            let _ = self.cleanup_inference(client, &request.call_id).await;
        }
        Ok(result)
    }
    fn mark_inference_unquiescent(&self, id: &str) {
        self.inner
            .inference_unquiescent
            .lock_recover()
            .insert(id.to_owned());
        crate::logging::record(
            &self.secret_policy_snapshot(),
            "daemon.inference_quiescence_unproven",
            &[("call", id)],
            None,
        );
    }
    fn clear_inference_quiescence(&self, id: &str) {
        self.inner.inference_unquiescent.lock_recover().remove(id);
    }
    fn is_inference_unquiescent(&self, id: &str) -> bool {
        self.inner.inference_unquiescent.lock_recover().contains(id)
    }
    fn inference_dispatch_fenced(&self) -> bool {
        !self.inner.inference_unquiescent.lock_recover().is_empty()
    }
    async fn cleanup_inference(&self, client: &str, id: &str) -> Result<()> {
        // Never mark cleanup complete on uncertainty: an unproven process
        // family may still hold native custody of these artifacts.
        if self.is_inference_unquiescent(id) {
            return Err(unknown());
        }
        for kind in ["input", "result"] {
            let path = self.artifact(id, kind);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(unknown()),
            }
            if path.try_exists().map_err(|_| unknown())? {
                return Err(unknown());
            }
        }
        self.inner
            .store
            .lock()
            .await
            .inference_cleanup_complete(client, id)
    }
    pub(super) fn inference_workers_active(&self) -> bool {
        self.inner
            .inference_workers
            .lock_recover()
            .values()
            .any(|w| !w.is_finished())
    }
    pub(super) fn run_workers_active(&self) -> bool {
        self.inner
            .run_workers
            .lock_recover()
            .values()
            .any(|worker| !worker.task.is_finished())
    }
    pub(super) async fn stop_inference_workers(&self) {
        self.inner.run_cancellation.send_replace(true);
        let workers = std::mem::take(&mut *self.inner.inference_workers.lock_recover());
        for (_, worker) in workers {
            let _ = worker.await;
        }
    }
}
