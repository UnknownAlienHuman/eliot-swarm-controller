use crate::{ARTIFACT_ID, EXECUTION_SHAPE};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use swarm_contracts::{
    RuntimeOutcome,
    error::{Error, Result},
    runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext},
};
use swarm_process::{private_permissions, write_private_new};

pub const MAX_RECORD_BYTES: u64 = 1_048_576;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DispatchIdentity {
    pub operation_id: String,
    /// Exact host digest of the retained Operation request, before enrichment.
    pub input_sha256: String,
    pub batch_run_id: String,
    pub requested_model: String,
    pub prompt_sha256: String,
    pub prompt_bytes: usize,
    pub task_snapshot_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRecord {
    schema: u8,
    module_artifact_id: String,
    execution_shape: String,
    identity: DispatchIdentity,
    binding_id: String,
    generation: i64,
    route_sha256: String,
    checksum: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchAdmissionRecord {
    schema: u8,
    module_artifact_id: String,
    operation_id: String,
    receipt: TaskDispatchAdmissionReceipt,
    checksum: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct OutcomeRecord {
    schema: u8,
    module_artifact_id: String,
    operation_id: String,
    outcome_sha256: String,
    outcome: RuntimeOutcome,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AckRecord {
    schema: u8,
    operation_id: String,
    outcome_sha256: String,
    acknowledgement: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultPageRecord {
    schema: u8,
    module_artifact_id: String,
    operation_id: String,
    result_page_sha256: String,
    params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultPageAckRecord {
    schema: u8,
    operation_id: String,
    result_page_sha256: String,
    acknowledgement: Value,
}

#[derive(Debug, Clone)]
pub struct RunStore {
    root: PathBuf,
}

impl RunStore {
    pub fn new_for_profile(module_state: &Path, profile: crate::Profile) -> Result<Self> {
        let state = fs::canonicalize(module_state)?;
        // Each descriptor version owns an immutable journal namespace. In
        // particular, BatchV4 must never reinterpret legacy BatchV3 receipts.
        let root = state.join(format!(
            "command-adapter-runs-v{}",
            profile.artifact_version()
        ));
        match fs::symlink_metadata(&root) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(Error::new(
                    "ADAPTER_STATE_INVALID",
                    "Command run store must be a regular child directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&root)?;
                private_permissions(&root, true)?;
            }
            Err(error) => return Err(error.into()),
        }
        let canonical = fs::canonicalize(&root)?;
        if canonical.parent() != Some(state.as_path()) {
            return Err(Error::new(
                "ADAPTER_STATE_INVALID",
                "Command run store escaped the module owner directory",
            ));
        }
        private_permissions(&canonical, true)?;
        Ok(Self { root: canonical })
    }

    pub fn directory(&self, operation_id: &str) -> Result<PathBuf> {
        let path = self
            .root
            .join(format!("op-{}", digest(operation_id.as_bytes())));
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "operation evidence path is not a regular directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&path)?;
                private_permissions(&path, true)?;
            }
            Err(error) => return Err(error.into()),
        }
        let canonical = fs::canonicalize(&path)?;
        if canonical.parent() != Some(self.root.as_path()) {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "operation evidence escaped the private run store",
            ));
        }
        Ok(canonical)
    }

    pub fn admit(
        &self,
        operation_id: &str,
        identity: Option<&DispatchIdentity>,
        binding_id: &str,
        generation: i64,
        route: &Value,
    ) -> Result<(PathBuf, bool)> {
        let dir = self.directory(operation_id)?;
        let path = dir.join("admission.json");
        if path.exists() {
            let saved: AdmissionRecord = read_json(&path)?;
            if saved.schema != 1
                || saved.module_artifact_id != ARTIFACT_ID
                || saved.execution_shape != EXECUTION_SHAPE
                || saved.identity.operation_id != operation_id
                || saved.binding_id != binding_id
                || saved.generation != generation
                || saved.route_sha256 != digest(canonical(route)?.as_bytes())
                || identity.is_some_and(|expected| !identity_matches(&saved.identity, expected))
                || !admission_checksum_valid(&saved)
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "saved operation admission differs from the current Store command",
                ));
            }
            return Ok((dir, true));
        }
        if fs::read_dir(&dir)?.next().transpose()?.is_some() {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "operation directory contains evidence without its admission marker",
            ));
        }
        let identity = identity.cloned().unwrap_or(DispatchIdentity {
            operation_id: operation_id.to_owned(),
            input_sha256: String::new(),
            batch_run_id: format!("command-batch:{}", &digest(operation_id.as_bytes())[..32]),
            requested_model: String::new(),
            prompt_sha256: String::new(),
            prompt_bytes: 0,
            task_snapshot_sha256: String::new(),
        });
        let mut saved = AdmissionRecord {
            schema: 1,
            module_artifact_id: ARTIFACT_ID.to_owned(),
            execution_shape: EXECUTION_SHAPE.to_owned(),
            identity,
            binding_id: binding_id.to_owned(),
            generation,
            route_sha256: digest(canonical(route)?.as_bytes()),
            checksum: String::new(),
        };
        saved.checksum = admission_checksum(&saved)?;
        write_new_json(&path, &saved)?;
        Ok((dir, false))
    }

    pub fn read_outcome(&self, operation_id: &str) -> Result<Option<(RuntimeOutcome, String)>> {
        let dir = self.directory(operation_id)?;
        let path = dir.join("outcome.json");
        if !path.exists() {
            return Ok(None);
        }
        let saved: OutcomeRecord = read_json(&path)?;
        let outcome_value = serde_json::to_value(&saved.outcome)?;
        let hash = digest(canonical(&outcome_value)?.as_bytes());
        let expected_name = format!("op-{}", digest(operation_id.as_bytes()));
        if saved.schema != 1
            || saved.module_artifact_id != ARTIFACT_ID
            || saved.operation_id != operation_id
            || saved.outcome.operation_id != operation_id
            || saved.outcome_sha256 != hash
            || dir.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str())
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved operation outcome failed its identity or digest check",
            ));
        }
        Ok(Some((saved.outcome, hash)))
    }

    pub fn save_dispatch_admission(
        &self,
        operation_id: &str,
        receipt: &TaskDispatchAdmissionReceipt,
    ) -> Result<()> {
        receipt.validate().map_err(|_| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "dispatch admission receipt is invalid",
            )
        })?;
        if receipt.operation_id != operation_id {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "dispatch admission operation identity differs from its directory",
            ));
        }
        let dir = self.directory(operation_id)?;
        let path = dir.join("dispatch-admission.json");
        if path.exists() {
            let saved: DispatchAdmissionRecord = read_json(&path)?;
            if saved.schema != 1
                || saved.module_artifact_id != ARTIFACT_ID
                || saved.operation_id != operation_id
                || saved.receipt != receipt.clone()
                || !dispatch_admission_checksum_valid(&saved)
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "saved dispatch admission differs from the current command",
                ));
            }
            return Ok(());
        }
        let mut saved = DispatchAdmissionRecord {
            schema: 1,
            module_artifact_id: ARTIFACT_ID.to_owned(),
            operation_id: operation_id.to_owned(),
            receipt: receipt.clone(),
            checksum: String::new(),
        };
        saved.checksum = dispatch_admission_checksum(&saved)?;
        write_new_json(&path, &saved)
    }

    pub fn read_dispatch_admission(
        &self,
        operation_id: &str,
    ) -> Result<Option<TaskDispatchAdmissionReceipt>> {
        let dir = self.directory(operation_id)?;
        let path = dir.join("dispatch-admission.json");
        if !path.exists() {
            return Ok(None);
        }
        let saved: DispatchAdmissionRecord = read_json(&path)?;
        if saved.schema != 1
            || saved.module_artifact_id != ARTIFACT_ID
            || saved.operation_id != operation_id
            || saved.receipt.operation_id != operation_id
            || !dispatch_admission_checksum_valid(&saved)
            || saved.receipt.validate().is_err()
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved dispatch admission failed its identity or digest check",
            ));
        }
        Ok(Some(saved.receipt))
    }

    pub fn save_outcome(&self, outcome: &RuntimeOutcome) -> Result<String> {
        let dir = self.directory(&outcome.operation_id)?;
        let path = dir.join("outcome.json");
        let value = serde_json::to_value(outcome)?;
        let hash = digest(canonical(&value)?.as_bytes());
        if path.exists() {
            let (saved, saved_hash) = self
                .read_outcome(&outcome.operation_id)?
                .ok_or_else(|| Error::new("ADAPTER_EVIDENCE_INVALID", "outcome disappeared"))?;
            let saved_value = serde_json::to_value(saved)?;
            if saved_hash != hash || canonical(&saved_value)? != canonical(&value)? {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "a different outcome is already saved for this operation",
                ));
            }
            return Ok(hash);
        }
        write_new_json(
            &path,
            &OutcomeRecord {
                schema: 1,
                module_artifact_id: ARTIFACT_ID.to_owned(),
                operation_id: outcome.operation_id.clone(),
                outcome_sha256: hash.clone(),
                outcome: serde_json::from_value(value)?,
            },
        )?;
        Ok(hash)
    }

    pub fn acknowledge(
        &self,
        operation_id: &str,
        outcome_sha256: &str,
        acknowledgement: Value,
    ) -> Result<()> {
        if acknowledgement["recorded"] != true {
            return Err(Error::new(
                "OUTCOME_ACK_INVALID",
                "module.outcome did not confirm durable recording",
            ));
        }
        let dir = self.directory(operation_id)?;
        write_replace_json(
            &dir.join("ack.json"),
            &AckRecord {
                schema: 1,
                operation_id: operation_id.to_owned(),
                outcome_sha256: outcome_sha256.to_owned(),
                acknowledgement,
            },
        )
    }

    pub fn outcome_pending(&self, operation_id: &str, outcome_sha256: &str) -> Result<bool> {
        let path = self.directory(operation_id)?.join("ack.json");
        if !path.exists() {
            return Ok(true);
        }
        let ack: AckRecord = read_json(&path)?;
        if ack.schema != 1
            || ack.operation_id != operation_id
            || ack.outcome_sha256 != outcome_sha256
            || ack.acknowledgement["recorded"] != true
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved outcome acknowledgement does not match its receipt",
            ));
        }
        Ok(false)
    }

    pub fn pending_outcomes(&self) -> Result<Vec<(RuntimeOutcome, String)>> {
        let mut dirs = fs::read_dir(&self.root)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        dirs.sort();
        let mut pending = Vec::new();
        for dir in dirs {
            let metadata = fs::symlink_metadata(&dir)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "run store contains a non-directory entry",
                ));
            }
            let path = dir.join("outcome.json");
            if !path.exists() {
                continue;
            }
            let saved: OutcomeRecord = read_json(&path)?;
            let outcome_value = serde_json::to_value(&saved.outcome)?;
            let hash = digest(canonical(&outcome_value)?.as_bytes());
            let expected_name = format!("op-{}", digest(saved.operation_id.as_bytes()));
            if saved.schema != 1
                || saved.module_artifact_id != ARTIFACT_ID
                || saved.operation_id != saved.outcome.operation_id
                || saved.outcome_sha256 != hash
                || dir.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str())
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "pending outcome failed its identity or digest check",
                ));
            }
            if self.outcome_pending(&saved.operation_id, &hash)? {
                pending.push((saved.outcome, hash));
            }
        }
        Ok(pending)
    }

    /// Save the exact authenticated `module.result` request before delivery.
    /// A retry may resend these same bytes only; a changed page conflicts.
    pub fn save_result_page(&self, operation_id: &str, params: &Value) -> Result<String> {
        if params["operation_id"].as_str() != Some(operation_id)
            || params["page"].as_object().is_none()
        {
            return Err(Error::new(
                "ADAPTER_RESULT_INVALID",
                "result page request differs from its exact Operation",
            ));
        }
        let dir = self.directory(operation_id)?;
        let admission: AdmissionRecord = read_json(&dir.join("admission.json"))?;
        if admission.schema != 1
            || admission.module_artifact_id != ARTIFACT_ID
            || admission.identity.operation_id != operation_id
            || !admission_checksum_valid(&admission)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result page has no matching immutable Operation admission",
            ));
        }
        let hash = digest(canonical(params)?.as_bytes());
        let path = dir.join("result-page.json");
        if path.exists() {
            let (saved, saved_hash) = self
                .read_result_page(operation_id)?
                .ok_or_else(|| Error::new("ADAPTER_EVIDENCE_INVALID", "page disappeared"))?;
            if saved_hash != hash || canonical(&saved)? != canonical(params)? {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "a different result page is already saved for this Operation",
                ));
            }
            return Ok(hash);
        }
        write_new_json(
            &path,
            &ResultPageRecord {
                schema: 1,
                module_artifact_id: ARTIFACT_ID.to_owned(),
                operation_id: operation_id.to_owned(),
                result_page_sha256: hash.clone(),
                params: params.clone(),
            },
        )?;
        Ok(hash)
    }

    pub fn read_result_page(&self, operation_id: &str) -> Result<Option<(Value, String)>> {
        let dir = self.directory(operation_id)?;
        let path = dir.join("result-page.json");
        if !path.exists() {
            return Ok(None);
        }
        let saved: ResultPageRecord = read_json(&path)?;
        let hash = digest(canonical(&saved.params)?.as_bytes());
        let expected_name = format!("op-{}", digest(operation_id.as_bytes()));
        if saved.schema != 1
            || saved.module_artifact_id != ARTIFACT_ID
            || saved.operation_id != operation_id
            || saved.params["operation_id"].as_str() != Some(operation_id)
            || saved.result_page_sha256 != hash
            || dir.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str())
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved result page failed its identity or digest check",
            ));
        }
        Ok(Some((saved.params, hash)))
    }

    pub fn acknowledge_result_page(
        &self,
        operation_id: &str,
        result_page_sha256: &str,
        acknowledgement: Value,
    ) -> Result<()> {
        if acknowledgement["recorded"] != true
            || acknowledgement["artifact_ref"]
                .as_str()
                .is_none_or(str::is_empty)
        {
            return Err(Error::new(
                "MODULE_RESULT_ACK_INVALID",
                "module.result did not confirm an immutable artifact reference",
            ));
        }
        let (params, saved_hash) = self
            .read_result_page(operation_id)?
            .ok_or_else(|| Error::new("ADAPTER_EVIDENCE_INVALID", "saved result page missing"))?;
        if saved_hash != result_page_sha256 || params["operation_id"] != operation_id {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result acknowledgement differs from its saved page",
            ));
        }
        let dir = self.directory(operation_id)?;
        write_replace_json(
            &dir.join("result-page-ack.json"),
            &ResultPageAckRecord {
                schema: 1,
                operation_id: operation_id.to_owned(),
                result_page_sha256: result_page_sha256.to_owned(),
                acknowledgement,
            },
        )
    }

    pub fn result_page_pending(&self, operation_id: &str, hash: &str) -> Result<bool> {
        let dir = self.directory(operation_id)?;
        if !dir.join("result-page.json").exists() {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result page has no durable outbox record",
            ));
        }
        let path = dir.join("result-page-ack.json");
        if !path.exists() {
            return Ok(true);
        }
        let ack: ResultPageAckRecord = read_json(&path)?;
        if ack.schema != 1
            || ack.operation_id != operation_id
            || ack.result_page_sha256 != hash
            || ack.acknowledgement["recorded"] != true
            || ack.acknowledgement["artifact_ref"]
                .as_str()
                .is_none_or(str::is_empty)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved result acknowledgement does not match its immutable page",
            ));
        }
        Ok(false)
    }

    pub fn pending_result_pages(&self) -> Result<Vec<(Value, String)>> {
        let mut dirs = fs::read_dir(&self.root)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        dirs.sort();
        let mut pending = Vec::new();
        for dir in dirs {
            let metadata = fs::symlink_metadata(&dir)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "run store contains a non-directory entry",
                ));
            }
            let path = dir.join("result-page.json");
            if !path.exists() {
                continue;
            }
            let saved: ResultPageRecord = read_json(&path)?;
            if saved.params["operation_id"].as_str() != Some(saved.operation_id.as_str()) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "pending result page does not name its exact Operation",
                ));
            }
            let Some((params, hash)) = self.read_result_page(&saved.operation_id)? else {
                continue;
            };
            if self.result_page_pending(&saved.operation_id, &hash)? {
                pending.push((params, hash));
            }
        }
        Ok(pending)
    }

    pub fn save_native_evidence(
        &self,
        operation_id: &str,
        stdout: &[u8],
        stderr: &[u8],
        receipt: &Value,
    ) -> Result<()> {
        let dir = self.directory(operation_id)?;
        let admission: AdmissionRecord = read_json(&dir.join("admission.json"))?;
        if admission.schema != 1
            || admission.module_artifact_id != ARTIFACT_ID
            || admission.execution_shape != EXECUTION_SHAPE
            || admission.identity.operation_id != operation_id
            || !admission_checksum_valid(&admission)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "native evidence has no matching immutable operation admission",
            ));
        }
        let mut durable_receipt = receipt.clone();
        let receipt_fields = durable_receipt.as_object_mut().ok_or_else(|| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "native evidence receipt must be an object",
            )
        })?;
        if receipt_fields
            .get("operation_id")
            .is_some_and(|saved| saved.as_str() != Some(operation_id))
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "native evidence operation differs from its immutable admission",
            ));
        }
        receipt_fields.insert("operation_id".to_owned(), json!(operation_id));
        if !admission.identity.input_sha256.is_empty() {
            if receipt_fields.get("input_sha256").is_some_and(|saved| {
                saved.as_str() != Some(admission.identity.input_sha256.as_str())
            }) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "native evidence input digest differs from its immutable admission",
                ));
            }
            receipt_fields.insert(
                "input_sha256".to_owned(),
                json!(admission.identity.input_sha256),
            );
        }
        write_replace_bytes(&dir.join("stdout.ndjson"), stdout)?;
        write_replace_bytes(&dir.join("stderr.txt"), stderr)?;
        write_replace_json(&dir.join("run.json"), &durable_receipt)
    }

    /// Read only a previously admitted native capture. This never creates an
    /// operation directory or admission record, and rechecks the normalized
    /// dispatch marker when the exact Store request carried dispatch context.
    pub fn read_native_output(
        &self,
        operation_id: &str,
        input_sha256: &str,
        binding_id: &str,
        generation: i64,
        route: &Value,
        expected: &Value,
    ) -> Result<Vec<u8>> {
        let unavailable = || {
            Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "exact Command capture evidence is not retained",
            )
        };
        let dir = self.existing_directory(operation_id)?;
        let admission: AdmissionRecord = read_json(&dir.join("admission.json"))?;
        let route_sha256 = digest(canonical(route)?.as_bytes());
        if admission.schema != 1
            || admission.module_artifact_id != ARTIFACT_ID
            || admission.execution_shape != EXECUTION_SHAPE
            || admission.identity.operation_id != operation_id
            || admission.identity.input_sha256 != input_sha256
            || admission.binding_id != binding_id
            || admission.generation != generation
            || admission.route_sha256 != route_sha256
            || !admission_checksum_valid(&admission)
        {
            return Err(unavailable());
        }

        let dispatch_marker = dir.join("dispatch-admission.json");
        let marker_metadata = fs::symlink_metadata(&dispatch_marker);
        match (
            expected["task_dispatch_context"].is_object(),
            marker_metadata,
        ) {
            (true, Ok(metadata)) if !metadata.file_type().is_symlink() && metadata.is_file() => {
                let saved: DispatchAdmissionRecord = read_json(&dispatch_marker)?;
                if saved.schema != 1
                    || saved.module_artifact_id != ARTIFACT_ID
                    || saved.operation_id != operation_id
                    || saved.receipt.operation_id != operation_id
                    || !dispatch_admission_checksum_valid(&saved)
                    || saved.receipt.validate().is_err()
                    || !dispatch_context_matches_projection(
                        &expected["task_dispatch_context"],
                        &saved.receipt.context(),
                    )
                    || serde_json::to_value(saved.receipt.module_receipt.clone())?
                        != expected["module_receipt"]
                    || saved.receipt.native_payload_sha256 != admission.identity.prompt_sha256
                    || saved.receipt.native_payload_bytes != admission.identity.prompt_bytes as u64
                    || (!expected["dispatch_admission"].is_null()
                        && serde_json::to_value(&saved.receipt)? != expected["dispatch_admission"])
                {
                    return Err(unavailable());
                }
            }
            (true, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(unavailable());
            }
            (false, Err(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                if !expected["dispatch_admission"].is_null() {
                    return Err(unavailable());
                }
            }
            _ => {
                return Err(unavailable());
            }
        }

        let native_output = expected["native_output"]
            .as_str()
            .filter(|value| matches!(*value, "stdout.ndjson" | "stderr.txt"))
            .ok_or_else(unavailable)?;
        let receipt: Value = read_json(&dir.join("run.json"))?;
        let stream = if native_output == "stdout.ndjson" {
            "stdout"
        } else {
            "stderr"
        };
        let limit = if stream == "stdout" {
            16 * 1024 * 1024
        } else {
            256 * 1024
        };
        let bytes = read_bounded_bytes(&dir.join(native_output), limit)?;
        let stream_bytes = expected["stream_bytes"].as_u64().ok_or_else(unavailable)?;
        let stored_bytes = expected["stored_bytes"].as_u64().ok_or_else(unavailable)?;
        let stream_sha256 = expected["stream_sha256"].as_str().ok_or_else(unavailable)?;
        let stored_sha256 = expected["stored_sha256"].as_str().ok_or_else(unavailable)?;
        let truncated = expected["truncated"].as_bool().ok_or_else(unavailable)?;
        let read_error = expected["read_error"].as_bool().ok_or_else(unavailable)?;
        if receipt["operation_id"].as_str() != Some(operation_id)
            || receipt["input_sha256"].as_str() != Some(input_sha256)
            || receipt_capture_fact(&receipt, stream, "bytes", "bytes").and_then(Value::as_u64)
                != Some(stream_bytes)
            || receipt_capture_fact(&receipt, stream, "stored_bytes", "stored_bytes")
                .and_then(Value::as_u64)
                != Some(stored_bytes)
            || receipt_capture_fact(&receipt, stream, "sha256", "sha256").and_then(Value::as_str)
                != Some(stream_sha256)
            || receipt_capture_fact(&receipt, stream, "stored_sha256", "stored_sha256")
                .and_then(Value::as_str)
                != Some(stored_sha256)
            || receipt_capture_fact(&receipt, stream, "truncated", "truncated")
                .and_then(Value::as_bool)
                != Some(truncated)
            || receipt_capture_fact(&receipt, stream, "read_error", "read_error")
                .and_then(Value::as_bool)
                != Some(read_error)
            || receipt
                .get("direct_child")
                .or_else(|| receipt.get("native_child"))
                != Some(&expected["native_child"])
            || bytes.len() as u64 != stored_bytes
            || bytes.len() as u64 > limit as u64
            || digest(&bytes) != stored_sha256
            || stream_bytes < stored_bytes
            || (!truncated && !read_error && stream_bytes != stored_bytes)
            || (!truncated && !read_error && stream_sha256 != stored_sha256)
        {
            return Err(unavailable());
        }
        Ok(bytes)
    }

    fn existing_directory(&self, operation_id: &str) -> Result<PathBuf> {
        let path = self
            .root
            .join(format!("op-{}", digest(operation_id.as_bytes())));
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::new(
                    "BATCH_OUTPUT_UNAVAILABLE",
                    "exact Command capture evidence is not retained",
                )
            } else {
                error.into()
            }
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "operation evidence path is not a regular directory",
            ));
        }
        let canonical = fs::canonicalize(&path)?;
        if canonical.parent() != Some(self.root.as_path()) {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "operation evidence escaped the private run store",
            ));
        }
        Ok(canonical)
    }
}

fn receipt_capture_fact<'a>(
    receipt: &'a Value,
    stream: &str,
    suffix: &str,
    nested_name: &str,
) -> Option<&'a Value> {
    let top_name = format!("{stream}_{suffix}");
    receipt
        .get(top_name.as_str())
        .or_else(|| receipt.get(stream).and_then(|value| value.get(nested_name)))
}

fn admission_checksum(record: &AdmissionRecord) -> Result<String> {
    let value = json!({
        "schema":record.schema,
        "module_artifact_id":record.module_artifact_id.clone(),
        "execution_shape":record.execution_shape.clone(),
        "identity":record.identity.clone(),
        "binding_id":record.binding_id.clone(),
        "generation":record.generation,
        "route_sha256":record.route_sha256.clone()
    });
    Ok(digest(canonical(&value)?.as_bytes()))
}

fn admission_checksum_valid(record: &AdmissionRecord) -> bool {
    admission_checksum(record).is_ok_and(|expected| expected == record.checksum)
}

fn dispatch_admission_checksum(record: &DispatchAdmissionRecord) -> Result<String> {
    let value = json!({
        "schema":record.schema,
        "module_artifact_id":record.module_artifact_id.clone(),
        "operation_id":record.operation_id.clone(),
        "receipt":record.receipt.clone()
    });
    Ok(digest(canonical(&value)?.as_bytes()))
}

fn dispatch_context_matches_projection(projection: &Value, context: &TaskDispatchContext) -> bool {
    projection.as_object().is_some_and(|fields| {
        fields.len() == 11
            && [
                "schema_version",
                "operation_id",
                "binding_id",
                "binding_generation",
                "worker_boot_id",
                "attempt_id",
                "task_id",
                "task_revision",
                "task_snapshot_sha256",
                "source_text_sha256",
                "source_text_bytes",
            ]
            .iter()
            .all(|key| fields.contains_key(*key))
    }) && context.validate().is_ok()
        && projection["schema_version"] == context.schema_version
        && projection["operation_id"] == context.operation_id
        && projection["binding_id"] == context.binding_id
        && projection["binding_generation"] == context.binding_generation
        && (projection["worker_boot_id"].is_null()
            || projection["worker_boot_id"] == context.worker_boot_id)
        && projection["attempt_id"] == context.attempt_id
        && projection["task_id"] == context.task_id
        && projection["task_revision"] == context.task_revision
        && projection["task_snapshot_sha256"] == context.task_snapshot_sha256
        && projection["source_text_sha256"] == context.source_text_sha256
        && projection["source_text_bytes"] == context.source_text_bytes
}

fn dispatch_admission_checksum_valid(record: &DispatchAdmissionRecord) -> bool {
    dispatch_admission_checksum(record).is_ok_and(|expected| expected == record.checksum)
}

fn identity_matches(saved: &DispatchIdentity, expected: &DispatchIdentity) -> bool {
    saved.operation_id == expected.operation_id
        && saved.input_sha256 == expected.input_sha256
        && saved.batch_run_id == expected.batch_run_id
        && saved.requested_model == expected.requested_model
        && saved.prompt_sha256 == expected.prompt_sha256
        && saved.prompt_bytes == expected.prompt_bytes
        && (expected.task_snapshot_sha256.is_empty()
            || saved.task_snapshot_sha256 == expected.task_snapshot_sha256)
}

fn canonical(value: &Value) -> Result<String> {
    fn ordered(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let sorted: std::collections::BTreeMap<_, _> = map
                    .iter()
                    .map(|(key, child)| (key.clone(), ordered(child)))
                    .collect();
                Value::Object(sorted.into_iter().collect())
            }
            Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
            other => other.clone(),
        }
    }
    Ok(serde_json::to_string(&ordered(value))?)
}

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_RECORD_BYTES
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved record is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(MAX_RECORD_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved record exceeds its size limit",
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("ADAPTER_EVIDENCE_INVALID", "saved JSON record is malformed"))
}

fn read_bounded_bytes(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            Error::new(
                "BATCH_OUTPUT_UNAVAILABLE",
                "exact Command capture bytes are not retained",
            )
        } else {
            error.into()
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "native capture is not a bounded regular file",
        ));
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "native capture exceeds its byte limit",
        ));
    }
    Ok(bytes)
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() as u64 > MAX_RECORD_BYTES {
        return Err(Error::new(
            "ADAPTER_RECORD_TOO_LARGE",
            "private operation record exceeds one MiB",
        ));
    }
    write_private_new(path, &bytes)
}

fn write_replace_json(path: &Path, value: &impl Serialize) -> Result<()> {
    write_replace_bytes(path, &serde_json::to_vec(value)?)
}

fn write_replace_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    if bytes.len() as u64 > MAX_RECORD_BYTES.max(16 * 1024 * 1024) {
        return Err(Error::new(
            "ADAPTER_RECORD_TOO_LARGE",
            "private evidence exceeds its storage limit",
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| Error::new("ADAPTER_STATE_INVALID", "evidence path has no parent"))?;
    if path.exists() {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > 16 * 1024 * 1024
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "existing evidence target is not a regular file",
            ));
        }
        let mut existing = Vec::with_capacity(metadata.len() as usize);
        File::open(path)?
            .take(16 * 1024 * 1024 + 1)
            .read_to_end(&mut existing)?;
        if existing.len() > 16 * 1024 * 1024 {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "existing evidence target exceeded its read boundary",
            ));
        }
        if existing == bytes {
            return Ok(());
        }
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "immutable evidence file already contains different bytes",
        ));
    }
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy(),
        uuid::Uuid::new_v4()
    ));
    let result = (|| -> Result<()> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        private_permissions(&temp, false)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, path)?;
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}
