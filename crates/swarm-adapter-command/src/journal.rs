use crate::{ARTIFACT_ID, EXECUTION_SHAPE};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use swarm_contracts::{
    EffectOutcome, RuntimeOutcome,
    error::{Error, Result},
    module_command::CommandCaptureReleaseEvidence,
    runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext},
};
use swarm_process::{
    private_permissions, remove_private_durable, sync_parent_directory, write_private_new,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeStreamFacts {
    pub bytes: u64,
    pub stored_bytes: u64,
    pub sha256: String,
    pub stored_sha256: String,
    pub truncated: bool,
    pub read_error: bool,
}

#[derive(Debug, Clone)]
pub struct NativeCaptureEvidence {
    pub stdout_facts: NativeStreamFacts,
    pub stderr_facts: NativeStreamFacts,
    pub native_child: Value,
    pub module_receipt: Option<Value>,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CaptureReleaseIntentRecord {
    schema: u8,
    module_artifact_id: String,
    release_operation_id: String,
    release_input_sha256: String,
    target_operation_id: String,
    binding_id: String,
    generation: i64,
    route_sha256: String,
    evidence: CommandCaptureReleaseEvidence,
    evidence_sha256: String,
    checksum: String,
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
                if self.existing_release_directory(operation_id)?.is_some() {
                    return Err(Error::new(
                        "COMMAND_CAPTURE_RELEASED",
                        "Command captures moved to an authorized release tombstone",
                    ));
                }
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
            if let Some(name) = dir.file_name().and_then(|name| name.to_str())
                && let Some(target_id) = name.strip_prefix("releasing-")
            {
                let intent = read_capture_release_intent(&dir, target_id)?;
                if name
                    != format!(
                        "releasing-{}",
                        digest(intent.target_operation_id.as_bytes())
                    )
                {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "release tombstone name differs from its target Operation",
                    ));
                }
                continue;
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
            if let Some(name) = dir.file_name().and_then(|name| name.to_str())
                && let Some(target_id) = name.strip_prefix("releasing-")
            {
                let intent = read_capture_release_intent(&dir, target_id)?;
                if name
                    != format!(
                        "releasing-{}",
                        digest(intent.target_operation_id.as_bytes())
                    )
                {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "release tombstone name differs from its target Operation",
                    ));
                }
                continue;
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
            (false, Err(error))
                if error.kind() == std::io::ErrorKind::NotFound
                    && !expected["dispatch_admission"].is_null() =>
            {
                return Err(unavailable());
            }
            (false, Err(error))
                if error.kind() == std::io::ErrorKind::NotFound
                    && expected["dispatch_admission"].is_null() =>
            {
                // Legacy Command descriptors do not retain normalized
                // dispatch-admission markers. Their exact input, binding,
                // route, outcome and capture digests are still checked below.
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

    /// Recover only the exact previously admitted native receipt and stream
    /// bytes. This never creates a run or replays the native operation.
    pub fn read_native_evidence(
        &self,
        identity: &DispatchIdentity,
        binding_id: &str,
        generation: i64,
        route: &Value,
        dispatch_admission: Option<&TaskDispatchAdmissionReceipt>,
    ) -> Result<Option<NativeCaptureEvidence>> {
        let operation_id = identity.operation_id.as_str();
        let dir = self.existing_directory(operation_id)?;
        let admission: AdmissionRecord = read_json(&dir.join("admission.json"))?;
        let route_sha256 = digest(canonical(route)?.as_bytes());
        if admission.schema != 1
            || admission.module_artifact_id != ARTIFACT_ID
            || admission.execution_shape != EXECUTION_SHAPE
            || admission.identity.operation_id != operation_id
            || !identity_matches(&admission.identity, identity)
            || admission.binding_id != binding_id
            || admission.generation != generation
            || admission.route_sha256 != route_sha256
            || !admission_checksum_valid(&admission)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved native evidence admission differs from the exact retry identity",
            ));
        }

        let saved_dispatch = self.read_dispatch_admission(operation_id)?;
        if saved_dispatch.as_ref() != dispatch_admission {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved dispatch admission differs from the exact retry identity",
            ));
        }
        if let Some(dispatch) = dispatch_admission {
            let context = dispatch.context();
            if dispatch.operation_id != operation_id
                || context.binding_id != binding_id
                || context.binding_generation != generation
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "saved dispatch admission is not bound to the exact retry scope",
                ));
            }
        }

        let receipt_path = dir.join("run.json");
        match fs::symlink_metadata(&receipt_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                for name in ["stdout.ndjson", "stderr.txt"] {
                    match fs::symlink_metadata(dir.join(name)) {
                        Ok(_) => {
                            return Err(Error::new(
                                "ADAPTER_EVIDENCE_INVALID",
                                "native capture bytes exist without their run receipt",
                            ));
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        }

        let receipt: Value = read_json(&receipt_path)?;
        let prompt_bytes = u64::try_from(identity.prompt_bytes).ok();
        if !receipt.is_object()
            || receipt["operation_id"].as_str() != Some(operation_id)
            || receipt["input_sha256"].as_str() != Some(identity.input_sha256.as_str())
            || receipt
                .get("schema")
                .is_some_and(|schema| schema.as_u64() != Some(1))
            || receipt
                .get("module_artifact_id")
                .is_some_and(|artifact| artifact.as_str() != Some(ARTIFACT_ID))
            || receipt
                .get("requested_model")
                .is_some_and(|model| model.as_str() != Some(identity.requested_model.as_str()))
            || receipt
                .get("prompt_sha256")
                .is_some_and(|prompt| prompt.as_str() != Some(identity.prompt_sha256.as_str()))
            || receipt
                .get("prompt_bytes")
                .is_some_and(|bytes| bytes.as_u64() != prompt_bytes)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved native run receipt differs from the admitted request",
            ));
        }

        let stdout_facts = NativeStreamFacts::from_receipt(&receipt, "stdout")?;
        let stderr_facts = NativeStreamFacts::from_receipt(&receipt, "stderr")?;
        let native_child = receipt
            .get("direct_child")
            .or_else(|| receipt.get("native_child"))
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| {
                Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "saved native run receipt has no exact child identity",
                )
            })?;

        let (task_dispatch_context, module_receipt, dispatch_value) =
            if let Some(dispatch) = dispatch_admission {
                (
                    serde_json::to_value(dispatch.context())?,
                    serde_json::to_value(dispatch.module_receipt.clone())?,
                    serde_json::to_value(dispatch)?,
                )
            } else {
                (Value::Null, Value::Null, Value::Null)
            };
        let expected = |native_output: &str, facts: &NativeStreamFacts| {
            json!({
                "task_dispatch_context":task_dispatch_context,
                "module_receipt":module_receipt,
                "dispatch_admission":dispatch_value,
                "native_output":native_output,
                "stream_bytes":facts.bytes,
                "stored_bytes":facts.stored_bytes,
                "stream_sha256":facts.sha256,
                "stored_sha256":facts.stored_sha256,
                "truncated":facts.truncated,
                "read_error":facts.read_error,
                "native_child":native_child
            })
        };
        let _stdout = self.read_native_output(
            operation_id,
            &identity.input_sha256,
            binding_id,
            generation,
            route,
            &expected("stdout.ndjson", &stdout_facts),
        )?;
        let _stderr = self.read_native_output(
            operation_id,
            &identity.input_sha256,
            binding_id,
            generation,
            route,
            &expected("stderr.txt", &stderr_facts),
        )?;
        if read_json::<Value>(&receipt_path)? != receipt {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "native run receipt changed during capture validation",
            ));
        }

        Ok(Some(NativeCaptureEvidence {
            stdout_facts,
            stderr_facts,
            native_child,
            module_receipt: dispatch_admission
                .map(|dispatch| serde_json::to_value(&dispatch.module_receipt))
                .transpose()?,
        }))
    }

    pub fn release_command_captures(
        &self,
        release_operation_id: &str,
        release_input_sha256: &str,
        binding_id: &str,
        generation: i64,
        route: &Value,
        evidence: &CommandCaptureReleaseEvidence,
    ) -> Result<RuntimeOutcome> {
        if !is_sha256(release_input_sha256)
            || release_operation_id.is_empty()
            || binding_id.is_empty()
            || generation <= 0
            || evidence.schema_version != 1
            || evidence.target_operation_id.is_empty()
            || !is_sha256(&evidence.target_input_sha256)
            || !is_sha256(&evidence.target_outcome_sha256)
            || evidence.outputs.len() != 2
        {
            return Err(Error::new(
                "COMMAND_RELEASE_EVIDENCE_INVALID",
                "Store capture release proof is malformed or exceeds its evidence bound",
            ));
        }
        let evidence_canonical = canonical_release_evidence(evidence)?;
        if evidence_canonical.len() as u64 > MAX_RECORD_BYTES {
            return Err(Error::new(
                "COMMAND_RELEASE_EVIDENCE_INVALID",
                "Store capture release proof is malformed or exceeds its evidence bound",
            ));
        }
        let evidence_sha256 = digest(evidence_canonical.as_bytes());
        let mut intent = CaptureReleaseIntentRecord {
            schema: 1,
            module_artifact_id: ARTIFACT_ID.to_owned(),
            release_operation_id: release_operation_id.to_owned(),
            release_input_sha256: release_input_sha256.to_owned(),
            target_operation_id: evidence.target_operation_id.clone(),
            binding_id: binding_id.to_owned(),
            generation,
            route_sha256: digest(canonical(route)?.as_bytes()),
            evidence: evidence.clone(),
            evidence_sha256,
            checksum: String::new(),
        };
        intent.checksum = capture_release_intent_checksum(&intent)?;

        let release_dir = self.existing_directory(release_operation_id)?;
        let release_admission: AdmissionRecord = read_json(&release_dir.join("admission.json"))?;
        if release_admission.schema != 1
            || release_admission.module_artifact_id != ARTIFACT_ID
            || release_admission.execution_shape != EXECUTION_SHAPE
            || release_admission.identity.operation_id != release_operation_id
            || !release_admission.identity.input_sha256.is_empty()
            || release_admission.binding_id != binding_id
            || release_admission.generation != generation
            || release_admission.route_sha256 != digest(canonical(route)?.as_bytes())
            || !admission_checksum_valid(&release_admission)
        {
            return Err(Error::new(
                "COMMAND_RELEASE_OPERATION_INVALID",
                "capture release is not bound to its exact admitted Store Operation",
            ));
        }
        persist_same_release_intent(&release_dir, &intent)?;
        let tombstone = self.release_directory_path(&intent.target_operation_id);
        if let Some(existing) = self.existing_release_directory(&intent.target_operation_id)? {
            let saved = read_capture_release_intent(&existing, &intent.target_operation_id)?;
            if saved != intent {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "Command run already has a different immutable release intent",
                ));
            }
            remove_captured_streams(&existing)?;
            return Ok(capture_release_outcome(&intent));
        }

        let target_dir = self.existing_directory(&intent.target_operation_id)?;
        let admission: AdmissionRecord = read_json(&target_dir.join("admission.json"))?;
        if admission.schema != 1
            || admission.module_artifact_id != ARTIFACT_ID
            || admission.execution_shape != EXECUTION_SHAPE
            || admission.identity.operation_id != intent.target_operation_id
            || admission.identity.input_sha256 != evidence.target_input_sha256
            || admission.binding_id != binding_id
            || admission.generation != generation
            || admission.route_sha256 != intent.route_sha256
            || !admission_checksum_valid(&admission)
        {
            return Err(Error::new(
                "COMMAND_RELEASE_TARGET_INVALID",
                "release proof differs from the exact admitted Command dispatch",
            ));
        }
        let (_, target_outcome_sha256) = self
            .read_outcome(&intent.target_operation_id)?
            .ok_or_else(|| {
                Error::new(
                    "COMMAND_RELEASE_TARGET_UNACKNOWLEDGED",
                    "Command dispatch has no durable outcome readback",
                )
            })?;
        if target_outcome_sha256 != evidence.target_outcome_sha256
            || self.outcome_pending(&intent.target_operation_id, &target_outcome_sha256)?
        {
            return Err(Error::new(
                "COMMAND_RELEASE_TARGET_UNACKNOWLEDGED",
                "Store release proof does not match the acknowledged Command outcome",
            ));
        }
        verify_release_output_pages(self, binding_id, generation, route, evidence)?;

        persist_same_release_intent(&target_dir, &intent)?;
        if tombstone.exists() {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "Command release tombstone appeared before the atomic rename",
            ));
        }
        fs::rename(&target_dir, &tombstone)?;
        sync_parent_directory(&tombstone)?;
        remove_captured_streams(&tombstone)?;
        Ok(capture_release_outcome(&intent))
    }

    /// Complete a previously acknowledged release after a restart that may
    /// have occurred between persisting the host ACK and deleting its tombstone.
    pub fn finalize_acknowledged_releases(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.root)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort();
        for path in entries {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "run store contains a non-UTF-8 entry",
                ));
            };
            if !name.starts_with("releasing-") {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "Command release tombstone is not a regular directory",
                ));
            }
            let canonical_path = fs::canonicalize(&path)?;
            if canonical_path.parent() != Some(self.root.as_path()) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "Command release tombstone escaped the private run store",
                ));
            }
            let target_id = name.strip_prefix("releasing-").unwrap_or_default();
            let intent = read_capture_release_intent(&canonical_path, target_id)?;
            if name
                != format!(
                    "releasing-{}",
                    digest(intent.target_operation_id.as_bytes())
                )
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release tombstone name differs from its target Operation",
                ));
            }
            let release_dir = self.existing_directory(&intent.release_operation_id)?;
            let release_intent =
                read_capture_release_intent(&release_dir, &intent.target_operation_id)?;
            if release_intent != intent {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release Operation and target tombstone intents differ",
                ));
            }
            let Some((outcome, hash)) = self.read_outcome(&intent.release_operation_id)? else {
                continue;
            };
            if !capture_release_outcome_matches(&outcome, &intent) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release Operation outcome differs from its immutable intent",
                ));
            }
            if !self.outcome_pending(&intent.release_operation_id, &hash)? {
                remove_release_tombstone(self, &intent)?;
            }
        }
        Ok(())
    }

    pub fn finalize_release_after_outcome_ack(
        &self,
        release_operation_id: &str,
        outcome_sha256: &str,
    ) -> Result<()> {
        let release_dir = self.existing_directory(release_operation_id)?;
        let intent_path = release_dir.join("release-intent.json");
        if !intent_path.exists() {
            return Ok(());
        }
        let intent: CaptureReleaseIntentRecord = read_json(&intent_path)?;
        validate_capture_release_intent(&intent, &intent.target_operation_id)?;
        let (outcome, saved_hash) = self
            .read_outcome(release_operation_id)?
            .ok_or_else(|| Error::new("ADAPTER_EVIDENCE_INVALID", "release outcome is missing"))?;
        if saved_hash != outcome_sha256
            || !capture_release_outcome_matches(&outcome, &intent)
            || self.outcome_pending(release_operation_id, outcome_sha256)?
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup requires the exact durable host outcome ACK",
            ));
        }
        remove_release_tombstone(self, &intent)
    }

    fn release_directory_path(&self, operation_id: &str) -> PathBuf {
        self.root
            .join(format!("releasing-{}", digest(operation_id.as_bytes())))
    }

    fn existing_release_directory(&self, operation_id: &str) -> Result<Option<PathBuf>> {
        let path = self.release_directory_path(operation_id);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "Command release tombstone is not a regular directory",
            ));
        }
        let canonical_path = fs::canonicalize(&path)?;
        if canonical_path.parent() != Some(self.root.as_path()) {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "Command release tombstone escaped the private run store",
            ));
        }
        Ok(Some(canonical_path))
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

fn verify_release_output_pages(
    store: &RunStore,
    binding_id: &str,
    generation: i64,
    route: &Value,
    evidence: &CommandCaptureReleaseEvidence,
) -> Result<()> {
    let mut seen_outputs = Vec::new();
    for output in &evidence.outputs {
        if !matches!(
            output.native_output.as_str(),
            "stdout.ndjson" | "stderr.txt"
        ) || seen_outputs
            .iter()
            .any(|seen| seen == &output.native_output)
            || output.target_command_output["native_output"] != output.native_output
            || output.target_command_output["operation_id"] != evidence.target_operation_id
            || output.target_command_output["input_sha256"] != evidence.target_input_sha256
            || output.pages.is_empty()
        {
            return Err(Error::new(
                "COMMAND_RELEASE_EVIDENCE_INVALID",
                "release proof does not contain two exact, distinct Command output streams",
            ));
        }
        seen_outputs.push(output.native_output.clone());
        let capture = store.read_native_output(
            &evidence.target_operation_id,
            &evidence.target_input_sha256,
            binding_id,
            generation,
            route,
            &output.target_command_output,
        )?;
        let total = output.target_command_output["stored_bytes"]
            .as_u64()
            .ok_or_else(|| {
                Error::new(
                    "COMMAND_RELEASE_EVIDENCE_INVALID",
                    "Store capture snapshot omitted its stored byte length",
                )
            })?;
        if capture.len() as u64 != total
            || digest(&capture)
                != output.target_command_output["stored_sha256"]
                    .as_str()
                    .unwrap_or_default()
        {
            return Err(Error::new(
                "COMMAND_RELEASE_CAPTURE_INVALID",
                "retained Command capture differs from the exact Store snapshot",
            ));
        }
        let mut covered = 0u64;
        for (index, artifact) in output.pages.iter().enumerate() {
            let end = artifact
                .offset_bytes
                .checked_add(artifact.byte_length)
                .ok_or_else(|| {
                    Error::new(
                        "COMMAND_RELEASE_COVERAGE_INVALID",
                        "Command result artifact page range overflows",
                    )
                })?;
            if artifact.offset_bytes != covered
                || artifact.total_bytes != total
                || artifact.artifact_byte_length != artifact.byte_length
                || artifact.artifact_sha256 != artifact.page_sha256
                || artifact.eof != (end == total)
                || (artifact.byte_length == 0
                    && !(total == 0 && output.pages.len() == 1 && index == 0))
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_COVERAGE_INVALID",
                    "Store artifact pages do not form exact contiguous output coverage",
                ));
            }
            let (params, page_hash) = read_release_result_page(store, artifact)?;
            let page = &params["page"];
            let source = &page["source"];
            let standard_source_matches = source["kind"] == "command_output"
                && source["result_operation_id"] == artifact.result_operation_id
                && source["result_input_sha256"] == artifact.result_input_sha256
                && source["input_operation_id"] == evidence.target_operation_id
                && source["target_input_sha256"] == evidence.target_input_sha256
                && source["target_command_output"] == output.target_command_output;
            let normalized_source_matches = source["schema_id"]
                == swarm_contracts::module_contract::NORMALIZED_RESULT_PAGE_SCHEMA_ID
                && source["result_operation_id"] == artifact.result_operation_id
                && source["result_input_sha256"] == artifact.result_input_sha256
                && source["origin"]["binding_id"] == binding_id
                && source["origin"]["binding_generation"] == generation
                && source["origin"]["target_operation_id"] == evidence.target_operation_id
                && source["origin"]["target_input_sha256"] == evidence.target_input_sha256
                && source["payload_sha256"] == output.target_command_output["stored_sha256"]
                && source["payload_bytes"] == total;
            if params["operation_id"] != artifact.result_operation_id
                || page["offset_bytes"].as_u64() != Some(artifact.offset_bytes)
                || page["byte_length"].as_u64() != Some(artifact.byte_length)
                || page["total_bytes"].as_u64() != Some(total)
                || page["page_sha256"].as_str() != Some(artifact.page_sha256.as_str())
                || page["eof"] != artifact.eof
                || (!standard_source_matches && !normalized_source_matches)
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_ARTIFACT_INVALID",
                    "saved result page differs from the Store artifact coverage proof",
                ));
            }
            let encoded = page["content_base64"].as_str().unwrap_or_default();
            let bytes = STANDARD.decode(encoded).map_err(|_| {
                Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "saved Command result page body is not canonical base64",
                )
            })?;
            if STANDARD.encode(&bytes) != encoded {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "saved Command result page body is not canonical base64",
                ));
            }
            let start = usize::try_from(artifact.offset_bytes).map_err(|_| {
                Error::new(
                    "COMMAND_RELEASE_COVERAGE_INVALID",
                    "Command result page offset exceeds the address space",
                )
            })?;
            let end_index = usize::try_from(end).map_err(|_| {
                Error::new(
                    "COMMAND_RELEASE_COVERAGE_INVALID",
                    "Command result page end exceeds the address space",
                )
            })?;
            if bytes.len() as u64 != artifact.byte_length
                || digest(&bytes) != artifact.page_sha256
                || page_hash.is_empty()
                || start > capture.len()
                || end_index > capture.len()
                || capture[start..end_index] != bytes
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_ARTIFACT_INVALID",
                    "acknowledged Store artifact bytes differ from the retained Command capture",
                ));
            }
            covered = end;
        }
        if covered != total {
            return Err(Error::new(
                "COMMAND_RELEASE_COVERAGE_INVALID",
                "Store artifact coverage does not reach the exact capture end",
            ));
        }
    }
    if seen_outputs.len() != 2
        || !seen_outputs.iter().any(|output| output == "stdout.ndjson")
        || !seen_outputs.iter().any(|output| output == "stderr.txt")
    {
        return Err(Error::new(
            "COMMAND_RELEASE_EVIDENCE_INVALID",
            "Store release proof must cover stdout and stderr",
        ));
    }
    Ok(())
}

fn read_release_result_page(
    store: &RunStore,
    artifact: &swarm_contracts::module_command::CommandCaptureArtifactPage,
) -> Result<(Value, String)> {
    if artifact.result_operation_id.is_empty()
        || !is_sha256(&artifact.result_input_sha256)
        || artifact.artifact_ref.is_empty()
        || !is_sha256(&artifact.artifact_sha256)
        || !is_sha256(&artifact.page_sha256)
    {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "Store artifact page identity or digest is malformed",
        ));
    }
    let dir = store.existing_directory(&artifact.result_operation_id)?;
    let saved: ResultPageRecord = read_json(&dir.join("result-page.json"))?;
    let params_value = serde_json::to_value(&saved.params)?;
    let hash = digest(canonical(&params_value)?.as_bytes());
    let expected_name = format!("op-{}", digest(artifact.result_operation_id.as_bytes()));
    if saved.schema != 1
        || saved.module_artifact_id != ARTIFACT_ID
        || saved.operation_id != artifact.result_operation_id
        || saved.params["operation_id"] != artifact.result_operation_id
        || saved.result_page_sha256 != hash
        || dir.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str())
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved result page failed its immutable identity or digest check",
        ));
    }
    let acknowledgement_path = dir.join("result-page-ack.json");
    let acknowledgement: ResultPageAckRecord = read_json(&acknowledgement_path)?;
    if acknowledgement.schema != 1
        || acknowledgement.operation_id != artifact.result_operation_id
        || acknowledgement.result_page_sha256 != hash
        || acknowledgement.acknowledgement["recorded"] != true
        || acknowledgement.acknowledgement["artifact_ref"] != artifact.artifact_ref
        || hash.is_empty()
        || artifact.artifact_sha256 != artifact.page_sha256
        || artifact.artifact_byte_length != artifact.byte_length
    {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_UNACKNOWLEDGED",
            "Store artifact proof differs from the exact saved result-page ACK",
        ));
    }
    Ok((saved.params, hash))
}

fn persist_same_release_intent(
    directory: &Path,
    expected: &CaptureReleaseIntentRecord,
) -> Result<()> {
    let path = directory.join("release-intent.json");
    if path.exists() {
        let saved: CaptureReleaseIntentRecord = read_json(&path)?;
        validate_capture_release_intent(&saved, &expected.target_operation_id)?;
        if saved != *expected {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "release Operation already has a different immutable intent",
            ));
        }
        return Ok(());
    }
    write_new_json(&path, expected)
}

fn read_capture_release_intent(
    directory: &Path,
    target_operation_id: &str,
) -> Result<CaptureReleaseIntentRecord> {
    let intent: CaptureReleaseIntentRecord = read_json(&directory.join("release-intent.json"))?;
    validate_capture_release_intent(&intent, target_operation_id)?;
    Ok(intent)
}

fn validate_capture_release_intent(
    intent: &CaptureReleaseIntentRecord,
    target_operation_id: &str,
) -> Result<()> {
    if intent.schema != 1
        || intent.module_artifact_id != ARTIFACT_ID
        || intent.release_operation_id.is_empty()
        || !is_sha256(&intent.release_input_sha256)
        || intent.target_operation_id != target_operation_id
        || intent.evidence.target_operation_id != target_operation_id
        || intent.evidence_sha256
            != digest(canonical_release_evidence(&intent.evidence)?.as_bytes())
        || intent.evidence.target_input_sha256.is_empty()
        || !is_sha256(&intent.evidence.target_input_sha256)
        || !is_sha256(&intent.evidence.target_outcome_sha256)
        || intent.binding_id.is_empty()
        || intent.generation <= 0
        || !is_sha256(&intent.route_sha256)
        || intent.checksum != capture_release_intent_checksum(intent)?
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "Command release intent failed its identity or digest check",
        ));
    }
    Ok(())
}

fn capture_release_intent_checksum(intent: &CaptureReleaseIntentRecord) -> Result<String> {
    let value = json!({
        "schema":intent.schema,
        "module_artifact_id":intent.module_artifact_id.clone(),
        "release_operation_id":intent.release_operation_id.clone(),
        "release_input_sha256":intent.release_input_sha256.clone(),
        "target_operation_id":intent.target_operation_id.clone(),
        "binding_id":intent.binding_id.clone(),
        "generation":intent.generation,
        "route_sha256":intent.route_sha256.clone(),
        "evidence":intent.evidence.clone(),
        "evidence_sha256":intent.evidence_sha256.clone(),
    });
    Ok(digest(canonical(&value)?.as_bytes()))
}

fn capture_release_outcome(intent: &CaptureReleaseIntentRecord) -> RuntimeOutcome {
    let artifact_refs: Vec<String> = intent
        .evidence
        .outputs
        .iter()
        .flat_map(|output| output.pages.iter().map(|page| page.artifact_ref.clone()))
        .collect();
    RuntimeOutcome {
        operation_id: intent.release_operation_id.clone(),
        outcome: EffectOutcome::Applied,
        native_scope_key: None,
        native_root_id: None,
        turn_id: None,
        native_input_id: None,
        details: json!({
            "execution_shape":EXECUTION_SHAPE,
            "completion_condition":"command_capture_release_recorded",
            "release_input_sha256":intent.release_input_sha256,
            "target_operation_id":intent.target_operation_id,
            "target_input_sha256":intent.evidence.target_input_sha256,
            "target_outcome_sha256":intent.evidence.target_outcome_sha256,
            "coverage_sha256":intent.evidence_sha256,
            "artifact_refs":artifact_refs,
            "native_capture_released":true,
            "native_replay":false
        }),
    }
}

fn capture_release_outcome_matches(
    outcome: &RuntimeOutcome,
    intent: &CaptureReleaseIntentRecord,
) -> bool {
    let expected = capture_release_outcome(intent);
    outcome.operation_id == expected.operation_id
        && outcome.outcome == expected.outcome
        && outcome.native_scope_key == expected.native_scope_key
        && outcome.native_root_id == expected.native_root_id
        && outcome.turn_id == expected.turn_id
        && outcome.native_input_id == expected.native_input_id
        && outcome.details["execution_shape"] == expected.details["execution_shape"]
        && outcome.details["completion_condition"] == expected.details["completion_condition"]
        && outcome.details["release_input_sha256"] == expected.details["release_input_sha256"]
        && outcome.details["target_operation_id"] == expected.details["target_operation_id"]
        && outcome.details["target_input_sha256"] == expected.details["target_input_sha256"]
        && outcome.details["target_outcome_sha256"] == expected.details["target_outcome_sha256"]
        && outcome.details["coverage_sha256"] == expected.details["coverage_sha256"]
        && outcome.details["artifact_refs"] == expected.details["artifact_refs"]
        && outcome.details["native_capture_released"] == true
        && outcome.details["native_replay"] == false
}

fn remove_captured_streams(directory: &Path) -> Result<()> {
    for name in ["stdout.ndjson", "stderr.txt"] {
        let path = directory.join(name);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "Command capture release target is not a regular file",
                ));
            }
            Ok(_) => {
                remove_private_durable(&path)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    sync_parent_directory(&directory.join("release-intent.json"))?;
    Ok(())
}

fn remove_release_tombstone(store: &RunStore, intent: &CaptureReleaseIntentRecord) -> Result<()> {
    let Some(directory) = store.existing_release_directory(&intent.target_operation_id)? else {
        return Ok(());
    };
    if read_capture_release_intent(&directory, &intent.target_operation_id)? != *intent {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "release tombstone differs from its acknowledged Operation intent",
        ));
    }
    const ALLOWED_FILES: [&str; 8] = [
        "ack.json",
        "admission.json",
        "dispatch-admission.json",
        "outcome.json",
        "release-intent.json",
        "run.json",
        "stderr.txt",
        "stdout.ndjson",
    ];
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || !ALLOWED_FILES.contains(&entry.file_name().to_string_lossy().as_ref())
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release tombstone contains an unexpected file or link",
            ));
        }
    }
    fs::remove_dir_all(&directory)?;
    sync_parent_directory(&directory)?;
    Ok(())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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

impl NativeStreamFacts {
    fn from_receipt(receipt: &Value, stream: &str) -> Result<Self> {
        let invalid = || {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved native capture facts are incomplete or malformed",
            )
        };
        let bytes = receipt_capture_fact(receipt, stream, "bytes", "bytes")
            .and_then(Value::as_u64)
            .ok_or_else(invalid)?;
        let stored_bytes = receipt_capture_fact(receipt, stream, "stored_bytes", "stored_bytes")
            .and_then(Value::as_u64)
            .ok_or_else(invalid)?;
        let sha256 = receipt_capture_fact(receipt, stream, "sha256", "sha256")
            .and_then(Value::as_str)
            .filter(|value| is_sha256(value))
            .ok_or_else(invalid)?
            .to_owned();
        let stored_sha256 = receipt_capture_fact(receipt, stream, "stored_sha256", "stored_sha256")
            .and_then(Value::as_str)
            .filter(|value| is_sha256(value))
            .ok_or_else(invalid)?
            .to_owned();
        let truncated = receipt_capture_fact(receipt, stream, "truncated", "truncated")
            .and_then(Value::as_bool)
            .ok_or_else(invalid)?;
        let read_error = receipt_capture_fact(receipt, stream, "read_error", "read_error")
            .and_then(Value::as_bool)
            .ok_or_else(invalid)?;
        if bytes < stored_bytes
            || truncated != (bytes > stored_bytes)
            || (!truncated && sha256 != stored_sha256)
        {
            return Err(invalid());
        }
        Ok(Self {
            bytes,
            stored_bytes,
            sha256,
            stored_sha256,
            truncated,
            read_error,
        })
    }
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

fn canonical_release_evidence(evidence: &CommandCaptureReleaseEvidence) -> Result<String> {
    // Match Store's typed-evidence -> JSON Value -> canonical JSON digest recipe.
    let value = serde_json::to_value(evidence)?;
    canonical(&value)
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
