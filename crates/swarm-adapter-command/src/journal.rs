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
    module_command::{
        CommandCaptureArtifactPage, CommandCaptureOutputCoverage, CommandCaptureReleaseEvidence,
    },
    runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext},
};
use swarm_process::{
    private_permissions, remove_private_durable, sync_parent_directory, write_private_new,
};

pub const MAX_RECORD_BYTES: u64 = 1_048_576;

const RELEASE_OPERATION_CLEANUP_PREFIX: &str = "release-cleanup-";
const RESULT_PAGE_CLEANUP_PREFIX: &str = "result-page-cleanup-";
const RESULT_PAGE_CLEANUP_ACK_PREFIX: &str = "result-page-ack-cleanup-";
const RELEASE_OPERATION_FILES: [&str; 5] = [
    "ack.json",
    "admission.json",
    "outcome.json",
    "release-intent.json",
    "run.json",
];
const RELEASE_TOMBSTONE_FILES: [&str; 8] = [
    "ack.json",
    "admission.json",
    "dispatch-admission.json",
    "outcome.json",
    "release-intent.json",
    "run.json",
    "stderr.txt",
    "stdout.ndjson",
];

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
struct ResultPageCleanupMarker {
    directory: PathBuf,
    directory_name: String,
    operation_digest: String,
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
                && name.starts_with("releasing-")
            {
                read_release_tombstone_intent(&dir, name)?;
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
                && name.starts_with("releasing-")
            {
                read_release_tombstone_intent(&dir, name)?;
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
            // The rename is the irreversible ownership transition, so the
            // original directory must already be gone. Retrying while both
            // the target directory and its tombstone exist is the ambiguous
            // state the release contract forbids resolving automatically.
            require_original_release_path_absent(&self.root, &intent.target_operation_id)?;
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
        require_original_release_path_absent(&self.root, &intent.target_operation_id)?;
        remove_captured_streams(&tombstone)?;
        Ok(capture_release_outcome(&intent))
    }

    /// Complete a previously acknowledged release after a restart that may
    /// have occurred during cleanup. The release Operation remains the durable
    /// cleanup anchor until the tombstone and every linked result page are gone.
    pub fn finalize_acknowledged_releases(&self) -> Result<()> {
        let result_page_cleanup_markers = read_result_page_cleanup_markers(self)?;
        let mut entries = fs::read_dir(&self.root)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        entries.sort();
        let mut release_operations = Vec::new();
        let mut cleanup_markers = Vec::new();
        let mut tombstones = Vec::new();
        for path in entries {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "run store contains a non-UTF-8 entry",
                ));
            };
            if !name.starts_with("releasing-")
                && !name.starts_with("op-")
                && !name.starts_with(RELEASE_OPERATION_CLEANUP_PREFIX)
                && !name.starts_with(RESULT_PAGE_CLEANUP_PREFIX)
            {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "run store contains a non-directory entry",
                ));
            }
            let canonical_path = fs::canonicalize(&path)?;
            if canonical_path.parent() != Some(self.root.as_path()) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "run store operation directory escaped the private run store",
                ));
            }
            if name.starts_with("releasing-") {
                tombstones.push((canonical_path, name.to_owned()));
            } else if name.starts_with("op-")
                && let Some(intent) = read_release_operation_intent(self, &canonical_path, name)?
            {
                release_operations.push(intent);
            } else if name.starts_with(RESULT_PAGE_CLEANUP_PREFIX) {
                // Parsed and path-checked by read_result_page_cleanup_markers.
            } else if !name.starts_with("op-") {
                let intent = read_release_operation_cleanup_marker(self, &canonical_path, name)?;
                cleanup_markers.push((canonical_path, name.to_owned(), intent));
            }
        }

        validate_result_page_cleanup_marker_owners(
            self,
            &result_page_cleanup_markers,
            &release_operations,
        )?;

        for (marker_directory, marker_name, marker_intent) in &cleanup_markers {
            let Some(marker_intent) = marker_intent else {
                continue;
            };
            if release_operations
                .iter()
                .any(|intent| intent.release_operation_id == marker_intent.release_operation_id)
                || cleanup_markers
                    .iter()
                    .any(|(other_directory, _, other_intent)| {
                        other_directory != marker_directory
                            && other_intent.as_ref().is_some_and(|other| {
                                other.release_operation_id == marker_intent.release_operation_id
                            })
                    })
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release cleanup marker is ambiguous with another release Operation",
                ));
            }

            let tombstone_name = format!(
                "releasing-{}",
                digest(marker_intent.target_operation_id.as_bytes())
            );
            if tombstones.iter().any(|(_, name)| name == &tombstone_name) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "release cleanup marker coexists with its target tombstone",
                ));
            }
            if marker_name
                != &format!(
                    "{RELEASE_OPERATION_CLEANUP_PREFIX}{}",
                    digest(marker_intent.release_operation_id.as_bytes())
                )
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release cleanup marker name differs from its immutable Operation",
                ));
            }
        }

        for (tombstone_directory, tombstone_name) in &tombstones {
            let saved_intent =
                read_release_tombstone_intent_if_present(tombstone_directory, tombstone_name)?;
            let target_digest = tombstone_name.strip_prefix("releasing-").ok_or_else(|| {
                Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release tombstone name has no releasing prefix",
                )
            })?;
            if !is_sha256(target_digest) {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release tombstone name does not contain a canonical target digest",
                ));
            }
            let mut matching_intents: Vec<_> = release_operations
                .iter()
                .filter(|intent| digest(intent.target_operation_id.as_bytes()) == target_digest)
                .collect();
            if let Some(saved_intent) = saved_intent.as_ref() {
                matching_intents.retain(|intent| *intent == saved_intent);
            }
            if matching_intents.len() != 1 {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release tombstone has no unique matching admitted release Operation",
                ));
            }
            let intent = matching_intents[0];
            validate_release_tombstone_subset(tombstone_directory, tombstone_name, intent)?;
            if saved_intent.is_none() && !release_operation_outcome_acknowledged(self, intent)? {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "intentless release tombstone residue lacks an exact durable release ACK",
                ));
            }
        }

        for intent in release_operations {
            if release_operation_outcome_acknowledged(self, &intent)? {
                finalize_acknowledged_release(self, &intent, &result_page_cleanup_markers)?;
            }
        }
        for (directory, name, intent) in cleanup_markers {
            if let Some(intent) = intent {
                finish_release_operation_cleanup(self, &directory, &intent)?;
            } else {
                remove_empty_release_operation_cleanup_marker(self, &directory, &name)?;
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
        match fs::symlink_metadata(&intent_path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release Operation intent is not a regular file",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let intent: CaptureReleaseIntentRecord = read_json(&intent_path)?;
        validate_capture_release_intent(&intent, &intent.target_operation_id)?;
        if intent.release_operation_id != release_operation_id {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release Operation intent names a different Operation",
            ));
        }
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
        validate_release_operation_directory(self, &intent)?;
        let result_page_cleanup_markers = read_result_page_cleanup_markers(self)?;
        finalize_acknowledged_release(self, &intent, &result_page_cleanup_markers)
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
    let mut seen_result_operation_ids = Vec::new();
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
        let mut unique_range_count = 0usize;
        let mut previous_range: Option<(&CommandCaptureArtifactPage, Vec<u8>)> = None;
        for artifact in &output.pages {
            if seen_result_operation_ids.contains(&artifact.result_operation_id) {
                return Err(Error::new(
                    "COMMAND_RELEASE_EVIDENCE_INVALID",
                    "one result Operation appears more than once in release evidence",
                ));
            }
            seen_result_operation_ids.push(artifact.result_operation_id.clone());
            let end = artifact
                .offset_bytes
                .checked_add(artifact.byte_length)
                .ok_or_else(|| {
                    Error::new(
                        "COMMAND_RELEASE_COVERAGE_INVALID",
                        "Command result artifact page range overflows",
                    )
                })?;
            if artifact.total_bytes != total
                || artifact.artifact_byte_length != artifact.byte_length
                || artifact.artifact_sha256 != artifact.page_sha256
                || artifact.eof != (end == total)
            {
                return Err(Error::new(
                    "COMMAND_RELEASE_COVERAGE_INVALID",
                    "Store artifact page metadata differs from its exact output range",
                ));
            }
            let (params, page_hash) = read_release_result_page(store, artifact)?;
            validate_release_page_linkage(
                evidence, output, artifact, &params, binding_id, generation,
            )?;
            let page = &params["page"];
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
            let duplicate_range = if let Some((previous, previous_bytes)) = &previous_range {
                if artifact.offset_bytes == previous.offset_bytes {
                    if artifact.byte_length != previous.byte_length
                        || artifact.total_bytes != previous.total_bytes
                        || artifact.page_sha256 != previous.page_sha256
                        || artifact.artifact_sha256 != previous.artifact_sha256
                        || artifact.eof != previous.eof
                        || bytes.as_slice() != previous_bytes.as_slice()
                    {
                        return Err(Error::new(
                            "COMMAND_RELEASE_COVERAGE_INVALID",
                            "overlapping Command result pages disagree on their exact range",
                        ));
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            };
            if !duplicate_range {
                if artifact.offset_bytes != covered
                    || (artifact.byte_length == 0 && !(total == 0 && unique_range_count == 0))
                {
                    return Err(Error::new(
                        "COMMAND_RELEASE_COVERAGE_INVALID",
                        "Store artifact pages do not form exact contiguous output coverage",
                    ));
                }
                covered = end;
                unique_range_count += 1;
                previous_range = Some((artifact, bytes));
            }
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

fn validate_release_page_linkage(
    evidence: &CommandCaptureReleaseEvidence,
    output: &CommandCaptureOutputCoverage,
    artifact: &CommandCaptureArtifactPage,
    params: &Value,
    binding_id: &str,
    generation: i64,
) -> Result<()> {
    let page = &params["page"];
    let source = &page["source"];
    let total = output.target_command_output["stored_bytes"]
        .as_u64()
        .ok_or_else(|| {
            Error::new(
                "COMMAND_RELEASE_EVIDENCE_INVALID",
                "Store capture snapshot omitted its stored byte length",
            )
        })?;
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
    if output.target_command_output["native_output"] != output.native_output
        || output.target_command_output["operation_id"] != evidence.target_operation_id
        || output.target_command_output["input_sha256"] != evidence.target_input_sha256
        || params["operation_id"] != artifact.result_operation_id
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
    Ok(())
}

fn read_release_result_page(
    store: &RunStore,
    artifact: &CommandCaptureArtifactPage,
) -> Result<(Value, String)> {
    let (directory, params, hash) = read_saved_release_result_page(store, artifact)?;
    validate_release_result_page_acknowledgement(&directory, artifact, &hash)?;
    Ok((params, hash))
}

fn read_saved_release_result_page(
    store: &RunStore,
    artifact: &CommandCaptureArtifactPage,
) -> Result<(PathBuf, Value, String)> {
    validate_release_result_page_descriptor(artifact)?;
    let directory = store.existing_directory(&artifact.result_operation_id)?;
    let (params, hash) = read_release_result_page_record(&directory, artifact)?;
    let expected_name = format!("op-{}", digest(artifact.result_operation_id.as_bytes()));
    if directory.file_name().and_then(|name| name.to_str()) != Some(expected_name.as_str()) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved result page is outside its immutable Operation directory",
        ));
    }
    Ok((directory, params, hash))
}

fn validate_release_result_page_descriptor(artifact: &CommandCaptureArtifactPage) -> Result<()> {
    if artifact.result_operation_id.is_empty()
        || !is_sha256(&artifact.result_input_sha256)
        || artifact.artifact_ref.is_empty()
        || !is_sha256(&artifact.artifact_sha256)
        || !is_sha256(&artifact.page_sha256)
        || artifact.artifact_sha256 != artifact.page_sha256
        || artifact.artifact_byte_length != artifact.byte_length
    {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_INVALID",
            "Store artifact page identity or digest is malformed",
        ));
    }
    Ok(())
}

fn read_release_result_page_record(
    directory: &Path,
    artifact: &CommandCaptureArtifactPage,
) -> Result<(Value, String)> {
    let saved: ResultPageRecord = read_json(&directory.join("result-page.json"))?;
    let params = serde_json::to_value(&saved.params)?;
    let hash = digest(canonical(&params)?.as_bytes());
    if saved.schema != 1
        || saved.module_artifact_id != ARTIFACT_ID
        || saved.operation_id != artifact.result_operation_id
        || saved.params["operation_id"] != artifact.result_operation_id
        || !is_sha256(&saved.result_page_sha256)
        || saved.result_page_sha256 != hash
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved result page failed its immutable identity or digest check",
        ));
    }
    Ok((params, hash))
}

fn validate_release_result_page_body(
    intent: &CaptureReleaseIntentRecord,
    output: &CommandCaptureOutputCoverage,
    artifact: &CommandCaptureArtifactPage,
    params: &Value,
    hash: &str,
) -> Result<()> {
    validate_release_page_linkage(
        &intent.evidence,
        output,
        artifact,
        params,
        &intent.binding_id,
        intent.generation,
    )?;
    let encoded = params["page"]["content_base64"]
        .as_str()
        .unwrap_or_default();
    let bytes = STANDARD.decode(encoded).map_err(|_| {
        Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved result page body is not canonical base64",
        )
    })?;
    if STANDARD.encode(&bytes) != encoded
        || bytes.len() as u64 != artifact.byte_length
        || digest(&bytes) != artifact.page_sha256
        || !is_sha256(hash)
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "saved result page bytes differ from the exact release artifact",
        ));
    }
    Ok(())
}

fn validate_release_result_page_acknowledgement(
    directory: &Path,
    artifact: &CommandCaptureArtifactPage,
    hash: &str,
) -> Result<()> {
    validate_release_result_page_acknowledgement_at(
        &directory.join("result-page-ack.json"),
        artifact,
        hash,
    )
}

fn validate_release_result_page_acknowledgement_at(
    acknowledgement_path: &Path,
    artifact: &CommandCaptureArtifactPage,
    hash: &str,
) -> Result<()> {
    let acknowledgement: ResultPageAckRecord = read_json(acknowledgement_path)?;
    if acknowledgement.schema != 1
        || acknowledgement.operation_id != artifact.result_operation_id
        || acknowledgement.result_page_sha256 != hash
        || acknowledgement.acknowledgement["recorded"] != true
        || acknowledgement.acknowledgement["artifact_ref"] != artifact.artifact_ref
        || !is_sha256(hash)
        || artifact.artifact_sha256 != artifact.page_sha256
        || artifact.artifact_byte_length != artifact.byte_length
    {
        return Err(Error::new(
            "COMMAND_RELEASE_ARTIFACT_UNACKNOWLEDGED",
            "Store artifact proof differs from the exact saved result-page ACK",
        ));
    }
    Ok(())
}

fn validate_release_result_page_admission(
    directory: &Path,
    intent: &CaptureReleaseIntentRecord,
    artifact: &CommandCaptureArtifactPage,
) -> Result<()> {
    let admission: AdmissionRecord = read_json(&directory.join("admission.json"))?;
    if admission.schema != 1
        || admission.module_artifact_id != ARTIFACT_ID
        || admission.execution_shape != EXECUTION_SHAPE
        || admission.identity.operation_id != artifact.result_operation_id
        || admission.binding_id != intent.binding_id
        || admission.generation != intent.generation
        || admission.route_sha256 != intent.route_sha256
        || !admission_checksum_valid(&admission)
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result page admission differs from the exact release binding and route",
        ));
    }
    Ok(())
}

#[derive(Debug)]
enum ResultPageAckFile {
    Original(PathBuf),
    Cleanup { path: PathBuf, page_sha256: String },
}

impl ResultPageAckFile {
    fn path(&self) -> &Path {
        match self {
            Self::Original(path) | Self::Cleanup { path, .. } => path,
        }
    }
}

#[derive(Debug, Default)]
struct ResultPageCleanupEntries {
    admission: bool,
    page: bool,
    acknowledgement: Option<ResultPageAckFile>,
}

fn result_page_cleanup_ack_hash(name: &str) -> Option<&str> {
    name.strip_prefix(RESULT_PAGE_CLEANUP_ACK_PREFIX)
        .and_then(|name| name.strip_suffix(".json"))
        .filter(|hash| is_sha256(hash))
}

fn inspect_result_page_cleanup_entries(directory: &Path) -> Result<ResultPageCleanupEntries> {
    let mut entries = ResultPageCleanupEntries::default();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup directory contains a non-UTF-8 entry",
            ));
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup directory contains a link or non-file entry",
            ));
        }
        match name.as_str() {
            "admission.json" if !entries.admission => entries.admission = true,
            "result-page.json" if !entries.page => entries.page = true,
            "result-page-ack.json" if entries.acknowledgement.is_none() => {
                entries.acknowledgement = Some(ResultPageAckFile::Original(entry.path()));
            }
            _ if let Some(page_sha256) = result_page_cleanup_ack_hash(&name)
                && entries.acknowledgement.is_none() =>
            {
                entries.acknowledgement = Some(ResultPageAckFile::Cleanup {
                    path: entry.path(),
                    page_sha256: page_sha256.to_owned(),
                });
            }
            _ => {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "result-page cleanup directory contains unexpected evidence",
                ));
            }
        }
    }
    Ok(entries)
}

fn validate_result_page_cleanup_marker_path(
    store: &RunStore,
    artifact: &CommandCaptureArtifactPage,
    directory: &Path,
) -> Result<()> {
    let expected = store.root.join(result_page_cleanup_marker_name(
        &artifact.result_operation_id,
    ));
    if directory != expected {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result-page cleanup marker differs from its exact Operation namespace",
        ));
    }
    let metadata = fs::symlink_metadata(directory)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result-page cleanup marker is not a regular directory",
        ));
    }
    let canonical = fs::canonicalize(directory)?;
    if canonical != directory || canonical.parent() != Some(store.root.as_path()) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result-page cleanup marker escaped its exact private run-store path",
        ));
    }
    require_path_absent(
        &store.root.join(format!(
            "op-{}",
            digest(artifact.result_operation_id.as_bytes())
        )),
        "result-page cleanup marker coexists with its original Operation directory",
    )
}

fn validate_release_result_page_cleanup_marker(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
    output: &CommandCaptureOutputCoverage,
    artifact: &CommandCaptureArtifactPage,
    directory: &Path,
) -> Result<ResultPageCleanupEntries> {
    validate_release_result_page_descriptor(artifact)?;
    validate_result_page_cleanup_marker_path(store, artifact, directory)?;
    let entries = inspect_result_page_cleanup_entries(directory)?;
    if entries.admission {
        validate_release_result_page_admission(directory, intent, artifact)?;
    }

    let page_sha256 = if entries.page {
        let (params, hash) = read_release_result_page_record(directory, artifact)?;
        validate_release_result_page_body(intent, output, artifact, &params, &hash)?;
        Some(hash)
    } else {
        None
    };

    match &entries.acknowledgement {
        Some(ResultPageAckFile::Original(path)) => {
            let Some(hash) = page_sha256.as_deref() else {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "unrenamed result-page ACK survived without its saved page body",
                ));
            };
            validate_release_result_page_acknowledgement_at(path, artifact, hash)?;
        }
        Some(ResultPageAckFile::Cleanup {
            path,
            page_sha256: ack_page_sha256,
        }) => {
            if page_sha256
                .as_deref()
                .is_some_and(|saved| saved != ack_page_sha256)
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "cleanup ACK hash differs from its surviving result-page body",
                ));
            }
            validate_release_result_page_acknowledgement_at(path, artifact, ack_page_sha256)?;
        }
        None => {}
    }
    Ok(entries)
}

fn validate_release_result_page_for_cleanup(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
    output: &CommandCaptureOutputCoverage,
    artifact: &CommandCaptureArtifactPage,
    cleanup_markers: &[ResultPageCleanupMarker],
) -> Result<ReleaseResultPageCleanup> {
    validate_release_result_page_descriptor(artifact)?;
    let path = store.root.join(format!(
        "op-{}",
        digest(artifact.result_operation_id.as_bytes())
    ));
    let marker_path = store.root.join(result_page_cleanup_marker_name(
        &artifact.result_operation_id,
    ));
    let marker = cleanup_markers
        .iter()
        .find(|marker| marker.directory == marker_path);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    if metadata.is_some() && marker.is_some() {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "result-page Operation and cleanup marker both exist",
        ));
    }
    let Some(metadata) = metadata else {
        if let Some(marker) = marker {
            validate_release_result_page_cleanup_marker(
                store,
                intent,
                output,
                artifact,
                &marker.directory,
            )?;
            return Ok(ReleaseResultPageCleanup::CleanupMarker(
                marker.directory.clone(),
            ));
        }
        return Ok(ReleaseResultPageCleanup::AlreadyRemoved);
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result Operation page path is not a regular directory",
        ));
    }
    let canonical = fs::canonicalize(&path)?;
    if canonical.parent() != Some(store.root.as_path()) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result Operation page directory escaped the private run store",
        ));
    }

    let (directory, params, hash) = read_saved_release_result_page(store, artifact)?;
    if directory != canonical {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result Operation page directory changed during validation",
        ));
    }
    let entries = inspect_result_page_cleanup_entries(&directory)?;
    if !entries.page || !entries.admission {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "original result-page Operation is missing its body or admission",
        ));
    }
    validate_release_result_page_admission(&directory, intent, artifact)?;
    validate_release_result_page_body(intent, output, artifact, &params, &hash)?;
    let acknowledgement_path = match entries.acknowledgement {
        None => return Ok(ReleaseResultPageCleanup::Unacknowledged),
        Some(ResultPageAckFile::Original(path)) => path,
        Some(ResultPageAckFile::Cleanup { .. }) => {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "original result-page Operation contains a cleanup ACK name",
            ));
        }
    };
    validate_release_result_page_acknowledgement_at(&acknowledgement_path, artifact, &hash)?;
    Ok(ReleaseResultPageCleanup::Ready(directory))
}

fn begin_release_result_page_cleanup(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
    output: &CommandCaptureOutputCoverage,
    artifact: &CommandCaptureArtifactPage,
) -> Result<ReleaseResultPageCleanup> {
    if !release_operation_outcome_acknowledged(store, intent)? {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result-page cleanup requires the exact durable release outcome ACK",
        ));
    }
    require_original_release_path_absent(&store.root, &intent.target_operation_id)?;
    let cleanup_markers = read_result_page_cleanup_markers(store)?;
    match validate_release_result_page_for_cleanup(
        store,
        intent,
        output,
        artifact,
        &cleanup_markers,
    )? {
        ReleaseResultPageCleanup::Ready(directory) => {
            let (params, page_sha256) = read_release_result_page_record(&directory, artifact)?;
            validate_release_result_page_body(intent, output, artifact, &params, &page_sha256)?;
            validate_release_result_page_admission(&directory, intent, artifact)?;
            validate_release_result_page_acknowledgement(&directory, artifact, &page_sha256)?;

            let marker = store.root.join(result_page_cleanup_marker_name(
                &artifact.result_operation_id,
            ));
            require_path_absent(
                &marker,
                "result-page cleanup marker already exists before its Operation rename",
            )?;
            fs::rename(&directory, &marker)?;
            sync_parent_directory(&marker)?;

            let acknowledgement = marker.join("result-page-ack.json");
            match fs::symlink_metadata(&acknowledgement) {
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "result-page ACK changed type during cleanup transition",
                    ));
                }
                Ok(_) => {
                    let cleanup_ack = marker.join(result_page_cleanup_ack_name(&page_sha256));
                    require_path_absent(
                        &cleanup_ack,
                        "result-page cleanup ACK marker already exists",
                    )?;
                    fs::rename(&acknowledgement, &cleanup_ack)?;
                    sync_parent_directory(&cleanup_ack)?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
            Ok(ReleaseResultPageCleanup::CleanupMarker(marker))
        }
        state => Ok(state),
    }
}

fn remove_result_page_cleanup_file(directory: &Path, name: &str) -> Result<()> {
    if !matches!(
        name,
        "admission.json" | "result-page.json" | "result-page-ack.json"
    ) && result_page_cleanup_ack_hash(name).is_none()
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "cleanup requested a file outside its exact result-page allowlist",
        ));
    }
    let path = directory.join(name);
    if optional_regular_file(&path, "result-page cleanup entry is not a regular file")? {
        remove_private_durable(&path)?;
    }
    Ok(())
}

fn finish_release_result_page_cleanup(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
    output: &CommandCaptureOutputCoverage,
    artifact: &CommandCaptureArtifactPage,
    directory: &Path,
) -> Result<()> {
    let entries =
        validate_release_result_page_cleanup_marker(store, intent, output, artifact, directory)?;
    if let Some(ResultPageAckFile::Original(path)) = entries.acknowledgement {
        let (params, page_sha256) = read_release_result_page_record(directory, artifact)?;
        validate_release_result_page_body(intent, output, artifact, &params, &page_sha256)?;
        let cleanup_ack = directory.join(result_page_cleanup_ack_name(&page_sha256));
        require_path_absent(
            &cleanup_ack,
            "result-page cleanup ACK marker already exists before ACK rename",
        )?;
        fs::rename(&path, &cleanup_ack)?;
        sync_parent_directory(&cleanup_ack)?;
    }

    let entries =
        validate_release_result_page_cleanup_marker(store, intent, output, artifact, directory)?;
    if entries.page {
        remove_result_page_cleanup_file(directory, "result-page.json")?;
    }
    let entries =
        validate_release_result_page_cleanup_marker(store, intent, output, artifact, directory)?;
    if entries.admission {
        remove_result_page_cleanup_file(directory, "admission.json")?;
    }
    let entries =
        validate_release_result_page_cleanup_marker(store, intent, output, artifact, directory)?;
    if let Some(acknowledgement) = entries.acknowledgement {
        let name = acknowledgement
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "result-page cleanup ACK name is not valid UTF-8",
                )
            })?;
        remove_result_page_cleanup_file(directory, name)?;
    }
    if fs::read_dir(directory)?.next().transpose()?.is_some() {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "result-page cleanup marker is not empty after allowlisted removal",
        ));
    }
    remove_empty_directory(directory)
}

#[cfg(test)]
fn remove_release_result_page_directory(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
    output: &CommandCaptureOutputCoverage,
    artifact: &CommandCaptureArtifactPage,
) -> Result<ReleaseResultPageCleanup> {
    match begin_release_result_page_cleanup(store, intent, output, artifact)? {
        ReleaseResultPageCleanup::CleanupMarker(directory) => {
            finish_release_result_page_cleanup(store, intent, output, artifact, &directory)?;
            Ok(ReleaseResultPageCleanup::AlreadyRemoved)
        }
        state => Ok(state),
    }
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

/// Read one release tombstone's saved intent and prove the directory name
/// belongs to the exact target Operation the intent names.
///
/// A tombstone directory is `releasing-<digest(target_operation_id)>`. The
/// stripped suffix is that digest, never an Operation ID, so it is only
/// usable to compare against the digest re-derived from the validated intent.
/// Readback therefore loads the intent first, then re-derives the expected
/// directory name and requires an exact match. This keeps restart scans
/// able to converge a tombstone the release path itself created instead of
/// rejecting it against its own digest.
fn read_release_tombstone_intent(
    directory: &Path,
    directory_name: &str,
) -> Result<CaptureReleaseIntentRecord> {
    read_release_tombstone_intent_if_present(directory, directory_name)?.ok_or_else(|| {
        Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone is missing its immutable intent",
        )
    })
}

fn read_release_tombstone_intent_if_present(
    directory: &Path,
    directory_name: &str,
) -> Result<Option<CaptureReleaseIntentRecord>> {
    let expected_digest = directory_name.strip_prefix("releasing-").ok_or_else(|| {
        Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone name has no releasing prefix",
        )
    })?;
    if !is_sha256(expected_digest) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone name does not contain a canonical target digest",
        ));
    }
    let raw_candidate = fs::canonicalize(directory)?;
    if raw_candidate.file_name().and_then(|name| name.to_str()) != Some(directory_name) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone name is not its own canonical directory",
        ));
    }
    let intent_path = directory.join("release-intent.json");
    match fs::symlink_metadata(&intent_path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release tombstone intent is not a regular file",
            ));
        }
        Ok(_) => {}
    }
    let intent: CaptureReleaseIntentRecord = read_json(&intent_path)?;
    validate_capture_release_intent(&intent, &intent.target_operation_id)?;
    if digest(intent.target_operation_id.as_bytes()) != expected_digest {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone name differs from its target Operation",
        ));
    }
    Ok(Some(intent))
}

fn validate_release_tombstone_subset(
    directory: &Path,
    directory_name: &str,
    intent: &CaptureReleaseIntentRecord,
) -> Result<bool> {
    let expected_name = format!(
        "releasing-{}",
        digest(intent.target_operation_id.as_bytes())
    );
    if directory_name != expected_name {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone name differs from its exact target Operation",
        ));
    }
    let canonical = fs::canonicalize(directory)?;
    if canonical.parent() != directory.parent()
        || canonical.file_name().and_then(|name| name.to_str()) != Some(directory_name)
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone escaped its exact private run-store path",
        ));
    }
    let mut intent_present = false;
    for entry in fs::read_dir(&canonical)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release tombstone contains a non-UTF-8 entry",
            ));
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || !RELEASE_TOMBSTONE_FILES.contains(&name)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release tombstone contains an unexpected file or link",
            ));
        }
        if name == "release-intent.json" {
            let saved: CaptureReleaseIntentRecord = read_json(&entry.path())?;
            validate_capture_release_intent(&saved, &intent.target_operation_id)?;
            if saved != *intent {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_CONFLICT",
                    "release tombstone differs from its exact acknowledged Operation intent",
                ));
            }
            intent_present = true;
        }
    }
    Ok(intent_present)
}

fn read_capture_release_intent(
    directory: &Path,
    target_operation_id: &str,
) -> Result<CaptureReleaseIntentRecord> {
    let intent: CaptureReleaseIntentRecord = read_json(&directory.join("release-intent.json"))?;
    validate_capture_release_intent(&intent, target_operation_id)?;
    Ok(intent)
}

fn read_release_operation_intent(
    store: &RunStore,
    directory: &Path,
    directory_name: &str,
) -> Result<Option<CaptureReleaseIntentRecord>> {
    let intent_path = directory.join("release-intent.json");
    match fs::symlink_metadata(&intent_path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "saved release intent is not a regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }

    let intent: CaptureReleaseIntentRecord = read_json(&intent_path)?;
    validate_capture_release_intent(&intent, &intent.target_operation_id)?;
    let release_directory_name = format!("op-{}", digest(intent.release_operation_id.as_bytes()));
    if directory_name == release_directory_name {
        let validated = validate_release_operation_directory(store, &intent)?;
        if validated != directory {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release Operation intent is outside its exact admitted directory",
            ));
        }
        return Ok(Some(intent));
    }

    // Before the ownership rename, the same immutable intent is also saved in
    // the target Operation directory. It is not the release Operation anchor.
    let target_directory_name = format!("op-{}", digest(intent.target_operation_id.as_bytes()));
    if directory_name == target_directory_name {
        return Ok(None);
    }
    Err(Error::new(
        "ADAPTER_EVIDENCE_INVALID",
        "release intent is stored under neither its release nor target Operation",
    ))
}

fn validate_release_operation_directory(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
) -> Result<PathBuf> {
    let directory = store.existing_directory(&intent.release_operation_id)?;
    let saved = read_capture_release_intent(&directory, &intent.target_operation_id)?;
    if saved != *intent {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "release Operation directory retains a different immutable intent",
        ));
    }
    let admission: AdmissionRecord = read_json(&directory.join("admission.json"))?;
    validate_release_operation_admission(&admission, intent)?;
    for entry in fs::read_dir(&directory)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        let file_name = entry.file_name();
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || file_name
                .to_str()
                .is_none_or(|name| !RELEASE_OPERATION_FILES.contains(&name))
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release Operation directory contains unexpected evidence or a link",
            ));
        }
    }
    Ok(directory)
}

fn validate_release_operation_admission(
    admission: &AdmissionRecord,
    intent: &CaptureReleaseIntentRecord,
) -> Result<()> {
    if admission.schema != 1
        || admission.module_artifact_id != ARTIFACT_ID
        || admission.execution_shape != EXECUTION_SHAPE
        || admission.identity.operation_id != intent.release_operation_id
        || !admission.identity.input_sha256.is_empty()
        || admission.binding_id != intent.binding_id
        || admission.generation != intent.generation
        || admission.route_sha256 != intent.route_sha256
        || !admission_checksum_valid(admission)
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release Operation admission differs from its immutable intent",
        ));
    }
    Ok(())
}

fn release_operation_cleanup_marker_name(release_operation_id: &str) -> String {
    format!(
        "{RELEASE_OPERATION_CLEANUP_PREFIX}{}",
        digest(release_operation_id.as_bytes())
    )
}

fn result_page_cleanup_marker_name(result_operation_id: &str) -> String {
    format!(
        "{RESULT_PAGE_CLEANUP_PREFIX}{}",
        digest(result_operation_id.as_bytes())
    )
}

fn result_page_cleanup_ack_name(result_page_sha256: &str) -> String {
    format!("{RESULT_PAGE_CLEANUP_ACK_PREFIX}{result_page_sha256}.json")
}

fn read_result_page_cleanup_markers(store: &RunStore) -> Result<Vec<ResultPageCleanupMarker>> {
    let mut markers = Vec::new();
    for entry in fs::read_dir(&store.root)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_str().ok_or_else(|| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "run store contains a non-UTF-8 entry",
            )
        })?;
        let Some(operation_digest) = name.strip_prefix(RESULT_PAGE_CLEANUP_PREFIX) else {
            continue;
        };
        if !is_sha256(operation_digest) {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup marker name is not a canonical Operation digest",
            ));
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup marker is not a regular directory",
            ));
        }
        let directory = fs::canonicalize(entry.path())?;
        if directory.parent() != Some(store.root.as_path())
            || directory.file_name().and_then(|name| name.to_str()) != Some(name)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup marker escaped its exact private run-store path",
            ));
        }
        require_path_absent(
            &store.root.join(format!("op-{operation_digest}")),
            "result-page cleanup marker coexists with its original Operation directory",
        )?;
        if markers
            .iter()
            .any(|marker: &ResultPageCleanupMarker| marker.operation_digest == operation_digest)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "multiple result-page cleanup markers name one Operation",
            ));
        }
        markers.push(ResultPageCleanupMarker {
            directory,
            directory_name: name.to_owned(),
            operation_digest: operation_digest.to_owned(),
        });
    }
    Ok(markers)
}

fn validate_result_page_cleanup_marker_owners(
    store: &RunStore,
    markers: &[ResultPageCleanupMarker],
    release_operations: &[CaptureReleaseIntentRecord],
) -> Result<()> {
    for marker in markers {
        let mut owner = None;
        let mut linked_page_count = 0usize;
        for intent in release_operations {
            for page in intent
                .evidence
                .outputs
                .iter()
                .flat_map(|output| output.pages.iter())
            {
                if digest(page.result_operation_id.as_bytes()) == marker.operation_digest
                    && result_page_cleanup_marker_name(&page.result_operation_id)
                        == marker.directory_name
                {
                    owner = Some(intent);
                    linked_page_count += 1;
                }
            }
        }
        if linked_page_count != 1 {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup marker has no unique immutable release-intent owner",
            ));
        }
        let intent = owner.ok_or_else(|| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup marker owner was not retained",
            )
        })?;
        if !release_operation_outcome_acknowledged(store, intent)? {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "result-page cleanup marker lacks its exact durable release outcome ACK",
            ));
        }
    }
    Ok(())
}

fn require_path_absent(path: &Path, description: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(Error::new("ADAPTER_EVIDENCE_CONFLICT", description)),
    }
}

fn optional_regular_file(path: &Path, description: &str) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(Error::new("ADAPTER_EVIDENCE_INVALID", description))
        }
        Ok(_) => Ok(true),
    }
}

fn expected_release_outcome_sha256(intent: &CaptureReleaseIntentRecord) -> Result<String> {
    let outcome = capture_release_outcome(intent);
    let value = serde_json::to_value(outcome)?;
    Ok(digest(canonical(&value)?.as_bytes()))
}

fn validate_release_cleanup_marker_records(
    directory: &Path,
    intent: &CaptureReleaseIntentRecord,
) -> Result<()> {
    let intent_path = directory.join("release-intent.json");
    if !optional_regular_file(&intent_path, "release cleanup intent is not a regular file")? {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "nonempty release cleanup marker is missing its last intent marker",
        ));
    }
    let saved: CaptureReleaseIntentRecord = read_json(&intent_path)?;
    validate_capture_release_intent(&saved, &intent.target_operation_id)?;
    if saved != *intent {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "release cleanup marker differs from its immutable Operation intent",
        ));
    }

    let admission_path = directory.join("admission.json");
    if optional_regular_file(
        &admission_path,
        "release cleanup admission is not a regular file",
    )? {
        let admission: AdmissionRecord = read_json(&admission_path)?;
        validate_release_operation_admission(&admission, intent)?;
    }

    let expected_outcome_sha256 = expected_release_outcome_sha256(intent)?;
    let outcome_path = directory.join("outcome.json");
    if optional_regular_file(
        &outcome_path,
        "release cleanup outcome is not a regular file",
    )? {
        let outcome: OutcomeRecord = read_json(&outcome_path)?;
        let value = serde_json::to_value(&outcome.outcome)?;
        let saved_sha256 = digest(canonical(&value)?.as_bytes());
        if outcome.schema != 1
            || outcome.module_artifact_id != ARTIFACT_ID
            || outcome.operation_id != intent.release_operation_id
            || outcome.outcome.operation_id != intent.release_operation_id
            || outcome.outcome_sha256 != saved_sha256
            || saved_sha256 != expected_outcome_sha256
            || !capture_release_outcome_matches(&outcome.outcome, intent)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup outcome differs from its exact acknowledged intent",
            ));
        }
    }

    let acknowledgement_path = directory.join("ack.json");
    if optional_regular_file(
        &acknowledgement_path,
        "release cleanup acknowledgement is not a regular file",
    )? {
        let acknowledgement: AckRecord = read_json(&acknowledgement_path)?;
        if acknowledgement.schema != 1
            || acknowledgement.operation_id != intent.release_operation_id
            || acknowledgement.outcome_sha256 != expected_outcome_sha256
            || acknowledgement.acknowledgement["recorded"] != true
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup acknowledgement differs from its exact outcome",
            ));
        }
    }
    Ok(())
}

fn validate_release_cleanup_dependencies_absent(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
) -> Result<()> {
    require_original_release_path_absent(&store.root, &intent.target_operation_id)?;
    let tombstone = store.root.join(format!(
        "releasing-{}",
        digest(intent.target_operation_id.as_bytes())
    ));
    require_path_absent(
        &tombstone,
        "release cleanup marker coexists with its target tombstone",
    )?;
    for output in &intent.evidence.outputs {
        for page in &output.pages {
            let page_directory = store.root.join(format!(
                "op-{}",
                digest(page.result_operation_id.as_bytes())
            ));
            require_path_absent(
                &page_directory,
                "release cleanup marker coexists with a linked result-page Operation",
            )?;
            require_path_absent(
                &store
                    .root
                    .join(result_page_cleanup_marker_name(&page.result_operation_id)),
                "release cleanup marker coexists with a linked result-page cleanup marker",
            )?;
        }
    }
    Ok(())
}

fn read_release_operation_cleanup_marker(
    store: &RunStore,
    directory: &Path,
    directory_name: &str,
) -> Result<Option<CaptureReleaseIntentRecord>> {
    let suffix = directory_name
        .strip_prefix(RELEASE_OPERATION_CLEANUP_PREFIX)
        .ok_or_else(|| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup marker has an invalid namespace",
            )
        })?;
    if !is_sha256(suffix) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release cleanup marker name is not a canonical Operation digest",
        ));
    }
    let canonical = fs::canonicalize(directory)?;
    if canonical.parent() != Some(store.root.as_path())
        || canonical.file_name().and_then(|name| name.to_str()) != Some(directory_name)
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release cleanup marker escaped its exact private run-store path",
        ));
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(&canonical)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup marker contains a non-UTF-8 entry",
            ));
        };
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || !RELEASE_OPERATION_FILES.contains(&name)
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup marker contains unexpected evidence or a link",
            ));
        }
        entries.push(name.to_owned());
    }

    let original_directory = store.root.join(format!("op-{suffix}"));
    require_path_absent(
        &original_directory,
        "release Operation exists beside its cleanup marker",
    )?;
    if entries.is_empty() {
        return Ok(None);
    }

    let intent_path = canonical.join("release-intent.json");
    if !entries.iter().any(|name| name == "release-intent.json") {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "nonempty release cleanup marker is missing its last intent marker",
        ));
    }
    let intent: CaptureReleaseIntentRecord = read_json(&intent_path)?;
    validate_capture_release_intent(&intent, &intent.target_operation_id)?;
    if digest(intent.release_operation_id.as_bytes()) != suffix {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release cleanup marker name differs from its exact Operation intent",
        ));
    }
    validate_release_cleanup_marker_records(&canonical, &intent)?;
    validate_release_cleanup_dependencies_absent(store, &intent)?;
    Ok(Some(intent))
}

fn release_operation_outcome_acknowledged(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
) -> Result<bool> {
    validate_release_operation_directory(store, intent)?;
    let Some((outcome, hash)) = store.read_outcome(&intent.release_operation_id)? else {
        return Ok(false);
    };
    if !capture_release_outcome_matches(&outcome, intent) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release Operation outcome differs from its immutable intent",
        ));
    }
    Ok(!store.outcome_pending(&intent.release_operation_id, &hash)?)
}

#[derive(Debug)]
enum ReleaseResultPageCleanup {
    AlreadyRemoved,
    Unacknowledged,
    Ready(PathBuf),
    CleanupMarker(PathBuf),
}

fn finalize_acknowledged_release(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
    result_page_cleanup_markers: &[ResultPageCleanupMarker],
) -> Result<()> {
    if !release_operation_outcome_acknowledged(store, intent)? {
        return Ok(());
    }
    require_original_release_path_absent(&store.root, &intent.target_operation_id)?;

    if intent.evidence.outputs.len() != 2 {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "acknowledged release intent does not contain both output streams",
        ));
    }
    let mut seen_outputs = Vec::new();
    let mut seen_result_operations = Vec::new();
    let mut pages_to_remove = Vec::new();
    for output in &intent.evidence.outputs {
        if !matches!(
            output.native_output.as_str(),
            "stdout.ndjson" | "stderr.txt"
        ) || seen_outputs
            .iter()
            .any(|seen| seen == &output.native_output)
            || output.target_command_output["native_output"] != output.native_output
            || output.target_command_output["operation_id"] != intent.target_operation_id
            || output.target_command_output["input_sha256"] != intent.evidence.target_input_sha256
            || !is_sha256(
                output.target_command_output["stored_sha256"]
                    .as_str()
                    .unwrap_or_default(),
            )
            || output.pages.is_empty()
        {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "acknowledged release output linkage is malformed",
            ));
        }
        seen_outputs.push(output.native_output.clone());
        let total = output.target_command_output["stored_bytes"]
            .as_u64()
            .ok_or_else(|| {
                Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release output omitted its stored byte length",
                )
            })?;
        let mut covered = 0u64;
        let mut previous_range: Option<&CommandCaptureArtifactPage> = None;
        for artifact in &output.pages {
            if artifact.result_operation_id.is_empty()
                || artifact.result_operation_id == intent.target_operation_id
                || artifact.result_operation_id == intent.release_operation_id
                || !is_sha256(&artifact.result_input_sha256)
                || artifact.artifact_ref.is_empty()
                || !is_sha256(&artifact.artifact_sha256)
                || !is_sha256(&artifact.page_sha256)
                || artifact.artifact_sha256 != artifact.page_sha256
                || artifact.artifact_byte_length != artifact.byte_length
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release result page descriptor is malformed or aliases another Operation",
                ));
            }
            let end = artifact
                .offset_bytes
                .checked_add(artifact.byte_length)
                .ok_or_else(|| {
                    Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "release result page range overflows",
                    )
                })?;
            if artifact.total_bytes != total
                || end > total
                || artifact.artifact_byte_length != artifact.byte_length
                || artifact.artifact_sha256 != artifact.page_sha256
                || artifact.eof != (end == total)
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release result pages do not form exact contiguous output coverage",
                ));
            }
            let duplicate_range = if let Some(previous) = previous_range
                && artifact.offset_bytes == previous.offset_bytes
            {
                if artifact.byte_length != previous.byte_length
                    || artifact.total_bytes != previous.total_bytes
                    || artifact.artifact_byte_length != previous.artifact_byte_length
                    || artifact.page_sha256 != previous.page_sha256
                    || artifact.artifact_sha256 != previous.artifact_sha256
                    || artifact.eof != previous.eof
                {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "overlapping release result pages disagree on their exact range",
                    ));
                }
                true
            } else {
                false
            };
            if !duplicate_range {
                if artifact.offset_bytes != covered
                    || (artifact.byte_length == 0 && !(total == 0 && previous_range.is_none()))
                {
                    return Err(Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "release result pages do not form exact contiguous output coverage",
                    ));
                }
                covered = end;
                previous_range = Some(artifact);
            }
            if seen_result_operations
                .iter()
                .any(|seen| seen == &artifact.result_operation_id)
            {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "release intent repeats a result Operation page directory",
                ));
            }
            seen_result_operations.push(artifact.result_operation_id.clone());
            match validate_release_result_page_for_cleanup(
                store,
                intent,
                output,
                artifact,
                result_page_cleanup_markers,
            )? {
                ReleaseResultPageCleanup::AlreadyRemoved => {}
                ReleaseResultPageCleanup::Unacknowledged => return Ok(()),
                ReleaseResultPageCleanup::Ready(_) | ReleaseResultPageCleanup::CleanupMarker(_) => {
                    pages_to_remove.push((output, artifact));
                }
            }
        }
        if covered != total {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release result page coverage does not reach the exact output length",
            ));
        }
    }
    if seen_outputs.len() != 2
        || !seen_outputs.iter().any(|output| output == "stdout.ndjson")
        || !seen_outputs.iter().any(|output| output == "stderr.txt")
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "acknowledged release does not name stdout and stderr",
        ));
    }

    let mut page_markers_to_finish = Vec::new();
    for (output, artifact) in pages_to_remove {
        match begin_release_result_page_cleanup(store, intent, output, artifact)? {
            ReleaseResultPageCleanup::AlreadyRemoved => {}
            ReleaseResultPageCleanup::Unacknowledged => return Ok(()),
            ReleaseResultPageCleanup::CleanupMarker(directory) => {
                page_markers_to_finish.push((output, artifact, directory));
            }
            ReleaseResultPageCleanup::Ready(_) => {
                return Err(Error::new(
                    "ADAPTER_EVIDENCE_INVALID",
                    "result-page cleanup transition left its Operation in the original namespace",
                ));
            }
        }
    }
    remove_release_tombstone(store, intent)?;
    for (output, artifact, directory) in page_markers_to_finish {
        finish_release_result_page_cleanup(store, intent, output, artifact, &directory)?;
    }
    remove_release_operation_directory(store, intent)
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

fn require_original_release_path_absent(root: &Path, target_operation_id: &str) -> Result<()> {
    let original = root.join(format!("op-{}", digest(target_operation_id.as_bytes())));
    match fs::symlink_metadata(&original) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
        Ok(_) => Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "release tombstone cannot be resolved while its original path exists",
        )),
    }
}

fn remove_release_tombstone(store: &RunStore, intent: &CaptureReleaseIntentRecord) -> Result<()> {
    if !release_operation_outcome_acknowledged(store, intent)? {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release tombstone cleanup requires the exact durable outcome ACK",
        ));
    }
    require_original_release_path_absent(&store.root, &intent.target_operation_id)?;
    let Some(directory) = store.existing_release_directory(&intent.target_operation_id)? else {
        return Ok(());
    };
    let directory_name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release tombstone name is not valid UTF-8",
            )
        })?;
    validate_release_tombstone_subset(&directory, directory_name, intent)?;
    for name in RELEASE_TOMBSTONE_FILES {
        if name != "release-intent.json" {
            remove_allowed_regular_file(&directory, name, &RELEASE_TOMBSTONE_FILES)?;
        }
    }
    validate_release_tombstone_subset(&directory, directory_name, intent)?;
    remove_allowed_regular_file(&directory, "release-intent.json", &RELEASE_TOMBSTONE_FILES)?;
    remove_empty_directory(&directory)
}
/// Durable per-run state left behind by a release whose host outcome is
/// already acknowledged.
///
/// The release Operation itself keeps its admitted directory under the same
/// private run store: admission.json, release-intent.json, outcome.json and
/// ack.json. Removing only the target tombstone would leave that directory
/// retained forever, which is exactly the swap the release contract rejects:
/// one bounded cleanup directory replaced by another forever-retained one.
/// This runs only after the caller has proven the exact durable outcome
/// acknowledgement for this intent, so no unacknowledged release evidence is
/// ever discarded.
fn remove_release_operation_directory(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
) -> Result<()> {
    let Some(marker) = begin_release_operation_cleanup(store, intent)? else {
        return Ok(());
    };
    finish_release_operation_cleanup(store, &marker, intent)
}

fn begin_release_operation_cleanup(
    store: &RunStore,
    intent: &CaptureReleaseIntentRecord,
) -> Result<Option<PathBuf>> {
    let directory = store.root.join(format!(
        "op-{}",
        digest(intent.release_operation_id.as_bytes())
    ));
    let marker = store.root.join(release_operation_cleanup_marker_name(
        &intent.release_operation_id,
    ));
    let metadata = match fs::symlink_metadata(&directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return match fs::symlink_metadata(&marker) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(error) => Err(error.into()),
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                    Err(Error::new(
                        "ADAPTER_EVIDENCE_INVALID",
                        "release cleanup marker is not a regular directory",
                    ))
                }
                Ok(_) => {
                    let name = marker
                        .file_name()
                        .and_then(|name| name.to_str())
                        .ok_or_else(|| {
                            Error::new(
                                "ADAPTER_EVIDENCE_INVALID",
                                "release cleanup marker name is not valid UTF-8",
                            )
                        })?;
                    let saved = read_release_operation_cleanup_marker(store, &marker, name)?;
                    if saved.as_ref().is_some_and(|saved| saved != intent)
                        || name
                            != release_operation_cleanup_marker_name(&intent.release_operation_id)
                    {
                        return Err(Error::new(
                            "ADAPTER_EVIDENCE_CONFLICT",
                            "existing release cleanup marker differs from its exact Operation",
                        ));
                    }
                    Ok(Some(marker))
                }
            };
        }
        Err(error) => return Err(error.into()),
    };
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release Operation directory is not a regular directory",
        ));
    }
    if !release_operation_outcome_acknowledged(store, intent)? {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release Operation cleanup requires the exact durable outcome ACK",
        ));
    }
    let canonical = validate_release_operation_directory(store, intent)?;
    if canonical != directory {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release Operation directory escaped its exact private run-store path",
        ));
    }
    validate_release_cleanup_dependencies_absent(store, intent)?;
    match fs::symlink_metadata(&marker) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
        Ok(_) => {
            return Err(Error::new(
                "ADAPTER_EVIDENCE_CONFLICT",
                "release Operation and cleanup marker both exist",
            ));
        }
    }
    fs::rename(&directory, &marker)?;
    sync_parent_directory(&marker)?;
    Ok(Some(marker))
}

fn finish_release_operation_cleanup(
    store: &RunStore,
    directory: &Path,
    intent: &CaptureReleaseIntentRecord,
) -> Result<()> {
    let name = directory
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup marker name is not valid UTF-8",
            )
        })?;
    if name != release_operation_cleanup_marker_name(&intent.release_operation_id) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release cleanup marker name differs from its exact Operation",
        ));
    }
    let Some(saved) = read_release_operation_cleanup_marker(store, directory, name)? else {
        return remove_empty_release_operation_cleanup_marker(store, directory, name);
    };
    if saved != *intent {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "release cleanup marker differs from its immutable Operation intent",
        ));
    }

    for name in ["run.json", "ack.json", "outcome.json", "admission.json"] {
        remove_allowed_regular_file(directory, name, &RELEASE_OPERATION_FILES)?;
    }
    let Some(saved) = read_release_operation_cleanup_marker(store, directory, name)? else {
        return remove_empty_release_operation_cleanup_marker(store, directory, name);
    };
    if saved != *intent {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_CONFLICT",
            "release cleanup marker changed before its last intent removal",
        ));
    }
    remove_allowed_regular_file(directory, "release-intent.json", &RELEASE_OPERATION_FILES)?;
    remove_empty_release_operation_cleanup_marker(store, directory, name)
}

fn remove_allowed_regular_file(directory: &Path, name: &str, allowed_files: &[&str]) -> Result<()> {
    if !allowed_files.contains(&name) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "cleanup requested a file outside its exact allowlist",
        ));
    }
    let path = directory.join(name);
    if optional_regular_file(&path, "cleanup entry is not a regular file")? {
        remove_private_durable(&path)?;
    }
    Ok(())
}

fn remove_empty_directory(directory: &Path) -> Result<()> {
    match fs::remove_dir(directory) {
        Ok(()) => sync_parent_directory(directory),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn remove_empty_release_operation_cleanup_marker(
    store: &RunStore,
    directory: &Path,
    directory_name: &str,
) -> Result<()> {
    let suffix = directory_name
        .strip_prefix(RELEASE_OPERATION_CLEANUP_PREFIX)
        .ok_or_else(|| {
            Error::new(
                "ADAPTER_EVIDENCE_INVALID",
                "release cleanup marker has an invalid namespace",
            )
        })?;
    if !is_sha256(suffix) {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release cleanup marker name is not a canonical Operation digest",
        ));
    }
    let canonical = fs::canonicalize(directory)?;
    if canonical.parent() != Some(store.root.as_path())
        || canonical.file_name().and_then(|name| name.to_str()) != Some(directory_name)
    {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release cleanup marker escaped its exact private run-store path",
        ));
    }
    let mut entries = fs::read_dir(&canonical)?;
    if entries.next().transpose()?.is_some() {
        return Err(Error::new(
            "ADAPTER_EVIDENCE_INVALID",
            "release cleanup marker is not empty after its last intent was removed",
        ));
    }
    let original_directory = store.root.join(format!("op-{suffix}"));
    require_path_absent(
        &original_directory,
        "empty release cleanup marker coexists with its Operation directory",
    )?;
    remove_empty_directory(&canonical)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Profile;

    struct TempState(PathBuf);

    impl TempState {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("r47-release-recovery-{}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).expect("create fixture state directory");
            Self(path)
        }
    }

    impl Drop for TempState {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const BINDING_ID: &str = "fixture-binding";
    const GENERATION: i64 = 3;
    const RELEASE_INPUT_SHA256: &str =
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    fn route_value() -> Value {
        json!({
            "runtime":crate::RUNTIME,
            "module_artifact_id":ARTIFACT_ID,
            "native_options":{"modelId":"fixture-model"}
        })
    }

    fn native_child() -> Value {
        json!({
            "spawn_returned_pid":17,
            "birth_identity":Value::Null,
            "exit_observed_through_child_handle":false,
            "exit":Value::Null,
            "family_departure_claimed":false,
            "manager_group_drain_required":true
        })
    }

    fn terminal_target_outcome(operation_id: &str) -> RuntimeOutcome {
        RuntimeOutcome {
            operation_id: operation_id.to_owned(),
            outcome: EffectOutcome::Unknown,
            native_scope_key: None,
            native_root_id: None,
            turn_id: None,
            native_input_id: None,
            details: json!({"completion_condition":"native_result_unconfirmed","native_replay":false}),
        }
    }

    struct CaptureFixture {
        store: RunStore,
        target_id: String,
        target_input_sha256: String,
        binding_id: String,
        generation: i64,
        route: Value,
    }

    impl CaptureFixture {
        fn new(store: RunStore, target_id: &str, input_sha256: &str) -> Result<Self> {
            let binding_id = BINDING_ID.to_owned();
            let generation = GENERATION;
            let route = route_value();
            let identity = DispatchIdentity {
                operation_id: target_id.to_owned(),
                input_sha256: input_sha256.to_owned(),
                batch_run_id: "fixture-batch".to_owned(),
                requested_model: "fixture-model".to_owned(),
                prompt_sha256: "b".repeat(64),
                prompt_bytes: 19,
                task_snapshot_sha256: "c".repeat(64),
            };
            store.admit(target_id, Some(&identity), &binding_id, generation, &route)?;
            let stdout = b"partial stdout retained after timeout\n".to_vec();
            let stderr = b"partial stderr retained after timeout\n".to_vec();
            store.save_native_evidence(
                target_id,
                &stdout,
                &stderr,
                &json!({
                    "stdout":{
                        "bytes":(stdout.len() + 9) as u64,
                        "stored_bytes":stdout.len() as u64,
                        "sha256":digest(b"partial stdout retained after timeout\nlate"),
                        "stored_sha256":digest(&stdout),
                        "truncated":false,
                        "read_error":true
                    },
                    "stderr":{
                        "bytes":(stderr.len() + 9) as u64,
                        "stored_bytes":stderr.len() as u64,
                        "sha256":digest(b"partial stderr retained after timeout\nlate"),
                        "stored_sha256":digest(&stderr),
                        "truncated":false,
                        "read_error":true
                    },
                    "native_child":native_child()
                }),
            )?;
            let outcome = terminal_target_outcome(target_id);
            let target_outcome_sha256 = store.save_outcome(&outcome)?;
            store.acknowledge(target_id, &target_outcome_sha256, json!({"recorded":true}))?;
            Ok(Self {
                store,
                target_id: target_id.to_owned(),
                target_input_sha256: input_sha256.to_owned(),
                binding_id,
                generation,
                route,
            })
        }

        fn release_evidence(&self) -> Result<CommandCaptureReleaseEvidence> {
            let mut outputs = Vec::new();
            for (native_output, bytes, result_input_sha256) in [
                (
                    "stdout.ndjson",
                    self.stdout_bytes(),
                    "d".repeat(64).as_str(),
                ),
                ("stderr.txt", self.stderr_bytes(), "e".repeat(64).as_str()),
            ] {
                let stream_sha256 = if native_output == "stdout.ndjson" {
                    digest(b"partial stdout retained after timeout\nlate")
                } else {
                    digest(b"partial stderr retained after timeout\nlate")
                };
                let stored = digest(bytes);
                let total = bytes.len() as u64;
                let snapshot = json!({
                    "schema_version":1,
                    "operation_id":self.target_id,
                    "method":"task.dispatch",
                    "operation_state":"outcome_unknown",
                    "operation_outcome":"unknown",
                    "completion_condition":"native_result_unconfirmed",
                    "input_sha256":self.target_input_sha256,
                    "module_receipt":Value::Null,
                    "task_dispatch_context":Value::Null,
                    "dispatch_admission":Value::Null,
                    "native_output":native_output,
                    "stream_bytes":(total + 9),
                    "stored_bytes":total,
                    "stream_sha256":stream_sha256,
                    "stored_sha256":stored,
                    "truncated":false,
                    "read_error":true,
                    "native_child":native_child(),
                    "native_response_identity":"unavailable",
                    "execution_complete":false,
                    "task_completion":"unknown",
                    "native_replay":false
                });
                assert_eq!(
                    self.store.read_native_output(
                        &self.target_id,
                        &self.target_input_sha256,
                        &self.binding_id,
                        self.generation,
                        &self.route,
                        &snapshot,
                    )?,
                    bytes
                );
                let result_operation_id = format!("fixture-result-{native_output}");
                let artifact_ref = format!("artifact:{result_operation_id}");
                let params = json!({
                    "operation_id":result_operation_id,
                    "page":{
                        "source":{
                            "kind":"command_output",
                            "result_operation_id":result_operation_id,
                            "result_input_sha256":result_input_sha256,
                            "input_operation_id":self.target_id,
                            "target_input_sha256":self.target_input_sha256,
                            "target_command_output":snapshot
                        },
                        "offset_bytes":0,
                        "byte_length":total,
                        "total_bytes":total,
                        "eof":true,
                        "media_type":if native_output == "stdout.ndjson" {
                            "application/x-ndjson"
                        } else {
                            "text/plain; charset=utf-8"
                        },
                        "content_base64":STANDARD.encode(bytes),
                        "page_sha256":stored
                    }
                });
                self.store.admit(
                    &result_operation_id,
                    None,
                    &self.binding_id,
                    self.generation,
                    &self.route,
                )?;
                let result_page_sha256 =
                    self.store.save_result_page(&result_operation_id, &params)?;
                self.store.acknowledge_result_page(
                    &result_operation_id,
                    &result_page_sha256,
                    json!({"recorded":true,"artifact_ref":artifact_ref}),
                )?;
                outputs.push(CommandCaptureOutputCoverage {
                    native_output: native_output.to_owned(),
                    target_command_output: snapshot,
                    pages: vec![CommandCaptureArtifactPage {
                        result_operation_id,
                        result_input_sha256: result_input_sha256.to_owned(),
                        artifact_ref,
                        artifact_sha256: stored.clone(),
                        artifact_byte_length: total,
                        offset_bytes: 0,
                        byte_length: total,
                        total_bytes: total,
                        page_sha256: stored,
                        eof: true,
                    }],
                });
            }
            Ok(CommandCaptureReleaseEvidence {
                schema_version: 1,
                target_operation_id: self.target_id.clone(),
                target_input_sha256: self.target_input_sha256.clone(),
                target_outcome_sha256: self
                    .store
                    .read_outcome(&self.target_id)?
                    .map(|(_, hash)| hash)
                    .ok_or_else(|| Error::new("FIXTURE_FAILED", "target outcome missing"))?,
                outputs,
            })
        }

        fn stdout_bytes(&self) -> &[u8] {
            b"partial stdout retained after timeout\n"
        }

        fn stderr_bytes(&self) -> &[u8] {
            b"partial stderr retained after timeout\n"
        }

        fn release(&self, release_operation_id: &str) -> Result<RuntimeOutcome> {
            let (release_directory, _) = self.store.admit(
                release_operation_id,
                None,
                &self.binding_id,
                self.generation,
                &self.route,
            )?;
            let evidence = if release_directory.join("release-intent.json").exists() {
                read_capture_release_intent(&release_directory, &self.target_id)?.evidence
            } else {
                self.release_evidence()?
            };
            self.store.release_command_captures(
                release_operation_id,
                RELEASE_INPUT_SHA256,
                &self.binding_id,
                self.generation,
                &self.route,
                &evidence,
            )
        }

        fn acknowledge_release(
            &self,
            release_operation_id: &str,
            outcome: &RuntimeOutcome,
        ) -> Result<String> {
            let hash = self.store.save_outcome(outcome)?;
            self.store
                .acknowledge(release_operation_id, &hash, json!({"recorded":true}))?;
            Ok(hash)
        }
    }

    fn operation_directory(store: &RunStore, operation_id: &str) -> PathBuf {
        store
            .root
            .join(format!("op-{}", digest(operation_id.as_bytes())))
    }

    fn tombstone_directory(store: &RunStore, target_id: &str) -> PathBuf {
        store
            .root
            .join(format!("releasing-{}", digest(target_id.as_bytes())))
    }

    fn stage_first_result_page_cleanup(
        fixture: &CaptureFixture,
        release_operation_id: &str,
    ) -> Result<(CaptureReleaseIntentRecord, PathBuf, String)> {
        let outcome = fixture.release(release_operation_id)?;
        fixture.acknowledge_release(release_operation_id, &outcome)?;
        let release_directory = operation_directory(&fixture.store, release_operation_id);
        let intent = read_capture_release_intent(&release_directory, &fixture.target_id)?;
        let output = &intent.evidence.outputs[0];
        let artifact = &output.pages[0];
        let page_directory = operation_directory(&fixture.store, &artifact.result_operation_id);
        let (_, page_sha256) = read_release_result_page_record(&page_directory, artifact)?;
        let marker =
            match begin_release_result_page_cleanup(&fixture.store, &intent, output, artifact)? {
                ReleaseResultPageCleanup::CleanupMarker(marker) => marker,
                _ => {
                    return Err(Error::new(
                        "FIXTURE_FAILED",
                        "exactly acknowledged result page did not enter cleanup namespace",
                    ));
                }
            };
        Ok((intent, marker, page_sha256))
    }

    fn stage_acknowledged_release_operation_cleanup(
        fixture: &CaptureFixture,
        release_operation_id: &str,
    ) -> Result<(CaptureReleaseIntentRecord, PathBuf)> {
        let outcome = fixture.release(release_operation_id)?;
        fixture.acknowledge_release(release_operation_id, &outcome)?;
        let release_directory = operation_directory(&fixture.store, release_operation_id);
        let intent = read_capture_release_intent(&release_directory, &fixture.target_id)?;
        remove_release_tombstone(&fixture.store, &intent)?;
        for output in &intent.evidence.outputs {
            for artifact in &output.pages {
                match remove_release_result_page_directory(
                    &fixture.store,
                    &intent,
                    output,
                    artifact,
                )? {
                    ReleaseResultPageCleanup::AlreadyRemoved => {}
                    ReleaseResultPageCleanup::Unacknowledged => {
                        return Err(Error::new(
                            "FIXTURE_FAILED",
                            "acknowledged fixture result page was unexpectedly pending",
                        ));
                    }
                    ReleaseResultPageCleanup::Ready(_)
                    | ReleaseResultPageCleanup::CleanupMarker(_) => {
                        return Err(Error::new(
                            "FIXTURE_FAILED",
                            "result-page cleanup did not complete its authorized transition",
                        ));
                    }
                }
            }
        }
        let marker = begin_release_operation_cleanup(&fixture.store, &intent)?
            .ok_or_else(|| Error::new("FIXTURE_FAILED", "release cleanup marker missing"))?;
        Ok((intent, marker))
    }

    // ---- D1: restart scans must use the validated intent's target ID ----

    /// Before the repair a tombstone directory name was used as the target
    /// Operation ID, so every restart scan rejected the tombstone the release
    /// path itself had created. This drives the real scans over a retained
    /// release acknowledgement.
    #[test]
    fn restart_scans_converge_a_retained_release_tombstone() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d1", &"a".repeat(64))?;
        let release_id = "fixture-release-d1";
        let outcome = fixture.release(release_id)?;
        let hash = fixture.acknowledge_release(release_id, &outcome)?;

        assert!(tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(fixture.store.read_outcome(release_id)?.is_some());

        fixture
            .store
            .finalize_release_after_outcome_ack(release_id, &hash)?;
        assert!(!tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(!operation_directory(&fixture.store, release_id).exists());
        assert!(!operation_directory(&fixture.store, &fixture.target_id).exists());

        // The same convergence is reached through the restart scan.
        fixture.store.finalize_acknowledged_releases()?;
        Ok(())
    }

    /// A tombstone whose directory name is not the digest of the target
    /// Operation named by its intent is damaged evidence, not a coalesced
    /// target. The scan must refuse it and must not delete it.
    #[test]
    fn restart_scan_refuses_a_tombstone_whose_name_differs_from_its_intent() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d1b", &"a".repeat(64))?;
        let release_id = "fixture-release-d1b";
        let outcome = fixture.release(release_id)?;
        let hash = fixture.acknowledge_release(release_id, &outcome)?;

        let tombstone = tombstone_directory(&fixture.store, &fixture.target_id);
        let forged = fixture.store.root.join("releasing-forged-name");
        fs::rename(&tombstone, &forged)?;

        let error = fixture.store.finalize_acknowledged_releases().unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_INVALID");
        assert!(
            forged.exists(),
            "damaged tombstone must not be deleted: {error:?}"
        );

        fs::rename(&forged, &tombstone)?;
        fixture
            .store
            .finalize_release_after_outcome_ack(release_id, &hash)?;
        assert!(!tombstone.exists());
        Ok(())
    }

    // ---- D2: an existing tombstone may not coexist with its original ----

    /// A crash, a manual copy, or a partial rename can leave both the original
    /// target directory and its tombstone present. That is the ambiguous
    /// state the contract forbids resolving automatically: the retry must
    /// refuse and must delete nothing.
    #[test]
    fn retry_refuses_when_the_original_and_tombstone_both_exist() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d2", &"b".repeat(64))?;
        let release_id = "fixture-release-d2";
        fixture.release(release_id)?;

        let recreated = operation_directory(&fixture.store, &fixture.target_id);
        fs::create_dir(&recreated)?;
        write_new_json(&recreated.join("outcome.json"), &json!({}))?;

        let error = fixture.release(release_id).unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_CONFLICT");
        let tombstone = tombstone_directory(&fixture.store, &fixture.target_id);
        assert!(
            tombstone.exists(),
            "ambiguous release must not be deleted: {error:?}"
        );
        assert!(
            recreated.join("outcome.json").exists(),
            "the original directory must be left intact: {error:?}"
        );
        Ok(())
    }

    /// Once the original directory is really gone, the same retry is the
    /// legitimate crash-after-rename path and must converge against the
    /// retained intent without replaying the native command.
    #[test]
    fn retry_without_the_original_resumes_the_same_release() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d2b", &"b".repeat(64))?;
        let release_id = "fixture-release-d2b";
        let first = fixture.release(release_id)?;
        let repeat = fixture.release(release_id)?;
        assert_eq!(
            serde_json::to_value(&first).expect("first release outcome serializes"),
            serde_json::to_value(&repeat).expect("repeated release outcome serializes")
        );
        assert!(
            !operation_directory(&fixture.store, &fixture.target_id).exists(),
            "the original directory must stay retired"
        );
        Ok(())
    }

    /// A non-directory at the original digest path is not proof of absence.
    /// Retry must refuse before it removes either captured stream or the
    /// unexpected original bytes.
    #[test]
    fn retry_refuses_unexpected_original_file_without_deleting_streams() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d2c", &"b".repeat(64))?;
        let release_id = "fixture-release-d2c";
        fixture.release(release_id)?;

        let tombstone = tombstone_directory(&fixture.store, &fixture.target_id);
        // Model the crash after the rename and before capture unlinking using
        // the exact bytes already covered by the retained release intent.
        fs::write(tombstone.join("stdout.ndjson"), fixture.stdout_bytes())?;
        fs::write(tombstone.join("stderr.txt"), fixture.stderr_bytes())?;
        let stdout = fs::read(tombstone.join("stdout.ndjson"))?;
        let stderr = fs::read(tombstone.join("stderr.txt"))?;
        let original = operation_directory(&fixture.store, &fixture.target_id);
        let unexpected = b"unexpected original-path bytes";
        fs::write(&original, unexpected)?;

        let error = fixture.release(release_id).unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_CONFLICT");
        assert_eq!(fs::read(&original)?, unexpected.to_vec());
        assert_eq!(fs::read(tombstone.join("stdout.ndjson"))?, stdout);
        assert_eq!(fs::read(tombstone.join("stderr.txt"))?, stderr);
        assert!(tombstone.exists());
        Ok(())
    }

    /// The release Operation's exact intent and ACK remain discoverable after
    /// the tombstone has been removed, so restart completes page and Operation
    /// cleanup and repeated restarts converge to the same empty run store.
    #[test]
    fn restart_cleanup_converges_after_tombstone_removal() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d5", &"e".repeat(64))?;
        let release_id = "fixture-release-d5";
        let outcome = fixture.release(release_id)?;
        fixture.acknowledge_release(release_id, &outcome)?;
        let release_directory = operation_directory(&fixture.store, release_id);
        let intent = read_capture_release_intent(&release_directory, &fixture.target_id)?;
        let result_operation_ids: Vec<_> = intent
            .evidence
            .outputs
            .iter()
            .flat_map(|output| output.pages.iter())
            .map(|page| page.result_operation_id.clone())
            .collect();
        assert_eq!(result_operation_ids.len(), 2);

        // Model a crash after target tombstone removal and before page or
        // release-Operation cleanup. The real release Operation and page
        // directories remain as the only restart-discovery evidence.
        remove_release_tombstone(&fixture.store, &intent)?;
        assert!(!tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(release_directory.exists());
        for result_operation_id in &result_operation_ids {
            assert!(operation_directory(&fixture.store, result_operation_id).exists());
        }

        let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted.finalize_acknowledged_releases()?;
        assert!(!release_directory.exists());
        for result_operation_id in &result_operation_ids {
            assert!(!operation_directory(&restarted, result_operation_id).exists());
        }

        let restarted_again = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted_again.finalize_acknowledged_releases()?;
        assert!(
            fs::read_dir(&restarted_again.root)?
                .next()
                .transpose()?
                .is_none(),
            "repeated restart cleanup must converge without retained adapter state"
        );
        Ok(())
    }

    /// The acknowledged release Operation recovers an older partially
    /// removed tombstone even when recursive cleanup already removed its
    /// local intent, target admission, outcome, and ACK files.
    #[test]
    fn restart_cleanup_converges_after_partial_tombstone_removal_with_missing_authority_files()
    -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d7", &"7".repeat(64))?;
        let release_id = "fixture-release-d7";
        let outcome = fixture.release(release_id)?;
        fixture.acknowledge_release(release_id, &outcome)?;

        let tombstone = tombstone_directory(&fixture.store, &fixture.target_id);
        for name in [
            "release-intent.json",
            "admission.json",
            "outcome.json",
            "ack.json",
        ] {
            fs::remove_file(tombstone.join(name))?;
        }
        let retained_record = tombstone.join("run.json");
        let retained_bytes = fs::read(&retained_record)?;

        let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted.finalize_acknowledged_releases()?;
        assert!(!tombstone.exists());
        assert!(!operation_directory(&restarted, release_id).exists());
        assert!(!retained_record.exists());
        assert_ne!(retained_bytes, Vec::<u8>::new());

        let restarted_again = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted_again.finalize_acknowledged_releases()?;
        assert!(
            fs::read_dir(&restarted_again.root)?
                .next()
                .transpose()?
                .is_none()
        );
        Ok(())
    }

    /// The reserved namespace is entered only after exact ACK validation.
    /// Once there, staged cleanup can resume after the ACK, outcome, and
    /// admission have already been removed, using the intent as the last file.
    #[test]
    fn restart_cleanup_converges_after_partial_release_operation_removal_with_missing_ack_outcome_and_admission()
    -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d8", &"8".repeat(64))?;
        let release_id = "fixture-release-d8";
        let (intent, marker) = stage_acknowledged_release_operation_cleanup(&fixture, release_id)?;
        let expected_marker_name = release_operation_cleanup_marker_name(release_id);
        assert_eq!(
            marker.file_name().and_then(|name| name.to_str()),
            Some(expected_marker_name.as_str())
        );
        for name in ["ack.json", "outcome.json", "admission.json", "run.json"] {
            let path = marker.join(name);
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        assert!(marker.join("release-intent.json").exists());
        assert_eq!(
            read_capture_release_intent(&marker, &intent.target_operation_id)?,
            intent
        );

        let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted.finalize_acknowledged_releases()?;
        assert!(!marker.exists());
        assert!(!operation_directory(&restarted, release_id).exists());

        let restarted_again = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted_again.finalize_acknowledged_releases()?;
        assert!(
            fs::read_dir(&restarted_again.root)?
                .next()
                .transpose()?
                .is_none()
        );
        Ok(())
    }

    /// A crash after deleting the last intent but before the nonrecursive
    /// directory removal leaves only the reserved empty marker to converge.
    #[test]
    fn restart_cleanup_converges_after_release_cleanup_intent_removal_before_rmdir() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d9", &"9".repeat(64))?;
        let release_id = "fixture-release-d9";
        let (_intent, marker) = stage_acknowledged_release_operation_cleanup(&fixture, release_id)?;
        for name in RELEASE_OPERATION_FILES {
            let path = marker.join(name);
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        assert!(fs::read_dir(&marker)?.next().transpose()?.is_none());

        let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted.finalize_acknowledged_releases()?;
        assert!(!marker.exists());
        let restarted_again = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        restarted_again.finalize_acknowledged_releases()?;
        assert!(
            fs::read_dir(&restarted_again.root)?
                .next()
                .transpose()?
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn startup_recovers_intentless_tombstone_before_pending_outcome_scan() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-r47-startup-a", &"a".repeat(64))?;
        let release_id = "fixture-release-r47-startup-a";
        let outcome = fixture.release(release_id)?;
        fixture.acknowledge_release(release_id, &outcome)?;

        let tombstone = tombstone_directory(&fixture.store, &fixture.target_id);
        fs::remove_file(tombstone.join("release-intent.json"))?;
        let error = fixture.store.pending_outcomes().unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_INVALID");

        let pending = crate::adapter::pending_startup_outcomes(&fixture.store)?;
        assert!(pending.is_empty());
        assert!(!tombstone.exists());
        assert!(!operation_directory(&fixture.store, release_id).exists());
        Ok(())
    }

    #[test]
    fn startup_recovers_acknowledged_cleanup_directory_before_pending_outcome_scan() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-r47-startup-b", &"b".repeat(64))?;
        let release_id = "fixture-release-r47-startup-b";
        let (_intent, marker) = stage_acknowledged_release_operation_cleanup(&fixture, release_id)?;
        assert!(marker.join("outcome.json").exists());

        let error = fixture.store.pending_outcomes().unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_INVALID");

        let pending = crate::adapter::pending_startup_outcomes(&fixture.store)?;
        assert!(pending.is_empty());
        assert!(!marker.exists());
        Ok(())
    }

    #[test]
    fn acknowledged_duplicate_result_page_operations_are_all_removed_after_release() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-r47-duplicate", &"c".repeat(64))?;
        let mut evidence = fixture.release_evidence()?;
        let duplicate_operation_id = "fixture-result-stdout-duplicate";
        let duplicate_input_sha256 = "f".repeat(64);
        let duplicate_artifact_ref = format!("artifact:{duplicate_operation_id}");
        let output = &mut evidence.outputs[0];
        let bytes = fixture.stdout_bytes();
        let total = bytes.len() as u64;
        let params = json!({
            "operation_id":duplicate_operation_id,
            "page":{
                "source":{
                    "kind":"command_output",
                    "result_operation_id":duplicate_operation_id,
                    "result_input_sha256":duplicate_input_sha256,
                    "input_operation_id":fixture.target_id,
                    "target_input_sha256":fixture.target_input_sha256,
                    "target_command_output":output.target_command_output
                },
                "offset_bytes":0,
                "byte_length":total,
                "total_bytes":total,
                "eof":true,
                "media_type":"application/x-ndjson",
                "content_base64":STANDARD.encode(bytes),
                "page_sha256":digest(bytes)
            }
        });
        fixture.store.admit(
            duplicate_operation_id,
            None,
            &fixture.binding_id,
            fixture.generation,
            &fixture.route,
        )?;
        let page_sha256 = fixture
            .store
            .save_result_page(duplicate_operation_id, &params)?;
        fixture.store.acknowledge_result_page(
            duplicate_operation_id,
            &page_sha256,
            json!({"recorded":true,"artifact_ref":duplicate_artifact_ref}),
        )?;
        output.pages.push(CommandCaptureArtifactPage {
            result_operation_id: duplicate_operation_id.to_owned(),
            result_input_sha256: duplicate_input_sha256,
            artifact_ref: duplicate_artifact_ref,
            artifact_sha256: digest(bytes),
            artifact_byte_length: total,
            offset_bytes: 0,
            byte_length: total,
            total_bytes: total,
            page_sha256: digest(bytes),
            eof: true,
        });

        let original_operation_id = output.pages[0].result_operation_id.clone();
        assert_eq!(output.pages[0].offset_bytes, output.pages[1].offset_bytes);
        assert_eq!(output.pages[0].byte_length, output.pages[1].byte_length);
        assert_ne!(original_operation_id, duplicate_operation_id);
        let release_operation_id = "fixture-release-r47-duplicate";
        fixture.store.admit(
            release_operation_id,
            None,
            &fixture.binding_id,
            fixture.generation,
            &fixture.route,
        )?;
        let outcome = fixture.store.release_command_captures(
            release_operation_id,
            RELEASE_INPUT_SHA256,
            &fixture.binding_id,
            fixture.generation,
            &fixture.route,
            &evidence,
        )?;
        let outcome_sha256 = fixture.store.save_outcome(&outcome)?;
        fixture.store.acknowledge(
            release_operation_id,
            &outcome_sha256,
            json!({"recorded":true}),
        )?;
        fixture
            .store
            .finalize_release_after_outcome_ack(release_operation_id, &outcome_sha256)?;

        assert!(!operation_directory(&fixture.store, &original_operation_id).exists());
        assert!(!operation_directory(&fixture.store, duplicate_operation_id).exists());
        assert!(!operation_directory(&fixture.store, release_operation_id).exists());
        assert!(!tombstone_directory(&fixture.store, &fixture.target_id).exists());
        Ok(())
    }

    /// Unknown marker contents are rejected before any exact allowed file or
    /// foreign bytes are removed.
    #[test]
    fn restart_cleanup_rejects_foreign_release_cleanup_residue_without_deleting_bytes() -> Result<()>
    {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d10", &"a".repeat(64))?;
        let release_id = "fixture-release-d10";
        let (_intent, marker) = stage_acknowledged_release_operation_cleanup(&fixture, release_id)?;
        let foreign = marker.join("foreign-evidence.bin");
        let foreign_bytes = b"unlinked release cleanup residue";
        fs::write(&foreign, foreign_bytes)?;

        let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let error = restarted.finalize_acknowledged_releases().unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_INVALID");
        assert_eq!(fs::read(&foreign)?, foreign_bytes.to_vec());
        assert!(marker.join("release-intent.json").exists());
        assert!(marker.join("ack.json").exists());
        assert!(marker.join("outcome.json").exists());
        assert!(marker.join("admission.json").exists());
        assert!(marker.exists());
        Ok(())
    }

    /// Missing tombstone authority is not enough to clean residue when the
    /// release Operation itself has no exact durable ACK.
    #[test]
    fn restart_cleanup_preserves_intentless_tombstone_without_release_ack() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d11", &"b".repeat(64))?;
        let release_id = "fixture-release-d11";
        fixture.release(release_id)?;

        let tombstone = tombstone_directory(&fixture.store, &fixture.target_id);
        let retained_record = tombstone.join("run.json");
        let retained_bytes = fs::read(&retained_record)?;
        fs::remove_file(tombstone.join("release-intent.json"))?;
        let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let error = restarted.finalize_acknowledged_releases().unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_INVALID");
        assert_eq!(fs::read(&retained_record)?, retained_bytes);
        assert!(tombstone.exists());
        assert!(operation_directory(&restarted, release_id).exists());
        Ok(())
    }

    /// A release ACK cannot substitute for a missing result-page ACK in the
    /// local journal. Keep the page and its release anchor until that exact
    /// ACK is restored, then allow a later cleanup pass to converge.
    #[test]
    fn release_cleanup_preserves_unacknowledged_or_foreign_result_page() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d6", &"f".repeat(64))?;
        let release_id = "fixture-release-d6";
        let outcome = fixture.release(release_id)?;
        let release_hash = fixture.acknowledge_release(release_id, &outcome)?;
        let release_directory = operation_directory(&fixture.store, release_id);
        let intent = read_capture_release_intent(&release_directory, &fixture.target_id)?;
        let page = &intent.evidence.outputs[0].pages[0];
        let page_directory = operation_directory(&fixture.store, &page.result_operation_id);
        let page_path = page_directory.join("result-page.json");
        let acknowledgement_path = page_directory.join("result-page-ack.json");
        let page_bytes = fs::read(&page_path)?;
        let acknowledgement_bytes = fs::read(&acknowledgement_path)?;
        fs::remove_file(&acknowledgement_path)?;

        fixture
            .store
            .finalize_release_after_outcome_ack(release_id, &release_hash)?;
        assert!(tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(release_directory.exists());
        assert_eq!(fs::read(&page_path)?, page_bytes);
        assert!(!acknowledgement_path.exists());

        fs::write(&acknowledgement_path, acknowledgement_bytes)?;
        let foreign_path = page_directory.join("foreign-page-evidence.bin");
        let foreign_bytes = b"unlinked foreign evidence";
        fs::write(&foreign_path, foreign_bytes)?;
        let error = fixture
            .store
            .finalize_release_after_outcome_ack(release_id, &release_hash)
            .unwrap_err();
        assert_eq!(error.code, "ADAPTER_EVIDENCE_INVALID");
        assert!(tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(release_directory.exists());
        assert_eq!(fs::read(&page_path)?, page_bytes);
        assert_eq!(fs::read(&foreign_path)?, foreign_bytes.to_vec());

        fs::remove_file(&foreign_path)?;
        fixture
            .store
            .finalize_release_after_outcome_ack(release_id, &release_hash)?;
        assert!(!tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(!release_directory.exists());
        assert!(!page_directory.exists());
        Ok(())
    }

    /// Restart cleanup resumes after the exact page ACK has authorized the
    /// operation-directory rename, even when an allowlisted file was already
    /// removed or the reserved cleanup directory is empty.
    #[test]
    fn restart_cleanup_converges_after_partial_result_page_removal_in_cleanup_namespace()
    -> Result<()> {
        for missing in ["ack", "body", "admission", "empty"] {
            let _state = TempState::new();
            let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
            let target_id = format!("fixture-target-r47-page-{missing}");
            let release_id = format!("fixture-release-r47-page-{missing}");
            let fixture = CaptureFixture::new(store, &target_id, &"f".repeat(64))?;
            let (intent, marker, page_sha256) =
                stage_first_result_page_cleanup(&fixture, &release_id)?;
            let page_operation_id = intent.evidence.outputs[0].pages[0]
                .result_operation_id
                .clone();
            let original = operation_directory(&fixture.store, &page_operation_id);
            assert!(!original.exists(), "the authorized rename must retire op-*");
            assert!(
                marker.exists(),
                "the cleanup marker must remain discoverable"
            );
            assert!(operation_directory(&fixture.store, &release_id).exists());

            let acknowledgement = marker.join(result_page_cleanup_ack_name(&page_sha256));
            match missing {
                "ack" => fs::remove_file(&acknowledgement)?,
                "body" => {
                    assert!(acknowledgement.is_file());
                    fs::remove_file(marker.join("result-page.json"))?;
                }
                "admission" => fs::remove_file(marker.join("admission.json"))?,
                "empty" => {
                    fs::remove_file(marker.join("result-page.json"))?;
                    fs::remove_file(marker.join("admission.json"))?;
                    fs::remove_file(acknowledgement)?;
                    assert!(fs::read_dir(&marker)?.next().transpose()?.is_none());
                }
                _ => unreachable!("the fixture case list is fixed"),
            }

            let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
            restarted.finalize_acknowledged_releases()?;
            assert!(!marker.exists());
            assert!(!operation_directory(&restarted, &release_id).exists());
            assert!(!tombstone_directory(&restarted, &target_id).exists());
            assert!(restarted.pending_result_pages()?.is_empty());
            assert!(fs::read_dir(&restarted.root)?.next().transpose()?.is_none());
        }
        Ok(())
    }

    /// A cleanup namespace is not permission to discard foreign bytes or a
    /// surviving page ACK whose artifact reference no longer matches intent.
    #[test]
    fn restart_cleanup_rejects_foreign_or_mismatched_result_page_cleanup_residue() -> Result<()> {
        for (residue, expected_error) in [
            ("foreign", "ADAPTER_EVIDENCE_INVALID"),
            ("mismatched-ack", "COMMAND_RELEASE_ARTIFACT_UNACKNOWLEDGED"),
        ] {
            let _state = TempState::new();
            let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
            let target_id = format!("fixture-target-r47-residue-{residue}");
            let release_id = format!("fixture-release-r47-residue-{residue}");
            let fixture = CaptureFixture::new(store, &target_id, &"a".repeat(64))?;
            let (intent, marker, page_sha256) =
                stage_first_result_page_cleanup(&fixture, &release_id)?;
            let release_directory = operation_directory(&fixture.store, &release_id);
            let tombstone = tombstone_directory(&fixture.store, &target_id);
            let ack_path = marker.join(result_page_cleanup_ack_name(&page_sha256));

            let (retained_path, retained_bytes) = if residue == "foreign" {
                let path = marker.join("foreign-page-evidence.bin");
                let bytes = b"foreign result-page cleanup residue";
                fs::write(&path, bytes)?;
                (path, bytes.to_vec())
            } else {
                let mut ack: Value = read_json(&ack_path)?;
                ack["acknowledgement"]["artifact_ref"] = json!("artifact:wrong-reference");
                let bytes = serde_json::to_vec(&ack)?;
                fs::write(&ack_path, &bytes)?;
                (ack_path.clone(), bytes)
            };

            let restarted = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
            let error = restarted.finalize_acknowledged_releases().unwrap_err();
            assert_eq!(error.code, expected_error);
            assert!(marker.exists());
            assert!(release_directory.exists());
            assert!(tombstone.exists());
            assert_eq!(fs::read(&retained_path)?, retained_bytes);
            assert_eq!(
                read_capture_release_intent(&release_directory, &target_id)?,
                intent
            );
        }
        Ok(())
    }

    // ---- D3: acknowledged releases must not retain per-run state ----

    /// After the host acknowledgement is durable, no per-run adapter state
    /// may survive: neither the target tombstone nor the release Operation's
    /// own admission/intent/outcome/acknowledgement directory.
    #[test]
    fn acknowledged_release_leaves_no_per_run_adapter_state() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d3", &"c".repeat(64))?;
        let release_id = "fixture-release-d3";
        let outcome = fixture.release(release_id)?;
        let hash = fixture.acknowledge_release(release_id, &outcome)?;

        assert!(operation_directory(&fixture.store, release_id).exists());

        fixture
            .store
            .finalize_release_after_outcome_ack(release_id, &hash)?;
        assert!(!tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(!operation_directory(&fixture.store, release_id).exists());
        assert!(!operation_directory(&fixture.store, &fixture.target_id).exists());

        fixture.store.finalize_acknowledged_releases()?;
        fixture.store.finalize_acknowledged_releases()?;
        assert!(
            fs::read_dir(&fixture.store.root)?
                .into_iter()
                .next()
                .transpose()?
                .is_none(),
            "an acknowledged release must leave the private run store empty"
        );
        Ok(())
    }

    /// Unacknowledged release evidence is never discarded. A release whose
    /// outcome is still pending keeps both its tombstone and its own
    /// admission/intent records so the acknowledgement can still converge.
    #[test]
    fn unacknowledged_release_evidence_is_never_deleted() -> Result<()> {
        let _state = TempState::new();
        let store = RunStore::new_for_profile(&_state.0, Profile::BatchV3)?;
        let fixture = CaptureFixture::new(store, "fixture-target-d4", &"d".repeat(64))?;
        let release_id = "fixture-release-d4";
        let outcome = fixture.release(release_id)?;
        let hash = fixture.store.save_outcome(&outcome)?;
        let intent = read_capture_release_intent(
            &operation_directory(&fixture.store, release_id),
            &fixture.target_id,
        )?;
        let result_operation_ids: Vec<_> = intent
            .evidence
            .outputs
            .iter()
            .flat_map(|output| output.pages.iter())
            .map(|page| page.result_operation_id.clone())
            .collect();
        let result_page_bytes: Vec<_> = result_operation_ids
            .iter()
            .map(|operation_id| {
                fs::read(operation_directory(&fixture.store, operation_id).join("result-page.json"))
            })
            .collect::<std::io::Result<_>>()?;

        fixture.store.finalize_acknowledged_releases()?;
        let cleanup_error = begin_release_operation_cleanup(&fixture.store, &intent).unwrap_err();
        assert_eq!(cleanup_error.code, "ADAPTER_EVIDENCE_INVALID");
        assert!(operation_directory(&fixture.store, release_id).exists());
        assert!(
            !fixture
                .store
                .root
                .join(release_operation_cleanup_marker_name(release_id))
                .exists(),
            "an unacknowledged Operation must never enter the cleanup namespace"
        );
        assert!(
            tombstone_directory(&fixture.store, &fixture.target_id).exists(),
            "a pending release acknowledgement must keep its tombstone"
        );
        assert!(
            operation_directory(&fixture.store, release_id).exists(),
            "a pending release acknowledgement must keep its own records"
        );
        for (operation_id, expected_page) in result_operation_ids.iter().zip(&result_page_bytes) {
            let directory = operation_directory(&fixture.store, operation_id);
            assert!(
                directory.exists(),
                "an unacknowledged release keeps page directories"
            );
            assert_eq!(
                fs::read(directory.join("result-page.json"))?,
                expected_page.clone()
            );
        }

        fixture
            .store
            .acknowledge(release_id, &hash, json!({"recorded":true}))?;
        fixture.store.finalize_acknowledged_releases()?;
        assert!(!tombstone_directory(&fixture.store, &fixture.target_id).exists());
        assert!(!operation_directory(&fixture.store, release_id).exists());
        for operation_id in &result_operation_ids {
            assert!(!operation_directory(&fixture.store, operation_id).exists());
        }
        Ok(())
    }
}
