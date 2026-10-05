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

#[derive(Debug, Clone)]
pub struct RunStore {
    root: PathBuf,
}

impl RunStore {
    pub fn new(module_state: &Path) -> Result<Self> {
        let state = fs::canonicalize(module_state)?;
        let root = state.join("command-adapter-runs");
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

    pub fn save_native_evidence(
        &self,
        operation_id: &str,
        stdout: &[u8],
        stderr: &[u8],
        receipt: &Value,
    ) -> Result<()> {
        let dir = self.directory(operation_id)?;
        write_replace_bytes(&dir.join("stdout.ndjson"), stdout)?;
        write_replace_bytes(&dir.join("stderr.txt"), stderr)?;
        write_replace_json(&dir.join("run.json"), receipt)
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
