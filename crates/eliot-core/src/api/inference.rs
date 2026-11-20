//! Closed Increment M1 wire contract. No provider or filesystem I/O.
use crate::{Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Lowercase hexadecimal SHA-256, shared by every digest boundary.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub const INFERENCE_PROFILE: &str = "memory-text-v1";
pub const MAX_INFERENCE_INPUT: usize = 32 * 1024;
pub const MAX_INFERENCE_RESULT: usize = 64 * 1024;
pub const MAX_INFERENCE_RANGE: usize = 16 * 1024;
pub const MAX_INFERENCE_CALLS: i64 = 1024;
pub const MAX_INFERENCE_RETENTION_RECEIPTS: i64 = 1024;
pub const MAX_INFERENCE_CLIENTS: i64 = 16;
pub const MAX_INFERENCE_DEADLINE_MS: u64 = 120_000;

pub fn inference_id(id: &str) -> Result<()> {
    if id.len() != 26
        || !matches!(id.as_bytes()[0], b'0'..=b'7')
        || !id
            .bytes()
            .all(|b| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&b))
    {
        return Err(Error::new("invalid", "expected a literal ULID"));
    }
    Ok(())
}

pub fn inference_client(id: &str) -> Result<()> {
    let suffix = id.strip_prefix("memory:").unwrap_or("");
    inference_id(suffix)
}

pub fn inference_digest(digest: &str) -> Result<()> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Error::new("invalid", "expected a lowercase SHA-256 digest"));
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceTextRequest {
    pub id: String,
    pub profile: String,
    /// Non-content local origin identifier, not an arbitrary diagnostic.
    pub origin: String,
    pub disclosure: InferenceDisclosure,
    pub text: String,
    pub input_sha256: String,
    pub input_bytes: usize,
    pub response_format: InferenceFormat,
    pub max_result_bytes: usize,
    pub deadline_ms: u64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InferenceDisclosure {
    ProviderAllowed,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InferenceFormat {
    Text,
}

impl InferenceTextRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.id)?;
        inference_digest(&self.input_sha256)?;
        if self.profile != INFERENCE_PROFILE {
            return Err(Error::new("unsupported", "unknown inference profile"));
        }
        if self.origin.is_empty()
            || self.origin.len() > 128
            || !self
                .origin
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_:-.".contains(&b))
            || self.text.is_empty()
            || self.text.len() > MAX_INFERENCE_INPUT
            || self.text.contains('\0')
            || self.input_bytes != self.text.len()
            || self.input_sha256 != sha256_hex(self.text.as_bytes())
            || self.max_result_bytes == 0
            || self.max_result_bytes > MAX_INFERENCE_RESULT
            || !(1000..=MAX_INFERENCE_DEADLINE_MS).contains(&self.deadline_ms)
        {
            return Err(Error::new(
                "invalid",
                "invalid inference input, digest or finite limits",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceReceipt {
    pub call_id: String,
    pub admission_seq: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InferenceState {
    Queued,
    Sending,
    Succeeded,
    Failed,
    Cancelled,
    Unknown,
    Released,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceCall {
    pub call_id: String,
    pub admission_seq: i64,
    pub state: InferenceState,
    pub provider_attempts: u32,
    pub native_session_id: Option<String>,
    pub native_turns: Option<u32>,
    pub result_sha256: Option<String>,
    pub result_bytes: Option<usize>,
    pub cleanup_complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceGetRequest {
    pub call_id: String,
}
impl InferenceGetRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.call_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceWriteGetRequest {
    pub id: String,
}
impl InferenceWriteGetRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InferenceWriteStatus {
    Retained,
    Absent,
    HistoryGap,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceWriteResponse {
    pub id: String,
    pub status: InferenceWriteStatus,
    pub method: Option<String>,
    pub payload_sha256: Option<String>,
    /// Original bounded mutation response, never a prompt or result text.
    pub result: Option<InferenceWriteResult>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum InferenceWriteResult {
    Receipt(InferenceReceipt),
    Mutation(InferenceMutationResponse),
    Retention(InferenceRetentionResponse),
}

// M1.2 fixed-window inventory and retention DTOs.
pub const INFERENCE_INVENTORY_QUERY_VERSION: u32 = 1;
pub const MAX_INFERENCE_INVENTORY_PAGE: usize = 100;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceHighWaterRequest {}

impl InferenceHighWaterRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceHighWaterResponse {
    pub client_id: String,
    pub call_high_water_seq: i64,
    pub retain_after_seq: i64,
    pub write_high_water_seq: i64,
    pub write_retain_after_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceInventoryCursor {
    pub client_id: String,
    pub call_after_seq: i64,
    pub call_through_seq: i64,
    pub write_after_seq: i64,
    pub write_through_seq: i64,
    pub limit: usize,
    pub query_version: u32,
    pub last_call_seq: i64,
    pub last_write_seq: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceListRequest {
    /// Exclusive start of the fixed call interval.
    pub call_after_seq: i64,
    /// Inclusive high-water captured by `inference.high_water`.
    pub call_through_seq: i64,
    /// Exclusive start of the fixed write interval.
    pub write_after_seq: i64,
    /// Inclusive high-water captured by `inference.high_water`.
    pub write_through_seq: i64,
    pub limit: usize,
    pub cursor: Option<InferenceInventoryCursor>,
}

impl InferenceListRequest {
    pub fn validate(&self) -> Result<()> {
        if self.call_after_seq < 0
            || self.call_through_seq < self.call_after_seq
            || self.write_after_seq < 0
            || self.write_through_seq < self.write_after_seq
            || !(1..=MAX_INFERENCE_INVENTORY_PAGE).contains(&self.limit)
        {
            return Err(Error::new(
                "invalid",
                "invalid inference inventory interval or page limit",
            ));
        }
        if let Some(cursor) = &self.cursor {
            inference_client(&cursor.client_id)?;
            if cursor.call_after_seq != self.call_after_seq
                || cursor.call_through_seq != self.call_through_seq
                || cursor.write_after_seq != self.write_after_seq
                || cursor.write_through_seq != self.write_through_seq
                || cursor.limit != self.limit
                || cursor.query_version != INFERENCE_INVENTORY_QUERY_VERSION
                || cursor.last_call_seq < self.call_after_seq
                || cursor.last_call_seq > self.call_through_seq
                || cursor.last_write_seq < self.write_after_seq
                || cursor.last_write_seq > self.write_through_seq
            {
                return Err(Error::new(
                    "invalid",
                    "inference inventory cursor does not match its fixed interval",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceWriteInventoryItem {
    pub write_seq: i64,
    pub id: String,
    pub method: String,
    pub payload_sha256: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceListResponse {
    pub client_id: String,
    pub calls: Vec<InferenceCall>,
    pub writes: Vec<InferenceWriteInventoryItem>,
    pub next: Option<InferenceInventoryCursor>,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceRetentionRequest {
    pub id: String,
    pub observed_call_high_water_seq: i64,
    pub observed_write_high_water_seq: i64,
    /// Memory's backup/fence owner assertion, serialized under its own lock.
    pub safe_retain_after_seq: i64,
    pub retain_after_seq: i64,
    pub safe_write_retain_after_seq: i64,
    pub write_retain_after_seq: i64,
    pub limit: usize,
}

impl InferenceRetentionRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.id)?;
        if self.observed_call_high_water_seq < 0
            || self.observed_write_high_water_seq < 0
            || self.safe_retain_after_seq < 0
            || self.retain_after_seq < 0
            || self.safe_write_retain_after_seq < 0
            || self.write_retain_after_seq < 0
            || self.safe_retain_after_seq > self.observed_call_high_water_seq
            || self.retain_after_seq > self.safe_retain_after_seq
            || self.safe_write_retain_after_seq > self.observed_write_high_water_seq
            || self.write_retain_after_seq > self.safe_write_retain_after_seq
            || !(1..=MAX_INFERENCE_INVENTORY_PAGE).contains(&self.limit)
        {
            return Err(Error::new(
                "invalid",
                "invalid inference retention cut or bound",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceRetentionResponse {
    pub retain_after_seq: i64,
    pub write_retain_after_seq: i64,
    pub pruned_calls: usize,
    pub pruned_writes: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceResultRequest {
    pub call_id: String,
    pub result_sha256: String,
    pub offset: usize,
    pub length: usize,
}
impl InferenceResultRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.call_id)?;
        inference_digest(&self.result_sha256)?;
        if self.length == 0
            || self.length > MAX_INFERENCE_RANGE
            || self.offset > MAX_INFERENCE_RESULT
        {
            return Err(Error::new("invalid", "result range exceeds finite bounds"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceResultResponse {
    pub call_id: String,
    pub result_sha256: String,
    pub total_bytes: usize,
    pub offset: usize,
    pub bytes: Vec<u8>,
    pub eof: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceCancelRequest {
    pub id: String,
    pub call_id: String,
}
impl InferenceCancelRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.id)?;
        inference_id(&self.call_id)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InferenceDisposition {
    MemoryCopyVerified,
    TerminalWithoutResult,
    AbandonForErasure,
}

/// Memory supplies this exact-call classification while serializing mutations
/// with its backup-set owner. The controller never inspects physical backups.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InferenceBackupDependency {
    NoBackupDependencies,
    ControllerClosed,
    ControllerDependent,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceReleaseRequest {
    pub id: String,
    pub call_id: String,
    pub disposition: InferenceDisposition,
    pub result_sha256: Option<String>,
    /// Retain the original M1 spelling so an admitted release can still replay
    /// its exact semantic binding after upgrade. Supply exactly one assertion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub no_backup_dependencies: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backup_dependency: Option<InferenceBackupDependency>,
}
impl InferenceReleaseRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.id)?;
        inference_id(&self.call_id)?;
        if let Some(digest) = &self.result_sha256 {
            inference_digest(digest)?;
        }
        self.dependency()?;
        Ok(())
    }

    pub fn dependency(&self) -> Result<InferenceBackupDependency> {
        match (self.no_backup_dependencies, self.backup_dependency) {
            (Some(true), None) => Ok(InferenceBackupDependency::NoBackupDependencies),
            (None, Some(dependency)) => Ok(dependency),
            _ => Err(Error::new(
                "invalid",
                "supply exactly one exact-call backup dependency assertion",
            )),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceMutationResponse {
    pub call_id: String,
    pub outcome: InferenceMutationOutcome,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InferenceMutationOutcome {
    CancelledBeforeSending,
    TooLate,
    PublicationFenced,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceCredentialRequest {
    pub id: String,
    pub client_id: String,
    pub expected_version: i64,
}
impl InferenceCredentialRequest {
    pub fn validate(&self) -> Result<()> {
        inference_id(&self.id)?;
        inference_client(&self.client_id)?;
        if self.expected_version < 0 || self.expected_version == i64::MAX {
            return Err(Error::new("invalid", "credential version out of range"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceCredentialResponse {
    pub client_id: String,
    pub token_version: i64,
    pub credential_file: String,
    pub phase: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceIdentityRequest {}
impl InferenceIdentityRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InferenceIdentityResponse {
    pub controller_id: String,
    pub client_id: String,
    pub principal: String,
    pub token_version: i64,
    pub contract_version: String,
}
