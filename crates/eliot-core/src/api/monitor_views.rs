//! Cached monitoring projections. Null counters and coverage remain unknown facts.
use super::SourceState;
use crate::{
    AgentEdit, AgentState, GitBaseline, GitCommit, GitPathStatus, ProcessRecord, ProcessSample,
    WorkspacePhase,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum ObservationView {
    Data(Box<ObservationData>),
    Unavailable {
        #[serde(deserialize_with = "unavailable")]
        available: bool,
        error_code: String,
    },
}

impl ObservationView {
    pub fn unavailable(error_code: impl Into<String>) -> Self {
        Self::Unavailable {
            available: false,
            error_code: error_code.into(),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ObservationData {
    pub processes_truncated: bool,
    #[serde(deserialize_with = "process_records")]
    pub processes: Vec<ProcessRecord>,
    #[serde(deserialize_with = "object")]
    pub stats: ProcessStatsView,
    #[serde(deserialize_with = "object")]
    pub liveness: LivenessView,
    #[serde(deserialize_with = "nullable_object")]
    pub workspace: Option<WorkspaceView>,
    #[serde(deserialize_with = "object")]
    pub git: GitView,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProcessStatsView {
    pub available: bool,
    #[serde(deserialize_with = "nullable")]
    pub sampled_at: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub age_ms: Option<i64>,
    pub ttl_ms: i64,
    pub clock_regressed: bool,
    pub coverage: StatsCoverage,
    #[serde(deserialize_with = "nullable")]
    pub complete: Option<bool>,
    #[serde(deserialize_with = "nullable")]
    pub cpu_time_ns: Option<u64>,
    #[serde(deserialize_with = "nullable")]
    pub rss_bytes: Option<u64>,
    #[serde(deserialize_with = "process_samples")]
    pub samples: Option<Vec<ProcessSample>>,
    #[serde(deserialize_with = "nullable")]
    pub error: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatsCoverage {
    Unknown,
    Stale,
    Complete,
    Partial,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LivenessView {
    pub state: AgentState,
    #[serde(deserialize_with = "nullable")]
    pub last_model_progress: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub age_ms: Option<i64>,
    pub clock_regressed: bool,
    pub basis: LivenessBasis,
    #[serde(deserialize_with = "nullable")]
    pub source_healthy: Option<bool>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivenessBasis {
    CommittedNativeFacts,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum WorkspaceView {
    Data(Box<WorkspaceObservation>),
    Unavailable {
        #[serde(deserialize_with = "unavailable")]
        available: bool,
        error: String,
    },
}

impl WorkspaceView {
    pub fn unavailable(error: impl Into<String>) -> Self {
        Self::Unavailable {
            available: false,
            error: error.into(),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceObservation {
    pub owner: String,
    pub target_generation: i64,
    pub phase: WorkspacePhase,
    pub setup_index: usize,
    pub processes_truncated: bool,
    #[serde(deserialize_with = "process_records")]
    pub processes: Vec<ProcessRecord>,
    #[serde(deserialize_with = "object")]
    pub stats: ProcessStatsView,
    #[serde(deserialize_with = "nullable_object")]
    pub source: Option<SourceState>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum GitView {
    Data(Box<GitObservation>),
    Unavailable {
        #[serde(deserialize_with = "unavailable")]
        available: bool,
        error_code: String,
    },
    Error {
        #[serde(deserialize_with = "unavailable")]
        available: bool,
        error: String,
    },
}

impl GitView {
    pub fn unavailable(error_code: impl Into<String>) -> Self {
        Self::Unavailable {
            available: false,
            error_code: error_code.into(),
        }
    }

    pub fn error(error: impl Into<String>) -> Self {
        Self::Error {
            available: false,
            error: error.into(),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GitObservation {
    pub available: bool,
    pub workspace: String,
    #[serde(deserialize_with = "nullable")]
    pub observed_at: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub age_ms: Option<i64>,
    pub ttl_ms: i64,
    pub clock_regressed: bool,
    #[serde(deserialize_with = "retry_ms")]
    pub retry_after_ms: u128,
    #[serde(deserialize_with = "object")]
    pub filesystem_invalidation: FilesystemInvalidationView,
    #[serde(deserialize_with = "nullable")]
    pub head: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub branch: Option<String>,
    #[serde(deserialize_with = "nullable_objects")]
    pub dirty: Option<Vec<GitPathStatus>>,
    #[serde(deserialize_with = "nullable_objects")]
    pub commits: Option<Vec<GitCommit>>,
    #[serde(deserialize_with = "nullable")]
    pub complete: Option<bool>,
    #[serde(deserialize_with = "nullable")]
    pub history_complete: Option<bool>,
    #[serde(deserialize_with = "nullable")]
    pub history_gap: Option<bool>,
    #[serde(deserialize_with = "nullable")]
    pub base_oid: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub committed_paths: Option<Vec<String>>,
    #[serde(deserialize_with = "nullable")]
    pub base_complete: Option<bool>,
    #[serde(deserialize_with = "nullable_object")]
    pub baseline: Option<GitBaseline>,
    pub edits_truncated: bool,
    #[serde(deserialize_with = "objects")]
    pub edits: Vec<AgentEdit>,
    #[serde(deserialize_with = "nullable_object")]
    pub source: Option<MonitoringSourceState>,
    #[serde(deserialize_with = "nullable")]
    pub error: Option<String>,
}

/// Exact `read_source_state` projection: digests, but no list-view owners.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitoringSourceState {
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub families: Vec<String>,
    pub state: String,
    pub health: String,
    #[serde(deserialize_with = "nullable")]
    pub cursor: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_store_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_digest: Option<Vec<u8>>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_first_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub last_batch_last_seq: Option<i64>,
    #[serde(deserialize_with = "nullable")]
    pub gap_id: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_digest: Option<Vec<u8>>,
    #[serde(deserialize_with = "nullable")]
    pub gap_from: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_to: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub gap_seq: Option<i64>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesystemInvalidationView {
    pub state: FilesystemInvalidationState,
    // An absent watch slot omits this field; an existing inactive slot emits null.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_retry"
    )]
    pub retry_after_ms: Option<Option<u128>>,
}

impl FilesystemInvalidationView {
    pub fn inactive() -> Self {
        Self {
            state: FilesystemInvalidationState::Inactive,
            retry_after_ms: None,
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilesystemInvalidationState {
    Healthy,
    Degraded,
    Unavailable,
    Inactive,
}

fn unavailable<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    let available = bool::deserialize(deserializer)?;
    if available {
        return Err(serde::de::Error::custom(
            "unavailable view must report false",
        ));
    }
    Ok(false)
}

fn nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn present_retry<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<u128>>, D::Error> {
    Option::<u128>::deserialize(deserializer).map(Some)
}

// serde's untagged buffer does not deserialize u128 directly. JSON durations
// already use serde_json's unsigned integer range; retain Duration::as_millis
// at the producer without changing its serialized numeric value.
fn retry_ms<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<u128, D::Error> {
    u64::deserialize(deserializer).map(u128::from)
}

fn object<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> Result<T, D::Error> {
    let value = Map::<String, Value>::deserialize(deserializer)?;
    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
}

fn objects<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> Result<Vec<T>, D::Error> {
    let values = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    values
        .into_iter()
        .map(|value| serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom))
        .collect()
}

fn nullable_object<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    Option::<Map<String, Value>>::deserialize(deserializer)?
        .map(|value| serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom))
        .transpose()
}

fn nullable_objects<'de, D: serde::Deserializer<'de>, T: DeserializeOwned>(
    deserializer: D,
) -> Result<Option<Vec<T>>, D::Error> {
    Option::<Vec<Map<String, Value>>>::deserialize(deserializer)?
        .map(|values| {
            values
                .into_iter()
                .map(|value| {
                    serde_json::from_value(Value::Object(value)).map_err(serde::de::Error::custom)
                })
                .collect()
        })
        .transpose()
}

fn require_process_record_object<E: serde::de::Error>(value: &Value) -> Result<(), E> {
    if !value.is_object() || !value.get("identity").is_some_and(Value::is_object) {
        return Err(E::custom("process record and identity must be objects"));
    }
    Ok(())
}

fn process_records<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<ProcessRecord>, D::Error> {
    Vec::<Value>::deserialize(deserializer)?
        .into_iter()
        .map(|value| {
            require_process_record_object::<D::Error>(&value)?;
            serde_json::from_value(value).map_err(serde::de::Error::custom)
        })
        .collect()
}

fn process_samples<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Vec<ProcessSample>>, D::Error> {
    Option::<Vec<Map<String, Value>>>::deserialize(deserializer)?
        .map(|samples| {
            samples
                .into_iter()
                .map(|sample| {
                    require_process_record_object::<D::Error>(
                        sample.get("record").unwrap_or(&Value::Null),
                    )?;
                    serde_json::from_value(Value::Object(sample)).map_err(serde::de::Error::custom)
                })
                .collect()
        })
        .transpose()
}
