//! Cached monitoring projections. Null counters and coverage remain unknown facts.
use super::SourceState;
use crate::{
    AgentEdit, AgentState, GitBaseline, GitCommit, GitPathStatus, ProcessRecord, ProcessSample,
    WorkspacePhase,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatsCoverage {
    Unknown,
    Stale,
    Complete,
    Partial,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivenessBasis {
    CommittedNativeFacts,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn stats() -> Value {
        json!({
            "available": false, "sampled_at": null, "age_ms": null, "ttl_ms": 4000,
            "clock_regressed": false, "coverage": "unknown", "complete": null,
            "cpu_time_ns": null, "rss_bytes": null, "samples": null, "error": null
        })
    }

    fn git() -> Value {
        json!({
            "available": false, "workspace": "C:\\repo", "observed_at": 1000,
            "age_ms": 5000, "ttl_ms": 4000, "clock_regressed": false,
            "retry_after_ms": 0, "filesystem_invalidation": {"state":"inactive"},
            "head": "abc", "branch": "main", "dirty": [], "commits": [],
            "complete": false, "history_complete": false, "history_gap": true,
            "base_oid": null, "committed_paths": [], "base_complete": false,
            "baseline": {"head":null,"updated_at":1000},
            "edits_truncated": false, "edits": [], "source": null, "error": null
        })
    }

    #[test]
    fn observation_preserves_unknown_samples_and_no_workspace() {
        let wire = json!({
            "processes_truncated":false, "processes":[], "stats":stats(),
            "liveness": {
                "state":"unknown", "last_model_progress":null, "age_ms":null,
                "clock_regressed":false, "basis":"committed_native_facts", "source_healthy":null
            },
            "workspace":null, "git":git()
        });
        let view: ObservationView = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(view).unwrap(), wire);
        assert_eq!(
            serde_json::to_value(ObservationView::unavailable("unavailable")).unwrap(),
            json!({"available":false,"error_code":"unavailable"})
        );
    }

    #[test]
    fn stats_preserve_partial_null_counters_and_stale_coverage() {
        let mut wire = stats();
        wire["available"] = json!(true);
        wire["coverage"] = json!("partial");
        wire["complete"] = json!(false);
        wire["sampled_at"] = json!(1000);
        wire["age_ms"] = json!(1);
        wire["samples"] = json!([{
            "record": {
                "identity":{"host_boot_id":"boot","pid":1,"start_time":2},
                "owner":"agent:a1#1", "role":"harness", "state":"unknown",
                "exit_code":null, "updated_at":1000
            }, "cpu_time_ns":null, "rss_bytes":123
        }]);
        let view: ProcessStatsView = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(view).unwrap(), wire);
        wire["available"] = json!(false);
        wire["coverage"] = json!("stale");
        wire["samples"] = Value::Null;
        wire["age_ms"] = json!(5000);
        let view: ProcessStatsView = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(view).unwrap(), wire);
        wire.as_object_mut().unwrap().remove("cpu_time_ns");
        assert!(serde_json::from_value::<ProcessStatsView>(wire).is_err());
    }

    #[test]
    fn git_retains_cached_unavailable_data_and_readback_digests() {
        let mut wire = git();
        wire["source"] = json!({
            "agent_id":"a1", "generation":1, "source":"git", "families":["git"],
            "state":"open", "health":"stale", "cursor":null, "last_store_seq":4,
            "last_batch_id":"batch", "last_batch_digest":[0,127,255],
            "last_batch_first_seq":null, "last_batch_last_seq":null,
            "gap_id":"gap", "gap_digest":[255,0,42], "gap_from":null, "gap_to":null,
            "gap_seq":4
        });
        let view: GitView = serde_json::from_value(wire.clone()).unwrap();
        let GitView::Data(data) = &view else {
            panic!("cached Git data was lost")
        };
        let source = data.source.as_ref().unwrap();
        assert_eq!(
            source.last_batch_digest.as_deref(),
            Some([0, 127, 255].as_slice())
        );
        assert_eq!(source.gap_digest.as_deref(), Some([255, 0, 42].as_slice()));
        assert_eq!(serde_json::to_value(view).unwrap(), wire);
        assert_eq!(
            serde_json::to_value(GitView::unavailable("unavailable")).unwrap(),
            json!({"available":false,"error_code":"unavailable"})
        );
        wire["available"] = json!(true);
        wire["age_ms"] = json!(1);
        let view: GitView = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(view).unwrap(), wire);
    }

    #[test]
    fn workspace_keeps_future_target_and_list_source_projection() {
        let wire = json!({
            "owner":"workspace:a1#1", "target_generation":1, "phase":"setup_running",
            "setup_index":0, "processes_truncated":false, "processes":[], "stats":stats(),
            "source":{
                "agent_id":"a1", "generation":1, "source":"workspace_process",
                "families":["workspace_process"],
                "owners":[{"family":"workspace_process","source":"workspace_process","mechanism":"os"}],
                "state":"open", "health":"ok", "cursor":null, "last_store_seq":null,
                "last_batch_id":null, "last_batch_first_seq":null, "last_batch_last_seq":null,
                "gap_id":null, "gap_from":null, "gap_to":null, "gap_seq":null
            }
        });
        let view: WorkspaceView = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(view).unwrap(), wire);
        assert_eq!(
            serde_json::to_value(WorkspaceView::unavailable("unavailable")).unwrap(),
            json!({"available":false,"error":"unavailable"})
        );
    }

    #[test]
    fn watcher_retry_preserves_absent_null_and_delay() {
        for wire in [
            json!({"state":"inactive"}),
            json!({"state":"inactive","retry_after_ms":null}),
            json!({"state":"healthy","retry_after_ms":null}),
            json!({"state":"degraded","retry_after_ms":2000}),
            json!({"state":"unavailable","retry_after_ms":0}),
        ] {
            let view: FilesystemInvalidationView = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(serde_json::to_value(view).unwrap(), wire);
        }
    }

    #[test]
    fn error_shapes_cannot_claim_available_or_accept_missing_views() {
        assert!(
            serde_json::from_value::<ObservationView>(json!({"available":true,"error_code":"x"}))
                .is_err()
        );
        assert!(serde_json::from_value::<GitView>(json!({"available":true,"error":"x"})).is_err());
        let mut wire = git();
        wire["error"] = json!("git_timeout");
        wire.as_object_mut().unwrap().remove("history_complete");
        assert!(serde_json::from_value::<GitView>(wire).is_err());
    }

    #[test]
    fn process_boundaries_reject_nested_positional_arrays() {
        let record = json!({
            "identity":{"host_boot_id":"boot","pid":1,"start_time":2},
            "owner":"agent:a1#1", "role":"harness", "state":"unknown",
            "exit_code":null, "updated_at":1000
        });
        let positional_record = json!([
            ["boot", 1, 2],
            "agent:a1#1",
            "harness",
            "unknown",
            null,
            1000
        ]);
        let mut positional_identity = record.clone();
        positional_identity["identity"] = json!(["boot", 1, 2]);
        for malformed in [positional_record, positional_identity] {
            let mut samples = stats();
            samples["samples"] = json!([{"record":malformed,"cpu_time_ns":null,"rss_bytes":null}]);
            assert!(serde_json::from_value::<ProcessStatsView>(samples).is_err());

            let observation = json!({
                "processes_truncated":false, "processes":[malformed], "stats":stats(),
                "liveness":{
                    "state":"unknown", "last_model_progress":null, "age_ms":null,
                    "clock_regressed":false,"basis":"committed_native_facts","source_healthy":null
                }, "workspace":null, "git":git()
            });
            assert!(serde_json::from_value::<ObservationView>(observation).is_err());

            let workspace = json!({
                "owner":"workspace:a1#1","target_generation":1,"phase":"setup_running",
                "setup_index":0,"processes_truncated":false,"processes":[malformed],
                "stats":stats(),"source":null
            });
            assert!(serde_json::from_value::<WorkspaceView>(workspace).is_err());
        }
    }
}
