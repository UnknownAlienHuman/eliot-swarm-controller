use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub use eliot_core::api::{
    DiscoverCoverage, DiscoverPage, DiscoverSubject, MAX_DISCOVER_PAGE_BYTES,
    MAX_DISCOVER_PAGE_ENTRIES, MAX_OBSERVATION_CURSOR_BYTES, MAX_OBSERVATION_PAGE_BYTES,
    MAX_OBSERVATION_PAGE_ENTRIES, ObservationPage, ObservationQuery,
};

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_TEXT_BYTES: usize = 512 * 1024;
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_CREDENTIAL_BYTES: usize = 4 * 1024;
pub const MAX_COMMAND_ID_BYTES: usize = 256;

pub(crate) fn empty_object() -> Value {
    Value::Object(Map::new())
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    #[serde(default = "empty_object")]
    pub params: Value,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

impl RpcResponse {
    pub(crate) fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id,
            result: Some(result),
            error: None,
        }
    }

    pub(crate) fn failure(id: Value, error: RpcError) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            id,
            result: None,
            error: Some(error),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct PluginManifest {
    pub name: String,
    pub version: String,
    pub protocol: String,
    pub harness_version: String,
    pub available: bool,
    pub mode: String,
    pub verbs: Vec<String>,
    pub child_verbs: Vec<String>,
    pub features: ManifestFeatures,
    pub sources: BTreeMap<String, BTreeMap<String, String>>,
    pub proxy: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct ManifestFeatures {
    pub start_requires_input: bool,
    pub native_children: bool,
    #[serde(default)]
    pub discover: bool,
    #[serde(default)]
    pub observe: bool,
}

pub fn fake_manifest() -> PluginManifest {
    let controlled = BTreeMap::from([
        ("message".to_owned(), "stream".to_owned()),
        ("session".to_owned(), "stream".to_owned()),
        ("turn".to_owned(), "stream".to_owned()),
    ]);
    PluginManifest {
        name: "fake".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: "eliot-plugin/1".to_owned(),
        harness_version: "fake-1".to_owned(),
        available: true,
        mode: "per_agent".to_owned(),
        verbs: vec!["launch".to_owned(), "send".to_owned(), "stop".to_owned()],
        child_verbs: Vec::new(),
        features: ManifestFeatures {
            start_requires_input: false,
            native_children: false,
            discover: true,
            observe: true,
        },
        sources: BTreeMap::from([
            ("controlled".to_owned(), controlled),
            (
                "observed".to_owned(),
                BTreeMap::from([
                    ("message".to_owned(), "native_read".to_owned()),
                    ("session".to_owned(), "native_read".to_owned()),
                    ("turn".to_owned(), "native_read".to_owned()),
                ]),
            ),
        ]),
        proxy: "none".to_owned(),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverParams {
    pub profile: Value,
    pub target_environment: Value,
    #[serde(default)]
    pub native_scope: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    pub limit: usize,
}

#[derive(Deserialize)]
pub struct InitializeParams {
    pub protocol: String,
    pub credential: String,
    pub instance_dir: String,
    #[serde(default = "empty_object")]
    pub config: Value,
}

#[derive(Deserialize)]
pub struct AgentStartParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    #[serde(default)]
    pub profile: Option<Value>,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub target_environment: Option<Value>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub source_plan: Option<Value>,
    #[serde(default)]
    pub source_state: Option<Value>,
    #[serde(default)]
    pub resume_from: Option<Value>,
    #[serde(default)]
    pub initial: Option<Value>,
}

#[derive(Deserialize)]
pub struct AgentCommandParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    pub verb: String,
    #[serde(default = "empty_object")]
    pub args: Value,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
}

#[derive(Deserialize)]
pub struct AgentStopParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    #[serde(default)]
    pub force: bool,
}

#[derive(Deserialize)]
pub struct AgentSnapshotParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    #[serde(default)]
    pub turn_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverAttachParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    pub mode: String,
    pub native_scope: String,
    pub native_ref: String,
    pub profile: Value,
    pub target_environment: Value,
    #[serde(default)]
    pub source_plan: Option<Value>,
    #[serde(default)]
    pub source_state: Option<Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverSnapshotParams {
    pub agent_id: String,
    pub generation: i64,
    pub native_scope: String,
    pub native_ref: String,
    pub observation: ObservationQuery,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObserverDetachParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    pub native_scope: String,
    pub native_ref: String,
}
