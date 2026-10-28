use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
pub const MAX_TEXT_BYTES: usize = 512 * 1024;
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_CREDENTIAL_BYTES: usize = 4 * 1024;
pub const MAX_COMMAND_ID_BYTES: usize = 256;
pub const MAX_DISCOVER_PAGE_ENTRIES: usize = 1_000;
pub const MAX_DISCOVER_PAGE_BYTES: usize = 900 * 1024;
pub const MAX_OBSERVATION_PAGE_ENTRIES: usize = 1_000;
pub const MAX_OBSERVATION_PAGE_BYTES: usize = 900 * 1024;
pub const MAX_OBSERVATION_CURSOR_BYTES: usize = 16 * 1024;

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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverSubject {
    pub native_ref: String,
    pub workspace: Option<String>,
    pub state: String,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub source: Option<String>,
    pub ephemeral: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverCoverage {
    pub kind: String,
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverPage {
    pub harness: String,
    pub native_scope: String,
    pub subjects: Vec<DiscoverSubject>,
    pub next_cursor: Option<String>,
    pub coverage: DiscoverCoverage,
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
pub struct ObservationQuery {
    #[serde(default = "native_observation_source")]
    pub source: String,
    #[serde(deserialize_with = "observation_view")]
    pub view: String,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default, deserialize_with = "observation_cursor")]
    pub cursor: Option<String>,
    #[serde(deserialize_with = "observation_limit")]
    pub limit: usize,
}

impl ObservationQuery {
    pub fn validate(&self) -> eliot_core::Result<()> {
        if self.source.trim().is_empty()
            || !matches!(self.view.as_str(), "subject" | "turns" | "items")
            || !(1..=MAX_OBSERVATION_PAGE_ENTRIES).contains(&self.limit)
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.is_empty() || cursor.len() > MAX_OBSERVATION_CURSOR_BYTES
            })
        {
            return Err(eliot_core::Error::new(
                "invalid",
                "invalid observation query",
            ));
        }
        Ok(())
    }
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

#[derive(Clone, Debug, Serialize)]
pub struct ObservationPage {
    pub agent_id: String,
    pub generation: i64,
    pub native_scope: String,
    pub native_ref: String,
    pub source: String,
    pub view: String,
    pub entries: Vec<Value>,
    pub next_cursor: Option<String>,
    pub coverage: Value,
}

impl ObservationPage {
    /// Check before publishing a page or accepting it from a plugin.
    pub fn validate(&self) -> eliot_core::Result<()> {
        if self.source.trim().is_empty()
            || !matches!(self.view.as_str(), "subject" | "turns" | "items")
        {
            return Err(eliot_core::Error::new(
                "invalid",
                "invalid observation page source or view",
            ));
        }
        if self.entries.len() > MAX_OBSERVATION_PAGE_ENTRIES
            || self.next_cursor.as_ref().is_some_and(|cursor| {
                cursor.is_empty() || cursor.len() > MAX_OBSERVATION_CURSOR_BYTES
            })
        {
            return Err(eliot_core::Error::new(
                "frame_too_large",
                "observation page exceeds entry or cursor bounds",
            ));
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|_| eliot_core::Error::new("internal", "cannot encode observation page"))?;
        if bytes.len() > MAX_OBSERVATION_PAGE_BYTES {
            return Err(eliot_core::Error::new(
                "frame_too_large",
                "observation page exceeds 900 KiB",
            ));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ObservationPage {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Page {
            agent_id: String,
            generation: i64,
            native_scope: String,
            native_ref: String,
            source: String,
            #[serde(deserialize_with = "observation_view")]
            view: String,
            #[serde(deserialize_with = "observation_entries")]
            entries: Vec<Value>,
            #[serde(default, deserialize_with = "observation_cursor")]
            next_cursor: Option<String>,
            coverage: Value,
        }
        let page = Page::deserialize(deserializer)?;
        let page = Self {
            agent_id: page.agent_id,
            generation: page.generation,
            native_scope: page.native_scope,
            native_ref: page.native_ref,
            source: page.source,
            view: page.view,
            entries: page.entries,
            next_cursor: page.next_cursor,
            coverage: page.coverage,
        };
        page.validate().map_err(serde::de::Error::custom)?;
        Ok(page)
    }
}

fn native_observation_source() -> String {
    "native".to_owned()
}

fn observation_view<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let view = String::deserialize(deserializer)?;
    if !matches!(view.as_str(), "subject" | "turns" | "items") {
        return Err(serde::de::Error::custom("invalid observation view"));
    }
    Ok(view)
}

fn observation_cursor<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<String>, D::Error> {
    let cursor = Option::<String>::deserialize(deserializer)?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_OBSERVATION_CURSOR_BYTES)
    {
        return Err(serde::de::Error::custom(
            "invalid observation cursor length",
        ));
    }
    Ok(cursor)
}

fn observation_limit<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    let limit = usize::deserialize(deserializer)?;
    if !(1..=MAX_OBSERVATION_PAGE_ENTRIES).contains(&limit) {
        return Err(serde::de::Error::custom(
            "observation limit must be between 1 and 1000",
        ));
    }
    Ok(limit)
}

fn observation_entries<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<Value>, D::Error> {
    let entries = Vec::<Value>::deserialize(deserializer)?;
    if entries.len() > MAX_OBSERVATION_PAGE_ENTRIES {
        return Err(serde::de::Error::custom(
            "observation page exceeds 1000 entries",
        ));
    }
    Ok(entries)
}
