use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub use eliot_core::limits::{
    MAX_COMMAND_ID_BYTES, MAX_CREDENTIAL_BYTES, MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_TEXT_BYTES,
};
pub use eliot_core::{AgentStartParams, NativeStoreBinding};
use eliot_core::{ContextItem, Error, SessionUsage};

/// Live session usage sample sent to the bound daemon source without adding a
/// native event row. `turn_id` is an optional source authority check only;
/// the usage snapshot remains session scoped.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SessionUsageSampleRequest {
    pub agent_id: String,
    pub generation: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    pub usage: SessionUsage,
}

impl SessionUsageSampleRequest {
    pub fn validate(&self) -> eliot_core::Result<()> {
        if self.agent_id.trim().is_empty()
            || self.generation < 1
            || self
                .turn_id
                .as_deref()
                .is_some_and(|turn_id| turn_id.trim().is_empty() || turn_id.len() > 256)
        {
            return Err(Error::new(
                "invalid",
                "session usage sample binding is invalid",
            ));
        }
        self.usage.validate()
    }
}

/// Positive response after the host accepts the sample.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionUsageSampleAck {
    pub accepted: bool,
}

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

/// A decoded frame on Eliot's private host/plugin connection.
pub enum PrivateIncoming {
    Request(RpcRequest),
    Response(RpcResponse),
}

/// Grammar failures, mapped to errors by each end of the private connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeErrorKind {
    InvalidJson,
    NotObject,
    InvalidVersion,
    InvalidRequest,
    InvalidRequestParams,
    InvalidResponse,
    InvalidResponseShape,
}

/// Decode the private protocol grammar, without transport or error policy.
pub fn decode_private_frame(line: &str) -> Result<PrivateIncoming, DecodeErrorKind> {
    let value: Value = serde_json::from_str(line).map_err(|_| DecodeErrorKind::InvalidJson)?;
    let object = value.as_object().ok_or(DecodeErrorKind::NotObject)?;
    if object.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(DecodeErrorKind::InvalidVersion);
    }

    if object.contains_key("method") {
        let request: RpcRequest =
            serde_json::from_value(value).map_err(|_| DecodeErrorKind::InvalidRequest)?;
        if !valid_private_id(&request.id) || !request.params.is_object() {
            return Err(DecodeErrorKind::InvalidRequestParams);
        }
        Ok(PrivateIncoming::Request(request))
    } else {
        let response: RpcResponse =
            serde_json::from_value(value).map_err(|_| DecodeErrorKind::InvalidResponse)?;
        if !valid_private_id(&response.id) || response.error.is_some() == response.result.is_some()
        {
            return Err(DecodeErrorKind::InvalidResponseShape);
        }
        Ok(PrivateIncoming::Response(response))
    }
}

pub fn valid_private_id(id: &Value) -> bool {
    id.is_null() || id.is_string() || id.is_number()
}

/// Encoding failures, mapped to errors by each end of the private connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeErrorKind {
    Unencodable,
    TooLarge,
}

/// Encode the private protocol grammar, without transport or error policy.
pub fn encode_private_frame<T: Serialize>(message: &T) -> Result<String, EncodeErrorKind> {
    let encoded = serde_json::to_string(message).map_err(|_| EncodeErrorKind::Unencodable)?;
    if encoded.len().saturating_add(1) > MAX_FRAME_BYTES {
        return Err(EncodeErrorKind::TooLarge);
    }
    Ok(encoded)
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

impl PluginManifest {
    /// The manifest of a per-Agent native adapter: protocol `eliot-plugin/1`,
    /// available, no child verbs. Each adapter passes its own name, package
    /// version, pinned harness version, verbs, features, sources and proxy.
    pub fn per_agent(
        name: &str,
        version: &str,
        harness_version: &str,
        verbs: &[&str],
        features: ManifestFeatures,
        sources: BTreeMap<String, BTreeMap<String, String>>,
        proxy: &str,
    ) -> Self {
        Self {
            name: name.to_owned(),
            version: version.to_owned(),
            protocol: "eliot-plugin/1".to_owned(),
            harness_version: harness_version.to_owned(),
            available: true,
            mode: "per_agent".to_owned(),
            verbs: verbs.iter().map(|verb| (*verb).to_owned()).collect(),
            child_verbs: Vec::new(),
            features,
            sources,
            proxy: proxy.to_owned(),
        }
    }
}

/// One stream-family source map: every listed family streams.
pub fn stream_sources(families: &[&str]) -> BTreeMap<String, String> {
    families
        .iter()
        .map(|family| ((*family).to_owned(), "stream".to_owned()))
        .collect()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileStoreParams {
    pub profile: Value,
    pub target_environment: Value,
    pub workspace: String,
}

/// `native.archive`: delete or archive exactly one retained native session
/// of a terminal Swarm-created Agent through a dedicated reader instance
/// with no turn and no inference. The daemon owns cleanup policy; the plugin
/// only maps the harness's documented native call (Codex `thread/archive`,
/// OpenCode `DELETE /api/session/{id}`). Raw store files are never touched.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeArchiveParams {
    pub profile: Value,
    pub native_ref: String,
    /// The Agent whose frozen store the reader must prove it serves before
    /// the delete. Required by harnesses whose sessions live in a per-Agent
    /// store (OpenCode); Codex pins its store through the frozen binding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    /// The frozen native scope recorded with the session. The reader proves
    /// its served store derives this exact scope before the delete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_scope: Option<String>,
    /// The Agent workspace the per-Agent server binds its directory context
    /// to (OpenCode). Absent for harnesses without a directory-bound server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
}

/// One catalogue phase per owned reader lifetime. OpenCode's native CLI uses
/// separate commands for ids and metadata. The host closes each family before
/// starting the next phase. Other harnesses use the default complete inventory.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelListParams {
    pub profile: Value,
    #[serde(default)]
    pub target_environment: Option<Value>,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub part: ModelListPart,
}

/// Host-owned scratch subtree for OpenCode's short-lived reader credentials.
pub const MODEL_READER_DIR: &str = "model-catalogue";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelListPart {
    #[default]
    Models,
    /// OpenCode model names and variants for the exact ids on the models phase.
    Variants,
}

/// One launch's verified native-store selection and optional plugin-owned
/// startup metadata from the same read. The daemon freezes both at dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileStoreResult {
    pub binding: eliot_core::NativeStoreBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup: Option<Value>,
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
    #[serde(default)]
    pub archived: bool,
    pub limit: usize,
}

#[derive(Deserialize)]
pub struct InitializeParams {
    pub protocol: String,
    pub credential: String,
    pub instance_dir: String,
    #[serde(default)]
    pub store_root: Option<String>,
    #[serde(default = "empty_object")]
    pub config: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentCommandParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    pub verb: String,
    /// Daemon-authorized Owner interrupt of this controlled native session.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub owner_control: bool,
    #[serde(default = "empty_object")]
    pub args: Value,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default)]
    pub request_id: Option<String>,
    /// FIFO `next_input` items claimed for this send. Empty (omitted) when none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<ContextItem>,
    /// Effective model for this `send`, resolved by the daemon from the
    /// command's retained configuration revision. `None` when no
    /// `configure` overlay applies; the plugin then uses its session values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_override: Option<String>,
    /// Effective effort for this `send`, resolved like `model_override`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_override: Option<String>,
    /// Configuration revision the send runs under. Present exactly when the
    /// overrides above apply, so the plugin can echo it in its response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_revision: Option<i64>,
}

/// Plugin-initiated system-context callback on the authenticated instance channel.
/// The connection credential binds the Agent generation; the callback carries no identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookEvent {
    /// Native hook point: `before_tool` or `after_tool` (Claude Code), or
    /// `before_model` (OpenCode or Antigravity).
    pub hook: String,
    /// Exact native turn requesting context.
    pub turn_id: String,
    /// Opaque native consumer identity for this callback.
    pub consumer_id: String,
}

/// Synchronous hook response: already-claimed context for this callback only.
/// Empty means no context; a late response is discarded, never replayed.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HookResponse {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<ContextItem>,
}

#[derive(Deserialize)]
pub struct AgentStopParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
}

#[derive(Deserialize)]
pub struct AgentSnapshotParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    #[serde(default)]
    pub turn_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentModelsParams {
    pub agent_id: String,
    pub generation: i64,
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
    pub observation: eliot_core::api::ObservationQuery,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChildParams {
    pub credential: String,
    pub registration: eliot_core::ChildRegistration,
}

/// Maximum encoded bytes of one [`McpHttpReady`] stdout frame (JSON line
/// plus its trailing newline).
pub const MAX_MCP_HTTP_READY_BYTES: usize = 256;

/// Ready frame emitted exactly once on stdout by `swarm mcp --http` after
/// its loopback listener binds. The managed parent reads one line and
/// parses it with [`McpHttpReady::decode_frame`]. Loopback-only: the
/// address must parse as a loopback socket address. This frame proves a
/// local listener, never public tunnel/auth readiness. Address-only by
/// design: the managed parent owns the child PID and already knows the route.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpHttpReady {
    /// Actual bound address, e.g. `127.0.0.1:4317`.
    pub address: String,
}

impl McpHttpReady {
    pub fn validate(&self) -> eliot_core::Result<()> {
        let address: std::net::SocketAddr = self.address.parse().map_err(|_| {
            eliot_core::Error::new("invalid", "MCP HTTP ready address is not a socket address")
        })?;
        if !address.ip().is_loopback() {
            return Err(eliot_core::Error::new(
                "invalid",
                "MCP HTTP ready address is not loopback",
            ));
        }
        Ok(())
    }

    /// Encode the single stdout line, trailing `\n` included.
    pub fn encode_frame(&self) -> eliot_core::Result<String> {
        self.validate()?;
        let mut line = serde_json::to_string(self).map_err(|_| {
            eliot_core::Error::new("internal", "MCP HTTP ready frame cannot be encoded")
        })?;
        line.push('\n');
        if line.len() > MAX_MCP_HTTP_READY_BYTES {
            return Err(eliot_core::Error::new(
                "frame_too_large",
                "MCP HTTP ready frame exceeds its bound",
            ));
        }
        Ok(line)
    }

    /// Decode one stdout line; a trailing newline is tolerated.
    pub fn decode_frame(line: &str) -> eliot_core::Result<Self> {
        let line = line.strip_suffix('\n').unwrap_or(line);
        if line.len() > MAX_MCP_HTTP_READY_BYTES {
            return Err(eliot_core::Error::new(
                "frame_too_large",
                "MCP HTTP ready frame exceeds its bound",
            ));
        }
        let ready: Self = serde_json::from_str(line)
            .map_err(|_| eliot_core::Error::new("invalid", "MCP HTTP ready frame is malformed"))?;
        ready.validate()?;
        Ok(ready)
    }
}

/// Private metadata-only quota request; never logged or persisted.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaReadParams {
    pub profile: Value,
    #[serde(default = "empty_object")]
    pub environment: Value,
}
