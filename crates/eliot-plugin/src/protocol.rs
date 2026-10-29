use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub use eliot_core::api::{
    DiscoverCoverage, DiscoverPage, DiscoverSubject, MAX_DISCOVER_PAGE_BYTES,
    MAX_DISCOVER_PAGE_ENTRIES, MAX_OBSERVATION_CURSOR_BYTES, MAX_OBSERVATION_PAGE_BYTES,
    MAX_OBSERVATION_PAGE_ENTRIES, ObservationPage, ObservationQuery,
};
pub use eliot_core::limits::{
    MAX_COMMAND_ID_BYTES, MAX_CREDENTIAL_BYTES, MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_TEXT_BYTES,
};
pub use eliot_core::{AgentStartParams, NativeStoreBinding};

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
pub struct ProfileStoreParams {
    pub profile: Value,
    pub target_environment: Value,
    pub workspace: String,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn private_requests_preserve_scalar_ids_and_object_params() {
        for id in ["null", "\"\"", "\"request\"", "1", "1.5", "1e3"] {
            for params in ["", ",\"params\":{\"value\":1}"] {
                let line = format!(
                    "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"\",\"extra\":true{params}}}"
                );
                let PrivateIncoming::Request(request) =
                    decode_private_frame(&line).expect("request")
                else {
                    panic!("request decoded as response");
                };
                assert_eq!(request.id, serde_json::from_str::<Value>(id).unwrap());
                assert_eq!(request.method, "");
                assert_eq!(
                    request.params,
                    if params.is_empty() {
                        json!({})
                    } else {
                        json!({"value": 1})
                    }
                );
            }
        }
    }

    #[test]
    fn private_responses_require_one_populated_option_branch() {
        for id in ["null", "\"response\"", "1.5", "1e3"] {
            for branch in [
                "\"result\":{}",
                "\"result\":false,\"error\":null",
                "\"result\":0",
                "\"result\":\"\"",
                "\"error\":{\"code\":-32000,\"message\":\"failure\"}",
                "\"result\":null,\"error\":{\"code\":-32000,\"message\":\"failure\"}",
            ] {
                let line = format!("{{\"jsonrpc\":\"2.0\",\"id\":{id},{branch},\"extra\":true}}");
                let PrivateIncoming::Response(response) =
                    decode_private_frame(&line).expect("response")
                else {
                    panic!("response decoded as request");
                };
                assert_eq!(response.id, serde_json::from_str::<Value>(id).unwrap());
                assert_ne!(response.result.is_some(), response.error.is_some());
            }
        }
    }

    #[test]
    fn private_decoder_distinguishes_existing_grammar_failures() {
        use DecodeErrorKind::*;

        for (line, expected) in [
            ("{", InvalidJson),
            ("null", NotObject),
            ("[]", NotObject),
            (r#"{"id":1,"method":"call"}"#, InvalidVersion),
            (r#"{"jsonrpc":2,"id":1,"method":"call"}"#, InvalidVersion),
            (
                r#"{"jsonrpc":"1.0","id":1,"method":"call"}"#,
                InvalidVersion,
            ),
            (r#"{"jsonrpc":"2.0","id":1,"method":null}"#, InvalidRequest),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":"call","params":null}"#,
                InvalidRequestParams,
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"method":"call","params":[]}"#,
                InvalidRequestParams,
            ),
            (
                r#"{"jsonrpc":"2.0","id":true,"method":"call"}"#,
                InvalidRequestParams,
            ),
            (
                r#"{"jsonrpc":"2.0","id":[],"method":"call"}"#,
                InvalidRequestParams,
            ),
            (
                r#"{"jsonrpc":"2.0","id":{},"method":"call"}"#,
                InvalidRequestParams,
            ),
            (r#"{"jsonrpc":"2.0","id":1,"error":true}"#, InvalidResponse),
            (r#"{"jsonrpc":"2.0","id":1}"#, InvalidResponseShape),
            (
                r#"{"jsonrpc":"2.0","id":1,"result":null}"#,
                InvalidResponseShape,
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"result":null,"error":null}"#,
                InvalidResponseShape,
            ),
            (
                r#"{"jsonrpc":"2.0","id":1,"result":{},"error":{"code":-1,"message":"failure"}}"#,
                InvalidResponseShape,
            ),
            (
                r#"{"jsonrpc":"2.0","id":true,"result":{}}"#,
                InvalidResponseShape,
            ),
            (
                r#"{"jsonrpc":"2.0","id":[],"result":{}}"#,
                InvalidResponseShape,
            ),
            (
                r#"{"jsonrpc":"2.0","id":{},"result":{}}"#,
                InvalidResponseShape,
            ),
        ] {
            assert_eq!(decode_private_frame(line).err(), Some(expected), "{line}");
        }
    }
}
