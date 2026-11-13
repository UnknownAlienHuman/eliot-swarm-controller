//! Bounded discovery DTOs and redacted request decoding, shared with the Router.
use crate::{Error, Result};
use schemars::{JsonSchema, generate::SchemaSettings};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

/// A custom `nullable` deserializer requires the key while accepting JSON null.
/// Preserve both facts instead of treating Rust Option as an optional key.
pub struct RequiredNullable<T>(std::marker::PhantomData<T>);

impl<T: JsonSchema> JsonSchema for RequiredNullable<T> {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("RequiredNullable_{}", T::schema_name()).into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <Option<T>>::json_schema(generator)
    }

    fn inline_schema() -> bool {
        true
    }
}

pub const MAX_METHOD_PAGE: usize = 50;
pub const MAX_METHOD_SCHEMA_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MethodsRequest {
    /// Case-sensitive substring of the canonical name or summary (at most 128 bytes).
    #[serde(default)]
    #[schemars(length(max = 128), extend("x-maxBytes" = 128))]
    pub query: Option<String>,
    /// Continue after the last canonical name returned by this same selection.
    #[serde(default)]
    #[schemars(length(max = 128), extend("x-maxBytes" = 128))]
    pub after: Option<String>,
    /// Page size, 1..=50; default 20.
    #[serde(default)]
    #[schemars(range(min = 1, max = 50))]
    pub limit: Option<usize>,
}

impl MethodsRequest {
    pub fn validate(&self) -> Result<()> {
        if self.query.as_ref().is_some_and(|s| s.len() > 128) {
            return Err(Error::invalid_field(
                "params.query",
                "string of at most 128 bytes",
            ));
        }
        if self.after.as_ref().is_some_and(|s| s.len() > 128) {
            return Err(Error::invalid_field(
                "params.after",
                "canonical method name of at most 128 bytes",
            ));
        }
        if !(1..=MAX_METHOD_PAGE).contains(&self.limit.unwrap_or(20)) {
            return Err(Error::invalid_field(
                "params.limit",
                "integer from 1 through 50",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MethodHelpRequest {
    /// Canonical API method, for example mail.send (CLI aliases are not methods).
    #[schemars(length(min = 1, max = 128), pattern(r"\S"), extend("x-maxBytes" = 128, "x-nonBlank" = true))]
    pub method: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IdentityRequest {}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectGetRequest {
    /// Nonempty project registration id.
    #[schemars(length(min = 1), pattern(r"\S"), extend("x-nonBlank" = true))]
    pub id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectListRequest {}

impl MethodHelpRequest {
    pub fn validate(&self) -> Result<()> {
        if self.method.trim().is_empty() || self.method.len() > 128 {
            return Err(Error::invalid_field(
                "params.method",
                "canonical method name of 1..128 bytes",
            ));
        }
        Ok(())
    }
}

impl ProjectGetRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(Error::invalid_field(
                "params.id",
                "nonblank project registration id",
            ));
        }
        Ok(())
    }
}

impl IdentityRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}
impl ProjectListRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct MethodSummary {
    pub method: String,
    pub summary: String,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct MethodsResponse {
    pub methods: Vec<MethodSummary>,
    pub next: Option<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct MethodHelpResponse {
    pub method: String,
    pub summary: String,
    pub cli_aliases: Vec<String>,
    pub request_schema: Value,
    pub response_schema: Value,
    pub example: Value,
    pub mutation_id: String,
    pub scopes: Vec<String>,
    pub limits: String,
    pub unsupported: Vec<String>,
}

#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct IdentityResponse {
    pub principal: String,
    pub agent_id: Option<String>,
    pub generation: Option<i64>,
    pub allowed_scopes: Vec<String>,
    pub limits: String,
}

pub fn request_schema<T: JsonSchema>() -> Value {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .unwrap_or(Value::Bool(false))
}

pub fn response_schema<T: JsonSchema>() -> Value {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_serialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .unwrap_or(Value::Bool(false))
}

/// Never forward serde's raw message: unknown keys/variants and invalid values
/// can contain credentials. Only schema-owned field names and numeric indices
/// enter the error, and the expectation comes from the DTO's schema.
pub fn decode_params<T: DeserializeOwned + JsonSchema>(params: Value) -> Result<T> {
    serde_path_to_error::deserialize(params).map_err(|error| {
        let schema = request_schema::<T>();
        let mut node = &schema;
        let mut path = "params".to_owned();
        let mut unknown_field = false;
        for segment in error.path() {
            node = resolve(&schema, node);
            match segment {
                serde_path_to_error::Segment::Map { key } => {
                    let Some(child) = node.get("properties").and_then(|p| p.get(key)) else {
                        unknown_field = true;
                        break;
                    };
                    path.push('.');
                    path.push_str(key);
                    node = child;
                }
                serde_path_to_error::Segment::Seq { index } => {
                    let Some(child) = node.get("items") else {
                        break;
                    };
                    path.push_str(&format!("[{index}]"));
                    node = child;
                }
                _ => break,
            }
        }
        // Serde reports a missing field at its parent. Accept the extracted
        // name only if it is a literal property owned by that DTO.
        let message = error.inner().to_string();
        unknown_field |= message.starts_with("unknown field `");
        if let Some(field) = message
            .strip_prefix("missing field `")
            .and_then(|s| s.strip_suffix('`'))
        {
            node = resolve(&schema, node);
            if let Some(child) = node.get("properties").and_then(|p| p.get(field)) {
                path.push('.');
                path.push_str(field);
                node = child;
            }
        }
        node = resolve(&schema, node);
        if unknown_field {
            path.push_str(".<unknown-field>");
        }
        let mut expectation = json!({
            "allowed_fields": node.get("properties").and_then(Value::as_object).map(|properties| properties.keys().collect::<Vec<_>>()),
            "type": node.get("type"), "enum": node.get("enum"),
            "minimum": node.get("minimum"), "maximum": node.get("maximum"),
            "description": node.get("description"), "required": node.get("required"),
        });
        if let Some(object) = expectation.as_object_mut() {
            object.retain(|_, value| !value.is_null());
            for key in ["anyOf", "oneOf", "minItems", "maxItems", "minLength", "maxLength", "pattern", "x-maxBytes", "x-maxEncodedBytes"] {
                if let Some(value) = node.get(key) { object.insert(key.to_owned(), value.clone()); }
            }
        }
        let expected =
            serde_json::to_string(&expectation).unwrap_or_else(|_| "DTO field shape".to_owned());
        Error::invalid_field(path, expected.chars().take(512).collect::<String>())
    })
}

fn resolve<'a>(root: &'a Value, mut node: &'a Value) -> &'a Value {
    // Schemars uses local refs; cap traversal even for recursive DTOs.
    for _ in 0..8 {
        let Some(reference) = node
            .get("$ref")
            .and_then(Value::as_str)
            .and_then(|s| s.strip_prefix('#'))
        else {
            break;
        };
        let Some(target) = root.pointer(reference) else {
            break;
        };
        node = target;
    }
    node
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        EventsReadRequest, EventsSubscribeRequest, MailSendRequest, ProviderClearHoldRequest,
        VarSetRequest,
    };

    #[test]
    fn malformed_fields_name_the_dto_path_without_echoing_values() {
        let secret = "credential-value-must-never-appear";
        for params in [
            json!({"id":"id", "to":[1], "mode":"queue", "text":secret}),
            json!({"id":"id", "to":["a1"], "mode":secret, "text":"hello"}),
            json!({"id":"id", "to":["a1"], "mode":"queue"}),
            json!({"id":"id", "to":["a1"], "mode":"queue", "text":"hello", (secret):true}),
        ] {
            let error = decode_params::<MailSendRequest>(params).unwrap_err();
            let wire = serde_json::to_string(&error).unwrap();
            assert_eq!(error.code, "invalid");
            assert!(!wire.contains(secret));
            assert!(error.details.unwrap().field.starts_with("params"));
        }
        let error = decode_params::<MailSendRequest>(
            json!({"id":"id","to":[false],"mode":"queue","text":"hi"}),
        )
        .unwrap_err();
        assert_eq!(error.details.unwrap().field, "params.to[0]");
        let error = decode_params::<VarSetRequest>(
            json!({"id":"id","key":"k","value":null,"expect_version":"bad"}),
        )
        .unwrap_err();
        assert_eq!(error.details.unwrap().field, "params.expect_version");
        let error = decode_params::<MailSendRequest>(
            json!({"id":"id","to":["a1"],"mode":"queue","text":"hi",(secret):true}),
        )
        .unwrap_err();
        let details = error.details.unwrap();
        assert_eq!(details.field, "params.<unknown-field>");
        assert!(details.expected.contains("allowed_fields"));
        assert!(details.expected.contains("reply_to_delivery"));
        assert!(!details.expected.contains(secret));
        let error =
            decode_params::<EventsReadRequest>(json!({"filter":{"agents":[false]}})).unwrap_err();
        assert_eq!(error.details.unwrap().field, "params.filter.agents[0]");
    }

    #[test]
    fn coordination_semantics_and_schema_constraints_share_the_dto_owner() {
        let schema = request_schema::<EventsSubscribeRequest>();
        assert!(schema["properties"]["wait_ms"].get("maximum").is_none());
        assert_eq!(
            decode_params::<EventsSubscribeRequest>(json!({"wait_ms":60000}))
                .unwrap()
                .wait_ms,
            30000
        );
        let schema = request_schema::<MailSendRequest>();
        assert_eq!(schema["properties"]["to"]["maxItems"], 100);
        assert_eq!(schema["properties"]["text"]["x-maxBytes"], 32768);
        assert_eq!(schema["additionalProperties"], false);
        let request = decode_params::<VarSetRequest>(
            json!({"id":"id","key":"k","value":null,"expect_version":-1}),
        )
        .unwrap();
        assert_eq!(
            request.validate().unwrap_err().details.unwrap().field,
            "params.expect_version"
        );
        let schema = request_schema::<VarSetRequest>();
        assert_eq!(schema["properties"]["expect_version"]["minimum"], 0);
        assert_eq!(schema["properties"]["value"]["x-maxEncodedBytes"], 16384);
        let schema = request_schema::<ProviderClearHoldRequest>();
        assert!(
            schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("expected_probe_command"))
        );
        assert_eq!(
            schema["properties"]["expected_probe_command"]["type"],
            json!(["string", "null"])
        );
    }
}
