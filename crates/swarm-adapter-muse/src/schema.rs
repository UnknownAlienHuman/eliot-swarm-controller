//! Public MSP JSON Schema access and validation.
//!
//! The bundled document is the unmodified public stable schema from
//! `meta-models/muse-code-sdk` at [`UPSTREAM_MSP_SCHEMA_COMMIT`]. This module
//! validates schema definitions and method parameters; it does not define an
//! MSP type system or connection state machine.
//!
//! The upstream `initialize` result's `schema.fingerprint` describes the
//! server binary's stable-surface bundle. It is distinct from
//! [`UPSTREAM_MSP_SCHEMA_SHA256_HEX`], which identifies this checked-in schema
//! file's bytes. See `wire/README.md` for the source anchors and contract.

use std::{error::Error, fmt, sync::OnceLock};

use jsonschema::ValidatorMap;
use serde_json::Value;

/// Exact upstream SDK commit that supplied `wire/msp.schema.json`.
pub const UPSTREAM_MSP_SCHEMA_COMMIT: &str = "a7c10c5dd3f66be412077d29f9d11111af70317b";

/// SHA-256 of the unmodified schema file bytes, without a wire-format prefix.
/// This is not the `initialize` stable-surface fingerprint.
pub const UPSTREAM_MSP_SCHEMA_SHA256_HEX: &str =
    "ed442d494cf8cdb85f0b1487f8d0f54b7570548561c6c92d6def09ce107e1664";

/// The unmodified public MSP stable schema document.
pub const MSP_SCHEMA_JSON: &str = include_str!("../wire/msp.schema.json");

static DOCUMENT: OnceLock<Result<Value, String>> = OnceLock::new();
static VALIDATORS: OnceLock<Result<ValidatorMap, String>> = OnceLock::new();

/// Errors while locating or validating an upstream MSP schema target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// The pinned document could not be parsed or compiled as a schema.
    InvalidBundledSchema(String),
    /// No `$defs` entry has this name.
    UnknownDefinition(String),
    /// No public method has this name.
    UnknownMethod(String),
    /// The named public method has no `params` member.
    MethodHasNoParameters(String),
    /// A parsed target did not receive a compiled validator.
    CompiledValidatorMissing(String),
    /// The value failed validation. Paths are included without echoing values.
    InvalidValue {
        target: String,
        instance_path: String,
        schema_path: String,
    },
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBundledSchema(message) => {
                write!(f, "bundled MSP schema is invalid: {message}")
            }
            Self::UnknownDefinition(name) => write!(f, "unknown MSP definition: {name}"),
            Self::UnknownMethod(name) => write!(f, "unknown MSP method: {name}"),
            Self::MethodHasNoParameters(name) => {
                write!(f, "MSP method has no params schema: {name}")
            }
            Self::CompiledValidatorMissing(pointer) => {
                write!(f, "compiled MSP schema has no validator for {pointer}")
            }
            Self::InvalidValue {
                target,
                instance_path,
                schema_path,
            } => write!(
                f,
                "value does not match MSP schema {target} at instance {instance_path} (schema {schema_path})"
            ),
        }
    }
}

impl Error for SchemaError {}

fn document() -> Result<&'static Value, SchemaError> {
    match DOCUMENT
        .get_or_init(|| serde_json::from_str(MSP_SCHEMA_JSON).map_err(|error| error.to_string()))
    {
        Ok(document) => Ok(document),
        Err(message) => Err(SchemaError::InvalidBundledSchema(message.clone())),
    }
}

fn validators() -> Result<&'static ValidatorMap, SchemaError> {
    let result = VALIDATORS.get_or_init(|| {
        let document = document().map_err(|error| error.to_string())?;
        // All references in this pinned document are local. Offline mode also
        // makes accidental future file or HTTP references fail closed.
        jsonschema::options()
            .offline()
            .build_map(document)
            .map_err(|error| error.to_string())
    });

    result
        .as_ref()
        .map_err(|message| SchemaError::InvalidBundledSchema(message.clone()))
}

fn pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

/// Return a named public definition from the upstream `$defs` object.
pub fn definition_schema(name: &str) -> Result<&'static Value, SchemaError> {
    document()?
        .get("$defs")
        .and_then(|definitions| definitions.get(name))
        .ok_or_else(|| SchemaError::UnknownDefinition(name.to_owned()))
}

/// Return the parameter schema for a public method.
///
/// Methods that take no parameters produce [`SchemaError::MethodHasNoParameters`].
pub fn method_parameters_schema(method: &str) -> Result<&'static Value, SchemaError> {
    let methods = document()?
        .get("methods")
        .ok_or_else(|| SchemaError::InvalidBundledSchema("missing methods object".into()))?;
    let method_schema = methods
        .get(method)
        .ok_or_else(|| SchemaError::UnknownMethod(method.to_owned()))?;
    method_schema
        .get("params")
        .ok_or_else(|| SchemaError::MethodHasNoParameters(method.to_owned()))
}

/// Return the upstream schema for `InitializeResult.schema.version`.
///
/// The returned schema says the value is an integer; its description records
/// that the v1 envelope schema version is `1`. No local version constraint is
/// added here.
pub fn initialize_schema_version_schema() -> Result<&'static Value, SchemaError> {
    schema_info_property("version")
}

/// Return the upstream schema for `InitializeResult.schema.fingerprint`.
///
/// The returned pattern and description come directly from `$defs.SchemaInfo`.
/// The fingerprint is the server binary's stable-surface bundle digest, not
/// [`UPSTREAM_MSP_SCHEMA_SHA256_HEX`].
pub fn initialize_schema_fingerprint_schema() -> Result<&'static Value, SchemaError> {
    schema_info_property("fingerprint")
}

fn schema_info_property(name: &str) -> Result<&'static Value, SchemaError> {
    definition_schema("SchemaInfo")?
        .get("properties")
        .and_then(|properties| properties.get(name))
        .ok_or_else(|| {
            SchemaError::InvalidBundledSchema(format!("missing SchemaInfo.properties.{name}"))
        })
}

fn validate_pointer(pointer: &str, target: String, value: &Value) -> Result<(), SchemaError> {
    let validator = validators()?
        .get(pointer)
        .ok_or_else(|| SchemaError::CompiledValidatorMissing(pointer.to_owned()))?;
    validator
        .validate(value)
        .map_err(|error| SchemaError::InvalidValue {
            target,
            instance_path: error.instance_path().to_string(),
            schema_path: error.schema_path().to_string(),
        })
}

/// Validate a JSON value against one upstream `$defs` entry.
pub fn validate_definition(name: &str, value: &Value) -> Result<(), SchemaError> {
    definition_schema(name)?;
    let pointer = format!("#/$defs/{}", pointer_token(name));
    validate_pointer(&pointer, format!("$defs/{name}"), value)
}

/// Validate a JSON value against the `params` schema for a public method.
pub fn validate_method_parameters(method: &str, value: &Value) -> Result<(), SchemaError> {
    method_parameters_schema(method)?;
    let pointer = format!("#/methods/{}/params", pointer_token(method));
    validate_pointer(&pointer, format!("methods/{method}/params"), value)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        SchemaError, definition_schema, initialize_schema_fingerprint_schema,
        initialize_schema_version_schema, method_parameters_schema, validate_definition,
        validate_method_parameters,
    };

    #[test]
    fn initialize_method_parameters_follow_the_public_schema() {
        let valid = json!({
            "clientInfo": {"name": "swarm_adapter", "version": "0.1.0"}
        });
        assert!(validate_method_parameters("initialize", &valid).is_ok());

        let missing_client_info = json!({});
        assert!(validate_method_parameters("initialize", &missing_client_info).is_err());

        let invalid_client_name = json!({
            "clientInfo": {"name": "Swarm Adapter", "version": "0.1.0"}
        });
        assert!(validate_method_parameters("initialize", &invalid_client_name).is_err());
    }

    #[test]
    fn initialize_schema_info_uses_the_pinned_fingerprint_rule() {
        let fingerprint_schema = initialize_schema_fingerprint_schema().unwrap();
        assert_eq!(
            fingerprint_schema
                .get("pattern")
                .and_then(|value| value.as_str()),
            Some("^sha256:[0-9a-f]{64}$")
        );
        assert!(
            fingerprint_schema
                .get("description")
                .and_then(|value| value.as_str())
                .unwrap()
                .contains("stable-surface bundle")
        );

        let version_schema = initialize_schema_version_schema().unwrap();
        assert_eq!(
            version_schema.get("type").and_then(|value| value.as_str()),
            Some("integer")
        );

        let valid = json!({
            "version": 1,
            "fingerprint": format!("sha256:{}", "a".repeat(64))
        });
        assert!(validate_definition("SchemaInfo", &valid).is_ok());

        let wrong_fingerprint_grammar = json!({
            "version": 1,
            "fingerprint": format!("sha256:{}", "A".repeat(64))
        });
        assert!(validate_definition("SchemaInfo", &wrong_fingerprint_grammar).is_err());

        let wrong_version_type = json!({
            "version": "1",
            "fingerprint": format!("sha256:{}", "a".repeat(64))
        });
        assert!(validate_definition("SchemaInfo", &wrong_version_type).is_err());
    }

    #[test]
    fn schema_target_errors_distinguish_unknown_and_parameterless_methods() {
        assert!(matches!(
            definition_schema("NotAnMspDefinition"),
            Err(SchemaError::UnknownDefinition(_))
        ));
        assert!(matches!(
            method_parameters_schema("not/a/method"),
            Err(SchemaError::UnknownMethod(_))
        ));
        assert!(matches!(
            method_parameters_schema("usage/read"),
            Err(SchemaError::MethodHasNoParameters(_))
        ));
    }
}
