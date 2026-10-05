use crate::{
    MAX_CONTROLLER_EFFECT_TEXT_BYTES, MAX_CONTROLLER_EFFECTS, MAX_INPUT_BYTES, Result, ScriptError,
    canonical_json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_SCHEMA_DEPTH: usize = 16;
pub const MAX_SCHEMA_PROPERTIES: usize = 64;
pub const MAX_SCHEMA_BYTES: usize = 32 * 1024;
pub const MAX_PATH_BYTES: usize = 240;

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InterpreterKind {
    Python,
    Powershell,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ScriptControllerEffect {
    TaskOwnerMessage,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ScriptValueSchema {
    Null,
    Boolean,
    Integer,
    Number,
    String {
        max_bytes: usize,
    },
    Array {
        items: Box<ScriptValueSchema>,
        max_items: usize,
    },
    Object {
        #[serde(default)]
        properties: BTreeMap<String, ScriptValueSchema>,
        #[serde(default)]
        required: Vec<String>,
        #[serde(default)]
        additional_properties: bool,
    },
}

impl ScriptValueSchema {
    pub fn validate_definition(&self, depth: usize) -> Result<()> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        match self {
            Self::Null | Self::Boolean | Self::Integer | Self::Number => {}
            Self::String { max_bytes } if *max_bytes <= MAX_INPUT_BYTES => {}
            Self::String { .. } => return Err(ScriptError::new("INVALID_PARAMS")),
            Self::Array { items, max_items } => {
                if *max_items > 4096 {
                    return Err(ScriptError::new("INVALID_PARAMS"));
                }
                items.validate_definition(depth + 1)?;
            }
            Self::Object {
                properties,
                required,
                ..
            } => {
                if properties.len() > MAX_SCHEMA_PROPERTIES {
                    return Err(ScriptError::new("INVALID_PARAMS"));
                }
                let mut names = BTreeSet::new();
                for name in required {
                    if !properties.contains_key(name) || !names.insert(name) {
                        return Err(ScriptError::new("INVALID_PARAMS"));
                    }
                }
                for (name, schema) in properties {
                    if name.is_empty() || name.len() > 128 || name.contains('\0') {
                        return Err(ScriptError::new("INVALID_PARAMS"));
                    }
                    schema.validate_definition(depth + 1)?;
                }
            }
        }
        Ok(())
    }

    pub fn validate_value(&self, value: &Value) -> Result<()> {
        self.validate_value_at(value, 0)
    }

    fn validate_value_at(&self, value: &Value, depth: usize) -> Result<()> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(ScriptError::new("SCRIPT_SCHEMA_MISMATCH"));
        }
        let valid = match self {
            Self::Null => value.is_null(),
            Self::Boolean => value.is_boolean(),
            Self::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
            Self::Number => value.is_number(),
            Self::String { max_bytes } => {
                value.as_str().is_some_and(|text| text.len() <= *max_bytes)
            }
            Self::Array { items, max_items } => {
                let Some(values) = value.as_array() else {
                    return Err(ScriptError::new("SCRIPT_SCHEMA_MISMATCH"));
                };
                if values.len() > *max_items {
                    return Err(ScriptError::new("SCRIPT_SCHEMA_MISMATCH"));
                }
                for item in values {
                    items.validate_value_at(item, depth + 1)?;
                }
                true
            }
            Self::Object {
                properties,
                required,
                additional_properties,
            } => {
                let Some(values) = value.as_object() else {
                    return Err(ScriptError::new("SCRIPT_SCHEMA_MISMATCH"));
                };
                if required.iter().any(|field| !values.contains_key(field))
                    || (!additional_properties
                        && values.keys().any(|key| !properties.contains_key(key)))
                {
                    return Err(ScriptError::new("SCRIPT_SCHEMA_MISMATCH"));
                }
                for (key, item) in values {
                    if let Some(schema) = properties.get(key) {
                        schema.validate_value_at(item, depth + 1)?;
                    }
                }
                true
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ScriptError::new("SCRIPT_SCHEMA_MISMATCH"))
        }
    }
}

pub fn validate_schema_pair(
    input_schema: &ScriptValueSchema,
    result_schema: &ScriptValueSchema,
) -> Result<()> {
    input_schema.validate_definition(0)?;
    result_schema.validate_definition(0)?;
    let pair = serde_json::json!({"input":input_schema,"result":result_schema});
    if canonical_json(&pair)?.len() > MAX_SCHEMA_BYTES {
        return Err(ScriptError::new("INVALID_PARAMS"));
    }
    Ok(())
}

pub fn validate_script_id(id: &str) -> Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(ScriptError::new("INVALID_PARAMS"));
    }
    Ok(())
}

pub fn validate_bundle_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.contains(['\\', ':', '\0'])
        || path.starts_with('/')
        || path.ends_with('/')
        || !path
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/'))
    {
        return Err(ScriptError::new("INVALID_PARAMS"));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." || component.ends_with('.')
        {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        let stem = component
            .split('.')
            .next()
            .unwrap_or("")
            .to_ascii_uppercase();
        if matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            || ["COM", "LPT"].iter().any(|prefix| {
                stem.strip_prefix(prefix).is_some_and(|suffix| {
                    suffix.len() == 1 && matches!(suffix.as_bytes()[0], b'1'..=b'9')
                })
            })
        {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
    }
    Ok(())
}

pub fn validate_effect_grants(effects: &[ScriptControllerEffect]) -> Result<()> {
    if effects.len() > MAX_CONTROLLER_EFFECTS
        || effects.iter().collect::<BTreeSet<_>>().len() != effects.len()
    {
        return Err(ScriptError::new("SCRIPT_EFFECTS_INVALID"));
    }
    Ok(())
}

pub fn validate_effect_text(text: &str) -> Result<()> {
    if text.trim().is_empty()
        || text.len() > MAX_CONTROLLER_EFFECT_TEXT_BYTES
        || text.contains('\0')
    {
        return Err(ScriptError::new("SCRIPT_EFFECTS_INVALID"));
    }
    Ok(())
}
