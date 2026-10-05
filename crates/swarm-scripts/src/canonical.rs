use crate::{Result, ScriptError};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

fn ordered(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let sorted: BTreeMap<_, _> = object
                .iter()
                .map(|(key, value)| (key.clone(), ordered(value)))
                .collect();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.iter().map(ordered).collect()),
        value => value.clone(),
    }
}

pub(crate) fn json(value: &Value) -> Result<String> {
    serde_json::to_string(&ordered(value)).map_err(|_| ScriptError::new("SCRIPT_PROTOCOL_INVALID"))
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
