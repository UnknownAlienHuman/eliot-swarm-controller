use serde::Deserialize;

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub(crate) enum StringListInput {
    One(String),
    Many(Vec<String>),
}

/// Decode the unambiguous single-string or array form of a string-ID list.
/// The owning DTO still validates item syntax, uniqueness and list bounds.
pub(crate) fn deserialize_string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(match StringListInput::deserialize(deserializer)? {
        StringListInput::One(value) => vec![value],
        StringListInput::Many(values) => values,
    })
}
