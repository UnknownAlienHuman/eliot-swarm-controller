use serde::{Deserialize, Deserializer, Serialize};

/// The provider configuration Swarm accepts and freezes. The adapter owns
/// translation into the native configuration.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalProvider {
    pub id: String,
    pub package: String,
    pub base_url: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub api_key_env: Option<String>,
    pub models: Vec<LocalModel>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocalModel {
    pub id: String,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub context_limit: Option<u64>,
    #[serde(
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub output_limit: Option<u64>,
}

fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

impl LocalProvider {
    pub fn contains_model(&self, provider: &str, model: &str) -> bool {
        self.id == provider && self.models.iter().any(|entry| entry.id == model)
    }
}
