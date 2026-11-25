use crate::{Error, Result, RoutingDecision};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

/// Launch selectors shared by the CLI, Router and Store. The original
/// envelope remains authoritative for key presence, adapter extensions and
/// the replay digest. Boundary validators still refuse malformed fields.
#[derive(Debug, Default, Serialize)]
pub struct LaunchCommandArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub registered_project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Owner-only model selector within the explicitly named saved profile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worktree: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub no_policy: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admission_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "_provider_probe")]
    pub provider_probe: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_override: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "switch_blank_scope")]
    pub switch_blank_scope: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goal: Option<Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_choice: Option<RoutingDecision>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<String>,
}

impl LaunchCommandArgs {
    pub fn required_text<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str> {
        value
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| Error::new("invalid", format!("{field} must be a non-empty string")))
    }

    pub fn optional_text<'a>(
        value: Option<&'a str>,
        original: &Value,
        field: &str,
    ) -> Result<Option<&'a str>> {
        if value.is_none() && original.get(field).is_some_and(|value| !value.is_null()) {
            return Err(Error::new("invalid", format!("{field} must be a string")));
        }
        Ok(value)
    }

    /// Project the selectors without changing the retained envelope. Some
    /// validators distinguish omission, null and a malformed value, and
    /// check the original key at the boundary that owns that decision.
    pub fn read(args: &Value) -> Result<Self> {
        let text = |key| args.get(key).and_then(Value::as_str).map(str::to_owned);
        let flag = |key| args.get(key).and_then(Value::as_bool);
        Ok(Self {
            workspace: text("workspace"),
            project: text("project"),
            registered_project: text("registered_project"),
            profile: text("profile"),
            role: text("role"),
            harness: text("harness"),
            model: text("model"),
            profile_model: optional_field(
                args,
                "profile_model",
                Value::is_string,
                "an exact model ID",
            )?,
            effort: text("effort"),
            auth: text("auth"),
            access: text("access"),
            cmd: text("cmd"),
            worktree: text("worktree"),
            base: text("base"),
            parent: text("parent"),
            text: text("text"),
            no_policy: text("no_policy"),
            admission_mode: text("admission_mode"),
            provider_probe: text(crate::PROVIDER_PROBE_LAUNCH_ARG),
            chat: flag("chat"),
            owner_override: flag("owner_override"),
            switch_blank_scope: flag(super::SWITCH_BLANK_SCOPE_ARG),
            goal: optional_field(args, "goal", Value::is_object, "a Goal object")?,
            profile_choice: optional_field(
                args,
                "profile_choice",
                Value::is_object,
                "a routing decision object",
            )?,
            sources: optional_field(args, "sources", Value::is_array, "an array of path strings")?
                .unwrap_or_default(),
        })
    }
}

fn optional_field<T: DeserializeOwned>(
    args: &Value,
    key: &str,
    shape: fn(&Value) -> bool,
    expected: &str,
) -> Result<Option<T>> {
    args.get(key)
        .map(|value| {
            let invalid = || Error::invalid_field(format!("args.{key}"), expected);
            if !shape(value) {
                return Err(invalid());
            }
            T::deserialize(value).map_err(|_| invalid())
        })
        .transpose()
}
