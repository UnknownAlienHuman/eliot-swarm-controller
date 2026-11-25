use crate::{Error, Result, RoutingDecision};
use serde::Serialize;
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
    pub fn read(args: &Value) -> Self {
        let text = |key| args.get(key).and_then(Value::as_str).map(str::to_owned);
        let flag = |key| args.get(key).and_then(Value::as_bool);
        Self {
            workspace: text("workspace"),
            project: text("project"),
            registered_project: text("registered_project"),
            profile: text("profile"),
            role: text("role"),
            harness: text("harness"),
            model: text("model"),
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
            goal: args.get("goal").and_then(Value::as_object).cloned(),
            profile_choice: args
                .get("profile_choice")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok()),
            sources: args
                .get("sources")
                .cloned()
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
        }
    }
}
