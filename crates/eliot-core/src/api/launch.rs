use crate::{Error, Result, RoutingDecision};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value};

const SELECTOR_FIELDS: [&str; 5] = [
    "model",
    "effort",
    "model_source",
    "effort_source",
    "daemon_model",
];

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
    /// Compatibility spelling for a named profile's launch model.
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
    pub fn selected_model(&self) -> Option<&str> {
        self.model.as_deref().or(self.profile_model.as_deref())
    }

    pub fn has_selection(&self) -> bool {
        self.selected_model().is_some() || self.effort.is_some()
    }

    /// Recover the saved bounds for an explicit delegated selection.
    pub fn restore_saved_selectors(snapshot: &mut Value) {
        let Some(snapshot) = snapshot.as_object_mut() else {
            return;
        };
        if let Some(Value::Object(mut saved)) = snapshot.remove("launch_selection") {
            for key in SELECTOR_FIELDS {
                snapshot.remove(key);
                if let Some(value) = saved.remove(key) {
                    snapshot.insert(key.to_owned(), value);
                }
            }
        }
    }

    pub fn check_model_bound(saved: &Value, model: Option<&str>) -> Result<()> {
        if let Some(model) = model
            && !saved["launch_models"]
                .as_array()
                .is_some_and(|models| models.iter().any(|allowed| allowed.as_str() == Some(model)))
        {
            return Err(Error::new(
                "model_outside_launch_bounds",
                "requested model is outside the saved profile's launch_models",
            ));
        }
        Ok(())
    }

    pub fn check_delegated_selectors(&self, saved: &Value) -> Result<()> {
        Self::check_model_bound(saved, self.selected_model())?;
        if let Some(effort) = self.effort.as_deref()
            && saved["effort"].as_str() != Some(effort)
        {
            return Err(Error::new(
                "not_allowed",
                "delegated launch effort must match the saved profile",
            ));
        }
        Ok(())
    }

    /// Freeze the selection while retaining the default selectors for children.
    pub fn freeze_saved_selectors(&self, snapshot: &mut Value) {
        if !self.has_selection() {
            return;
        }
        let Some(snapshot) = snapshot.as_object_mut() else {
            return;
        };
        let saved = SELECTOR_FIELDS
            .into_iter()
            .filter_map(|key| {
                snapshot
                    .get(key)
                    .map(|value| (key.to_owned(), value.clone()))
            })
            .collect();
        snapshot.insert("launch_selection".to_owned(), Value::Object(saved));
        if let Some(model) = self.selected_model() {
            snapshot.remove("daemon_model");
            snapshot.insert("model".to_owned(), Value::String(model.to_owned()));
            snapshot.insert(
                "model_source".to_owned(),
                Value::String("launch".to_owned()),
            );
        }
        if let Some(effort) = &self.effort {
            snapshot.insert("effort".to_owned(), Value::String(effort.clone()));
            snapshot.insert(
                "effort_source".to_owned(),
                Value::String("launch".to_owned()),
            );
        }
    }

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
