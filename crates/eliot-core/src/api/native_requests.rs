//! Public discovery/history request grammar and public-context reply checks.
//! Exact native subject/store/cursor fingerprints remain the Host/plugin boundary.

use super::{
    DiscoverPage, MAX_DISCOVER_PAGE_BYTES, MAX_OBSERVATION_CURSOR_BYTES, ObservationPage,
    ObservationQuery,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDiscoverRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub profile: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub harness: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub native_scope: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "crate::is_false")]
    pub archived: bool,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl AgentDiscoverRequest {
    pub fn new(profile: impl Into<String>) -> Self {
        Self {
            profile: profile.into(),
            harness: None,
            native_scope: None,
            cursor: None,
            archived: false,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_text(&self.profile)?;
        for value in [&self.harness, &self.native_scope, &self.cursor]
            .into_iter()
            .flatten()
        {
            validate_text(value)?;
        }
        if self
            .native_scope
            .as_ref()
            .is_some_and(|value| !bounded_text(value, 512))
            || self
                .cursor
                .as_ref()
                .is_some_and(|value| !bounded_text(value, MAX_OBSERVATION_CURSOR_BYTES))
        {
            return Err(Error::new("invalid", "invalid discovery scope or cursor"));
        }
        validate_limit(self.limit)
    }

    pub fn validate_response(&self, page: &DiscoverPage) -> Result<()> {
        self.validate().map_err(|_| invalid_response())?;
        if !bounded_text(&page.harness, 128)
            || page.harness.trim().is_empty()
            || self
                .harness
                .as_ref()
                .is_some_and(|value| value != &page.harness)
            || self
                .native_scope
                .as_ref()
                .is_some_and(|value| value != &page.native_scope)
            || !bounded_text(&page.native_scope, 512)
            || page.subjects.len() > self.limit
            || !bounded_text(&page.coverage.kind, 128)
            || page.coverage.complete != page.next_cursor.is_none()
            || page.next_cursor.as_ref().is_some_and(|value| {
                !bounded_text(value, MAX_OBSERVATION_CURSOR_BYTES)
                    || Some(value) == self.cursor.as_ref()
            })
            || serde_json::to_vec(page).map_or(true, |bytes| bytes.len() > MAX_DISCOVER_PAGE_BYTES)
        {
            return Err(invalid_response());
        }
        let mut subjects = BTreeSet::new();
        if page.subjects.iter().any(|subject| {
            !bounded_text(&subject.native_ref, 512)
                || !bounded_text(&subject.state, 128)
                || !subjects.insert(subject.native_ref.as_str())
        }) {
            return Err(invalid_response());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHistoryRequest {
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    #[schemars(range(min = 0))]
    pub generation: Option<i64>,
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub source: String,
    #[serde(deserialize_with = "required_text")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub view: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub turn_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub cursor: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl AgentHistoryRequest {
    pub fn new(id: impl Into<String>, source: impl Into<String>, view: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            generation: None,
            source: source.into(),
            view: view.into(),
            turn_id: None,
            cursor: None,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_text(&self.id)?;
        validate_text(&self.source)?;
        validate_text(&self.view)?;
        if !matches!(self.source.as_str(), "native" | "rollout")
            || self.generation.is_some_and(|value| value < 0)
            || (self.view == "items") != self.turn_id.is_some()
        {
            return Err(Error::new(
                "invalid",
                "invalid history source, generation or turn binding",
            ));
        }
        if let Some(turn) = &self.turn_id {
            validate_text(turn)?;
            if !bounded_text(turn, MAX_OBSERVATION_CURSOR_BYTES) {
                return Err(Error::new("invalid", "invalid history turn_id"));
            }
        }
        if let Some(cursor) = &self.cursor {
            validate_text(cursor)?;
        }
        // Reuse the existing private query bounds; notably history cursors do
        // not gain a control-character restriction or any new fingerprint rule.
        ObservationQuery {
            source: self.source.clone(),
            view: self.view.clone(),
            turn_id: self.turn_id.clone(),
            cursor: self.cursor.clone(),
            limit: self.limit,
        }
        .validate()
    }

    pub fn validate_response(&self, page: &ObservationPage) -> Result<()> {
        self.validate().map_err(|_| invalid_response())?;
        page.validate().map_err(|_| invalid_response())?;
        if page.agent_id != self.id
            || page.generation < 0
            || self
                .generation
                .is_some_and(|value| value != page.generation)
            || page.native_scope.is_empty()
            || page.native_ref.is_empty()
            || page.source != self.source
            || page.view != self.view
            || page.entries.len() > self.limit
            || !page.coverage.is_object()
            || page
                .next_cursor
                .as_ref()
                .is_some_and(|value| Some(value) == self.cursor.as_ref())
        {
            return Err(invalid_response());
        }
        // Scope/ref are present but have no expected values in this public
        // request. The Router/Host separately verifies its exact current subject.
        Ok(())
    }
}

fn validate_text(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new("invalid", "text must be non-empty"));
    }
    Ok(())
}

fn bounded_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}

fn validate_limit(value: usize) -> Result<()> {
    if !(1..=super::MAX_LIMIT).contains(&value) {
        return Err(Error::new("invalid", "limit must be between 1 and 1000"));
    }
    Ok(())
}

fn required_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    validate_text(&value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn optional_text<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let value = Option::<String>::deserialize(deserializer)?;
    if let Some(value) = &value {
        validate_text(value).map_err(serde::de::Error::custom)?;
    }
    Ok(value)
}

fn optional_generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error> {
    let value = Option::<i64>::deserialize(deserializer)?;
    if value.is_some_and(|value| value < 0) {
        return Err(serde::de::Error::custom("generation must be nonnegative"));
    }
    Ok(value)
}

fn invalid_response() -> Error {
    Error::new("unknown", "native metadata response is invalid")
}
