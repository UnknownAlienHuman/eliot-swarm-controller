//! Public discovery/history request grammar and public-context reply checks.
//! Exact native subject/store/cursor fingerprints remain the Host/plugin boundary.

use super::{
    DiscoverPage, MAX_DISCOVER_PAGE_BYTES, MAX_OBSERVATION_CURSOR_BYTES, ObservationPage,
    ObservationQuery,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDiscoverRequest {
    #[serde(deserialize_with = "required_text")]
    pub profile: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    pub harness: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    pub native_scope: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    pub cursor: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    pub limit: usize,
}

impl AgentDiscoverRequest {
    pub fn new(profile: impl Into<String>) -> Self {
        Self {
            profile: profile.into(),
            harness: None,
            native_scope: None,
            cursor: None,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHistoryRequest {
    #[serde(deserialize_with = "required_text")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_generation"
    )]
    pub generation: Option<i64>,
    #[serde(deserialize_with = "required_text")]
    pub source: String,
    #[serde(deserialize_with = "required_text")]
    pub view: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    pub turn_id: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_text"
    )]
    pub cursor: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{DiscoverCoverage, DiscoverSubject};
    use serde_json::{Value, json};

    fn discover(value: Value) -> Result<AgentDiscoverRequest> {
        let request: AgentDiscoverRequest = serde_json::from_value(value)
            .map_err(|_| Error::new("invalid", "invalid fixture request"))?;
        request.validate()?;
        Ok(request)
    }

    fn history(value: Value) -> Result<AgentHistoryRequest> {
        let request: AgentHistoryRequest = serde_json::from_value(value)
            .map_err(|_| Error::new("invalid", "invalid fixture request"))?;
        request.validate()?;
        Ok(request)
    }

    fn discover_page() -> DiscoverPage {
        DiscoverPage {
            harness: "codex".to_owned(),
            native_scope: "scope".to_owned(),
            subjects: vec![DiscoverSubject {
                native_ref: "thread".to_owned(),
                workspace: None,
                state: "unknown".to_owned(),
                created_at: None,
                updated_at: None,
                source: None,
                ephemeral: None,
            }],
            next_cursor: None,
            coverage: DiscoverCoverage {
                kind: "state_db_only".to_owned(),
                complete: true,
            },
        }
    }

    fn history_page() -> ObservationPage {
        ObservationPage {
            agent_id: "a1".to_owned(),
            generation: 2,
            native_scope: "private scope".to_owned(),
            native_ref: "private ref".to_owned(),
            source: "rollout".to_owned(),
            view: "turns".to_owned(),
            entries: vec![json!(["opaque", 7])],
            next_cursor: None,
            coverage: json!({"kind":"rollout_jsonl","complete":false,"partial_tail":true}),
        }
    }

    fn unknown(result: Result<()>) {
        let error = result.unwrap_err();
        assert_eq!(error.code, "unknown");
        assert_eq!(error.message, "native metadata response is invalid");
        assert!(error.diagnostics.is_empty());
    }

    #[test]
    fn public_defaults_keep_required_fields_and_omit_nullable_options() {
        let request = discover(
            json!({"profile":" profile ","harness":null,"native_scope":null,"cursor":null}),
        )
        .unwrap();
        assert_eq!(request, AgentDiscoverRequest::new(" profile "));
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({"profile":" profile ","limit":100})
        );
        let request = history(json!({"id":" a01 ","source":"native","view":"turns","generation":null,"turn_id":null,"cursor":null})).unwrap();
        assert_eq!(
            request,
            AgentHistoryRequest::new(" a01 ", "native", "turns")
        );
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({"id":" a01 ","source":"native","view":"turns","limit":100})
        );
        for value in [
            json!({}),
            json!({"profile":null}),
            json!({"profile":" "}),
            json!({"profile":false}),
            json!({"profile":"p","extra":0}),
        ] {
            assert_eq!(discover(value).unwrap_err().code, "invalid");
        }
        for field in ["id", "source", "view"] {
            let mut missing = json!({"id":"a1","source":"native","view":"turns"});
            missing.as_object_mut().unwrap().remove(field);
            assert!(history(missing).is_err());
            for bad in [Value::Null, json!(true), json!(1), json!(""), json!(" \t ")] {
                let mut request = json!({"id":"a1","source":"native","view":"turns"});
                request[field] = bad;
                assert_eq!(history(request).unwrap_err().code, "invalid");
            }
        }
    }

    #[test]
    fn nullable_optional_fields_reject_wrong_types_and_empty_strings() {
        for field in ["harness", "native_scope", "cursor"] {
            for bad in [
                json!(true),
                json!(1),
                json!([]),
                json!({}),
                json!(""),
                json!(" \t "),
            ] {
                assert!(discover(json!({"profile":"p",(field):bad})).is_err());
            }
        }
        for field in ["turn_id", "cursor"] {
            for bad in [
                json!(true),
                json!(1),
                json!([]),
                json!({}),
                json!(""),
                json!(" \t "),
            ] {
                assert!(
                    history(json!({"id":"a1","source":"rollout","view":"turns",(field):bad}))
                        .is_err()
                );
            }
        }
        for bad in [
            json!(-1),
            json!(1.0),
            json!("1"),
            json!(true),
            json!(i64::MAX as u64 + 1),
            json!(u64::MAX),
        ] {
            assert!(
                history(json!({"id":"a1","source":"native","view":"turns","generation":bad}))
                    .is_err()
            );
        }
        for generation in [0, i64::MAX] {
            let request = history(
                json!({"id":"a1","source":"native","view":"turns","generation":generation}),
            )
            .unwrap();
            assert_eq!(request.generation, Some(generation));
        }
        assert!(history(json!({"id":"a1","source":"native","view":"turns","extra":0})).is_err());
    }

    #[test]
    fn discovery_scope_cursor_layers_use_bytes_and_controls_without_normalization() {
        let mut request = AgentDiscoverRequest::new("p".repeat(4096));
        request.harness = Some(" raw harness ".to_owned());
        request.native_scope = Some("é".repeat(256));
        request.cursor = Some("c".repeat(MAX_OBSERVATION_CURSOR_BYTES));
        request.validate().unwrap();
        let wire = serde_json::to_value(&request).unwrap();
        assert_eq!(wire["harness"], " raw harness ");
        request.native_scope.as_mut().unwrap().push('x');
        assert!(request.validate().is_err());
        request.native_scope = Some("scope\n".to_owned());
        assert!(request.validate().is_err());
        request.native_scope = None;
        request.cursor.as_mut().unwrap().push('x');
        assert!(request.validate().is_err());
        request.cursor = Some("opaque\0cursor".to_owned());
        assert!(request.validate().is_err());
    }

    #[test]
    fn history_preserves_current_cursor_policy_and_public_turn_relation() {
        let mut request = AgentHistoryRequest::new("a1", "native", "items");
        assert!(request.validate().is_err());
        request.turn_id = Some("é".repeat(MAX_OBSERVATION_CURSOR_BYTES / 2));
        request.cursor = Some("opaque\ncursor".to_owned());
        request.validate().unwrap();
        request.turn_id.as_mut().unwrap().push('x');
        assert!(request.validate().is_err());
        request.turn_id = Some("turn\n".to_owned());
        assert!(request.validate().is_err());
        request.turn_id = Some(" raw turn ".to_owned());
        request.validate().unwrap();
        request.view = "turns".to_owned();
        assert!(request.validate().is_err());
        request.turn_id = None;
        request.validate().unwrap();
        request.source = "other".to_owned();
        assert!(request.validate().is_err());
        request.source = "rollout".to_owned();
        request.view = "other".to_owned();
        assert!(request.validate().is_err());
        request.view = "subject".to_owned();
        request.cursor = Some("c".repeat(MAX_OBSERVATION_CURSOR_BYTES + 1));
        assert!(request.validate().is_err());
    }

    #[test]
    fn public_limits_are_strict_for_both_methods() {
        for bad in [
            Value::Null,
            json!(0),
            json!(-1),
            json!(1001),
            json!(1.0),
            json!("100"),
            json!(true),
            json!(u64::MAX),
        ] {
            assert!(discover(json!({"profile":"p","limit":bad})).is_err());
            assert!(
                history(json!({"id":"a1","source":"native","view":"turns","limit":bad})).is_err()
            );
        }
        for limit in [1, 1000] {
            assert_eq!(
                discover(json!({"profile":"p","limit":limit}))
                    .unwrap()
                    .limit,
                limit
            );
            assert_eq!(
                history(json!({"id":"a1","source":"native","view":"turns","limit":limit}))
                    .unwrap()
                    .limit,
                limit
            );
        }
    }

    #[test]
    fn discovery_reply_matches_only_available_public_context() {
        let mut request = AgentDiscoverRequest::new("an-alias-not-a-harness");
        let mut page = discover_page();
        request.validate_response(&page).unwrap();
        request.harness = Some("fake".to_owned());
        unknown(request.validate_response(&page));
        request.harness = Some("codex".to_owned());
        request.native_scope = Some("different".to_owned());
        unknown(request.validate_response(&page));
        request.native_scope = Some("scope".to_owned());
        request.validate_response(&page).unwrap();
        page.next_cursor = Some("cursor".to_owned());
        unknown(request.validate_response(&page));
        page.coverage.complete = false;
        request.validate_response(&page).unwrap();
        request.cursor = Some("cursor".to_owned());
        unknown(request.validate_response(&page));
        request.cursor = None;
        page.next_cursor = Some("bad\ncursor".to_owned());
        unknown(request.validate_response(&page));
        page = discover_page();
        request.limit = 0;
        unknown(request.validate_response(&page));
    }

    #[test]
    fn malformed_discovery_subjects_harness_and_encoded_pages_are_unknown() {
        let request = AgentDiscoverRequest::new("p");
        for harness in [
            "".to_owned(),
            " ".to_owned(),
            "h\n".to_owned(),
            "h".repeat(129),
        ] {
            let mut page = discover_page();
            page.harness = harness;
            unknown(request.validate_response(&page));
        }
        for reference in ["".to_owned(), "r\n".to_owned(), "r".repeat(513)] {
            let mut page = discover_page();
            page.subjects[0].native_ref = reference;
            unknown(request.validate_response(&page));
        }
        for state in ["".to_owned(), "s\n".to_owned(), "s".repeat(129)] {
            let mut page = discover_page();
            page.subjects[0].state = state;
            unknown(request.validate_response(&page));
        }
        let mut page = discover_page();
        page.subjects.push(page.subjects[0].clone());
        unknown(request.validate_response(&page));
        let mut page = discover_page();
        page.subjects[0].workspace = Some("\0".repeat(160 * 1024));
        unknown(request.validate_response(&page));
        let mut request = request;
        request.limit = 1;
        let mut page = discover_page();
        let mut second = page.subjects[0].clone();
        second.native_ref = "second".to_owned();
        page.subjects.push(second);
        unknown(request.validate_response(&page));
    }

    #[test]
    fn history_reply_does_not_infer_a_private_subject_or_complete_coverage() {
        let mut request = AgentHistoryRequest::new("a1", "rollout", "turns");
        let mut page = history_page();
        request.validate_response(&page).unwrap();
        assert_eq!(page.coverage["complete"], false);
        request.generation = Some(1);
        unknown(request.validate_response(&page));
        request.generation = Some(2);
        request.cursor = Some("old".to_owned());
        page.next_cursor = Some("old".to_owned());
        unknown(request.validate_response(&page)); // partial_tail is not an exception
        page.next_cursor = None;
        request.validate_response(&page).unwrap();
        for field in ["id", "source", "view"] {
            let mut request = request.clone();
            match field {
                "id" => request.id = "a2".to_owned(),
                "source" => request.source = "native".to_owned(),
                _ => request.view = "subject".to_owned(),
            }
            unknown(request.validate_response(&page));
        }
        let mut malformed = page.clone();
        malformed.coverage = json!([]);
        unknown(request.validate_response(&malformed));
        let mut malformed = page.clone();
        malformed.entries = vec![json!(null), json!(1)];
        request.limit = 1;
        unknown(request.validate_response(&malformed));
        request.limit = 100;
        request.source = "not-supported".to_owned();
        unknown(request.validate_response(&page));
        assert_eq!(request.validate().unwrap_err().code, "invalid");
    }

    #[test]
    fn history_bad_page_bounds_become_constant_unknown() {
        let request = AgentHistoryRequest::new("a1", "rollout", "turns");
        let mut page = history_page();
        page.entries = vec![json!("\0".repeat(160 * 1024))];
        unknown(request.validate_response(&page));
        let mut page = history_page();
        page.next_cursor = Some("c".repeat(MAX_OBSERVATION_CURSOR_BYTES + 1));
        unknown(request.validate_response(&page));
        let mut page = history_page();
        page.agent_id = "wrong".to_owned();
        unknown(request.validate_response(&page));
        let mut page = history_page();
        page.generation = -1;
        unknown(request.validate_response(&page));
    }
}
