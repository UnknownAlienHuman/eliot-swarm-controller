//! Harness-neutral discovery and private-history pages shared with plugins.
//! Vendor records remain opaque; native transport types stay in their plugins.

pub use crate::limits::{
    MAX_DISCOVER_PAGE_BYTES, MAX_DISCOVER_PAGE_ENTRIES, MAX_OBSERVATION_CURSOR_BYTES,
    MAX_OBSERVATION_PAGE_BYTES, MAX_OBSERVATION_PAGE_ENTRIES,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverSubject {
    pub native_ref: String,
    pub workspace: Option<String>,
    pub state: String,
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub source: Option<String>,
    pub ephemeral: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverCoverage {
    pub kind: String,
    pub complete: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverPage {
    pub harness: String,
    pub native_scope: String,
    pub subjects: Vec<DiscoverSubject>,
    pub next_cursor: Option<String>,
    pub coverage: DiscoverCoverage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservationQuery {
    #[serde(default = "native_observation_source")]
    pub source: String,
    #[serde(deserialize_with = "observation_view")]
    pub view: String,
    #[serde(default)]
    pub turn_id: Option<String>,
    #[serde(default, deserialize_with = "observation_cursor")]
    pub cursor: Option<String>,
    #[serde(deserialize_with = "observation_limit")]
    pub limit: usize,
}

impl ObservationQuery {
    pub fn validate(&self) -> Result<()> {
        if self.source.trim().is_empty()
            || !matches!(self.view.as_str(), "subject" | "turns" | "items")
            || !(1..=MAX_OBSERVATION_PAGE_ENTRIES).contains(&self.limit)
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.is_empty() || cursor.len() > MAX_OBSERVATION_CURSOR_BYTES
            })
        {
            return Err(Error::new("invalid", "invalid observation query"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ObservationPage {
    pub agent_id: String,
    pub generation: i64,
    pub native_scope: String,
    pub native_ref: String,
    pub source: String,
    pub view: String,
    pub entries: Vec<Value>,
    pub next_cursor: Option<String>,
    pub coverage: Value,
}

impl ObservationPage {
    /// Check before publishing a page or accepting it from a plugin.
    pub fn validate(&self) -> Result<()> {
        if self.source.trim().is_empty()
            || !matches!(self.view.as_str(), "subject" | "turns" | "items")
        {
            return Err(Error::new(
                "invalid",
                "invalid observation page source or view",
            ));
        }
        if self.entries.len() > MAX_OBSERVATION_PAGE_ENTRIES
            || self.next_cursor.as_ref().is_some_and(|cursor| {
                cursor.is_empty() || cursor.len() > MAX_OBSERVATION_CURSOR_BYTES
            })
        {
            return Err(Error::new(
                "frame_too_large",
                "observation page exceeds entry or cursor bounds",
            ));
        }
        let bytes = serde_json::to_vec(self)
            .map_err(|_| Error::new("internal", "cannot encode observation page"))?;
        if bytes.len() > MAX_OBSERVATION_PAGE_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "observation page exceeds 900 KiB",
            ));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ObservationPage {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Page {
            agent_id: String,
            generation: i64,
            native_scope: String,
            native_ref: String,
            source: String,
            #[serde(deserialize_with = "observation_view")]
            view: String,
            #[serde(deserialize_with = "observation_entries")]
            entries: Vec<Value>,
            #[serde(default, deserialize_with = "observation_cursor")]
            next_cursor: Option<String>,
            coverage: Value,
        }
        let page = Page::deserialize(deserializer)?;
        let page = Self {
            agent_id: page.agent_id,
            generation: page.generation,
            native_scope: page.native_scope,
            native_ref: page.native_ref,
            source: page.source,
            view: page.view,
            entries: page.entries,
            next_cursor: page.next_cursor,
            coverage: page.coverage,
        };
        page.validate().map_err(serde::de::Error::custom)?;
        Ok(page)
    }
}

fn native_observation_source() -> String {
    "native".to_owned()
}

fn observation_view<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let view = String::deserialize(deserializer)?;
    if !matches!(view.as_str(), "subject" | "turns" | "items") {
        return Err(serde::de::Error::custom("invalid observation view"));
    }
    Ok(view)
}

fn observation_cursor<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let cursor = Option::<String>::deserialize(deserializer)?;
    if cursor
        .as_ref()
        .is_some_and(|cursor| cursor.is_empty() || cursor.len() > MAX_OBSERVATION_CURSOR_BYTES)
    {
        return Err(serde::de::Error::custom(
            "invalid observation cursor length",
        ));
    }
    Ok(cursor)
}

fn observation_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<usize, D::Error> {
    let limit = usize::deserialize(deserializer)?;
    if !(1..=MAX_OBSERVATION_PAGE_ENTRIES).contains(&limit) {
        return Err(serde::de::Error::custom(
            "observation limit must be between 1 and 1000",
        ));
    }
    Ok(limit)
}

fn observation_entries<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Value>, D::Error> {
    let entries = Vec::<Value>::deserialize(deserializer)?;
    if entries.len() > MAX_OBSERVATION_PAGE_ENTRIES {
        return Err(serde::de::Error::custom(
            "observation page exceeds 1000 entries",
        ));
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn page() -> ObservationPage {
        ObservationPage {
            agent_id: "a1".to_owned(),
            generation: 1,
            native_scope: "scope".to_owned(),
            native_ref: "subject".to_owned(),
            source: "native".to_owned(),
            view: "subject".to_owned(),
            entries: vec![],
            next_cursor: None,
            coverage: json!({"complete": true}),
        }
    }

    #[test]
    fn discovery_move_preserves_wire_order_and_nullable_fields() {
        let page = DiscoverPage {
            harness: "fake".to_owned(),
            native_scope: "scope".to_owned(),
            subjects: vec![DiscoverSubject {
                native_ref: "subject".to_owned(),
                workspace: None,
                state: "unknown".to_owned(),
                created_at: None,
                updated_at: None,
                source: None,
                ephemeral: None,
            }],
            next_cursor: None,
            coverage: DiscoverCoverage {
                kind: "fixture".to_owned(),
                complete: true,
            },
        };
        let encoded = serde_json::to_string(&page).unwrap();
        assert_eq!(
            encoded,
            r#"{"harness":"fake","native_scope":"scope","subjects":[{"native_ref":"subject","workspace":null,"state":"unknown","created_at":null,"updated_at":null,"source":null,"ephemeral":null}],"next_cursor":null,"coverage":{"kind":"fixture","complete":true}}"#
        );
        let decoded: DiscoverPage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);
    }

    #[test]
    fn private_query_defaults_and_validation_are_unchanged() {
        let query: ObservationQuery =
            serde_json::from_value(json!({"view": "subject", "limit": 100})).unwrap();
        assert_eq!(
            serde_json::to_value(&query).unwrap(),
            json!({
                "source": "native", "view": "subject", "turn_id": null, "cursor": null, "limit": 100,
            })
        );
        let items_without_turn: ObservationQuery =
            serde_json::from_value(json!({"view": "items", "limit": 1})).unwrap();
        items_without_turn.validate().unwrap();
        let private_source: ObservationQuery = serde_json::from_value(json!({
            "source": "opaque-source", "view": "subject", "limit": 1, "cursor": "line\nbreak",
        }))
        .unwrap();
        private_source.validate().unwrap();
        assert!(serde_json::from_value::<ObservationQuery>(json!({"view": "subject"})).is_err());
        assert!(
            serde_json::from_value::<ObservationQuery>(json!({"view": "subject", "limit": 0}))
                .is_err()
        );
        assert!(
            serde_json::from_value::<ObservationQuery>(json!({"view": "subject", "limit": 1001}))
                .is_err()
        );
    }

    #[test]
    fn observation_move_preserves_wire_and_opaque_entries_and_coverage() {
        let mut page = page();
        assert_eq!(
            serde_json::to_string(&page).unwrap(),
            r#"{"agent_id":"a1","generation":1,"native_scope":"scope","native_ref":"subject","source":"native","view":"subject","entries":[],"next_cursor":null,"coverage":{"complete":true}}"#
        );
        page.entries = vec![json!({"native": [1, true, null]}), json!(["opaque", 3])];
        page.coverage = json!({"partial_tail": true, "custom": [false, null]});
        page.next_cursor = Some("opaque-cursor".to_owned());
        let encoded = serde_json::to_string(&page).unwrap();
        let decoded: ObservationPage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);
        assert_eq!(decoded.entries, page.entries);
        assert_eq!(decoded.coverage, page.coverage);
    }

    #[test]
    fn observation_limits_still_count_encoded_bytes_and_cursor_length() {
        let mut page = page();
        page.entries = vec![Value::Null; MAX_OBSERVATION_PAGE_ENTRIES];
        page.next_cursor = Some("x".repeat(MAX_OBSERVATION_CURSOR_BYTES));
        page.validate().unwrap();
        page.entries.push(Value::Null);
        assert_eq!(page.validate().unwrap_err().code, "frame_too_large");
        page.entries.clear();
        page.next_cursor.as_mut().unwrap().push('x');
        assert_eq!(page.validate().unwrap_err().code, "frame_too_large");
        page.next_cursor = None;
        page.coverage = json!({"data": ""});
        let overhead = serde_json::to_vec(&page).unwrap().len();
        page.coverage["data"] = json!("x".repeat(MAX_OBSERVATION_PAGE_BYTES - overhead));
        assert_eq!(
            serde_json::to_vec(&page).unwrap().len(),
            MAX_OBSERVATION_PAGE_BYTES
        );
        page.validate().unwrap();
        page.coverage["data"] = json!("x".repeat(MAX_OBSERVATION_PAGE_BYTES - overhead + 1));
        assert_eq!(page.validate().unwrap_err().code, "frame_too_large");
        page.coverage = json!({"data": "\0".repeat(MAX_OBSERVATION_PAGE_BYTES / 6 + 1)});
        assert_eq!(page.validate().unwrap_err().code, "frame_too_large");
    }
}
