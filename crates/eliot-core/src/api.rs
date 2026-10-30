//! Shared public API DTOs for events, views, reads and mutations. Authorization,
//! admission and effect handling remain in the Router and Store.

mod agent_views;
mod monitor_views;
mod mutation_validation;
mod mutations;
mod native_pages;
mod native_requests;
mod provider;
mod read_validation;
mod reads;
mod rules;
mod scripts;
mod timers;

pub use agent_views::{
    AgentEditsRequest, AgentEditsResponse, AgentGetRequest, AgentGetResponse, AgentGitRequest,
    AgentGitResponse, AgentListRequest, AgentListResponse, AgentListRow, AgentProcessesRequest,
    AgentProcessesResponse, AgentStatsRequest, AgentStatsResponse, DaemonStatusRequest,
    DaemonStatusResponse, PROCESS_CURSOR_MAX_BYTES, ProcessPageCursor, ProjectRegistrationView,
    SourceOwner, SourceState, StatusBlocker, UnsettledCommandSummary, WorkspaceCustodyView,
};
pub use monitor_views::{
    FilesystemInvalidationState, FilesystemInvalidationView, GitObservation, GitView,
    LivenessBasis, LivenessView, MonitoringSourceState, ObservationData, ObservationView,
    ProcessStatsView, StatsCoverage, WorkspaceObservation, WorkspaceView,
};
pub use mutations::{
    CommandSubmitRequest, CommandSubmitResponse, DaemonShutdownRequest, DaemonShutdownResponse,
    ShutdownMode,
};
pub use native_pages::{
    DiscoverCoverage, DiscoverPage, DiscoverSubject, MAX_DISCOVER_PAGE_BYTES,
    MAX_DISCOVER_PAGE_ENTRIES, MAX_OBSERVATION_CURSOR_BYTES, MAX_OBSERVATION_PAGE_BYTES,
    MAX_OBSERVATION_PAGE_ENTRIES, ObservationPage, ObservationQuery,
};
pub use native_requests::{AgentDiscoverRequest, AgentHistoryRequest};
pub use provider::{
    ProviderClearHoldRequest, ProviderClearHoldResponse, ProviderCorrectiveEvidence,
    ProviderHoldsRequest, ProviderHoldsResponse,
};
pub use reads::{
    AgentResultRequest, AgentResultResponse, CommandGetRequest, RuleGetRequest, RuleStateView,
    TurnGetRequest,
};
pub use rules::{RuleCatchUpRequest, RuleLimit, RuleLimitScope};
pub use scripts::{
    FrozenScriptContext, FrozenScriptInterpreter, FrozenScriptRun, RunDefinitionArgs,
    ScriptActionRecord, ScriptCommandContext, ScriptCustody, ScriptDirectExit, ScriptInputContext,
    ScriptInterpreter, ScriptProcessResult, ScriptRuleContext, ScriptRunApplied,
    ScriptTimerContext, ScriptTimerInvocation, bound_script_outcome_stderr,
};
pub use timers::{CatchUp, TimerGetRequest, TimerRunRequest, TimerSchedule, TimerStateView};

use crate::{Error, Event, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1_000;
const MAX_WAIT_MS: u64 = 30_000;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsFilter {
    #[serde(default, deserialize_with = "filter_values")]
    pub agents: Vec<String>,
    #[serde(default, deserialize_with = "filter_values")]
    pub kinds: Vec<String>,
    #[serde(default, deserialize_with = "filter_values")]
    pub sources: Vec<String>,
}

impl EventsFilter {
    pub fn validate(&self) -> Result<()> {
        for values in [&self.agents, &self.kinds, &self.sources] {
            validate_filter_values(values)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsReadRequest {
    #[serde(default, deserialize_with = "event_after")]
    pub after: i64,
    #[serde(default = "default_limit", deserialize_with = "event_limit")]
    pub limit: usize,
    #[serde(default, deserialize_with = "object_filter")]
    pub filter: EventsFilter,
}

impl Default for EventsReadRequest {
    fn default() -> Self {
        Self {
            after: 0,
            limit: DEFAULT_LIMIT,
            filter: EventsFilter::default(),
        }
    }
}

impl EventsReadRequest {
    pub fn validate(&self) -> Result<()> {
        validate_query(self.after, self.limit, &self.filter)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventsSubscribeRequest {
    #[serde(default, deserialize_with = "event_after")]
    pub after: i64,
    #[serde(default = "default_limit", deserialize_with = "event_limit")]
    pub limit: usize,
    #[serde(default, deserialize_with = "object_filter")]
    pub filter: EventsFilter,
    #[serde(default, deserialize_with = "event_wait_ms")]
    pub wait_ms: u64,
}

impl Default for EventsSubscribeRequest {
    fn default() -> Self {
        Self {
            after: 0,
            limit: DEFAULT_LIMIT,
            filter: EventsFilter::default(),
            wait_ms: 0,
        }
    }
}

impl EventsSubscribeRequest {
    /// Large wait values are valid and are clamped when decoded from the wire.
    pub fn validate(&self) -> Result<()> {
        validate_query(self.after, self.limit, &self.filter)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventsReadResponse {
    pub events: Vec<Event>,
    pub next: i64,
    pub high_water: i64,
    pub truncated: bool,
}

impl EventsReadResponse {
    pub fn validate(&self, after: i64) -> Result<()> {
        validate_event_response(after, self.next, self.high_water, &self.events)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventsSubscribeResponse {
    pub events: Vec<Event>,
    pub next: i64,
    pub high_water: i64,
    pub truncated: bool,
    pub lagged: bool,
}

impl EventsSubscribeResponse {
    pub fn validate(&self, after: i64) -> Result<()> {
        validate_event_response(after, self.next, self.high_water, &self.events)
    }
}

fn validate_event_response(after: i64, next: i64, high_water: i64, events: &[Event]) -> Result<()> {
    if after < 0 || next < after || high_water < next {
        return Err(Error::new("unknown", "event response is invalid"));
    }
    let mut previous = after;
    for event in events {
        if event.seq <= previous || event.seq > next {
            return Err(Error::new("unknown", "event response is invalid"));
        }
        previous = event.seq;
    }
    Ok(())
}

fn default_limit() -> usize {
    DEFAULT_LIMIT
}

fn validate_query(after: i64, limit: usize, filter: &EventsFilter) -> Result<()> {
    if after < 0 {
        return Err(Error::new("invalid", "after must be a nonnegative integer"));
    }
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(Error::new("invalid", "limit must be between 1 and 1000"));
    }
    filter.validate()
}

fn validate_filter_values(values: &[String]) -> Result<()> {
    if values.len() > MAX_LIMIT || values.iter().any(String::is_empty) {
        return Err(Error::new(
            "invalid",
            "event filter lists require at most 1000 non-empty strings",
        ));
    }
    Ok(())
}

fn event_after<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let after = i64::deserialize(deserializer)?;
    if after < 0 {
        return Err(serde::de::Error::custom(
            "after must be a nonnegative integer",
        ));
    }
    Ok(after)
}

fn event_limit<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<usize, D::Error> {
    let limit = usize::deserialize(deserializer)?;
    if !(1..=MAX_LIMIT).contains(&limit) {
        return Err(serde::de::Error::custom("limit must be between 1 and 1000"));
    }
    Ok(limit)
}

fn filter_values<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    let values = Vec::<String>::deserialize(deserializer)?;
    validate_filter_values(&values).map_err(serde::de::Error::custom)?;
    Ok(values)
}

fn object_filter<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<EventsFilter, D::Error> {
    // A named struct alone accepts positional sequences. Actual nested filters
    // require an object, then reuse the single published DTO's field validation.
    let object = Map::<String, Value>::deserialize(deserializer)?;
    serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
}

fn event_wait_ms<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<u64, D::Error> {
    Ok(u64::deserialize(deserializer)?.min(MAX_WAIT_MS))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn invalid_for_both(value: Value) {
        assert!(
            serde_json::from_value::<EventsReadRequest>(value.clone()).is_err(),
            "read accepted {value}"
        );
        assert!(
            serde_json::from_value::<EventsSubscribeRequest>(value.clone()).is_err(),
            "subscribe accepted {value}"
        );
    }

    #[test]
    fn omitted_fields_keep_the_existing_defaults() {
        let read: EventsReadRequest = serde_json::from_value(json!({})).unwrap();
        assert_eq!(read, EventsReadRequest::default());
        assert_eq!(read.after, 0);
        assert_eq!(read.limit, 100);
        assert_eq!(read.filter, EventsFilter::default());
        let subscribe: EventsSubscribeRequest = serde_json::from_value(json!({})).unwrap();
        assert_eq!(subscribe, EventsSubscribeRequest::default());
        assert_eq!(subscribe.wait_ms, 0);
    }

    #[test]
    fn supplied_bad_after_is_never_treated_as_omitted() {
        for after in [
            Value::Null,
            json!(0.0),
            json!(1.0),
            json!("1"),
            json!(true),
            json!(false),
            json!(-1),
            json!(i64::MAX as u64 + 1),
            json!(u64::MAX),
            json!([]),
            json!({}),
        ] {
            invalid_for_both(json!({"after":after}));
        }
        for after in [0, i64::MAX] {
            let read: EventsReadRequest = serde_json::from_value(json!({"after":after})).unwrap();
            let subscribe: EventsSubscribeRequest =
                serde_json::from_value(json!({"after":after})).unwrap();
            assert_eq!(read.after, after);
            assert_eq!(subscribe.after, after);
        }
    }

    #[test]
    fn limits_are_strict_and_bounded() {
        for limit in [
            Value::Null,
            json!(0),
            json!(1001),
            json!(-1),
            json!(1.0),
            json!("100"),
            json!(true),
            json!(u64::MAX),
        ] {
            invalid_for_both(json!({"limit":limit}));
        }
        for limit in [1, 1000] {
            let read: EventsReadRequest = serde_json::from_value(json!({"limit":limit})).unwrap();
            let subscribe: EventsSubscribeRequest =
                serde_json::from_value(json!({"limit":limit})).unwrap();
            assert_eq!(read.limit, limit);
            assert_eq!(subscribe.limit, limit);
        }
    }

    #[test]
    fn filters_reject_bad_types_and_unknown_fields_without_tightening_strings() {
        for filter in [
            Value::Null,
            json!([]),
            json!([[], [], []]),
            json!([["a1"], [], []]),
            json!(false),
            json!("all"),
            json!({"tree":"a1"}),
            json!({"ephemeral":true}),
        ] {
            invalid_for_both(json!({"filter":filter}));
        }
        for name in ["agents", "kinds", "sources"] {
            for values in [
                Value::Null,
                json!(true),
                json!("a1"),
                json!([""]),
                json!([1]),
                json!([false]),
                json!([null]),
                json!([{}]),
            ] {
                invalid_for_both(json!({"filter":{(name):values}}));
            }
        }
        let filter =
            json!({"agents":["a01","a+1","not-an-agent"," "],"kinds":[" "],"sources":["x\n"]});
        let read: EventsReadRequest = serde_json::from_value(json!({"filter":filter})).unwrap();
        assert_eq!(read.filter.agents, ["a01", "a+1", "not-an-agent", " "]);
        read.validate().unwrap();
    }

    #[test]
    fn filter_count_is_checked_before_any_deduplication() {
        for name in ["agents", "kinds", "sources"] {
            invalid_for_both(json!({"filter":{(name):vec!["duplicate";1001]}}));
            let read: EventsReadRequest =
                serde_json::from_value(json!({"filter":{(name):vec!["duplicate";1000]}})).unwrap();
            let values = match name {
                "agents" => read.filter.agents,
                "kinds" => read.filter.kinds,
                _ => read.filter.sources,
            };
            assert_eq!(values.len(), 1000);
        }
    }

    #[test]
    fn requests_reject_unknown_root_fields() {
        invalid_for_both(Value::Null);
        invalid_for_both(json!({"unexpected":0}));
        assert!(serde_json::from_value::<EventsReadRequest>(json!({"wait_ms":0})).is_err());
    }

    #[test]
    fn subscribe_wait_rejects_bad_types_but_clamps_large_unsigned_values() {
        for wait in [
            Value::Null,
            json!(-1),
            json!(0.0),
            json!(1.0),
            json!("1000"),
            json!(true),
            json!([]),
            json!({}),
        ] {
            assert!(
                serde_json::from_value::<EventsSubscribeRequest>(json!({"wait_ms":wait})).is_err()
            );
        }
        for (wait, expected) in [
            (0, 0),
            (1000, 1000),
            (30_000, 30_000),
            (30_001, 30_000),
            (u64::MAX, 30_000),
        ] {
            let request: EventsSubscribeRequest =
                serde_json::from_value(json!({"wait_ms":wait})).unwrap();
            assert_eq!(request.wait_ms, expected);
            request.validate().unwrap();
        }
        assert!(
            serde_json::from_str::<EventsSubscribeRequest>("{\"wait_ms\":18446744073709551616}")
                .is_err()
        );
    }

    #[test]
    fn typed_requests_keep_the_valid_wire_shape() {
        let read = EventsReadRequest {
            after: 7,
            limit: 2,
            filter: EventsFilter {
                agents: vec!["a01".to_owned()],
                ..Default::default()
            },
        };
        let wire = serde_json::to_value(&read).unwrap();
        assert_eq!(
            wire,
            json!({"after":7,"limit":2,"filter":{"agents":["a01"],"kinds":[],"sources":[]}})
        );
        assert_eq!(
            serde_json::from_value::<EventsReadRequest>(wire).unwrap(),
            read
        );
        let subscribe = EventsSubscribeRequest {
            after: read.after,
            limit: read.limit,
            filter: read.filter,
            wait_ms: 1000,
        };
        let wire = serde_json::to_value(&subscribe).unwrap();
        assert_eq!(wire["wait_ms"], 1000);
        assert_eq!(
            serde_json::from_value::<EventsSubscribeRequest>(wire).unwrap(),
            subscribe
        );
        assert!(
            EventsReadRequest {
                after: -1,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            EventsSubscribeRequest {
                limit: 0,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn response_fields_are_required_instead_of_defaulted() {
        let read = json!({"events":[],"next":0,"high_water":0,"truncated":false});
        assert!(serde_json::from_value::<EventsReadResponse>(read.clone()).is_ok());
        for field in ["events", "next", "high_water", "truncated"] {
            let mut missing = read.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<EventsReadResponse>(missing).is_err());
        }
        let subscribe =
            json!({"events":[],"next":0,"high_water":0,"truncated":false,"lagged":false});
        assert!(serde_json::from_value::<EventsSubscribeResponse>(subscribe.clone()).is_ok());
        for field in ["events", "next", "high_water", "truncated", "lagged"] {
            let mut missing = subscribe.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<EventsSubscribeResponse>(missing).is_err());
        }
    }

    fn response_validation(
        after: i64,
        next: i64,
        high_water: i64,
        sequences: &[i64],
    ) -> [Result<()>; 2] {
        let events: Vec<_> = sequences
            .iter()
            .map(|&seq| Event {
                seq,
                ts: 0,
                kind: "fixture".to_owned(),
                agent: None,
                subject: None,
                generation: None,
                source: "fixture".to_owned(),
                native_id: None,
                data: json!({"private_fixture":"must not enter an error"}),
                cause: None,
            })
            .collect();
        [
            EventsReadResponse {
                events: events.clone(),
                next,
                high_water,
                truncated: next < high_water,
            }
            .validate(after),
            EventsSubscribeResponse {
                events,
                next,
                high_water,
                truncated: next < high_water,
                lagged: true,
            }
            .validate(after),
        ]
    }

    #[test]
    fn successful_response_bounds_allow_empty_and_filtered_pages() {
        for (after, next, high_water, sequences) in [
            (0, 0, 0, vec![]),
            (5, 5, 5, vec![]),
            (5, 9, 12, vec![]),
            (0, 5, 5, vec![1, 4, 5]),
            (2, 9, 12, vec![3, 7]),
            (i64::MAX - 1, i64::MAX, i64::MAX, vec![i64::MAX]),
        ] {
            for result in response_validation(after, next, high_water, &sequences) {
                result.unwrap();
            }
        }
    }

    #[test]
    fn malformed_response_cursors_are_unknown_without_clamping() {
        for (after, next, high_water) in [
            (-1, 0, 0),
            (0, -1, 0),
            (0, 0, -1),
            (5, 4, 9),
            (5, 6, 4),
            (0, 6, 5),
        ] {
            for result in response_validation(after, next, high_water, &[]) {
                let error = result.unwrap_err();
                assert_eq!(error.code, "unknown");
                assert_eq!(error.message, "event response is invalid");
                assert!(error.diagnostics.is_empty());
            }
        }
    }

    #[test]
    fn malformed_response_event_order_never_exposes_payloads() {
        for sequences in [
            vec![-1],
            vec![2],
            vec![10],
            vec![3, 3],
            vec![7, 3],
            vec![3, 10],
        ] {
            for result in response_validation(2, 9, 12, &sequences) {
                let error = result.unwrap_err();
                assert_eq!(error.code, "unknown");
                assert_eq!(error.message, "event response is invalid");
                assert!(error.diagnostics.is_empty());
            }
        }
    }
}
