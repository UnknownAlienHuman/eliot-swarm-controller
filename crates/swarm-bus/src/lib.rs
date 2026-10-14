//! Pure metadata routing and service-scope contracts for the Swarm bus.
//!
//! This crate deliberately does not own durable events, consumer cursors,
//! acknowledgements, Operations, authority, IPC, or a worker runtime. A host
//! supplies already-authorized subscriptions and already-normalized event
//! metadata. Store remains the only authority for durable reads and atomic
//! action/cursor admission.

use serde::Serialize;
use std::{error::Error, fmt};

pub mod managed_worker_config;
pub mod service_scope;

pub use managed_worker_config::{ManagedWorkerConfigExpectation, verify_managed_worker_config};

pub const MAX_SELECTOR_NAME_BYTES: usize = 256;
pub const MAX_CONSUMER_ID_BYTES: usize = 128;
pub const MAX_SUBSCRIPTIONS_PER_EVENT: usize = 256;

/// Normalized public outcome metadata. Raw event payloads are never accepted by
/// this router.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    Applied,
    Completed,
    Failed,
    Incomplete,
    Cancelled,
    Rejected,
    Sent,
    Answered,
    Invalidated,
    Unknown,
}

impl EventStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Incomplete => "incomplete",
            Self::Cancelled => "cancelled",
            Self::Rejected => "rejected",
            Self::Sent => "sent",
            Self::Answered => "answered",
            Self::Invalidated => "invalidated",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Result<Self, ValidationError> {
        match value {
            "applied" => Ok(Self::Applied),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "incomplete" => Ok(Self::Incomplete),
            "cancelled" => Ok(Self::Cancelled),
            "rejected" => Ok(Self::Rejected),
            "sent" => Ok(Self::Sent),
            "answered" => Ok(Self::Answered),
            "invalidated" => Ok(Self::Invalidated),
            "unknown" => Ok(Self::Unknown),
            _ => Err(ValidationError::UnsupportedStatus),
        }
    }
}

/// Store observation identity plus its safe selector projection. Construction
/// validates bounds; payload, source event keys, and binding credentials are
/// intentionally not representable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventMetadata {
    observation_id: i64,
    source_id: String,
    event_kind: String,
    status: Option<EventStatus>,
}

impl EventMetadata {
    pub fn new(
        observation_id: i64,
        source_id: &str,
        event_kind: &str,
        status: Option<EventStatus>,
    ) -> Result<Self, ValidationError> {
        if observation_id <= 0 {
            return Err(ValidationError::InvalidObservationId);
        }
        Ok(Self {
            observation_id,
            source_id: selector_name(source_id)?,
            event_kind: selector_name(event_kind)?,
            status,
        })
    }

    pub const fn observation_id(&self) -> i64 {
        self.observation_id
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn event_kind(&self) -> &str {
        &self.event_kind
    }

    pub const fn status(&self) -> Option<EventStatus> {
        self.status
    }
}

/// The two closed event actions currently represented by Store automation
/// selectors. These names route an existing configured action; they do not
/// execute it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    ReviewDispatch,
    ScriptRun,
}

impl Action {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReviewDispatch => "review_dispatch",
            Self::ScriptRun => "script_run",
        }
    }
}

/// An exact (source, kind, optional normalized status) selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EventSelector {
    source_id: String,
    event_kind: String,
    status: Option<EventStatus>,
}

impl EventSelector {
    pub fn new(
        source_id: &str,
        event_kind: &str,
        status: Option<EventStatus>,
    ) -> Result<Self, ValidationError> {
        Ok(Self {
            source_id: selector_name(source_id)?,
            event_kind: selector_name(event_kind)?,
            status,
        })
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn event_kind(&self) -> &str {
        &self.event_kind
    }

    pub const fn status(&self) -> Option<EventStatus> {
        self.status
    }

    pub fn matches(&self, event: &EventMetadata) -> bool {
        self.source_id == event.source_id
            && self.event_kind == event.event_kind
            && self
                .status
                .is_none_or(|expected| event.status == Some(expected))
    }
}

/// A Store-authorized recipient's route. Consumer identity is supplied by the
/// host after its authorization check; this type has no impersonation field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Subscription {
    consumer_id: String,
    selector: EventSelector,
    action: Action,
}

impl Subscription {
    pub fn new(
        consumer_id: &str,
        selector: EventSelector,
        action: Action,
    ) -> Result<Self, ValidationError> {
        let consumer_id = consumer_name(consumer_id)?;
        if action == Action::ReviewDispatch
            && (selector.source_id != "controller"
                || selector.event_kind != "task.submission"
                || selector.status != Some(EventStatus::Applied))
        {
            return Err(ValidationError::InvalidReviewDispatchSelector);
        }
        Ok(Self {
            consumer_id,
            selector,
            action,
        })
    }

    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    pub fn selector(&self) -> &EventSelector {
        &self.selector
    }

    pub const fn action(&self) -> Action {
        self.action
    }
}

/// One payload-free fanout result. It names an occurrence and an already
/// authorized consumer/action pair; it contains no cursor or acknowledgement.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct DispatchItem {
    consumer_id: String,
    observation_id: i64,
    action: Action,
}

impl DispatchItem {
    pub fn consumer_id(&self) -> &str {
        &self.consumer_id
    }

    pub const fn observation_id(&self) -> i64 {
        self.observation_id
    }

    pub const fn action(&self) -> Action {
        self.action
    }

    fn same_slot(&self, other: &Self) -> bool {
        self.consumer_id == other.consumer_id
            && self.observation_id == other.observation_id
            && self.action == other.action
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePlan {
    items: Vec<DispatchItem>,
}

impl RoutePlan {
    pub fn items(&self) -> &[DispatchItem] {
        &self.items
    }

    pub fn into_items(self) -> Vec<DispatchItem> {
        self.items
    }
}

/// Match one already-projected event against at most 256 host-authorized
/// subscriptions. Output order and duplicate-slot handling are deterministic.
pub fn route_event(
    event: &EventMetadata,
    subscriptions: &[Subscription],
) -> Result<RoutePlan, ValidationError> {
    if subscriptions.len() > MAX_SUBSCRIPTIONS_PER_EVENT {
        return Err(ValidationError::TooManySubscriptions);
    }

    let mut items = Vec::with_capacity(subscriptions.len());
    for subscription in subscriptions {
        if subscription.selector.matches(event) {
            let item = DispatchItem {
                consumer_id: subscription.consumer_id.clone(),
                observation_id: event.observation_id,
                action: subscription.action,
            };
            if !items
                .iter()
                .any(|queued: &DispatchItem| queued.same_slot(&item))
            {
                items.push(item);
            }
        }
    }
    items.sort();
    Ok(RoutePlan { items })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationError {
    InvalidObservationId,
    InvalidConsumerId,
    InvalidSelectorName,
    UnsupportedStatus,
    InvalidReviewDispatchSelector,
    TooManySubscriptions,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObservationId => f.write_str("observation ID must be positive"),
            Self::InvalidConsumerId => f.write_str("consumer ID is invalid or too long"),
            Self::InvalidSelectorName => f.write_str("event selector is invalid or too long"),
            Self::UnsupportedStatus => {
                f.write_str("event status is not a supported normalized value")
            }
            Self::InvalidReviewDispatchSelector => {
                f.write_str("ReviewDispatch requires the applied TaskSubmission selector")
            }
            Self::TooManySubscriptions => {
                f.write_str("event fanout exceeds the supported subscription bound")
            }
        }
    }
}

impl Error for ValidationError {}

fn selector_name(value: &str) -> Result<String, ValidationError> {
    if !valid_name(value, MAX_SELECTOR_NAME_BYTES) {
        return Err(ValidationError::InvalidSelectorName);
    }
    Ok(value.to_owned())
}

fn consumer_name(value: &str) -> Result<String, ValidationError> {
    if !valid_name(value, MAX_CONSUMER_ID_BYTES) {
        return Err(ValidationError::InvalidConsumerId);
    }
    Ok(value.to_owned())
}

fn valid_name(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-/@".contains(&byte))
}

pub use service_scope::{managed_service_state_path, managed_worker_config_path};
