//! Pure metadata routing and bounded volatile scheduling for the Swarm bus.
//!
//! This crate deliberately does not own durable events, consumer cursors,
//! acknowledgements, Operations, authority, IPC, or a worker runtime. A host
//! supplies already-authorized subscriptions and already-normalized event
//! metadata. Store remains the only authority for durable reads and atomic
//! action/cursor admission.

use serde::Serialize;
use std::{collections::VecDeque, error::Error, fmt};

pub mod managed_worker_config;
pub mod service_scope;

pub use managed_worker_config::{ManagedWorkerConfigExpectation, verify_managed_worker_config};

pub const MAX_SELECTOR_NAME_BYTES: usize = 256;
pub const MAX_CONSUMER_ID_BYTES: usize = 128;
pub const MAX_SUBSCRIPTIONS_PER_EVENT: usize = 256;
pub const MAX_QUEUE_RECORDS: usize = 8_192;
pub const MAX_QUEUE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_QUEUE_ITEM_BYTES: usize = 1_024;

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

    fn accounted_bytes(&self) -> usize {
        // Logical retained size: fixed metadata plus the one bounded string.
        // The queue separately bounds record count and total accounted bytes.
        64usize.saturating_add(self.consumer_id.len())
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
pub struct QueueLimits {
    max_records: usize,
    max_bytes: usize,
    max_item_bytes: usize,
}

impl QueueLimits {
    pub const DEFAULT: Self = Self {
        max_records: 256,
        max_bytes: 256 * 1024,
        max_item_bytes: MAX_QUEUE_ITEM_BYTES,
    };

    pub fn new(
        max_records: usize,
        max_bytes: usize,
        max_item_bytes: usize,
    ) -> Result<Self, QueueError> {
        if max_records == 0
            || max_records > MAX_QUEUE_RECORDS
            || max_bytes == 0
            || max_bytes > MAX_QUEUE_BYTES
            || max_item_bytes == 0
            || max_item_bytes > MAX_QUEUE_ITEM_BYTES
            || max_item_bytes > max_bytes
        {
            return Err(QueueError::InvalidLimits);
        }
        Ok(Self {
            max_records,
            max_bytes,
            max_item_bytes,
        })
    }

    pub const fn max_records(self) -> usize {
        self.max_records
    }

    pub const fn max_bytes(self) -> usize {
        self.max_bytes
    }

    pub const fn max_item_bytes(self) -> usize {
        self.max_item_bytes
    }
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueError {
    InvalidLimits,
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => f.write_str("bus queue limits are outside supported bounds"),
        }
    }
}

impl Error for QueueError {}

/// Nonblocking admission outcome. Full means the hint was not queued; the
/// caller must recover from the Store cursor/journal rather than treating it as
/// delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueOutcome {
    Queued,
    AlreadyQueued,
    Full,
    ItemTooLarge,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueStats {
    pub queued_records: usize,
    pub accounted_bytes: usize,
    pub rejected_records: u64,
    pub rejected_bytes: u64,
}

pub struct WorkQueue {
    limits: QueueLimits,
    items: VecDeque<DispatchItem>,
    accounted_bytes: usize,
    rejected_records: u64,
    rejected_bytes: u64,
}

impl WorkQueue {
    pub fn new(limits: QueueLimits) -> Self {
        Self {
            limits,
            items: VecDeque::with_capacity(limits.max_records),
            accounted_bytes: 0,
            rejected_records: 0,
            rejected_bytes: 0,
        }
    }

    /// Bounded, nonblocking and metadata-only. Duplicate queued slots coalesce
    /// only while resident in this volatile queue; durable retries still need
    /// Store-side semantic deduplication.
    pub fn try_enqueue(&mut self, item: DispatchItem) -> EnqueueOutcome {
        let bytes = item.accounted_bytes();
        if bytes > self.limits.max_item_bytes {
            self.reject(bytes);
            return EnqueueOutcome::ItemTooLarge;
        }
        if self.items.iter().any(|queued| queued.same_slot(&item)) {
            return EnqueueOutcome::AlreadyQueued;
        }
        if self.items.len() >= self.limits.max_records
            || bytes > self.limits.max_bytes.saturating_sub(self.accounted_bytes)
        {
            self.reject(bytes);
            return EnqueueOutcome::Full;
        }
        self.accounted_bytes = self.accounted_bytes.saturating_add(bytes);
        self.items.push_back(item);
        EnqueueOutcome::Queued
    }

    pub fn try_pop(&mut self) -> Option<DispatchItem> {
        let item = self.items.pop_front()?;
        self.accounted_bytes = self.accounted_bytes.saturating_sub(item.accounted_bytes());
        Some(item)
    }

    pub fn stats(&self) -> QueueStats {
        QueueStats {
            queued_records: self.items.len(),
            accounted_bytes: self.accounted_bytes,
            rejected_records: self.rejected_records,
            rejected_bytes: self.rejected_bytes,
        }
    }

    fn reject(&mut self, bytes: usize) {
        self.rejected_records = self.rejected_records.saturating_add(1);
        self.rejected_bytes = self
            .rejected_bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DemandSnapshot {
    /// Consumers with due dispatch work. A saved but idle selector is not demand.
    pub due_consumer_work: u32,
    pub durable_pending_items: u32,
    pub live_followers: u32,
}

impl DemandSnapshot {
    pub const fn is_active(self) -> bool {
        self.due_consumer_work > 0 || self.durable_pending_items > 0 || self.live_followers > 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleAction {
    Start { generation: u64 },
    CancelStart { generation: u64 },
    Stop { generation: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleError {
    GenerationExhausted,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationExhausted => f.write_str("bus lifecycle generation exhausted"),
        }
    }
}

impl Error for LifecycleError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Dormant,
    Starting { generation: u64 },
    Running { generation: u64 },
    Stopping { generation: u64 },
}

/// Push-driven one-worker-per-host-scope lifecycle gate. The host calls
/// update_demand when retained demand changes; this crate starts no task,
/// timer, process, or polling loop. Failed/unexpected exits do not auto-retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DemandGate {
    demand: DemandSnapshot,
    phase: Phase,
    last_generation: u64,
}

impl Default for DemandGate {
    fn default() -> Self {
        Self {
            demand: DemandSnapshot::default(),
            phase: Phase::Dormant,
            last_generation: 0,
        }
    }
}

impl DemandGate {
    pub const fn demand(&self) -> DemandSnapshot {
        self.demand
    }

    pub fn update_demand(
        &mut self,
        demand: DemandSnapshot,
    ) -> Result<Option<LifecycleAction>, LifecycleError> {
        if demand == self.demand {
            return Ok(None);
        }
        self.demand = demand;
        if demand.is_active() {
            return match self.phase {
                Phase::Dormant => self.begin_start().map(Some),
                Phase::Starting { .. } | Phase::Running { .. } | Phase::Stopping { .. } => Ok(None),
            };
        }
        match self.phase {
            Phase::Starting { generation } => {
                self.phase = Phase::Stopping { generation };
                Ok(Some(LifecycleAction::CancelStart { generation }))
            }
            Phase::Running { generation } => {
                self.phase = Phase::Stopping { generation };
                Ok(Some(LifecycleAction::Stop { generation }))
            }
            Phase::Dormant | Phase::Stopping { .. } => Ok(None),
        }
    }

    /// Resolve the single in-flight start callback. A late successful start
    /// after cancellation receives a stop request and remains single-flight
    /// until worker_stopped confirms release.
    pub fn start_finished(
        &mut self,
        generation: u64,
        started: bool,
    ) -> Result<Option<LifecycleAction>, LifecycleError> {
        match self.phase {
            Phase::Starting {
                generation: current,
            } if current == generation => {
                if started {
                    self.phase = Phase::Running { generation };
                } else {
                    self.phase = Phase::Dormant;
                }
                Ok(None)
            }
            Phase::Stopping {
                generation: current,
            } if current == generation => {
                if started {
                    Ok(Some(LifecycleAction::Stop { generation }))
                } else {
                    self.phase = Phase::Dormant;
                    if self.demand.is_active() {
                        self.begin_start().map(Some)
                    } else {
                        Ok(None)
                    }
                }
            }
            _ => Ok(None),
        }
    }

    /// Confirm worker release. A new start is issued only if fresh retained
    /// demand still exists; a crashed worker with unchanged demand is not
    /// restarted in a tight loop.
    pub fn worker_stopped(
        &mut self,
        generation: u64,
    ) -> Result<Option<LifecycleAction>, LifecycleError> {
        match self.phase {
            Phase::Stopping {
                generation: current,
            } if current == generation => {
                self.phase = Phase::Dormant;
                if self.demand.is_active() {
                    self.begin_start().map(Some)
                } else {
                    Ok(None)
                }
            }
            Phase::Running {
                generation: current,
            }
            | Phase::Starting {
                generation: current,
            } if current == generation => {
                self.phase = Phase::Dormant;
                Ok(None)
            }
            _ => Ok(None),
        }
    }

    /// Explicit retry hook after a failed or unexpectedly stopped worker.
    pub fn retry(&mut self) -> Result<Option<LifecycleAction>, LifecycleError> {
        if self.phase == Phase::Dormant && self.demand.is_active() {
            self.begin_start().map(Some)
        } else {
            Ok(None)
        }
    }

    fn begin_start(&mut self) -> Result<LifecycleAction, LifecycleError> {
        let generation = self
            .last_generation
            .checked_add(1)
            .ok_or(LifecycleError::GenerationExhausted)?;
        self.last_generation = generation;
        self.phase = Phase::Starting { generation };
        Ok(LifecycleAction::Start { generation })
    }
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
