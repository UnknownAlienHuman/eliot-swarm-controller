//! Bounded subscriptions over committed facts (R20, Documentation
//! Program §17.3): the subscription layer of the MCP facade.
//!
//! Surface: two facade-protocol methods, `eliot/subscribe` and
//! `eliot/unsubscribe`, routed by the facade's `on_custom_request`, and
//! two server notifications, `notifications/eliot/committed` and
//! `notifications/eliot/lagged`, sent with RMCP's `CustomNotification`.
//! RMCP 3.5.0's built-in `subscriptions/listen` machinery cannot
//! express this contract — its sink rejects custom notifications and
//! filters only the standard notification types — so the bounded,
//! lagged semantics live at this facade protocol layer instead, in the
//! same style as the Tasks projection (protocol methods on the
//! handler, never tools, never a second store of state).
//!
//! Categories derive ONLY from committed facts. All three are exact
//! filters over the one committed observation stream that
//! `report.delta` reads, so every category shares the stream's single
//! cursor space and a cursor from any notification resyncs through any
//! of the exact reads:
//!
//! - `reports` — every observation visible through `report.delta`:
//!   scoped committed report transitions. Normalized message bus facts
//!   do not duplicate the raw mailbox deliveries in this projection.
//! - `mailbox` — the `message.read` predicate applied to the same
//!   stream (kinds `message.send` / `task.feedback` /
//!   `check.completed` whose committed payload is addressed to this
//!   facade's own client): committed mailbox deliveries. An oversized
//!   mailbox payload is projected by the store as a gap reference
//!   without an inline payload, so its recipient is not visible here;
//!   it still surfaces under `reports` (and `operations`) as that gap
//!   reference, and the full bytes stay readable via the exact reads.
//! - `operations` — stream entries carrying an `operation_id`:
//!   Operation admissions and their recorded outcomes, i.e. committed
//!   Operation state transitions. A rejected admission commits one
//!   bounded failure fact with its Operation receipt. It is notified
//!   only to clients authorized to read that Operation; an exact
//!   request replay creates no additional fact or notification.
//!
//! Nothing is sourced from a volatile or native live stream, and a
//! subscription never creates facts: it forwards reads of the durable
//! stream only. The categories expose nothing the facade's credential
//! cannot already read through the `report_delta` / `message_read` /
//! `operation_get` tools.
//!
//! Bounds: each subscription owns a queue of at most
//! [`MAX_QUEUE_DEPTH`] undelivered notifications and the session holds
//! at most [`MAX_SUBSCRIPTIONS`] subscriptions. When the queue is full
//! the poller stops delivering, fast-forwards its cursor to the stream
//! head counting the matching entries it skips, and — as soon as a
//! queue slot frees, before any newer item — emits exactly one
//! `lagged` notification for the whole episode, carrying
//! `dropped_items` and the `(from_cursor, through_cursor]` range.
//! Nothing is ever lost silently and nothing is buffered without
//! bound. Authority is never the notification tail: resync is
//! `report.delta` / `message.read` / `operation.get` from
//! `from_cursor`, and the lagged notification names exactly that.
//!
//! Reconnect is not replay continuity. Subscriptions live and die with
//! the MCP session: a reconnected client finds no subscription under
//! its old ID and re-establishes state by naming its last honestly
//! reported cursor as `after` on a fresh `eliot/subscribe`; missed
//! facts arrive because they are committed and readable, not because
//! notifications were replayed. Unsubscribing, lagging or disconnecting
//! an observer has no effect on the host, on Operations, or on other
//! subscriptions — the pump only ever reads.
//!
//! Connection design (the S7 open point): the subscription pollers
//! share ONE dedicated IPC connection of their own, separate from the
//! sequential tool-call connection. Tool calls therefore never wait
//! behind a poll, a stalled poll never wedges a tool call, and a dead
//! pump link is dropped under exactly the tool path's discipline (the
//! same transport error codes drop the link; the next poll reconnects
//! first; a failed read is never silently retried) without touching
//! the tool link — and vice versa. The host accepts concurrent
//! connections and authenticates each one; its disconnect bookkeeping
//! is a no-op for non-module principals, so pump reconnects have no
//! store side effects. All pollers of the session serialize their
//! short page reads on that one pump connection.

use crate::config::Ipc;
use rmcp::{
    model::{CustomNotification, ServerNotification},
    service::{Peer, RoleServer},
};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex as StdMutex, Weak},
    time::Duration,
};
use swarm_client::Client;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};
use tokio::sync::{Mutex, mpsc, watch};
use uuid::Uuid;

/// Extension identifier advertised in the server capabilities
/// (SEP-1724 extensions map), alongside the Tasks extension.
pub const EXTENSION_ID: &str = "eliot/subscriptions";
pub const SUBSCRIBE_METHOD: &str = "eliot/subscribe";
pub const UNSUBSCRIBE_METHOD: &str = "eliot/unsubscribe";
pub const COMMITTED_NOTIFICATION: &str = "notifications/eliot/committed";
pub const LAGGED_NOTIFICATION: &str = "notifications/eliot/lagged";

/// Undelivered notifications buffered per subscription. A page read
/// returns at most 50 entries, so one page always fits an empty queue;
/// 64 leaves headroom for a lagged marker plus a page in flight while
/// keeping a dead consumer's facade-side footprint trivially small.
pub const MAX_QUEUE_DEPTH: usize = 64;
/// Subscriptions per MCP session. Each subscription costs two tasks
/// and one bounded queue and nothing else, but the session is one
/// manager client; 16 is far beyond any real fan-out.
pub const MAX_SUBSCRIPTIONS: usize = 16;
/// How often each poller reads the committed stream. Notifications
/// are a freshness hint over durable facts, not a live feed; a
/// quarter second keeps them prompt without busy-polling the host.
pub const POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Stream entries read per poll page (the store's page default).
const PAGE_LIMIT: i64 = 50;
/// Pages one tick may consume, delivering or fast-forwarding, before
/// yielding to the next tick — a busy stream cannot starve the
/// session, and the queue bound still applies within the tick.
const MAX_PAGES_PER_TICK: usize = 8;
/// The exact reads a subscriber resyncs through, named in the
/// subscribe acknowledgement and in every lagged notification.
const RESYNC_READS: [&str; 3] = ["report.delta", "message.read", "operation.get"];
/// The committed kinds `message.read` returns, mirrored from the
/// store's mailbox filter: the pump applies the same predicate to the
/// same stream the store filters server-side.
const MAILBOX_KINDS: [&str; 3] = ["message.send", "task.feedback", "check.completed"];

/// One subscription category: an exact filter over the committed
/// observation stream, never a source of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    /// Every committed observation (the `report.delta` source).
    Reports,
    /// Committed deliveries addressed to the facade's own client
    /// (the `message.read` predicate).
    Mailbox,
    /// Committed entries carrying an Operation ID: admissions and
    /// recorded outcomes.
    Operations,
}

impl Category {
    pub fn name(self) -> &'static str {
        match self {
            Category::Reports => "reports",
            Category::Mailbox => "mailbox",
            Category::Operations => "operations",
        }
    }

    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "reports" => Some(Category::Reports),
            "mailbox" => Some(Category::Mailbox),
            "operations" => Some(Category::Operations),
            _ => None,
        }
    }

    fn matches(self, item: &Value, client_id: &str) -> bool {
        match self {
            Category::Reports => true,
            Category::Operations => item["operation_id"].is_string(),
            Category::Mailbox => {
                MAILBOX_KINDS.contains(&item["kind"].as_str().unwrap_or_default())
                    && item["payload"]["recipient"].as_str() == Some(client_id)
            }
        }
    }
}

/// The subset of `categories` one committed stream entry belongs to.
/// Pure: the entry is a `report.delta` item (or its gap reference,
/// whose detached payload matches `reports`/`operations` by identity
/// but never `mailbox`, whose filter needs the inline payload).
pub fn matched_categories(categories: &[Category], item: &Value, client_id: &str) -> Vec<Category> {
    categories
        .iter()
        .copied()
        .filter(|category| category.matches(item, client_id))
        .collect()
}

/// One lagged episode: the matching entries in
/// `(from_cursor, through_cursor]` were committed but will not be
/// delivered as notifications. `from_cursor` is the last cursor the
/// subscriber was honestly told about, so the exact reads from it
/// recover everything the episode skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaggedGap {
    pub dropped_items: u64,
    pub from_cursor: i64,
    pub through_cursor: i64,
    /// Whether the fast-forward reached the stream head. Until it
    /// does, further arrivals may extend the same episode; the marker
    /// is only emitted once, when a queue slot frees, and entries past
    /// `through_cursor` are then delivered normally again.
    pub head_reached: bool,
}

/// The notification for one committed stream entry. `frame` is the
/// S2 projection frame of the exact page the entry was read from,
/// verbatim: its `range` and `projection_revision` let the subscriber
/// detect any gap itself instead of trusting this facade.
pub fn committed_notification(
    subscription_id: &str,
    matched: &[Category],
    item: &Value,
    frame: &Value,
) -> CustomNotification {
    CustomNotification::new(
        COMMITTED_NOTIFICATION,
        Some(json!({
            "subscription_id": subscription_id,
            "categories": matched.iter().map(|c| c.name()).collect::<Vec<_>>(),
            "cursor": item["cursor"],
            "item": item,
            "frame": frame,
        })),
    )
}

/// The single lagged marker for one overflow episode.
pub fn lagged_notification(subscription_id: &str, gap: &LaggedGap) -> CustomNotification {
    CustomNotification::new(
        LAGGED_NOTIFICATION,
        Some(json!({
            "subscription_id": subscription_id,
            "dropped_items": gap.dropped_items,
            "from_cursor": gap.from_cursor,
            "through_cursor": gap.through_cursor,
            "resync": {"after": gap.from_cursor, "reads": RESYNC_READS},
        })),
    )
}

/// The pollers' committed-fact source: `report.delta` pages over the
/// session's dedicated pump connection (see the module docs for the
/// connection design). Reads only; a failed page is reported to the
/// poller, which retries on its next tick from the same cursor.
pub struct PumpSource {
    root: PathBuf,
    credential: Credential,
    ipc_config: Arc<Ipc>,
    client: Arc<Mutex<Option<Client>>>,
    client_id: String,
}

impl PumpSource {
    pub fn new(
        root: PathBuf,
        credential: Credential,
        ipc_config: Arc<Ipc>,
        client: Arc<Mutex<Option<Client>>>,
    ) -> Self {
        let client_id = credential.client_id.clone();
        Self {
            root,
            credential,
            ipc_config,
            client,
            client_id,
        }
    }

    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    async fn delta_page(&self, after: i64) -> Result<Value> {
        super::request_on(
            &self.client,
            &self.root,
            &self.credential,
            &self.ipc_config,
            "report.delta",
            json!({"after": after, "limit": PAGE_LIMIT}),
        )
        .await
    }
}

/// Everything one live subscription costs: a stop flag and up to two
/// tasks (the forwarder exists only on the facade path; `open` alone
/// leaves delivery to the receiver's owner). Dropping or aborting
/// them changes nothing outside this session.
struct Entry {
    stop: watch::Sender<bool>,
    poller: tokio::task::JoinHandle<()>,
    forwarder: Option<tokio::task::JoinHandle<()>>,
}

/// The session's subscription registry. Lives inside the facade;
/// subscriptions are session state only — the facade stores no
/// subscription fact on the host and none survives the session.
pub struct SubscriptionHub {
    queue_depth: usize,
    poll_interval: Duration,
    entries: StdMutex<HashMap<String, Entry>>,
}

impl SubscriptionHub {
    pub fn new(queue_depth: usize, poll_interval: Duration) -> Self {
        Self {
            queue_depth,
            poll_interval,
            entries: StdMutex::new(HashMap::new()),
        }
    }

    /// Open one subscription: spawn its poller and return its ID, the
    /// receiving end of its bounded queue, and the acknowledgement
    /// the client resyncs from. Whoever owns the receiver owns
    /// delivery from the queue; the facade attaches a forwarder that
    /// drains it onto the MCP transport.
    pub fn open(
        self: &Arc<Self>,
        source: Arc<PumpSource>,
        categories: Vec<Category>,
        after: Option<i64>,
    ) -> Result<(String, mpsc::Receiver<CustomNotification>, Value)> {
        {
            let entries = self.entries.lock().expect("subscription registry");
            if entries.len() >= MAX_SUBSCRIPTIONS {
                return Err(Error::new(
                    "SUBSCRIPTION_LIMIT",
                    "this session already holds the maximum number of subscriptions",
                ));
            }
        }
        let id = Uuid::new_v4().to_string();
        let (queue_tx, queue_rx) = mpsc::channel::<CustomNotification>(self.queue_depth);
        let (stop_tx, stop_rx) = watch::channel(false);
        let poller = tokio::spawn(poll_loop(
            Arc::downgrade(self),
            id.clone(),
            source,
            categories.clone(),
            after,
            queue_tx,
            stop_rx,
            self.poll_interval,
        ));
        self.entries.lock().expect("subscription registry").insert(
            id.clone(),
            Entry {
                stop: stop_tx,
                poller,
                forwarder: None,
            },
        );
        let ack = json!({
            "subscription_id": id,
            "categories": categories.iter().map(|c| c.name()).collect::<Vec<_>>(),
            "cursor": after,
            "starts_at_head": after.is_none(),
            "queue_capacity": self.queue_depth,
            "poll_interval_ms": self.poll_interval.as_millis() as u64,
            "notifications": {
                "committed": COMMITTED_NOTIFICATION,
                "lagged": LAGGED_NOTIFICATION,
            },
            "resync": {
                "after": after,
                "reads": RESYNC_READS,
                "note": "notifications are a bounded freshness hint over committed facts, \
                         never complete history; a lagged notification marks an explicit \
                         gap, and the exact reads from the last delivered cursor are the \
                         authority. Subscriptions die with the session: after a reconnect \
                         the old subscription_id is unknown, and state is re-established \
                         by naming the last cursor as `after` on a fresh subscribe — \
                         never by replay continuity.",
            },
        });
        Ok((id, queue_rx, ack))
    }

    /// Open one subscription and forward its queue onto the MCP
    /// transport through `peer`. The forwarder drains the bounded
    /// queue; `send_notification` completes only when RMCP's sink
    /// accepted the message, so a stalled client back-pressures the
    /// forwarder — and only the forwarder. A dead transport ends the
    /// subscription: stop the poller and forget the entry. Nothing
    /// here touches the host or any Operation.
    pub fn subscribe(
        self: &Arc<Self>,
        source: Arc<PumpSource>,
        peer: Peer<RoleServer>,
        categories: Vec<Category>,
        after: Option<i64>,
    ) -> Result<Value> {
        let (id, queue_rx, ack) = self.open(source, categories, after)?;
        let stop = self
            .entries
            .lock()
            .expect("subscription registry")
            .get(&id)
            .map(|entry| entry.stop.clone());
        let Some(stop) = stop else {
            return Ok(ack); // unsubscribed again already; nothing to forward
        };
        let forwarder = {
            let hub: Weak<SubscriptionHub> = Arc::downgrade(self);
            let id = id.clone();
            tokio::spawn(async move {
                let mut queue_rx = queue_rx;
                while let Some(notification) = queue_rx.recv().await {
                    if peer
                        .send_notification(ServerNotification::CustomNotification(notification))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                let _ = stop.send(true);
                if let Some(hub) = hub.upgrade() {
                    hub.forget(&id);
                }
            })
        };
        if let Some(entry) = self
            .entries
            .lock()
            .expect("subscription registry")
            .get_mut(&id)
        {
            entry.forwarder = Some(forwarder);
        } else {
            // Unsubscribed again while the forwarder was being
            // attached: nobody owns it now, so end it here.
            forwarder.abort();
        }
        Ok(ack)
    }

    /// Stop one subscription: its tasks end and its queue is dropped.
    /// Unknown IDs are the store-style NOT_FOUND the facade's other
    /// protocol methods use — including every ID from a previous
    /// session, which is how a reconnect learns its subscriptions
    /// are gone.
    pub fn unsubscribe(&self, id: &str) -> Result<Value> {
        let entry = self
            .entries
            .lock()
            .expect("subscription registry")
            .remove(id)
            .ok_or_else(|| Error::new("NOT_FOUND", "unknown subscription"))?;
        let _ = entry.stop.send(true);
        entry.poller.abort();
        if let Some(forwarder) = entry.forwarder {
            forwarder.abort();
        }
        Ok(json!({"subscription_id": id, "unsubscribed": true}))
    }

    /// Remove an entry whose tasks already ended on their own
    /// (transport death). Never aborts: the caller may be one of them.
    fn forget(&self, id: &str) {
        self.entries
            .lock()
            .expect("subscription registry")
            .remove(id);
    }
}

/// Parse the `eliot/subscribe` params: a non-empty `categories` array
/// of known category names, and an optional non-negative `after`
/// cursor. Anything else is INVALID_PARAMS, in the facade's style.
pub fn parse_subscribe(params: &Value) -> Result<(Vec<Category>, Option<i64>)> {
    let raw = params
        .get("categories")
        .and_then(Value::as_array)
        .filter(|list| !list.is_empty())
        .ok_or_else(|| {
            Error::invalid("categories must be a non-empty array of subscription categories")
        })?;
    let mut categories: Vec<Category> = Vec::new();
    for entry in raw {
        let name = entry
            .as_str()
            .ok_or_else(|| Error::invalid("categories entries must be category name strings"))?;
        let category = Category::parse(name)
            .ok_or_else(|| Error::invalid(format!("unknown subscription category {name:?}")))?;
        if !categories.contains(&category) {
            categories.push(category);
        }
    }
    let after = match params.get("after") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_i64()
                .filter(|cursor| *cursor >= 0)
                .ok_or_else(|| Error::invalid("after must be a non-negative integer cursor"))?,
        ),
    };
    Ok((categories, after))
}

/// Parse the `eliot/unsubscribe` params: the subscription ID.
pub fn parse_unsubscribe(params: &Value) -> Result<String> {
    params
        .get("subscription_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::invalid("subscription_id must be a non-empty string"))
}

/// One subscription's delivery loop. State is exactly three facts:
/// the examination `cursor` (None until a cursor-less subscription
/// reaches the current head), `delivered_through` (the last cursor
/// whose matching entries are all queued — the resync point every
/// lagged marker names), and the pending `lagged` episode. The cursor
/// never advances past an entry that was neither delivered nor
/// counted as dropped, and never moves on a failed read.
#[allow(clippy::too_many_arguments)]
async fn poll_loop(
    hub: Weak<SubscriptionHub>,
    id: String,
    source: Arc<PumpSource>,
    categories: Vec<Category>,
    after: Option<i64>,
    queue: mpsc::Sender<CustomNotification>,
    mut stop: watch::Receiver<bool>,
    poll_interval: Duration,
) {
    let mut cursor: Option<i64> = after;
    let mut establish_from: i64 = 0;
    let mut delivered_through: i64 = after.unwrap_or(0);
    let mut lagged: Option<LaggedGap> = None;
    loop {
        if *stop.borrow() {
            break;
        }
        // A pending lagged marker precedes any newer item. Until it
        // is queued, no post-episode item is delivered ahead of it.
        if lagged.is_some() {
            match queue.try_send(lagged_notification(&id, lagged.as_ref().expect("checked"))) {
                Ok(()) => lagged = None,
                Err(mpsc::error::TrySendError::Full(_)) => {}
                Err(mpsc::error::TrySendError::Closed(_)) => break,
            }
        }
        if lagged.as_ref().is_some_and(|gap| !gap.head_reached) {
            // The marker is still queued behind a full queue and the
            // episode is still short of the head: keep fast-forwarding
            // (counting drops) instead of delivering. The range in
            // the eventual marker is exactly what was skipped; entries
            // past it are delivered normally once polling resumes.
            let mut gap = lagged.take().expect("episode checked above");
            let mut dropped = 0_u64;
            let scanned = scan_to_head(
                &source,
                cursor.unwrap_or(0),
                &categories,
                Some(&mut dropped),
            )
            .await;
            gap.dropped_items += dropped;
            if let Some((head, reached)) = scanned {
                cursor = Some(head);
                gap.through_cursor = head;
                gap.head_reached = reached;
            }
            lagged = Some(gap);
        } else if lagged.is_some() {
            // Episode closed, marker still waiting for a queue slot:
            // deliver nothing new this tick.
        } else if cursor.is_none() {
            // Cursor-less subscription: fast-forward to the current
            // head, delivering and counting nothing — the
            // pre-subscription past is not a gap. Delivery starts on
            // a later tick, from the head.
            match scan_to_head(&source, establish_from, &categories, None).await {
                Some((head, true)) => {
                    cursor = Some(head);
                    delivered_through = head;
                }
                Some((advanced, false)) => establish_from = advanced,
                None => {}
            }
        } else {
            match deliver_tick(
                &source,
                &categories,
                cursor.expect("cursor established"),
                &mut delivered_through,
                &mut lagged,
                &queue,
                &id,
            )
            .await
            {
                Some(next) => cursor = Some(next),
                None => break, // queue closed: the forwarder is gone
            }
        }
        tokio::select! {
            _ = stop.changed() => break,
            _ = tokio::time::sleep(poll_interval) => {}
        }
    }
    if let Some(hub) = hub.upgrade() {
        hub.forget(&id);
    }
}

/// Poll pages from `cursor`, queueing one committed notification per
/// matching entry. Returns the advanced cursor, or None when the
/// queue closed (the forwarder is gone: stop the subscription).
/// On queue-full the current entry starts a lagged episode and the
/// rest of the page is consumed in count-only mode; the episode's
/// fast-forward continues on later ticks (see `poll_loop`).
#[allow(clippy::too_many_arguments)]
async fn deliver_tick(
    source: &PumpSource,
    categories: &[Category],
    mut cursor: i64,
    delivered_through: &mut i64,
    lagged: &mut Option<LaggedGap>,
    queue: &mpsc::Sender<CustomNotification>,
    id: &str,
) -> Option<i64> {
    for _ in 0..MAX_PAGES_PER_TICK {
        let page = match source.delta_page(cursor).await {
            Ok(page) => page,
            // A failed read is not a closed delivery queue. Keep the
            // cursor at the last fully examined entry and retry next tick.
            Err(_) => return Some(cursor),
        };
        let frame = page["projection"].clone();
        let has_newer = frame["has_newer"] == json!(true);
        let items = page["items"].as_array().cloned().unwrap_or_default();
        let mut episode: Option<LaggedGap> = None;
        for item in &items {
            let item_cursor = item["cursor"].as_i64().unwrap_or(cursor);
            cursor = item_cursor;
            if let Some(gap) = &mut episode {
                // Count-only mode for the rest of this page.
                if !matched_categories(categories, item, source.client_id()).is_empty() {
                    gap.dropped_items += 1;
                    gap.through_cursor = item_cursor;
                }
                continue;
            }
            let matched = matched_categories(categories, item, source.client_id());
            if matched.is_empty() {
                continue;
            }
            match queue.try_send(committed_notification(id, &matched, item, &frame)) {
                Ok(()) => *delivered_through = item_cursor,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    episode = Some(LaggedGap {
                        dropped_items: 1,
                        from_cursor: *delivered_through,
                        through_cursor: item_cursor,
                        head_reached: false,
                    });
                }
                Err(mpsc::error::TrySendError::Closed(_)) => return None,
            }
        }
        if let Some(mut gap) = episode {
            gap.head_reached = !has_newer;
            *lagged = Some(gap);
            return Some(cursor);
        }
        if !has_newer || items.is_empty() {
            return Some(cursor);
        }
    }
    Some(cursor)
}

/// Page from `from` toward the current stream head without
/// delivering. With `dropped` set, matching entries are counted into
/// it (a lagged episode's fast-forward); without it nothing is
/// counted (head establishment for a cursor-less subscription).
/// Returns `(cursor, reached_head)`, or None when a page read failed —
/// the caller retries next tick from its saved position, so a failed
/// read never moves any cursor.
async fn scan_to_head(
    source: &PumpSource,
    from: i64,
    categories: &[Category],
    mut dropped: Option<&mut u64>,
) -> Option<(i64, bool)> {
    let mut cursor = from;
    for _ in 0..MAX_PAGES_PER_TICK {
        let page = source.delta_page(cursor).await.ok()?;
        let has_newer = page["projection"]["has_newer"] == json!(true);
        let items = page["items"].as_array().cloned().unwrap_or_default();
        for item in &items {
            cursor = item["cursor"].as_i64().unwrap_or(cursor);
            if let Some(counter) = dropped.as_deref_mut()
                && !matched_categories(categories, item, source.client_id()).is_empty()
            {
                *counter += 1;
            }
        }
        if !has_newer || items.is_empty() {
            return Some((cursor, true));
        }
    }
    Some((cursor, false))
}
