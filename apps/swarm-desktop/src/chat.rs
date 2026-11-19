//! Retained conversation as pure logic.
//!
//! Built only from existing reads: `agent.result` turns (authoritative state
//! and final text), retained journal events for the Agent (`turn.started`
//! links a turn to its input command, `message.completed` carries progress
//! messages, `tool.*` carries tool activity) and `command.get` for the input
//! text a command was admitted with. `agent.history` is the observed-subject
//! surface and is not used for Eliot-managed Agents.
//!
//! Identity is stable: one virtualized item per turn (`turn:<id>`), keyed
//! parts inside it (`in:<command>`, message native id, `tools:<first item>`,
//! `final:<turn>`), and a content revision per part. Reactor resets a whole
//! virtual collection when its key list changes, so activity inside a turn
//! never changes the item keys; only a new or evicted turn does.
//!
//! Coverage is explicit and never inferred: how many turns the result read
//! returned against its limit, whether the Agent's creation event is still
//! retained (its events are read from there), the retention floor, evictions
//! and inputs whose text could not be read.

use std::collections::{HashMap, HashSet, VecDeque};

use eliot_core::{Event, Turn, TurnOutcome, TurnState};

/// Turns added per "Load older" step.
pub const TURN_PAGE: usize = 20;
/// `agent.result` upper bound.
pub const MAX_RESULT_LIMIT: usize = 1_000;
/// Resident turns; older ones are evicted and reported.
pub const MAX_RESIDENT_TURNS: usize = 200;
/// Resident text bytes across all turns.
pub const MAX_RESIDENT_BYTES: usize = 8 * 1024 * 1024;
/// Retained bytes per message (Copy copies at most this much).
pub const MAX_TEXT_BYTES: usize = 256 * 1024;
/// Characters shown for plain (non-Markdown) bodies.
pub const MAX_RENDER_CHARS: usize = 12_000;
/// Tool rows kept per collapsed group; counters stay exact.
pub const MAX_TOOL_ROWS: usize = 40;
/// Tool identities indexed per turn; older ones leave the index (their rows
/// stay, bounded, with exact counters) instead of growing it forever.
pub const MAX_TOOL_IDS_PER_TURN: usize = 256;
/// Evicted turn ids remembered so late events never resurrect them; the
/// exact eviction count lives in coverage and survives snapshot rebuilds.
const MAX_EVICTED_IDS: usize = 1_024;
/// Progress messages kept per turn.
pub const MAX_MESSAGES_PER_TURN: usize = 200;
/// Progress text bytes kept per turn; later messages are counted, not kept.
pub const MAX_TURN_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

/// Event kinds the conversation and its readback invalidation consume.
pub const CHAT_KINDS: &[&str] = &[
    "agent.created",
    "agent.failed",
    "agent.lost",
    "agent.stopped",
    "session.ready",
    "session.ended",
    "turn.started",
    "turn.completed",
    "turn.unknown",
    "turn.resolved",
    "message.completed",
    "tool.started",
    "tool.completed",
    "request.opened",
    "request.closed",
    "request.resolved",
    // A steer's acceptance names the turn it steers: its text joins that
    // turn (the design's "Steer:" bubble).
    "command.accepted",
];

/// FNV-1a over bytes: content revision and Markdown cache key.
pub fn text_hash(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

fn mix(hash: u64, value: u64) -> u64 {
    (hash ^ value).wrapping_mul(0x0100_0000_01b3)
}

/// Bound retained text at a char boundary with an explicit marker.
pub fn bound_text(text: &str) -> String {
    if text.len() <= MAX_TEXT_BYTES {
        return text.to_owned();
    }
    let mut end = MAX_TEXT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!(
        "{}\n… [retained text truncated at {} KiB in the UI; `swarm result` reads it in full]",
        &text[..end],
        MAX_TEXT_BYTES / 1024
    )
}

/// Plain-text preview: the full body when short, a bounded prefix otherwise.
pub fn preview(text: &str) -> String {
    if text.chars().count() <= MAX_RENDER_CHARS {
        return text.to_owned();
    }
    let prefix: String = text.chars().take(MAX_RENDER_CHARS).collect();
    format!("{prefix}\n… [display truncated; Copy message copies the retained text]")
}

/// Turn status as read back, never guessed from event order alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnStatus {
    Running,
    Completed(Option<TurnOutcome>),
    Unknown,
}

#[derive(Clone, Debug)]
struct ToolRow {
    item_id: String,
    tool: String,
    status: Option<String>,
    done: bool,
    started_ms: Option<i64>,
    completed_ms: Option<i64>,
    paths: Vec<String>,
    paths_truncated: bool,
}

#[derive(Clone, Debug)]
enum Activity {
    Message {
        key: String,
        text: String,
    },
    /// An input steered into the running turn (its command id): shown in
    /// the turn where it was accepted.
    Steer {
        command: String,
    },
    Tools {
        key: String,
        rows: Vec<ToolRow>,
        total: usize,
        done: usize,
        failed: usize,
    },
}

#[derive(Clone, Debug)]
struct TurnEntry {
    turn_id: String,
    started_ms: Option<i64>,
    first_seq: Option<i64>,
    status: TurnStatus,
    command_id: Option<String>,
    final_text: Option<String>,
    event_final: Option<String>,
    /// Committed vendor error or terminal status from turn readback.
    failure_reason: Option<String>,
    /// Effective model the failed turn ran under, from its retained
    /// configuration; named in the failure so a denied default says so.
    failure_model: Option<String>,
    read_back: bool,
    activity: Vec<Activity>,
    messages: usize,
    message_bytes: usize,
    messages_dropped: usize,
    /// Tool item id to the index of the activity group that started it.
    item_groups: HashMap<String, usize>,
    /// Insertion order of the indexed tool ids, for bounded eviction.
    item_order: VecDeque<String>,
}

impl TurnEntry {
    fn new(turn_id: &str) -> Self {
        Self {
            turn_id: turn_id.to_owned(),
            started_ms: None,
            first_seq: None,
            status: TurnStatus::Running,
            command_id: None,
            final_text: None,
            event_final: None,
            failure_reason: None,
            failure_model: None,
            read_back: false,
            activity: Vec::new(),
            messages: 0,
            message_bytes: 0,
            messages_dropped: 0,
            item_groups: HashMap::new(),
            item_order: VecDeque::new(),
        }
    }

    fn order(&self) -> (i64, i64) {
        (
            self.started_ms.unwrap_or(i64::MAX),
            self.first_seq.unwrap_or(i64::MAX),
        )
    }

    fn bytes(&self) -> usize {
        let activity: usize = self
            .activity
            .iter()
            .map(|entry| match entry {
                Activity::Message { text, .. } => text.len(),
                Activity::Steer { command } => command.len(),
                Activity::Tools { rows, .. } => rows
                    .iter()
                    .map(|row| {
                        row.item_id.len()
                            + row.tool.len()
                            + row.status.as_ref().map_or(0, String::len)
                            + row.paths.iter().map(String::len).sum::<usize>()
                            + 32
                    })
                    .sum(),
            })
            .sum();
        activity
            + self.final_text.as_ref().map_or(0, String::len)
            + self.event_final.as_ref().map_or(0, String::len)
            + self.item_groups.keys().map(String::len).sum::<usize>()
            + self.item_order.iter().map(String::len).sum::<usize>()
    }

    fn tool_event(
        &mut self,
        item_id: &str,
        tool: &str,
        status: Option<String>,
        done: bool,
        event: &Event,
    ) {
        // Only semantic metadata already retained by the adapter; never raw vendor frames.
        let paths: Vec<String> = event
            .data
            .get("changed_paths")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(serde_json::Value::as_str)
            .take(20)
            .map(|path| path.chars().take(1_024).collect())
            .collect();
        let paths_truncated = event
            .data
            .get("changed_paths_truncated")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
            || event
                .data
                .get("changed_paths")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|paths| {
                    paths.len() > 20
                        || paths
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .any(|path| path.chars().count() > 1_024)
                });
        let failed_now = done && status.as_deref().is_some_and(|status| !is_success(status));
        if let Some(&group) = self.item_groups.get(item_id)
            && let Some(Activity::Tools {
                rows,
                done: done_count,
                failed,
                ..
            }) = self.activity.get_mut(group)
        {
            match rows.iter_mut().find(|row| row.item_id == item_id) {
                Some(row) => {
                    if done && !row.done {
                        row.done = true;
                        row.completed_ms = Some(event.ts);
                        *done_count += 1;
                        *failed += usize::from(failed_now);
                    }
                    if !paths.is_empty() {
                        row.paths = paths;
                    }
                    row.paths_truncated |= paths_truncated;
                    if status.is_some() || done {
                        // A completion without status must not keep "running".
                        row.status = status;
                    }
                }
                None if done => {
                    // The started row left the bounded list; counters stay exact
                    // in the group that started the item.
                    *done_count += 1;
                    *failed += usize::from(failed_now);
                }
                None => {}
            }
            return;
        }
        let row = ToolRow {
            item_id: item_id.to_owned(),
            tool: tool.to_owned(),
            status,
            done,
            started_ms: (!done).then_some(event.ts),
            completed_ms: done.then_some(event.ts),
            paths,
            paths_truncated,
        };
        if !matches!(self.activity.last(), Some(Activity::Tools { .. })) {
            self.activity.push(Activity::Tools {
                key: format!("tools:{item_id}"),
                rows: Vec::new(),
                total: 0,
                done: 0,
                failed: 0,
            });
        }
        let group = self.activity.len() - 1;
        if let Some(Activity::Tools {
            rows,
            total,
            done: done_count,
            failed,
            ..
        }) = self.activity.last_mut()
        {
            rows.push(row);
            if rows.len() > MAX_TOOL_ROWS {
                rows.remove(0);
            }
            *total += 1;
            *done_count += usize::from(done);
            *failed += usize::from(failed_now);
        }
        self.item_groups.insert(item_id.to_owned(), group);
        if !self.item_order.iter().any(|known| known == item_id) {
            self.item_order.push_back(item_id.to_owned());
        }
        // Only settled identities leave: an unfinished tool keeps the
        // identity its completion settles (settling the row and group
        // that started it), and a settled entry dedupes redeliveries.
        // Every retained byte still counts toward the resident budget,
        // so whole turns leave when tools pile up.
        while self.item_order.len() > MAX_TOOL_IDS_PER_TURN {
            if !self.evict_one_tool_id() {
                break;
            }
        }
    }

    /// Drop the oldest settled tool identity. An unfinished tool, or one
    /// whose row already left the bounded list, keeps its identity: only
    /// a row that shows done proves the completion was recorded.
    fn evict_one_tool_id(&mut self) -> bool {
        let settled = self
            .item_order
            .iter()
            .position(|item_id| self.tool_settled(item_id));
        match settled.and_then(|position| self.item_order.remove(position)) {
            Some(item_id) => {
                self.item_groups.remove(&item_id);
                true
            }
            None => false,
        }
    }

    /// Whether the tool's completion was recorded on its row.
    fn tool_settled(&self, item_id: &str) -> bool {
        match self
            .item_groups
            .get(item_id)
            .and_then(|group| self.activity.get(*group))
        {
            Some(Activity::Tools { rows, .. }) => rows
                .iter()
                .find(|row| row.item_id == item_id)
                .is_some_and(|row| row.done),
            _ => false,
        }
    }
}

fn is_success(status: &str) -> bool {
    matches!(status, "completed" | "success" | "succeeded" | "ok")
}

/// Input text for one command, read from its retained admission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputText {
    Loaded(String),
    Unavailable(String),
}

/// Explicit coverage of what the resident conversation contains.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coverage {
    /// Generation whose turns `agent.result` returned.
    pub generation: i64,
    /// `agent.result` limit used and rows it returned.
    pub result_limit: usize,
    pub result_returned: usize,
    /// Committed cursor: events are applied through this sequence.
    pub cursor: i64,
    /// Retention floor from `events.bounds`.
    pub pruned_through: i64,
    /// The Agent's `agent.created` event was read (it is still retained).
    pub reached_creation: bool,
    /// Turns dropped from memory by the resident bounds.
    pub evicted_turns: usize,
    /// Set when live delivery overflowed or failed; cleared by a reread.
    pub stale: Option<String>,
}

impl Coverage {
    /// True when the result read returned every retained turn of the generation.
    pub fn turns_complete(&self) -> bool {
        self.result_returned < self.result_limit
    }

    /// The Agent's events are read from the retention floor; without its
    /// creation event, older activity was pruned and cannot be read.
    pub fn activity_pruned(&self) -> bool {
        !self.reached_creation && self.pruned_through > 0
    }

    /// "Load older" pages turns only: the events already start at creation.
    pub fn can_load_older(&self) -> bool {
        !self.turns_complete() && self.result_limit < MAX_RESULT_LIMIT
    }

    /// Human-readable coverage lines; every limit is named, none is hidden.
    /// Only what is unusual about the loaded conversation, in words: older
    /// turns not loaded, retention pruning, unreadable inputs, evicted
    /// turns, a reread in progress. Empty when everything is here.
    pub fn notes(&self, inputs_missing: usize) -> Vec<String> {
        let mut notes = Vec::new();
        if !self.turns_complete() {
            notes.push("Older turns exist; Load older messages shows them.".to_owned());
        }
        if self.generation > 1 {
            notes.push("Earlier runs are not shown.".to_owned());
        }
        if self.activity_pruned() {
            notes.push("Older activity was removed by retention.".to_owned());
        }
        match inputs_missing {
            0 => {}
            1 => notes.push("1 message could not be read.".to_owned()),
            count => notes.push(format!("{count} messages could not be read.")),
        }
        match self.evicted_turns {
            0 => {}
            1 => notes.push("1 older turn left memory to keep the window light.".to_owned()),
            count => notes.push(format!(
                "{count} older turns left memory to keep the window light."
            )),
        }
        if self.stale.is_some() {
            notes.push("Catching up with new activity.".to_owned());
        }
        notes
    }
}

/// What a batch of events invalidated; the caller coalesces one reread.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Invalidation {
    /// A turn ended or changed outcome: reread results.
    pub turns: bool,
    /// Requests or lifecycle changed: reread `agent.get`.
    pub detail: bool,
    /// Commands linked to turns whose input text is not loaded.
    pub inputs: Vec<String>,
    /// A newer generation started: the whole snapshot must be reread.
    pub generation_changed: bool,
}

impl Invalidation {
    pub fn any(&self) -> bool {
        self.turns || self.detail || !self.inputs.is_empty() || self.generation_changed
    }

    pub fn merge(&mut self, other: Invalidation) {
        self.turns |= other.turns;
        self.detail |= other.detail;
        self.generation_changed |= other.generation_changed;
        for input in other.inputs {
            if !self.inputs.contains(&input) {
                self.inputs.push(input);
            }
        }
    }
}

/// Kind of one keyed part inside a turn item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartKind {
    Input,
    /// An input steered into the running turn.
    Steer,
    Progress,
    Tools,
    Final,
    Working,
    Notice,
    /// A submitted input that definitely did not go through: the `swarm
    /// why` summary, its origin, and Retry.
    Failed,
    /// A native turn that started and then failed.
    TurnFailed,
    /// A submitted input whose outcome Swarm cannot prove: the summary and
    /// origin, never Retry (an uncertain effect is not replayed).
    Unconfirmed,
}

/// Why a local input did not start: the `swarm why` summary line and its
/// origin, and whether the failure is definite (Retry) or uncertain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InputFailure {
    pub code: String,
    pub message: String,
    pub origin: String,
    pub definite: bool,
}

/// One keyed part of a turn item.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Part {
    pub key: String,
    pub revision: u64,
    pub kind: PartKind,
    pub label: String,
    /// Retained (bounded) text; Markdown for progress/final parts.
    pub text: String,
    /// Bounded detail lines, e.g. tool rows of a collapsed group.
    pub detail: Vec<String>,
}

impl Part {
    fn new(key: String, kind: PartKind, label: String, text: String, detail: Vec<String>) -> Self {
        let mut revision = mix(text_hash(&label), text_hash(&text));
        revision = mix(revision, kind as u64);
        for line in &detail {
            revision = mix(revision, text_hash(line));
        }
        Self {
            key,
            revision,
            kind,
            label,
            text,
            detail,
        }
    }
}

/// One virtualized item: a turn, or a submitted input not yet linked to one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnItem {
    pub key: String,
    pub revision: u64,
    pub parts: Vec<Part>,
}

/// Retained conversation for one Agent.
#[derive(Clone, Debug, Default)]
pub struct Conversation {
    agent: String,
    turns: HashMap<String, TurnEntry>,
    inputs: HashMap<String, InputText>,
    /// Exact pending text of unlinked local inputs, for Retry: the bounded
    /// `inputs` copy is the display preview only (#643). One entry per
    /// still-waiting local input (each at most the 512 KiB submission
    /// limit), dropped when the input links, is forgotten or leaves.
    pending_exact: HashMap<String, String>,
    local: Vec<String>,
    /// What each local input waits for ("Sending…", "Starting Codex…"),
    /// shown under it until its turn starts.
    local_notes: HashMap<String, String>,
    /// A local input that will not start: shown instead of its note.
    local_failures: HashMap<String, InputFailure>,
    applied_seq: i64,
    coverage: Coverage,
    /// Turns evicted by the resident bounds; late events never resurrect them.
    evicted: HashSet<String>,
    /// Eviction order of `evicted`, so the no-resurrection index stays bounded.
    evicted_order: VecDeque<String>,
}

fn data_str<'a>(event: &'a Event, field: &str) -> Option<&'a str> {
    event
        .data
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
}

impl TurnEntry {
    /// The commands whose input text this turn shows: the one that started
    /// it, then every steer accepted into it.
    fn commands(&self) -> impl Iterator<Item = &str> {
        self.command_id
            .as_deref()
            .into_iter()
            .chain(self.activity.iter().filter_map(|entry| match entry {
                Activity::Steer { command } => Some(command.as_str()),
                _ => None,
            }))
    }
}

impl Conversation {
    /// Whether a resident turn shows `command`'s input.
    fn references(&self, command: &str) -> bool {
        self.turns
            .values()
            .any(|turn| turn.commands().any(|known| known == command))
    }

    pub fn new(agent: &str) -> Self {
        Self {
            agent: agent.to_owned(),
            ..Self::default()
        }
    }

    pub fn agent(&self) -> &str {
        &self.agent
    }

    pub fn coverage(&self) -> &Coverage {
        &self.coverage
    }

    pub fn set_stale(&mut self, reason: Option<String>) {
        self.coverage.stale = reason;
    }

    /// Record the live cursor (the scan cursor also passes other Agents'
    /// and filtered-out records).
    pub fn advance_cursor(&mut self, next: i64) {
        self.coverage.cursor = self.coverage.cursor.max(next);
    }

    /// Replace everything with one snapshot: coverage, authoritative turns,
    /// the scanned window's events and input texts. Local submissions that no
    /// turn has linked yet survive the replacement.
    pub fn reset(&mut self, coverage: Coverage, turns: &[Turn], events: &[Event]) -> Invalidation {
        let local = std::mem::take(&mut self.local);
        let inputs = std::mem::take(&mut self.inputs);
        let pending_exact = std::mem::take(&mut self.pending_exact);
        let notes = std::mem::take(&mut self.local_notes);
        let failures = std::mem::take(&mut self.local_failures);
        *self = Self::new(&self.agent);
        self.coverage = coverage;
        self.inputs = inputs;
        self.pending_exact = pending_exact;
        self.local = local;
        self.local_notes = notes;
        self.local_failures = failures;
        let cursor = self.coverage.cursor;
        let mut invalidation = self.apply_events(events);
        self.coverage.cursor = cursor.max(self.applied_seq);
        self.apply_turns(turns);
        let referenced: HashSet<String> = self
            .inputs
            .keys()
            .filter(|command| self.local.contains(command) || self.references(command))
            .cloned()
            .collect();
        self.inputs
            .retain(|command, _| referenced.contains(command));
        self.pending_exact
            .retain(|command, _| self.local.iter().any(|known| known == command));
        invalidation.inputs = self.missing_inputs();
        invalidation.turns = false;
        invalidation.detail = false;
        invalidation
    }

    /// Apply journal events in sequence order. Replayed sequences are ignored,
    /// so a resnapshot followed by overlapping delivery never doubles a row.
    pub fn apply_events(&mut self, events: &[Event]) -> Invalidation {
        let mut invalidation = Invalidation::default();
        for event in events {
            if event.seq <= self.applied_seq || event.agent.as_deref() != Some(self.agent.as_str())
            {
                continue;
            }
            self.applied_seq = event.seq;
            self.coverage.cursor = self.coverage.cursor.max(event.seq);
            if event.kind == "agent.created" {
                self.coverage.reached_creation = true;
                continue;
            }
            if let Some(generation) = event.generation
                && self.coverage.generation > 0
                && generation != self.coverage.generation
            {
                // Older generations are outside this view; a newer one means
                // the Agent resumed and the snapshot must be reread.
                invalidation.generation_changed |= generation > self.coverage.generation;
                continue;
            }
            match event.kind.as_str() {
                // The Agent's state changed: its detail (and with it the
                // sidebar row and its menu) reads it again, a loss or stop
                // included (k113: a lost Agent offers Resume, not Delete).
                "request.opened" | "request.closed" | "request.resolved" | "agent.failed"
                | "agent.lost" | "agent.stopped" | "session.ready" | "session.ended" => {
                    invalidation.detail = true
                }
                _ => {}
            }
            let Some(turn_id) = data_str(event, "turn_id") else {
                continue;
            };
            if self.evicted.contains(turn_id) {
                continue;
            }
            // Only a start creates a turn: late activity for a discarded
            // turn never resurrects it with a fresh timestamp, and a
            // completion whose start is below the retention floor does
            // not fabricate a newest turn (its record still arrives
            // through `agent.result`).
            if !self.turns.contains_key(turn_id) && event.kind != "turn.started" {
                continue;
            }
            let turn = self
                .turns
                .entry(turn_id.to_owned())
                .or_insert_with(|| TurnEntry::new(turn_id));
            turn.first_seq.get_or_insert(event.seq);
            turn.started_ms.get_or_insert(event.ts);
            match event.kind.as_str() {
                "turn.started" => {
                    if let Some(command) = data_str(event, "command_id") {
                        turn.command_id = Some(command.to_owned());
                        if !self.inputs.contains_key(command) {
                            invalidation.inputs.push(command.to_owned());
                        }
                    }
                    invalidation.detail = true;
                }
                "turn.completed" | "turn.resolved" => {
                    if !turn.read_back {
                        turn.status = TurnStatus::Completed(None);
                    }
                    invalidation.turns = true;
                    invalidation.detail = true;
                }
                "command.accepted" if data_str(event, "verb") == Some("steer") => {
                    if let Some(command) = data_str(event, "command_id") {
                        let present = turn.activity.iter().any(|entry| {
                            matches!(entry, Activity::Steer { command: known } if known == command)
                        });
                        if !present {
                            turn.activity.push(Activity::Steer {
                                command: command.to_owned(),
                            });
                        }
                        if !self.inputs.contains_key(command) {
                            invalidation.inputs.push(command.to_owned());
                        }
                    }
                }
                "turn.unknown" => {
                    turn.status = TurnStatus::Unknown;
                    invalidation.turns = true;
                    invalidation.detail = true;
                }
                "message.completed" => {
                    let text = data_str(event, "text").unwrap_or_default();
                    let phase = data_str(event, "phase").unwrap_or_default();
                    if matches!(phase, "final_answer" | "final") {
                        turn.event_final = Some(bound_text(text));
                    } else if !text.trim().is_empty()
                        && (turn.messages >= MAX_MESSAGES_PER_TURN
                            || turn.message_bytes + text.len() > MAX_TURN_MESSAGE_BYTES)
                    {
                        turn.messages_dropped += 1;
                    } else if !text.trim().is_empty() {
                        let key = event
                            .native_id
                            .clone()
                            .unwrap_or_else(|| format!("seq:{}", event.seq));
                        let present = turn.activity.iter().any(
                            |entry| matches!(entry, Activity::Message { key: k, .. } if *k == key),
                        );
                        if !present {
                            let text = bound_text(text);
                            turn.messages += 1;
                            turn.message_bytes += text.len();
                            turn.activity.push(Activity::Message { key, text });
                        }
                    }
                }
                "tool.started" | "tool.completed" => {
                    let item = data_str(event, "item_id")
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("seq:{}", event.seq));
                    let tool = data_str(event, "tool").unwrap_or("tool");
                    let status = data_str(event, "status").map(str::to_owned);
                    turn.tool_event(&item, tool, status, event.kind == "tool.completed", event);
                }
                _ => {}
            }
        }
        let linked: HashSet<String> = self
            .local
            .iter()
            .filter(|command| self.references(command))
            .cloned()
            .collect();
        self.local.retain(|command| !linked.contains(command));
        for command in &linked {
            self.local_notes.remove(command);
        }
        self.pending_exact
            .retain(|command, _| self.local.iter().any(|known| known == command));
        self.enforce_bounds();
        invalidation
    }

    /// Apply authoritative turn reads (one `agent.result` page): state,
    /// outcome and public final text. The page also refreshes the turn
    /// coverage, so an Agent snapshotted at creation does not keep claiming
    /// zero turns of generation 0 after its first turns complete.
    pub fn apply_turns(&mut self, turns: &[Turn]) {
        if let Some(first) = turns.first()
            && first.agent == self.agent
        {
            if self.coverage.generation == 0 {
                self.coverage.generation = first.generation;
            }
            if first.generation == self.coverage.generation {
                self.coverage.result_returned = turns.len();
            }
        }
        for read in turns {
            if read.agent != self.agent || self.evicted.contains(&read.turn_id) {
                continue;
            }
            let turn = self
                .turns
                .entry(read.turn_id.clone())
                .or_insert_with(|| TurnEntry::new(&read.turn_id));
            if let Some(started) = read.started_at.or(read.completed_at) {
                turn.started_ms = Some(started);
            }
            turn.status = match read.state {
                TurnState::Running => TurnStatus::Running,
                TurnState::Completed => TurnStatus::Completed(read.outcome),
                TurnState::Unknown => TurnStatus::Unknown,
            };
            turn.final_text = read.final_text.as_deref().map(bound_text);
            turn.failure_reason = read
                .native_error_message
                .as_deref()
                .or(read.native_status.as_deref())
                .map(bound_text);
            turn.failure_model = read
                .effective_model
                .clone()
                .or_else(|| read.config.as_ref().and_then(|config| config.model.clone()));
            turn.read_back = true;
        }
        self.enforce_bounds();
    }

    /// Record input texts read from retained commands.
    pub fn apply_inputs(&mut self, inputs: Vec<(String, InputText)>) {
        for (command, text) in inputs {
            // Replies for departed owners never load the budget: only a
            // waiting local input or a resident turn's command keeps text.
            let wanted =
                self.local.iter().any(|known| known == &command) || self.references(&command);
            if wanted {
                self.inputs.insert(command, text);
            }
        }
        // Inputs alone can exceed the budget, so the limit holds on
        // insertion instead of waiting for the next turn event.
        self.enforce_bounds();
    }

    /// A command submitted from this window: shown at once, linked to its turn
    /// when `turn.started` names it.
    pub fn note_local_input(&mut self, command: &str, text: &str) {
        // A Send acknowledgement can land after `turn.started` already
        // linked the command: a linked command keeps no exact text, and
        // nothing is retained for it at all.
        if self
            .turns
            .values()
            .any(|turn| turn.command_id.as_deref() == Some(command))
        {
            return;
        }
        self.inputs
            .insert(command.to_owned(), InputText::Loaded(bound_text(text)));
        self.pending_exact
            .insert(command.to_owned(), text.to_owned());
        if !self.local.iter().any(|known| known == command) {
            self.local.push(command.to_owned());
        }
        // A local prompt counts from insertion: evict old turns to fit it.
        self.enforce_bounds();
    }

    /// What a local input waits for, shown under it until its turn starts.
    pub fn set_local_note(&mut self, command: &str, note: Option<String>) {
        match note {
            Some(note) => {
                self.local_notes.insert(command.to_owned(), note);
            }
            None => {
                self.local_notes.remove(command);
            }
        }
    }

    /// Drop a local input that was never admitted (a definite refusal).
    pub fn forget_local(&mut self, command: &str) {
        self.local.retain(|known| known != command);
        self.local_notes.remove(command);
        self.local_failures.remove(command);
        self.inputs.remove(command);
        self.pending_exact.remove(command);
    }

    /// Local inputs still shown as waiting (a note, no failure yet): the
    /// ones whose command outcome is worth reading.
    pub fn waiting_locals(&self) -> Vec<String> {
        self.local
            .iter()
            .filter(|command| {
                self.local_notes.contains_key(*command)
                    && !self.local_failures.contains_key(*command)
            })
            .cloned()
            .collect()
    }

    /// The local input will not start: its failure replaces the waiting
    /// note, so the conversation never spins on it.
    pub fn set_local_failure(&mut self, command: &str, failure: InputFailure) {
        if self.local.iter().any(|known| known == command) {
            self.local_notes.remove(command);
            self.local_failures.insert(command.to_owned(), failure);
        }
    }

    /// The exact submitted text of a local input, for Retry: only the
    /// retained exact payload ever resubmits, never the bounded display
    /// preview or its truncation marker (#643).
    pub fn local_text(&self, command: &str) -> Option<String> {
        if !self.local.iter().any(|known| known == command) {
            return None;
        }
        self.pending_exact.get(command).cloned()
    }

    /// Commands linked to resident turns whose input text is not loaded.
    pub fn missing_inputs(&self) -> Vec<String> {
        let mut missing: Vec<String> = self
            .turns
            .values()
            .flat_map(TurnEntry::commands)
            .filter(|command| !self.inputs.contains_key(*command))
            .map(str::to_owned)
            .collect();
        missing.sort();
        missing.dedup();
        missing
    }

    /// Resident turns without a linked input or whose input read failed.
    /// An input still being read is not counted: the note describes what
    /// is missing now, and clears once the text arrives.
    pub fn inputs_unavailable(&self) -> usize {
        self.turns
            .values()
            .filter(|turn| match turn.command_id.as_deref() {
                None => true,
                Some(command) => {
                    matches!(self.inputs.get(command), Some(InputText::Unavailable(_)))
                }
            })
            .count()
    }

    /// Input texts no resident turn and no waiting local input references.
    /// Auxiliary indexes leave with their owner instead of lingering.
    fn prune_inputs(&mut self) {
        let mut keep: HashSet<String> = self.local.iter().cloned().collect();
        keep.extend(
            self.turns
                .values()
                .flat_map(TurnEntry::commands)
                .map(str::to_owned),
        );
        self.inputs
            .retain(|command, _| keep.contains(command.as_str()));
    }

    /// Remember an evicted turn so late events never resurrect it. The
    /// index is bounded: the exact count lives in `coverage.evicted_turns`,
    /// and a snapshot rebuild starts a fresh window from retained history.
    fn remember_evicted(&mut self, turn_id: String) {
        if self.evicted.insert(turn_id.clone()) {
            self.evicted_order.push_back(turn_id);
            while self.evicted_order.len() > MAX_EVICTED_IDS {
                let Some(old) = self.evicted_order.pop_front() else {
                    break;
                };
                self.evicted.remove(&old);
            }
        }
    }

    fn enforce_bounds(&mut self) {
        loop {
            self.prune_inputs();
            let bytes: usize = self.turns.values().map(TurnEntry::bytes).sum::<usize>()
                + self
                    .inputs
                    .values()
                    .map(|text| match text {
                        InputText::Loaded(text) => text.len(),
                        InputText::Unavailable(reason) => reason.len(),
                    })
                    .sum::<usize>()
                + self.pending_exact.values().map(String::len).sum::<usize>();
            if self.turns.len() <= 1
                || (self.turns.len() <= MAX_RESIDENT_TURNS && bytes <= MAX_RESIDENT_BYTES)
            {
                // The newest turn always stays: one oversized turn is bounded
                // by its own per-message and per-turn caps instead.
                return;
            }
            let Some(oldest) = self
                .turns
                .values()
                .min_by_key(|turn| (turn.order(), turn.turn_id.clone()))
                .map(|turn| turn.turn_id.clone())
            else {
                return;
            };
            self.turns.remove(&oldest);
            self.remember_evicted(oldest);
            self.coverage.evicted_turns += 1;
        }
    }

    /// Ordered virtualized items with stable keys and content revisions.
    pub fn items(&self) -> Vec<TurnItem> {
        let mut turns: Vec<&TurnEntry> = self.turns.values().collect();
        turns.sort_by(|a, b| a.order().cmp(&b.order()).then(a.turn_id.cmp(&b.turn_id)));
        let mut items: Vec<TurnItem> = turns.iter().map(|turn| self.turn_item(turn)).collect();
        for command in &self.local {
            let text = match self.inputs.get(command) {
                Some(InputText::Loaded(text)) => text.clone(),
                _ => String::new(),
            };
            let mut parts = vec![Part::new(
                format!("in:{command}"),
                PartKind::Input,
                "You".to_owned(),
                text,
                Vec::new(),
            )];
            if let Some(failure) = self.local_failures.get(command) {
                let (kind, label) = if failure.definite {
                    (PartKind::Failed, "Did not start")
                } else {
                    (PartKind::Unconfirmed, "Not confirmed")
                };
                parts.push(Part::new(
                    format!("fail:{command}"),
                    kind,
                    label.to_owned(),
                    eliot_core::reason_message(&failure.code, &failure.message),
                    vec![format!(
                        "Origin: {}",
                        eliot_core::origin_words(&failure.origin)
                    )],
                ));
            } else if let Some(note) = self.local_notes.get(command) {
                parts.push(Part::new(
                    format!("wait:{command}"),
                    PartKind::Working,
                    note.clone(),
                    note.clone(),
                    Vec::new(),
                ));
            }
            let revision = parts
                .iter()
                .fold(text_hash(command), |hash, part| mix(hash, part.revision));
            items.push(TurnItem {
                key: format!("local:{command}"),
                revision,
                parts,
            });
        }
        items
    }

    fn turn_item(&self, turn: &TurnEntry) -> TurnItem {
        let mut parts = Vec::new();
        match turn.command_id.as_deref() {
            Some(command) => {
                let (label, text) = match self.inputs.get(command) {
                    Some(InputText::Loaded(text)) => ("You".to_owned(), text.clone()),
                    Some(InputText::Unavailable(reason)) => (
                        "You".to_owned(),
                        format!("(input text unavailable: {reason})"),
                    ),
                    None => ("You".to_owned(), "(loading input text…)".to_owned()),
                };
                parts.push(Part::new(
                    format!("in:{command}"),
                    PartKind::Input,
                    label,
                    text,
                    Vec::new(),
                ));
            }
            None => parts.push(Part::new(
                format!("in:turn:{}", turn.turn_id),
                PartKind::Notice,
                "Input".to_owned(),
                "The message that started this turn is not in the loaded history.".to_owned(),
                Vec::new(),
            )),
        }
        let final_text = turn.final_text.clone().or_else(|| turn.event_final.clone());
        // The last progress message is often the answer itself (Muse reports
        // both); once the final exists it is shown once, as the final.
        let echoed = final_text.as_deref().and_then(|answer| {
            turn.activity
                .iter()
                .rposition(|entry| matches!(entry, Activity::Message { .. }))
                .filter(|&index| {
                    matches!(&turn.activity[index],
                        Activity::Message { text, .. } if text.trim() == answer.trim())
                })
        });
        for (index, entry) in turn.activity.iter().enumerate() {
            if Some(index) == echoed {
                continue;
            }
            match entry {
                Activity::Message { key, text } => parts.push(Part::new(
                    key.clone(),
                    PartKind::Progress,
                    "Assistant · progress".to_owned(),
                    text.clone(),
                    Vec::new(),
                )),
                Activity::Steer { command } => parts.push(Part::new(
                    format!("steer:{command}"),
                    PartKind::Steer,
                    "You · steered".to_owned(),
                    match self.inputs.get(command) {
                        Some(InputText::Loaded(text)) => text.clone(),
                        Some(InputText::Unavailable(reason)) => {
                            format!("(input text unavailable: {reason})")
                        }
                        None => "(loading input text…)".to_owned(),
                    },
                    Vec::new(),
                )),
                Activity::Tools {
                    key,
                    rows,
                    total,
                    done,
                    failed,
                } => {
                    if *total > rows.len() {
                        parts.push(Part::new(
                            format!("{key}:omitted"),
                            PartKind::Notice,
                            "Tools".to_owned(),
                            format!(
                                "{} · {} earlier tools not listed",
                                tools_summary(*total, *done, *failed),
                                total - rows.len()
                            ),
                            Vec::new(),
                        ));
                    }
                    for row in rows {
                        let name = match row.tool.as_str() {
                            "commandExecution" => "Run command",
                            "fileChange" => "Edit files",
                            other => other,
                        };
                        let status = match (row.done, row.status.as_deref()) {
                            (false, _) => "Running",
                            (true, Some(status)) if is_success(status) => "Completed",
                            (true, Some(status)) => status,
                            (true, None) => "Finished · result not reported",
                        };
                        let elapsed = row
                            .started_ms
                            .zip(row.completed_ms)
                            .and_then(|(start, end)| end.checked_sub(start))
                            .filter(|ms| *ms >= 0);
                        let duration = elapsed.map_or_else(
                            || "duration unavailable".to_owned(),
                            |ms| format!("~{:.1} s", ms as f64 / 1_000.0),
                        );
                        let mut detail = row.paths.clone();
                        if row.paths_truncated {
                            detail.push(
                                "More changed paths were not retained in this card.".to_owned(),
                            );
                        }
                        detail.push("Tool output was not retained by the app.".to_owned());
                        if elapsed.is_some() {
                            detail.push(
                                "Duration is measured between the recorded start and finish."
                                    .to_owned(),
                            );
                        }
                        parts.push(Part::new(
                            format!("tools:{}", row.item_id),
                            PartKind::Tools,
                            "Tool".to_owned(),
                            format!("{name} · {status} · {duration}"),
                            detail,
                        ));
                    }
                }
            }
        }

        if turn.messages_dropped > 0 {
            parts.push(Part::new(
                format!("dropped:{}", turn.turn_id),
                PartKind::Notice,
                "Progress".to_owned(),
                format!(
                    "{} not kept: a turn keeps a bounded number.",
                    count(
                        turn.messages_dropped,
                        "more progress message",
                        "more progress messages"
                    )
                ),
                Vec::new(),
            ));
        }
        let final_key = format!("final:{}", turn.turn_id);
        let label = match &turn.status {
            TurnStatus::Completed(Some(TurnOutcome::Success)) | TurnStatus::Completed(None) => {
                "Assistant".to_owned()
            }
            TurnStatus::Completed(Some(TurnOutcome::Failure)) => "Assistant · failed".to_owned(),
            TurnStatus::Completed(Some(TurnOutcome::Interrupted)) => {
                "Assistant · interrupted".to_owned()
            }
            TurnStatus::Completed(Some(TurnOutcome::Unknown)) => {
                "Assistant · result not confirmed".to_owned()
            }
            TurnStatus::Unknown => "Assistant · result not confirmed".to_owned(),
            TurnStatus::Running => "Assistant · working".to_owned(),
        };
        match (final_text, &turn.status) {
            (Some(text), _) => parts.push(Part::new(
                final_key,
                PartKind::Final,
                label,
                text,
                Vec::new(),
            )),
            (None, TurnStatus::Running) => parts.push(Part::new(
                final_key,
                PartKind::Working,
                label,
                "Working…".to_owned(),
                Vec::new(),
            )),
            (None, _) => parts.push(Part::new(
                final_key,
                PartKind::Notice,
                label,
                "No public final text for this turn.".to_owned(),
                Vec::new(),
            )),
        }
        // A failed turn shows its native error inline, names the model it
        // ran under, and points at the model picker for the next try. The
        // picker itself stays available: nothing else in the chat changes.
        // Failures render once, as one failed part.
        if matches!(
            turn.status,
            TurnStatus::Completed(Some(TurnOutcome::Failure))
        ) {
            let reason = turn
                .failure_reason
                .clone()
                .unwrap_or_else(|| "The turn failed.".to_owned());
            let mut detail = Vec::new();
            if let Some(model) = turn.failure_model.as_deref() {
                detail.push(format!("Model {model}"));
            }
            let final_part = parts.pop().expect("final part was just pushed");
            let (text, detail) = match final_part.kind {
                PartKind::Final => {
                    let mut with_text = vec![reason];
                    with_text.extend(detail);
                    (final_part.text, with_text)
                }
                _ => (reason, detail),
            };
            parts.push(Part::new(
                format!("failed:{}", turn.turn_id),
                PartKind::TurnFailed,
                final_part.label,
                text,
                detail,
            ));
        }
        let revision = parts.iter().fold(text_hash(&turn.turn_id), |hash, part| {
            mix(hash, part.revision)
        });
        TurnItem {
            key: format!("turn:{}", turn.turn_id),
            revision,
            parts,
        }
    }

    /// Texts that render as Markdown, for off-thread parsing.
    pub fn markdown_texts(turns: &[Turn], events: &[Event]) -> Vec<String> {
        let mut texts: Vec<String> = turns
            .iter()
            .filter_map(|turn| turn.final_text.as_deref().map(bound_text))
            .collect();
        texts.extend(events.iter().filter_map(|event| {
            (event.kind == "message.completed")
                .then(|| data_str(event, "text").map(bound_text))
                .flatten()
        }));
        texts
    }

    /// Full retained text of one part, for Copy message.
    pub fn part_text(&self, key: &str) -> Option<String> {
        self.items()
            .into_iter()
            .flat_map(|item| item.parts)
            .find(|part| part.key == key)
            .map(|part| {
                if part.detail.is_empty() {
                    part.text
                } else {
                    format!("{}\n{}", part.text, part.detail.join("\n"))
                }
            })
    }
}

/// `n` and the noun in the right number: "1 tool", "3 tools".
fn count(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// A turn's tool card in words: "Ran 3 tools" once all finished well,
/// else "3 tools · 1 running · 1 failed".
fn tools_summary(total: usize, done: usize, failed: usize) -> String {
    let tools = count(total, "tool", "tools");
    let running = total.saturating_sub(done);
    if running == 0 && failed == 0 {
        return format!("Ran {tools}");
    }
    let mut parts = vec![tools];
    if running > 0 {
        parts.push(format!("{running} running"));
    }
    if failed > 0 {
        parts.push(format!("{failed} failed"));
    }
    parts.join(" · ")
}

/// Bounded file and folder references in prose, Markdown destinations, code
/// and tool details: a name with a known extension, or an absolute Windows
/// path (a folder has no extension). Resolution and existence checks belong
/// to the click's native I/O boundary.
pub fn file_references(text: &str) -> Vec<String> {
    let mut paths = Vec::new();
    // Only paired backticks/double quotes preserve spaces. Apostrophes in
    // prose never turn a sentence into a path candidate.
    let mut quoted = Vec::new();
    let mut prose = text.to_owned();
    for delimiter in ['`', '"'] {
        let mut marks = text.match_indices(delimiter);
        while let (Some((start, _)), Some((end, _))) = (marks.next(), marks.next()) {
            let span = &text[start + 1..end];
            if (span.contains(' ') && span.contains(['/', '\\'])) || span.starts_with(r"\\?\") {
                quoted.push(span);
                // Do not also offer broken pieces of a quoted path with spaces.
                prose.replace_range(start..end + 1, &" ".repeat(end + 1 - start));
                if quoted.len() == 24 {
                    break;
                }
            }
        }
        if quoted.len() == 24 {
            break;
        }
    }
    let words = prose.split(|c: char| {
        c.is_whitespace() || matches!(c, '`' | '"' | '\'' | '(' | ')' | '[' | ']' | '<' | '>')
    });
    for token in quoted.into_iter().chain(words) {
        let token = token.trim_matches(|c: char| matches!(c, ',' | ';' | '!' | '?' | '*'));
        let path = token.split('#').next().unwrap_or(token);
        let mut path = path;
        while let Some((head, tail)) = path.rsplit_once(':') {
            if !tail.is_empty() && tail.bytes().all(|b| b.is_ascii_digit()) {
                path = head;
            } else {
                break;
            }
        }
        // A period is part of a verbatim filename. Explorer must either
        // preserve that exact target or show the reveal error for it.
        let path = if path.starts_with(r"\\?\") {
            path
        } else {
            path.trim_end_matches(['.', ':'])
        };
        let unc = path
            .strip_prefix(r"\\?\UNC\")
            .or_else(|| {
                path.strip_prefix(r"\\")
                    .filter(|rest| !rest.starts_with(['?', '.']))
            })
            .and_then(|rest| rest.split_once('\\'))
            .is_some_and(|(host, remaining)| {
                !host.is_empty() && !remaining.split('\\').next().unwrap_or_default().is_empty()
            });
        let drive = path.strip_prefix(r"\\?\").unwrap_or(path).as_bytes();
        let absolute = unc
            || (drive.len() >= 3
                && drive[0].is_ascii_alphabetic()
                && drive[1] == b':'
                && matches!(drive[2], b'\\' | b'/'));
        if path.len() > 2_048
            || path.contains("://")
            || (!absolute && path.starts_with(['-', '/', '\\']))
            || path.contains(['=', '|'])
        {
            continue;
        }
        let extension = path.rsplit('.').next().unwrap_or_default();
        let file = path.contains('.')
            && !extension.is_empty()
            && extension.len() <= 12
            && extension.chars().all(char::is_alphanumeric)
            && extension.chars().any(char::is_alphabetic)
            && (path.contains(['/', '\\'])
                || matches!(
                    extension,
                    "rs" | "md"
                        | "toml"
                        | "txt"
                        | "json"
                        | "jsonl"
                        | "py"
                        | "ps1"
                        | "js"
                        | "ts"
                        | "tsx"
                        | "yaml"
                        | "yml"
                        | "csv"
                        | "log"
                        | "sql"
                ));
        if (file || absolute) && path != ".." && !paths.iter().any(|p| p == path) {
            paths.push(path.to_owned());
            if paths.len() == 24 {
                break;
            }
        }
    }
    paths
}
