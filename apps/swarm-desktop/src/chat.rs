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
//! returned against its limit, which journal window was scanned, whether the
//! Agent's creation event was inside it, the retention floor, evictions and
//! inputs whose text could not be read.

use std::collections::{HashMap, HashSet};

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
/// Progress messages kept per turn.
pub const MAX_MESSAGES_PER_TURN: usize = 200;
/// Progress text bytes kept per turn; later messages are counted, not kept.
pub const MAX_TURN_MESSAGE_BYTES: usize = 2 * 1024 * 1024;

/// Event kinds the conversation and its readback invalidation consume.
pub const CHAT_KINDS: &[&str] = &[
    "agent.created",
    "agent.failed",
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
}

#[derive(Clone, Debug)]
enum Activity {
    Message {
        key: String,
        text: String,
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
    read_back: bool,
    activity: Vec<Activity>,
    messages: usize,
    message_bytes: usize,
    messages_dropped: usize,
    /// Tool item id to the index of the activity group that started it.
    item_groups: HashMap<String, usize>,
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
            read_back: false,
            activity: Vec::new(),
            messages: 0,
            message_bytes: 0,
            messages_dropped: 0,
            item_groups: HashMap::new(),
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
                Activity::Tools { rows, .. } => rows.len() * 64,
            })
            .sum();
        activity
            + self.final_text.as_ref().map_or(0, String::len)
            + self.event_final.as_ref().map_or(0, String::len)
    }

    fn tool_event(&mut self, item_id: &str, tool: &str, status: Option<String>, done: bool) {
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
                        *done_count += 1;
                        *failed += usize::from(failed_now);
                    }
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
    /// Exclusive lower bound of the scanned journal window.
    pub window_from: i64,
    /// Committed cursor: events are applied through this sequence.
    pub cursor: i64,
    /// Retention floor from `events.bounds`.
    pub pruned_through: i64,
    /// The Agent's `agent.created` event was inside the scanned window.
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

    /// The window reached the retention floor before the Agent's creation:
    /// older activity was pruned and cannot be read.
    pub fn activity_pruned(&self) -> bool {
        !self.reached_creation && self.window_from <= self.pruned_through && self.pruned_through > 0
    }

    /// Older retained activity exists beyond the scanned window.
    pub fn activity_more(&self) -> bool {
        !self.reached_creation && self.window_from > self.pruned_through
    }

    pub fn can_load_older(&self) -> bool {
        (!self.turns_complete() && self.result_limit < MAX_RESULT_LIMIT) || self.activity_more()
    }

    /// Human-readable coverage lines; every limit is named, none is hidden.
    pub fn lines(&self, inputs_missing: usize) -> Vec<String> {
        let mut lines = Vec::new();
        let turns = if self.turns_complete() {
            format!(
                "Turns: all {} retained turns of generation {}",
                self.result_returned, self.generation
            )
        } else {
            format!(
                "Turns: newest {} of generation {}; older turns exist",
                self.result_returned, self.generation
            )
        };
        lines.push(if self.generation > 1 {
            format!("{turns}; earlier generations are not shown")
        } else {
            turns
        });
        let activity = if self.reached_creation {
            format!(
                "Activity: complete from creation through event {}",
                self.cursor
            )
        } else if self.activity_pruned() {
            format!(
                "Activity: events {}–{}; earlier activity was pruned by retention (floor {})",
                self.window_from + 1,
                self.cursor,
                self.pruned_through
            )
        } else {
            format!(
                "Activity: events {}–{}; older activity not loaded",
                self.window_from + 1,
                self.cursor
            )
        };
        lines.push(activity);
        if inputs_missing > 0 {
            lines.push(format!(
                "{inputs_missing} turn input(s) not linked or unreadable in the loaded window"
            ));
        }
        if self.evicted_turns > 0 {
            lines.push(format!(
                "{} older turn(s) evicted from memory by the resident bound",
                self.evicted_turns
            ));
        }
        if let Some(stale) = &self.stale {
            lines.push(format!("Stale: {stale}; rereading"));
        }
        lines
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
    Progress,
    Tools,
    Final,
    Working,
    Notice,
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
    local: Vec<String>,
    applied_seq: i64,
    coverage: Coverage,
    /// Turns evicted by the resident bounds; late events never resurrect them.
    evicted: HashSet<String>,
}

fn data_str<'a>(event: &'a Event, field: &str) -> Option<&'a str> {
    event
        .data
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
}

impl Conversation {
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
        *self = Self::new(&self.agent);
        self.coverage = coverage;
        self.inputs = inputs;
        self.local = local;
        let cursor = self.coverage.cursor;
        let mut invalidation = self.apply_events(events);
        self.coverage.cursor = cursor.max(self.applied_seq);
        self.apply_turns(turns);
        self.inputs.retain(|command, _| {
            self.local.contains(command)
                || self
                    .turns
                    .values()
                    .any(|turn| turn.command_id.as_deref() == Some(command))
        });
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
                "request.opened" | "request.closed" | "request.resolved" | "agent.failed"
                | "session.ready" | "session.ended" => invalidation.detail = true,
                _ => {}
            }
            let Some(turn_id) = data_str(event, "turn_id") else {
                continue;
            };
            if self.evicted.contains(turn_id) {
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
                    turn.tool_event(&item, tool, status, event.kind == "tool.completed");
                }
                _ => {}
            }
        }
        self.local.retain(|command| {
            !self
                .turns
                .values()
                .any(|turn| turn.command_id.as_deref() == Some(command))
        });
        self.enforce_bounds();
        invalidation
    }

    /// Apply authoritative turn reads: state, outcome and public final text.
    pub fn apply_turns(&mut self, turns: &[Turn]) {
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
            turn.read_back = true;
        }
        self.enforce_bounds();
    }

    /// Record input texts read from retained commands.
    pub fn apply_inputs(&mut self, inputs: Vec<(String, InputText)>) {
        for (command, text) in inputs {
            self.inputs.insert(command, text);
        }
    }

    /// A command submitted from this window: shown at once, linked to its turn
    /// when `turn.started` names it.
    pub fn note_local_input(&mut self, command: &str, text: &str) {
        self.inputs
            .insert(command.to_owned(), InputText::Loaded(bound_text(text)));
        let linked = self
            .turns
            .values()
            .any(|turn| turn.command_id.as_deref() == Some(command));
        if !linked && !self.local.iter().any(|known| known == command) {
            self.local.push(command.to_owned());
        }
    }

    /// Commands linked to resident turns whose input text is not loaded.
    pub fn missing_inputs(&self) -> Vec<String> {
        let mut missing: Vec<String> = self
            .turns
            .values()
            .filter_map(|turn| turn.command_id.clone())
            .filter(|command| !self.inputs.contains_key(command))
            .collect();
        missing.sort();
        missing.dedup();
        missing
    }

    /// Resident turns without a linked input or with an unreadable one.
    pub fn inputs_unavailable(&self) -> usize {
        self.turns
            .values()
            .filter(|turn| match turn.command_id.as_deref() {
                None => true,
                Some(command) => !matches!(self.inputs.get(command), Some(InputText::Loaded(_))),
            })
            .count()
    }

    fn enforce_bounds(&mut self) {
        loop {
            let bytes: usize = self.turns.values().map(TurnEntry::bytes).sum();
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
            self.evicted.insert(oldest);
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
            let part = Part::new(
                format!("in:{command}"),
                PartKind::Input,
                "You · submitted, waiting for its turn".to_owned(),
                text,
                Vec::new(),
            );
            items.push(TurnItem {
                key: format!("local:{command}"),
                revision: part.revision,
                parts: vec![part],
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
                "Input not linked: the turn started outside the loaded activity window.".to_owned(),
                Vec::new(),
            )),
        }
        for entry in &turn.activity {
            parts.push(match entry {
                Activity::Message { key, text } => Part::new(
                    key.clone(),
                    PartKind::Progress,
                    "Assistant · progress".to_owned(),
                    text.clone(),
                    Vec::new(),
                ),
                Activity::Tools {
                    key,
                    rows,
                    total,
                    done,
                    failed,
                } => {
                    let mut summary = format!("{total} tool operation(s) · {done} finished");
                    if *failed > 0 {
                        summary.push_str(&format!(" · {failed} not successful"));
                    }
                    if total > done {
                        summary.push_str(&format!(" · {} running", total - done));
                    }
                    let mut detail: Vec<String> = Vec::new();
                    if *total > rows.len() {
                        detail.push(format!(
                            "… {} earlier operation(s) not listed",
                            total - rows.len()
                        ));
                    }
                    detail.extend(rows.iter().map(|row| {
                        let mark = match (row.done, row.status.as_deref()) {
                            (false, _) => "…",
                            (true, Some(status)) if !is_success(status) => "✕",
                            (true, _) => "✓",
                        };
                        match row.status.as_deref() {
                            Some(status) => format!("{mark} {} ({status})", row.tool),
                            None => format!("{mark} {}", row.tool),
                        }
                    }));
                    Part::new(
                        key.clone(),
                        PartKind::Tools,
                        "Tools".to_owned(),
                        summary,
                        detail,
                    )
                }
            });
        }
        if turn.messages_dropped > 0 {
            parts.push(Part::new(
                format!("dropped:{}", turn.turn_id),
                PartKind::Notice,
                "Progress".to_owned(),
                format!(
                    "{} further progress message(s) not kept by the per-turn bound.",
                    turn.messages_dropped
                ),
                Vec::new(),
            ));
        }
        let final_key = format!("final:{}", turn.turn_id);
        let final_text = turn.final_text.clone().or_else(|| turn.event_final.clone());
        let label = match &turn.status {
            TurnStatus::Completed(Some(TurnOutcome::Success)) | TurnStatus::Completed(None) => {
                "Assistant".to_owned()
            }
            TurnStatus::Completed(Some(outcome)) => format!("Assistant · {}", outcome.as_str()),
            TurnStatus::Unknown => "Assistant · outcome unknown".to_owned(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn event(seq: i64, kind: &str, data: serde_json::Value) -> Event {
        Event {
            seq,
            ts: 1_000 + seq,
            kind: kind.to_owned(),
            agent: Some("a1".to_owned()),
            subject: None,
            generation: Some(1),
            source: "stream".to_owned(),
            native_id: Some(format!("n{seq}")),
            data,
            cause: None,
        }
    }

    fn turn(id: &str, state: TurnState, final_text: Option<&str>) -> Turn {
        Turn {
            agent: "a1".to_owned(),
            generation: 1,
            turn_id: id.to_owned(),
            state,
            status: None,
            final_text: final_text.map(str::to_owned),
            started_at: Some(2_000),
            completed_at: None,
            usage: None,
            outcome: Some(TurnOutcome::Success),
            native_status: None,
            config_revision: 0,
        }
    }

    fn coverage() -> Coverage {
        Coverage {
            generation: 1,
            result_limit: TURN_PAGE,
            cursor: 0,
            ..Coverage::default()
        }
    }

    fn sample_events() -> Vec<Event> {
        vec![
            event(1, "agent.created", json!({})),
            event(
                2,
                "turn.started",
                json!({"turn_id": "t1", "command_id": "c1"}),
            ),
            event(
                3,
                "message.completed",
                json!({"turn_id": "t1", "phase": "commentary", "text": "looking"}),
            ),
            event(
                4,
                "tool.started",
                json!({"turn_id": "t1", "item_id": "i1", "tool": "exec"}),
            ),
            event(
                5,
                "tool.completed",
                json!({"turn_id": "t1", "item_id": "i1", "tool": "exec", "status": "completed"}),
            ),
            event(
                6,
                "message.completed",
                json!({"turn_id": "t1", "phase": "final_answer", "text": "done"}),
            ),
            event(7, "turn.completed", json!({"turn_id": "t1"})),
        ]
    }

    #[test]
    fn snapshot_builds_one_item_per_turn_with_stable_part_keys() {
        let mut chat = Conversation::new("a1");
        let missing = chat.reset(
            coverage(),
            &[turn("t1", TurnState::Completed, Some("**done**"))],
            &sample_events(),
        );
        assert_eq!(missing.inputs, vec!["c1".to_owned()]);
        chat.apply_inputs(vec![("c1".to_owned(), InputText::Loaded("hi".to_owned()))]);
        let items = chat.items();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].key, "turn:t1");
        let keys: Vec<_> = items[0]
            .parts
            .iter()
            .map(|part| part.key.as_str())
            .collect();
        assert_eq!(keys, vec!["in:c1", "n3", "tools:i1", "final:t1"]);
        assert_eq!(items[0].parts[3].text, "**done**");
        assert!(chat.coverage().reached_creation);
    }

    #[test]
    fn replayed_events_never_double_rows() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &sample_events());
        let before = chat.items();
        chat.apply_events(&sample_events());
        assert_eq!(chat.items(), before);
    }

    #[test]
    fn activity_changes_revision_but_not_item_keys() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &sample_events()[..3]);
        let before = chat.items();
        chat.apply_events(&sample_events()[3..5]);
        let after = chat.items();
        assert_eq!(before.len(), after.len());
        assert_eq!(before[0].key, after[0].key);
        assert_ne!(before[0].revision, after[0].revision);
        assert_eq!(
            before[0].parts[1], after[0].parts[1],
            "unchanged part keeps its revision"
        );
    }

    #[test]
    fn authoritative_turn_read_wins_over_event_final() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &sample_events());
        assert_eq!(chat.items()[0].parts.last().unwrap().text, "done");
        chat.apply_turns(&[turn("t1", TurnState::Completed, Some("authoritative"))]);
        assert_eq!(chat.items()[0].parts.last().unwrap().text, "authoritative");
    }

    #[test]
    fn turn_end_and_requests_invalidate_readback() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &sample_events()[..2]);
        let invalidation = chat.apply_events(&[
            event(8, "request.opened", json!({"turn_id": "t1"})),
            event(9, "turn.completed", json!({"turn_id": "t1"})),
        ]);
        assert!(invalidation.turns && invalidation.detail);
    }

    #[test]
    fn local_input_shows_until_its_turn_links_it() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &[]);
        chat.note_local_input("c9", "hello");
        assert_eq!(chat.items()[0].key, "local:c9");
        chat.apply_events(&[event(
            10,
            "turn.started",
            json!({"turn_id": "t9", "command_id": "c9"}),
        )]);
        let items = chat.items();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].key, "turn:t9");
        assert_eq!(items[0].parts[0].text, "hello");
    }

    #[test]
    fn other_agents_and_generations_are_ignored() {
        let mut chat = Conversation::new("a1");
        chat.reset(
            Coverage {
                generation: 2,
                ..coverage()
            },
            &[],
            &[],
        );
        let mut foreign = event(3, "turn.started", json!({"turn_id": "tx"}));
        foreign.agent = Some("a2".to_owned());
        let old_generation = event(4, "turn.started", json!({"turn_id": "ty"}));
        chat.apply_events(&[foreign, old_generation]);
        assert!(chat.items().is_empty());
    }

    #[test]
    fn coverage_names_pruned_and_unloaded_history() {
        let mut coverage = coverage();
        coverage.window_from = 50;
        coverage.pruned_through = 50;
        coverage.cursor = 90;
        assert!(coverage.activity_pruned());
        assert!(coverage.lines(0)[1].contains("pruned by retention"));
        coverage.pruned_through = 10;
        assert!(coverage.activity_more());
        assert!(coverage.lines(0)[1].contains("older activity not loaded"));
        coverage.result_returned = TURN_PAGE;
        assert!(!coverage.turns_complete());
        assert!(coverage.can_load_older());
    }

    #[test]
    fn resident_turns_stay_bounded_and_evictions_are_counted() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &[]);
        let events: Vec<Event> = (0..(MAX_RESIDENT_TURNS as i64 + 5))
            .map(|index| {
                event(
                    index + 1,
                    "turn.started",
                    json!({"turn_id": format!("t{index:04}")}),
                )
            })
            .collect();
        chat.apply_events(&events);
        assert_eq!(chat.items().len(), MAX_RESIDENT_TURNS);
        assert_eq!(chat.coverage().evicted_turns, 5);
        assert_eq!(chat.items()[0].key, "turn:t0005");
    }

    #[test]
    fn evicted_turns_are_not_resurrected_by_late_events() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &[]);
        let events: Vec<Event> = (0..(MAX_RESIDENT_TURNS as i64 + 1))
            .map(|index| {
                event(
                    index + 1,
                    "turn.started",
                    json!({"turn_id": format!("t{index:04}")}),
                )
            })
            .collect();
        chat.apply_events(&events);
        assert_eq!(chat.coverage().evicted_turns, 1);
        chat.apply_events(&[event(9_000, "turn.completed", json!({"turn_id": "t0000"}))]);
        assert_eq!(chat.items().len(), MAX_RESIDENT_TURNS);
        assert!(chat.items().iter().all(|item| item.key != "turn:t0000"));
    }

    #[test]
    fn one_oversized_turn_is_never_evicted_entirely() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &[]);
        let big = "x".repeat(MAX_TEXT_BYTES - 1);
        let events: Vec<Event> = (0..60)
            .map(|index| {
                event(
                    index + 1,
                    "message.completed",
                    json!({"turn_id": "t1", "phase": "commentary", "text": big}),
                )
            })
            .collect();
        chat.apply_events(&events);
        let items = chat.items();
        assert_eq!(items.len(), 1);
        assert!(items[0].parts.iter().any(|part| part.key == "dropped:t1"));
    }

    #[test]
    fn newer_generation_events_request_a_resnapshot() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &[]);
        let mut resumed = event(5, "turn.started", json!({"turn_id": "t2"}));
        resumed.generation = Some(2);
        assert!(chat.apply_events(&[resumed]).generation_changed);
    }

    #[test]
    fn completion_without_status_is_not_left_running() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &[]);
        chat.apply_events(&[
            event(
                1,
                "tool.started",
                json!({"turn_id": "t1", "item_id": "i1", "tool": "exec", "status": "inProgress"}),
            ),
            event(
                2,
                "tool.completed",
                json!({"turn_id": "t1", "item_id": "i1", "tool": "exec"}),
            ),
        ]);
        let tools = &chat.items()[0].parts[1];
        assert_eq!(tools.detail, vec!["✓ exec".to_owned()]);
    }

    #[test]
    fn tool_counters_stay_exact_past_the_row_bound() {
        let mut chat = Conversation::new("a1");
        chat.reset(coverage(), &[], &[]);
        let mut events = Vec::new();
        let mut seq = 1;
        for index in 0..(MAX_TOOL_ROWS + 10) {
            events.push(event(
                seq,
                "tool.started",
                json!({"turn_id": "t1", "item_id": format!("i{index}"), "tool": "exec"}),
            ));
            seq += 1;
        }
        for index in 0..(MAX_TOOL_ROWS + 10) {
            events.push(event(seq, "tool.completed", json!({"turn_id": "t1", "item_id": format!("i{index}"), "tool": "exec", "status": "completed"})));
            seq += 1;
        }
        chat.apply_events(&events);
        let tools = &chat.items()[0].parts[1];
        assert_eq!(tools.kind, PartKind::Tools);
        assert!(tools.text.starts_with(&format!(
            "{} tool operation(s) · {} finished",
            MAX_TOOL_ROWS + 10,
            MAX_TOOL_ROWS + 10
        )));
    }
}
