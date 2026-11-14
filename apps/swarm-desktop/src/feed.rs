//! Background reads that feed one conversation: snapshot, coalesced
//! reread and live wake/drain. Plain blocking functions for Reactor's
//! one-shot background workers; no WinUI types, so they build everywhere.
//!
//! Snapshot recovery follows the issue's order: capture the committed cursor
//! first (`events.bounds`), then load the current reads, then scan the
//! retained window up to that cursor. Live delivery continues strictly after
//! it, so an event is either inside the snapshot or delivered later, never
//! both and never neither. Markdown is parsed here, off the UI thread, into
//! owned blocks keyed by content hash.

use std::collections::HashSet;
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use eliot_core::api::AgentGetResponse;
use eliot_core::{Event, Turn};

use crate::chat::{Conversation, Coverage, InputText, text_hash};
use crate::client;
use crate::live::{COALESCE_MS, DRAIN_LIMIT, DRAIN_PAGES};
use crate::markdown::{self, Doc};

/// Journal rows scanned at most, however often "Load older" is used.
pub const MAX_WINDOW_ROWS: i64 = 80_000;

type Fallible<T> = Result<T, String>;

fn text(error: eliot_core::Error) -> String {
    error.to_string()
}

/// How much retained history a snapshot reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Depth {
    pub result_limit: usize,
    pub window_rows: i64,
}

impl Default for Depth {
    fn default() -> Self {
        Self {
            result_limit: crate::chat::TURN_PAGE,
            window_rows: crate::chat::ACTIVITY_WINDOW,
        }
    }
}

impl Depth {
    /// One "Load older" step, bounded.
    pub fn older(self) -> Self {
        Self {
            result_limit: (self.result_limit + crate::chat::TURN_PAGE)
                .min(crate::chat::MAX_RESULT_LIMIT),
            window_rows: (self.window_rows + crate::chat::ACTIVITY_WINDOW).min(MAX_WINDOW_ROWS),
        }
    }
}

/// Everything one conversation snapshot read.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub coverage: Coverage,
    pub turns: Vec<Turn>,
    pub events: Vec<Event>,
    pub inputs: Vec<(String, InputText)>,
    pub detail: AgentGetResponse,
    pub docs: Vec<(u64, Doc)>,
}

/// A coalesced authoritative reread after events invalidated state.
#[derive(Clone, Debug)]
pub struct Reread {
    pub turns: Option<Vec<Turn>>,
    pub detail: Option<AgentGetResponse>,
    pub inputs: Vec<(String, InputText)>,
    pub docs: Vec<(u64, Doc)>,
}

/// One live delivery: events strictly after the cursor and the next cursor.
#[derive(Clone, Debug)]
pub struct Delivery {
    pub events: Vec<Event>,
    pub next: i64,
    pub truncated: bool,
    pub lagged: bool,
    pub docs: Vec<(u64, Doc)>,
}

fn parse_docs(texts: Vec<String>) -> Vec<(u64, Doc)> {
    let mut seen = HashSet::new();
    texts
        .into_iter()
        .filter_map(|text| {
            let hash = text_hash(&text);
            seen.insert(hash).then(|| (hash, markdown::parse(&text)))
        })
        .collect()
}

fn linked_commands(events: &[Event]) -> Vec<String> {
    let mut commands: Vec<String> = events
        .iter()
        .filter(|event| event.kind == "turn.started")
        .filter_map(|event| event.data.get("command_id")?.as_str().map(str::to_owned))
        .collect();
    commands.sort();
    commands.dedup();
    commands
}

/// Snapshot one Agent: cursor first, then the reads, then the window.
pub fn snapshot(data_dir: &Path, agent: &str, depth: Depth) -> Fallible<Snapshot> {
    client::block_on(async {
        let filter = client::chat_filter(agent);
        let bounds = client::events_bounds(data_dir, &filter)
            .await
            .map_err(text)?;
        let detail = client::get_agent(data_dir, agent).await.map_err(text)?;
        let results = client::agent_results(data_dir, agent, depth.result_limit)
            .await
            .map_err(text)?;
        let window_from = bounds
            .pruned_through
            .max(bounds.hi.saturating_sub(depth.window_rows))
            .max(0);
        let events = client::scan_events(data_dir, window_from, bounds.hi, &filter)
            .await
            .map_err(text)?;
        let inputs = client::input_texts(data_dir, linked_commands(&events)).await;
        let docs = parse_docs(Conversation::markdown_texts(&results.turns, &events));
        Ok(Snapshot {
            coverage: Coverage {
                generation: results.generation,
                result_limit: depth.result_limit,
                result_returned: results.turns.len(),
                window_from,
                cursor: bounds.hi,
                pruned_through: bounds.pruned_through,
                ..Coverage::default()
            },
            turns: results.turns,
            events,
            inputs,
            detail,
            docs,
        })
    })
}

/// Authoritative reread of what a delivery invalidated, in one task.
pub fn reread(
    data_dir: &Path,
    agent: &str,
    result_limit: usize,
    turns: bool,
    detail: bool,
    inputs: Vec<String>,
) -> Fallible<Reread> {
    client::block_on(async {
        let detail = if detail {
            Some(client::get_agent(data_dir, agent).await.map_err(text)?)
        } else {
            None
        };
        let turns = if turns {
            Some(
                client::agent_results(data_dir, agent, result_limit)
                    .await
                    .map_err(text)?
                    .turns,
            )
        } else {
            None
        };
        let inputs = client::input_texts(data_dir, inputs).await;
        let docs = parse_docs(Conversation::markdown_texts(
            turns.as_deref().unwrap_or_default(),
            &[],
        ));
        Ok(Reread {
            turns,
            detail,
            inputs,
            docs,
        })
    })
}

/// Run `future`, giving up promptly when the UI cancelled this task. A
/// dropped read only closes its IPC connection; it has no side effect.
async fn cancellable<F: Future>(future: F, cancelled: &dyn Fn() -> bool) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    loop {
        tokio::select! {
            output = &mut future => return Some(output),
            () = tokio::time::sleep(Duration::from_millis(250)) => {
                if cancelled() {
                    return None;
                }
            }
        }
    }
}

/// One live step. `wake` long-polls for the first event and then drains the
/// burst; otherwise only drains. `Ok(None)` means the task was cancelled.
pub fn deliver(
    data_dir: &Path,
    agent: &str,
    after: i64,
    wake: bool,
    cancelled: &dyn Fn() -> bool,
) -> Fallible<Option<Delivery>> {
    client::block_on(async {
        let filter = client::chat_filter(agent);
        let mut events = Vec::new();
        let mut cursor = after;
        if wake {
            let Some(page) =
                cancellable(client::events_wake(data_dir, after, &filter), cancelled).await
            else {
                return Ok(None);
            };
            let page = page.map_err(text)?;
            let woke = !page.events.is_empty();
            events.extend(page.events);
            cursor = page.next;
            if page.lagged || (!woke && !page.truncated) {
                let docs = parse_docs(Conversation::markdown_texts(&[], &events));
                return Ok(Some(Delivery {
                    events,
                    next: cursor,
                    truncated: page.truncated,
                    lagged: page.lagged,
                    docs,
                }));
            }
            // Coalesce the rest of the burst into this one UI update.
            tokio::time::sleep(Duration::from_millis(COALESCE_MS)).await;
        }
        let mut truncated = false;
        for _ in 0..DRAIN_PAGES {
            let page = client::events_read(data_dir, cursor, DRAIN_LIMIT, &filter)
                .await
                .map_err(text)?;
            events.extend(page.events);
            truncated = page.truncated;
            if page.next <= cursor {
                break;
            }
            cursor = page.next;
            if !truncated {
                break;
            }
        }
        let docs = parse_docs(Conversation::markdown_texts(&[], &events));
        Ok(Some(Delivery {
            events,
            next: cursor,
            truncated,
            lagged: false,
            docs,
        }))
    })
}

/// Sleep for a reconnect backoff, giving up promptly on cancellation.
pub fn backoff(delay_ms: u64, cancelled: &dyn Fn() -> bool) -> bool {
    client::block_on(async {
        cancellable(
            tokio::time::sleep(Duration::from_millis(delay_ms)),
            cancelled,
        )
        .await
        .is_some()
    })
}
