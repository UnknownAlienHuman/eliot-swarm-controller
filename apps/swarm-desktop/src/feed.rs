//! Background reads that feed one conversation: snapshot, coalesced
//! reread and live wake/drain. Plain blocking functions for Reactor's
//! one-shot background workers; no WinUI types, so they build everywhere.
//!
//! Snapshot recovery follows the issue's order: capture the committed cursor
//! first (`events.bounds`), then load the current reads, then read the
//! Agent's retained events up to that cursor. Live delivery continues
//! strictly after where that read stopped, so an event is either inside the
//! snapshot or delivered later, never both and never neither. Markdown is
//! parsed here, off the UI thread, into owned blocks keyed by content hash.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::Path;
use std::time::Duration;

use eliot_core::api::AgentGetResponse;
use eliot_core::{Event, Turn};

use crate::chat::{Conversation, Coverage, InputText, text_hash};
use crate::client;
use crate::live::{COALESCE_MS, DRAIN_LIMIT, DRAIN_PAGES};
use crate::markdown::{self, Doc};

/// `events.read` pages one snapshot reads at most. Each page holds at most
/// 1,000 of the Agent's own matching rows (the daemon reads them through its
/// agent index), so the work follows the Agent's history, not the journal;
/// a longer history continues through live delivery from where it stopped.
const SNAPSHOT_PAGES: usize = 64;

type Fallible<T> = eliot_core::Result<T>;

/// How many authoritative turns a snapshot reads; its journal events always
/// start at the Agent's creation (or the retention floor).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Depth {
    pub result_limit: usize,
}

impl Default for Depth {
    fn default() -> Self {
        Self {
            result_limit: crate::chat::TURN_PAGE,
        }
    }
}

impl Depth {
    /// "Load older": one more page of turns.
    pub fn older(self) -> Self {
        Self {
            result_limit: (self.result_limit + crate::chat::TURN_PAGE)
                .min(crate::chat::MAX_RESULT_LIMIT),
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
    /// The cursor this delivery read strictly after.
    pub after: i64,
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

/// Commands of the turns the resident bounds will keep: the newest
/// [`crate::chat::MAX_RESIDENT_TURNS`] turn ids by first event (the order
/// the bounds evict by), so a snapshot fetches only resident input texts
/// instead of one RPC per `turn.started` in the whole scanned window, and
/// evicted prompts never load the budget (#642).
fn resident_commands(events: &[Event]) -> Vec<String> {
    let mut first: HashMap<&str, (i64, i64)> = HashMap::new();
    for event in events {
        if let Some(turn_id) = event.data.get("turn_id").and_then(|value| value.as_str()) {
            first.entry(turn_id).or_insert((event.ts, event.seq));
        }
    }
    let mut resident: Vec<&str> = first.keys().copied().collect();
    resident.sort_by(|a, b| first[b].cmp(&first[a]));
    resident.truncate(crate::chat::MAX_RESIDENT_TURNS);
    let keep: HashSet<&str> = resident.into_iter().collect();
    let mut commands: Vec<String> = events
        .iter()
        .filter(|event| {
            event.kind == "turn.started"
                && event
                    .data
                    .get("turn_id")
                    .and_then(|value| value.as_str())
                    .is_some_and(|turn_id| keep.contains(turn_id))
        })
        .filter_map(|event| event.data.get("command_id")?.as_str().map(str::to_owned))
        .collect();
    commands.sort();
    commands.dedup();
    commands
}

/// Snapshot one Agent: cursor first, then the reads, then its events.
pub fn snapshot(data_dir: &Path, agent: &str, depth: Depth) -> Fallible<Snapshot> {
    client::block_on(async {
        let filter = client::chat_filter(agent);
        let bounds = client::events_bounds(data_dir, &filter).await?;
        let detail = client::get_agent(data_dir, agent).await?;
        let results = client::agent_results(data_dir, agent, depth.result_limit).await?;
        // Every retained event of this Agent, from its creation: Agent ids
        // are never reused, so nothing of it lies below the retention floor
        // except what retention removed. Live delivery resumes at `next`.
        let scan = client::scan_events_budgeted(
            data_dir,
            bounds.pruned_through,
            bounds.hi,
            &filter,
            SNAPSHOT_PAGES,
            &|| false,
        )
        .await?;
        let events = scan.events;
        let inputs = client::input_texts(data_dir, resident_commands(&events)).await;
        let docs = parse_docs(Conversation::markdown_texts(&results.turns, &events));
        Ok(Snapshot {
            coverage: Coverage {
                generation: results.generation,
                result_limit: depth.result_limit,
                result_returned: results.turns.len(),
                cursor: scan.next,
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
            Some(client::get_agent(data_dir, agent).await?)
        } else {
            None
        };
        let turns = if turns {
            Some(
                client::agent_results(data_dir, agent, result_limit)
                    .await?
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
            let page = page?;
            let woke = !page.events.is_empty();
            events.extend(page.events);
            cursor = page.next;
            if page.lagged || (!woke && !page.truncated) {
                let docs = parse_docs(Conversation::markdown_texts(&[], &events));
                return Ok(Some(Delivery {
                    after,
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
            let page = client::events_read(data_dir, cursor, DRAIN_LIMIT, &filter).await?;
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
            after,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn started(seq: i64, turn: &str, command: Option<&str>) -> Event {
        let mut data = serde_json::Map::new();
        data.insert("turn_id".to_owned(), json!(turn));
        if let Some(command) = command {
            data.insert("command_id".to_owned(), json!(command));
        }
        Event {
            seq,
            ts: 1_000 + seq,
            kind: "turn.started".to_owned(),
            agent: Some("a1".to_owned()),
            subject: None,
            generation: Some(1),
            source: "stream".to_owned(),
            native_id: None,
            data: serde_json::Value::Object(data),
            cause: None,
        }
    }

    #[test]
    fn snapshot_fetches_only_resident_turn_inputs() {
        // #642: a snapshot asks only for the commands of the turns the
        // resident bounds will keep, not every turn.started in the scan.
        let last = crate::chat::MAX_RESIDENT_TURNS as i64 + 5;
        let events: Vec<Event> = (0..last)
            .map(|index| started(index + 1, &format!("t{index}"), Some(&format!("c{index}"))))
            .collect();
        let commands = resident_commands(&events);
        assert_eq!(commands.len(), crate::chat::MAX_RESIDENT_TURNS);
        for dropped in 0..5 {
            assert!(!commands.contains(&format!("c{dropped}")));
        }
        assert!(commands.contains(&format!("c{}", last - 1)));
    }
}
