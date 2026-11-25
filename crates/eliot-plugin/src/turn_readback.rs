//! Recover one exact input from bounded, newest-first native history.

use std::collections::HashSet;

use eliot_core::{Error, Result};

mod files;
pub use files::{find_exact_history, is_native_uuid, read_history_records};

const MAX_TURN_HISTORY_PAGES: usize = 4;

/// The adapter maps native messages and preserves the server's page order.
pub enum HistoryMessage<A, T> {
    Input(String),
    Assistant(A),
    Terminal(T),
    Other,
}

pub struct HistoryPage<A, T> {
    pub messages: Vec<HistoryMessage<A, T>>,
    pub next: Option<String>,
}

pub enum TurnHistory<A, T> {
    Running,
    Terminal(TerminalHistory<A, T>),
}

pub struct TerminalHistory<A, T> {
    pub input_position: usize,
    pub terminal_position: usize,
    pub terminal: T,
    /// Includes assistants from newer segments for native counter deduplication.
    /// Project text and usage only inside the exact terminal-to-input interval.
    pub assistants: Vec<(usize, A)>,
}

/// A terminal binds only an input in its own newest-first history segment.
/// A different input clears the candidate terminal. A missing input or
/// readback error leaves the turn unresolved.
pub async fn read_turn_history<A, T, PageRead>(
    harness: &str,
    input_id: &str,
    mut read_page: impl FnMut(Option<String>) -> PageRead,
) -> Result<TurnHistory<A, T>>
where
    PageRead: Future<Output = Result<HistoryPage<A, T>>> + Send,
{
    let mut cursor = None;
    let mut seen_cursors = HashSet::new();
    let mut terminal = None;
    let mut assistants = Vec::new();
    let mut position = 0;
    for _ in 0..MAX_TURN_HISTORY_PAGES {
        let HistoryPage { messages, next } = read_page(cursor).await?;
        if messages.is_empty() {
            break;
        }
        for message in messages {
            match message {
                HistoryMessage::Input(id) if id == input_id => {
                    let Some((terminal_position, terminal)) = terminal else {
                        return Ok(TurnHistory::Running);
                    };
                    return Ok(TurnHistory::Terminal(TerminalHistory {
                        input_position: position,
                        terminal_position,
                        terminal,
                        assistants,
                    }));
                }
                HistoryMessage::Input(_) => terminal = None,
                HistoryMessage::Assistant(message) => assistants.push((position, message)),
                HistoryMessage::Terminal(message) if terminal.is_none() => {
                    terminal = Some((position, message));
                }
                _ => {}
            }
            position += 1;
        }
        match next {
            Some(next) if !next.is_empty() && seen_cursors.insert(next.clone()) => {
                cursor = Some(next);
            }
            Some(_) => {
                return Err(Error::new(
                    "native_readback",
                    format!("{harness} message cursor did not advance"),
                ));
            }
            None => break,
        }
    }
    Err(Error::new(
        "unknown",
        format!("{harness} terminal readback found no exact user message"),
    ))
}
