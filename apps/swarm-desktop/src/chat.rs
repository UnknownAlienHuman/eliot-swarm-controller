//! Bounded conversation transcript as pure logic.
//!
//! Messages carry a stable item id and a content revision. A re-read or a
//! late reply replaces a row only when its revision is newer, so streaming
//! updates and reconnect readback cannot resurrect older text over a newer
//! draft or final. Display truncates long bodies with an explicit marker; the
//! full text stays in the retained daemon records, never in an unbounded UI
//! string.

/// Maximum message characters rendered in the transcript. Longer bodies show
/// a truncation marker; the daemon retains the full text.
pub const MAX_RENDER_CHARS: usize = 4_000;

/// Maximum messages kept per conversation in the UI. Retained history paging
/// is a later slice; the resident view stays bounded.
pub const MAX_MESSAGES: usize = 50;

/// Who produced a transcript row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    User,
    Assistant,
}

impl Role {
    pub fn label(self) -> &'static str {
        match self {
            Self::User => "You",
            Self::Assistant => "Assistant",
        }
    }
}

/// One transcript row: stable identity plus content revision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChatMessage {
    /// Stable item id (command, turn or request id from readback).
    pub key: String,
    /// Content revision; only a newer revision may replace this row.
    pub revision: u64,
    pub role: Role,
    pub text: String,
}

/// Bounded per-conversation transcript.
#[derive(Clone, Debug, Default)]
pub struct Transcript {
    messages: Vec<ChatMessage>,
}

impl Transcript {
    /// Insert or replace the row for `key`. A revision older than the stored
    /// one is ignored and reports false; same-length edits with a newer
    /// revision still apply because identity is the key, not the text.
    pub fn upsert(&mut self, key: &str, revision: u64, role: Role, text: String) -> bool {
        if let Some(row) = self.messages.iter_mut().find(|row| row.key == key) {
            if revision < row.revision {
                return false;
            }
            row.revision = revision;
            row.role = role;
            row.text = text;
            return true;
        }
        self.messages.push(ChatMessage {
            key: key.to_owned(),
            revision,
            role,
            text,
        });
        if self.messages.len() > MAX_MESSAGES {
            let overflow = self.messages.len() - MAX_MESSAGES;
            self.messages.drain(..overflow);
        }
        true
    }

    /// Rows in insertion order.
    pub fn messages(&self) -> &[ChatMessage] {
        &self.messages
    }

    /// Number of rows currently resident.
    pub fn len(&self) -> usize {
        self.messages.len()
    }
}

/// Render preview of one message body: the full text when short, a bounded
/// prefix plus an explicit truncation marker otherwise.
pub fn preview(text: &str) -> String {
    if text.chars().count() <= MAX_RENDER_CHARS {
        return text.to_owned();
    }
    let prefix: String = text.chars().take(MAX_RENDER_CHARS).collect();
    format!("{prefix}\n… [truncated for display; full text is in the daemon records]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_replaces_with_newer_revision() {
        let mut transcript = Transcript::default();
        assert!(transcript.upsert("t1", 1, Role::Assistant, "old".to_owned()));
        assert!(transcript.upsert("t1", 2, Role::Assistant, "new".to_owned()));
        assert_eq!(transcript.messages()[0].text, "new");
    }

    #[test]
    fn upsert_ignores_older_revision() {
        let mut transcript = Transcript::default();
        transcript.upsert("t1", 5, Role::Assistant, "final".to_owned());
        assert!(!transcript.upsert("t1", 4, Role::Assistant, "late".to_owned()));
        assert_eq!(transcript.messages()[0].text, "final");
    }

    #[test]
    fn upsert_applies_same_length_newer_edit() {
        let mut transcript = Transcript::default();
        transcript.upsert("t1", 1, Role::Assistant, "aaaa".to_owned());
        assert!(transcript.upsert("t1", 2, Role::Assistant, "aaab".to_owned()));
        assert_eq!(transcript.messages()[0].text, "aaab");
    }

    #[test]
    fn transcript_stays_bounded() {
        let mut transcript = Transcript::default();
        for index in 0..(MAX_MESSAGES + 10) {
            transcript.upsert(
                &format!("m{index}"),
                1,
                Role::User,
                format!("message {index}"),
            );
        }
        assert_eq!(transcript.len(), MAX_MESSAGES);
        assert_eq!(transcript.messages()[0].key, "m10");
    }

    #[test]
    fn preview_marks_truncation() {
        let long = "x".repeat(MAX_RENDER_CHARS + 1);
        let rendered = preview(&long);
        assert!(rendered.contains("[truncated for display;"));
        assert!(rendered.len() < long.len() + 128);
        assert_eq!(preview("short"), "short");
    }
}
