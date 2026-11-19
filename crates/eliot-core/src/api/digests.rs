//! Durable completion digest over a frozen child command/turn set.
//!
//! A digest registers one parent mailbox recipient for a captured set. When
//! the set satisfies its `all`/`any` mode the daemon delivers exactly one
//! coalesced `mail.send` (queue) result to the parent. The digest mail id is
//! the registration id, so a reconnect or restart replays the same mail id
//! and the existing mail stable effect identity makes the retry a no-op.
//! The digest thread (`completion-*`) is ordinary queued mail: it wakes
//! through the standard mail notice gate and never emits an attention
//! incident.

use crate::{Error, Result, parse_agent_id};
use serde::{Deserialize, Serialize};

use super::coordination::MAX_MAIL_TEXT_BYTES;
use super::waits::{MAX_SWARM_WAIT_TARGETS, WaitEntryResult, WaitTurnRef, truncate_bytes};

/// Thread prefix for completion digests. Distinct from `attention-*` so a
/// successful completion is never confused with an attention incident, and
/// never suffixed `-advisory` so it proves an ordinary mail wake.
pub const DIGEST_THREAD_PREFIX: &str = "completion-";

/// One durable digest registration over an explicitly captured child set.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmDigestRequest {
    #[schemars(
        description = "Nonblank caller-retained mutation id; reuse it to replay the same digest."
    )]
    pub id: String,
    #[schemars(
        description = "Concrete canonical a<positive i64> parent Agent mailbox for the one coalesced result."
    )]
    pub to: String,
    #[schemars(description = "Wait mode: all or any.")]
    pub mode: String,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub turns: Vec<WaitTurnRef>,
}

impl SwarmDigestRequest {
    pub fn new(
        id: impl Into<String>,
        to: impl Into<String>,
        mode: impl Into<String>,
        commands: Vec<String>,
        turns: Vec<WaitTurnRef>,
    ) -> Self {
        Self {
            id: id.into(),
            to: to.into(),
            mode: mode.into(),
            commands,
            turns,
        }
    }

    pub fn total(&self) -> usize {
        self.commands.len().saturating_add(self.turns.len())
    }

    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(Error::new("invalid", "digest id must be non-empty"));
        }
        parse_agent_id(&self.to)
            .map_err(|_| Error::new("invalid", "digest recipient must be a concrete Agent id"))?;
        if self.mode != "all" && self.mode != "any" {
            return Err(Error::new("invalid", "mode must be all or any"));
        }
        let total = self.total();
        if total == 0 || total > MAX_SWARM_WAIT_TARGETS {
            return Err(Error::new(
                "invalid",
                "digest requires 1..=256 captured entries",
            ));
        }
        for id in &self.commands {
            if id.trim().is_empty() {
                return Err(Error::new(
                    "invalid",
                    "digest identifiers must be non-empty strings",
                ));
            }
        }
        for turn in &self.turns {
            turn.validate()?;
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &SwarmDigestResponse) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.to != self.to
            || response.mode != self.mode
            || response.results.len() != self.total()
        {
            return Err(binding_mismatch());
        }
        if response.state != "pending" && response.state != "delivered" {
            return Err(binding_mismatch());
        }
        if response.state == "delivered" {
            if response.delivery_id.as_deref() != Some(&format!("{}/{}", self.id, self.to)) {
                return Err(binding_mismatch());
            }
            if !response.completed {
                return Err(binding_mismatch());
            }
        } else if response.delivery_id.is_some() {
            return Err(binding_mismatch());
        }
        let mut expected: Vec<(String, String)> = Vec::new();
        for id in &self.commands {
            expected.push(("command".to_owned(), id.clone()));
        }
        for turn in &self.turns {
            expected.push(("turn".to_owned(), turn.canonical_id()));
        }
        for (result, (kind, id)) in response.results.iter().zip(expected.iter()) {
            if &result.kind != kind || &result.id != id || result.validate().is_err() {
                return Err(binding_mismatch());
            }
        }
        let should_complete = digest_mode_completed(self.mode.as_str(), &response.results);
        if response.completed != should_complete {
            return Err(binding_mismatch());
        }
        if response.state == "delivered" && !response.completed {
            return Err(binding_mismatch());
        }
        Ok(())
    }
}

/// Durable digest result. `completed` mirrors the requested mode condition
/// over terminal entries; `state` is `delivered` only after the one parent
/// mailbox mail is retained.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmDigestResponse {
    pub id: String,
    pub to: String,
    pub mode: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<String>,
    pub completed: bool,
    pub results: Vec<WaitEntryResult>,
}

impl SwarmDigestResponse {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(Error::new("invalid", "digest id must be non-empty"));
        }
        parse_agent_id(&self.to)
            .map_err(|_| Error::new("invalid", "digest recipient must be a concrete Agent id"))?;
        if self.mode != "all" && self.mode != "any" {
            return Err(Error::new("invalid", "mode must be all or any"));
        }
        if self.state != "pending" && self.state != "delivered" {
            return Err(Error::new(
                "invalid",
                "digest state must be pending or delivered",
            ));
        }
        if self.state == "delivered" {
            if self.delivery_id.as_deref() != Some(&format!("{}/{}", self.id, self.to)) {
                return Err(Error::new("invalid", "digest delivery id is invalid"));
            }
            if !self.completed {
                return Err(Error::new(
                    "invalid",
                    "a delivered digest must be completed",
                ));
            }
        } else if self.delivery_id.is_some() {
            return Err(Error::new(
                "invalid",
                "a pending digest carries no delivery id",
            ));
        }
        if self.results.len() > MAX_SWARM_WAIT_TARGETS {
            return Err(Error::new(
                "invalid",
                "digest response exceeds the entry bound",
            ));
        }
        for entry in &self.results {
            entry.validate()?;
        }
        Ok(())
    }
}

/// Whether the mode condition holds over terminal entries (`still_running`
/// is the only non-terminal status; `not_found`/`not_visible` count as
/// settled honest outcomes).
pub fn digest_mode_completed(mode: &str, results: &[WaitEntryResult]) -> bool {
    match mode {
        "all" => results.iter().all(WaitEntryResult::is_terminal),
        _ => results.iter().any(WaitEntryResult::is_terminal),
    }
}

/// Stable digest mail thread for one registration id. Unique per digest so
/// each digest wakes once; never an `attention-*` thread.
pub fn digest_thread(id: &str) -> String {
    let prefix: String = id.chars().take(20).collect();
    format!("{DIGEST_THREAD_PREFIX}{prefix}")
}

fn escape_mail_text(text: &str) -> std::borrow::Cow<'_, str> {
    if text.contains('\0') {
        std::borrow::Cow::Owned(text.replace('\0', "\\0"))
    } else {
        std::borrow::Cow::Borrowed(text)
    }
}

/// One status line naming the entry's turn identity once. A turn entry's id
/// already is `agent/generation/turn_id`, so only a command entry adds its
/// bound `agent` and `turn_id`.
fn digest_entry_line(entry: &WaitEntryResult, truncated: bool) -> String {
    let id = escape_mail_text(&entry.id);
    let (Some(agent), Some(turn_id)) = (entry.agent.as_deref(), entry.turn_id.as_deref()) else {
        return format!("- {} {id}: {}", entry.kind, entry.status);
    };
    let flag = (entry.final_text.is_some() || truncated).then(|| format!("truncated={truncated}"));
    let detail = if entry.kind == "turn" {
        flag
    } else {
        let identity = format!(
            "agent {}, turn_id {}",
            escape_mail_text(agent),
            escape_mail_text(turn_id)
        );
        Some(match flag {
            Some(flag) => format!("{identity}, {flag}"),
            None => identity,
        })
    };
    match detail {
        Some(detail) => format!("- {} {id}: {} ({detail})", entry.kind, entry.status),
        None => format!("- {} {id}: {}", entry.kind, entry.status),
    }
}

/// Excerpt share of the mail bound reserved across one digest batch: status
/// lines never take it while a rendered entry carries an answer, so a full
/// batch of long identities still delivers excerpts. Lines beyond the rest
/// are cut with an explicit count.
const DIGEST_EXCERPT_RESERVE_BYTES: usize = MAX_MAIL_TEXT_BYTES / 4;

fn digest_cut_marker(count: usize) -> String {
    format!("... {count} more entries truncated to the mail text bound")
}

/// Fair water-fill per-entry byte cap across `lengths` within `usable` bytes.
pub fn fair_excerpt_cap(lengths: &[usize], usable: usize) -> usize {
    let mut sorted = lengths.to_vec();
    sorted.sort_unstable();
    let mut rem = usable;
    for (pos, &len) in sorted.iter().enumerate() {
        let left = sorted.len() - pos;
        if len.saturating_mul(left) <= rem {
            rem -= len;
        } else {
            return rem / left;
        }
    }
    usize::MAX
}

/// One coalesced parent mailbox body for a settled set. Includes bounded
/// `final_text` excerpts (each status line naming its turn identity once,
/// plus `truncated`) within the mail text bound, distinct from attention
/// reports and noting that no model turn was used.
///
/// Budget: header, footer and every status line come first, except that
/// [`DIGEST_EXCERPT_RESERVE_BYTES`] (or the whole answer demand when smaller)
/// stays reserved for excerpts across the batch. Lines that do not fit beside
/// that reserve are cut from the tail with an explicit count; the kept
/// entries then share every remaining byte by fair water-fill.
pub fn digest_text(id: &str, to: &str, mode: &str, results: &[WaitEntryResult]) -> String {
    let id = escape_mail_text(id);
    let to = escape_mail_text(to);
    let terminal = results.iter().filter(|entry| entry.is_terminal()).count();
    let succeeded = results.iter().filter(|entry| entry.succeeded()).count();
    let header = format!(
        "Eliot completion digest {id} for {to} (mode {mode}): {terminal}/{} terminal, {succeeded} succeeded.",
        results.len()
    );
    let footer = "Delivered once to the parent mailbox. No model turn was used. Successful completion is not an attention incident.";
    let texts: Vec<Option<std::borrow::Cow<'_, str>>> = results
        .iter()
        .map(|entry| {
            entry
                .final_text
                .as_deref()
                .filter(|text| !text.is_empty())
                .map(escape_mail_text)
        })
        .collect();
    // An entry with an answer budgets its longer `truncated=false` line.
    let line_lens: Vec<usize> = results
        .iter()
        .zip(&texts)
        .map(|(entry, text)| digest_entry_line(entry, text.is_none() && entry.truncated).len())
        .collect();
    let demand: usize = texts.iter().flatten().map(|text| text.len() + 1).sum();
    let reserve = demand.min(DIGEST_EXCERPT_RESERVE_BYTES);
    let fixed = header.len() + 1 + footer.len();
    let all_lines: usize = line_lens.iter().map(|len| len + 1).sum();
    let kept = if fixed + all_lines + reserve <= MAX_MAIL_TEXT_BYTES {
        results.len()
    } else {
        let marker = digest_cut_marker(results.len()).len() + 1;
        let budget = MAX_MAIL_TEXT_BYTES.saturating_sub(fixed + marker + reserve);
        let mut used = 0;
        line_lens
            .iter()
            .take_while(|len| {
                used += *len + 1;
                used <= budget
            })
            .count()
    };
    let marker = (kept < results.len()).then(|| digest_cut_marker(results.len() - kept));
    let used = fixed
        + line_lens[..kept].iter().map(|len| len + 1).sum::<usize>()
        + marker.as_ref().map_or(0, |marker| marker.len() + 1);
    let available = MAX_MAIL_TEXT_BYTES.saturating_sub(used);
    let text_lens: Vec<usize> = texts[..kept]
        .iter()
        .flatten()
        .map(|text| text.len())
        .collect();
    let cap = if text_lens.is_empty() {
        usize::MAX
    } else if available <= text_lens.len() {
        0
    } else {
        fair_excerpt_cap(&text_lens, available - text_lens.len())
    };
    let mut body = format!("{header}\n");
    for (entry, text) in results[..kept].iter().zip(&texts[..kept]) {
        match text.as_deref() {
            Some(text) => {
                let (excerpt, cut) = truncate_bytes(text, cap);
                body.push_str(&digest_entry_line(entry, entry.truncated || cut));
                if !excerpt.is_empty() {
                    body.push('\n');
                    body.push_str(&excerpt);
                }
            }
            None => body.push_str(&digest_entry_line(entry, entry.truncated)),
        }
        body.push('\n');
    }
    if let Some(marker) = marker {
        body.push_str(&marker);
        body.push('\n');
    }
    body.push_str(footer);
    // Only an oversized digest id or recipient can push the frame itself
    // past the bound; the counts header is then the whole body.
    if body.len() > MAX_MAIL_TEXT_BYTES {
        return header;
    }
    body
}

fn binding_mismatch() -> Error {
    Error::new(
        "unknown",
        "digest response does not match the requested binding",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::WaitEntryResult;
    use serde_json::json;

    fn request() -> SwarmDigestRequest {
        SwarmDigestRequest::new(
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "a3",
            "all",
            vec!["c1".to_owned()],
            vec![WaitTurnRef::new("a1", 1, "t1")],
        )
    }

    #[test]
    fn digest_rejects_bad_shape_and_bounds() {
        let mut bad = request();
        bad.to = "owner".to_owned();
        assert!(bad.validate().is_err());
        bad = request();
        bad.mode = "first".to_owned();
        assert!(bad.validate().is_err());
        bad = request();
        bad.commands.clear();
        bad.turns.clear();
        assert!(bad.validate().is_err());
        bad = request();
        bad.id = "  ".to_owned();
        assert!(bad.validate().is_err());
    }

    #[test]
    fn digest_response_binds_exact_order_and_delivery() {
        let req = request();
        let good = SwarmDigestResponse {
            id: req.id.clone(),
            to: req.to.clone(),
            mode: req.mode.clone(),
            state: "delivered".to_owned(),
            delivery_id: Some(format!("{}/{}", req.id, req.to)),
            completed: true,
            results: vec![
                WaitEntryResult::new("command", "c1", "succeeded"),
                WaitEntryResult::new("turn", "a1/1/t1", "failed"),
            ],
        };
        req.validate_response(&good).unwrap();
        let mut pending = good.clone();
        pending.state = "pending".to_owned();
        assert!(req.validate_response(&pending).is_err());
        let mut swapped = good.clone();
        swapped.results.swap(0, 1);
        assert!(req.validate_response(&swapped).is_err());
        let mut incomplete = good;
        incomplete.results[1] = WaitEntryResult::new("turn", "a1/1/t1", "still_running");
        incomplete.completed = false;
        incomplete.state = "pending".to_owned();
        incomplete.delivery_id = None;
        req.validate_response(&incomplete).unwrap();
    }

    #[test]
    fn digest_text_coalesces_all_entries_in_one_mail_body() {
        let results = vec![
            WaitEntryResult::new("command", "c1", "succeeded"),
            WaitEntryResult::new("turn", "a1/1/t1", "failed"),
        ];
        let text = digest_text("D1", "a3", "all", &results);
        assert!(text.starts_with("Eliot completion digest D1 for a3"));
        assert!(text.contains("- command c1: succeeded"));
        assert!(text.contains("- turn a1/1/t1: failed"));
        assert!(text.contains("not an attention incident"));
        assert!(!text.contains("agent.attention"));
        assert!(!text.contains("attention-"));
        assert!(text.len() <= MAX_MAIL_TEXT_BYTES);
    }

    #[test]
    fn digest_text_truncates_explicitly_within_mail_bound() {
        let results: Vec<WaitEntryResult> = (0..256)
            .map(|i| {
                WaitEntryResult::new("command", format!("c{i}-{}", "x".repeat(200)), "succeeded")
            })
            .collect();
        let text = digest_text("D1", "a3", "all", &results);
        assert!(text.len() <= MAX_MAIL_TEXT_BYTES);
        assert!(text.contains("truncated to the mail text bound"));
    }

    #[test]
    fn digest_text_includes_bounded_final_text_excerpts_fairly() {
        use crate::api::MAX_WAIT_FINAL_TEXT_BYTES;

        let short = WaitEntryResult::new("command", "c1", "succeeded")
            .with_turn("a1", 1, "t1")
            .with_final_text("answer one", MAX_WAIT_FINAL_TEXT_BYTES);
        let big_two = WaitEntryResult::new("command", "c2", "succeeded")
            .with_turn("a2", 1, "t2")
            .with_final_text(
                &"é".repeat(MAX_WAIT_FINAL_TEXT_BYTES / 2),
                MAX_WAIT_FINAL_TEXT_BYTES,
            );
        let big_three = WaitEntryResult::new("command", "c3", "succeeded")
            .with_turn("a3", 1, "t3")
            .with_final_text(
                &"z".repeat(MAX_WAIT_FINAL_TEXT_BYTES),
                MAX_WAIT_FINAL_TEXT_BYTES,
            );
        let withheld = WaitEntryResult::new("turn", "a4/1/t4", "succeeded")
            .with_turn("a4", 1, "t4")
            .with_final_text("ignored", 0);

        let text = digest_text("D1", "a0", "all", &[short, big_two, big_three, withheld]);
        assert!(text.len() <= MAX_MAIL_TEXT_BYTES);
        assert!(text.contains(
            "- command c1: succeeded (agent a1, turn_id t1, truncated=false)\nanswer one"
        ));
        assert!(text.contains("- command c2: succeeded (agent a2, turn_id t2, truncated=true)"));
        assert!(text.contains("- command c3: succeeded (agent a3, turn_id t3, truncated=true)"));
        assert!(text.contains("zzzz"));
        assert!(text.contains("- turn a4/1/t4: succeeded (truncated=true)"));
    }

    #[test]
    fn digest_text_escapes_nul_before_budgeting_and_validates_as_mail() {
        use crate::api::{MAX_WAIT_FINAL_TEXT_BYTES, MailMode, MailSendRequest};

        let nul_flood = WaitEntryResult::new("command", "c1", "succeeded")
            .with_turn("a1", 1, "t1")
            .with_final_text(
                &"\0".repeat(MAX_WAIT_FINAL_TEXT_BYTES),
                MAX_WAIT_FINAL_TEXT_BYTES,
            );
        let with_nul = WaitEntryResult::new("command", "c2", "succeeded")
            .with_turn("a2", 1, "t2")
            .with_final_text("before\0after", MAX_WAIT_FINAL_TEXT_BYTES);

        let text = digest_text("D1", "a1", "all", &[nul_flood, with_nul]);
        assert!(!text.contains('\0'));
        assert!(text.len() <= MAX_MAIL_TEXT_BYTES);
        assert!(text.contains("before\\0after"));
        assert!(text.contains("\\0"));

        let mail = MailSendRequest {
            id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            to: vec!["a1".to_owned()],
            mode: MailMode::Queue,
            text,
            thread: Some(digest_thread("01ARZ3NDEKTSV4RRFFQ69G5FAV")),
            reply_to_delivery: None,
        };
        mail.validate().unwrap();
    }

    fn uuid_turn(i: usize) -> String {
        format!("{i:08x}-0000-4000-8000-{i:012x}")
    }

    fn long_answer(i: usize) -> String {
        format!("answer {i:03} {}", "y".repeat(200))
    }

    #[test]
    fn digest_text_full_turn_batch_renders_identity_once_and_keeps_every_answer() {
        use crate::api::MAX_WAIT_FINAL_TEXT_BYTES;

        let results: Vec<WaitEntryResult> = (0..MAX_SWARM_WAIT_TARGETS)
            .map(|i| {
                let agent = format!("a{}", 1_000_000 + i);
                let turn_id = uuid_turn(i);
                WaitEntryResult::new("turn", format!("{agent}/1/{turn_id}"), "succeeded")
                    .with_turn(agent, 1, turn_id)
                    .with_final_text(&long_answer(i), MAX_WAIT_FINAL_TEXT_BYTES)
            })
            .collect();
        let text = digest_text("D1", "a1", "all", &results);
        assert!(text.len() <= MAX_MAIL_TEXT_BYTES);
        assert!(!text.contains("truncated to the mail text bound"));
        for i in 0..MAX_SWARM_WAIT_TARGETS {
            assert_eq!(text.matches(&uuid_turn(i)).count(), 1, "turn {i}");
            assert!(text.contains(&format!("answer {i:03}")), "answer {i}");
        }
    }

    #[test]
    fn digest_text_reserves_excerpt_space_when_status_lines_overflow() {
        use crate::api::MAX_WAIT_FINAL_TEXT_BYTES;

        let results: Vec<WaitEntryResult> = (0..MAX_SWARM_WAIT_TARGETS)
            .map(|i| {
                let turn_id = uuid_turn(i);
                WaitEntryResult::new(
                    "command",
                    format!("01ARZ3NDEKTSV4RRFFQ69G{i:04}"),
                    "succeeded",
                )
                .with_turn(format!("a{}", 1_000_000 + i), 1, turn_id)
                .with_final_text(&long_answer(i), MAX_WAIT_FINAL_TEXT_BYTES)
            })
            .collect();
        let text = digest_text("D1", "a1", "all", &results);
        assert!(text.len() <= MAX_MAIL_TEXT_BYTES);
        let kept = text.matches("\n- command ").count();
        assert!(kept > 0 && kept < MAX_SWARM_WAIT_TARGETS);
        assert!(text.contains(&format!(
            "... {} more entries truncated to the mail text bound",
            MAX_SWARM_WAIT_TARGETS - kept
        )));
        for i in 0..kept {
            assert!(text.contains(&format!("answer {i:03}")), "answer {i}");
        }
    }

    #[test]
    fn digest_decoding_rejects_unknown_fields() {
        assert!(
            serde_json::from_value::<SwarmDigestRequest>(json!({
                "id": "D1", "to": "a3", "mode": "all",
                "commands": ["c1"], "turns": [], "extra": 0
            }))
            .is_err()
        );
    }
}
