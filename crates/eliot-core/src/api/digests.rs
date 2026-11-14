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
use super::waits::{MAX_SWARM_WAIT_TARGETS, WaitEntryResult, WaitTurnRef};

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

/// One coalesced parent mailbox body for a settled set. Bounded to the mail
/// text limit with explicit truncation; distinct from attention reports and
/// notes that no model turn was used.
pub fn digest_text(id: &str, to: &str, mode: &str, results: &[WaitEntryResult]) -> String {
    let terminal = results.iter().filter(|entry| entry.is_terminal()).count();
    let succeeded = results.iter().filter(|entry| entry.succeeded()).count();
    let header = format!(
        "Eliot completion digest {id} for {to} (mode {mode}): {terminal}/{} terminal, {succeeded} succeeded.",
        results.len()
    );
    let footer = "Delivered once to the parent mailbox; run swarm inbox, then ack processed delivery IDs. No model turn was used. Successful completion is not an attention incident.";
    let mut body = format!("{header}\n{footer}");
    // Prepend per-entry lines while they fit; otherwise keep counts only.
    let mut lines: Vec<String> = Vec::with_capacity(results.len());
    for entry in results {
        lines.push(format!("- {} {}: {}", entry.kind, entry.id, entry.status));
    }
    // Try full body first.
    let full = format!("{header}\n{}\n{footer}", lines.join("\n"));
    if full.len() <= MAX_MAIL_TEXT_BYTES {
        return full;
    }
    // Keep as many leading entries as fit, then mark truncation explicitly.
    body = format!("{header}\n{footer}");
    let mut kept: Vec<String> = Vec::new();
    for line in lines {
        let candidate = if kept.is_empty() {
            format!("{header}\n{line}\n... truncated to the mail text bound\n{footer}")
        } else {
            format!(
                "{header}\n{}\n{line}\n... truncated to the mail text bound\n{footer}",
                kept.join("\n")
            )
        };
        if candidate.len() > MAX_MAIL_TEXT_BYTES {
            break;
        }
        kept.push(line);
        body = candidate;
        if kept.len() >= results.len() {
            break;
        }
    }
    // If even one entry line cannot fit alongside header/footer, return the
    // counts-only body (always within the bound: header/footer are small).
    if kept.is_empty() {
        let truncated = format!("{header}\n... entries truncated to the mail text bound\n{footer}");
        if truncated.len() <= MAX_MAIL_TEXT_BYTES {
            return truncated;
        }
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
