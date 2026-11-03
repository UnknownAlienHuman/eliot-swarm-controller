//! Addressed mail and shared-variable DTOs (L5.1).
//!
//! Delivery rows are per-recipient `<write-id>/<recipient-agent>` values bound
//! to the recipient generation at admission. Reads never mark state; only an
//! explicit `mail.ack` over exact delivery ids does. Variable writes carry an
//! optional compare-and-swap version; an absent row reads as version 0.

use super::mutations::nullable;
use crate::{Error, Result, parse_agent_id};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;

/// Mail body bound from the coordination contract: 32 KiB.
pub const MAX_MAIL_TEXT_BYTES: usize = 32 * 1024;
/// One admission fans out to at most this many concrete recipients.
pub const MAX_MAIL_RECIPIENTS: usize = 100;
/// Thread labels stay small enough for list output.
pub const MAX_MAIL_THREAD_BYTES: usize = 256;
/// `mail.inbox` returns at most this many rows per page.
pub const MAX_MAIL_INBOX_ROWS: usize = 100;
/// One acknowledgement names at most this many exact delivery ids.
pub const MAX_MAIL_ACK_IDS: usize = 100;
/// Encoded variable values are at most 16 KiB.
pub const MAX_VAR_VALUE_BYTES: usize = 16 * 1024;
/// Variable keys stay small enough for list output.
pub const MAX_VAR_KEY_BYTES: usize = 256;
/// Optional variable notes stay small.
pub const MAX_VAR_NOTE_BYTES: usize = 1024;
/// Variable scopes encode as `global` or `project:<name>`.
pub const MAX_VAR_SCOPE_BYTES: usize = 1024;
/// A routed recipient receives at most one generic wake notice per minute.
pub const MAIL_NOTICE_COOLDOWN_MS: i64 = 60 * 1000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MailMode {
    Queue,
    Send,
    Steer,
    Inject,
}

impl MailMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queue => "queue",
            Self::Send => "send",
            Self::Steer => "steer",
            Self::Inject => "inject",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailSendRequest {
    pub id: String,
    pub to: Vec<String>,
    pub mode: MailMode,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to_delivery: Option<String>,
}

impl MailSendRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if self.to.is_empty() || self.to.len() > MAX_MAIL_RECIPIENTS {
            return Err(Error::new(
                "invalid",
                "mail recipients require 1 to 100 Agent ids",
            ));
        }
        let mut seen = HashSet::new();
        for recipient in &self.to {
            parse_agent_id(recipient)?;
            if !seen.insert(recipient) {
                return Err(Error::new(
                    "invalid",
                    "mail recipients must not repeat an Agent",
                ));
            }
        }
        validate_mail_text(&self.text)?;
        if let Some(thread) = self.thread.as_deref() {
            validate_mail_thread(thread)?;
        }
        if let Some(reply) = self.reply_to_delivery.as_deref() {
            validate_delivery_id(reply)?;
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &MailSendResponse) -> Result<()> {
        if self.validate().is_err() || response.deliveries.len() != self.to.len() {
            return Err(unproven());
        }
        for (summary, recipient) in response.deliveries.iter().zip(self.to.iter()) {
            if summary.recipient != *recipient
                || summary.delivery_id != format!("{}/{}", self.id, recipient)
                || summary.recipient_generation < 0
            {
                return Err(unproven());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailDeliverySummary {
    pub delivery_id: String,
    pub recipient: String,
    pub recipient_generation: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailSendResponse {
    pub deliveries: Vec<MailDeliverySummary>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailDelivery {
    pub delivery_id: String,
    pub sender: String,
    pub sender_generation: Option<i64>,
    pub recipient: String,
    pub recipient_generation: i64,
    pub thread: Option<String>,
    pub reply_to_delivery: Option<String>,
    pub mode: MailMode,
    pub text: String,
    pub admitted_at: i64,
    pub presented_at: Option<i64>,
    pub presentation_command: Option<String>,
    pub read_at: Option<i64>,
}

impl MailDelivery {
    pub fn validate(&self) -> Result<()> {
        validate_delivery_id(&self.delivery_id)?;
        if self.sender != "owner" {
            parse_agent_id(&self.sender)?;
        }
        if self.sender == "owner" && self.sender_generation.is_some() {
            return Err(Error::new(
                "invalid",
                "owner mail carries no sender generation",
            ));
        }
        if self.sender != "owner"
            && self
                .sender_generation
                .is_none_or(|generation| generation < 0)
        {
            return Err(Error::new(
                "invalid",
                "Agent mail carries its sender generation",
            ));
        }
        parse_agent_id(&self.recipient)?;
        if self.recipient_generation < 0 || self.admitted_at < 0 {
            return Err(Error::new(
                "invalid",
                "mail delivery carries invalid stamps",
            ));
        }
        if let Some(thread) = self.thread.as_deref() {
            validate_mail_thread(thread)?;
        }
        if let Some(reply) = self.reply_to_delivery.as_deref() {
            validate_delivery_id(reply)?;
        }
        validate_mail_text(&self.text)?;
        if self.presented_at.is_some_and(|at| at < 0) || self.read_at.is_some_and(|at| at < 0) {
            return Err(Error::new(
                "invalid",
                "mail delivery carries invalid stamps",
            ));
        }
        if self
            .presentation_command
            .as_deref()
            .is_some_and(|command| command.trim().is_empty())
        {
            return Err(Error::new(
                "invalid",
                "mail presentation command must not be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailInboxRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
}

impl MailInboxRequest {
    pub fn effective_limit(&self) -> usize {
        self.limit.unwrap_or(MAX_MAIL_INBOX_ROWS)
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| parse_agent_id(agent).is_err())
        {
            return Err(Error::new("invalid", "mail mailbox must name an Agent"));
        }
        if !(1..=MAX_MAIL_INBOX_ROWS).contains(&self.effective_limit()) {
            return Err(Error::new(
                "invalid",
                "mail inbox limit must be between 1 and 100",
            ));
        }
        if self
            .after
            .as_deref()
            .is_some_and(|after| validate_delivery_id(after).is_err())
        {
            return Err(Error::new("invalid", "mail inbox cursor is invalid"));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &MailInboxResponse) -> Result<()> {
        if self.validate().is_err() || response.deliveries.len() > self.effective_limit() {
            return Err(unproven());
        }
        let mut recipient = None;
        for delivery in &response.deliveries {
            delivery.validate().map_err(|_| unproven())?;
            match recipient {
                None => recipient = Some(delivery.recipient.as_str()),
                Some(expected) if expected == delivery.recipient => {}
                _ => return Err(unproven()),
            }
        }
        if let Some(agent) = self.agent.as_deref()
            && recipient.is_some_and(|actual| actual != agent)
        {
            return Err(unproven());
        }
        let expected_next = response
            .deliveries
            .last()
            .map(|delivery| &delivery.delivery_id);
        if response.next.as_ref() != expected_next {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailInboxResponse {
    pub deliveries: Vec<MailDelivery>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailReadRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub delivery_id: String,
}

impl MailReadRequest {
    pub fn validate(&self) -> Result<()> {
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| parse_agent_id(agent).is_err())
        {
            return Err(Error::new("invalid", "mail mailbox must name an Agent"));
        }
        validate_delivery_id(&self.delivery_id)?;
        Ok(())
    }

    pub fn validate_response(&self, response: &MailDelivery) -> Result<()> {
        if self.validate().is_err() {
            return Err(unproven());
        }
        response.validate().map_err(|_| unproven())?;
        if response.delivery_id != self.delivery_id {
            return Err(unproven());
        }
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| agent != response.recipient)
        {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailAckRequest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub deliveries: Vec<String>,
}

impl MailAckRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| parse_agent_id(agent).is_err())
        {
            return Err(Error::new("invalid", "mail mailbox must name an Agent"));
        }
        if self.deliveries.is_empty() || self.deliveries.len() > MAX_MAIL_ACK_IDS {
            return Err(Error::new(
                "invalid",
                "mail acknowledgement requires 1 to 100 delivery ids",
            ));
        }
        let mut seen = HashSet::new();
        for delivery in &self.deliveries {
            validate_delivery_id(delivery)?;
            if !seen.insert(delivery) {
                return Err(Error::new(
                    "invalid",
                    "mail acknowledgement must not repeat a delivery",
                ));
            }
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &MailAckResponse) -> Result<()> {
        if self.validate().is_err() || response.acked != self.deliveries {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailAckResponse {
    pub acked: Vec<String>,
}

/// A parsed variable scope. Tree scopes stay `unsupported` until hierarchy
/// (L5.3) connects tree membership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarScope {
    Global,
    Project(String),
}

impl VarScope {
    pub fn encoded(&self) -> String {
        match self {
            Self::Global => "global".to_owned(),
            Self::Project(project) => format!("project:{project}"),
        }
    }

    pub fn project(&self) -> Option<&str> {
        match self {
            Self::Global => None,
            Self::Project(project) => Some(project),
        }
    }
}

pub fn parse_var_scope(scope: &str) -> Result<VarScope> {
    if scope == "global" {
        return Ok(VarScope::Global);
    }
    if scope == "tree" || scope.starts_with("tree:") {
        return Err(Error::new(
            "unsupported",
            "tree variable scopes need hierarchy membership",
        ));
    }
    if let Some(project) = scope.strip_prefix("project:") {
        if project.is_empty() || scope.len() > MAX_VAR_SCOPE_BYTES || scope.contains('\0') {
            return Err(Error::new("invalid", "project variable scope is invalid"));
        }
        return Ok(VarScope::Project(project.to_owned()));
    }
    Err(Error::new(
        "invalid",
        "variable scope must be global or project:<name>",
    ))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarSetRequest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub key: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl VarSetRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if let Some(scope) = self.scope.as_deref() {
            parse_var_scope(scope)?;
        }
        validate_var_key(&self.key)?;
        validate_var_value(&self.value)?;
        if self.expect_version.is_some_and(|version| version < 0) {
            return Err(Error::new(
                "invalid",
                "expected variable version must be zero or positive",
            ));
        }
        if let Some(note) = self.note.as_deref() {
            validate_var_note(note)?;
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &VarSetResponse) -> Result<()> {
        if self.validate().is_err() {
            return Err(unproven());
        }
        if self
            .scope
            .as_deref()
            .is_some_and(|scope| scope != response.scope)
            || response.key != self.key
            || response.version < 1
            || response.set_by.trim().is_empty()
        {
            return Err(unproven());
        }
        parse_var_scope(&response.scope).map_err(|_| unproven())?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarSetResponse {
    pub scope: String,
    pub key: String,
    pub version: i64,
    pub set_by: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarGetRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub key: String,
}

impl VarGetRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(scope) = self.scope.as_deref() {
            parse_var_scope(scope)?;
        }
        validate_var_key(&self.key)?;
        Ok(())
    }

    pub fn validate_response(&self, response: &VarGetResponse) -> Result<()> {
        if self.validate().is_err() {
            return Err(unproven());
        }
        if self
            .scope
            .as_deref()
            .is_some_and(|scope| scope != response.scope)
            || response.key != self.key
            || response.version < 1
            || response.set_by.trim().is_empty()
            || response.set_at < 0
        {
            return Err(unproven());
        }
        parse_var_scope(&response.scope).map_err(|_| unproven())?;
        validate_var_value(&response.value).map_err(|_| unproven())?;
        if response
            .note
            .as_deref()
            .is_some_and(|note| validate_var_note(note).is_err())
        {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarGetResponse {
    pub scope: String,
    pub key: String,
    pub value: Value,
    pub version: i64,
    pub set_by: String,
    pub note: Option<String>,
    pub set_at: i64,
}

fn validate_write_id(id: &str) -> Result<()> {
    if id.trim().is_empty() {
        return Err(Error::new("invalid", "mail write id must not be empty"));
    }
    Ok(())
}

fn validate_mail_text(text: &str) -> Result<()> {
    if text.len() > MAX_MAIL_TEXT_BYTES {
        return Err(Error::new("invalid", "mail text exceeds the 32 KiB bound"));
    }
    if text.contains('\0') {
        return Err(Error::new("invalid", "mail text must not contain NUL"));
    }
    Ok(())
}

fn validate_mail_thread(thread: &str) -> Result<()> {
    if thread.trim().is_empty()
        || thread.len() > MAX_MAIL_THREAD_BYTES
        || thread.chars().any(char::is_control)
    {
        return Err(Error::new("invalid", "mail thread label is invalid"));
    }
    Ok(())
}

fn validate_delivery_id(delivery: &str) -> Result<()> {
    let Some((head, tail)) = delivery.split_once('/') else {
        return Err(Error::new(
            "invalid",
            "mail delivery id must use the <write-id>/<recipient> form",
        ));
    };
    if head.trim().is_empty() || delivery.len() > 512 || delivery.contains('\0') {
        return Err(Error::new("invalid", "mail delivery id is invalid"));
    }
    parse_agent_id(tail)?;
    Ok(())
}

fn validate_var_key(key: &str) -> Result<()> {
    if key.trim().is_empty() || key.len() > MAX_VAR_KEY_BYTES || key.contains('\0') {
        return Err(Error::new("invalid", "variable key is invalid"));
    }
    Ok(())
}

fn validate_var_value(value: &Value) -> Result<()> {
    let encoded = serde_json::to_string(value)
        .map_err(|_| Error::new("invalid", "variable value is invalid"))?;
    if encoded.len() > MAX_VAR_VALUE_BYTES {
        return Err(Error::new(
            "invalid",
            "variable value exceeds the 16 KiB bound",
        ));
    }
    Ok(())
}

fn validate_var_note(note: &str) -> Result<()> {
    if note.len() > MAX_VAR_NOTE_BYTES || note.contains('\0') {
        return Err(Error::new("invalid", "variable note is invalid"));
    }
    Ok(())
}

fn unproven() -> Error {
    Error::new(
        "unknown",
        "coordination response does not match its requested binding",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn send_request() -> MailSendRequest {
        MailSendRequest {
            id: "01J0000000000000000000000".to_owned(),
            to: vec!["a1".to_owned(), "a2".to_owned()],
            mode: MailMode::Queue,
            text: "hello".to_owned(),
            thread: None,
            reply_to_delivery: None,
        }
    }

    fn delivery(recipient: &str) -> MailDelivery {
        MailDelivery {
            delivery_id: format!("01J0000000000000000000000/{recipient}"),
            sender: "a9".to_owned(),
            sender_generation: Some(1),
            recipient: recipient.to_owned(),
            recipient_generation: 1,
            thread: None,
            reply_to_delivery: None,
            mode: MailMode::Queue,
            text: "hello".to_owned(),
            admitted_at: 7,
            presented_at: None,
            presentation_command: None,
            read_at: None,
        }
    }

    #[test]
    fn mail_send_validates_recipients_text_and_bindings() {
        let request = send_request();
        request.validate().unwrap();
        let response = MailSendResponse {
            deliveries: vec![
                MailDeliverySummary {
                    delivery_id: "01J0000000000000000000000/a1".to_owned(),
                    recipient: "a1".to_owned(),
                    recipient_generation: 1,
                },
                MailDeliverySummary {
                    delivery_id: "01J0000000000000000000000/a2".to_owned(),
                    recipient: "a2".to_owned(),
                    recipient_generation: 0,
                },
            ],
        };
        request.validate_response(&response).unwrap();

        for to in [
            Vec::new(),
            vec!["a1".to_owned(), "a1".to_owned()],
            vec!["a0".to_owned()],
            vec!["1".to_owned()],
            vec!["a1".to_owned(); 101],
        ] {
            assert!(
                MailSendRequest {
                    to,
                    ..send_request()
                }
                .validate()
                .is_err(),
                "bad recipient list accepted"
            );
        }
        assert!(
            MailSendRequest {
                text: "x".repeat(MAX_MAIL_TEXT_BYTES + 1),
                ..send_request()
            }
            .validate()
            .is_err()
        );
        assert!(
            MailSendRequest {
                text: "x\0y".to_owned(),
                ..send_request()
            }
            .validate()
            .is_err()
        );
        // Empty bodies are allowed; the unread-count bound covers them.
        MailSendRequest {
            text: String::new(),
            ..send_request()
        }
        .validate()
        .unwrap();
        assert!(
            MailSendRequest {
                thread: Some("has\ttab".to_owned()),
                ..send_request()
            }
            .validate()
            .is_err()
        );
        assert!(
            MailSendRequest {
                reply_to_delivery: Some("not-a-delivery".to_owned()),
                ..send_request()
            }
            .validate()
            .is_err()
        );
        assert!(
            request
                .validate_response(&MailSendResponse { deliveries: vec![] })
                .is_err()
        );
        assert!(serde_json::from_value::<MailSendRequest>(json!({"unexpected": 1})).is_err());
        assert!(
            serde_json::from_value::<MailSendRequest>(json!({
                "id": "01J0000000000000000000000", "to": ["a1"], "mode": "broadcast",
                "text": "hi",
            }))
            .is_err()
        );
        assert_eq!(
            serde_json::from_value::<MailMode>(json!("queue")).unwrap(),
            MailMode::Queue
        );
    }

    #[test]
    fn inbox_pages_stay_within_one_mailbox_and_point_at_their_tail() {
        let request = MailInboxRequest {
            agent: Some("a1".to_owned()),
            limit: Some(2),
            after: None,
        };
        request.validate().unwrap();
        request
            .validate_response(&MailInboxResponse {
                deliveries: vec![delivery("a1")],
                next: Some("01J0000000000000000000000/a1".to_owned()),
            })
            .unwrap();
        MailInboxRequest::default()
            .validate_response(&MailInboxResponse {
                deliveries: vec![],
                next: None,
            })
            .unwrap();
        assert_eq!(MailInboxRequest::default().effective_limit(), 100);

        for request in [
            MailInboxRequest {
                agent: Some("nope".to_owned()),
                ..Default::default()
            },
            MailInboxRequest {
                limit: Some(0),
                ..Default::default()
            },
            MailInboxRequest {
                limit: Some(101),
                ..Default::default()
            },
            MailInboxRequest {
                after: Some("not-a-delivery".to_owned()),
                ..Default::default()
            },
        ] {
            assert!(request.validate().is_err());
        }
        // A page mixing mailboxes, or pointing past its tail, is unproven.
        assert!(
            MailInboxRequest::default()
                .validate_response(&MailInboxResponse {
                    deliveries: vec![delivery("a1"), delivery("a2")],
                    next: Some("01J0000000000000000000000/a2".to_owned()),
                })
                .is_err()
        );
        assert!(
            request
                .validate_response(&MailInboxResponse {
                    deliveries: vec![delivery("a1")],
                    next: None,
                })
                .is_err()
        );
        assert!(
            request
                .validate_response(&MailInboxResponse {
                    deliveries: vec![delivery("a2")],
                    next: Some("01J0000000000000000000000/a2".to_owned()),
                })
                .is_err()
        );
    }

    #[test]
    fn read_and_ack_bind_exact_delivery_ids() {
        let read = MailReadRequest {
            agent: Some("a1".to_owned()),
            delivery_id: "01J0000000000000000000000/a1".to_owned(),
        };
        read.validate().unwrap();
        read.validate_response(&delivery("a1")).unwrap();
        assert!(read.validate_response(&delivery("a2")).is_err());

        let ack = MailAckRequest {
            id: "01J0000000000000000000001".to_owned(),
            agent: Some("a1".to_owned()),
            deliveries: vec!["01J0000000000000000000000/a1".to_owned()],
        };
        ack.validate().unwrap();
        ack.validate_response(&MailAckResponse {
            acked: ack.deliveries.clone(),
        })
        .unwrap();
        assert!(
            ack.validate_response(&MailAckResponse { acked: vec![] })
                .is_err()
        );
        assert!(
            MailAckRequest {
                deliveries: vec![],
                ..ack.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            MailAckRequest {
                deliveries: vec![
                    "01J0000000000000000000000/a1".to_owned(),
                    "01J0000000000000000000000/a1".to_owned(),
                ],
                ..ack.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            MailAckRequest {
                agent: Some("a1".to_owned()),
                ..MailAckRequest {
                    id: "  ".to_owned(),
                    agent: None,
                    deliveries: ack.deliveries.clone(),
                }
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn delivery_rejects_owner_generation_and_bad_shapes() {
        delivery("a1").validate().unwrap();
        let mut owner = delivery("a1");
        owner.sender = "owner".to_owned();
        owner.sender_generation = None;
        owner.validate().unwrap();
        owner.sender_generation = Some(1);
        assert!(owner.validate().is_err());
        let mut agent = delivery("a1");
        agent.sender_generation = None;
        assert!(agent.validate().is_err());
        let mut bad = delivery("a1");
        bad.delivery_id = "bare-id".to_owned();
        assert!(bad.validate().is_err());
    }

    #[test]
    fn var_scopes_and_values_stay_bounded() {
        assert_eq!(parse_var_scope("global").unwrap(), VarScope::Global);
        assert_eq!(
            parse_var_scope("project:app").unwrap(),
            VarScope::Project("app".to_owned())
        );
        assert_eq!(VarScope::Global.encoded(), "global");
        assert_eq!(VarScope::Project("app".to_owned()).encoded(), "project:app");
        assert_eq!(
            parse_var_scope("tree:root").unwrap_err().code,
            "unsupported"
        );
        assert!(parse_var_scope("project:").is_err());
        assert!(parse_var_scope("other").is_err());

        let set = VarSetRequest {
            id: "01J0000000000000000000002".to_owned(),
            scope: Some("project:app".to_owned()),
            key: "branch".to_owned(),
            value: json!("main"),
            expect_version: Some(0),
            note: None,
        };
        set.validate().unwrap();
        set.validate_response(&VarSetResponse {
            scope: "project:app".to_owned(),
            key: "branch".to_owned(),
            version: 1,
            set_by: "agent:a1:1".to_owned(),
        })
        .unwrap();
        assert!(
            set.validate_response(&VarSetResponse {
                scope: "project:other".to_owned(),
                key: "branch".to_owned(),
                version: 1,
                set_by: "agent:a1:1".to_owned(),
            })
            .is_err()
        );
        // An omitted scope binds nothing until the Router resolves the caller.
        let unscoped = VarSetRequest {
            scope: None,
            ..set.clone()
        };
        unscoped.validate().unwrap();
        unscoped
            .validate_response(&VarSetResponse {
                scope: "project:app".to_owned(),
                key: "branch".to_owned(),
                version: 1,
                set_by: "agent:a1:1".to_owned(),
            })
            .unwrap();

        assert!(
            VarSetRequest {
                expect_version: Some(-1),
                ..set.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            VarSetRequest {
                key: "  ".to_owned(),
                ..set.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            VarSetRequest {
                value: json!("x".repeat(MAX_VAR_VALUE_BYTES)),
                ..set.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            VarSetRequest {
                note: Some("n".repeat(MAX_VAR_NOTE_BYTES + 1)),
                ..set.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            VarSetRequest {
                scope: Some("tree:root".to_owned()),
                ..set.clone()
            }
            .validate()
            .is_err()
        );

        let get = VarGetRequest {
            scope: Some("global".to_owned()),
            key: "flag".to_owned(),
        };
        get.validate().unwrap();
        get.validate_response(&VarGetResponse {
            scope: "global".to_owned(),
            key: "flag".to_owned(),
            value: json!({"on": true}),
            version: 3,
            set_by: "owner".to_owned(),
            note: Some("why".to_owned()),
            set_at: 9,
        })
        .unwrap();
        assert!(
            get.validate_response(&VarGetResponse {
                scope: "global".to_owned(),
                key: "other".to_owned(),
                value: json!(true),
                version: 3,
                set_by: "owner".to_owned(),
                note: None,
                set_at: 9,
            })
            .is_err()
        );
        assert!(serde_json::from_value::<VarSetRequest>(json!({"unexpected": 1})).is_err());
    }
}
