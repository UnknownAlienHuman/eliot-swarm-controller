//! Addressed mail and shared-variable DTOs (L5.1).
//!
//! Delivery rows are per-recipient `<write-id>/<recipient-agent>` values bound
//! to the recipient generation at admission. Reads never mark state; only an
//! explicit `mail.ack` over exact delivery ids does. Variable writes carry an
//! optional compare-and-swap version; an absent row reads as version 0.

use super::mutations::nullable;
use crate::{AgentState, Error, GitCommit, Result, parse_agent_id};
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
/// A routed recipient receives at most one generic notice per minute through
/// the shared mail/claim notice gate.
pub const MAIL_NOTICE_COOLDOWN_MS: i64 = 60 * 1000;
/// Default unread-text bound per Agent mailbox: 10 MiB.
pub const DEFAULT_MAX_UNREAD_BYTES_PER_AGENT: i64 = 10 * 1024 * 1024;
/// Default unread-message bound per Agent mailbox: 10,000 rows.
pub const DEFAULT_MAX_UNREAD_MESSAGES_PER_AGENT: i64 = 10_000;

/// Owner-configured unread bounds for one Agent mailbox (`[mail]` config).
/// Admission rejects an overfull mailbox with `mailbox_full` before writing
/// any row, so there is never a partial fanout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MailLimits {
    pub max_unread_bytes_per_agent: i64,
    pub max_unread_messages_per_agent: i64,
}

impl Default for MailLimits {
    fn default() -> Self {
        Self {
            max_unread_bytes_per_agent: DEFAULT_MAX_UNREAD_BYTES_PER_AGENT,
            max_unread_messages_per_agent: DEFAULT_MAX_UNREAD_MESSAGES_PER_AGENT,
        }
    }
}

impl MailLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_unread_bytes_per_agent <= 0 || self.max_unread_messages_per_agent <= 0 {
            return Err(Error::new(
                "invalid",
                "mail unread bounds must be positive integers",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailSendRequest {
    #[schemars(
        description = "Caller-retained mutation id; MCP callers may omit id and the server mints a ULID, returned for retry. Otherwise use a fresh ULID for a new write."
    )]
    pub id: String,
    #[schemars(length(min = 1, max = MAX_MAIL_RECIPIENTS), extend("uniqueItems" = true), description = "Distinct concrete canonical a<positive i64> Agent ids.")]
    pub to: Vec<String>,
    #[schemars(
        description = "Only queue is implemented; send/steer/inject parse but return unsupported."
    )]
    pub mode: MailMode,
    #[schemars(extend("x-maxBytes" = MAX_MAIL_TEXT_BYTES), description = "Mail body, at most 32768 UTF-8 bytes, no NUL; empty text is allowed.")]
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-maxBytes" = MAX_MAIL_THREAD_BYTES), description = "Optional nonblank thread label, no control characters.")]
    pub thread: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Exact <write-id>/<recipient-agent> delivery id, at most 512 bytes, no NUL."
    )]
    pub reply_to_delivery: Option<String>,
}

impl MailSendRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)
            .map_err(|e| e.with_field("params.id", "nonblank mutation id"))?;
        if self.to.is_empty() || self.to.len() > MAX_MAIL_RECIPIENTS {
            return Err(Error::invalid_field(
                "params.to",
                "1 to 100 concrete Agent ids",
            ));
        }
        let mut seen = HashSet::new();
        for recipient in &self.to {
            parse_agent_id(recipient).map_err(|e| {
                e.with_field("params.to", "distinct canonical a<positive i64> Agent ids")
            })?;
            if !seen.insert(recipient) {
                return Err(Error::invalid_field("params.to", "distinct Agent ids"));
            }
        }
        validate_mail_text(&self.text).map_err(|e| {
            e.with_field(
                "params.text",
                "string at most 32768 UTF-8 bytes, without NUL",
            )
        })?;
        if let Some(thread) = self.thread.as_deref() {
            validate_mail_thread(thread).map_err(|e| {
                e.with_field(
                    "params.thread",
                    "nonblank label at most 256 bytes, without control characters",
                )
            })?;
        }
        if let Some(reply) = self.reply_to_delivery.as_deref() {
            validate_delivery_id(reply).map_err(|e| {
                e.with_field(
                    "params.reply_to_delivery",
                    "<write-id>/<recipient-agent> delivery id, at most 512 bytes, no NUL",
                )
            })?;
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

/// Literal rule definition for one scoped `mail.send` rule action.
///
/// Rule execution always queues (`MailMode::Queue`); the definition carries
/// no mode field. Field-for-field it mirrors `MailSendRequest` without the
/// public write id: the Store derives the child command id deterministically
/// and never reuses the public write-id namespace.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailDefinitionArgs {
    pub to: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to_delivery: Option<String>,
    pub text: String,
}

impl MailDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
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
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailDeliverySummary {
    pub delivery_id: String,
    pub recipient: String,
    pub recipient_generation: i64,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailSendResponse {
    pub deliveries: Vec<MailDeliverySummary>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailInboxRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "Owner must name a concrete Agent mailbox; Agent callers may omit for self, never another mailbox."
    )]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = MAX_MAIL_INBOX_ROWS))]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(description = "Exact last delivery id returned by this mailbox selection.")]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailInboxResponse {
    pub deliveries: Vec<MailDelivery>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MailAckRequest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[schemars(length(min = 1, max = MAX_MAIL_ACK_IDS), extend("uniqueItems" = true), description = "Distinct exact delivery ids; reading a delivery never acknowledges it.")]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarSetRequest {
    #[schemars(
        description = "Caller-retained mutation id; MCP callers may omit id and the server mints a ULID, returned for retry. Otherwise use a fresh ULID for a new write."
    )]
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-maxBytes" = MAX_VAR_SCOPE_BYTES), description = "global (Owner only) or project:<nonempty name>, no NUL. Omission defaults to the Agent project; Owner must supply scope. tree is unsupported.")]
    pub scope: Option<String>,
    #[schemars(extend("x-maxBytes" = MAX_VAR_KEY_BYTES), description = "Nonblank variable key, at most 256 UTF-8 bytes, no NUL.")]
    pub key: String,
    #[schemars(extend("x-maxEncodedBytes" = MAX_VAR_VALUE_BYTES), description = "Any JSON value, including null; compact encoded JSON is at most 16384 bytes.")]
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        range(min = 0),
        description = "Optional CAS version: zero creates an absent key; a stale version conflicts."
    )]
    pub expect_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-maxBytes" = MAX_VAR_NOTE_BYTES), description = "Optional note, at most 1024 UTF-8 bytes, no NUL.")]
    pub note: Option<String>,
}

impl VarSetRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)
            .map_err(|e| e.with_field("params.id", "nonblank mutation id"))?;
        if let Some(scope) = self.scope.as_deref() {
            parse_var_scope(scope).map_err(|e| {
                e.with_field(
                    "params.scope",
                    "global or project:<nonempty name>; tree is unsupported",
                )
            })?;
        }
        validate_var_key(&self.key).map_err(|e| {
            e.with_field(
                "params.key",
                "nonblank string at most 256 UTF-8 bytes, no NUL",
            )
        })?;
        validate_var_value(&self.value).map_err(|e| {
            e.with_field("params.value", "JSON value encoded in at most 16384 bytes")
        })?;
        if self.expect_version.is_some_and(|version| version < 0) {
            return Err(Error::invalid_field(
                "params.expect_version",
                "integer zero or greater",
            ));
        }
        if let Some(note) = self.note.as_deref() {
            validate_var_note(note).map_err(|e| {
                e.with_field("params.note", "string at most 1024 UTF-8 bytes, no NUL")
            })?;
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

/// Literal rule definition for one versioned `var.set` rule action.
///
/// Mirrors `VarSetRequest` without the public write id. Scope defaults are
/// resolved under the saved originating principal at admission and again at
/// execution: an omitted scope means the principal's own project, `global`
/// stays Owner-only, and any other project scope is rejected.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarDefinitionArgs {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub key: String,
    pub value: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expect_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl VarDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
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
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarSetResponse {
    pub scope: String,
    pub key: String,
    pub version: i64,
    pub set_by: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VarGetRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        description = "global (readable by Agents; Owner-only writes) or project:<name>; omitted for the Agent project, required for Owner. tree is unsupported."
    )]
    pub scope: Option<String>,
    #[schemars(extend("x-maxBytes" = MAX_VAR_KEY_BYTES), description = "Nonblank key, no NUL.")]
    pub key: String,
}

impl VarGetRequest {
    pub fn validate(&self) -> Result<()> {
        if let Some(scope) = self.scope.as_deref() {
            parse_var_scope(scope).map_err(|e| {
                e.with_field(
                    "params.scope",
                    "global or project:<nonempty name>; tree is unsupported",
                )
            })?;
        }
        validate_var_key(&self.key).map_err(|e| {
            e.with_field(
                "params.key",
                "nonblank string at most 256 UTF-8 bytes, no NUL",
            )
        })?;
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

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
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

/// Project-relative claim globs stay small enough for list output.
pub const MAX_CLAIM_GLOB_BYTES: usize = 1024;
/// `**` segments per claim glob. One `claim.set` runs the overlap DP
/// against every live claim under the Store lock, and the DP grows with
/// the `**` count on both sides — this cap keeps every production
/// comparison tiny no matter how the 1024 bytes are shaped.
pub const MAX_CLAIM_DOUBLE_STAR_SEGMENTS: usize = 8;
/// Optional claim notes stay small.
pub const MAX_CLAIM_NOTE_BYTES: usize = 1024;
/// Live claims per Agent generation stay bounded.
pub const MAX_CLAIMS_PER_AGENT: usize = 100;
/// Claims expire one hour after admission unless the caller says otherwise.
pub const DEFAULT_CLAIM_TTL_SECS: u64 = 3_600;
/// Claims live at most one day; refresh explicitly instead.
pub const MAX_CLAIM_TTL_SECS: u64 = 24 * 3_600;
/// `who` returns at most this many current workers.
pub const MAX_WHO_WORKERS: usize = 100;
/// Each `who` path list is truncated to this many entries.
pub const MAX_WHO_PATHS: usize = 100;

/// Normalize a project-relative claim glob: collapse duplicate slashes, drop
/// leading `./` segments and one trailing slash. Absolute paths, `..`
/// segments, NUL, backslashes and bracket/brace metacharacters are rejected:
/// globs use `/` separators with `*`, `**` and `?` only, and at most
/// [`MAX_CLAIM_DOUBLE_STAR_SEGMENTS`] `**` segments.
pub fn normalize_claim_glob(glob: &str) -> Result<String> {
    if glob.len() > MAX_CLAIM_GLOB_BYTES || glob.contains('\0') || glob.trim().is_empty() {
        return Err(Error::new("invalid", "claim glob is invalid"));
    }
    if glob.contains('\\')
        || glob.contains('[')
        || glob.contains(']')
        || glob.contains('{')
        || glob.contains('}')
    {
        return Err(Error::new(
            "invalid",
            "claim globs use / separators with *, ** and ? only",
        ));
    }
    if glob.starts_with('/')
        || glob.starts_with("~/")
        || (glob.len() >= 2
            && glob.as_bytes()[0].is_ascii_alphabetic()
            && glob.as_bytes()[1] == b':')
    {
        return Err(Error::new("invalid", "claim globs are project-relative"));
    }
    let mut segments: Vec<&str> = Vec::new();
    for segment in glob.split('/') {
        if segment.is_empty() || segment == "." {
            continue;
        }
        if segment == ".." {
            return Err(Error::new(
                "invalid",
                "claim globs must not escape the project",
            ));
        }
        segments.push(segment);
    }
    if segments.is_empty() {
        return Err(Error::new("invalid", "claim glob is invalid"));
    }
    if segments.iter().filter(|segment| **segment == "**").count() > MAX_CLAIM_DOUBLE_STAR_SEGMENTS
    {
        return Err(Error::new(
            "invalid",
            format!("claim globs hold at most {MAX_CLAIM_DOUBLE_STAR_SEGMENTS} `**` segments"),
        ));
    }
    Ok(segments.join("/"))
}

fn is_literal_glob(normalized: &str) -> bool {
    !normalized.contains(['*', '?'])
}

/// Match a normalized glob against a normalized project-relative path. A
/// `**` segment crosses directories; a trailing `/**` also matches the
/// directory itself. Inputs that fail normalization never match.
pub fn claim_glob_matches(pattern: &str, path: &str) -> bool {
    let Ok(pattern) = normalize_claim_glob(pattern) else {
        return false;
    };
    let Ok(path) = normalize_claim_glob(path) else {
        return false;
    };
    claim_glob_matches_normalized(&pattern, &path)
}

/// `claim_glob_matches` for already-normalized inputs.
fn claim_glob_matches_normalized(pattern: &str, path: &str) -> bool {
    if is_literal_glob(pattern) {
        return pattern == path;
    }
    let Ok(glob) = globset::GlobBuilder::new(pattern)
        .literal_separator(true)
        .build()
    else {
        return false;
    };
    if glob.compile_matcher().is_match(path) {
        return true;
    }
    // A trailing `/**` also covers the directory itself.
    pattern
        .strip_suffix("/**")
        .is_some_and(|directory| directory == path)
}

/// Whether two claim globs may overlap. A literal side reduces to exact
/// matching against the other side; two wildcard sides stay conservative:
/// segments are compatible when equal or either side carries a wildcard,
/// and `**` realigns either side across directories — so `src/**` overlaps
/// both `src/*.rs` and `src/lib/**`. A false positive only adds an
/// advisory notice; a false negative would hide one.
pub fn claim_globs_overlap(left: &str, right: &str) -> Result<bool> {
    let left = normalize_claim_glob(left)?;
    let right = normalize_claim_glob(right)?;
    Ok(claim_globs_overlap_normalized(&left, &right))
}

/// `claim_globs_overlap` for already-normalized globs. The Store's
/// conflict scan holds normalized globs on both sides, so it calls this
/// once per candidate instead of re-normalizing per comparison. Both
/// inputs must be `normalize_claim_glob` output (as stored at admission):
/// unnormalized input — duplicate slashes, `.` segments, absolute paths —
/// may compare wrong, so callers with raw input must use
/// `claim_globs_overlap` instead.
pub fn claim_globs_overlap_normalized(left: &str, right: &str) -> bool {
    if left == right {
        return true;
    }
    if is_literal_glob(left) {
        return claim_glob_matches_normalized(right, left);
    }
    if is_literal_glob(right) {
        return claim_glob_matches_normalized(left, right);
    }
    segments_may_overlap(&collapsed_segments(left), &collapsed_segments(right))
}

fn collapsed_segments(normalized: &str) -> Vec<&str> {
    let mut segments: Vec<&str> = Vec::new();
    for segment in normalized.split('/') {
        if segment == "**" && segments.last() == Some(&"**") {
            continue;
        }
        segments.push(segment);
    }
    segments
}

fn segment_may_overlap(left: &str, right: &str) -> bool {
    left == right || left.contains(['*', '?']) || right.contains(['*', '?'])
}

/// Bounded segment-overlap DP over (left index, right index): each cell
/// is computed once into a memo table, and a `**` cell scans at most one
/// row or column, so the worst case is O(n·m·(n+m)) for n×m segments
/// instead of the exponential re-recursion of naive skip loops. The bound
/// is load-bearing: globs may be 1024 bytes and this runs under the Store
/// lock against every live claim, so one adversarial `(**/*)^k` pair must
/// not stall the daemon. Admission additionally caps `**` segments at
/// [`MAX_CLAIM_DOUBLE_STAR_SEGMENTS`], so production pairs stay far below
/// the worst case; the DP stays bounded regardless.
fn segments_may_overlap(left: &[&str], right: &[&str]) -> bool {
    let (n, m) = (left.len(), right.len());
    let stride = m + 1;
    // dp[i][j]: segments left[i..] may overlap right[j..].
    let mut dp = vec![false; (n + 1) * stride];
    for i in (0..=n).rev() {
        for j in (0..=m).rev() {
            let cell = match (left.get(i), right.get(j)) {
                (None, None) => true,
                (None, Some(_)) => right[j] == "**" && dp[i * stride + j + 1],
                (Some(_), None) => left[i] == "**" && dp[(i + 1) * stride + j],
                (Some(first), Some(second)) => {
                    if *first == "**" && *second == "**" {
                        dp[(i + 1) * stride + j + 1]
                            || dp[(i + 1) * stride + j]
                            || dp[i * stride + j + 1]
                    } else if *first == "**" {
                        // `**` absorbs any run of right segments.
                        (j..=m).any(|skip| dp[(i + 1) * stride + skip])
                    } else if *second == "**" {
                        (i..=n).any(|skip| dp[skip * stride + j + 1])
                    } else {
                        segment_may_overlap(first, second) && dp[(i + 1) * stride + j + 1]
                    }
                }
            };
            dp[i * stride + j] = cell;
        }
    }
    dp[0]
}

fn validate_claim_note(note: &str) -> Result<()> {
    if note.len() > MAX_CLAIM_NOTE_BYTES || note.contains('\0') {
        return Err(Error::new("invalid", "claim note is invalid"));
    }
    Ok(())
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimSetRequest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub glob: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
}

impl ClaimSetRequest {
    pub fn effective_ttl_secs(&self) -> u64 {
        self.ttl_secs.unwrap_or(DEFAULT_CLAIM_TTL_SECS)
    }

    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| parse_agent_id(agent).is_err())
        {
            return Err(Error::new("invalid", "claim Agent must name an Agent"));
        }
        normalize_claim_glob(&self.glob)?;
        if let Some(note) = self.note.as_deref() {
            validate_claim_note(note)?;
        }
        if self
            .ttl_secs
            .is_some_and(|ttl| ttl == 0 || ttl > MAX_CLAIM_TTL_SECS)
        {
            return Err(Error::new(
                "invalid",
                "claim TTL must be between 1 second and one day",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &ClaimSetResponse) -> Result<()> {
        if self.validate().is_err() {
            return Err(unproven());
        }
        if response.glob != normalize_claim_glob(&self.glob).map_err(|_| unproven())?
            || response.note.as_deref() != self.note.as_deref()
            || response.expires_at <= response.admitted_at
            || response.conflicts.len() > MAX_WHO_WORKERS
        {
            return Err(unproven());
        }
        parse_agent_id(&response.agent).map_err(|_| unproven())?;
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| agent != response.agent)
            || response.generation < 0
        {
            return Err(unproven());
        }
        for conflict in &response.conflicts {
            conflict.validate().map_err(|_| unproven())?;
            if conflict.agent == response.agent {
                return Err(unproven());
            }
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimConflict {
    pub agent: String,
    pub generation: i64,
    pub glob: String,
}

impl ClaimConflict {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new(
                "invalid",
                "claim conflict carries invalid stamps",
            ));
        }
        normalize_claim_glob(&self.glob)?;
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimSetResponse {
    pub agent: String,
    pub generation: i64,
    pub glob: String,
    #[serde(deserialize_with = "nullable")]
    pub note: Option<String>,
    pub expires_at: i64,
    pub admitted_at: i64,
    pub conflicts: Vec<ClaimConflict>,
    pub truncated: bool,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimView {
    pub agent: String,
    pub generation: i64,
    pub glob: String,
    #[serde(deserialize_with = "nullable")]
    pub note: Option<String>,
    pub expires_at: i64,
    pub created_at: i64,
}

impl ClaimView {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0 || self.created_at < 0 || self.expires_at <= self.created_at {
            return Err(Error::new("invalid", "claim carries invalid stamps"));
        }
        normalize_claim_glob(&self.glob)?;
        if let Some(note) = self.note.as_deref() {
            validate_claim_note(note)?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimListRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
}

impl ClaimListRequest {
    pub fn validate(&self) -> Result<()> {
        if self
            .project
            .as_deref()
            .is_some_and(|project| project.trim().is_empty())
        {
            return Err(Error::new("invalid", "claim project filter is invalid"));
        }
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| parse_agent_id(agent).is_err())
        {
            return Err(Error::new(
                "invalid",
                "claim Agent filter must name an Agent",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &ClaimListResponse) -> Result<()> {
        if self.validate().is_err() {
            return Err(unproven());
        }
        let mut previous: Option<(i64, &str)> = None;
        for claim in &response.claims {
            claim.validate().map_err(|_| unproven())?;
            if self
                .agent
                .as_deref()
                .is_some_and(|agent| agent != claim.agent)
            {
                return Err(unproven());
            }
            let key = (
                parse_agent_id(&claim.agent).map_err(|_| unproven())?,
                claim.glob.as_str(),
            );
            if previous.is_some_and(|prior| prior >= key) {
                return Err(unproven());
            }
            previous = Some(key);
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimListResponse {
    pub claims: Vec<ClaimView>,
    pub truncated: bool,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimReleaseRequest {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    pub glob: String,
}

impl ClaimReleaseRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| parse_agent_id(agent).is_err())
        {
            return Err(Error::new("invalid", "claim Agent must name an Agent"));
        }
        normalize_claim_glob(&self.glob)?;
        Ok(())
    }

    pub fn validate_response(&self, response: &ClaimReleaseResponse) -> Result<()> {
        if self.validate().is_err() {
            return Err(unproven());
        }
        if response.glob != normalize_claim_glob(&self.glob).map_err(|_| unproven())?
            || response.generation < 0
        {
            return Err(unproven());
        }
        parse_agent_id(&response.agent).map_err(|_| unproven())?;
        if self
            .agent
            .as_deref()
            .is_some_and(|agent| agent != response.agent)
        {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimReleaseResponse {
    pub agent: String,
    pub generation: i64,
    pub glob: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhoRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history: Option<bool>,
}

impl WhoRequest {
    pub fn want_history(&self) -> bool {
        self.history.unwrap_or(false)
    }

    pub fn validate(&self) -> Result<()> {
        if self
            .project
            .as_deref()
            .is_some_and(|project| project.trim().is_empty())
        {
            return Err(Error::new("invalid", "who project filter is invalid"));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &WhoResponse) -> Result<()> {
        if self.validate().is_err() || response.workers.len() > MAX_WHO_WORKERS {
            return Err(unproven());
        }
        for worker in &response.workers {
            worker.validate().map_err(|_| unproven())?;
            if self
                .project
                .as_deref()
                .is_some_and(|project| project != worker.project)
            {
                return Err(unproven());
            }
            if !self.want_history() && !worker.recent_history.is_empty() {
                return Err(unproven());
            }
        }
        for shared in &response.shared {
            shared.validate().map_err(|_| unproven())?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhoWorker {
    pub agent: String,
    pub generation: i64,
    pub state: AgentState,
    pub project: String,
    #[serde(deserialize_with = "nullable")]
    pub workspace: Option<String>,
    pub singly_owned: bool,
    pub git_available: bool,
    pub attribution_complete: bool,
    pub edits_truncated: bool,
    pub attributed_edits: Vec<String>,
    pub dirty: Vec<String>,
    pub committed_paths: Vec<String>,
    #[serde(deserialize_with = "nullable")]
    pub base_oid: Option<String>,
    pub claims: Vec<ClaimView>,
    pub recent_history: Vec<GitCommit>,
}

impl WhoWorker {
    pub fn validate(&self) -> Result<()> {
        parse_agent_id(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "who worker carries invalid stamps"));
        }
        if self.state.is_terminal() {
            return Err(Error::new(
                "invalid",
                "terminal Agents are never current workers",
            ));
        }
        if self.project.trim().is_empty() {
            return Err(Error::new("invalid", "who worker project is invalid"));
        }
        if !self.singly_owned && !self.dirty.is_empty() {
            return Err(Error::new(
                "invalid",
                "shared checkouts never attribute dirty files",
            ));
        }
        if self.workspace.is_none() && (self.singly_owned || self.git_available) {
            return Err(Error::new("invalid", "who worker without a workspace"));
        }
        if self.attributed_edits.len() > MAX_WHO_PATHS
            || self.dirty.len() > MAX_WHO_PATHS
            || self.committed_paths.len() > MAX_WHO_PATHS
            || self.claims.len() > MAX_CLAIMS_PER_AGENT
        {
            return Err(Error::new(
                "invalid",
                "who worker lists exceed their bounds",
            ));
        }
        for claim in &self.claims {
            claim.validate()?;
            if claim.agent != self.agent || claim.generation != self.generation {
                return Err(Error::new("invalid", "who worker carries a foreign claim"));
            }
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhoSharedWorkspace {
    pub workspace: String,
    pub agents: Vec<String>,
    pub dirty: Vec<String>,
}

impl WhoSharedWorkspace {
    pub fn validate(&self) -> Result<()> {
        if self.workspace.trim().is_empty() || self.agents.len() < 2 {
            return Err(Error::new("invalid", "shared workspace needs two owners"));
        }
        for agent in &self.agents {
            parse_agent_id(agent)?;
        }
        if self.dirty.len() > MAX_WHO_PATHS {
            return Err(Error::new("invalid", "shared dirty list exceeds its bound"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhoResponse {
    pub workers: Vec<WhoWorker>,
    pub shared: Vec<WhoSharedWorkspace>,
    pub truncated: bool,
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

    fn claim_set_request() -> ClaimSetRequest {
        ClaimSetRequest {
            id: "01J0000000000000000000003".to_owned(),
            agent: None,
            glob: "src/**".to_owned(),
            note: Some("refactor".to_owned()),
            ttl_secs: None,
        }
    }

    fn claim_view(agent: &str) -> ClaimView {
        ClaimView {
            agent: agent.to_owned(),
            generation: 1,
            glob: "src/**".to_owned(),
            note: None,
            expires_at: 3_601_000,
            created_at: 1_000,
        }
    }

    fn who_worker(agent: &str) -> WhoWorker {
        WhoWorker {
            agent: agent.to_owned(),
            generation: 1,
            state: AgentState::Idle,
            project: "app".to_owned(),
            workspace: Some("/repo".to_owned()),
            singly_owned: true,
            git_available: true,
            attribution_complete: true,
            edits_truncated: false,
            attributed_edits: vec!["src/main.rs".to_owned()],
            dirty: vec!["src/main.rs".to_owned()],
            committed_paths: Vec::new(),
            base_oid: None,
            claims: Vec::new(),
            recent_history: Vec::new(),
        }
    }

    #[test]
    fn claim_globs_normalize_match_and_detect_literal_overlap() {
        assert_eq!(
            normalize_claim_glob("./src//main.rs").unwrap(),
            "src/main.rs"
        );
        assert_eq!(normalize_claim_glob("src/").unwrap(), "src");
        assert_eq!(normalize_claim_glob("**").unwrap(), "**");
        for bad in [
            "",
            "  ",
            "/abs/path",
            "~/home",
            "C:/win/path",
            "c:relative",
            "src/../escape",
            "..",
            "src\\win.rs",
            "src/[ab].rs",
            "src/{a,b}",
            "src/\0.rs",
            &"x".repeat(MAX_CLAIM_GLOB_BYTES + 1),
        ] {
            assert!(normalize_claim_glob(bad).is_err(), "glob accepted: {bad:?}");
        }

        assert!(claim_glob_matches("src/**", "src/main.rs"));
        assert!(claim_glob_matches("src/**", "src"));
        assert!(claim_glob_matches("src/**", "src/a/b.rs"));
        assert!(claim_glob_matches("**", "src/a/b.rs"));
        assert!(claim_glob_matches("src/*.rs", "src/main.rs"));
        assert!(!claim_glob_matches("src/*.rs", "src/a/main.rs"));
        assert!(claim_glob_matches("src/ma?n.rs", "src/main.rs"));
        assert!(!claim_glob_matches("src/ma?n.rs", "src/man.rs"));
        assert!(claim_glob_matches("src/main.rs", "src/main.rs"));
        assert!(!claim_glob_matches("src/main.rs", "src/other.rs"));
        assert!(!claim_glob_matches("other/**", "src/main.rs"));
        assert!(!claim_glob_matches("src/**", "/abs/path"));

        assert!(claim_globs_overlap("src/**", "./src/**").unwrap());
        assert!(claim_globs_overlap("src/**", "src/main.rs").unwrap());
        assert!(claim_globs_overlap("src/main.rs", "src/**").unwrap());
        assert!(!claim_globs_overlap("src/**", "other/**").unwrap());
        // Wildcard-vs-wildcard overlap is conservative: `**` covers any
        // segment shape below it, and two wildcard segments in the same
        // position may intersect.
        assert!(claim_globs_overlap("src/**", "src/*.rs").unwrap());
        assert!(claim_globs_overlap("src/*.rs", "src/**").unwrap());
        assert!(claim_globs_overlap("src/**", "src/lib/**").unwrap());
        assert!(claim_globs_overlap("src/*.rs", "src/m*").unwrap());
        assert!(!claim_globs_overlap("src/*.rs", "other/*.rs").unwrap());
        assert!(claim_globs_overlap("src/**", "../escape").is_err());
    }

    #[test]
    fn claim_overlap_stays_fast_on_maximal_alternating_globs() {
        // Two ~1024-byte alternating `**/*` runs with disjoint literal
        // tails: the naive skip-loop recursion re-solves the same (i, j)
        // states exponentially (~13.7M calls for the 8-repetition pair),
        // while a bounded DP visits each state once. No path ends in both
        // `x` and `y`, so the pair is disjoint — and the call must return
        // instead of stalling the Store lock it runs under. Admission
        // rejects this shape (>8 `**` segments), so the DP is exercised
        // directly through the normalized entry point; both inputs are
        // already in normalized form.
        let left = format!("{}x", "**/*/".repeat(204));
        let right = format!("{}y", "**/*/".repeat(204));
        assert!(left.len() <= MAX_CLAIM_GLOB_BYTES);
        assert!(right.len() <= MAX_CLAIM_GLOB_BYTES);
        assert!(normalize_claim_glob(&left).is_err());
        assert!(normalize_claim_glob(&right).is_err());
        let started = std::time::Instant::now();
        assert!(!claim_globs_overlap_normalized(&left, &right));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "overlap took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn claim_globs_cap_double_star_segments_at_eight() {
        let capped = format!("{}tail", "**/".repeat(MAX_CLAIM_DOUBLE_STAR_SEGMENTS));
        assert_eq!(normalize_claim_glob(&capped).unwrap(), capped);
        let over = format!("{}tail", "**/".repeat(MAX_CLAIM_DOUBLE_STAR_SEGMENTS + 1));
        let error = normalize_claim_glob(&over).unwrap_err();
        assert_eq!(error.code, "invalid");
        // The `claim.set` validator rejects the same glob as `invalid`.
        let error = ClaimSetRequest {
            glob: over,
            ..claim_set_request()
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "invalid");
    }

    #[test]
    fn claim_requests_validate_bounds_and_response_bindings() {
        let request = claim_set_request();
        assert_eq!(request.effective_ttl_secs(), DEFAULT_CLAIM_TTL_SECS);
        request.validate().unwrap();
        request
            .validate_response(&ClaimSetResponse {
                agent: "a1".to_owned(),
                generation: 1,
                glob: "src/**".to_owned(),
                note: Some("refactor".to_owned()),
                expires_at: 3_601_000,
                admitted_at: 1_000,
                conflicts: vec![ClaimConflict {
                    agent: "a2".to_owned(),
                    generation: 1,
                    glob: "src/main.rs".to_owned(),
                }],
                truncated: false,
            })
            .unwrap();
        assert!(
            request
                .validate_response(&ClaimSetResponse {
                    agent: "a1".to_owned(),
                    generation: 1,
                    glob: "src/other.rs".to_owned(),
                    note: Some("refactor".to_owned()),
                    expires_at: 3_601_000,
                    admitted_at: 1_000,
                    conflicts: Vec::new(),
                    truncated: false,
                })
                .is_err()
        );
        // A self-conflict is never proven.
        assert!(
            request
                .validate_response(&ClaimSetResponse {
                    agent: "a1".to_owned(),
                    generation: 1,
                    glob: "src/**".to_owned(),
                    note: Some("refactor".to_owned()),
                    expires_at: 3_601_000,
                    admitted_at: 1_000,
                    conflicts: vec![ClaimConflict {
                        agent: "a1".to_owned(),
                        generation: 1,
                        glob: "src/**".to_owned(),
                    }],
                    truncated: false,
                })
                .is_err()
        );

        for request in [
            ClaimSetRequest {
                id: "  ".to_owned(),
                ..claim_set_request()
            },
            ClaimSetRequest {
                agent: Some("nope".to_owned()),
                ..claim_set_request()
            },
            ClaimSetRequest {
                glob: "../escape".to_owned(),
                ..claim_set_request()
            },
            ClaimSetRequest {
                note: Some("n".repeat(MAX_CLAIM_NOTE_BYTES + 1)),
                ..claim_set_request()
            },
            ClaimSetRequest {
                ttl_secs: Some(0),
                ..claim_set_request()
            },
            ClaimSetRequest {
                ttl_secs: Some(MAX_CLAIM_TTL_SECS + 1),
                ..claim_set_request()
            },
        ] {
            assert!(request.validate().is_err());
        }
        assert!(serde_json::from_value::<ClaimSetRequest>(json!({"unexpected": 1})).is_err());

        claim_view("a1").validate().unwrap();
        let mut expired = claim_view("a1");
        expired.expires_at = expired.created_at;
        assert!(expired.validate().is_err());

        let list = ClaimListRequest {
            project: Some("app".to_owned()),
            agent: None,
        };
        list.validate().unwrap();
        list.validate_response(&ClaimListResponse {
            claims: vec![claim_view("a1"), claim_view("a2")],
            truncated: false,
        })
        .unwrap();
        // Numeric agent order: a2 sorts before a10.
        list.validate_response(&ClaimListResponse {
            claims: vec![claim_view("a2"), claim_view("a10")],
            truncated: false,
        })
        .unwrap();
        assert!(
            list.validate_response(&ClaimListResponse {
                claims: vec![claim_view("a10"), claim_view("a2")],
                truncated: false,
            })
            .is_err()
        );
        assert!(
            list.validate_response(&ClaimListResponse {
                claims: vec![claim_view("a2"), claim_view("a1")],
                truncated: false,
            })
            .is_err()
        );
        assert!(
            ClaimListRequest {
                agent: Some("a1".to_owned()),
                ..Default::default()
            }
            .validate_response(&ClaimListResponse {
                claims: vec![claim_view("a2")],
                truncated: false,
            })
            .is_err()
        );

        let release = ClaimReleaseRequest {
            id: "01J0000000000000000000004".to_owned(),
            agent: None,
            glob: "src/**".to_owned(),
        };
        release.validate().unwrap();
        release
            .validate_response(&ClaimReleaseResponse {
                agent: "a1".to_owned(),
                generation: 1,
                glob: "src/**".to_owned(),
            })
            .unwrap();
    }

    #[test]
    fn who_responses_bind_project_history_and_attribution_rules() {
        let request = WhoRequest {
            project: Some("app".to_owned()),
            history: None,
        };
        request.validate().unwrap();
        request
            .validate_response(&WhoResponse {
                workers: vec![who_worker("a1")],
                shared: Vec::new(),
                truncated: false,
            })
            .unwrap();

        // History appears only when asked.
        let with_history = WhoRequest {
            project: None,
            history: Some(true),
        };
        let mut worker = who_worker("a1");
        worker.recent_history = vec![crate::GitCommit {
            oid: "abc".to_owned(),
            paths: vec!["src/main.rs".to_owned()],
        }];
        with_history
            .validate_response(&WhoResponse {
                workers: vec![worker.clone()],
                shared: Vec::new(),
                truncated: false,
            })
            .unwrap();
        assert!(
            request
                .validate_response(&WhoResponse {
                    workers: vec![worker],
                    shared: Vec::new(),
                    truncated: false,
                })
                .is_err()
        );

        // Terminal Agents, foreign projects, shared dirty attribution and
        // foreign claims are never proven.
        let mut terminal = who_worker("a1");
        terminal.state = AgentState::Stopped;
        assert!(
            request
                .validate_response(&WhoResponse {
                    workers: vec![terminal],
                    shared: Vec::new(),
                    truncated: false,
                })
                .is_err()
        );
        let mut foreign = who_worker("a1");
        foreign.project = "other".to_owned();
        assert!(
            request
                .validate_response(&WhoResponse {
                    workers: vec![foreign],
                    shared: Vec::new(),
                    truncated: false,
                })
                .is_err()
        );
        let mut shared_dirty = who_worker("a1");
        shared_dirty.singly_owned = false;
        assert!(
            request
                .validate_response(&WhoResponse {
                    workers: vec![shared_dirty],
                    shared: Vec::new(),
                    truncated: false,
                })
                .is_err()
        );
        let mut foreign_claim = who_worker("a1");
        foreign_claim.claims = vec![claim_view("a2")];
        assert!(
            request
                .validate_response(&WhoResponse {
                    workers: vec![foreign_claim],
                    shared: Vec::new(),
                    truncated: false,
                })
                .is_err()
        );
        let mut no_workspace = who_worker("a1");
        no_workspace.workspace = None;
        assert!(
            request
                .validate_response(&WhoResponse {
                    workers: vec![no_workspace],
                    shared: Vec::new(),
                    truncated: false,
                })
                .is_err()
        );

        WhoSharedWorkspace {
            workspace: "/repo".to_owned(),
            agents: vec!["a1".to_owned(), "a2".to_owned()],
            dirty: Vec::new(),
        }
        .validate()
        .unwrap();
        assert!(
            WhoSharedWorkspace {
                workspace: "/repo".to_owned(),
                agents: vec!["a1".to_owned()],
                dirty: Vec::new(),
            }
            .validate()
            .is_err()
        );
    }
}
