//! Addressed mail, the shared notice gate and shared variables (L5.1).
//!
//! One row is one concrete recipient delivery; selector expansion snapshots
//! concrete recipients once, inside the admission transaction, and mailbox
//! bounds are checked for every recipient before any row is written, so an
//! overfull mailbox rejects without a partial fanout. Reads never mark
//! state; only an explicit ack over exact delivery ids does, idempotently.
//! A notice claim is an atomic check-and-set: at most one generic notice per
//! recipient per minute with at most one open (or unknown) notice command,
//! shared by mail wakes and claim-conflict notices. Variable writes are
//! compare-and-swap on an incrementing version; an absent row reads as
//! version 0, so `expect_version: 0` creates.

use super::{
    Change, Error, MAX_EVENT_BYTES, Result, Store, canonical, command_binding, commit_transaction,
    decode_json, encode_json, enforce_page_bytes, history_expired, insert_daemon_event,
    load_agent_by_num, load_command_record, provider_holds, sql_error, turn_state_name,
    validate_id_age, write_binding,
};
use eliot_core::{
    Agent, CommandState, TurnState,
    api::{
        MAIL_NOTICE_COOLDOWN_MS, MailAckRequest, MailAckResponse, MailDelivery,
        MailDeliverySummary, MailInboxRequest, MailInboxResponse, MailLimits, MailMode,
        MailReadRequest, MailSendRequest, MailSendResponse, VarGetRequest, VarGetResponse,
        VarScope, VarSetRequest, VarSetResponse, parse_var_scope,
    },
    parse_agent_id,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::{Value, json};

const MAIL_SEND_METHOD: &str = "mail.send";
const MAIL_ACK_METHOD: &str = "mail.ack";
const VAR_SET_METHOD: &str = "var.set";

/// Which advisory channel a coalesced notice speaks for. Mail and claims
/// share one gate — one cooldown, one open notice command per recipient —
/// and differ only in the pending proof and the generic text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoticeKind {
    Mail,
    Claim,
}

/// The pending proof a notice claim carries: mail counts unread rows inside
/// the claim transaction, claims arrive with their overlap count.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NoticePending {
    Mail,
    Claim { overlapping: i64 },
}

impl NoticePending {
    pub fn kind(self) -> NoticeKind {
        match self {
            NoticePending::Mail => NoticeKind::Mail,
            NoticePending::Claim { .. } => NoticeKind::Claim,
        }
    }
}

/// A claimed notice: the pending count for the generic notice text and the
/// recipient generation the notice command must bind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoticeClaim {
    pub kind: NoticeKind,
    pub count: i64,
    pub generation: i64,
}

/// Durable notice state for one recipient.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoticeState {
    pub last_notice_at: i64,
    pub open_notice_command: Option<String>,
}

pub(super) enum Sender {
    Owner,
    Agent {
        num: i64,
        id: String,
        generation: i64,
    },
}

pub(super) fn parse_sender(principal: &str, domain: &str) -> Result<Sender> {
    if principal == "owner" {
        return Ok(Sender::Owner);
    }
    if let Some(rest) = principal.strip_prefix("agent:") {
        let Some((id, generation)) = rest.split_once(':') else {
            return Err(Error::new(
                "not_allowed",
                format!("{domain} principal is not an Agent"),
            ));
        };
        let num = parse_agent_id(id)?;
        let generation = generation.parse::<i64>().map_err(|_| {
            Error::new("not_allowed", format!("{domain} principal is not an Agent"))
        })?;
        if generation < 0 {
            return Err(Error::new(
                "not_allowed",
                format!("{domain} principal is not an Agent"),
            ));
        }
        return Ok(Sender::Agent {
            num,
            id: id.to_owned(),
            generation,
        });
    }
    Err(Error::new(
        "not_allowed",
        "coordination needs the owner or an Agent principal",
    ))
}

fn delivery_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MailDelivery> {
    let mode: String = row.get(7)?;
    let mode: MailMode = serde_json::from_value(Value::String(mode)).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(MailDelivery {
        delivery_id: row.get(0)?,
        sender: row.get(1)?,
        sender_generation: row.get(2)?,
        recipient: row.get(3)?,
        recipient_generation: row.get(4)?,
        thread: row.get(5)?,
        reply_to_delivery: row.get(6)?,
        mode,
        text: row.get(8)?,
        admitted_at: row.get(9)?,
        presented_at: row.get(10)?,
        presentation_command: row.get(11)?,
        read_at: row.get(12)?,
    })
}

const DELIVERY_COLUMNS: &str = "m.delivery_id, m.sender, m.sender_generation,
    (SELECT 'a' || a.id FROM agents a WHERE a.id = m.recipient),
    m.recipient_generation, m.thread, m.reply_to_delivery, m.mode, m.text,
    m.admitted_at, m.presented_at, m.presentation_command, m.read_at";

impl Store {
    /// Queue one message to concrete recipients, snapshotting their
    /// generations in a single bounded transaction. Every recipient mailbox
    /// must fit the owner-configured unread `limits`, checked before any row
    /// is written, so an overfull mailbox rejects without a partial fanout.
    pub fn mail_send(
        &mut self,
        principal: &str,
        request: &MailSendRequest,
        limits: MailLimits,
        now: i64,
    ) -> Result<Change<MailSendResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.mail_send_inner(principal, request, limits, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Read one page of unread deliveries. Read-only: nothing is marked.
    pub fn mail_inbox(
        &self,
        principal: &str,
        request: &MailInboxRequest,
        now: i64,
    ) -> Result<MailInboxResponse> {
        request.validate()?;
        let mailbox = self.resolve_mailbox(principal, request.agent.as_deref())?;
        let mut deliveries = self.inbox_page(mailbox, request, now)?;
        enforce_page_bytes(&mut deliveries)?;
        let next = deliveries
            .last()
            .map(|delivery| delivery.delivery_id.clone());
        let response = MailInboxResponse { deliveries, next };
        request.validate_response(&response)?;
        Ok(response)
    }

    /// Read one exact delivery. Read-only: nothing is marked. An absent
    /// delivery whose write id is old enough to have been pruned reads
    /// `history_gap`; any other absent delivery reads `not_found`.
    pub fn mail_read(
        &self,
        principal: &str,
        request: &MailReadRequest,
        now: i64,
    ) -> Result<MailDelivery> {
        request.validate()?;
        let mailbox = self.resolve_mailbox(principal, request.agent.as_deref())?;
        let delivery: Option<MailDelivery> = self
            .connection
            .query_row(
                &format!("SELECT {DELIVERY_COLUMNS} FROM messages m WHERE m.delivery_id = ?1"),
                params![request.delivery_id],
                delivery_row,
            )
            .optional()
            .map_err(sql_error)?;
        let Some(delivery) = delivery else {
            return Err(absent_delivery_error(&request.delivery_id, mailbox, now));
        };
        if delivery.recipient != mailbox_name(mailbox) {
            return Err(Error::new("not_found", "mail delivery was not found"));
        }
        delivery.validate()?;
        request.validate_response(&delivery)?;
        Ok(delivery)
    }

    /// Acknowledge exact delivery ids, idempotently. An absent id that may
    /// have been pruned (own mailbox suffix, write id past the gap age)
    /// succeeds as already acked, so a retried ack never fails on removed
    /// history; any other unknown id rejects the whole acknowledgement and
    /// marks nothing. Nothing arrived after the fetch is affected.
    pub fn mail_ack(
        &mut self,
        principal: &str,
        request: &MailAckRequest,
        now: i64,
    ) -> Result<Change<MailAckResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.mail_ack_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Compare-and-swap one variable. An omitted expectation writes
    /// unconditionally; every write increments the version and emits
    /// `var.changed`.
    pub fn var_set(
        &mut self,
        principal: &str,
        request: &VarSetRequest,
        now: i64,
    ) -> Result<Change<VarSetResponse>> {
        self.ensure_writer_healthy()?;
        let result = self.var_set_inner(principal, request, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Read one variable value with its version and writer.
    pub fn var_get(&self, principal: &str, request: &VarGetRequest) -> Result<VarGetResponse> {
        request.validate()?;
        let scope = self.resolve_var_scope(principal, request.scope.as_deref(), false)?;
        let row: Option<(String, i64, String, Option<String>, i64)> = self
            .connection
            .query_row(
                "SELECT value, version, set_by, note, set_at FROM vars
                 WHERE scope = ?1 AND key = ?2",
                params![scope.encoded(), request.key],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()
            .map_err(sql_error)?;
        let Some((value, version, set_by, note, set_at)) = row else {
            return Err(Error::new("not_found", "variable was not found"));
        };
        let response = VarGetResponse {
            scope: scope.encoded(),
            key: request.key.clone(),
            value: decode_json(&value)?,
            version,
            set_by,
            note,
            set_at,
        };
        request.validate_response(&response)?;
        Ok(response)
    }

    /// Atomically claim one generic notice for a recipient, or suppress it.
    /// Suppression (cooldown, an open or unknown notice command, an unknown
    /// command or turn, a pending request, a provider hold, or nothing
    /// pending) returns `Ok(None)` and records nothing. The gate is shared:
    /// a mail wake and a claim-conflict notice for the same recipient
    /// coalesce into one cooldown and one open notice command.
    pub fn notice_claim(
        &mut self,
        recipient: &str,
        notice_command: &str,
        pending: NoticePending,
        now: i64,
    ) -> Result<Option<NoticeClaim>> {
        self.ensure_writer_healthy()?;
        let result = self.notice_claim_inner(recipient, notice_command, pending, now);
        self.fault_on_persistence_error(&result);
        result
    }

    /// Read durable notice state for one recipient.
    pub fn notice_state(&self, recipient: &str) -> Result<Option<NoticeState>> {
        let num = parse_agent_id(recipient)?;
        let row: Option<(i64, Option<String>)> = self
            .connection
            .query_row(
                "SELECT last_notice_at, open_notice_command FROM mail_notices WHERE recipient = ?1",
                params![num],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        Ok(
            row.map(|(last_notice_at, open_notice_command)| NoticeState {
                last_notice_at,
                open_notice_command,
            }),
        )
    }

    /// Release a claimed notice whose notice command was never admitted, so a
    /// failed `send` does not consume the notice cooldown. Only clears when
    /// the stored open command still names this notice; a newer claim is
    /// never disturbed.
    pub fn notice_release(&mut self, recipient: &str, notice_command: &str) -> Result<()> {
        self.ensure_writer_healthy()?;
        let result = self.notice_release_inner(recipient, notice_command);
        self.fault_on_persistence_error(&result);
        result
    }

    fn resolve_mailbox(&self, principal: &str, agent: Option<&str>) -> Result<i64> {
        match parse_sender(principal, "mail")? {
            Sender::Owner => {
                let Some(agent) = agent else {
                    return Err(Error::new(
                        "invalid",
                        "owner mail access must name the mailbox explicitly",
                    ));
                };
                let num = parse_agent_id(agent)?;
                if load_agent_by_num(&self.connection, num)?.is_none() {
                    return Err(Error::new(
                        "not_found",
                        format!("Agent {agent} was not found"),
                    ));
                }
                Ok(num)
            }
            Sender::Agent {
                num,
                id,
                generation,
            } => {
                if agent.is_some_and(|mailbox| mailbox != id) {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents read only their own mailbox",
                    ));
                }
                let current = load_agent_by_num(&self.connection, num)?.ok_or_else(|| {
                    Error::new("stale_generation", "Agent generation is no longer current")
                })?;
                if current.generation != generation {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation is no longer current",
                    ));
                }
                Ok(num)
            }
        }
    }

    fn resolve_var_scope(
        &self,
        principal: &str,
        scope: Option<&str>,
        write: bool,
    ) -> Result<VarScope> {
        let sender = parse_sender(principal, "mail")?;
        if let Sender::Agent {
            num, generation, ..
        } = &sender
        {
            let current = load_agent_by_num(&self.connection, *num)?.ok_or_else(|| {
                Error::new("stale_generation", "Agent generation is no longer current")
            })?;
            if current.generation != *generation {
                return Err(Error::new(
                    "stale_generation",
                    "Agent generation is no longer current",
                ));
            }
        }
        let resolved = match (scope, &sender) {
            (Some(scope), _) => parse_var_scope(scope)?,
            (None, Sender::Agent { num, .. }) => {
                let agent = load_agent_by_num(&self.connection, *num)?.ok_or_else(|| {
                    Error::new("stale_generation", "sender generation is no longer current")
                })?;
                VarScope::Project(agent.project)
            }
            (None, Sender::Owner) => {
                return Err(Error::new(
                    "invalid",
                    "owner variable access must name the scope explicitly",
                ));
            }
        };
        match (&resolved, &sender) {
            (VarScope::Global, Sender::Owner) => Ok(resolved),
            (VarScope::Global, Sender::Agent { .. }) if !write => Ok(resolved),
            (VarScope::Global, Sender::Agent { .. }) => Err(Error::new(
                "not_allowed",
                "only the owner writes global variables",
            )),
            (VarScope::Project(_), Sender::Owner) => Ok(resolved),
            (VarScope::Project(project), Sender::Agent { num, .. }) => {
                let agent = load_agent_by_num(&self.connection, *num)?.ok_or_else(|| {
                    Error::new("stale_generation", "sender generation is no longer current")
                })?;
                if agent.project != *project {
                    return Err(Error::new(
                        "not_allowed",
                        "Agents access only their own project variables",
                    ));
                }
                Ok(resolved)
            }
        }
    }

    fn inbox_page(
        &self,
        mailbox: i64,
        request: &MailInboxRequest,
        now: i64,
    ) -> Result<Vec<MailDelivery>> {
        let after_row: Option<i64> = match request.after.as_deref() {
            None => None,
            Some(after) => {
                let row: Option<i64> = self
                    .connection
                    .query_row(
                        "SELECT id FROM messages WHERE delivery_id = ?1 AND recipient = ?2",
                        params![after, mailbox],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(sql_error)?;
                Some(row.ok_or_else(|| {
                    if delivery_may_be_pruned(after, mailbox, now) {
                        Error::new(
                            "history_gap",
                            "mail inbox cursor names removed history; restart from the first page",
                        )
                    } else {
                        Error::new("invalid", "mail inbox cursor names no delivery here")
                    }
                })?)
            }
        };
        let mut statement = self
            .connection
            .prepare(&format!(
                "SELECT {DELIVERY_COLUMNS} FROM messages m
                 WHERE m.recipient = ?1 AND m.read_at IS NULL
                   AND (?2 IS NULL OR m.id > ?2)
                 ORDER BY m.id ASC LIMIT ?3"
            ))
            .map_err(sql_error)?;
        let limit = i64::try_from(request.effective_limit()).unwrap_or(i64::MAX);
        let deliveries = statement
            .query_map(params![mailbox, after_row, limit], delivery_row)
            .map_err(sql_error)?
            .collect::<rusqlite::Result<Vec<MailDelivery>>>()
            .map_err(sql_error)?;
        for delivery in &deliveries {
            delivery.validate()?;
        }
        Ok(deliveries)
    }

    fn mail_send_inner(
        &mut self,
        principal: &str,
        request: &MailSendRequest,
        limits: MailLimits,
        now: i64,
    ) -> Result<Change<MailSendResponse>> {
        request.validate()?;
        limits.validate()?;
        if request.mode != MailMode::Queue {
            return Err(Error::new(
                "unsupported",
                "only queued mail is connected; presentation modes are not",
            ));
        }
        let sender = parse_sender(principal, "mail")?;
        let sender_agent = match &sender {
            Sender::Owner => None,
            Sender::Agent {
                num, generation, ..
            } => {
                let agent = load_agent_by_num(&self.connection, *num)?.ok_or_else(|| {
                    Error::new("stale_generation", "sender generation is no longer current")
                })?;
                if agent.generation != *generation {
                    return Err(Error::new(
                        "stale_generation",
                        "sender generation is no longer current",
                    ));
                }
                Some(agent)
            }
        };
        let semantic_payload = json!({
            "to": request.to,
            "mode": request.mode.as_str(),
            "text": request.text,
            "thread": request.thread,
            "reply_to_delivery": request.reply_to_delivery,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "mail write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != MAIL_SEND_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "mail write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<MailSendResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        // Snapshot every recipient once, in this transaction: unknown ids
        // never create Agents, and bounds are checked for all before any
        // row is written, so there is no partial fanout.
        let mut recipients = Vec::with_capacity(request.to.len());
        for recipient in &request.to {
            let num = parse_agent_id(recipient)?;
            let agent = load_agent_by_num(&tx, num)?.ok_or_else(|| {
                Error::new("not_found", format!("Agent {recipient} was not found"))
            })?;
            if sender_agent
                .as_ref()
                .is_some_and(|sender| agent.project != sender.project)
            {
                return Err(Error::new(
                    "not_allowed",
                    "Agents mail only their own project",
                ));
            }
            recipients.push(agent);
        }
        if let Some(reply) = request.reply_to_delivery.as_deref() {
            Self::check_reply_participants_tx(&tx, reply, &sender, &recipients, now)?;
        }
        let addition = i64::try_from(request.text.len()).unwrap_or(i64::MAX);
        for agent in &recipients {
            let num = parse_agent_id(&agent.id)?;
            let unread: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE recipient = ?1 AND read_at IS NULL",
                    params![num],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if unread >= limits.max_unread_messages_per_agent {
                return Err(Error::new(
                    "mailbox_full",
                    format!("mailbox {} has no unread room", agent.id),
                ));
            }
            let bytes: i64 = tx
                .query_row(
                    "SELECT COALESCE(SUM(LENGTH(CAST(text AS BLOB))), 0) FROM messages
                     WHERE recipient = ?1 AND read_at IS NULL",
                    params![num],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            if bytes.saturating_add(addition) > limits.max_unread_bytes_per_agent {
                return Err(Error::new(
                    "mailbox_full",
                    format!("mailbox {} has no unread room", agent.id),
                ));
            }
        }

        let (sender_name, sender_generation) = match &sender {
            Sender::Owner => ("owner".to_owned(), None),
            Sender::Agent { id, generation, .. } => (id.clone(), Some(*generation)),
        };
        let mut deliveries = Vec::with_capacity(recipients.len());
        for agent in &recipients {
            let num = parse_agent_id(&agent.id)?;
            let delivery_id = format!("{}/{}", request.id, agent.id);
            let content_digest = canonical::digest(&json!({
                "sender": sender_name,
                "sender_generation": sender_generation,
                "recipient": agent.id,
                "mode": request.mode.as_str(),
                "text": request.text,
                "thread": request.thread,
                "reply_to_delivery": request.reply_to_delivery,
            }))?;
            db!(tx.execute(
                "INSERT INTO messages(delivery_id, sender, sender_generation, recipient,
                                      recipient_generation, thread, reply_to_delivery, mode, text,
                                      content_digest, admitted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    delivery_id,
                    sender_name,
                    sender_generation,
                    num,
                    agent.generation,
                    request.thread,
                    request.reply_to_delivery,
                    request.mode.as_str(),
                    request.text,
                    content_digest,
                    now
                ],
            ));
            deliveries.push(MailDeliverySummary {
                delivery_id,
                recipient: agent.id.clone(),
                recipient_generation: agent.generation,
            });
        }
        let value = MailSendResponse { deliveries };
        request.validate_response(&value)?;
        retain_write_tx(
            &tx,
            &request.id,
            principal,
            MAIL_SEND_METHOD,
            &payload_digest,
            &serde_json::to_value(&value)
                .map_err(|error| Error::new("internal", error.to_string()))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value,
            events: Vec::new(),
        })
    }

    fn check_reply_participants_tx(
        tx: &Transaction<'_>,
        reply: &str,
        sender: &Sender,
        recipients: &[eliot_core::Agent],
        now: i64,
    ) -> Result<()> {
        let row: Option<(String, i64)> = tx
            .query_row(
                "SELECT sender, recipient FROM messages WHERE delivery_id = ?1",
                params![reply],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let Some((sender_name, recipient_num)) = row else {
            // A reply names exact history, but no single mailbox is
            // authorized here yet: only the write-id age classifies it, and
            // the age rule alone leaks nothing beyond the caller's own input.
            let gap = reply
                .split_once('/')
                .is_some_and(|(head, _)| history_expired(head, now));
            return Err(if gap {
                Error::new("history_gap", "replied-to delivery is no longer retained")
            } else {
                Error::new("not_found", "replied-to delivery was not found")
            });
        };
        let mut participants = vec![recipient_num];
        if sender_name != "owner" {
            participants.push(parse_agent_id(&sender_name)?);
        }
        for agent in recipients {
            if !participants.contains(&parse_agent_id(&agent.id)?) {
                return Err(Error::new(
                    "invalid",
                    "a reply addresses only the original participants",
                ));
            }
        }
        if let Sender::Agent { num, .. } = sender
            && !participants.contains(num)
        {
            return Err(Error::new(
                "not_allowed",
                "a reply comes from an original participant",
            ));
        }
        Ok(())
    }

    fn mail_ack_inner(
        &mut self,
        principal: &str,
        request: &MailAckRequest,
        now: i64,
    ) -> Result<Change<MailAckResponse>> {
        request.validate()?;
        let mailbox = self.resolve_mailbox(principal, request.agent.as_deref())?;
        let semantic_payload = json!({
            "agent": mailbox_name(mailbox),
            "deliveries": request.deliveries,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "mail write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != MAIL_ACK_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "mail write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<MailAckResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        for delivery in &request.deliveries {
            let found: bool = tx
                .query_row(
                    "SELECT EXISTS(
                         SELECT 1 FROM messages WHERE delivery_id = ?1 AND recipient = ?2
                     )",
                    params![delivery, mailbox],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            // A gap-eligible absent id may have been acked and then pruned
            // (pruning deletes acknowledged rows only), or never issued;
            // either way succeeding changes nothing, while failing would
            // break a retried ack over removed history. The sibling update
            // below is a no-op for it. Every other absent id still rejects
            // the whole batch, and the remainder can only read `not_found`.
            if !found && !delivery_may_be_pruned(delivery, mailbox, now) {
                return Err(absent_delivery_error(delivery, mailbox, now));
            }
        }
        for delivery in &request.deliveries {
            db!(tx.execute(
                "UPDATE messages SET read_at = ?1
                 WHERE delivery_id = ?2 AND recipient = ?3 AND read_at IS NULL",
                params![now, delivery, mailbox],
            ));
        }
        let value = MailAckResponse {
            acked: request.deliveries.clone(),
        };
        request.validate_response(&value)?;
        retain_write_tx(
            &tx,
            &request.id,
            principal,
            MAIL_ACK_METHOD,
            &payload_digest,
            &serde_json::to_value(&value)
                .map_err(|error| Error::new("internal", error.to_string()))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value,
            events: Vec::new(),
        })
    }

    fn var_set_inner(
        &mut self,
        principal: &str,
        request: &VarSetRequest,
        now: i64,
    ) -> Result<Change<VarSetResponse>> {
        request.validate()?;
        let scope = self.resolve_var_scope(principal, request.scope.as_deref(), true)?;
        let encoded_scope = scope.encoded();
        let encoded_value = encode_json(&request.value)?;
        if encoded_value.len() > eliot_core::api::MAX_VAR_VALUE_BYTES {
            return Err(Error::new(
                "invalid",
                "variable value exceeds the 16 KiB bound",
            ));
        }
        let semantic_payload = json!({
            "scope": encoded_scope,
            "key": request.key,
            "value": request.value,
            "expect_version": request.expect_version,
            "note": request.note,
        });
        let payload_digest = canonical::digest(&semantic_payload)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        if command_binding(&tx, &request.id)?.is_some() {
            return Err(Error::new(
                "id_conflict",
                "variable write id is already used by a command",
            ));
        }
        if let Some((old_principal, old_method, old_digest, old_result)) =
            write_binding(&tx, &request.id)?
        {
            if old_principal != principal
                || old_method != VAR_SET_METHOD
                || old_digest != payload_digest
            {
                return Err(Error::new(
                    "id_conflict",
                    "variable write id is bound to another principal, method, or payload",
                ));
            }
            let value = serde_json::from_value::<VarSetResponse>(decode_json(&old_result)?)
                .map_err(|error| Error::new("internal", error.to_string()))?;
            request.validate_response(&value)?;
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(Change {
                value,
                events: Vec::new(),
            });
        }
        validate_id_age(&request.id, now)?;

        let current: Option<(String, i64)> = tx
            .query_row(
                "SELECT value, version FROM vars WHERE scope = ?1 AND key = ?2",
                params![encoded_scope, request.key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(sql_error)?;
        let (current_value, current) = match current {
            Some((value, version)) => (Some(value), version),
            None => (None, 0),
        };
        if request
            .expect_version
            .is_some_and(|expected| expected != current)
        {
            let mut message = format!(
                "variable version does not match the expectation (current version: {current}"
            );
            if let Some(value) = current_value.as_deref()
                && value.len() <= eliot_core::api::MAX_VAR_VALUE_BYTES
            {
                message.push_str(", value: ");
                message.push_str(value);
            }
            message.push(')');
            return Err(Error::new("version_conflict", message));
        }
        let version = current + 1;
        db!(tx.execute(
            "INSERT INTO vars(scope, key, value, version, set_by, note, set_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(scope, key) DO UPDATE SET
                 value = excluded.value, version = excluded.version,
                 set_by = excluded.set_by, note = excluded.note, set_at = excluded.set_at",
            params![
                encoded_scope,
                request.key,
                encoded_value,
                version,
                principal,
                request.note,
                now
            ],
        ));
        let event = insert_daemon_event(
            &tx,
            now,
            "var.changed",
            "coordination",
            None,
            None,
            json!({
                "scope": encoded_scope,
                "key": request.key,
                "version": version,
                "set_by": principal,
            }),
        )?;
        let value = VarSetResponse {
            scope: encoded_scope,
            key: request.key.clone(),
            version,
            set_by: principal.to_owned(),
        };
        request.validate_response(&value)?;
        retain_write_tx(
            &tx,
            &request.id,
            principal,
            VAR_SET_METHOD,
            &payload_digest,
            &serde_json::to_value(&value)
                .map_err(|error| Error::new("internal", error.to_string()))?,
            now,
        )?;
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Change {
            value,
            events: vec![event],
        })
    }

    fn notice_claim_inner(
        &mut self,
        recipient: &str,
        notice_command: &str,
        pending: NoticePending,
        now: i64,
    ) -> Result<Option<NoticeClaim>> {
        let num = parse_agent_id(recipient)?;
        if notice_command.trim().is_empty() {
            return Err(Error::new("invalid", "notice command id must not be empty"));
        }
        if let NoticePending::Claim { overlapping } = pending
            && overlapping < 1
        {
            return Err(Error::new(
                "invalid",
                "claim notice needs a command id and a conflict",
            ));
        }
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let agent = load_agent_by_num(&tx, num)?;
        let Some(agent) = agent else {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(None);
        };
        let count = match notice_pending_count_tx(&tx, pending, num)? {
            Some(count) => count,
            None => {
                commit_transaction(tx, &self.write_faulted)?;
                return Ok(None);
            }
        };
        if notice_suppressed_tx(&tx, num, &agent, now)? {
            commit_transaction(tx, &self.write_faulted)?;
            return Ok(None);
        }
        db!(tx.execute(
            "INSERT INTO mail_notices(recipient, last_notice_at, open_notice_command)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(recipient) DO UPDATE SET
                 last_notice_at = excluded.last_notice_at,
                 open_notice_command = excluded.open_notice_command",
            params![num, now, notice_command],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(Some(NoticeClaim {
            kind: pending.kind(),
            count,
            generation: agent.generation,
        }))
    }

    fn notice_release_inner(&mut self, recipient: &str, notice_command: &str) -> Result<()> {
        let num = parse_agent_id(recipient)?;
        let tx = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        db!(tx.execute(
            "DELETE FROM mail_notices WHERE recipient = ?1 AND open_notice_command = ?2",
            params![num, notice_command],
        ));
        commit_transaction(tx, &self.write_faulted)?;
        Ok(())
    }
}

/// Resolve the pending proof inside the claim transaction: mail counts
/// its unread rows, claims arrive with their overlap count. `None`
/// suppresses the notice without recording anything.
fn notice_pending_count_tx(
    tx: &Transaction<'_>,
    pending: NoticePending,
    num: i64,
) -> Result<Option<i64>> {
    match pending {
        NoticePending::Mail => {
            let unread: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM messages WHERE recipient = ?1 AND read_at IS NULL",
                    params![num],
                    |row| row.get(0),
                )
                .map_err(sql_error)?;
            Ok((unread != 0).then_some(unread))
        }
        NoticePending::Claim { overlapping } => Ok(Some(overlapping)),
    }
}

/// Whether the shared suppression gates skip this notice: the cooldown, an
/// open or unknown notice command, an unknown command or turn, a pending
/// request, or a provider hold. A suppression records nothing.
fn notice_suppressed_tx(tx: &Transaction<'_>, num: i64, agent: &Agent, now: i64) -> Result<bool> {
    let notice: Option<(i64, Option<String>)> = tx
        .query_row(
            "SELECT last_notice_at, open_notice_command FROM mail_notices WHERE recipient = ?1",
            params![num],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    if notice
        .as_ref()
        .is_some_and(|(last, _)| now.saturating_sub(*last) < MAIL_NOTICE_COOLDOWN_MS)
    {
        return Ok(true);
    }
    if let Some((_, Some(open))) = notice.as_ref() {
        let blocking = load_command_record(tx, open)?.is_some_and(|record| {
            matches!(
                record.state,
                CommandState::Accepted
                    | CommandState::Queued
                    | CommandState::Dispatched
                    | CommandState::Unknown
            )
        });
        if blocking {
            return Ok(true);
        }
    }
    let unknown_command: bool = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM commands WHERE target = ?1 AND state = 'unknown'
             )",
            params![num],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if unknown_command {
        return Ok(true);
    }
    let unknown_turn: bool = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM turns WHERE agent = ?1 AND state = ?2
             )",
            params![num, turn_state_name(TurnState::Unknown)],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if unknown_turn {
        return Ok(true);
    }
    let pending_request: bool = tx
        .query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM pending_requests
                 WHERE agent = ?1 AND generation = ?2 AND state = 'pending'
             )",
            params![num, agent.generation],
            |row| row.get(0),
        )
        .map_err(sql_error)?;
    if pending_request {
        return Ok(true);
    }
    match provider_holds::require_unheld_tx(tx, agent) {
        Ok(()) => Ok(false),
        Err(error) if error.code == "provider_held" => Ok(true),
        Err(error) => Err(error),
    }
}

fn mailbox_name(num: i64) -> String {
    format!("a{num}")
}

/// Whether an absent delivery of an already-authorized mailbox may have been
/// pruned: the recipient suffix must name this mailbox, and the write id must
/// be older than the retry window plus the allowed clock skew, exactly the
/// admission `id_expired` age rule shared with command history. Retention
/// keeps every younger mail row, so any other absent delivery never existed.
/// Pure: a function of the caller's own input, so it leaks nothing across
/// mailboxes — a foreign suffix never reads a gap.
fn delivery_may_be_pruned(delivery_id: &str, mailbox: i64, now: i64) -> bool {
    let Some((head, tail)) = delivery_id.split_once('/') else {
        return false;
    };
    if tail != mailbox_name(mailbox) {
        return false;
    }
    history_expired(head, now)
}

/// Error for an absent delivery of an already-authorized mailbox:
/// `history_gap` when the delivery may have been pruned, else `not_found`.
fn absent_delivery_error(delivery_id: &str, mailbox: i64, now: i64) -> Error {
    if delivery_may_be_pruned(delivery_id, mailbox, now) {
        Error::new(
            "history_gap",
            "mail delivery is no longer retained; unread mail is never pruned",
        )
    } else {
        Error::new("not_found", "mail delivery was not found")
    }
}

pub(super) fn retain_write_tx(
    tx: &Transaction<'_>,
    id: &str,
    principal: &str,
    method: &str,
    payload_digest: &[u8],
    result: &Value,
    now: i64,
) -> Result<()> {
    let result_json = encode_json(result)?;
    if result_json.len() > MAX_EVENT_BYTES {
        return Err(Error::new(
            "frame_too_large",
            "coordination write result exceeds 64 KiB",
        ));
    }
    db!(tx.execute(
        "INSERT INTO writes(id, principal, method, payload_digest, result, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, principal, method, payload_digest, result_json, now],
    ));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{NativeEvent, SourceBatch, SubmitCommand};
    use std::{
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    };
    use ulid::Ulid;

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
    }

    fn store() -> Store {
        Store::open(Path::new(":memory:")).unwrap()
    }

    fn ready_agent(store: &mut Store, project: &str, now: i64) -> String {
        let launch_id = Ulid::new().to_string();
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":project, "workspace":"synthetic-workspace"}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        store
            .put_batch(
                SourceBatch {
                    batch_id: Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: Ulid::new().to_string(),
                        data: json!({"command_id":launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    fn agent_principal(agent: &str) -> String {
        format!("agent:{agent}:1")
    }

    fn send_request(to: Vec<String>, text: &str) -> MailSendRequest {
        MailSendRequest {
            id: Ulid::new().to_string(),
            to,
            mode: MailMode::Queue,
            text: text.to_owned(),
            thread: None,
            reply_to_delivery: None,
        }
    }

    fn unread(store: &Store, recipient: &str) -> i64 {
        let num = parse_agent_id(recipient).unwrap();
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE recipient = ?1 AND read_at IS NULL",
                params![num],
                |row| row.get(0),
            )
            .unwrap()
    }

    const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

    fn ulid_at(ts_ms: i64, tag: u8) -> String {
        let mut bytes = [0_u8; 16];
        bytes[..6].copy_from_slice(&(ts_ms as u64).to_be_bytes()[2..8]);
        bytes[6..].copy_from_slice(&[tag; 10]);
        Ulid::from_bytes(bytes).to_string()
    }

    fn insert_message(
        store: &mut Store,
        delivery_id: &str,
        recipient: i64,
        admitted_at: i64,
        read_at: Option<i64>,
    ) {
        store
            .connection
            .execute(
                "INSERT INTO messages(delivery_id, sender, recipient, recipient_generation,
                                      mode, text, content_digest, admitted_at, read_at)
                 VALUES (?1, 'owner', ?2, 1, 'queue', 'seeded', x'00', ?3, ?4)",
                params![delivery_id, recipient, admitted_at, read_at],
            )
            .unwrap();
    }

    #[test]
    fn mail_migration_installs_v24_tables_and_prune_index() {
        let store = store();
        let version: i64 = store
            .connection
            .query_row("SELECT MAX(version) FROM schema_migrations", [], |row| {
                row.get(0)
            })
            .expect("read Store migration version");
        assert_eq!(version, super::super::schema::CURRENT_SCHEMA_VERSION);
        assert_eq!(version, 26);
        for table in ["messages", "vars", "mail_notices", "claims"] {
            let found: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    params![table],
                    |row| row.get(0),
                )
                .expect("count coordination table");
            assert_eq!(found, 1, "missing {table}");
        }
        for index in ["messages_prune_acked", "claims_expiry"] {
            let found: i64 = store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
                    params![index],
                    |row| row.get(0),
                )
                .expect("count coordination index");
            assert_eq!(found, 1, "missing {index}");
        }
    }

    #[test]
    fn mail_send_inbox_read_ack_roundtrip() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let b = ready_agent(&mut store, "app", now);
        let sent = store
            .mail_send(
                "owner",
                &send_request(vec![a.clone(), b.clone()], "hello"),
                MailLimits::default(),
                now,
            )
            .expect("queue mail")
            .value;
        assert_eq!(sent.deliveries.len(), 2);
        assert_eq!(sent.deliveries[0].recipient, a);
        assert_eq!(sent.deliveries[1].recipient, b);
        assert_eq!(sent.deliveries[0].recipient_generation, 1);

        // Reads are read-only: repeated reads return the same rows.
        let inbox = MailInboxRequest {
            agent: None,
            limit: None,
            after: None,
        };
        let first = store
            .mail_inbox(&agent_principal(&b), &inbox, now)
            .expect("read inbox");
        assert_eq!(first.deliveries.len(), 1);
        assert_eq!(first.deliveries[0].text, "hello");
        assert_eq!(first.deliveries[0].read_at, None);
        assert_eq!(first.next, Some(first.deliveries[0].delivery_id.clone()));
        let second = store
            .mail_inbox(&agent_principal(&b), &inbox, now)
            .expect("reread inbox");
        assert_eq!(second, first);
        let one = store
            .mail_read(
                &agent_principal(&b),
                &MailReadRequest {
                    agent: None,
                    delivery_id: first.deliveries[0].delivery_id.clone(),
                },
                now,
            )
            .expect("read one delivery");
        assert_eq!(one, first.deliveries[0]);
        assert_eq!(unread(&store, &b), 1);

        // Only an explicit ack over the exact id marks it.
        let ack = MailAckRequest {
            id: Ulid::new().to_string(),
            agent: None,
            deliveries: vec![first.deliveries[0].delivery_id.clone()],
        };
        let acked = store
            .mail_ack(&agent_principal(&b), &ack, now)
            .expect("ack delivery")
            .value;
        assert_eq!(acked.acked, ack.deliveries);
        assert_eq!(unread(&store, &b), 0);
        let empty = store
            .mail_inbox(&agent_principal(&b), &inbox, now)
            .expect("inbox after ack");
        assert!(empty.deliveries.is_empty());
        assert_eq!(empty.next, None);
        // Ack is idempotent, and the read row stays readable.
        store
            .mail_ack(&agent_principal(&b), &ack, now)
            .expect("replay ack");
        let read = store
            .mail_read(
                &agent_principal(&b),
                &MailReadRequest {
                    agent: None,
                    delivery_id: first.deliveries[0].delivery_id.clone(),
                },
                now,
            )
            .expect("read acked delivery");
        assert!(read.read_at.is_some());
        // The other recipient still holds its own unread row.
        assert_eq!(unread(&store, &a), 1);
    }

    #[test]
    fn mail_inbox_truncates_large_pages_within_byte_bound() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let principal = agent_principal(&a);
        // 50 messages of 32 KiB each: 1.6 MiB total, well under the 10 MiB
        // mailbox bound but over the 900 KiB page bound.
        let text = "y".repeat(32 * 1024);
        for _ in 0..50 {
            store
                .mail_send(
                    "owner",
                    &send_request(vec![a.clone()], &text),
                    MailLimits::default(),
                    now,
                )
                .expect("queue large mail");
        }
        assert_eq!(unread(&store, &a), 50);
        let first = store
            .mail_inbox(
                &principal,
                &MailInboxRequest {
                    agent: None,
                    limit: Some(100),
                    after: None,
                },
                now,
            )
            .expect("read truncated inbox");
        // The page fits the byte bound and keeps `next` on its last row.
        assert!(
            first.deliveries.len() < 50,
            "expected truncation, got {}",
            first.deliveries.len()
        );
        let encoded = serde_json::to_string(&first.deliveries).expect("encode page");
        assert!(
            encoded.len() <= 900 * 1024,
            "page exceeds 900 KiB: {}",
            encoded.len()
        );
        assert_eq!(
            first.next,
            Some(
                first
                    .deliveries
                    .last()
                    .expect("nonempty page")
                    .delivery_id
                    .clone()
            )
        );
        // Continuation returns the remainder with no loss or duplication.
        let mut seen: Vec<String> = first
            .deliveries
            .iter()
            .map(|delivery| delivery.delivery_id.clone())
            .collect();
        let mut after = first.next.clone();
        loop {
            let page = store
                .mail_inbox(
                    &principal,
                    &MailInboxRequest {
                        agent: None,
                        limit: Some(100),
                        after: after.clone(),
                    },
                    now,
                )
                .expect("read continuation page");
            if page.deliveries.is_empty() {
                assert_eq!(page.next, None);
                break;
            }
            assert_eq!(
                page.next,
                Some(
                    page.deliveries
                        .last()
                        .expect("nonempty page")
                        .delivery_id
                        .clone()
                )
            );
            for delivery in &page.deliveries {
                assert!(
                    !seen.contains(&delivery.delivery_id),
                    "duplicate delivery {}",
                    delivery.delivery_id
                );
                seen.push(delivery.delivery_id.clone());
            }
            after = page.next.clone();
        }
        assert_eq!(seen.len(), 50, "all large messages are reachable");
    }

    #[test]
    fn mail_send_replays_and_rejects_rebinding() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let request = send_request(vec![a.clone()], "same");
        let first = store
            .mail_send("owner", &request, MailLimits::default(), now)
            .expect("send")
            .value;
        let replay = store
            .mail_send("owner", &request, MailLimits::default(), now)
            .expect("replay")
            .value;
        assert_eq!(replay, first);
        assert_eq!(unread(&store, &a), 1);

        let mut rebound = request.clone();
        rebound.text = "different".to_owned();
        assert_eq!(
            store
                .mail_send("owner", &rebound, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "id_conflict"
        );
        assert_eq!(
            store
                .mail_send(&agent_principal(&a), &request, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "id_conflict"
        );
        let mut bad_id = send_request(vec![a.clone()], "bad");
        bad_id.id = "not-a-ulid".to_owned();
        assert_eq!(
            store
                .mail_send("owner", &bad_id, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "invalid"
        );
        let mut expired = send_request(vec![a.clone()], "old");
        expired.id = Ulid::from_parts(1, 1).to_string();
        assert_eq!(
            store
                .mail_send("owner", &expired, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "id_expired"
        );
        let mut presented = send_request(vec![a.clone()], "live");
        presented.mode = MailMode::Send;
        assert_eq!(
            store
                .mail_send("owner", &presented, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "unsupported"
        );
    }

    #[test]
    fn mail_send_rejects_unknown_recipients_and_foreign_projects() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let before = store.list_agents(None, 100).unwrap().len();
        let error = store
            .mail_send(
                "owner",
                &send_request(vec!["a999".to_owned()], "ghost"),
                MailLimits::default(),
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_found");
        assert_eq!(store.list_agents(None, 100).unwrap().len(), before);

        let b = ready_agent(&mut store, "other", now);
        let error = store
            .mail_send(
                &agent_principal(&a),
                &send_request(vec![b.clone()], "cross"),
                MailLimits::default(),
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
        let error = store
            .mail_send(
                &format!("agent:{a}:9"),
                &send_request(vec![a.clone()], "stale"),
                MailLimits::default(),
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "stale_generation");
        // Same-project Agent mail works.
        let c = ready_agent(&mut store, "app", now);
        store
            .mail_send(
                &agent_principal(&a),
                &send_request(vec![c.clone()], "peer"),
                MailLimits::default(),
                now,
            )
            .expect("peer mail");
        assert_eq!(unread(&store, &c), 1);
    }

    #[test]
    fn stale_agent_principal_cannot_read_ack_or_write_vars() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let stale = agent_principal(&a);
        let sent = store
            .mail_send(
                "owner",
                &send_request(vec![a.clone()], "hi"),
                MailLimits::default(),
                now,
            )
            .expect("queue mail")
            .value;
        let delivery = sent.deliveries[0].delivery_id.clone();
        store
            .mail_inbox(
                &stale,
                &MailInboxRequest {
                    agent: None,
                    limit: None,
                    after: None,
                },
                now,
            )
            .expect("inbox while current");
        // Advance the generation: the gen-1 principal is now previous.
        let num = parse_agent_id(&a).unwrap();
        store
            .connection
            .execute(
                "UPDATE agents SET generation = 2 WHERE id = ?1",
                params![num],
            )
            .unwrap();
        let current = format!("agent:{a}:2");
        let inbox = MailInboxRequest {
            agent: None,
            limit: None,
            after: None,
        };
        assert_eq!(
            store.mail_inbox(&stale, &inbox, now).unwrap_err().code,
            "stale_generation"
        );
        assert_eq!(
            store
                .mail_read(
                    &stale,
                    &MailReadRequest {
                        agent: None,
                        delivery_id: delivery.clone(),
                    },
                    now
                )
                .unwrap_err()
                .code,
            "stale_generation"
        );
        assert_eq!(
            store
                .mail_ack(
                    &stale,
                    &MailAckRequest {
                        id: Ulid::new().to_string(),
                        agent: None,
                        deliveries: vec![delivery.clone()],
                    },
                    now,
                )
                .unwrap_err()
                .code,
            "stale_generation"
        );
        let var_set = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: Some("project:app".to_owned()),
            key: "k".to_owned(),
            value: json!(1),
            expect_version: None,
            note: None,
        };
        assert_eq!(
            store.var_set(&stale, &var_set, now).unwrap_err().code,
            "stale_generation"
        );
        assert_eq!(
            store
                .var_get(
                    &stale,
                    &VarGetRequest {
                        scope: Some("project:app".to_owned()),
                        key: "k".to_owned(),
                    },
                )
                .unwrap_err()
                .code,
            "stale_generation"
        );
        // The current generation still reads, acks, and writes.
        store
            .mail_inbox(&current, &inbox, now)
            .expect("inbox with current generation");
        store
            .mail_read(
                &current,
                &MailReadRequest {
                    agent: None,
                    delivery_id: delivery.clone(),
                },
                now,
            )
            .expect("read with current generation");
        store
            .var_set(&current, &var_set, now)
            .expect("var write with current generation");
        store
            .mail_ack(
                &current,
                &MailAckRequest {
                    id: Ulid::new().to_string(),
                    agent: None,
                    deliveries: vec![delivery],
                },
                now,
            )
            .expect("ack with current generation");
    }

    #[test]
    fn mailbox_default_limits_match_the_coordination_contract() {
        assert_eq!(
            MailLimits::default(),
            MailLimits {
                max_unread_bytes_per_agent: 10 * 1024 * 1024,
                max_unread_messages_per_agent: 10_000,
            }
        );
        MailLimits::default()
            .validate()
            .expect("defaults are valid");
    }

    #[test]
    fn mailbox_bounds_reject_without_partial_fanout() {
        let mut store = store();
        let now = now_ms();
        // Non-default bounds prove the plumbed limits are honored, not the
        // compiled-in contract defaults.
        let limits = MailLimits {
            max_unread_bytes_per_agent: 16,
            max_unread_messages_per_agent: 3,
        };
        let a = ready_agent(&mut store, "app", now);
        let b = ready_agent(&mut store, "app", now);
        for _ in 0..3 {
            store
                .mail_send("owner", &send_request(vec![b.clone()], "hi"), limits, now)
                .expect("fill the count bound");
        }
        let error = store
            .mail_send(
                "owner",
                &send_request(vec![a.clone(), b.clone()], "one more"),
                limits,
                now,
            )
            .unwrap_err();
        assert_eq!(error.code, "mailbox_full");
        assert_eq!(unread(&store, &a), 0, "no partial fanout to a");
        assert_eq!(unread(&store, &b), 3);

        // The byte bound admits an exact fill and refuses one more byte.
        let big = ready_agent(&mut store, "app", now);
        store
            .mail_send(
                "owner",
                &send_request(vec![big.clone()], "0123456789abcdef"),
                limits,
                now,
            )
            .expect("fill the byte bound exactly");
        let error = store
            .mail_send("owner", &send_request(vec![big.clone()], "y"), limits, now)
            .unwrap_err();
        assert_eq!(error.code, "mailbox_full");
    }

    #[test]
    fn mailbox_byte_bound_counts_bytes_not_characters() {
        let mut store = store();
        let now = now_ms();
        // 'é' is 2 bytes in UTF-8 but 1 character: with a 4-byte bound, two
        // of them fill it while counting only 2 characters. A
        // character-counting sum would admit two more bytes; a byte sum
        // refuses one more.
        let limits = MailLimits {
            max_unread_bytes_per_agent: 4,
            ..MailLimits::default()
        };
        let a = ready_agent(&mut store, "app", now);
        store
            .mail_send("owner", &send_request(vec![a.clone()], "éé"), limits, now)
            .expect("fill the byte bound exactly");
        let error = store
            .mail_send("owner", &send_request(vec![a.clone()], "y"), limits, now)
            .unwrap_err();
        assert_eq!(error.code, "mailbox_full");
    }

    #[test]
    fn mail_send_rejects_nonpositive_limits() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        for limits in [
            MailLimits {
                max_unread_bytes_per_agent: 0,
                ..MailLimits::default()
            },
            MailLimits {
                max_unread_messages_per_agent: 0,
                ..MailLimits::default()
            },
            MailLimits {
                max_unread_bytes_per_agent: -1,
                max_unread_messages_per_agent: -1,
            },
        ] {
            let error = store
                .mail_send("owner", &send_request(vec![a.clone()], "hi"), limits, now)
                .unwrap_err();
            assert_eq!(error.code, "invalid");
        }
        assert_eq!(unread(&store, &a), 0);
        assert!(!store.is_write_faulted());
    }

    #[test]
    fn removed_mail_history_reads_history_gap() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let b = ready_agent(&mut store, "app", now);
        let num_a = parse_agent_id(&a).unwrap();
        let old = now - 31 * DAY_MS;
        let pruned_id = format!("{}/{}", ulid_at(old, 1), a);
        let kept_id = format!("{}/{}", Ulid::new(), a);
        insert_message(&mut store, &pruned_id, num_a, old, Some(old));
        insert_message(&mut store, &kept_id, num_a, now, Some(now));

        let report = store.prune_retention(None, None, Some(30), now).unwrap();
        assert_eq!(report.mail_deleted, 1);
        assert_eq!(report.events_deleted, 0);
        assert_eq!(report.commands_deleted, 0);
        assert_eq!(report.writes_deleted, 0);

        // The removed delivery reads history_gap on every read path (ack
        // instead succeeds as already acked; see
        // retried_ack_after_prune_succeeds_for_pruned_ids).
        let read = MailReadRequest {
            agent: None,
            delivery_id: pruned_id.clone(),
        };
        assert_eq!(
            store
                .mail_read(&agent_principal(&a), &read, now)
                .unwrap_err()
                .code,
            "history_gap"
        );
        let cursor = MailInboxRequest {
            agent: None,
            limit: None,
            after: Some(pruned_id.clone()),
        };
        assert_eq!(
            store
                .mail_inbox(&agent_principal(&a), &cursor, now)
                .unwrap_err()
                .code,
            "history_gap"
        );
        let mut reply = send_request(vec![a.clone()], "reply");
        reply.reply_to_delivery = Some(pruned_id.clone());
        assert_eq!(
            store
                .mail_send("owner", &reply, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "history_gap"
        );

        // Young or foreign ids still read their old errors, and the kept row
        // still reads.
        let young = MailReadRequest {
            agent: None,
            delivery_id: format!("{}/{}", Ulid::new(), a),
        };
        assert_eq!(
            store
                .mail_read(&agent_principal(&a), &young, now)
                .unwrap_err()
                .code,
            "not_found"
        );
        let foreign = MailReadRequest {
            agent: None,
            delivery_id: format!("{}/{}", ulid_at(old, 2), b),
        };
        assert_eq!(
            store
                .mail_read(&agent_principal(&a), &foreign, now)
                .unwrap_err()
                .code,
            "not_found"
        );
        let young_cursor = MailInboxRequest {
            agent: None,
            limit: None,
            after: Some(format!("{}/{}", Ulid::new(), a)),
        };
        assert_eq!(
            store
                .mail_inbox(&agent_principal(&a), &young_cursor, now)
                .unwrap_err()
                .code,
            "invalid"
        );
        let mut young_reply = send_request(vec![a.clone()], "reply");
        young_reply.reply_to_delivery = Some(format!("{}/{}", Ulid::new(), a));
        assert_eq!(
            store
                .mail_send("owner", &young_reply, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "not_found"
        );
        let kept = MailReadRequest {
            agent: None,
            delivery_id: kept_id,
        };
        store
            .mail_read(&agent_principal(&a), &kept, now)
            .expect("young acked mail still reads");
    }

    #[test]
    fn mail_write_ids_beyond_clock_skew_in_the_future_are_refused() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let principal = agent_principal(&a);
        let skew = super::super::RETRY_CLOCK_SKEW_MS;
        // An id more than the allowed skew ahead of admission is refused on
        // all three write paths through the shared command id rule.
        let mut ahead = send_request(vec![a.clone()], "future");
        ahead.id = ulid_at(now + skew + 1, 1);
        assert_eq!(
            store
                .mail_send("owner", &ahead, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "invalid"
        );
        let ack = MailAckRequest {
            id: ulid_at(now + skew + 1, 2),
            agent: None,
            deliveries: vec![format!("{}/{}", ulid_at(now, 3), a)],
        };
        assert_eq!(
            store.mail_ack(&principal, &ack, now).unwrap_err().code,
            "invalid"
        );
        let var = VarSetRequest {
            id: ulid_at(now + skew + 1, 4),
            scope: Some("project:app".to_owned()),
            key: "k".to_owned(),
            value: json!(1),
            expect_version: None,
            note: None,
        };
        assert_eq!(
            store.var_set(&principal, &var, now).unwrap_err().code,
            "invalid"
        );
        assert_eq!(unread(&store, &a), 0);
        // Exactly the skew ahead still admits: the boundary favors the writer.
        let mut edge = send_request(vec![a.clone()], "edge");
        edge.id = ulid_at(now + skew, 5);
        store
            .mail_send("owner", &edge, MailLimits::default(), now)
            .expect("skew-boundary send admits");
        assert_eq!(unread(&store, &a), 1);
    }

    #[test]
    fn replay_of_a_pruned_message_id_is_refused_never_readmitted() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let principal = agent_principal(&a);
        let mut request = send_request(vec![a.clone()], "short-lived");
        request.id = ulid_at(now, 1);
        let sent = store
            .mail_send("owner", &request, MailLimits::default(), now)
            .expect("send")
            .value;
        let delivery = sent.deliveries[0].delivery_id.clone();
        store
            .mail_ack(
                &principal,
                &MailAckRequest {
                    id: ulid_at(now, 2),
                    agent: None,
                    deliveries: vec![delivery.clone()],
                },
                now,
            )
            .expect("ack");
        // Past both windows, one pass removes the mail row and the send's
        // write binding: the send and the ack bindings share the commands
        // cutoff, so both go.
        let later = now + 31 * DAY_MS;
        let report = store
            .prune_retention(None, Some(30), Some(30), later)
            .unwrap();
        assert_eq!(report.mail_deleted, 1);
        assert_eq!(report.writes_deleted, 2);
        // Replaying the pruned send id is refused: it reads expired, never
        // re-admits as new mail.
        assert_eq!(
            store
                .mail_send("owner", &request, MailLimits::default(), later)
                .unwrap_err()
                .code,
            "id_expired"
        );
        let rows: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
            .unwrap();
        assert_eq!(rows, 0, "a pruned send id must not fan out again");
        // And the removed delivery still reads history_gap on read paths.
        assert_eq!(
            store
                .mail_read(
                    &principal,
                    &MailReadRequest {
                        agent: None,
                        delivery_id: delivery,
                    },
                    later,
                )
                .unwrap_err()
                .code,
            "history_gap"
        );
    }

    #[test]
    fn retried_ack_after_prune_succeeds_for_pruned_ids() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let b = ready_agent(&mut store, "app", now);
        let principal = agent_principal(&a);
        let first = store
            .mail_send(
                "owner",
                &send_request(vec![a.clone()], "one"),
                MailLimits::default(),
                now,
            )
            .expect("send one")
            .value
            .deliveries[0]
            .delivery_id
            .clone();
        let second = store
            .mail_send(
                "owner",
                &send_request(vec![a.clone()], "two"),
                MailLimits::default(),
                now,
            )
            .expect("send two")
            .value
            .deliveries[0]
            .delivery_id
            .clone();
        // Ack only the first; the second stays unread so it survives pruning.
        store
            .mail_ack(
                &principal,
                &MailAckRequest {
                    id: Ulid::new().to_string(),
                    agent: None,
                    deliveries: vec![first.clone()],
                },
                now,
            )
            .expect("ack first");
        let later = now + 31 * DAY_MS;
        let report = store.prune_retention(None, None, Some(30), later).unwrap();
        assert_eq!(report.mail_deleted, 1);
        // A retried batch over the pruned id and a live sibling succeeds for
        // both: the pruned id counts as already acked, the sibling gets acked.
        let retry = MailAckRequest {
            id: ulid_at(later, 7),
            agent: None,
            deliveries: vec![first.clone(), second.clone()],
        };
        let acked = store
            .mail_ack(&principal, &retry, later)
            .expect("retried ack succeeds")
            .value;
        assert_eq!(acked.acked, vec![first.clone(), second.clone()]);
        assert_eq!(unread(&store, &a), 0);
        // Foreign and young-unknown ids still reject the whole batch
        // atomically: nothing is marked.
        let mut fresh = send_request(vec![a.clone()], "three");
        fresh.id = ulid_at(later, 8);
        let third = store
            .mail_send("owner", &fresh, MailLimits::default(), later)
            .expect("send three")
            .value
            .deliveries[0]
            .delivery_id
            .clone();
        let mut to_b = send_request(vec![b.clone()], "for b");
        to_b.id = ulid_at(later, 10);
        let foreign = store
            .mail_send("owner", &to_b, MailLimits::default(), later)
            .expect("send to b")
            .value
            .deliveries[0]
            .delivery_id
            .clone();
        let bad = MailAckRequest {
            id: ulid_at(later, 11),
            agent: None,
            deliveries: vec![third.clone(), foreign.clone()],
        };
        assert_eq!(
            store.mail_ack(&principal, &bad, later).unwrap_err().code,
            "not_found"
        );
        assert_eq!(unread(&store, &a), 1, "failed ack marks nothing");
        assert_eq!(unread(&store, &b), 1);
        let young = MailAckRequest {
            id: ulid_at(later, 12),
            agent: None,
            deliveries: vec![format!("{}/{}", ulid_at(later, 13), a)],
        };
        assert_eq!(
            store.mail_ack(&principal, &young, later).unwrap_err().code,
            "not_found"
        );
        assert_eq!(unread(&store, &a), 1);
    }

    #[test]
    fn mail_ack_rejects_unknown_and_foreign_ids_atomically() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let b = ready_agent(&mut store, "app", now);
        let sent = store
            .mail_send(
                "owner",
                &send_request(vec![a.clone(), b.clone()], "hi"),
                MailLimits::default(),
                now,
            )
            .expect("send")
            .value;
        let own = sent.deliveries[0].delivery_id.clone();
        let foreign = sent.deliveries[1].delivery_id.clone();

        let ack = MailAckRequest {
            id: Ulid::new().to_string(),
            agent: None,
            deliveries: vec![own.clone(), foreign.clone()],
        };
        assert_eq!(
            store
                .mail_ack(&agent_principal(&a), &ack, now)
                .unwrap_err()
                .code,
            "not_found"
        );
        assert_eq!(unread(&store, &a), 1, "failed ack marks nothing");
        let missing = MailAckRequest {
            id: Ulid::new().to_string(),
            agent: None,
            deliveries: vec![format!("{}/{}", Ulid::new(), a)],
        };
        assert_eq!(
            store
                .mail_ack(&agent_principal(&a), &missing, now)
                .unwrap_err()
                .code,
            "not_found"
        );
        // Owner access names the mailbox explicitly.
        let implicit = MailAckRequest {
            id: Ulid::new().to_string(),
            agent: None,
            deliveries: vec![own.clone()],
        };
        assert_eq!(
            store.mail_ack("owner", &implicit, now).unwrap_err().code,
            "invalid"
        );
        let explicit = MailAckRequest {
            agent: Some(a.clone()),
            ..implicit
        };
        store
            .mail_ack("owner", &explicit, now)
            .expect("owner explicit ack");
        assert_eq!(unread(&store, &a), 0);
        assert_eq!(unread(&store, &b), 1);
    }

    #[test]
    fn mail_replies_reference_exact_participants() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let b = ready_agent(&mut store, "app", now);
        let c = ready_agent(&mut store, "app", now);
        let sent = store
            .mail_send(
                &agent_principal(&a),
                &send_request(vec![b.clone()], "start"),
                MailLimits::default(),
                now,
            )
            .expect("start thread")
            .value;
        let parent = sent.deliveries[0].delivery_id.clone();

        let mut reply = send_request(vec![a.clone()], "back");
        reply.reply_to_delivery = Some(parent.clone());
        store
            .mail_send(&agent_principal(&b), &reply, MailLimits::default(), now)
            .expect("participant reply");

        let mut outsider_recipient = send_request(vec![c.clone()], "hijack");
        outsider_recipient.reply_to_delivery = Some(parent.clone());
        assert_eq!(
            store
                .mail_send(
                    &agent_principal(&a),
                    &outsider_recipient,
                    MailLimits::default(),
                    now
                )
                .unwrap_err()
                .code,
            "invalid"
        );
        let mut outsider_sender = send_request(vec![a.clone()], "intrude");
        outsider_sender.reply_to_delivery = Some(parent.clone());
        assert_eq!(
            store
                .mail_send(
                    &agent_principal(&c),
                    &outsider_sender,
                    MailLimits::default(),
                    now
                )
                .unwrap_err()
                .code,
            "not_allowed"
        );
        let mut missing = send_request(vec![a.clone()], "lost");
        missing.reply_to_delivery = Some(format!("{}/{}", Ulid::new(), a));
        assert_eq!(
            store
                .mail_send(&agent_principal(&b), &missing, MailLimits::default(), now)
                .unwrap_err()
                .code,
            "not_found"
        );
    }

    #[test]
    fn var_set_get_enforces_cas_and_emits_changes() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let principal = agent_principal(&a);
        let scope = "project:app".to_owned();

        let create = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: Some(scope.clone()),
            key: "branch".to_owned(),
            value: json!("main"),
            expect_version: Some(0),
            note: None,
        };
        let first = store
            .var_set(&principal, &create, now)
            .expect("create variable");
        assert_eq!(first.value.version, 1);
        assert_eq!(first.value.set_by, principal);
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.events[0].kind, "var.changed");
        assert_eq!(first.events[0].data["version"], 1);
        assert_eq!(first.events[0].data["scope"], "project:app");
        assert_eq!(first.events[0].data["set_by"], principal);

        // A blind write still increments the version.
        let blind = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: Some(scope.clone()),
            key: "branch".to_owned(),
            value: json!("dev"),
            expect_version: None,
            note: None,
        };
        let second = store.var_set(&principal, &blind, now).expect("blind write");
        assert_eq!(second.value.version, 2);

        // A stale expectation conflicts without writing, returning the
        // current version and value.
        let stale = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: Some(scope.clone()),
            key: "branch".to_owned(),
            value: json!("stale"),
            expect_version: Some(1),
            note: None,
        };
        let conflict = store.var_set(&principal, &stale, now).unwrap_err();
        assert_eq!(conflict.code, "version_conflict");
        assert!(
            conflict.message.contains("current version: 2"),
            "conflict reports version: {}",
            conflict.message
        );
        assert!(
            conflict.message.contains("\"dev\""),
            "conflict reports value: {}",
            conflict.message
        );
        // An expectation on an absent row reports version 0 with no value.
        let missing = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: Some(scope.clone()),
            key: "absent".to_owned(),
            value: json!(1),
            expect_version: Some(1),
            note: None,
        };
        let absent = store.var_set(&principal, &missing, now).unwrap_err();
        assert_eq!(absent.code, "version_conflict");
        assert!(
            absent.message.contains("current version: 0"),
            "absent reports version 0: {}",
            absent.message
        );
        assert!(
            !absent.message.contains("value:"),
            "absent reports no value: {}",
            absent.message
        );
        let read = store
            .var_get(
                &principal,
                &VarGetRequest {
                    scope: Some(scope.clone()),
                    key: "branch".to_owned(),
                },
            )
            .expect("read variable");
        assert_eq!(read.version, 2);
        assert_eq!(read.value, json!("dev"));

        // The write id replays its retained version without a new event.
        let replay = store.var_set(&principal, &blind, now).expect("replay");
        assert_eq!(replay.value.version, 2);
        assert!(replay.events.is_empty());

        // Same id, different payload, conflicts.
        let mut rebound = blind.clone();
        rebound.value = json!("other");
        assert_eq!(
            store.var_set(&principal, &rebound, now).unwrap_err().code,
            "id_conflict"
        );
    }

    #[test]
    fn var_scope_authority_separates_projects_and_global() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let principal = agent_principal(&a);

        // An omitted scope resolves to the caller's project.
        let implied = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: None,
            key: "k".to_owned(),
            value: json!(1),
            expect_version: None,
            note: None,
        };
        let written = store
            .var_set(&principal, &implied, now)
            .expect("implied project scope")
            .value;
        assert_eq!(written.scope, "project:app");
        store
            .var_get(
                &principal,
                &VarGetRequest {
                    scope: None,
                    key: "k".to_owned(),
                },
            )
            .expect("implied project read");

        let foreign = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: Some("project:other".to_owned()),
            key: "k".to_owned(),
            value: json!(1),
            expect_version: None,
            note: None,
        };
        assert_eq!(
            store.var_set(&principal, &foreign, now).unwrap_err().code,
            "not_allowed"
        );
        assert_eq!(
            store
                .var_get(
                    &principal,
                    &VarGetRequest {
                        scope: Some("project:other".to_owned()),
                        key: "k".to_owned(),
                    },
                )
                .unwrap_err()
                .code,
            "not_allowed"
        );

        let global = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: Some("global".to_owned()),
            key: "flag".to_owned(),
            value: json!(true),
            expect_version: None,
            note: None,
        };
        assert_eq!(
            store.var_set(&principal, &global, now).unwrap_err().code,
            "not_allowed"
        );
        store
            .var_set("owner", &global, now)
            .expect("owner writes global");
        store
            .var_get(
                &principal,
                &VarGetRequest {
                    scope: Some("global".to_owned()),
                    key: "flag".to_owned(),
                },
            )
            .expect("Agents read global variables");

        let owner_implied = VarSetRequest {
            id: Ulid::new().to_string(),
            scope: None,
            key: "k".to_owned(),
            value: json!(1),
            expect_version: None,
            note: None,
        };
        assert_eq!(
            store
                .var_set("owner", &owner_implied, now)
                .unwrap_err()
                .code,
            "invalid"
        );
        assert_eq!(
            store
                .var_get(
                    "owner",
                    &VarGetRequest {
                        scope: None,
                        key: "missing".to_owned(),
                    },
                )
                .unwrap_err()
                .code,
            "invalid"
        );
        assert_eq!(
            store
                .var_get(
                    "owner",
                    &VarGetRequest {
                        scope: Some("global".to_owned()),
                        key: "missing".to_owned(),
                    },
                )
                .unwrap_err()
                .code,
            "not_found"
        );
    }

    #[test]
    fn notice_claim_bounds_wakes_and_honors_suppression_gates() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        // An empty mailbox never claims.
        assert_eq!(
            store
                .notice_claim(&a, &Ulid::new().to_string(), NoticePending::Mail, now)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .notice_claim("a999", &Ulid::new().to_string(), NoticePending::Mail, now)
                .unwrap(),
            None
        );
        store
            .mail_send(
                "owner",
                &send_request(vec![a.clone()], "wake"),
                MailLimits::default(),
                now,
            )
            .expect("queue mail");

        let notice = Ulid::new().to_string();
        let claim = store
            .notice_claim(&a, &notice, NoticePending::Mail, now)
            .expect("claim notice")
            .expect("first wake");
        assert_eq!(claim.kind, NoticeKind::Mail);
        assert_eq!(claim.count, 1);
        assert_eq!(claim.generation, 1);
        let state = store.notice_state(&a).unwrap().expect("notice state");
        assert_eq!(state.last_notice_at, now);
        assert_eq!(state.open_notice_command.as_deref(), Some(notice.as_str()));
        // The cooldown suppresses an immediate second wake.
        assert_eq!(
            store
                .notice_claim(
                    &a,
                    &Ulid::new().to_string(),
                    NoticePending::Mail,
                    now + 1_000
                )
                .unwrap(),
            None
        );

        // An open notice command still suppresses after the cooldown.
        let mut send = SubmitCommand::new(notice.clone(), "send");
        send.target = Some(a.clone());
        send.expected_generation = Some(1);
        send.args = json!({"text": "1 unread messages; run swarm inbox"});
        store.submit("owner", send, now).expect("admit notice");
        assert_eq!(
            store
                .notice_claim(
                    &a,
                    &Ulid::new().to_string(),
                    NoticePending::Mail,
                    now + 61_000
                )
                .unwrap(),
            None,
            "open notice command suppresses"
        );
        // An unknown notice is not blindly retried either.
        store.dispatch(&notice, now).expect("dispatch notice");
        store
            .settle(&notice, CommandState::Unknown, None, now)
            .expect("notice goes unknown");
        assert_eq!(
            store
                .notice_claim(
                    &a,
                    &Ulid::new().to_string(),
                    NoticePending::Mail,
                    now + 122_000
                )
                .unwrap(),
            None,
            "unknown commands gate new wakes"
        );
        // Owner resolution to a terminal state releases the gate.
        store
            .resolve_command(
                "owner",
                &eliot_core::api::CommandResolveRequest {
                    id: Ulid::new().to_string(),
                    command_id: notice.clone(),
                    resolution: eliot_core::api::CommandResolution::Failed,
                    reason: "notice never proven".to_owned(),
                },
                now,
            )
            .expect("resolve notice");
        let later = now + 183_000;
        assert!(
            store
                .notice_claim(&a, &Ulid::new().to_string(), NoticePending::Mail, later)
                .unwrap()
                .is_some()
        );

        // A pending request suppresses while it stands.
        let num = parse_agent_id(&a).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO pending_requests(agent, generation, request_id, turn_id, kind,
                                              details, state)
                 VALUES (?1, 1, 'r1', 't1', 'question', '{}', 'pending')",
                params![num],
            )
            .unwrap();
        assert_eq!(
            store
                .notice_claim(
                    &a,
                    &Ulid::new().to_string(),
                    NoticePending::Mail,
                    later + 61_000
                )
                .unwrap(),
            None
        );
        store
            .connection
            .execute(
                "UPDATE pending_requests SET state = 'resolved'
                 WHERE agent = ?1 AND request_id = 'r1'",
                params![num],
            )
            .unwrap();

        // An unknown turn suppresses while it stands.
        store
            .connection
            .execute(
                "INSERT INTO turns(agent, generation, turn_id, state)
                 VALUES (?1, 1, 't9', 'unknown')",
                params![num],
            )
            .unwrap();
        assert_eq!(
            store
                .notice_claim(
                    &a,
                    &Ulid::new().to_string(),
                    NoticePending::Mail,
                    later + 122_000
                )
                .unwrap(),
            None
        );
        store
            .connection
            .execute(
                "UPDATE turns SET state = 'completed' WHERE agent = ?1 AND turn_id = 't9'",
                params![num],
            )
            .unwrap();

        // A provider hold suppresses while it stands.
        let agent = store.get_agent(&a).unwrap().expect("recipient");
        let scope = eliot_core::frozen_provider_scope(&agent).unwrap();
        store
            .connection
            .execute(
                "INSERT INTO provider_holds(scope, kind, state, updated_at)
                 VALUES (?1, 'rate_limit', 'held', ?2)",
                params![scope, now],
            )
            .unwrap();
        assert_eq!(
            store
                .notice_claim(
                    &a,
                    &Ulid::new().to_string(),
                    NoticePending::Mail,
                    later + 183_000
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn notice_release_frees_cooldown_without_disturbing_newer_claims() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        store
            .mail_send(
                "owner",
                &send_request(vec![a.clone()], "wake"),
                MailLimits::default(),
                now,
            )
            .expect("queue mail");
        let first = Ulid::new().to_string();
        store
            .notice_claim(&a, &first, NoticePending::Mail, now)
            .expect("claim")
            .expect("first wake");
        assert!(store.notice_state(&a).unwrap().is_some());
        // A failed send releases the claim: no cooldown remains.
        store
            .notice_release(&a, &first)
            .expect("release failed claim");
        assert_eq!(store.notice_state(&a).unwrap(), None);
        // An immediate retry can claim again.
        let second = Ulid::new().to_string();
        store
            .notice_claim(&a, &second, NoticePending::Mail, now + 1)
            .expect("reclaim")
            .expect("retry wake");
        // Releasing the stale first notice leaves the newer claim standing.
        store
            .notice_release(&a, &first)
            .expect("release stale notice");
        let state = store.notice_state(&a).unwrap().expect("newer claim stands");
        assert_eq!(state.open_notice_command.as_deref(), Some(second.as_str()));
        // Releasing an unknown recipient id is a parse error, not a panic.
        assert!(store.notice_release("a0", &second).is_err());
    }

    #[test]
    fn busy_recipient_queues_notice_and_keeps_cooldown() {
        let mut store = store();
        let now = now_ms();
        let a = ready_agent(&mut store, "app", now);
        let num = parse_agent_id(&a).unwrap();
        // Busy with a running turn: sends queue instead of failing.
        store
            .connection
            .execute(
                "UPDATE agents SET state = 'busy', active_turn = 't1' WHERE id = ?1",
                params![num],
            )
            .unwrap();
        store
            .connection
            .execute(
                "INSERT INTO turns(agent, generation, turn_id, state) VALUES (?1, 1, 't1', 'running')",
                params![num],
            )
            .unwrap();
        store
            .mail_send(
                "owner",
                &send_request(vec![a.clone()], "wake"),
                MailLimits::default(),
                now,
            )
            .expect("queue mail");
        let notice = Ulid::new().to_string();
        let claim = store
            .notice_claim(&a, &notice, NoticePending::Mail, now)
            .expect("claim")
            .expect("busy recipient still wakes");
        assert_eq!(claim.generation, 1);
        // The notice send is admitted as queued for the busy Agent.
        let mut send = SubmitCommand::new(notice.clone(), "send");
        send.target = Some(a.clone());
        send.expected_generation = Some(1);
        send.args = json!({"text": "1 unread messages; run swarm inbox"});
        let admitted = store.submit("owner", send, now).expect("admit notice send");
        assert_eq!(admitted.command.state, CommandState::Queued);
        // The cooldown stands: a second wake is suppressed by the open command.
        assert_eq!(
            store
                .notice_claim(
                    &a,
                    &Ulid::new().to_string(),
                    NoticePending::Mail,
                    now + 61_000
                )
                .unwrap(),
            None
        );
        let state = store.notice_state(&a).unwrap().expect("notice state");
        assert_eq!(state.open_notice_command.as_deref(), Some(notice.as_str()));
    }
}
