use super::*;
use eliot_core::api::harness::{HarnessCapabilities, LegacyPreWriteRefusal};
use eliot_core::{AgentState, Event, WorkspaceRemovalOutcome};
use serde::Serialize;

const EVIDENCE_PAGE: usize = 64;

/// Produced by a guarded read of the retained checkout, with no Git writes.
pub enum RemovalEvidence {
    Present,
    Absent(WorkspaceRemovalOutcome),
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CommandEvidence<'a> {
    AttentionGenerationRefused,
    ArchivedCodexResume { native_ref: &'a str },
    NativeCommandReceipt { seq: i64 },
    ResumeSessionReady { seq: i64 },
    SendTurnStarted { seq: i64 },
    NativeControlRefusedBeforeWrite,
    AntigravitySendStreamRefusal,
    CodexSendSourceRefusal,
    ClaudeSteerContextRefusal,
    ForceStopMissingCustody,
    OwnedExitAndReaderDrain,
    RemovalNeverDispatched,
    RemovalAbsenceVerified,
}

impl CommandEvidence<'_> {
    fn reason(&self) -> &'static str {
        match self {
            Self::AttentionGenerationRefused => {
                "The Store refused the attention subject before inserting the event."
            }
            Self::ArchivedCodexResume { .. } => {
                "Codex refused to resume the exact archived session, and the attempt's process and reader closed."
            }
            Self::NativeCommandReceipt { .. } => {
                "The exact committed receipt proves the command took effect."
            }
            Self::ResumeSessionReady { .. } => {
                "The exact resumed generation committed native session readiness."
            }
            Self::SendTurnStarted { .. } => {
                "The exact send was bound to a committed native turn start."
            }
            Self::NativeControlRefusedBeforeWrite => {
                "The adapter refused the exact turn control before a native write."
            }
            Self::AntigravitySendStreamRefusal => {
                "The Antigravity stream gate refused this Send before writing the native prompt."
            }
            Self::CodexSendSourceRefusal => {
                "The Codex event-source gate refused this Send before starting a native turn."
            }
            Self::ClaudeSteerContextRefusal => {
                "The daemon failed to render this Claude Steer before handing it to the adapter."
            }
            Self::ForceStopMissingCustody => {
                "The force-stop custody guard refused this Stop before opening a Windows Job."
            }
            Self::OwnedExitAndReaderDrain => {
                "Owned processes exited and the native reader drained for the exact Stop generation."
            }
            Self::RemovalNeverDispatched => {
                "The checkout is present and the retained removal marker proves Git was never dispatched."
            }
            Self::RemovalAbsenceVerified => {
                "Readback proved the exact checkout and its Git registration absent, with no Git write dispatched by this command."
            }
        }
    }
}

impl Store {
    /// One bounded page during the existing reconciliation or maintenance pass.
    /// Each proof and state transition uses the same committed Store snapshot.
    pub fn reconcile_retained_command_evidence(
        &mut self,
        after: Option<&str>,
        now: i64,
    ) -> Result<Change<Option<String>>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let ids =
                {
                    let mut stmt = tx.prepare(
                    "SELECT id FROM commands WHERE state='unknown' AND method='command.submit'
                        AND (verb IN ('resume','stop','attention.emit','inject','send')
                            OR (verb='steer' AND json_extract(outcome,'$.code')='source_integrity')
                            OR (verb IN ('interrupt','steer')
                                AND json_extract(outcome,'$.code')='stale_turn'))
                        AND id > COALESCE(?1,'') ORDER BY id LIMIT ?2",
                ).map_err(sql_error)?;
                    stmt.query_map(params![after, EVIDENCE_PAGE as i64 + 1], |r| {
                        r.get::<_, String>(0)
                    })
                    .map_err(sql_error)?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(sql_error)?
                };
            let mut events = Vec::new();
            for id in ids.iter().take(EVIDENCE_PAGE) {
                if let Some(record) = load_command_record(&tx, id)? {
                    reconcile_command_tx(&tx, &record, now, &mut events)?;
                }
            }
            let next = (ids.len() > EVIDENCE_PAGE).then(|| ids[EVIDENCE_PAGE - 1].clone());
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: next,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }

    pub fn is_no_write_unknown_removal(outcome: &Value, id: &str) -> bool {
        Self::is_legacy_missing_branch_removal(outcome, id)
            || (outcome["stage"] == "removal"
                && outcome["command_id"].as_str() == Some(id)
                && outcome["native_git_write_dispatched"].as_bool() == Some(false)
                && outcome.get("removal_write_guard").is_none()
                && outcome["result"]["state"] == "unknown"
                && outcome["result"]["code"] == "workspace_git_drain_unknown")
    }

    pub fn reconcile_no_write_removal(
        &mut self,
        workspace: &WorkspacePreparation,
        id: &str,
        proof: RemovalEvidence,
        now: i64,
    ) -> Result<Change<bool>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let mut events = Vec::new();
            let applied = super::super::workspace::reconcile_no_write_removal_tx(
                &tx,
                workspace,
                id,
                proof,
                now,
                &mut events,
            )?;
            if let Some((record, state, outcome)) = applied {
                let evidence = if state == CommandState::Applied {
                    CommandEvidence::RemovalAbsenceVerified
                } else {
                    CommandEvidence::RemovalNeverDispatched
                };
                events.push(settle_evidence_tx(
                    &tx, &record, state, outcome, &evidence, now,
                )?);
            }
            let value = !events.is_empty();
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change { value, events })
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

fn reconcile_command_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    now: i64,
    events: &mut Vec<Event>,
) -> Result<()> {
    let command = &record.command;
    if record.state != CommandState::Unknown {
        return Ok(());
    }
    let outcome = command.outcome.as_ref().unwrap_or(&Value::Null);
    if command.verb == "attention.emit"
        && record.target.is_none()
        && command.generation.is_none()
        && outcome["cause"] == "user_event_commit_failed"
        && outcome["code"] == "stale_generation"
    {
        events.push(settle_evidence_tx(
            tx,
            record,
            CommandState::Failed,
            outcome.clone(),
            &CommandEvidence::AttentionGenerationRefused,
            now,
        )?);
        return Ok(());
    }
    let (Some(target), Some(generation)) = (record.target, command.generation) else {
        return Ok(());
    };
    let Some(agent) = load_agent_by_num(tx, target)?.filter(|agent| !agent.observe_only) else {
        return Ok(());
    };
    if generation > agent.generation {
        return Ok(());
    }
    if let Some(evidence) = retained_pre_write_refusal(record, &agent.harness, outcome)
        && (!matches!(evidence, CommandEvidence::ClaudeSteerContextRefusal)
            || has_unclaimed_steer_dispatch(tx, record, target, generation)?)
    {
        events.push(settle_evidence_tx(
            tx,
            record,
            CommandState::Rejected,
            outcome.clone(),
            &evidence,
            now,
        )?);
        if command.verb == "send" {
            super::super::context::settle_send_claims_tx(
                tx,
                record,
                CommandState::Rejected,
                now,
                events,
            )?;
        }
        return Ok(());
    }
    if command.verb == "resume"
        && super::super::load_linked_command(tx, &command.id)?
            .is_some_and(|send| !send.state.is_terminal() && send.state != CommandState::Unknown)
    {
        // Reconciliation does not release an input that still needs dispatch.
        return Ok(());
    }
    if command.verb == "resume"
        && agent.generation == generation
        && agent.is_terminal()
        && let Some(native_ref) = agent.native_ref.as_deref()
        && archived_resume_refusal(outcome, native_ref)
        && super::super::capacity::has_owned_exit_and_reader_drain_tx(tx, target, generation)?
        && !tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM events INDEXED BY events_agent
                WHERE agent=?1 AND generation=?2 AND kind='session.ready' AND source='stream')",
                params![target, generation],
                |row| row.get::<_, bool>(0),
            )
            .map_err(sql_error)?
    {
        events.push(settle_evidence_tx(
            tx,
            record,
            CommandState::Failed,
            outcome.clone(),
            &CommandEvidence::ArchivedCodexResume { native_ref },
            now,
        )?);
    } else if command.verb == "stop"
        && (agent.generation > generation || agent.is_terminal())
        && super::super::capacity::has_owned_exit_and_reader_drain_tx(tx, target, generation)?
    {
        events.push(settle_evidence_tx(tx, record, CommandState::Applied,
            json!({"state":"applied","process_custody_exit_proven":true,"native_source_drain_proven":true}),
            &CommandEvidence::OwnedExitAndReaderDrain, now)?);
        if agent.generation == generation && agent.state == AgentState::Lost {
            super::super::lifecycle::settle_lost_stop_after_owned_exit_tx(
                tx,
                &command.id,
                now,
                events,
            )?;
        }
    } else if matches!(command.verb.as_str(), "send" | "inject" | "resume")
        && let Some((seq, receipt)) = applied_command_receipt(tx, record, target, generation)?
    {
        events.push(settle_evidence_tx(
            tx,
            record,
            CommandState::Applied,
            receipt,
            &CommandEvidence::NativeCommandReceipt { seq },
            now,
        )?);
    } else if let Some((evidence, outcome)) =
        native_lifecycle_receipt(tx, record, target, generation)?
    {
        events.push(settle_evidence_tx(
            tx,
            record,
            CommandState::Applied,
            outcome,
            &evidence,
            now,
        )?);
    }
    Ok(())
}

fn retained_pre_write_refusal(
    record: &CommandRecord,
    harness: &str,
    outcome: &Value,
) -> Option<CommandEvidence<'static>> {
    if outcome["cause"] != "plugin_outcome_unknown" {
        return None;
    }
    let location = outcome["where"].as_str()?;
    let code = outcome["code"].as_str()?;
    let message = outcome["message"].as_str()?;
    let command = &record.command;
    let diagnostic = (command.verb.as_str(), location, code);
    let legacy_unclassified_origin = outcome["origin"] == "unknown"
        && outcome["origin_evidence"] == "no origin classified where this error was created";
    let bound_turn = command
        .turn_id
        .as_ref()
        .is_some_and(|turn| !turn.is_empty());
    if diagnostic == ("stop", "plugin.command", "unknown")
        && command.args["force"] == true
        && message == "generation custody namespace was not recorded"
    {
        return Some(CommandEvidence::ForceStopMissingCustody);
    }
    HarnessCapabilities::of(harness)
        .legacy_pre_write_refusals
        .iter()
        .find_map(|refusal| match refusal {
            LegacyPreWriteRefusal::StaleTurn(verb, refusal_message)
                if diagnostic == (verb.as_str(), "plugin.command.unknown", "stale_turn")
                    && bound_turn
                    && message == *refusal_message =>
            {
                Some(CommandEvidence::NativeControlRefusedBeforeWrite)
            }
            LegacyPreWriteRefusal::SendStreamUnresolved(refusal_message)
                if diagnostic == ("send", "plugin.command.unknown", "unknown")
                    && legacy_unclassified_origin
                    && command.turn_id.is_none()
                    && message == *refusal_message =>
            {
                Some(CommandEvidence::AntigravitySendStreamRefusal)
            }
            LegacyPreWriteRefusal::SendSourceGate {
                location: gate,
                origin_evidence,
            } if diagnostic == ("send", *gate, "unknown")
                && command.turn_id.is_none()
                && outcome["origin"] == "swarm"
                && outcome["origin_evidence"] == *origin_evidence =>
            {
                Some(CommandEvidence::CodexSendSourceRefusal)
            }
            LegacyPreWriteRefusal::SteerContextRendering
                if diagnostic == ("steer", "plugin.command", "source_integrity")
                    && legacy_unclassified_origin
                    && bound_turn
                    && record.expected_turn_id == command.turn_id
                    && message == "persisted inject args are malformed"
                    && outcome.get("claim").is_none()
                    && super::super::context::decode_inject_args(&command.args).is_err() =>
            {
                Some(CommandEvidence::ClaudeSteerContextRefusal)
            }
            _ => None,
        })
}

fn has_unclaimed_steer_dispatch(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    target: i64,
    generation: i64,
) -> Result<bool> {
    // An ordinary dispatch records the exact turn. A hook dispatch records
    // its consumer and application point. Require the former and rule out
    // any other dispatch, claim or delivery receipt for this command.
    tx.query_row(
        "SELECT COUNT(*) = 1 AND COALESCE(MAX(
            kind='command.dispatched' AND source='daemon'
            AND json_extract(data,'$.verb')='steer'
            AND json_extract(data,'$.turn_id')=?4
            AND json_type(data,'$.consumer_id') IS NULL
            AND json_type(data,'$.application_point') IS NULL
        ),0)
         FROM events INDEXED BY events_agent
         WHERE agent=?1 AND generation=?2 AND json_extract(data,'$.command_id')=?3
           AND (kind IN ('command.dispatched','command.applied','command.resolved')
                OR json_type(data,'$.outcome.claim') IS NOT NULL
                OR json_type(data,'$.outcome.context_outcome') IS NOT NULL)",
        params![
            target,
            generation,
            record.command.id,
            record.command.turn_id
        ],
        |row| row.get(0),
    )
    .map_err(sql_error)
}

fn archived_resume_refusal(outcome: &Value, native_ref: &str) -> bool {
    !native_ref.is_empty()
        && native_ref
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        && outcome["code"] == "native_rejected"
        && outcome["cause"] == "plugin_outcome_unknown"
        && outcome["origin"] == "harness"
        && outcome["origin_evidence"] == "native JSON-RPC refusal; no structured upstream error"
        && outcome["where"] == "plugin.codex.native.response"
        && outcome["message"]
            == format!(
                "session {native_ref} is archived. Run `codex unarchive {native_ref}` to unarchive it first."
            )
}

fn native_lifecycle_receipt(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    target: i64,
    generation: i64,
) -> Result<Option<(CommandEvidence<'static>, Value)>> {
    let id = record.command.id.as_str();
    match record.command.verb.as_str() {
        "resume" => {
            // The native intake enforces one start per generation and verifies
            // its retained subject. Journal order also requires this dispatch.
            let ready = tx.query_row(
                "SELECT e.seq FROM events e INDEXED BY events_agent WHERE e.agent=?1 AND e.generation=?2
                    AND e.kind='session.ready' AND e.source='stream' AND e.native_id IS NOT NULL
                    AND json_type(e.data,'$.native_ref')='text' AND json_extract(e.data,'$.native_ref')<>''
                    AND json_type(e.data,'$.native_scope')='text' AND json_extract(e.data,'$.native_scope')<>''
                    AND (json_extract(e.data,'$.command_id')=?3 OR (
                      json_extract(e.data,'$.command_id') IS NULL
                      AND EXISTS (SELECT 1 FROM agents bound
                        WHERE bound.id=?1 AND bound.generation=?2
                          AND bound.native_ref=json_extract(e.data,'$.native_ref')
                          AND bound.native_scope=json_extract(e.data,'$.native_scope'))
                    ))
                    AND e.ts<=?4
                    AND NOT EXISTS (SELECT 1 FROM commands prior
                      WHERE prior.target=?1 AND prior.generation=?2
                        AND prior.verb IN ('launch','resume') AND prior.id<>?3)
                    AND EXISTS (SELECT 1 FROM events d INDEXED BY events_agent WHERE d.agent=?1 AND d.generation=?2
                      AND d.source='daemon' AND d.kind='command.dispatched' AND d.seq<e.seq
                      AND d.ts<=e.ts AND json_extract(d.data,'$.command_id')=?3
                      AND NOT EXISTS (SELECT 1 FROM events finished INDEXED BY events_agent
                        WHERE finished.agent=?1 AND finished.generation=?2 AND finished.source='daemon'
                          AND finished.kind IN ('command.unknown','command.failed','command.rejected')
                          AND finished.seq>d.seq AND finished.seq<e.seq
                          AND json_extract(finished.data,'$.command_id')=?3))
                 ORDER BY e.seq LIMIT 1",
                params![target,generation,id,record.command.updated_at], |r| r.get::<_, i64>(0),
            ).optional().map_err(sql_error)?;
            Ok(ready.map(|seq| {
                (
                    CommandEvidence::ResumeSessionReady { seq },
                    json!({"state":"ready"}),
                )
            }))
        }
        "send" => {
            let event = tx
                .query_row(
                    "SELECT e.seq,t.turn_id FROM turns t JOIN events e INDEXED BY events_turn_lifecycle
                    ON e.agent=t.agent AND e.generation=t.generation
                    AND json_extract(e.data,'$.turn_id')=t.turn_id
                 WHERE t.agent=?1 AND t.generation=?2 AND t.start_command_id=?3
                    AND (?4 IS NULL OR t.turn_id=?4)
                    AND e.kind IN ('turn.started', 'turn.completed', 'turn.unknown')
                    AND e.kind='turn.started' AND e.source='stream' AND e.native_id IS NOT NULL
                    AND json_extract(e.data,'$.command_id')=?3
                 ORDER BY e.seq LIMIT 1",
                    params![target, generation, id, record.command.turn_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(sql_error)?;
            Ok(event.map(|(seq, turn_id)| {
                (
                    CommandEvidence::SendTurnStarted { seq },
                    json!({"state":"applied","turn_id":turn_id}),
                )
            }))
        }
        _ => Ok(None),
    }
}

fn applied_command_receipt(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    agent: i64,
    generation: i64,
) -> Result<Option<(i64, Value)>> {
    let row: Option<(i64, String)> = tx
        .query_row(
            "SELECT seq,data FROM events INDEXED BY events_agent WHERE agent=?1 AND generation=?2 AND source='daemon'
            AND kind='command.applied' AND json_extract(data,'$.command_id')=?3
            ORDER BY seq DESC LIMIT 1",
            params![agent, generation, record.command.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sql_error)?;
    if let Some((seq, data)) = row {
        let data = decode_json(&data)?;
        if let Some(outcome) = data.get("outcome").filter(|value| value.is_object()) {
            return Ok(Some((seq, outcome.clone())));
        }
    }
    Ok(None)
}

fn settle_evidence_tx(
    tx: &Transaction<'_>,
    record: &CommandRecord,
    state: CommandState,
    mut outcome: Value,
    evidence: &CommandEvidence<'_>,
    now: i64,
) -> Result<Event> {
    outcome["resolution_source"] = json!("retained_evidence");
    outcome["reason"] = json!(evidence.reason());
    outcome["evidence"] = json!(evidence);
    update_command_state(tx, &record.command.id, state, Some(&outcome), None, now)?;
    insert_daemon_command_event(
        tx,
        now,
        "command.resolved",
        record.target,
        record.command.generation,
        &record.command.id,
        json!({
            "command_id": record.command.id, "state": state, "resolution_source": "retained_evidence",
            "reason": evidence.reason(), "evidence": evidence,
        }),
    )
}
