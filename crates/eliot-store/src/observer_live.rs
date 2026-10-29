//! Granted normalized observer facts. This path never applies controlled Agent
//! reduction, command effects, identity changes or speculative input association.
use super::*;
use eliot_core::SourceHealthUpdate;

pub(super) fn require_transcript_families(
    connection: &Connection,
    agent: &Agent,
    source: &str,
    families: &[String],
) -> Result<()> {
    if source != "transcript" || families.is_empty() {
        return Err(Error::new(
            "not_allowed",
            "observer facts require the granted transcript source",
        ));
    }
    let supported = observer::live_families(&agent.harness)?;
    let id = parse_agent_id(&agent.id)?;
    let state = load_source_state(connection, id, agent.generation, source)?.ok_or_else(|| {
        Error::new(
            "source_integrity",
            "observer successor has no committed source state",
        )
    })?;
    if state.state != "open" {
        return Err(Error::new(
            "source_integrity",
            "observer successor is closed",
        ));
    }
    for family in families {
        if !supported.contains(&family.as_str()) {
            return Err(Error::new(
                "not_allowed",
                "family is outside the observed live-fact grant",
            ));
        }
        let owner: Option<(String,String)> = connection.query_row(
            "SELECT source, mechanism FROM source_owners WHERE agent = ?1 AND generation = ?2 AND family = ?3",
            params![id,agent.generation,family], |row| Ok((row.get(0)?,row.get(1)?)),
        ).optional().map_err(sql_error)?;
        if owner
            .as_ref()
            .is_none_or(|(owner, mechanism)| owner != source || mechanism != "rollout_tail")
        {
            return Err(Error::new(
                "source_integrity",
                "observer family has not been handed to the live transcript",
            ));
        }
    }
    Ok(())
}

fn optional_text<'a>(data: &'a Value, field: &str) -> Result<Option<&'a str>> {
    match data.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        _ => Err(Error::new(
            "invalid",
            "observed text field must be a string or null",
        )),
    }
}

fn optional_time(data: &Value, field: &str) -> Result<Option<i64>> {
    match data.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(|| {
            Error::new(
                "invalid",
                "observed turn timing must be integer epoch milliseconds",
            )
        }),
    }
}

pub(super) fn persist_turn_fact(
    tx: &Transaction<'_>,
    agent: &Agent,
    event: &NativeEvent,
) -> Result<()> {
    if !matches!(
        event.kind.as_str(),
        "turn.started" | "turn.completed" | "message.completed"
    ) {
        return Ok(());
    }
    let turn_id = required_string(&event.data, "turn_id")?;
    let id = parse_agent_id(&agent.id)?;
    let existing = load_turn(tx, id, agent.generation, &turn_id)?;
    let mut turn = existing.clone().unwrap_or(Turn {
        agent: agent.id.clone(),
        generation: agent.generation,
        turn_id: turn_id.clone(),
        state: TurnState::Unknown,
        status: None,
        final_text: None,
        started_at: None,
        completed_at: None,
    });
    match event.kind.as_str() {
        "turn.started" => {
            if turn.state == TurnState::Completed {
                return Err(Error::new(
                    "source_integrity",
                    "observed start would regress a completed turn",
                ));
            }
            turn.state = TurnState::Running;
            if let Some(at) = optional_time(&event.data, "started_at")? {
                if turn.started_at.is_some_and(|known| known != at) {
                    return Err(Error::new(
                        "source_integrity",
                        "observed start timing changed",
                    ));
                }
                turn.started_at = Some(at);
            }
        }
        "turn.completed" => {
            let status = required_string(&event.data, "status")?;
            if turn.state == TurnState::Completed {
                return Err(Error::new(
                    "source_integrity",
                    "observed turn terminal fact changed after completion",
                ));
            }
            turn.state = TurnState::Completed;
            turn.status = Some(status);
            if let Some(at) = optional_time(&event.data, "started_at")? {
                if turn.started_at.is_some_and(|known| known != at) {
                    return Err(Error::new(
                        "source_integrity",
                        "observed start timing changed",
                    ));
                }
                turn.started_at = Some(at);
            }
            turn.completed_at = optional_time(&event.data, "completed_at")?;
            if let Some(text) = optional_text(&event.data, "final_text")? {
                turn.final_text = Some(text.to_owned());
            }
        }
        "message.completed" => {
            if turn.state != TurnState::Completed
                && event.data.get("phase").and_then(Value::as_str) == Some("final_answer")
                && let Some(text) = optional_text(&event.data, "text")?
            {
                turn.final_text = Some(text.to_owned());
            }
        }
        _ => return Ok(()),
    }
    if existing.is_none() {
        insert_turn(
            tx,
            TurnInsert {
                agent: id,
                generation: agent.generation,
                turn_id: &turn_id,
                state: turn.state,
                status: turn.status.as_deref(),
                final_text: turn.final_text.as_deref(),
                started_at: turn.started_at,
                completed_at: turn.completed_at,
            },
        )?;
    } else {
        db!(tx.execute(
            "UPDATE turns SET state = ?1, status = ?2, final_text = ?3, started_at = ?4, completed_at = ?5
             WHERE agent = ?6 AND generation = ?7 AND turn_id = ?8",
            params![turn_state_name(turn.state),turn.status,turn.final_text,turn.started_at,turn.completed_at,id,agent.generation,turn_id],
        ));
    }
    Ok(())
}

impl Store {
    pub fn set_native_source_health(
        &mut self,
        update: SourceHealthUpdate,
        expected_subject: (&str, &str),
        now: i64,
    ) -> Result<Change<SourceHealthUpdate>> {
        self.ensure_writer_healthy()?;
        let result = (|| {
            update.validate()?;
            if matches!(
                update.source.as_str(),
                "daemon" | "os" | "git" | "workspace_process"
            ) {
                return Err(Error::new(
                    "not_allowed",
                    "observer health cannot change monitor sources",
                ));
            }
            let id = parse_agent_id(&update.agent_id)?;
            let tx = self
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let agent = load_agent_by_num(&tx, id)?
                .ok_or_else(|| Error::new("not_found", "health Agent was not found"))?;
            observer::require_live_subject(&tx, &agent, update.generation, expected_subject)?;
            if update.source != "observe" {
                let families = source_handoff::owner_bindings(&tx, id, update.generation)?
                    .into_iter()
                    .filter(|owner| owner.source == update.source)
                    .map(|owner| owner.family)
                    .collect::<Vec<_>>();
                require_transcript_families(&tx, &agent, &update.source, &families)?;
            }
            let state = load_source_state(&tx, id, update.generation, &update.source)?.ok_or_else(
                || Error::new("source_integrity", "health source has no committed state"),
            )?;
            if state.state != "open" {
                return Err(Error::new(
                    "source_integrity",
                    "health cannot reopen a closed source",
                ));
            }
            let mut events = Vec::new();
            if state.health != update.health {
                let changed = tx.execute("UPDATE source_state SET health = ?1, updated_at = ?2 WHERE agent = ?3 AND generation = ?4 AND source = ?5 AND state = 'open'",
                    params![update.health,now,id,update.generation,update.source]).map_err(sql_error)?;
                if changed != 1 {
                    return Err(Error::new(
                        "source_integrity",
                        "health source changed before commit",
                    ));
                }
                events.push(insert_daemon_event(
                    &tx,
                    now,
                    "source.health",
                    "source",
                    Some(id),
                    Some(update.generation),
                    json!({"source":update.source,"state":"open","health":update.health}),
                )?);
            }
            commit_transaction(tx, &self.write_faulted)?;
            Ok(Change {
                value: update,
                events,
            })
        })();
        self.fault_on_persistence_error(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{SourceHandoff, SourceHandoffGap};
    const SUBJECT: (&str, &str) = ("fixture:scope", "fixture-native");

    fn fixture() -> (Store, String, String, i64) {
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
        let id = Ulid::new().to_string();
        let admitted = store
            .submit(
                "owner",
                SubmitCommand {
                    id: id.clone(),
                    target: None,
                    verb: "attach".to_owned(),
                    args: attach_args(),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .unwrap();
        let agent = admitted.command.target.unwrap();
        store.dispatch(&id, now).unwrap();
        (store, agent, id, now)
    }

    fn attach_args() -> Value {
        json!({"mode":"observe","profile":"fake","harness":"fake","native_scope":SUBJECT.0,
            "native_ref":SUBJECT.1,"profile_snapshot":{"harness":"fake"}})
    }

    fn attached(store: &mut Store, agent: &str, command: &str, generation: i64, now: i64) {
        store.settle_observe_attach(command,json!({"agent_id":agent,"generation":generation,"command_id":command,
            "native_scope":SUBJECT.0,"native_ref":SUBJECT.1,"state":"attached","observation_state":"unknown",
            "metadata":{},"coverage":{}}),now).unwrap();
    }

    fn request(agent: &str) -> SourceHandoff {
        SourceHandoff {
            agent_id: agent.to_owned(),
            generation: 1,
            source: "observe".to_owned(),
            families: vec!["turn".to_owned(), "message".to_owned()],
            successor: "transcript".to_owned(),
            mechanism: "rollout_tail".to_owned(),
            cursor: None,
            gap: Some(SourceHandoffGap {
                gap_id: "edge-gap".to_owned(),
                reason: "live_edge".to_owned(),
                from: None,
                to: Some("edge:0".to_owned()),
            }),
        }
    }

    fn ready() -> (Store, String, String, i64) {
        let (mut store, agent, command, now) = fixture();
        attached(&mut store, &agent, &command, 1, now + 1);
        store
            .record_observed_source_handoff(request(&agent), SUBJECT, now + 2)
            .unwrap();
        (store, agent, command, now)
    }

    fn batch(agent: &str, id: &str, kind: &str, data: Value, ts: i64) -> SourceBatch {
        SourceBatch {
            batch_id: id.to_owned(),
            agent_id: agent.to_owned(),
            generation: 1,
            source: "transcript".to_owned(),
            events: vec![NativeEvent {
                kind: kind.to_owned(),
                native_id: id.to_owned(),
                data,
                ts: Some(ts),
            }],
            cursor: Some(format!("edge:{id}")),
        }
    }

    #[test]
    fn open_dispatch_is_not_ready_until_attach_result_is_committed() {
        let (mut store, agent, command, now) = fixture();
        assert!(store.observer_active(&agent, 1).unwrap());
        assert!(!store.observer_ready(&agent, 1, SUBJECT).unwrap());
        let state = store.read_source_state(&agent, 1, "observe").unwrap();
        assert_eq!(state["state"], "open");
        assert_eq!(state["observer_ready"], false);
        let high_water = store.high_water().unwrap();
        assert_eq!(
            store
                .record_observed_source_handoff(request(&agent), SUBJECT, now + 1)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(store.high_water().unwrap(), high_water);
        attached(&mut store, &agent, &command, 1, now + 2);
        assert!(store.observer_ready(&agent, 1, SUBJECT).unwrap());
        assert!(
            !store
                .observer_ready(&agent, 1, (SUBJECT.0, "other"))
                .unwrap()
        );
        assert_eq!(
            store.read_source_state(&agent, 1, "observe").unwrap()["observer_ready"],
            true
        );
        store
            .record_observed_source_handoff(request(&agent), SUBJECT, now + 3)
            .unwrap();
    }

    #[test]
    fn observed_facts_keep_unknown_identity_and_original_event_timestamp() {
        let (mut store, agent, command, now) = ready();
        let before = store.get_agent(&agent).unwrap().unwrap();
        let command_before = store.get_command(&command).unwrap();
        let input = batch(
            &agent,
            "message",
            "message.completed",
            json!({"turn_id":"t1","text":"answer","phase":"final_answer"}),
            1200,
        );
        let committed = store
            .put_observed_batch(input.clone(), SUBJECT, now + 3)
            .unwrap();
        assert_eq!(committed.events.len(), 1);
        assert_eq!(committed.events[0].ts, 1200);
        let turn = store.get_turn(&agent, 1, "t1").unwrap().unwrap();
        assert_eq!(turn.state, TurnState::Unknown);
        assert_eq!(turn.started_at, None);
        assert_eq!(turn.completed_at, None);
        assert_eq!(turn.final_text.as_deref(), Some("answer"));
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        assert_eq!(before.state, AgentState::Unknown);
        assert!(!before.own_identity);
        assert_eq!(store.get_command(&command).unwrap(), command_before);
        let token: Option<String> = store
            .connection
            .query_row(
                "SELECT token FROM agents WHERE id=?1",
                [parse_agent_id(&agent).unwrap()],
                |row| row.get(0),
            )
            .unwrap();
        assert!(token.is_none());
        assert_eq!(store.last_model_progress(&agent, 1).unwrap(), Some(1200));
        let state = store.read_source_state(&agent, 1, "transcript").unwrap();
        assert_eq!(
            state["last_batch_digest"],
            json!(input.semantic_digest().unwrap())
        );
        let duplicate = store
            .put_observed_batch(input.clone(), SUBJECT, now + 4)
            .unwrap();
        assert!(duplicate.events.is_empty());
        assert_eq!(duplicate.ack, committed.ack);
        let mut changed = input;
        changed.events[0].ts = Some(1201);
        assert_eq!(
            store
                .put_observed_batch(changed, SUBJECT, now + 5)
                .unwrap_err()
                .code,
            "source_integrity"
        );
    }

    #[test]
    fn commentary_or_unknown_phase_is_not_final_and_terminal_does_not_invent_start() {
        let (mut store, agent, _, now) = ready();
        for (id, phase) in [
            ("commentary", Some("commentary")),
            ("unknown", None),
            ("final", Some("final_answer")),
        ] {
            let mut data = json!({"turn_id":id,"text":"message"});
            if let Some(phase) = phase {
                data["phase"] = json!(phase);
            }
            store
                .put_observed_batch(
                    batch(&agent, &format!("m-{id}"), "message.completed", data, 100),
                    SUBJECT,
                    now + 3,
                )
                .unwrap();
            store
                .put_observed_batch(
                    batch(
                        &agent,
                        &format!("c-{id}"),
                        "turn.completed",
                        json!({"turn_id":id,"status":"completed"}),
                        200,
                    ),
                    SUBJECT,
                    now + 4,
                )
                .unwrap();
            let turn = store.get_turn(&agent, 1, id).unwrap().unwrap();
            assert_eq!(turn.state, TurnState::Completed);
            assert_eq!(turn.started_at, None);
            assert_eq!(turn.completed_at, None);
            assert_eq!(
                turn.final_text.as_deref(),
                if phase == Some("final_answer") {
                    Some("message")
                } else {
                    None
                }
            );
        }
        store
            .put_observed_batch(
                batch(
                    &agent,
                    "terminal-text",
                    "turn.completed",
                    json!({"turn_id":"terminal","status":"completed","final_text":"authoritative"}),
                    300,
                ),
                SUBJECT,
                now + 5,
            )
            .unwrap();
        assert_eq!(
            store
                .get_turn(&agent, 1, "terminal")
                .unwrap()
                .unwrap()
                .final_text
                .as_deref(),
            Some("authoritative")
        );
    }

    #[test]
    fn native_turn_timing_is_distinct_from_original_record_time() {
        let (mut store, agent, _, now) = ready();
        let started = batch(
            &agent,
            "timing-start",
            "turn.started",
            json!({"turn_id":"timed","started_at":1000}),
            9000,
        );
        let events = store
            .put_observed_batch(started, SUBJECT, now + 3)
            .unwrap()
            .events;
        assert_eq!(events[0].ts, 9000);
        let turn = store.get_turn(&agent, 1, "timed").unwrap().unwrap();
        assert_eq!(turn.state, TurnState::Running);
        assert_eq!(turn.started_at, Some(1000));
        let ended = batch(
            &agent,
            "timing-end",
            "turn.completed",
            json!({"turn_id":"timed","status":"completed","started_at":1000,"completed_at":2000,"duration_ms":1000}),
            10000,
        );
        assert_eq!(
            store
                .put_observed_batch(ended, SUBJECT, now + 4)
                .unwrap()
                .events[0]
                .ts,
            10000
        );
        let turn = store.get_turn(&agent, 1, "timed").unwrap().unwrap();
        assert_eq!(turn.started_at, Some(1000));
        assert_eq!(turn.completed_at, Some(2000));
        store
            .put_observed_batch(
                batch(
                    &agent,
                    "missing-start",
                    "turn.started",
                    json!({"turn_id":"unknown-time"}),
                    11000,
                ),
                SUBJECT,
                now + 5,
            )
            .unwrap();
        assert_eq!(
            store
                .get_turn(&agent, 1, "unknown-time")
                .unwrap()
                .unwrap()
                .started_at,
            None
        );
    }

    #[test]
    fn observed_authority_never_reaches_control_or_local_command_effects() {
        let (mut store, agent, _, now) = ready();
        let input = batch(
            &agent,
            "local",
            "turn.started",
            json!({"turn_id":"t1","command_id":"local-command"}),
            100,
        );
        let before = store.high_water().unwrap();
        assert_eq!(
            store
                .put_observed_batch(input.clone(), SUBJECT, now + 3)
                .unwrap_err()
                .code,
            "not_allowed"
        );
        assert_eq!(
            store.put_batch(input, now + 3).unwrap_err().code,
            "not_allowed"
        );
        assert!(
            store
                .put_observed_batch(
                    batch(&agent, "session", "session.ended", json!({}), 100),
                    SUBJECT,
                    now + 3
                )
                .is_err()
        );
        assert!(
            store
                .put_observed_batch(
                    batch(
                        &agent,
                        "subject",
                        "message.completed",
                        json!({"turn_id":"t","text":"x"}),
                        100
                    ),
                    (SUBJECT.0, "other"),
                    now + 3
                )
                .is_err()
        );
        assert!(
            store
                .record_source_handoff(request(&agent), now + 3)
                .is_err()
        );
        assert_eq!(store.high_water().unwrap(), before);
    }

    #[test]
    fn health_is_noop_when_unchanged_and_recovery_preserves_gap_and_cursor() {
        let (mut store, agent, _, now) = ready();
        let gap = SourceGap {
            gap_id: "replacement".to_owned(),
            agent_id: agent.clone(),
            generation: 1,
            source: "transcript".to_owned(),
            families: vec!["turn".to_owned(), "message".to_owned()],
            reason: "replaced".to_owned(),
            from: Some("edge:0".to_owned()),
            to: None,
        };
        store
            .record_observed_source_gap(gap, SUBJECT, now + 3)
            .unwrap();
        let before = store.read_source_state(&agent, 1, "transcript").unwrap();
        let first = store
            .set_native_source_health(
                SourceHealthUpdate::new(&agent, 1, "transcript", "stale"),
                SUBJECT,
                now + 4,
            )
            .unwrap();
        assert_eq!(first.events.len(), 1);
        assert!(
            store
                .set_native_source_health(first.value, SUBJECT, now + 5)
                .unwrap()
                .events
                .is_empty()
        );
        assert_eq!(
            store
                .set_native_source_health(
                    SourceHealthUpdate::new(&agent, 1, "transcript", "ok"),
                    SUBJECT,
                    now + 6
                )
                .unwrap()
                .events
                .len(),
            1
        );
        let after = store.read_source_state(&agent, 1, "transcript").unwrap();
        for field in [
            "state",
            "cursor",
            "gap_id",
            "gap_from",
            "gap_to",
            "gap_seq",
            "gap_digest",
            "last_handoff",
            "last_handoff_digest",
        ] {
            assert_eq!(before[field], after[field]);
        }
        assert!(
            store
                .set_native_source_health(
                    SourceHealthUpdate::new(&agent, 1, "os", "ok"),
                    SUBJECT,
                    now + 7
                )
                .is_err()
        );
        assert_eq!(store.last_model_progress(&agent, 1).unwrap(), None);
    }

    #[test]
    fn detach_closes_successors_and_late_unavailability_never_changes_new_generation() {
        let (mut store, agent, _, now) = ready();
        store.ensure_process_source(&agent, 1, now + 3).unwrap();
        store
            .put_observed_batch(
                batch(
                    &agent,
                    "unfinished",
                    "message.completed",
                    json!({"turn_id":"unfinished","text":"commentary","phase":"commentary"}),
                    100,
                ),
                SUBJECT,
                now + 3,
            )
            .unwrap();
        let detach = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: detach.clone(),
                    target: Some(agent.clone()),
                    verb: "detach".to_owned(),
                    args: json!({}),
                    expected_generation: Some(1),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now + 4,
            )
            .unwrap();
        assert!(!store.observer_ready(&agent, 1, SUBJECT).unwrap());
        store.dispatch(&detach, now + 4).unwrap();
        store.settle_observe_detach(&detach,json!({"agent_id":agent,"generation":2,"command_id":detach,"native_scope":SUBJECT.0,"native_ref":SUBJECT.1,"state":"detached","observation_state":"unknown"}),now+5).unwrap();
        for row in store.list_source_states(&agent, 1).unwrap() {
            assert_eq!(
                row["state"],
                if row["source"] == "os" {
                    "open"
                } else {
                    "closed"
                }
            );
        }
        assert_eq!(
            store
                .get_turn(&agent, 1, "unfinished")
                .unwrap()
                .unwrap()
                .state,
            TurnState::Unknown
        );
        let attach = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: attach.clone(),
                    target: None,
                    verb: "attach".to_owned(),
                    args: attach_args(),
                    expected_generation: None,
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now + 6,
            )
            .unwrap();
        store.dispatch(&attach, now + 6).unwrap();
        attached(&mut store, &agent, &attach, 3, now + 7);
        let before = store.get_agent(&agent).unwrap().unwrap();
        let sources = store.list_source_states(&agent, 3).unwrap();
        store
            .mark_observer_unavailable(&agent, 1, "old_reader", true, now + 8)
            .unwrap();
        assert_eq!(store.get_agent(&agent).unwrap().unwrap(), before);
        assert_eq!(store.list_source_states(&agent, 3).unwrap(), sources);
        assert!(store.observer_ready(&agent, 3, SUBJECT).unwrap());
    }
}
