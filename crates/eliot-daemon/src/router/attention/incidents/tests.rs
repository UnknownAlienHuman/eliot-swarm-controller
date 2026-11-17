use super::super::{observe::capture_child_activity, sweep::reserve_incident};
use super::*;
use eliot_core::CommandState;
use serde_json::json;

#[cfg(test)]
#[test]
fn attention_digest_fixed_vectors_preserve_length_prefixed_bytes() {
    let expected = "attention-43ee655579de01ca739b3f95c1c2d3f46d353b2c0df818064ea594506cdb2617";
    assert_eq!(digest_id(&["ab", "c"]), expected);
    assert_eq!(digest_id(&["ab".to_owned(), "c".to_owned()]), expected);
    assert_eq!(
        digest_id(&["a", "bc"]),
        "attention-9a8acca1b6c6c0befd3fbc756aed625da998c998f7252e738c4ef061906b9b21"
    );
    assert_eq!(
        loss_incident_id("a42", 7, 1234),
        "attention-8fd04c48fa0f140136cdb4a8874fb63adc1bc3a06853f59af98b11ddd1404431"
    );
}

#[cfg(test)]
mod child_attention_tests {
    use super::*;

    fn store_with_children(child_count: usize) -> (eliot_store::Store, Agent, Vec<String>) {
        use eliot_core::{ChildRegistration, NativeEvent, SourceBatch, SubmitCommand};
        let now = now_ms();
        let mut store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        let mut launch = SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({"harness":"codex", "profile":"page-test", "profile_snapshot":{},
            "project":"page-test", "workspace":"page-test"});
        let command = launch.id.clone();
        let manager_id = store
            .submit("owner", launch, now)
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&command, now).unwrap();
        let plan = ["session", "turn", "message"]
            .into_iter()
            .map(|family| (family.to_owned(), "stream".to_owned()))
            .collect();
        store
            .install_plugin_sources(&manager_id, 1, &command, &plan, now)
            .unwrap();
        store.put_batch(SourceBatch {
            batch_id: "ready".to_owned(), agent_id: manager_id.clone(), generation: 1,
            source: "stream".to_owned(), cursor: None,
            events: vec![NativeEvent { kind: "session.ready".to_owned(), native_id: "ready".to_owned(),
                ts: Some(now), data: json!({"command_id":command,"native_scope":"page-test","native_ref":"manager"}) }],
        }, now).unwrap();
        store
            .settle(&command, CommandState::Applied, None, now)
            .unwrap();
        let mut children = Vec::new();
        for index in 0..child_count {
            children.push(
                store
                    .register_native_child(
                        &manager_id,
                        1,
                        ChildRegistration {
                            registration_id: format!("child-{index}"),
                            parent_id: manager_id.clone(),
                            parent_generation: 1,
                            parent_native_ref: "manager".to_owned(),
                            native_scope: "page-test".to_owned(),
                            native_ref: format!("child-{index}"),
                        },
                        now,
                    )
                    .unwrap()
                    .value
                    .agent
                    .id,
            );
        }
        let manager = store.get_agent(&manager_id).unwrap().unwrap();
        (store, manager, children)
    }

    #[tokio::test]
    async fn pages_release_store_and_reject_a_replaced_manager_generation() {
        use eliot_core::SubmitCommand;
        let now = now_ms();
        let (store, manager, _) =
            store_with_children(eliot_store::NATIVE_CHILD_ATTENTION_PAGE_SIZE + 1);
        let manager_id = manager.id.clone();
        let store = Arc::new(tokio::sync::Mutex::new(store));
        let capture = capture_child_activity(&store, &manager, Duration::from_secs(900));
        tokio::pin!(capture);
        // The first pending point is the explicit handoff after one complete page.
        assert!(futures_util::poll!(&mut capture).is_pending());
        {
            let mut writer = store
                .try_lock()
                .expect("capture retained Store across a page handoff");
            writer.mark_lost(&manager_id, 1, now + 1).unwrap();
            // Store-only fixture: no process or native reader was launched.
            writer
                .close_native_sources(&manager_id, 1, now + 1)
                .unwrap();
            writer
                .release_restart_closed_generation(
                    &manager_id,
                    1,
                    "store_only_fixture",
                    false,
                    now + 1,
                )
                .unwrap();
            let mut resume = SubmitCommand::new(ulid::Ulid::new().to_string(), "resume");
            resume.target = Some(manager_id.clone());
            resume.expected_generation = Some(1);
            let resume_id = resume.id.clone();
            writer.submit("owner", resume, now + 2).unwrap();
            writer.dispatch(&resume_id, now + 2).unwrap();
            assert_eq!(
                writer.get_agent(&manager_id).unwrap().unwrap().generation,
                2
            );
        }
        assert!(capture.await.unwrap().is_none());
    }

    fn child_turn_fact(
        store: &mut eliot_store::Store,
        manager: &Agent,
        child: &str,
        turn: &str,
        kind: &str,
    ) {
        let id = ulid::Ulid::new().to_string();
        let data = if kind == "turn.completed" {
            json!({"turn_id":turn, "status":"completed"})
        } else {
            json!({"turn_id":turn})
        };
        store
            .put_native_child_batch(
                &manager.id,
                manager.generation,
                eliot_core::SourceBatch {
                    batch_id: id.clone(),
                    agent_id: child.to_owned(),
                    generation: 1,
                    source: "stream".to_owned(),
                    cursor: Some(id.clone()),
                    events: vec![eliot_core::NativeEvent {
                        kind: kind.to_owned(),
                        native_id: id,
                        ts: Some(now_ms()),
                        data,
                    }],
                },
                now_ms(),
            )
            .unwrap();
    }

    async fn assert_overlap_invalidates_capture(existing_child: bool) {
        let (mut store, manager, children) =
            store_with_children(eliot_store::NATIVE_CHILD_ATTENTION_PAGE_SIZE + 1);
        for child in &children[..eliot_store::NATIVE_CHILD_ATTENTION_PAGE_SIZE] {
            child_turn_fact(&mut store, &manager, child, "finished", "turn.completed");
        }
        let a = children.last().unwrap();
        child_turn_fact(&mut store, &manager, a, "A", "turn.started");
        let through = store
            .native_children_attention_end(&manager.id, manager.generation)
            .unwrap();
        let store = Arc::new(tokio::sync::Mutex::new(store));
        let capture = capture_child_activity(&store, &manager, Duration::from_secs(900));
        tokio::pin!(capture);
        assert!(futures_util::poll!(&mut capture).is_pending());
        {
            let mut writer = store.try_lock().unwrap();
            let b = if existing_child {
                children[0].clone()
            } else {
                writer
                    .register_native_child(
                        &manager.id,
                        manager.generation,
                        eliot_core::ChildRegistration {
                            registration_id: "overlapping-B".to_owned(),
                            parent_id: manager.id.clone(),
                            parent_generation: manager.generation,
                            parent_native_ref: "manager".to_owned(),
                            native_scope: "page-test".to_owned(),
                            native_ref: "overlapping-B".to_owned(),
                        },
                        now_ms(),
                    )
                    .unwrap()
                    .value
                    .agent
                    .id
            };
            child_turn_fact(&mut writer, &manager, &b, "B", "turn.started");
            child_turn_fact(&mut writer, &manager, a, "A", "turn.completed");
            if existing_child {
                assert_eq!(
                    writer
                        .native_children_attention_end(&manager.id, manager.generation)
                        .unwrap(),
                    through
                );
            }
            let current = writer.get_agent(&manager.id).unwrap().unwrap();
            assert_eq!(
                (current.generation, current.active_turn),
                (manager.generation, manager.active_turn.clone())
            );
        }
        // The old mixed-time read would return zero and retire the incident.
        assert!(capture.await.unwrap().is_none());
        let (stable, _) = capture_child_activity(&store, &manager, Duration::from_secs(900))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stable.unfinished, 1);
    }

    #[tokio::test]
    async fn overlapping_new_child_invalidates_the_whole_capture() {
        assert_overlap_invalidates_capture(false).await;
    }

    #[tokio::test]
    async fn existing_child_new_turn_invalidates_without_registration_change() {
        assert_overlap_invalidates_capture(true).await;
    }

    #[tokio::test]
    async fn stale_empty_capture_cannot_retire_or_rearm_an_existing_incident() {
        let (store, owner, children) = store_with_children(1);
        let store = Arc::new(tokio::sync::Mutex::new(store));
        let (summary, revision) = capture_child_activity(&store, &owner, Duration::from_secs(900))
            .await
            .unwrap()
            .unwrap();
        let mut captured = manager();
        captured.agent = owner.clone();
        captured.children = summary;
        captured.children_revision = revision;
        let key = IncidentKey {
            agent: owner.id.clone(),
            generation: owner.generation,
            kind: IncidentKind::ChildrenUnattended,
            turn_id: None,
        };
        let state = Arc::new(Mutex::new(AttentionState::default()));
        reserve_incident(&state, &key).unwrap().commit(
            key.clone(),
            ActiveIncident {
                incident_id: "already-delivered".to_owned(),
                baseline_progress_at: None,
                baseline_model_progress_at: None,
                incident_delivery: None,
                recovery_delivery: None,
            },
        );
        {
            let mut writer = store.lock().await;
            child_turn_fact(&mut writer, &owner, &children[0], "late", "turn.started");
            assert!(!child_capture_is_current(&writer, &captured).unwrap());
            retire_child_incident_if_current(&writer, &state, &key, &captured).unwrap();
            assert!(state.lock().unwrap().active.contains_key(&key));
            assert!(reserve_incident(&state, &key).is_none());
            child_turn_fact(&mut writer, &owner, &children[0], "late", "turn.completed");
        }
        let (summary, revision) = capture_child_activity(&store, &owner, Duration::from_secs(900))
            .await
            .unwrap()
            .unwrap();
        captured.children = summary;
        captured.children_revision = revision;
        let writer = store.lock().await;
        retire_child_incident_if_current(&writer, &state, &key, &captured).unwrap();
        assert!(!state.lock().unwrap().active.contains_key(&key));
    }

    fn manager() -> CapturedAgent {
        let now = now_ms();
        let mut agent = Agent::new("a1", "project", "codex", "profile", now);
        agent.generation = 1;
        agent.state = AgentState::Busy;
        agent.active_turn = Some("parent-turn".to_owned());
        CapturedAgent {
            agent,
            source_states: Vec::new(),
            source_healthy: Some(true),
            unhealthy_source_seq: None,
            last_model_progress_at: Some(now),
            progress_basis_at: Some(now),
            progress_age_ms: Some(0),
            turn_command_id: None,
            tool_coverage: eliot_store::ActiveToolCoverage::Clear,
            pending_kinds: Vec::new(),
            pending_kinds_truncated: false,
            children: eliot_store::NativeChildrenAttention {
                unfinished: 1,
                ..Default::default()
            },
            children_revision: None,
            manager_progress_at: Some(now),
        }
    }

    #[test]
    fn decision_requires_unfinished_children_and_manager_or_child_suspicion() {
        let window = Duration::from_secs(900);
        let mut captured = manager();
        assert!(child_attention_activity(&captured, window).is_none());
        for state in [AgentState::Idle, AgentState::Stopped] {
            captured.agent.state = state;
            assert_eq!(
                child_attention_activity(&captured, window),
                Some("manager_inactive_with_unfinished_children")
            );
        }
        captured.agent.state = AgentState::Busy;
        captured.children.stalled = 1;
        assert_eq!(
            child_attention_activity(&captured, window),
            Some("child_progress_needs_manager")
        );
        captured.children.stalled = 0;
        captured.children.lost = 1;
        assert_eq!(
            child_attention_activity(&captured, window),
            Some("child_progress_needs_manager")
        );
        captured.agent.state = AgentState::Waiting;
        assert!(child_attention_activity(&captured, window).is_none());
        captured.agent.state = AgentState::Busy;
        captured.children.lost = 0;
        captured.manager_progress_at = Some(now_ms() - 900_001);
        assert_eq!(
            child_attention_activity(&captured, window),
            Some("manager_inactive_with_unfinished_children")
        );
        captured.children.unfinished = 0;
        captured.agent.state = AgentState::Idle;
        assert!(child_attention_activity(&captured, window).is_none());
    }

    #[test]
    fn mail_wake_and_parent_turn_changes_do_not_rearm_child_cohort() {
        let window = Duration::from_secs(900);
        let mut captured = manager();
        captured.agent.state = AgentState::Idle;
        let (key, _) = desired_incident(&captured, window).unwrap();
        let state = Arc::new(Mutex::new(AttentionState::default()));
        let incident = ActiveIncident {
            incident_id: "frozen-child-incident".to_owned(),
            baseline_progress_at: captured.progress_basis_at,
            baseline_model_progress_at: captured.last_model_progress_at,
            incident_delivery: None,
            recovery_delivery: None,
        };
        reserve_incident(&state, &key)
            .unwrap()
            .commit(key.clone(), incident.clone());
        captured.agent.active_turn = Some("mail-wake-turn".to_owned());
        captured.manager_progress_at = Some(now_ms());
        captured.children.unfinished = 100;
        let (same_key, _) = desired_incident(&captured, window).unwrap();
        assert_eq!(same_key, key);
        assert!(reserve_incident(&state, &same_key).is_none());
        assert!(!incident_condition_cleared(&same_key, &incident, &captured));
    }
}

#[cfg(test)]
mod loss_identity_tests {
    use super::*;

    #[test]
    fn loss_incident_id_is_deterministic_per_loss_event() {
        let first = loss_incident_id("a1", 2, 99);
        assert_eq!(first, loss_incident_id("a1", 2, 99));
        assert_ne!(first, loss_incident_id("a1", 2, 100));
        assert_ne!(first, loss_incident_id("a1", 3, 99));
        assert_ne!(first, loss_incident_id("a2", 2, 99));
    }

    #[test]
    fn loss_mutation_ids_are_deterministic_per_incident_phase_and_purpose() {
        let loss_ts = 1_700_000_000_000_i64;
        let emit = attention_ulid("attention-abc", "incident", "emit", loss_ts);
        assert_eq!(
            emit,
            attention_ulid("attention-abc", "incident", "emit", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abc", "incident", "affected-mail", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abc", "recovery", "emit", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abd", "incident", "emit", loss_ts)
        );
        assert_ne!(
            emit,
            attention_ulid("attention-abc", "incident", "emit", loss_ts + 1)
        );
        // Deterministic ids stay valid ULIDs carrying the loss timestamp, so
        // admission can check their shape without a retry-window rebuke.
        let parsed = ulid::Ulid::from_string(&emit).expect("deterministic ULID");
        assert_eq!(parsed.timestamp_ms(), loss_ts as u64);
    }
}
