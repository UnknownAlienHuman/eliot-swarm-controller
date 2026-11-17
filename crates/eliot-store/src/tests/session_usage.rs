use super::*;

#[test]
fn session_usage_keeps_only_the_latest_snapshot() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "session-usage", now);
    assert!(
        store
            .get_session_usage(&agent, 1)
            .expect("read absent session usage")
            .is_none()
    );

    let commit =
        |store: &mut Store, batch_id: &str, native_id: &str, usage: serde_json::Value, at: i64| {
            store
                .put_batch(
                    SourceBatch {
                        batch_id: batch_id.to_owned(),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: "session.usage".to_owned(),
                            native_id: native_id.to_owned(),
                            data: json!({"usage": usage}),
                        }],
                        cursor: None,
                    },
                    at,
                )
                .expect("commit session usage");
        };

    // First snapshot commits with the caller timestamp as observed_at.
    commit(
        &mut store,
        "session-usage:1",
        "session-usage:n1",
        json!({
            "context_used_tokens": 10,
            "context_window_tokens": 1000,
            "source": "codex:thread/tokenUsage/updated",
        }),
        now + 1,
    );
    let first = store
        .get_session_usage(&agent, 1)
        .expect("read session usage")
        .expect("snapshot retained");
    assert_eq!(first.context_used_tokens, Some(10));
    assert_eq!(
        first.source.as_deref(),
        Some("codex:thread/tokenUsage/updated")
    );

    // A newer snapshot replaces the retained one.
    commit(
        &mut store,
        "session-usage:2",
        "session-usage:n2",
        json!({
            "context_used_tokens": 20,
            "context_window_tokens": 1000,
            "cumulative_cost": {"amount": 0.5, "unit": "usd_estimate"},
            "source": "antigravity:usage_update",
        }),
        now + 2,
    );
    let second = store
        .get_session_usage(&agent, 1)
        .expect("read session usage")
        .expect("snapshot replaced");
    assert_eq!(second.context_used_tokens, Some(20));
    assert_eq!(second.source.as_deref(), Some("antigravity:usage_update"));

    // An older snapshot never regresses the readback.
    commit(
        &mut store,
        "session-usage:3",
        "session-usage:n3",
        json!({
            "context_used_tokens": 5,
            "context_window_tokens": 1000,
            "source": "codex:thread/tokenUsage/updated",
            "observed_at": now,
        }),
        now + 3,
    );
    assert_eq!(
        store
            .get_session_usage(&agent, 1)
            .expect("read session usage"),
        Some(second.clone()),
        "older snapshot must not replace a newer one"
    );

    // An equal-timestamp snapshot with different content never replaces.
    commit(
        &mut store,
        "session-usage:3b",
        "session-usage:n3b",
        json!({
            "context_used_tokens": 99,
            "context_window_tokens": 1000,
            "source": "codex:thread/tokenUsage/updated",
            "observed_at": now + 2,
        }),
        now + 5,
    );
    assert_eq!(
        store
            .get_session_usage(&agent, 1)
            .expect("read session usage"),
        Some(second.clone()),
        "equal timestamp must not replace the retained snapshot"
    );

    // A byte-identical duplicate is ignored, even on a newer commit.
    commit(
        &mut store,
        "session-usage:4",
        "session-usage:n4",
        serde_json::to_value(&second).expect("encode snapshot"),
        now + 4,
    );
    assert_eq!(
        store
            .get_session_usage(&agent, 1)
            .expect("read session usage"),
        Some(second),
        "duplicate snapshot must not replace the retained one"
    );
}

#[test]
fn session_growth_view_reads_retained_file_bytes() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "growth-bytes-probe", now);
    store
        .put_batch(
            SourceBatch {
                batch_id: "session-usage:bytes".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.usage".to_owned(),
                    native_id: "session-usage:nbytes".to_owned(),
                    data: json!({"usage": {
                        "context_used_tokens": 10,
                        "context_window_tokens": null,
                        "source": "claude:result",
                        "session_file_bytes": 2048,
                    }}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit session usage");
    let low = SessionGrowthThresholds {
        file_bytes: 1024,
        ..SessionGrowthThresholds::default()
    };
    let warned = store
        .session_growth_view("claude", &agent, 1, &low)
        .expect("growth view")
        .expect("retained file bytes warn under a low threshold");
    assert_eq!(warned.basis.as_deref(), Some("session_file_bytes"));
    assert!(warned.warn);
    // Default thresholds stay quiet: only an exposed, crossed size warns.
    assert!(
        store
            .session_growth_view("claude", &agent, 1, &SessionGrowthThresholds::default())
            .expect("growth view")
            .is_none()
    );
}

#[test]
fn session_fallback_projects_cumulative_counters_only() {
    use eliot_core::{SessionCost, SessionTokenTotals, SessionUsage};

    let snapshot = SessionUsage {
        context_used_tokens: Some(100),
        context_window_tokens: Some(200_000),
        cumulative_tokens: Some(SessionTokenTotals {
            input_tokens: Some(47_024),
            output_tokens: Some(207),
            cache_read_tokens: None,
            cache_write_tokens: Some(1_000),
        }),
        cumulative_cost: Some(SessionCost {
            amount: Some(0.5),
            unit: Some("usd_estimate".to_owned()),
        }),
        session_file_bytes: None,
        source: Some("muse:session/tokenUsage".to_owned()),
        observed_at: Some(7),
    };
    let usage = crate::session_fallback_usage(&snapshot).expect("cumulative counters project");
    assert_eq!(usage.scope.as_deref(), Some("session"));
    assert_eq!(usage.input_tokens, Some(47_024));
    assert_eq!(usage.output_tokens, Some(207));
    assert_eq!(usage.cache_write_tokens, Some(1_000));
    assert_eq!(usage.total_tokens, None);

    // Context occupancy or cost alone is not usage: no fallback.
    let sparse = SessionUsage {
        cumulative_tokens: None,
        cumulative_cost: Some(SessionCost {
            amount: Some(1.0),
            unit: Some("usd_estimate".to_owned()),
        }),
        source: Some("claude:result".to_owned()),
        observed_at: Some(8),
        ..snapshot.clone()
    };
    assert!(crate::session_fallback_usage(&sparse).is_none());
    let empty = SessionUsage {
        cumulative_tokens: Some(SessionTokenTotals {
            input_tokens: None,
            output_tokens: None,
            cache_read_tokens: None,
            cache_write_tokens: None,
        }),
        ..snapshot
    };
    assert!(crate::session_fallback_usage(&empty).is_none());
}
