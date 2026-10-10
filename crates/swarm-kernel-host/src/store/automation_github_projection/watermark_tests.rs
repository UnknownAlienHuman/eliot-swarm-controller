use super::*;

fn fixture() -> (Connection, AutomationEntry) {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(super::super::SCHEMA).unwrap();
    let mut entry = AutomationEntry::new("manager", "fixture", "labels", 1);
    entry.enabled = true;
    entry.steps = vec![AutomationStep::GithubProjection];
    entry.github_projection = Some(config::GithubProjectionSettings {
        source_id: "fixture".into(),
        label: "accepted".into(),
        present: true,
    });
    (db, entry)
}

fn observe(db: &Connection, stream: &str, kind: &str) {
    db.execute(
        "INSERT INTO observations(source_stream_id,kind,payload_json,recorded_at_ms) \
         VALUES(?1,?2,'{}',1)",
        params![stream, kind],
    )
    .unwrap();
}

#[test]
fn quiet_host_catch_up_covers_global_cut_without_acceptance_facts() {
    let (mut db, entry) = fixture();
    observe(&db, "controller:read-position:operation", "read.position");
    let tx = db.transaction().unwrap();
    let cut = super::super::automation::observation_cut(&tx).unwrap();
    configure_activation(&tx, None, &entry, true, cut, 1).unwrap();
    let result = reconcile_entry(&tx, &entry, 1, 2).unwrap();
    assert_eq!(result["processed"], 0);
    assert_eq!(result["cursor"], cut);
    assert_eq!(result["catch_up_until"], Value::Null);
    let effects: i64 = tx
        .query_row("SELECT count(*) FROM operations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(effects, 0);
}

#[test]
fn bounded_catch_up_preserves_pages_and_defers_facts_after_activation_cut() {
    let (mut db, entry) = fixture();
    for _ in 0..3 {
        observe(&db, ACCEPTANCE_STREAM, "task.acceptance");
    }
    observe(&db, "controller:other", "task.acceptance");
    let tx = db.transaction().unwrap();
    let cut = super::super::automation::observation_cut(&tx).unwrap();
    configure_activation(&tx, None, &entry, true, cut, 1).unwrap();
    observe(&tx, ACCEPTANCE_STREAM, "task.acceptance");

    let no_budget = reconcile_entry(&tx, &entry, 0, 2).unwrap();
    assert_eq!(no_budget["cursor"], 0);
    assert_eq!(no_budget["catch_up_until"], cut);
    for expected_cursor in [1, 2] {
        let page = reconcile_entry(&tx, &entry, 1, 3).unwrap();
        assert_eq!(page["cursor"], expected_cursor);
        assert_eq!(page["catch_up_until"], cut);
    }
    let last_replay_page = reconcile_entry(&tx, &entry, 1, 4).unwrap();
    assert_eq!(last_replay_page["cursor"], cut);
    assert_eq!(last_replay_page["catch_up_until"], Value::Null);
    let live_page = reconcile_entry(&tx, &entry, 1, 5).unwrap();
    assert_eq!(live_page["cursor"], cut + 1);
    assert_eq!(live_page["catch_up_until"], Value::Null);
    let effects: i64 = tx
        .query_row("SELECT count(*) FROM operations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(effects, 0, "unbound facts cannot reserve GitHub effects");
}
