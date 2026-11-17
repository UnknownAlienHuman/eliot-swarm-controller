use super::*;

fn explain_plan(store: &Store, sql: &str, parameters: impl rusqlite::Params) -> String {
    let mut statement = store
        .connection
        .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
        .expect("prepare EXPLAIN QUERY PLAN");
    let rows = statement
        .query_map(parameters, |row| row.get::<_, String>(3))
        .expect("read query plan");
    let mut plan = Vec::new();
    for row in rows {
        plan.push(row.expect("plan row"));
    }
    plan.join("\n")
}

fn assert_ordered_index_plan(plan: &str, table: &str, access: &str) {
    assert!(
        plan.contains(access),
        "query plan should contain {access}, got:\n{plan}"
    );
    for line in plan.lines() {
        assert!(
            !(line.contains(&format!("SCAN {table}")) && !line.contains("USING INDEX")),
            "query plan should not table-scan {table}, got:\n{plan}"
        );
    }
    assert!(
        !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
        "query plan should not sort, got:\n{plan}"
    );
}

fn insert_command(store: &Store, id: &str, target: i64, verb: &str, state: &str, created_at: i64) {
    store
        .connection
        .execute(
            "INSERT INTO commands(id, principal, method, payload_digest, target, verb, args,
                 state, created_at, updated_at)
             VALUES (?1, 'owner', 'command.submit', x'00', ?2, ?3, '{}', ?4, ?5, ?5)",
            rusqlite::params![id, target, verb, state, created_at],
        )
        .expect("insert command");
}

#[test]
fn ordered_store_reads_install_v18_indexes() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    let found: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index'
               AND name IN ('commands_unsettled_created', 'processes_owner_identity')",
            [],
            |row| row.get(0),
        )
        .expect("count v18 indexes");
    assert_eq!(found, 2);
    // The unordered commands_open partial index is redundant with
    // commands_unsettled_created and would keep the old sort plan.
    let dropped: i64 = store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master
             WHERE type = 'index' AND name = 'commands_open'",
            [],
            |row| row.get(0),
        )
        .expect("count dropped index");
    assert_eq!(dropped, 0);
}

#[test]
fn ordered_store_reads_keep_unsettled_order_and_observer_exclusions() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    store
        .connection
        .execute(
            "INSERT INTO agents(project, harness, profile, state, created_at, updated_at)
             VALUES ('proj', 'fake', 'fake', 'idle', 1000, 1000)",
            [],
        )
        .expect("insert agent");
    let agent = store.connection.last_insert_rowid();
    let mut observers = Vec::new();
    for _ in 0..2 {
        store
            .connection
            .execute(
                "INSERT INTO agents(project, harness, profile, state, origin, observe_only,
                     created_at, updated_at)
                 VALUES ('proj', 'codex', 'codex', 'unknown', 'external', 1, 1000, 1000)",
                [],
            )
            .expect("insert observer");
        observers.push(store.connection.last_insert_rowid());
    }
    let observed_with_source = observers[0];
    let observed_without_source = observers[1];
    store
        .connection
        .execute(
            "INSERT INTO source_state(agent, generation, source, state, health, updated_at)
             VALUES (?1, 0, 'stream', 'open', 'ok', 1000)",
            [observed_with_source],
        )
        .expect("insert open source");

    insert_command(&store, "cmd-applied", agent, "send", "applied", 100);
    insert_command(&store, "cmd-rejected", agent, "send", "rejected", 150);
    insert_command(&store, "cmd-accepted", agent, "send", "accepted", 200);
    insert_command(&store, "cmd-queued", agent, "send", "queued", 250);
    insert_command(&store, "cmd-dispatched", agent, "send", "dispatched", 300);
    insert_command(&store, "cmd-tie-first", agent, "send", "queued", 400);
    insert_command(&store, "cmd-tie-second", agent, "send", "accepted", 400);
    insert_command(
        &store,
        "cmd-unknown-send",
        observed_with_source,
        "send",
        "unknown",
        500,
    );
    insert_command(
        &store,
        "cmd-unknown-attach-hidden",
        observed_without_source,
        "attach",
        "unknown",
        550,
    );
    insert_command(
        &store,
        "cmd-unknown-attach-visible",
        observed_with_source,
        "attach",
        "unknown",
        600,
    );
    insert_command(
        &store,
        "cmd-unknown-detach-managed",
        agent,
        "detach",
        "unknown",
        700,
    );

    let expected = [
        "cmd-accepted",
        "cmd-queued",
        "cmd-dispatched",
        "cmd-tie-first",
        "cmd-tie-second",
        "cmd-unknown-send",
        "cmd-unknown-attach-visible",
        "cmd-unknown-detach-managed",
    ];
    let listed = store.list_unsettled_commands(100).expect("list unsettled");
    let ids: Vec<&str> = listed.iter().map(|command| command.id.as_str()).collect();
    assert_eq!(ids, expected);
    let first_page = store.list_unsettled_commands(3).expect("first page");
    let first_ids: Vec<&str> = first_page
        .iter()
        .map(|command| command.id.as_str())
        .collect();
    assert_eq!(first_ids, expected[..3]);

    let list_sql = format!(
        "{} WHERE {} ORDER BY created_at, rowid LIMIT ?1",
        crate::LIST_UNSETTLED_COMMANDS_COLUMNS,
        crate::UNSETTLED_COMMANDS_WHERE
    );
    let plan = explain_plan(&store, &list_sql, rusqlite::params![100]);
    assert_ordered_index_plan(
        &plan,
        "commands",
        "SCAN commands USING INDEX commands_unsettled_created",
    );
    store
        .connection
        .execute_batch("ANALYZE")
        .expect("analyze Store");
    let analyzed_plan = explain_plan(&store, &list_sql, rusqlite::params![100]);
    assert_ordered_index_plan(
        &analyzed_plan,
        "commands",
        "SCAN commands USING INDEX commands_unsettled_created",
    );
}

/// Pre-split `list_processes` statement: the optional-cursor OR form. Kept as the
/// page-identity oracle for the split first-page/cursor-range queries.
fn legacy_process_page(
    store: &Store,
    owner: &str,
    after: Option<&ProcessIdentity>,
    limit: i64,
) -> Vec<ProcessRecord> {
    const LEGACY_SQL: &str =
        "SELECT host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at
             FROM processes WHERE owner = ?1
               AND (?2 IS NULL OR (host_boot_id, pid, start_time) > (?2, ?3, ?4))
             ORDER BY host_boot_id, pid, start_time LIMIT ?5";
    let start = after
        .map(crate::validate_process_identity)
        .transpose()
        .expect("valid cursor");
    let mut statement = store
        .connection
        .prepare(LEGACY_SQL)
        .expect("prepare legacy page");
    let rows = statement
        .query_map(
            rusqlite::params![
                owner,
                after.map(|id| id.host_boot_id.as_str()),
                after.map(|id| id.pid),
                start,
                limit
            ],
            crate::process_record_from_row,
        )
        .expect("read legacy page");
    rows.collect::<Result<Vec<_>, _>>()
        .expect("collect legacy page")
}

#[test]
fn live_process_owners_reports_only_non_exited_custody_with_honest_totals() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    insert_process(&store, "boot", 1, 1, "agent:a1#1", "running");
    insert_process(&store, "boot", 2, 1, "agent:a1#1", "exited");
    insert_process(&store, "boot", 3, 1, "agent:a2#3", "unknown");
    insert_process(&store, "boot", 4, 1, "workspace:a3#1", "running");
    insert_process(&store, "boot", 5, 1, "workspace:a4#2", "exited");
    insert_process(&store, "boot", 6, 1, "bogus-owner", "running");
    let (owners, total) = store.live_process_owners(100).expect("live owners");
    // Four distinct owners retain non-exited records; the malformed owner
    // counts toward the total but never fabricates custody.
    assert_eq!(total, 4);
    assert_eq!(owners.len(), 3);
    assert!(!owners.iter().any(|owner| owner.agent == "bogus-owner"));
    let plugin: Vec<(&str, i64)> = owners
        .iter()
        .filter(|owner| !owner.workspace)
        .map(|owner| (owner.agent.as_str(), owner.generation))
        .collect();
    assert_eq!(plugin, [("a1", 1), ("a2", 3)]);
    let workspace: Vec<(&str, i64)> = owners
        .iter()
        .filter(|owner| owner.workspace)
        .map(|owner| (owner.agent.as_str(), owner.generation))
        .collect();
    assert_eq!(workspace, [("a3", 1)]);
    let (bounded, bounded_total) = store.live_process_owners(2).expect("bounded owners");
    assert_eq!(bounded_total, 4);
    assert_eq!(bounded.len(), 2);
}

#[test]
fn ordered_store_reads_paginate_processes_by_owner_identity() {
    let store = Store::open(Path::new(":memory:")).expect("open Store");
    let owner = "owner-a";
    for (boot, pid, start) in [
        ("boot-b", 100, 10),
        ("boot-a", 300, 30),
        ("boot-a", 100, 20),
        ("boot-a", 100, 10),
        ("boot-b", 50, 5),
    ] {
        insert_process(&store, boot, pid, start, owner, "running");
    }
    insert_process(&store, "boot-a", 1, 1, "owner-b", "running");
    let expected = [
        ("boot-a", 100, 10),
        ("boot-a", 100, 20),
        ("boot-a", 300, 30),
        ("boot-b", 50, 5),
        ("boot-b", 100, 10),
    ]
    .map(|(boot, pid, start)| ProcessIdentity {
        host_boot_id: boot.to_owned(),
        pid,
        start_time: start,
    });

    let full = store.list_processes(owner, None, 100).expect("list all");
    let ids: Vec<ProcessIdentity> = full.iter().map(|row| row.identity.clone()).collect();
    assert_eq!(ids, expected);

    let mut walked = Vec::new();
    let mut after: Option<ProcessIdentity> = None;
    loop {
        let page = store
            .list_processes(owner, after.as_ref(), 2)
            .expect("walk page");
        if page.is_empty() {
            break;
        }
        after = Some(page.last().expect("nonempty page").identity.clone());
        walked.extend(page);
    }
    assert_eq!(walked, full);

    let other = store
        .list_processes("owner-b", None, 100)
        .expect("other owner");
    assert_eq!(other.len(), 1);

    // A cursor between rows seeks the range start; a tail cursor ends the walk.
    let between = store
        .list_processes(
            owner,
            Some(&ProcessIdentity {
                host_boot_id: "boot-a".to_owned(),
                pid: 100,
                start_time: 15,
            }),
            100,
        )
        .expect("between page");
    assert_eq!(between.first().expect("between row").identity, expected[1]);
    let past_tail = store
        .list_processes(owner, Some(&expected[4]), 100)
        .expect("past tail page");
    assert!(past_tail.is_empty());

    // Identical pages before/after the split on every cursor position.
    let mut cursors: Vec<Option<ProcessIdentity>> = vec![None];
    cursors.extend(expected.iter().cloned().map(Some));
    for cursor in &cursors {
        for limit in [1usize, 2, 100] {
            assert_eq!(
                store
                    .list_processes(owner, cursor.as_ref(), limit)
                    .expect("split page"),
                legacy_process_page(&store, owner, cursor.as_ref(), limit as i64),
                "split page differs at cursor {cursor:?} limit {limit}"
            );
        }
    }
    assert_eq!(
        store
            .list_processes("owner-b", None, 10)
            .expect("split other"),
        legacy_process_page(&store, "owner-b", None, 10)
    );

    assert_eq!(
        store
            .list_processes(
                owner,
                Some(&ProcessIdentity {
                    host_boot_id: String::new(),
                    pid: 1,
                    start_time: 0,
                }),
                10,
            )
            .expect_err("empty boot id")
            .code,
        "invalid"
    );
    assert_eq!(
        store
            .list_processes(owner, Some(&expected[0]), 0)
            .expect_err("empty limit")
            .code,
        "invalid"
    );
    assert_eq!(
        store
            .list_processes("", None, 10)
            .expect_err("empty owner")
            .code,
        "invalid"
    );

    let first_plan = explain_plan(
        &store,
        crate::LIST_PROCESSES_FIRST_PAGE_SQL,
        rusqlite::params![owner, 2],
    );
    assert_ordered_index_plan(
        &first_plan,
        "processes",
        "SEARCH processes USING INDEX processes_owner_identity (owner=?",
    );
    store
        .connection
        .execute_batch("ANALYZE")
        .expect("analyze Store");
    let analyzed_first = explain_plan(
        &store,
        crate::LIST_PROCESSES_FIRST_PAGE_SQL,
        rusqlite::params![owner, 2],
    );
    assert_ordered_index_plan(
        &analyzed_first,
        "processes",
        "SEARCH processes USING INDEX processes_owner_identity (owner=?",
    );
    let analyzed_tail = explain_plan(
        &store,
        crate::LIST_PROCESSES_CURSOR_PAGE_SQL,
        rusqlite::params![owner, "boot-b", 100u32, 10i64, 2],
    );
    assert_ordered_index_plan(
        &analyzed_tail,
        "processes",
        "SEARCH processes USING INDEX processes_owner_identity (owner=?",
    );
}

#[test]
fn enforce_page_bytes_keeps_longest_fitting_prefix() {
    // Reference quadratic sizing: reserialize each shrinking prefix.
    fn reference_keep(items: &[String]) -> usize {
        let mut keep = items.len();
        while serde_json::to_vec(&items[..keep])
            .expect("encode prefix")
            .len()
            > crate::MAX_PAGE_BYTES
        {
            keep -= 1;
            if keep == 0 {
                break;
            }
        }
        keep
    }

    // Mixed entry sizes, including bytes that must be escaped in JSON
    // (quotes, backslashes, control characters) so the byte count covers
    // escaped output rather than in-memory length.
    let small = "a\"b\\c\n\u{1f600}".to_owned();
    let medium = "m".repeat(1_000);
    let large = "L\"\\".repeat(200_000);
    let tail = "t".repeat(500);
    let candidates = vec![small.clone(), medium.clone(), large.clone(), tail.clone()];
    let expected = reference_keep(&candidates);

    let mut sized = candidates.clone();
    crate::enforce_page_bytes(&mut sized).expect("mixed page fits a prefix");
    assert_eq!(
        sized.len(),
        expected,
        "linear sizing must keep the same prefix as reserializing each prefix"
    );
    assert_eq!(&sized, &candidates[..expected]);
    let encoded = serde_json::to_vec(&sized).expect("encode kept prefix");
    assert!(encoded.len() <= crate::MAX_PAGE_BYTES);
    if expected < candidates.len() {
        let mut extended = sized.clone();
        extended.push(candidates[expected].clone());
        assert!(
            serde_json::to_vec(&extended)
                .expect("encode overflow")
                .len()
                > crate::MAX_PAGE_BYTES,
            "kept prefix must be the longest fitting one"
        );
    }
}

#[test]
fn enforce_page_bytes_rejects_oversized_first_entry() {
    let oversized = "x".repeat(crate::MAX_PAGE_BYTES);
    let mut items = vec![oversized];
    let error = crate::enforce_page_bytes(&mut items).expect_err("first entry cannot fit");
    assert_eq!(error.code, "frame_too_large");
    assert!(items.is_empty());

    let mut empty: Vec<String> = Vec::new();
    crate::enforce_page_bytes(&mut empty).expect("empty page stays empty");
}

#[test]
fn exact_turn_unknown_mask_uses_covering_index_and_matches_per_turn() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = ready_agent(&mut store, "turn-unknown-index", now);
    let other_agent = ready_agent(&mut store, "turn-unknown-index-other", now + 1);
    const SETTLED_TURNS: usize = 64;
    let mut turn_ids = Vec::with_capacity(SETTLED_TURNS + 2);
    for n in 0..SETTLED_TURNS {
        let turn_id = format!("page-turn-{n:04}");
        let send = send_request(&agent, 1, &format!("page turn {n}"));
        let send_id = send.id.clone();
        let base = now + 10 + n as i64;
        store.submit("owner", send, base).expect("admit send");
        store.dispatch(&send_id, base + 1).expect("dispatch send");
        for (kind, native_id, data, at) in [
            (
                "turn.started",
                format!("page-turn-{n:04}:started"),
                json!({"command_id": send_id, "turn_id": turn_id}),
                base + 2,
            ),
            (
                "turn.completed",
                format!("page-turn-{n:04}:completed"),
                json!({"command_id": send_id, "turn_id": turn_id, "status": "completed"}),
                base + 3,
            ),
        ] {
            store
                .put_batch(
                    SourceBatch {
                        batch_id: format!("page-turn-batch-{n:04}-{kind}"),
                        agent_id: agent.clone(),
                        generation: 1,
                        source: "stream".to_owned(),
                        events: vec![NativeEvent {
                            ts: None,
                            kind: kind.to_owned(),
                            native_id,
                            data,
                        }],
                        cursor: None,
                    },
                    at,
                )
                .expect("commit settled turn");
        }
        turn_ids.push(turn_id);
    }
    let live = now + 100_000;
    let other_first = send_request(&other_agent, 1, "other agent primer");
    let other_first_id = other_first.id.clone();
    store
        .submit("owner", other_first, live)
        .expect("admit other primer");
    store
        .dispatch(&other_first_id, live + 1)
        .expect("dispatch other primer");
    store
        .put_batch(
            SourceBatch {
                batch_id: "other-turn-started".to_owned(),
                agent_id: other_agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "other-turn:started".to_owned(),
                    data: json!({"command_id": other_first_id, "turn_id": "other-turn"}),
                }],
                cursor: None,
            },
            live + 2,
        )
        .expect("commit other turn start");
    let mut other_send = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    other_send.target = Some(other_agent.clone());
    other_send.expected_generation = Some(1);
    other_send.expected_turn_id = Some("other-turn".to_owned());
    let other_send_id = other_send.id.clone();
    store
        .submit("owner", other_send, live + 3)
        .expect("admit other send");
    store
        .dispatch(&other_send_id, live + 4)
        .expect("dispatch other send");
    store
        .settle(&other_send_id, CommandState::Unknown, None, live + 5)
        .expect("settle other unknown");
    let first = send_request(&agent, 1, "page active turn");
    let first_id = first.id.clone();
    store
        .submit("owner", first, live + 10)
        .expect("admit first");
    store
        .dispatch(&first_id, live + 11)
        .expect("dispatch first");
    store
        .put_batch(
            SourceBatch {
                batch_id: "page-active-started".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "turn.started".to_owned(),
                    native_id: "page-active:started".to_owned(),
                    data: json!({"command_id": first_id, "turn_id": "page-active"}),
                }],
                cursor: None,
            },
            live + 12,
        )
        .expect("commit page active turn");
    let mut second = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    second.target = Some(agent.clone());
    second.expected_generation = Some(1);
    second.expected_turn_id = Some("page-active".to_owned());
    let second_id = second.id.clone();
    store
        .submit("owner", second, live + 13)
        .expect("admit second");
    store
        .dispatch(&second_id, live + 14)
        .expect("dispatch second");
    let mut third = SubmitCommand::new(Ulid::new().to_string(), "interrupt");
    third.target = Some(agent.clone());
    third.expected_generation = Some(1);
    third.expected_turn_id = Some("page-active".to_owned());
    let third_id = third.id.clone();
    store
        .submit("owner", third, live + 15)
        .expect("admit third");
    store
        .dispatch(&third_id, live + 16)
        .expect("dispatch third");
    store
        .settle(&second_id, CommandState::Unknown, None, live + 17)
        .expect("settle page unknown");
    let memory_unknown = vec![third_id.clone()];
    turn_ids.push("page-active".to_owned());
    turn_ids.push("other-turn".to_owned());
    let mut expected = std::collections::HashSet::new();
    for turn_id in &turn_ids {
        if store
            .turn_has_unknown_command(&agent, 1, turn_id, &memory_unknown)
            .expect("per-turn mask")
        {
            expected.insert(turn_id.clone());
        }
    }
    assert_eq!(
        expected,
        std::collections::HashSet::from(["page-active".to_owned()])
    );
    let actual = store
        .turns_with_unknown_commands(&agent, 1, &turn_ids, &memory_unknown)
        .expect("page mask");
    assert_eq!(actual, expected);
    assert!(
        store
            .turns_with_unknown_commands(&agent, 1, &[], &memory_unknown)
            .expect("empty page")
            .is_empty()
    );
    assert_eq!(
        store
            .turns_with_unknown_commands(&other_agent, 1, &["other-turn".to_owned()], &[])
            .expect("other agent mask"),
        std::collections::HashSet::from(["other-turn".to_owned()])
    );
    let plan: Vec<String> = store
        .connection
        .prepare(
            "EXPLAIN QUERY PLAN SELECT DISTINCT turn_id FROM commands INDEXED BY commands_turn_unknown
             WHERE target=?1 AND generation=?2 AND turn_id IN (SELECT value FROM json_each(?3))
               AND state IN ('accepted','queued','dispatched','unknown')
               AND (state='unknown' OR id IN (SELECT value FROM json_each(?4)))",
        )
        .expect("prepare plan")
        .query_map(rusqlite::params![1_i64, 1_i64, "[]", "[]"], |row| {
            row.get::<_, String>(3)
        })
        .expect("run plan")
        .collect::<Result<Vec<_>, _>>()
        .expect("read plan");
    assert!(!plan.is_empty());
    assert!(
        plan.iter()
            .any(|line| line.contains("commands_turn_unknown")),
        "plan uses covering index: {plan:?}"
    );
    assert!(
        !plan.iter().any(|line| line.contains("SCAN commands")),
        "plan avoids full scan: {plan:?}"
    );
}
