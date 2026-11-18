/// Retained-unknown drain predicate: exhaustive over every stored unknown,
// with no page cap and no id lists as bind parameters.
use super::*;

#[test]
fn retained_unknown_predicate_covers_every_unknown_without_id_lists() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    // Deliberately beyond the old 1,000-row classification page: every one
    // of these must classify, however old, or the oldest would block drain
    // forever after a restart empties the in-memory unknown cache.
    const RETAINED: usize = 1050;
    let mut first_id = String::new();
    for i in 0..RETAINED {
        let id = Ulid::new().to_string();
        store
            .submit(
                "owner",
                SubmitCommand {
                    id: id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args: json!({"project":"retained-drain", "workspace":"synthetic-workspace",
                        "harness":"codex", "profile":"fake", "profile_snapshot":{}}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now + i as i64,
            )
            .expect("admit retained launch");
        store
            .dispatch(&id, now + i as i64)
            .expect("dispatch launch");
        if i == 0 {
            first_id = id;
        }
    }
    store
        .recover_on_restart(now + 100_000)
        .expect("restart recovery");
    assert_eq!(
        store.count_retained_unknown_commands().unwrap(),
        RETAINED as i64,
        "every stored unknown classifies, however old"
    );
    assert_eq!(
        store.count_unsettled_commands_excluding_retained().unwrap(),
        0
    );
    assert!(
        store
            .list_unsettled_commands_excluding_retained(64)
            .unwrap()
            .is_empty()
    );
    assert!(
        store.is_retained_unknown(&first_id).unwrap(),
        "per-id check agrees with the set predicate on the oldest row"
    );
    // A later live command sorts behind the whole retained history yet stays
    // listed: the predicate excludes in SQL before the bound with a single
    // LIMIT bind, so no bind-parameter limit is approached at any scale.
    let live_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: live_id.clone(),
                target: None,
                selector: None,
                verb: "launch".to_owned(),
                args: json!({"project":"retained-drain-live", "workspace":"synthetic-workspace",
                    "harness":"codex", "profile":"fake", "profile_snapshot":{}}),
                expected_generation: Some(0),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 200_000,
        )
        .expect("admit live launch");
    store
        .dispatch(&live_id, now + 200_000)
        .expect("dispatch live launch");
    assert!(!store.is_retained_unknown(&live_id).unwrap());
    let page = store
        .list_unsettled_commands_excluding_retained(64)
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].id, live_id);
    assert_eq!(
        store.count_unsettled_commands_excluding_retained().unwrap(),
        1
    );
    assert_eq!(
        store.count_retained_unknown_commands().unwrap(),
        RETAINED as i64
    );
    let retained_page = store.list_retained_unknown_commands(64).unwrap();
    assert_eq!(retained_page.len(), 64);
    assert!(
        retained_page
            .iter()
            .all(|command| command.state == CommandState::Unknown)
    );
}
