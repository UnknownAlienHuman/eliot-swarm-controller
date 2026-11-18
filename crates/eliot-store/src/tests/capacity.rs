use super::*;

#[cfg(windows)]
#[test]
fn custody_binding_preserves_windows_case_and_junction_aliases() {
    let root = std::env::temp_dir().join(format!("eliot-custody-alias-{}", Ulid::new()));
    let dir = root.join("MixedCaseData");
    fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("eliot.db")).unwrap();
    let namespace = store.custody_namespace().unwrap();
    drop(store);
    let case_alias = PathBuf::from(dir.to_str().unwrap().to_ascii_uppercase());
    let store = Store::open(&case_alias.join("eliot.db")).unwrap();
    assert_eq!(store.custody_namespace().unwrap(), namespace);
    drop(store);

    let junction = root.join("junction");
    let output = std::process::Command::new("cmd.exe")
        .args(["/d", "/c", "mklink", "/J"])
        .arg(&junction)
        .arg(&dir)
        .output()
        .unwrap();
    if output.status.success() {
        let store = Store::open(&junction.join("eliot.db")).unwrap();
        assert_eq!(store.custody_namespace().unwrap(), namespace);
        drop(store);
        fs::remove_dir(&junction).unwrap();
    } else {
        eprintln!(
            "junction alias unavailable on this host: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    fs::remove_dir_all(root).unwrap();
}

#[cfg(unix)]
#[test]
fn custody_binding_preserves_non_utf8_data_directories() {
    use std::os::unix::ffi::OsStringExt;
    let root = std::env::temp_dir().join(format!("eliot-custody-bytes-{}", Ulid::new()));
    let dir = root.join(std::ffi::OsString::from_vec(b"data-\xff".to_vec()));
    fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("eliot.db")).unwrap();
    let namespace = store.custody_namespace().unwrap();
    drop(store);
    let store = Store::open(&dir.join("eliot.db")).unwrap();
    assert_eq!(store.custody_namespace().unwrap(), namespace);
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn backup_at_another_data_dir_has_its_own_namespace_for_the_same_next_generation() {
    let root = std::env::temp_dir().join(format!("eliot-custody-copy-{}", Ulid::new()));
    let a = root.join("a");
    let b = root.join("b");
    fs::create_dir_all(&a).unwrap();
    fs::create_dir_all(&b).unwrap();
    let mut original = Store::open(&a.join("eliot.db")).unwrap();
    original
        .connection
        .execute("VACUUM INTO ?1", [b.join("eliot.db").to_str().unwrap()])
        .unwrap();
    let mut copy = Store::open(&b.join("eliot.db")).unwrap();
    assert_eq!(
        original.inference_controller_id().unwrap(),
        copy.inference_controller_id().unwrap()
    );
    assert_ne!(
        original.custody_namespace().unwrap(),
        copy.custody_namespace().unwrap()
    );
    let now = Ulid::new().timestamp_ms() as i64;
    let mut generations = Vec::new();
    for store in [&mut original, &mut copy] {
        let launch = capacity_launch_request("copy", "copy", json!([]));
        let id = launch.id.clone();
        let agent = store
            .submit("owner", launch, now)
            .unwrap()
            .command
            .target
            .unwrap();
        store.dispatch(&id, now).unwrap();
        generations.push((
            agent.clone(),
            store
                .generation_custody_namespace(&agent, 1)
                .unwrap()
                .unwrap(),
        ));
    }
    assert_eq!(generations[0].0, generations[1].0);
    assert_ne!(generations[0].1, generations[1].1);
    drop((original, copy));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn retained_namespace_survives_restart_copy_and_pruned_start_event() {
    let root = std::env::temp_dir().join(format!("eliot-custody-retained-{}", Ulid::new()));
    let a = root.join("a");
    let b = root.join("b");
    fs::create_dir_all(&a).unwrap();
    fs::create_dir_all(&b).unwrap();
    let path = a.join("eliot.db");
    let mut store = Store::open(&path).unwrap();
    let namespace = store.custody_namespace().unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    let launch = capacity_launch_request("retained-copy", "copy", json!([]));
    let id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .unwrap()
        .command
        .target
        .unwrap();
    store.dispatch(&id, now).unwrap();
    store.recover_on_restart(now + 1).unwrap();
    store
        .connection
        .execute("DELETE FROM events WHERE kind='agent.starting'", [])
        .unwrap();
    store
        .connection
        .execute("VACUUM INTO ?1", [b.join("eliot.db").to_str().unwrap()])
        .unwrap();
    drop(store);
    let original = Store::open(&a.join(".").join("eliot.db")).unwrap();
    assert_eq!(original.custody_namespace().unwrap(), namespace);
    let copy = Store::open(&b.join("eliot.db")).unwrap();
    assert_ne!(copy.custody_namespace().unwrap(), namespace);
    for store in [&original, &copy] {
        assert_eq!(
            store.generation_custody_namespace(&agent, 1).unwrap(),
            Some(namespace.clone())
        );
        assert_eq!(
            store.restart_retained_generations(None).unwrap()[0]
                .custody_namespace
                .as_deref(),
            Some(namespace.as_str())
        );
        assert_eq!(store.generation_custody_namespace(&agent, 2).unwrap(), None);
    }
    drop((original, copy));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn schema_51_upgrade_records_the_legacy_generation_name_before_reminting() {
    let path = capacity_store_path();
    let mut store = Store::open(&path).unwrap();
    let old_namespace = store.inference_controller_id().unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    let launch = capacity_launch_request("legacy-named", "copy", json!([]));
    let id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .unwrap()
        .command
        .target
        .unwrap();
    store.dispatch(&id, now).unwrap();
    store.connection.execute_batch("DROP TABLE IF EXISTS chat_organization; DELETE FROM schema_migrations WHERE version IN (64,65); DROP INDEX IF EXISTS agents_recent; DELETE FROM schema_migrations WHERE version=63; DROP TABLE launch_intents; DROP TABLE assignment_waits; DROP TABLE arbitration_cursor; DROP TABLE manager_budgets; DELETE FROM schema_migrations WHERE version=62; ALTER TABLE artifacts DROP COLUMN seq; DELETE FROM schema_migrations WHERE version=60; ALTER TABLE turns DROP COLUMN effective_model;
                 ALTER TABLE turns DROP COLUMN native_error_message;
                 ALTER TABLE workspaces DROP COLUMN original_preparation;
                 DROP TABLE project_transfers; DELETE FROM schema_migrations WHERE version IN (60,61);
                 DELETE FROM schema_migrations WHERE version=59;
                 DROP INDEX IF EXISTS commands_prune_roots; DROP INDEX IF EXISTS completion_digests_pending_cursor; DROP INDEX IF EXISTS agents_list_archived_state; DROP INDEX IF EXISTS workspaces_start_command; DELETE FROM schema_migrations WHERE version=58; ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 DELETE FROM schema_migrations WHERE version=57;
                 ALTER TABLE turns DROP COLUMN resolution_source;
                 DELETE FROM schema_migrations WHERE version=56;
                 ALTER TABLE commands DROP COLUMN assignment_binding;
                 ALTER TABLE cleanup_plans DROP COLUMN assignment_binding;
                 DROP TABLE project_assignments;
                 DELETE FROM schema_migrations WHERE version=55;
                 DROP TABLE paused_assignment_timers; ALTER TABLE rule_state DROP COLUMN source_present;
                 DELETE FROM schema_migrations WHERE version=54;
                 DROP INDEX agents_list_project_scope; ALTER TABLE agents DROP COLUMN project_scope;
                 DELETE FROM schema_migrations WHERE version=53;
                 DROP TABLE custody_binding; ALTER TABLE agents DROP COLUMN custody_namespace; DELETE FROM schema_migrations WHERE version=52;").unwrap();
    drop(store);
    let store = Store::open(&path).unwrap();
    let git_hints_index: bool = store.connection.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='index' AND name='events_git_hints')", [], |row| row.get(0)).unwrap();
    assert!(
        git_hints_index,
        "schema51 index survives the custody upgrade"
    );
    assert_ne!(store.custody_namespace().unwrap(), old_namespace);
    assert_eq!(
        store.generation_custody_namespace(&agent, 1).unwrap(),
        Some(old_namespace)
    );
    drop(store);
    remove_capacity_store_files(&path);
}

#[test]
fn configured_capacity_scopes_are_reserved_atomically_and_retries_are_idempotent() {
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let mut account_store = Store::open(Path::new(":memory:")).expect("open Store");
    let first = capacity_launch_request(
        "capacity-account-first",
        "profile-a",
        json!([
            {"scope":"profile:profile-a","max_live":null},
            {"scope":"account:shared-account","max_live":1},
            {"scope":"proxy:route-a","max_live":null}
        ]),
    );
    let first_admission = account_store
        .submit("owner", first.clone(), now)
        .expect("reserve all configured scopes at admission");
    let first_agent = first_admission.command.target.as_deref().unwrap();
    assert_eq!(capacity_generation(&account_store, first_agent), Some(1));
    let high_water = account_store.high_water().unwrap();
    assert!(
        !account_store
            .submit("owner", first, now + 1)
            .expect("retained id lookup precedes capacity reservation")
            .is_new
    );
    assert_eq!(account_store.high_water().unwrap(), high_water);
    let account_blocked = capacity_launch_request(
        "capacity-account-second",
        "profile-b",
        json!([
            {"scope":"profile:profile-b","max_live":null},
            {"scope":"account:shared-account","max_live":1},
            {"scope":"proxy:route-b","max_live":null}
        ]),
    );
    let account_blocked_id = account_blocked.id.clone();
    let error = account_store
        .submit("owner", account_blocked, now + 2)
        .unwrap_err();
    assert_eq!(error.code, "capacity_unavailable");
    assert!(error.message.contains("account:shared-account"));
    assert!(error.message.contains("max_live=1"));
    assert_eq!(account_store.agent_count().unwrap(), 1);
    assert!(
        account_store
            .get_command(&account_blocked_id)
            .unwrap()
            .is_none()
    );

    let mut named_store = Store::open(Path::new(":memory:")).expect("open Store");
    let named_small = capacity_launch_request(
        "capacity-named-small",
        "fixture-small",
        json!([{"scope":"profile:fixture-small","max_live":1}]),
    );
    let named_small_agent = named_store
        .submit("owner", named_small, now)
        .expect("Store admits a configured named small profile")
        .command
        .target
        .expect("named small Agent");
    let named_small_agent = named_store.get_agent(&named_small_agent).unwrap().unwrap();
    assert_eq!(named_small_agent.harness, "codex");
    assert_eq!(named_small_agent.profile, "fixture-small");

    let mut proxy_store = Store::open(Path::new(":memory:")).expect("open Store");
    proxy_store
        .submit(
            "owner",
            capacity_launch_request(
                "capacity-proxy-first",
                "profile-c",
                json!([
                    {"scope":"profile:profile-c","max_live":null},
                    {"scope":"account:account-c","max_live":null},
                    {"scope":"proxy:shared-route","max_live":1}
                ]),
            ),
            now,
        )
        .expect("admit first proxy-scoped Agent");
    assert_eq!(
        proxy_store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-proxy-second",
                    "profile-d",
                    json!([
                        {"scope":"profile:profile-d","max_live":null},
                        {"scope":"account:account-d","max_live":null},
                        {"scope":"proxy:shared-route","max_live":1}
                    ]),
                ),
                now + 1,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let mut profile_store = Store::open(Path::new(":memory:")).expect("open Store");
    profile_store
        .submit(
            "owner",
            capacity_launch_request(
                "capacity-profile-first",
                "profile-limited",
                json!([
                    {"scope":"profile:profile-limited","max_live":1},
                    {"scope":"account:account-e","max_live":null},
                    {"scope":"proxy:route-e","max_live":null}
                ]),
            ),
            now,
        )
        .expect("admit first profile-scoped Agent");
    assert_eq!(
        profile_store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-profile-second",
                    "profile-limited",
                    json!([
                        {"scope":"profile:profile-limited","max_live":1},
                        {"scope":"account:account-f","max_live":null},
                        {"scope":"proxy:route-f","max_live":null}
                    ]),
                ),
                now + 1,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let mut disabled_store = Store::open(Path::new(":memory:")).expect("open Store");
    assert_eq!(
        disabled_store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-disabled",
                    "profile-disabled",
                    json!([{"scope":"profile:profile-disabled","max_live":0}]),
                ),
                now,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let mut unlimited_store = Store::open(Path::new(":memory:")).expect("open Store");
    for (project, profile) in [
        ("capacity-unlimited-one", "profile-unlimited-one"),
        ("capacity-unlimited-two", "profile-unlimited-two"),
    ] {
        unlimited_store
            .submit(
                "owner",
                capacity_launch_request(
                    project,
                    profile,
                    json!([
                        {"scope":format!("profile:{profile}"),"max_live":null},
                        {"scope":"account:unlimited","max_live":null},
                        {"scope":"proxy:unlimited","max_live":null}
                    ]),
                ),
                now,
            )
            .expect("None capacity is unlimited");
    }
}

#[test]
fn unknown_start_capacity_survives_recovery_and_store_reopen() {
    let path = capacity_store_path();
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let first = capacity_launch_request(
        "capacity-unknown-first",
        "profile-unknown-first",
        json!([
            {"scope":"profile:profile-unknown-first","max_live":null},
            {"scope":"account:unknown-shared","max_live":1}
        ]),
    );
    let first_id = first.id.clone();
    let first_agent;
    {
        let mut store = Store::open(&path).expect("open persistent Store");
        first_agent = store
            .submit("owner", first, now)
            .expect("admit launch")
            .command
            .target
            .expect("launch Agent");
        store.dispatch(&first_id, now + 1).expect("dispatch launch");
        store
            .settle(
                &first_id,
                CommandState::Unknown,
                Some(json!({"reason":"start_readback_unknown"})),
                now + 2,
            )
            .expect("retain uncertain native start");
        store
            .recover_on_restart(now + 3)
            .expect("restart keeps uncertain start");
        assert_eq!(capacity_generation(&store, &first_agent), Some(1));
    }
    {
        let mut reopened = Store::open(&path).expect("reopen persistent Store");
        assert_eq!(capacity_generation(&reopened, &first_agent), Some(1));
        assert_eq!(
            reopened
                .submit(
                    "owner",
                    capacity_launch_request(
                        "capacity-unknown-second",
                        "profile-unknown-second",
                        json!([
                            {"scope":"profile:profile-unknown-second","max_live":null},
                            {"scope":"account:unknown-shared","max_live":1}
                        ]),
                    ),
                    now + 4,
                )
                .unwrap_err()
                .code,
            "capacity_unavailable"
        );
    }
    remove_capacity_store_files(&path);
}

#[test]
fn restart_readback_skips_exited_children_and_bounds_live_records() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch = capacity_launch_request(
        "restart-heavy",
        "restart-heavy",
        json!([{"scope": "profile:restart-heavy", "max_live": null}]),
    );
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit launch")
        .command
        .target
        .expect("launch Agent");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch launch");
    store
        .settle(
            &launch_id,
            CommandState::Unknown,
            Some(json!({"reason":"start_readback_unknown"})),
            now + 2,
        )
        .expect("retain uncertain native start");
    store.recover_on_restart(now + 3).expect("recover");
    let owner = format!("agent:{agent}#1");
    let insert_on = |store: &Store, boot: &str, pid: i64, role: &str, state: &str| {
        store
            .connection
            .execute(
                "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
                 VALUES (?6, ?1, 1, ?2, ?3, ?4, ?5, 1)",
                rusqlite::params![
                    pid,
                    owner,
                    role,
                    state,
                    (state == "exited").then_some(0),
                    boot
                ],
            )
            .expect("record process");
    };
    let insert = |store: &Store, pid: i64, role: &str, state: &str| {
        insert_on(store, "boot-a", pid, role, state)
    };
    // A worker that ran compilers leaves far more exited children than one
    // page; only one exited witness per boot is custody evidence and startup
    // must not fail.
    insert(&store, 1, "plugin", "exited");
    for pid in 2..=(crate::MAX_PAGE_ENTRIES as i64 + 600) {
        insert(&store, pid, "child", "exited");
    }
    insert(&store, 9_000_000, "child", "running");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(retained.len(), 1);
    assert!(!retained[0].processes_truncated);
    let roles: Vec<_> = retained[0]
        .processes
        .iter()
        .map(|process| (process.role.as_str(), process.exited))
        .collect();
    assert_eq!(roles, [("plugin", true), ("child", true), ("child", false)]);
    assert_eq!(retained[0].processes[1].identity.pid, 2);

    // Boot presence survives: an exited child on another boot still yields one
    // witness for that boot, so Linux custody sees the same boots as before.
    insert_on(&store, "boot-b", 5, "child", "exited");
    insert_on(&store, "boot-b", 6, "child", "exited");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    let boots: Vec<_> = retained[0]
        .processes
        .iter()
        .map(|process| (process.identity.host_boot_id.as_str(), process.identity.pid))
        .collect();
    assert_eq!(
        boots,
        [
            ("boot-a", 1),
            ("boot-a", 2),
            ("boot-a", 9_000_000),
            ("boot-b", 5)
        ]
    );

    // An overflow of records that still need a liveness check keeps only this
    // generation retained, without failing the readback.
    for pid in 10_000_000..(10_000_000 + crate::MAX_PAGE_ENTRIES as i64) {
        insert(&store, pid, "child", "running");
    }
    let retained = store
        .restart_retained_generations(None)
        .expect("bounded readback never fails startup");
    assert!(retained[0].processes_truncated);
    assert_eq!(retained[0].processes.len(), crate::MAX_PAGE_ENTRIES);
}

#[test]
fn verified_restart_closure_releases_only_the_retained_lost_generation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let scopes = |profile: &str| {
        json!([
            {"scope": format!("profile:{profile}"), "max_live": null},
            {"scope": "account:restart-shared", "max_live": 1}
        ])
    };
    let launch = capacity_launch_request("restart-first", "restart-first", scopes("restart-first"));
    let launch_id = launch.id.clone();
    let agent = store
        .submit("owner", launch, now)
        .expect("admit launch")
        .command
        .target
        .expect("launch Agent");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch launch");
    store
        .settle(
            &launch_id,
            CommandState::Unknown,
            Some(json!({"reason":"start_readback_unknown"})),
            now + 2,
        )
        .expect("retain uncertain native start");
    store.recover_on_restart(now + 3).expect("recover");

    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(retained.len(), 1);
    assert_eq!(
        (retained[0].agent.as_str(), retained[0].generation),
        (agent.as_str(), 1)
    );
    assert!(retained[0].custody_namespace.is_some());
    assert!(retained[0].processes.is_empty());
    assert!(
        store
            .restart_retained_generations(Some(&agent))
            .expect("read next page")
            .is_empty()
    );

    let notice = store
        .record_restart_capacity_retained(&agent, 1, "windows_job_live", now + 5)
        .expect("record retained reservation");
    assert_eq!(notice.len(), 1);
    assert_eq!(notice[0].kind, "capacity.retained");
    assert_eq!(notice[0].data["missing"], "windows_job_live");

    // Closure for any other generation cannot touch the retained slot.
    assert!(
        store
            .release_restart_closed_generation(&agent, 2, "windows_job_closed", false, now + 6)
            .expect("stale closure")
            .is_empty()
    );
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let other =
        || capacity_launch_request("restart-other", "restart-other", scopes("restart-other"));
    assert_eq!(
        store.submit("owner", other(), now + 7).unwrap_err().code,
        "capacity_unavailable"
    );

    let released = store
        .release_restart_closed_generation(&agent, 1, "unix_group_empty", true, now + 8)
        .expect("release verified closure");
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].kind, "capacity.released");
    assert_eq!(released[0].data["reason"], "restart_custody_closed");
    assert_eq!(released[0].data["evidence"], "unix_group_empty");
    assert_eq!(released[0].data["orphan_possible"], true);
    assert_eq!(capacity_generation(&store, &agent), None);
    assert!(
        store
            .restart_retained_generations(None)
            .expect("nothing retained")
            .is_empty()
    );
    assert!(
        store
            .release_restart_closed_generation(&agent, 1, "unix_group_empty", true, now + 9)
            .expect("repeated closure")
            .is_empty()
    );
    store
        .submit("owner", other(), now + 10)
        .expect("shared scope admits after verified closure");
}

#[test]
fn generations_started_before_the_custody_namespace_are_not_named() {
    let path = capacity_store_path();
    let mut store = Store::open(&path).expect("open persistent Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let launch = capacity_launch_request(
        "restart-legacy",
        "restart-legacy",
        json!([{"scope":"profile:restart-legacy","max_live":null}]),
    );
    let launch_id = launch.id.clone();
    store.submit("owner", launch, now).expect("admit launch");
    store
        .dispatch(&launch_id, now + 1)
        .expect("dispatch launch");
    // Return the file to schema 16, as an older binary left it, and upgrade.
    // Drop the v38 inference, v37 digest, v34 config and v33 session-usage tables first and
    // undo the v36/v35 column adds, so the reopen exercises their migration
    // blocks, then the v32 attention-wake table. Drop the
    // v29/v30 activity artifacts next: the v30 events triggers would
    // otherwise fire during the table rebuilds below while their
    // native_child_registrations reads no longer resolve (#418). Drop the
    // v31 fleet indexes too, or the parent-column drop below fails and the
    // reopen would recreate existing indexes.
    store
        .connection
        .execute_batch(
            "DROP TABLE IF EXISTS chat_organization; DELETE FROM schema_migrations WHERE version IN (64,65); DROP INDEX IF EXISTS agents_recent; DELETE FROM schema_migrations WHERE version=63; DROP TABLE launch_intents; DROP TABLE assignment_waits; DROP TABLE arbitration_cursor; DROP TABLE manager_budgets; DELETE FROM schema_migrations WHERE version=62; ALTER TABLE artifacts DROP COLUMN seq; DELETE FROM schema_migrations WHERE version=60; ALTER TABLE turns DROP COLUMN effective_model;
                 ALTER TABLE turns DROP COLUMN native_error_message;
                 ALTER TABLE workspaces DROP COLUMN original_preparation;
                 DROP TABLE project_transfers; DELETE FROM schema_migrations WHERE version IN (60,61);
                 DELETE FROM schema_migrations WHERE version=59;
                 DROP INDEX IF EXISTS commands_prune_roots; DROP INDEX IF EXISTS completion_digests_pending_cursor; DROP INDEX IF EXISTS agents_list_archived_state; DROP INDEX IF EXISTS workspaces_start_command; DELETE FROM schema_migrations WHERE version=58; ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 DELETE FROM schema_migrations WHERE version=57;
                 ALTER TABLE turns DROP COLUMN resolution_source;
                 DELETE FROM schema_migrations WHERE version=56;
                 ALTER TABLE commands DROP COLUMN assignment_binding;
                 ALTER TABLE cleanup_plans DROP COLUMN assignment_binding;
                 DROP TABLE project_assignments;
                 DELETE FROM schema_migrations WHERE version=55;
                 DROP TABLE paused_assignment_timers; ALTER TABLE rule_state DROP COLUMN source_present;
                 DELETE FROM schema_migrations WHERE version=54;
                 DROP INDEX agents_list_project_scope; ALTER TABLE agents DROP COLUMN project_scope;
                 DELETE FROM schema_migrations WHERE version=53;
                 DROP TABLE custody_binding; ALTER TABLE agents DROP COLUMN custody_namespace;
                 DELETE FROM schema_migrations WHERE version=52;
                 DROP INDEX events_git_hints;
                 DELETE FROM schema_migrations WHERE version=51;
                 DROP INDEX processes_live_owner;
             DELETE FROM schema_migrations WHERE version=50;
             DROP INDEX events_heavy_agent_generation;
             DELETE FROM schema_migrations WHERE version=49;
             DROP INDEX agents_list_archived;
                 ALTER TABLE agents DROP COLUMN archived;
                 ALTER TABLE agents DROP COLUMN chat;
                 DELETE FROM schema_migrations WHERE version=48;
                 DROP INDEX writes_principal_method; DELETE FROM schema_migrations WHERE version = 47;
             DROP TABLE artifacts; DROP TABLE cleanup_plans;
             DROP INDEX commands_turn_unknown;
             DROP TRIGGER assignment_commands_insert;
             DROP TRIGGER assignment_commands_update;
             DROP TRIGGER assignment_commands_delete;
             DROP TRIGGER assignment_turns_insert;
             DROP TRIGGER assignment_turns_update;
             DROP TRIGGER assignment_turns_delete;
             DROP VIEW assignment_busy_membership;
             DROP INDEX commands_assignment_busy;
             DROP INDEX commands_assignment_turn;
             ALTER TABLE commands DROP COLUMN assignment_busy;
             DELETE FROM schema_migrations WHERE version = 43;
             ALTER TABLE agents DROP COLUMN profile_choice;
             DROP INDEX settings_history_created;
                     DROP TABLE settings_history;
                     DELETE FROM schema_migrations WHERE version = 41;
                     DROP INDEX inference_write_inventory;
                     ALTER TABLE writes DROP COLUMN inference_seq;
                     ALTER TABLE turns DROP COLUMN preceded_by;
             DELETE FROM schema_migrations WHERE version = 45;
             DELETE FROM schema_migrations WHERE version >= 39;
                     DROP TABLE inference_calls;
             DROP TABLE inference_clients;
             DELETE FROM schema_migrations WHERE version = 38;
             DROP TABLE completion_digests;
             DELETE FROM schema_migrations WHERE version = 37;
             DROP TABLE agent_config;
             ALTER TABLE agents DROP COLUMN profile_inherited;
             DELETE FROM schema_migrations WHERE version = 36;
             DELETE FROM schema_migrations WHERE version = 35;
             DELETE FROM schema_migrations WHERE version = 34;
             ALTER TABLE commands DROP COLUMN config_revision;
             ALTER TABLE turns DROP COLUMN config_revision;
             DROP TABLE session_usage;
             DELETE FROM schema_migrations WHERE version = 33;
             DROP TABLE attention_wakes;
             DELETE FROM schema_migrations WHERE version = 32;
             DROP INDEX agents_list_state;
             DROP INDEX agents_list_project;
             DROP INDEX agents_list_harness;
             DROP INDEX agents_list_parent;
             DROP INDEX agents_list_root;
             DELETE FROM schema_migrations WHERE version = 31;
             DROP TRIGGER native_child_activity_revision;
             DROP TRIGGER native_child_progress_pruned;
             DROP INDEX native_children_producer;
             DROP INDEX turns_native_activity;
             DROP INDEX events_native_progress_time;
             DROP INDEX events_git_progress_time;
             ALTER TABLE agents DROP COLUMN native_child_revision;
             DELETE FROM schema_migrations WHERE version = 30;
             ALTER TABLE turns DROP COLUMN outcome;
             ALTER TABLE turns DROP COLUMN native_status;
             DELETE FROM schema_migrations WHERE version = 29;
             DROP INDEX agents_parent; DROP INDEX agents_root;
             ALTER TABLE agents DROP COLUMN parent; ALTER TABLE agents DROP COLUMN root;
             ALTER TABLE agents DROP COLUMN depth; ALTER TABLE agents DROP COLUMN control;
             ALTER TABLE commands DROP COLUMN selector;
             DELETE FROM schema_migrations WHERE version = 23;
             DROP TABLE store_identity; DELETE FROM schema_migrations WHERE version = 17;
             DROP INDEX commands_unsettled_created;
             DROP INDEX processes_owner_identity;
             CREATE INDEX commands_open ON commands(state)
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown');
             DELETE FROM schema_migrations WHERE version = 18;
             DROP TABLE goals; DROP TABLE goal_markers;
             DELETE FROM schema_migrations WHERE version = 19;
             DELETE FROM schema_migrations WHERE version = 20;
             DROP INDEX messages_prune_acked;
             DELETE FROM schema_migrations WHERE version = 22;
             DROP TABLE messages; DROP TABLE vars; DROP TABLE mail_notices;
             DELETE FROM schema_migrations WHERE version = 21;
             DROP TABLE claims;
             DELETE FROM schema_migrations WHERE version = 24;
             DROP TABLE owned_exits;
             DELETE FROM schema_migrations WHERE version = 25;
             DROP TABLE native_child_registrations;
             DELETE FROM schema_migrations WHERE version = 26;
             ALTER TABLE turns DROP COLUMN usage;
             DELETE FROM schema_migrations WHERE version = 27;
             DROP INDEX commands_context_pending;
             DELETE FROM schema_migrations WHERE version = 28;",
        )
        .expect("roll back to schema 16");
    drop(store);
    let mut store = Store::open(&path).expect("migrate 16 to latest");
    store.recover_on_restart(now + 2).expect("recover");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(retained.len(), 1);
    assert!(retained[0].custody_namespace.is_none());
    assert!(!store.custody_namespace().expect("namespace").is_empty());
    drop(store);
    remove_capacity_store_files(&path);
}

#[test]
fn capacity_migration_uses_commands_and_physical_facts_without_rewriting_snapshots() {
    let path = capacity_store_path();
    let pending_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:pending","max_live":1}],"keep":"pending"}"#;
    let rejected_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:rejected","max_live":1}],"keep":"rejected"}"#;
    let unknown_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:unknown","max_live":1}],"keep":"unknown"}"#;
    let ended_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:ended","max_live":1}],"keep":"ended"}"#;
    let closed_only_snapshot = r#"{"capacity_scopes":[{"scope":"profile:closed-only","max_live":1}],"keep":"closed-only"}"#;
    let owned_exit_snapshot =
        r#"{"capacity_scopes":[{"scope":"profile:owned-exit","max_live":1}],"keep":"owned-exit"}"#;
    let rejected_dispatched_snapshot = r#"{"capacity_scopes":[{"scope":"profile:rejected-dispatched","max_live":1}],"keep":"rejected-dispatched"}"#;
    let rejected_resume_snapshot = r#"{"capacity_scopes":[{"scope":"profile:rejected-resume","max_live":1}],"keep":"rejected-resume"}"#;
    let pending_resume_snapshot = r#"{"capacity_scopes":[{"scope":"profile:pending-resume","max_live":1}],"keep":"pending-resume"}"#;
    {
        let old = rusqlite::Connection::open(&path).expect("create v8 fixture");
        old.execute_batch(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY);
             INSERT INTO schema_migrations VALUES (1),(2),(3),(4),(5),(6),(7),(8);
             CREATE TABLE writes (
                 id TEXT PRIMARY KEY, principal TEXT NOT NULL, method TEXT NOT NULL,
                 payload_digest BLOB NOT NULL, result TEXT NOT NULL, created_at INTEGER NOT NULL
             );
             CREATE TABLE agents(
                 id INTEGER PRIMARY KEY, generation INTEGER NOT NULL,
                 observe_only INTEGER NOT NULL, profile TEXT NOT NULL,
                 profile_snapshot TEXT NOT NULL,
                 state TEXT NOT NULL, project TEXT NOT NULL, harness TEXT NOT NULL,
                 updated_at INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE commands(
                 id TEXT PRIMARY KEY, principal TEXT NOT NULL, method TEXT NOT NULL,
                 payload_digest BLOB NOT NULL, target INTEGER REFERENCES agents(id),
                 expected_generation INTEGER, generation INTEGER, expected_turn_id TEXT,
                 parent_command TEXT REFERENCES commands(id), link_kind TEXT, ordinal INTEGER,
                 turn_id TEXT, request_id TEXT, verb TEXT NOT NULL, args TEXT NOT NULL,
                 state TEXT NOT NULL, outcome TEXT, created_at INTEGER NOT NULL,
                 updated_at INTEGER NOT NULL,
                 CHECK((parent_command IS NULL AND link_kind IS NULL AND ordinal IS NULL)
                    OR (parent_command IS NOT NULL AND link_kind IS NOT NULL
                        AND ordinal IS NOT NULL AND ordinal >= 0))
             );
             CREATE INDEX commands_target ON commands(target, created_at);
             CREATE INDEX commands_open ON commands(state)
                 WHERE state IN ('accepted', 'queued', 'dispatched', 'unknown');
             CREATE UNIQUE INDEX commands_children
                 ON commands(parent_command, ordinal)
                 WHERE parent_command IS NOT NULL;
             CREATE INDEX commands_request_open
                 ON commands(target, generation, request_id, state)
                 WHERE request_id IS NOT NULL;
             CREATE TABLE events(
                 seq INTEGER PRIMARY KEY AUTOINCREMENT,
                 agent INTEGER, generation INTEGER, family TEXT,
                 kind TEXT, source TEXT, data TEXT,
                 ts INTEGER NOT NULL DEFAULT 0,
                 native_id TEXT
             );
             CREATE TABLE turns(
                 agent INTEGER NOT NULL REFERENCES agents(id),
                 generation INTEGER NOT NULL,
                 turn_id TEXT NOT NULL,
                 state TEXT NOT NULL,
                 status TEXT,
                 final_text TEXT,
                 started_at INTEGER,
                 completed_at INTEGER,
                 PRIMARY KEY(agent, generation, turn_id)
             );
             CREATE TABLE source_state(
                 agent INTEGER, generation INTEGER, source TEXT, state TEXT
             );
             CREATE TABLE processes(
                 host_boot_id TEXT NOT NULL CHECK(length(host_boot_id) > 0),
                 pid INTEGER NOT NULL CHECK(pid > 0 AND pid <= 4294967295),
                 start_time INTEGER NOT NULL CHECK(start_time >= 0),
                 owner TEXT NOT NULL,
                 role TEXT NOT NULL,
                 state TEXT NOT NULL CHECK(state IN ('running', 'exited', 'unknown')),
                 exit_code INTEGER,
                 updated_at INTEGER NOT NULL,
                 PRIMARY KEY(host_boot_id, pid, start_time),
                 CHECK(exit_code IS NULL OR state = 'exited')
             );
             CREATE INDEX processes_owner ON processes(owner, state);
             CREATE TABLE workspaces (
                 agent INTEGER PRIMARY KEY REFERENCES agents(id),
                 target_generation INTEGER NOT NULL CHECK(target_generation > 0),
                 start_command_id TEXT NOT NULL REFERENCES commands(id),
                 project TEXT NOT NULL, repo_path TEXT NOT NULL, repo_common_dir TEXT NOT NULL,
                 requested_branch TEXT NOT NULL, base_ref TEXT, base_oid TEXT NOT NULL,
                 worktree_path TEXT NOT NULL UNIQUE, frozen_setup TEXT NOT NULL,
                 phase TEXT NOT NULL CHECK(phase IN ('creating','setup_ready','setup_running','ready','unknown','failed','removing','removed')),
                 setup_index INTEGER NOT NULL DEFAULT 0 CHECK(setup_index >= 0),
                 outcome TEXT, updated_at INTEGER NOT NULL
             );",
        )
        .expect("create v8 tables");
        for (id, generation, profile, snapshot) in [
            (1_i64, 0_i64, "pending", pending_snapshot),
            (2, 0, "rejected", rejected_snapshot),
            (3, 1, "unknown", unknown_snapshot),
            (4, 1, "ended", ended_snapshot),
            (5, 1, "closed-only", closed_only_snapshot),
            (6, 1, "owned-exit", owned_exit_snapshot),
            (7, 0, "unadmitted", "{}"),
            (8, 1, "rejected-dispatched", rejected_dispatched_snapshot),
            (9, 1, "rejected-resume", rejected_resume_snapshot),
            (10, 1, "pending-resume", pending_resume_snapshot),
        ] {
            old.execute(
                "INSERT INTO agents(id, generation, observe_only, profile, profile_snapshot,
                                    state, project, harness)
                 VALUES (?1, ?2, 0, ?3, ?4, 'idle', 'capacity-test', 'test-harness')",
                rusqlite::params![id, generation, profile, snapshot],
            )
            .expect("insert v8 Agent");
        }
        old.execute(
            "INSERT INTO commands(
                 id, principal, method, payload_digest, target, generation,
                 verb, args, state, created_at, updated_at
             ) VALUES
                 ('pending-launch','owner','command.submit',zeroblob(32),1,0,'launch','{}','accepted',1,1),
                 ('rejected-launch','owner','command.submit',zeroblob(32),2,0,'launch','{}','rejected',2,2),
                 ('unknown-launch','owner','command.submit',zeroblob(32),3,1,'launch','{}','unknown',3,3),
                 ('rejected-dispatched-start','owner','command.submit',zeroblob(32),8,1,'launch','{}','rejected',4,4),
                 ('rejected-resume','owner','command.submit',zeroblob(32),9,1,'resume','{}','rejected',5,5),
                 ('pending-resume','owner','command.submit',zeroblob(32),10,1,'resume','{}','accepted',6,6)",
            [],
        )
        .expect("insert retained start commands");
        old.execute(
            "INSERT INTO events(agent, generation, family, kind, source, data)
             VALUES (4,1,'session','session.ended','stream','{}'),
                    (10,1,'session','session.ended','stream','{}'),
                    (8,1,'agent','agent.starting','daemon','{\"command_id\":\"rejected-dispatched-start\"}')",
            [],
        )
        .expect("insert terminal native fact");
        old.execute(
            "INSERT INTO source_state(agent, generation, source, state)
             VALUES (4,1,'stream','closed'),
                    (4,1,'os','closed'),
                    (5,1,'stream','closed'),
                    (6,1,'stream','closed'),
                    (6,1,'os','closed'),
                    (10,1,'stream','closed'),
                    (10,1,'os','closed')",
            [],
        )
        .expect("insert source facts");
        old.execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state,
                 exit_code, updated_at)
             VALUES ('boot',1,0,'agent:a4#1','plugin','exited',NULL,1),
                    ('boot',2,0,'agent:a6#1','plugin','exited',NULL,1),
                    ('boot',3,0,'agent:a10#1','plugin','exited',NULL,1)",
            [],
        )
        .expect("insert owned process exit fact");
    }

    let store = Store::open(&path).expect("migrate v8 Store");
    assert_eq!(capacity_generation(&store, "a1"), Some(1));
    assert_eq!(capacity_generation(&store, "a2"), None);
    assert_eq!(capacity_generation(&store, "a3"), Some(1));
    assert_eq!(capacity_generation(&store, "a4"), None);
    assert_eq!(
        capacity_generation(&store, "a5"),
        Some(1),
        "closed reader source alone is not process-exit proof"
    );
    assert_eq!(capacity_generation(&store, "a6"), None);
    assert_eq!(capacity_generation(&store, "a7"), None);
    assert_eq!(capacity_generation(&store, "a8"), None);
    assert_eq!(
        capacity_generation(&store, "a9"),
        Some(1),
        "pre-dispatch resume rejection does not prove the prior generation ended"
    );
    assert_eq!(
        capacity_generation(&store, "a10"),
        Some(2),
        "retained resume admission reserves its next generation after terminal proof"
    );
    let retained_snapshot: String = store
        .connection
        .query_row(
            "SELECT profile_snapshot FROM agents WHERE id = 5",
            [],
            |row| row.get(0),
        )
        .expect("read legacy snapshot");
    assert_eq!(retained_snapshot, closed_only_snapshot);
    drop(store);
    remove_capacity_store_files(&path);
}

#[test]
fn pre_dispatch_initial_launch_rejection_is_terminal_cancels_linked_input_and_frees_drain_slot() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let mut request = capacity_launch_request(
        "readiness-failure",
        "readiness-profile",
        json!([{"scope":"profile:readiness-profile","max_live":1}]),
    );
    request.args["text"] = json!("launch input never reaches the native process");
    let start_id = request.id.clone();
    let admission = store
        .submit("owner", request, now)
        .expect("admit initial launch");
    let agent = admission.command.target.clone().expect("launch target");
    let linked_id = admission
        .linked_command
        .as_ref()
        .expect("linked launch input")
        .id
        .clone();
    assert_eq!(capacity_generation(&store, &agent), Some(1));

    let settled = store
        .settle(
            &start_id,
            CommandState::Rejected,
            Some(json!({"reason":"readiness_failed"})),
            now + 1,
        )
        .expect("record proven pre-dispatch readiness rejection");
    let failed = store.get_agent(&agent).unwrap().unwrap();
    assert_eq!(failed.generation, 0);
    assert_eq!(failed.state, AgentState::Failed);
    assert!(
        failed.is_terminal(),
        "the never-started Agent no longer blocks drain"
    );
    assert_eq!(store.get_agent_token(&agent, 0).unwrap(), None);
    assert_eq!(capacity_generation(&store, &agent), None);
    assert_eq!(
        store.get_command(&linked_id).unwrap().unwrap().state,
        CommandState::Cancelled
    );
    assert!(settled.events.iter().any(|event| {
        event.kind == "agent.failed" && event.data["reason"] == "launch_not_dispatched"
    }));
    assert!(settled.events.iter().any(|event| {
        event.kind == "command.cancelled" && event.data["command_id"] == linked_id
    }));

    store
        .submit(
            "owner",
            capacity_launch_request(
                "readiness-retry-new-id",
                "readiness-profile",
                json!([{"scope":"profile:readiness-profile","max_live":1}]),
            ),
            now + 2,
        )
        .expect("failed generation-0 launch no longer blocks drain or capacity");
}

#[test]
fn stop_and_source_closure_alone_do_not_release_capacity_or_fail_host_settlement() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _) = ready_capacity_agent(&mut store, "capacity-stop-proof", now);
    store
        .ensure_process_source(&agent, 1, now + 1)
        .expect("install OS process source");

    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                selector: None,
                verb: "stop".to_owned(),
                args: json!({}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 2,
        )
        .expect("admit stop");
    store.dispatch(&stop_id, now + 2).unwrap();
    store
        .settle(&stop_id, CommandState::Applied, None, now + 3)
        .expect("stop acknowledgement does not prove process exit");
    assert_eq!(capacity_generation(&store, &agent), Some(1));

    store
        .set_process_source_status(&agent, 1, "ok", true, now + 4)
        .expect("persist source closure without an observed process exit");
    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 5)
            .expect("insufficient evidence must not block Lost/source settlement")
            .is_empty()
    );
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    assert_eq!(
        store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-stop-blocked",
                    "resume-profile",
                    json!([{"scope":"profile:resume-profile","max_live":1}]),
                ),
                now + 6,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable"
    );

    let numeric_agent = eliot_core::parse_agent_id(&agent).unwrap();
    let process_owner = format!("agent:{agent}#1");
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
             VALUES ('capacity-stop-boot', 81002, 1, ?1, 'plugin', 'exited', 0, ?2)",
            rusqlite::params![process_owner, now + 7],
        )
        .expect("persist exact exited process record");
    store
        .close_native_sources(&agent, 1, now + 8)
        .expect("persist native reader drain independently");
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let events = store
        .record_owned_control_exit(&agent, 1, now + 9)
        .expect("matching OS, process, and reader facts release capacity");
    let released = events
        .iter()
        .find(|event| event.kind == "capacity.released")
        .expect("capacity release records custody guarantee");
    assert_eq!(
        released.data["custody"],
        if cfg!(windows) { "exact" } else { "degraded" }
    );
    assert_eq!(released.data["orphan_possible"], !cfg!(windows));
    assert_eq!(capacity_generation(&store, &agent), None);
    assert!(numeric_agent > 0);
}

/// A force-stopped generation that never committed `session.ended`, with its
/// plugin process recorded as exited; returns the Agent id. The stop already
/// closed its native sources administratively.
fn stopped_capacity_agent_without_session_end(
    store: &mut Store,
    project: &str,
    now: i64,
) -> String {
    let (agent, _) = ready_capacity_agent(store, project, now);
    store
        .ensure_process_source(&agent, 1, now + 1)
        .expect("install OS process source");
    let stop_id = Ulid::new().to_string();
    store
        .submit(
            "owner",
            SubmitCommand {
                id: stop_id.clone(),
                target: Some(agent.clone()),
                selector: None,
                verb: "stop".to_owned(),
                args: json!({"force": true}),
                expected_generation: Some(1),
                expected_turn_id: None,
                request_id: None,
                probe: false,
            },
            now + 2,
        )
        .expect("admit forced stop");
    store.dispatch(&stop_id, now + 2).unwrap();
    store
        .settle(&stop_id, CommandState::Applied, None, now + 3)
        .expect("forced stop applied without session.ended");
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
             VALUES ('capacity-order-boot', 81003, 1, ?1, 'plugin', 'exited', 0, ?2)",
            rusqlite::params![format!("agent:{agent}#1"), now + 4],
        )
        .expect("persist exact exited plugin process");
    assert_eq!(capacity_generation(store, &agent), Some(1));
    agent
}

#[test]
fn owned_exit_capacity_releases_when_the_os_source_closes_last() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = stopped_capacity_agent_without_session_end(&mut store, "capacity-os-last", now);
    // The host proves exit and drain before the monitor's next sweep closes `os`.
    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 5)
            .expect("owned exit with OS still open")
            .is_empty()
    );
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let events = store
        .set_process_source_status(&agent, 1, "ok", true, now + 6)
        .expect("monitor closes the OS source");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "capacity.released")
            .count(),
        1,
        "the last exit fact releases the reservation: {events:?}"
    );
    assert_eq!(capacity_generation(&store, &agent), None);
    let repeated = store
        .set_process_source_status(&agent, 1, "ok", true, now + 7)
        .expect("repeated closure");
    assert!(
        repeated
            .iter()
            .all(|event| event.kind != "capacity.released")
    );
}

#[test]
fn os_closure_without_a_host_drain_proof_keeps_capacity() {
    // A forced stop whose reader never drained: the host records no owned-exit
    // proof, so closing `os` must not release although the stop closed the
    // native sources administratively.
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent = stopped_capacity_agent_without_session_end(&mut store, "capacity-no-drain", now);
    let events = store
        .set_process_source_status(&agent, 1, "ok", true, now + 5)
        .expect("monitor closes the OS source");
    assert!(events.iter().all(|event| event.kind != "capacity.released"));
    assert_eq!(capacity_generation(&store, &agent), Some(1));
    let released = store
        .record_owned_control_exit(&agent, 1, now + 6)
        .expect("the host drain proof arrives last");
    assert!(
        released
            .iter()
            .any(|event| event.kind == "capacity.released")
    );
    assert_eq!(capacity_generation(&store, &agent), None);
}

#[test]
fn restart_reconciles_a_stopped_generation_that_kept_its_reservation() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let agent =
        stopped_capacity_agent_without_session_end(&mut store, "capacity-restart-stopped", now);
    store.recover_on_restart(now + 5).expect("recover");
    let retained = store
        .restart_retained_generations(None)
        .expect("read retained generations");
    assert_eq!(
        retained.len(),
        1,
        "a stopped generation still holding capacity is retained"
    );
    assert_eq!(
        (retained[0].agent.as_str(), retained[0].generation),
        (agent.as_str(), 1)
    );
    let released = store
        .release_restart_closed_generation(&agent, 1, "windows_job_closed", false, now + 6)
        .expect("release verified closure");
    assert!(
        released
            .iter()
            .any(|event| event.kind == "capacity.released")
    );
    assert_eq!(capacity_generation(&store, &agent), None);
}

#[test]
fn session_end_alone_retains_capacity_until_exact_owned_exit_proof_settles() {
    let mut store = Store::open(Path::new(":memory:")).expect("open Store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    let (agent, _) = ready_capacity_agent(&mut store, "capacity-regression", now);
    store
        .ensure_process_source(&agent, 1, now + 1)
        .expect("install OS process source");
    assert_eq!(capacity_generation(&store, &agent), Some(1));

    // End alone keeps reservation.
    store
        .put_batch(
            SourceBatch {
                batch_id: "regression:ended".to_owned(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                cursor: None,
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: "regression:ended".to_owned(),
                    data: json!({}),
                }],
            },
            now + 2,
        )
        .expect("commit session ended");
    assert_eq!(
        store.get_agent(&agent).unwrap().unwrap().state,
        AgentState::Stopped
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(1),
        "session.ended alone must retain capacity reservation"
    );
    assert_eq!(
        store
            .submit(
                "owner",
                capacity_launch_request(
                    "capacity-regression-blocked",
                    "resume-profile",
                    json!([{"scope":"profile:resume-profile","max_live":1}]),
                ),
                now + 3,
            )
            .unwrap_err()
            .code,
        "capacity_unavailable",
        "configured capacity is still held by generation 1"
    );

    // Owned-exit proof without all source/process closure still keeps it.
    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 4)
            .expect("owned-exit proof before physical closure is accepted")
            .is_empty(),
        "unclosed physical facts must not emit capacity.released"
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(1),
        "reservation remains held without physical process/source closure"
    );

    let process_owner = format!("agent:{agent}#1");
    store
        .connection
        .execute(
            "INSERT INTO processes(host_boot_id, pid, start_time, owner, role, state, exit_code, updated_at)
             VALUES ('capacity-regression-boot', 81099, 1, ?1, 'plugin', 'exited', 0, ?2)",
            rusqlite::params![process_owner, now + 5],
        )
        .expect("persist exact exited process record");
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(1),
        "reservation remains held while OS source is still open"
    );

    // Full exact proof releases it even if OS closure arrives later.
    let events = store
        .set_process_source_status(&agent, 1, "ok", true, now + 6)
        .expect("late monitor closure of OS process source");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == "capacity.released")
            .count(),
        1,
        "late OS closure completes the proof and releases reservation: {events:?}"
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        None,
        "reservation is now released"
    );

    // Existing stale-generation isolation must remain.
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    store
        .submit("owner", resume, now + 7)
        .expect("admit resume for generation 2 into released capacity");
    assert_eq!(capacity_generation(&store, &agent), Some(2));

    assert!(
        store
            .record_owned_control_exit(&agent, 1, now + 8)
            .expect("stale generation 1 exit proof is harmless")
            .is_empty(),
        "stale generation exit proof must emit nothing"
    );
    assert_eq!(
        capacity_generation(&store, &agent),
        Some(2),
        "stale generation-1 exit cannot release generation-2 reservation"
    );

    let repeated_os = store
        .set_process_source_status(&agent, 1, "ok", true, now + 9)
        .expect("repeated stale os closure");
    assert!(
        repeated_os
            .iter()
            .all(|event| event.kind != "capacity.released"),
        "stale OS closure cannot release generation-2 reservation"
    );
    assert_eq!(capacity_generation(&store, &agent), Some(2));
}
// Step 2c: project, manager-assignment and global admission scopes share one
// Store transaction with the profile, account and proxy scopes. The unit is a
// controlled generation or reservation, an idle manager included. A missing
// limit is unlimited; lowering a limit blocks new admissions without stopping
// running work; retained unknown reservations stay counted.

use serde_json::Value;

fn stamped_2c_scopes(
    profile: &str,
    _project: &str,
    _global: Option<u64>,
    _project_cap: Option<u64>,
    _manager_cap: Option<u64>,
) -> Value {
    json!([
        {"scope": format!("profile:{profile}"), "max_live": null},
    ])
}

fn install_2c_policy(
    store: &mut Store,
    global: Option<u64>,
    projects: impl IntoIterator<Item = (&'static str, Option<u64>, Option<u64>)>,
) {
    let projects = projects
        .into_iter()
        .map(|(project, project_cap, manager_cap)| {
            (
                project.to_owned(),
                eliot_core::api::ProjectScopeLimits {
                    project: project_cap,
                    manager_assignment: manager_cap,
                    ..Default::default()
                },
            )
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    store.set_capacity_policy(move || {
        Ok(eliot_core::api::ScopeLimits {
            global,
            projects: projects.clone(),
        })
    });
}

fn launch_2c_root(
    store: &mut Store,
    project: &str,
    profile: &str,
    scopes: Value,
    now: i64,
) -> (String, String) {
    let request = capacity_launch_request(project, profile, scopes);
    let launch_id = request.id.clone();
    let admitted = store.submit("owner", request, now).unwrap();
    let agent = admitted.command.target.expect("launch target");
    (launch_id, agent)
}

fn launch_2c_worker(
    store: &mut Store,
    parent: &str,
    project: &str,
    profile: &str,
    scopes: Value,
    now: i64,
) -> (String, String) {
    let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
    request.args = json!({
        "project": project,
        "workspace": "synthetic-workspace",
        "harness": "codex",
        "profile": profile,
        "parent": parent,
        "profile_snapshot": {"capacity_scopes": scopes},
    });
    request.expected_generation = Some(0);
    let launch_id = request.id.clone();
    let admitted = store.submit("owner", request, now).unwrap();
    let agent = admitted.command.target.expect("launch target");
    (launch_id, agent)
}

fn drive_2c_idle(store: &mut Store, agent: &str, launch_id: &str, now: i64) {
    store.dispatch(launch_id, now).unwrap();
    install_fixture_sources(store, agent, launch_id, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.to_owned(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: "ready".to_owned(),
                        data: json!({"command_id": launch_id, "native_scope": "s", "native_ref": agent}),
                }],
                cursor: None,
            },
            now,
        )
        .unwrap();
    store
        .settle(launch_id, CommandState::Applied, None, now)
        .unwrap();
}
#[test]
fn overlapping_project_and_global_caps_bind_one_transaction() {
    let path = capacity_store_path();
    let mut store = Store::open(&path).unwrap();
    install_2c_policy(
        &mut store,
        Some(3),
        [("proj", Some(2), Some(5)), ("other", Some(5), Some(5))],
    );
    let now = Ulid::new().timestamp_ms() as i64;
    let _first = launch_2c_root(
        &mut store,
        "proj",
        "prof-a",
        stamped_2c_scopes("prof-a", "proj", Some(3), Some(2), Some(5)),
        now,
    );
    let _second = launch_2c_root(
        &mut store,
        "proj",
        "prof-b",
        stamped_2c_scopes("prof-b", "proj", Some(3), Some(2), Some(5)),
        now,
    );
    assert_eq!(store.agent_count().unwrap(), 2);

    // The project cap is full: a third profile name cannot evade it, and the
    // refusal names the scope with its Owner remedy.
    let third = capacity_launch_request(
        "proj",
        "prof-c",
        stamped_2c_scopes("prof-c", "proj", Some(3), Some(2), Some(5)),
    );
    let third_id = third.id.clone();
    let failure = store.submit("owner", third, now + 1).unwrap_err();
    assert_eq!(failure.code, "capacity_unavailable");
    assert!(
        failure.message.contains("project:proj"),
        "{}",
        failure.message
    );
    assert!(
        failure
            .message
            .contains("config.toml [capacity.projects.proj] max_live"),
        "{}",
        failure.message
    );
    // Nothing was admitted or reserved: no command, no generation, no Agent.
    assert!(store.get_command(&third_id).unwrap().is_none());
    assert_eq!(store.agent_count().unwrap(), 2);

    // Another project still fits under the global cap.
    let _other = launch_2c_root(
        &mut store,
        "other",
        "prof-d",
        stamped_2c_scopes("prof-d", "other", Some(3), Some(5), Some(5)),
        now + 2,
    );
    assert_eq!(store.agent_count().unwrap(), 3);

    // The global cap is now full: any new admission is refused by name.
    let overflow = capacity_launch_request(
        "other",
        "prof-e",
        stamped_2c_scopes("prof-e", "other", Some(3), Some(5), Some(5)),
    );
    let failure = store.submit("owner", overflow, now + 3).unwrap_err();
    assert_eq!(failure.code, "capacity_unavailable");
    assert!(
        failure.message.contains("global:swarm"),
        "{}",
        failure.message
    );
    assert_eq!(store.agent_count().unwrap(), 3);
    remove_capacity_store_files(&path);
}

#[test]
fn worker_launch_counts_against_its_managers_project() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    install_2c_policy(&mut store, None, [("team", Some(5), Some(0))]);
    let now = Ulid::new().timestamp_ms() as i64;
    let scopes = |profile: &str| stamped_2c_scopes(profile, "team", None, Some(5), Some(2));
    let checkout = std::env::temp_dir();
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: "team".to_owned(),
                checkout: checkout.to_string_lossy().into_owned(),
                repo_common_dir: checkout.join(".git").to_string_lossy().into_owned(),
                git: None,
                profiles: vec!["mgr".to_owned()],
                actions: vec!["launch".to_owned()],
                interpreters: Default::default(),
                run_env: Vec::new(),
            },
            now,
        )
        .unwrap();
    let claim = |store: &mut Store, agent: &str| {
        store
            .claim_project_manager(
                "owner",
                &eliot_core::api::ProjectAssignmentClaimRequest {
                    id: Ulid::new().to_string(),
                    project: "team".to_owned(),
                    manager: Some(eliot_core::api::ProjectManagerIdentity::Local {
                        agent: agent.to_owned(),
                        generation: 1,
                    }),
                },
                now,
            )
            .unwrap()
            .value
            .assignment
    };
    let (manager_launch, manager) = launch_2c_root(&mut store, "team", "mgr", scopes("mgr"), now);
    drive_2c_idle(&mut store, &manager, &manager_launch, now);
    let assignment = claim(&mut store, &manager);
    install_2c_policy(&mut store, None, [("team", Some(5), Some(2))]);
    // The idle manager itself occupies one assignment slot.
    let (_first_launch, _first_worker) =
        launch_2c_worker(&mut store, &manager, "team", "w1", scopes("w1"), now + 1);
    assert_eq!(store.agent_count().unwrap(), 2);

    // A new worker cannot evade the manager-assignment cap.
    let overflow = {
        let mut request = SubmitCommand::new(Ulid::new().to_string(), "launch");
        request.args = json!({
            "project": "team",
            "workspace": "synthetic-workspace",
            "harness": "codex",
            "profile": "w2",
            "parent": manager,
            "profile_snapshot": {"capacity_scopes": scopes("w2")},
        });
        request.expected_generation = Some(0);
        request
    };
    let failure = store.submit("owner", overflow, now + 2).unwrap_err();
    assert_eq!(failure.code, "capacity_unavailable");
    assert!(
        failure.message.contains("manager_assignment:team"),
        "{}",
        failure.message
    );
    assert_eq!(store.agent_count().unwrap(), 2);
    let usage = |store: &Store, scope| {
        crate::capacity::capacity_scope_usage(&store.connection, scope).unwrap()
    };
    assert_eq!(usage(&store, "manager_assignment:team"), 2);
    store
        .release_project_manager(
            "owner",
            &eliot_core::api::ProjectAssignmentReleaseRequest {
                id: Ulid::new().to_string(),
                project: "team".to_owned(),
                expected_epoch: assignment.epoch,
            },
            now + 3,
        )
        .unwrap();
    assert_eq!(usage(&store, "manager_assignment:team"), 0);
    assert_eq!(usage(&store, "project:team"), 2);
    assert_eq!(usage(&store, "global:swarm"), 2);

    let (next_launch, next) = launch_2c_root(&mut store, "team", "mgr", scopes("mgr"), now + 4);
    drive_2c_idle(&mut store, &next, &next_launch, now + 5);
    assert!(claim(&mut store, &next).epoch > assignment.epoch);
    launch_2c_worker(&mut store, &next, "team", "w2", scopes("w2"), now + 6);
    assert_eq!(usage(&store, "manager_assignment:team"), 2);
    assert_eq!(usage(&store, "project:team"), 4);
    assert_eq!(usage(&store, "global:swarm"), 4);
}

#[test]
fn lowered_cap_blocks_new_admissions_without_stopping_work() {
    let path = capacity_store_path();
    let mut store = Store::open(&path).unwrap();
    install_2c_policy(&mut store, None, [("proj", Some(3), None)]);
    let now = Ulid::new().timestamp_ms() as i64;
    let (_first_launch, first) = launch_2c_root(
        &mut store,
        "proj",
        "prof-a",
        stamped_2c_scopes("prof-a", "proj", None, Some(3), None),
        now,
    );
    let (_second_launch, second) = launch_2c_root(
        &mut store,
        "proj",
        "prof-b",
        stamped_2c_scopes("prof-b", "proj", None, Some(3), None),
        now + 1,
    );

    // The Owner lowers the project cap from 3 to 2; the writer-boundary policy
    // is refreshed for this admission, while existing reservations remain.
    install_2c_policy(&mut store, None, [("proj", Some(2), None)]);
    let late = capacity_launch_request(
        "proj",
        "prof-c",
        stamped_2c_scopes("prof-c", "proj", None, Some(3), None),
    );
    let failure = store.submit("owner", late, now + 2).unwrap_err();
    assert_eq!(failure.code, "capacity_unavailable");
    assert!(
        failure.message.contains("project:proj"),
        "{}",
        failure.message
    );

    // Running work is untouched: both reservations stay live and non-terminal.
    for agent in [first, second] {
        let row = store.get_agent(&agent).unwrap().unwrap();
        assert!(capacity_generation(&store, &agent).is_some());
        assert!(!row.is_terminal(), "{agent} must keep running");
    }
    assert_eq!(store.agent_count().unwrap(), 2);
    remove_capacity_store_files(&path);
}

#[test]
fn unknown_reservation_keeps_project_scope_counted() {
    let path = capacity_store_path();
    let mut store = Store::open(&path).unwrap();
    install_2c_policy(&mut store, None, [("proj", Some(1), None)]);
    let now = Ulid::new().timestamp_ms() as i64;
    let (first_id, first) = launch_2c_root(
        &mut store,
        "proj",
        "prof-a",
        stamped_2c_scopes("prof-a", "proj", None, Some(1), None),
        now,
    );
    store.dispatch(&first_id, now + 1).unwrap();
    store
        .settle(
            &first_id,
            CommandState::Unknown,
            Some(json!({"reason": "start_readback_unknown"})),
            now + 2,
        )
        .unwrap();
    store.recover_on_restart(now + 3).unwrap();
    assert_eq!(
        capacity_generation(&store, &first),
        Some(1),
        "retained unknown generation must keep its reservation"
    );

    // The retained reservation still occupies the project scope.
    let late = capacity_launch_request(
        "proj",
        "prof-b",
        stamped_2c_scopes("prof-b", "proj", None, Some(1), None),
    );
    let failure = store.submit("owner", late, now + 4).unwrap_err();
    assert_eq!(failure.code, "capacity_unavailable");
    assert!(
        failure.message.contains("project:proj"),
        "{}",
        failure.message
    );
    assert_eq!(store.agent_count().unwrap(), 1);
    remove_capacity_store_files(&path);
}

#[test]
fn preconstructed_launch_refreshes_daemon_policy_at_reservation_boundary() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    install_2c_policy(&mut store, Some(1), []);
    let now = Ulid::new().timestamp_ms() as i64;
    let first = capacity_launch_request("fresh-policy", "first", json!([]));
    store.submit("owner", first, now).unwrap();

    // This request was constructed while the old policy allowed it. The
    // writer boundary must load the lowered policy before reserving.
    let second = capacity_launch_request("fresh-policy", "second", json!([]));
    install_2c_policy(&mut store, Some(0), []);
    let error = store.submit("owner", second, now + 1).unwrap_err();
    assert_eq!(error.code, "capacity_unavailable");
    assert!(error.message.contains("global:swarm"));
    assert!(error.message.contains("[capacity] global_max_live"));
}

#[test]
fn accepted_launch_replay_survives_a_later_lowered_policy() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    install_2c_policy(&mut store, Some(1), []);
    let now = Ulid::new().timestamp_ms() as i64;
    let launch = capacity_launch_request("accepted-replay", "first", json!([]));
    let replay = launch.clone();
    let first = store.submit("owner", launch, now).unwrap();
    install_2c_policy(&mut store, Some(0), []);
    let second = store.submit("owner", replay, now + 1).unwrap();
    assert!(!second.is_new);
    assert_eq!(second.command.id, first.command.id);
    assert_eq!(second.command.target, first.command.target);
}

#[test]
fn retained_resume_reads_current_lowered_policy() {
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    install_2c_policy(&mut store, Some(1), []);
    let now = Ulid::new().timestamp_ms() as i64;
    let agent =
        stopped_capacity_agent_without_session_end(&mut store, "resume-current-policy", now);
    store.record_owned_control_exit(&agent, 1, now + 5).unwrap();
    store
        .set_process_source_status(&agent, 1, "ok", true, now + 6)
        .unwrap();
    assert_eq!(capacity_generation(&store, &agent), None);
    install_2c_policy(&mut store, Some(0), []);
    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent);
    resume.expected_generation = Some(1);
    let error = store.submit("owner", resume, now + 7).unwrap_err();
    assert_eq!(error.code, "capacity_unavailable");
    assert!(error.message.contains("global:swarm"));
}

#[test]
fn concurrent_admissions_race_under_overlapping_project_and_global_caps() {
    let path = capacity_store_path();
    let now = Ulid::new().timestamp_ms() as i64;
    let race = |projects: &[&str]| {
        // Complete database opening and migrations before the barrier. The
        // contention under test is between immediate admission transactions.
        let contenders: Vec<_> = projects
            .iter()
            .enumerate()
            .map(|(index, project)| {
                let mut store = Store::open(&path).unwrap();
                install_2c_policy(
                    &mut store,
                    Some(3),
                    [
                        ("race-a", Some(2), None),
                        ("race-b", Some(2), None),
                        ("race-c", Some(2), None),
                    ],
                );
                let profile = format!("{project}-{index}");
                (store, capacity_launch_request(project, &profile, json!([])))
            })
            .collect();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(contenders.len()));
        let threads: Vec<_> = contenders
            .into_iter()
            .map(|(mut store, request)| {
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.submit("owner", request, now)
                })
            })
            .collect();
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect::<Vec<_>>()
    };
    let assert_results =
        |results: Vec<eliot_core::Result<crate::Admission>>, accepted, scope: &str| {
            assert_eq!(results.iter().filter(|r| r.is_ok()).count(), accepted);
            for error in results.into_iter().filter_map(Result::err) {
                assert_eq!(error.code, "capacity_unavailable");
                assert!(error.message.contains(scope), "{}", error.message);
            }
        };
    assert_results(race(&["race-a"; 4]), 2, "project:race-a");
    assert_results(
        race(&["race-b", "race-b", "race-c", "race-c"]),
        1,
        "global:swarm",
    );
    let store = Store::open(&path).unwrap();
    assert_eq!(
        crate::capacity::capacity_scope_usage(&store.connection, "project:race-a").unwrap(),
        2
    );
    assert_eq!(
        crate::capacity::capacity_scope_usage(&store.connection, "global:swarm").unwrap(),
        3
    );
    drop(store);
    remove_capacity_store_files(&path);
}

#[test]
fn queued_and_immediate_admissions_share_overlapping_caps() {
    let path = capacity_store_path();
    let now = Ulid::new().timestamp_ms() as i64;
    let open = || {
        let mut store = Store::open(&path).unwrap();
        install_2c_policy(&mut store, Some(2), [("race-a", Some(1), None)]);
        store
    };
    let mut preparer = open();
    let queued = capacity_launch_request("race-a", "interactive", json!([]));
    let intent = eliot_core::api::LaunchQueueRequest {
        id: queued.id.clone(),
        args: queued.args.clone(),
    };
    preparer
        .enqueue_launch("owner", &intent, queued.clone(), "race-a", None, now)
        .unwrap();
    let pending = preparer.pending_launches().unwrap().pop().unwrap();
    let mut queued_writer = open();
    let mut direct_writer = open();
    let direct = capacity_launch_request("race-a", "direct", json!([]));
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let second_barrier = barrier.clone();
    let queued_thread = std::thread::spawn(move || {
        barrier.wait();
        queued_writer.admit_launch_intent(&pending, queued, None, now)
    });
    let direct_thread = std::thread::spawn(move || {
        second_barrier.wait();
        direct_writer.submit("owner", direct, now)
    });
    let queued = queued_thread.join().unwrap();
    let direct = direct_thread.join().unwrap();
    assert_ne!(queued.is_ok(), direct.is_ok());
    let queued_won = queued.is_ok();
    let error = queued.err().or_else(|| direct.err()).unwrap();
    assert_eq!(error.code, "capacity_unavailable");
    assert!(error.message.contains("project:race-a"));
    assert_eq!(
        crate::capacity::capacity_scope_usage(&preparer.connection, "global:swarm").unwrap(),
        1
    );
    let receipt = preparer.launch_intent("owner", &intent.id).unwrap();
    assert_eq!(receipt.command_id.is_some(), queued_won);
    assert_eq!(preparer.arbitration_cursor().unwrap().is_some(), queued_won);
    assert_eq!(
        preparer.pending_launches().unwrap().len(),
        usize::from(!queued_won)
    );
    drop(preparer);
    remove_capacity_store_files(&path);
}

#[test]
fn overview_totals_match_team_and_fleet_rows() {
    let root = std::env::temp_dir().join(format!("eliot-overview-{}", Ulid::new()));
    for name in ["checkout-a", "checkout-b", "common"] {
        fs::create_dir_all(root.join(name)).unwrap();
    }
    let mut store = Store::open(&root.join("eliot.db")).unwrap();
    let now = Ulid::new().timestamp_ms() as i64;
    let unlimited =
        |profile: &str, project: &str| stamped_2c_scopes(profile, project, None, None, None);
    for project in ["team-a", "team-b"] {
        let checkout = root.join("checkout-a");
        store
            .upsert_project_registration(
                "owner",
                &ProjectRegistrationInput {
                    id: project.to_owned(),
                    checkout: checkout.to_str().unwrap().to_owned(),
                    repo_common_dir: root.join("common").to_str().unwrap().to_owned(),
                    git: None,
                    profiles: vec![
                        "m1".to_owned(),
                        "w1".to_owned(),
                        "w2".to_owned(),
                        "o1".to_owned(),
                    ],
                    actions: vec![
                        "launch".to_owned(),
                        eliot_core::api::PROJECT_ASSIGNMENT_CLAIM_ACTION.to_owned(),
                    ],
                    interpreters: std::collections::BTreeMap::new(),
                    run_env: Vec::new(),
                },
                now,
            )
            .unwrap();
    }

    // team-b first: one agent whose unknown start is retained across the
    // restart below, so the later live team is untouched by the recovery.
    let (lost_launch, _lost) = launch_2c_root(
        &mut store,
        "team-b",
        "o1",
        unlimited("o1", "team-b"),
        now + 1,
    );
    store.dispatch(&lost_launch, now + 2).unwrap();
    store
        .settle(
            &lost_launch,
            CommandState::Unknown,
            Some(json!({"reason": "start_readback_unknown"})),
            now + 3,
        )
        .unwrap();
    store.recover_on_restart(now + 4).unwrap();

    // team-a: one claimed manager plus two live workers.
    let (manager_launch, manager) = launch_2c_root(
        &mut store,
        "team-a",
        "m1",
        unlimited("m1", "team-a"),
        now + 5,
    );
    drive_2c_idle(&mut store, &manager, &manager_launch, now + 6);
    store
        .claim_project_manager(
            &format!("agent:{manager}:1"),
            &eliot_core::api::ProjectAssignmentClaimRequest {
                id: Ulid::new().to_string(),
                project: "team-a".to_owned(),
                manager: None,
            },
            now + 7,
        )
        .unwrap();
    let (_w1_launch, first_worker) = launch_2c_worker(
        &mut store,
        &manager,
        "team-a",
        "w1",
        unlimited("w1", "team-a"),
        now + 8,
    );
    let (_w2_launch, second_worker) = launch_2c_worker(
        &mut store,
        &manager,
        "team-a",
        "w2",
        unlimited("w2", "team-a"),
        now + 9,
    );

    // One observed native child of the team-a manager, listed separately.
    let mut attach = SubmitCommand::new(Ulid::new().to_string(), "attach");
    attach.args = json!({
        "mode": "observe",
        "profile": "obs",
        "harness": "codex",
        "native_scope": "obs-scope",
        "native_ref": "obs-ref",
        "profile_snapshot": {"harness": "codex"},
        "project": "team-a",
    });
    let child = store
        .submit("owner", attach, now + 10)
        .unwrap()
        .command
        .target
        .expect("attach target");
    let manager_num = eliot_core::parse_agent_id(&manager).unwrap();
    let child_num = eliot_core::parse_agent_id(&child).unwrap();
    store
        .connection
        .execute(
            "INSERT INTO native_child_registrations(producer, producer_generation, parent, parent_generation, registration_id, child, request, ack) VALUES (?1, 1, ?2, 1, 'obs-reg', ?3, '{}', '{}')",
            rusqlite::params![manager_num, manager_num, child_num],
        )
        .unwrap();

    let limits = eliot_core::api::ScopeLimits {
        global: Some(4),
        projects: [
            (
                "team-a".to_owned(),
                eliot_core::api::ProjectScopeLimits {
                    project: Some(2),
                    manager_assignment: Some(3),
                    ..Default::default()
                },
            ),
            (
                "team-b".to_owned(),
                eliot_core::api::ProjectScopeLimits {
                    project: Some(1),
                    manager_assignment: Some(1),
                    ..Default::default()
                },
            ),
        ]
        .into_iter()
        .collect(),
    };
    let data = store
        .project_overview(
            &limits,
            &eliot_core::api::ProjectOverviewRequest::default(),
            "owner",
            now + 11,
        )
        .unwrap();
    assert!(data.page_complete);

    assert_eq!(data.projects.len(), 2);
    let team_a = &data.projects[0];
    let team_b = &data.projects[1];
    assert_eq!(team_a.project, "team-a");
    assert_eq!(team_b.project, "team-b");

    // team-a: only the dispatched/ready manager is live. The reserved workers
    // remain queued until dispatch and readiness evidence exists.
    let team_manager = team_a.manager.as_ref().expect("team-a manager");
    assert_eq!(team_manager.agent, manager);
    assert_eq!(team_manager.generation, 1);
    assert_eq!(team_a.worker_total, 2);
    assert_eq!(team_a.workers.len(), 2);
    assert!(
        team_a
            .workers
            .iter()
            .any(|worker| worker.agent == first_worker)
    );
    assert!(
        team_a
            .workers
            .iter()
            .any(|worker| worker.agent == second_worker)
    );
    assert_eq!(team_a.totals.live, 1);
    assert_eq!(team_a.totals.active_turns, 0);
    assert_eq!(team_a.totals.retained_unknown, 0);
    assert_eq!(team_a.totals.queued, 3);
    assert_eq!(team_a.totals.lifecycle.live, 1);
    assert_eq!(team_a.totals.lifecycle.active, 0);
    assert_eq!(team_a.totals.lifecycle.queued, 3);
    assert_eq!(team_a.totals.lifecycle.retained_unknown, 0);
    assert_eq!(team_a.queue, 3);
    assert_eq!(team_a.capacity.len(), 2);
    assert_eq!(team_a.capacity[0].scope, "project:team-a");
    assert_eq!(team_a.capacity[0].usage, 3);
    assert_eq!(team_a.capacity[0].limit, Some(2));
    assert_eq!(team_a.capacity[1].scope, "manager_assignment:team-a");
    assert_eq!(team_a.capacity[1].usage, 3);
    assert_eq!(team_a.capacity[1].limit, Some(3));

    // team-b: no live team left, but the retained unknown reservation stays
    // counted against its project scope.
    assert!(team_b.manager.is_none());
    assert_eq!(team_b.worker_total, 1);
    assert_eq!(team_b.workers.len(), 1);
    assert_eq!(team_b.totals.live, 0);
    assert_eq!(team_b.totals.retained_unknown, 1);
    assert_eq!(team_b.totals.lifecycle.retained_unknown, 1);
    assert_eq!(team_b.queue, 0);
    assert_eq!(team_b.capacity[0].usage, 1);
    assert_eq!(team_b.capacity[0].limit, Some(1));

    // Fleet totals match the team rows: every holder is counted exactly once,
    // and the observed native child is listed separately.
    assert_eq!(data.fleet.live_harnesses, 1);
    assert_eq!(data.fleet.live_by_harness.len(), 1);
    assert_eq!(data.fleet.live_by_harness[0].harness, "codex");
    assert_eq!(data.fleet.live_by_harness[0].total, 1);
    assert_eq!(data.fleet.active_turns, 0);
    assert_eq!(data.fleet.queued_work, 3);
    assert_eq!(data.fleet.retained_unknown, 1);
    assert_eq!(data.fleet.capacity.usage, 4);
    assert_eq!(data.fleet.native_child_total, 1);
    assert_eq!(data.fleet.native_children.len(), 1);
    assert_eq!(data.fleet.native_children[0].child, child);
    assert_eq!(
        data.fleet.native_children[0].project,
        Some("team-a".to_owned())
    );
    assert_eq!(
        data.fleet.live_harnesses,
        team_a.totals.live + team_b.totals.live
    );
    assert_eq!(
        data.fleet.retained_unknown,
        team_a.totals.retained_unknown + team_b.totals.retained_unknown
    );
    assert_eq!(
        data.fleet.queued_work,
        team_a.totals.queued + team_b.totals.queued
    );
    drop(data);
    drop(store);
    fs::remove_dir_all(&root).unwrap();
}
