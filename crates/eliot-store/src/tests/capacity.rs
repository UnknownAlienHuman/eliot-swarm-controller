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
    store.connection.execute_batch("DROP INDEX IF EXISTS commands_prune_roots; DROP INDEX IF EXISTS completion_digests_pending_cursor; DROP INDEX IF EXISTS agents_list_archived_state; DROP INDEX IF EXISTS workspaces_start_command; DELETE FROM schema_migrations WHERE version=58; ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 ALTER TABLE turns DROP COLUMN effective_model;
                 ALTER TABLE turns DROP COLUMN native_error_message;
                 DELETE FROM schema_migrations WHERE version=58;
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
            "DROP INDEX IF EXISTS commands_prune_roots; DROP INDEX IF EXISTS completion_digests_pending_cursor; DROP INDEX IF EXISTS agents_list_archived_state; DROP INDEX IF EXISTS workspaces_start_command; DELETE FROM schema_migrations WHERE version=58; ALTER TABLE project_assignments DROP COLUMN workspace_adopted;
                 ALTER TABLE project_assignments DROP COLUMN workspace_path;
                 ALTER TABLE turns DROP COLUMN effective_model;
                 ALTER TABLE turns DROP COLUMN native_error_message;
                 DELETE FROM schema_migrations WHERE version=58;
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
                 state TEXT NOT NULL, project TEXT NOT NULL, harness TEXT NOT NULL
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
