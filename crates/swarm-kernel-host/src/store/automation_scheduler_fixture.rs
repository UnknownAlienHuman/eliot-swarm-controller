use super::{DueSourceDisposition, DueSourceOutcome, aggregate_source_disposition};

fn outcome(disposition: DueSourceDisposition) -> DueSourceOutcome {
    DueSourceOutcome {
        kind: "fixture_source",
        disposition,
        cursor_advanced: disposition == DueSourceDisposition::Progressed,
        damaged_subjects: Vec::new(),
        pending_subjects: Vec::new(),
    }
}

#[test]
fn invocation_without_retained_cursor_progress_is_idle() {
    assert_eq!(
        DueSourceDisposition::observe(true, false, false),
        DueSourceDisposition::Idle
    );
    assert_eq!(
        DueSourceDisposition::observe(true, true, false),
        DueSourceDisposition::Progressed
    );
}

#[test]
fn aggregate_degradation_precedes_progress_and_idle() {
    assert_eq!(
        aggregate_source_disposition(&[
            outcome(DueSourceDisposition::Idle),
            outcome(DueSourceDisposition::Progressed),
            outcome(DueSourceDisposition::Degraded),
        ]),
        DueSourceDisposition::Degraded
    );
    assert_eq!(
        aggregate_source_disposition(&[
            outcome(DueSourceDisposition::Idle),
            outcome(DueSourceDisposition::Progressed),
        ]),
        DueSourceDisposition::Progressed
    );
}

#[test]
fn first_retained_schedule_cursor_counts_as_progress() {
    let after = vec![("schedule-a".to_owned(), Some(40), None, None)];
    assert!(super::schedule_cursor_advanced(&[], &after));
    assert!(!super::schedule_cursor_advanced(&after, &after));
}

#[test]
fn schedule_capacity_is_pending_but_check_and_unknown_errors_stay_unclassified() {
    use super::super::automation_reconcile::SubjectErrorDisposition;

    let classified = super::super::schedules::classify_scheduler_error(&crate::error::Error::new(
        "SCHEDULE_REGISTRY_FULL",
        "bounded fixture",
    ));
    match classified {
        Some(SubjectErrorDisposition::Pending { code, .. }) => {
            assert_eq!(code, "SCHEDULE_REGISTRY_FULL");
        }
        _ => panic!("schedule capacity must remain pending"),
    }

    for error in [
        crate::error::Error::new(
            "CHECK_LAUNCH_RECEIPT_INVALID",
            "retained CheckRun evidence is invalid",
        ),
        crate::error::Error::new("STORE_ERROR", "must remain fatal"),
    ] {
        assert!(
            super::super::schedules::classify_scheduler_error(&error).is_none(),
            "{} must not be converted into an interval-schedule pending result",
            error.code
        );
    }

    let wrapped_infrastructure = crate::error::Error::new(
        "CHECK_LAUNCH_RECEIPT_INVALID",
        "receipt failure with a secondary storage error",
    )
    .with_secondary_code("STORE_ERROR");
    let retained = super::check_recovery_failure(wrapped_infrastructure).unwrap_err();
    assert_eq!(retained.code, "CHECK_LAUNCH_RECEIPT_INVALID");
    assert_eq!(retained.secondary_codes, vec!["STORE_ERROR"]);
    let schedule_infrastructure = crate::error::Error::new(
        "SCHEDULE_REGISTRY_FULL",
        "registry capacity with a storage failure",
    )
    .with_secondary_code("STORE_ERROR");
    assert!(super::super::schedules::classify_scheduler_error(&schedule_infrastructure).is_none());
}

#[test]
fn malformed_check_recovery_receipts_are_retained_bounded_and_unknowns_stay_fatal() {
    let error = crate::error::Error::new(
        "CHECK_LAUNCH_RECEIPT_INVALID",
        "x".repeat(super::MAX_CHECK_RECOVERY_ERROR_MESSAGE_CHARS + 1),
    );
    let check_recovery = super::check_recovery_failure(error).unwrap();

    assert_eq!(check_recovery["disposition"], "degraded");
    assert_eq!(
        check_recovery["pending_error"]["code"],
        "CHECK_LAUNCH_RECEIPT_INVALID"
    );
    assert_eq!(check_recovery["pending_error"]["message_truncated"], true);
    assert_eq!(
        check_recovery["pending_error"]["message"]
            .as_str()
            .unwrap()
            .chars()
            .count(),
        super::MAX_CHECK_RECOVERY_ERROR_MESSAGE_CHARS
    );
    assert_eq!(
        check_recovery["pending_error"]["error_digest"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
    assert_eq!(
        super::aggregate_scheduler_disposition(
            &[outcome(DueSourceDisposition::Progressed)],
            &check_recovery
        ),
        DueSourceDisposition::Degraded,
        "a retained CheckRun failure must remain visible in the aggregate"
    );

    for error in [
        crate::error::Error::new(
            "CHECK_LAUNCH_IDENTITY_UNKNOWN",
            "process identity remains uncertain",
        ),
        crate::error::Error::new("STORE_ERROR", "storage failure remains fatal"),
    ] {
        assert_eq!(
            super::check_recovery_failure(error.clone())
                .unwrap_err()
                .code,
            error.code
        );
    }
}

#[test]
fn pending_source_outcome_exposes_exact_subject_evidence_without_degrading() {
    use super::super::automation_reconcile::QuarantineEvidence;
    use super::DueSourceEvidence;

    let outcome = DueSourceOutcome {
        kind: "interval_schedule",
        disposition: DueSourceDisposition::Idle,
        cursor_advanced: false,
        damaged_subjects: Vec::new(),
        pending_subjects: vec![DueSourceEvidence {
            kind: "interval_schedule",
            code: "SCHEDULE_REGISTRY_FULL".to_owned(),
            evidence: QuarantineEvidence {
                subject_identity: "schedule_id:interval-a".to_owned(),
                source_pointer: Some("config/schedules/interval-a".to_owned()),
                source_digest: Some("a".repeat(64)),
            },
            source_key: None,
            source_raw: None,
        }],
    }
    .value();

    assert_eq!(outcome["disposition"], "idle");
    assert_eq!(outcome["cursor_advanced"], false);
    assert_eq!(
        outcome["pending_subjects"][0]["code"],
        "SCHEDULE_REGISTRY_FULL"
    );
    assert_eq!(
        outcome["pending_subjects"][0]["subject_identity"],
        "schedule_id:interval-a"
    );
    assert_eq!(
        outcome["pending_subjects"][0]["source_pointer"],
        "config/schedules/interval-a"
    );
    assert_eq!(
        outcome["pending_subjects"][0]["source_digest"]
            .as_str()
            .unwrap()
            .len(),
        64
    );
}

fn scheduler_owner_fixture() -> rusqlite::Connection {
    use super::super::{host_lifecycle, set_meta};
    use serde_json::json;

    let mut db = rusqlite::Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY, value_json TEXT NOT NULL);")
        .unwrap();
    set_meta(&db, "controller_id", &json!("scheduler-fixture-host")).unwrap();
    set_meta(&db, "host_epoch", &json!(3)).unwrap();
    let tx = db.transaction().unwrap();
    host_lifecycle::start(&tx, 100).unwrap();
    host_lifecycle::ready(&tx, 101).unwrap();
    host_lifecycle::update_optional_worker_with_child(
        &tx,
        "automation-scheduler",
        "running",
        0,
        None,
        None,
        None,
        102,
    )
    .unwrap();
    let scope = super::provision(
        &tx,
        &crate::model::Credential {
            client_id: super::SERVICE_ID.to_owned(),
            token: "scheduler-fixture-private-credential".to_owned(),
        },
        None,
        &"a".repeat(64),
    )
    .unwrap();
    super::begin_owner(
        &tx,
        &scope,
        "fixture-launch-3",
        "unused-fixture-owner-token",
    )
    .unwrap();
    tx.commit().unwrap();
    db
}

fn scheduler_fixture_group_identity() -> serde_json::Value {
    if cfg!(target_os = "linux") {
        serde_json::json!({
            "pid":42,
            "pgid":42,
            "start_ticks":"123",
            "boot_id":"aabbccdd-0011-2233-4455-66778899aabb",
            "scope":"linux_process_group",
            "purpose":"automation_scheduler",
            "disposition_source":"proc_group_members",
        })
    } else {
        serde_json::json!({
            "pid":42,
            "creation_filetime":123,
            "scope":"windows_job",
            "purpose":"automation_scheduler",
            "disposition_source":"job_accounting",
        })
    }
}

#[test]
fn enabled_scheduler_demand_survives_exhausted_source_page() {
    let db = scheduler_owner_fixture();
    let mut config = crate::config::Config::default();
    config.automation_scheduler.enabled = true;
    let scope = super::current_scope(&db).unwrap();
    let page = super::project_due_page(&db, &config, scope).unwrap();
    assert!(page.next_due_at_ms.is_none());
    assert!(!page.is_due());
    assert!(super::has_demand(&config));

    config.automation_scheduler.enabled = false;
    assert!(!super::has_demand(&config));
}

#[test]
fn scheduler_owner_group_identity_shared_shape_is_closed_and_pure() {
    use serde_json::{Value, json};
    use swarm_automation::service_owner_group_identity_shape;

    // Deliberately synthetic process facts: this predicate must need neither
    // an owner token nor a live OS process to validate the data contract.
    let identity = scheduler_fixture_group_identity();
    assert!(service_owner_group_identity_shape(&identity));
    for value in [Value::Null, json!([]), json!({}), json!("private-secret")] {
        assert!(!service_owner_group_identity_shape(&value));
    }
    for key in identity.as_object().unwrap().keys() {
        let mut damaged = identity.clone();
        damaged.as_object_mut().unwrap().remove(key);
        assert!(
            !service_owner_group_identity_shape(&damaged),
            "missing {key}"
        );
    }
    for (key, value) in [
        ("pid", json!(0)),
        ("pid", json!("42")),
        ("purpose", json!("check")),
        ("scope", json!("launcher_spawned_process")),
        ("disposition_source", json!("private-secret")),
        ("owner_token", json!("private-secret")),
    ] {
        let mut damaged = identity.clone();
        damaged[key] = value;
        assert!(
            !service_owner_group_identity_shape(&damaged),
            "invalid {key}"
        );
    }
    if cfg!(target_os = "linux") {
        for (key, value) in [
            ("pgid", json!(43)),
            ("start_ticks", json!("")),
            ("start_ticks", json!("12x")),
            ("start_ticks", json!(123)),
            ("boot_id", json!("")),
            ("boot_id", json!("g")),
            ("boot_id", json!("a".repeat(65))),
        ] {
            let mut damaged = identity.clone();
            damaged[key] = value;
            assert!(
                !service_owner_group_identity_shape(&damaged),
                "invalid {key}"
            );
        }
    } else {
        let mut damaged = identity;
        damaged["creation_filetime"] = json!(0);
        assert!(!service_owner_group_identity_shape(&damaged));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn scheduler_owner_status_retains_stronger_start_ticks_projection_bound() {
    use super::super::{meta, set_meta};
    use serde_json::json;

    let db = scheduler_owner_fixture();
    scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Active);
    let mut owner = meta(&db, super::OWNER_KEY).unwrap().unwrap();
    for length in [20, 21] {
        owner["group_identity"]["start_ticks"] = json!("1".repeat(length));
        assert!(swarm_automation::service_owner_group_identity_shape(
            &owner["group_identity"]
        ));
        set_meta(&db, super::OWNER_KEY, &owner).unwrap();
        let before = scheduler_fixture_meta_rows(&db);
        let observation = scheduler_owner_observation(&db);
        assert_eq!(observation["ready"], length == 20);
        assert_eq!(
            observation["status"],
            if length == 20 { "current" } else { "invalid" }
        );
        assert_eq!(scheduler_fixture_meta_rows(&db), before);
    }
}

fn scheduler_fixture_set_phase(db: &rusqlite::Connection, phase: super::SchedulerOwnerPhase) {
    let mut owner = super::owner_record(db).unwrap().unwrap();
    owner.phase = phase;
    owner.group_identity = Some(scheduler_fixture_group_identity());
    let tx = db.unchecked_transaction().unwrap();
    super::save_owner(&tx, &owner).unwrap();
    tx.commit().unwrap();
}

fn scheduler_owner_observation(db: &rusqlite::Connection) -> serde_json::Value {
    super::super::host_lifecycle::status(db).unwrap()["optional_workers"]["automation-scheduler"]
        ["owner"]
        .clone()
}

fn scheduler_fixture_meta_rows(db: &rusqlite::Connection) -> Vec<(String, String)> {
    db.prepare("SELECT key,value_json FROM meta ORDER BY key")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

#[test]
fn scheduler_owner_status_launching_is_not_ready_despite_running_marker() {
    let db = scheduler_owner_fixture();
    let lifecycle = super::super::host_lifecycle::status(&db).unwrap();
    let worker = &lifecycle["optional_workers"]["automation-scheduler"];
    assert_eq!(worker["state"], "running");
    assert_eq!(worker["owner"]["status"], "current");
    assert_eq!(worker["owner"]["phase"], "launching");
    assert_eq!(worker["owner"]["ready"], false);
    assert!(worker["owner"]["group_identity"].is_null());
}

#[test]
fn scheduler_owner_status_active_exact_current_scope_is_ready_and_read_only() {
    let db = scheduler_owner_fixture();
    scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Active);
    let before = scheduler_fixture_meta_rows(&db);
    let owner = scheduler_owner_observation(&db);
    assert_eq!(owner["status"], "current");
    assert_eq!(owner["phase"], "active");
    assert_eq!(owner["ready"], true);
    assert_eq!(
        owner["scope"],
        serde_json::json!(super::current_scope(&db).unwrap())
    );
    assert_eq!(owner["scope"]["generation"], 1);
    assert_eq!(
        owner["host_scope"]["controller_id"],
        "scheduler-fixture-host"
    );
    assert_eq!(owner["host_scope"]["host_epoch"], 3);
    assert_eq!(owner["launch_id"], "fixture-launch-3");
    assert_eq!(owner["group_identity"], scheduler_fixture_group_identity());
    assert_eq!(owner["readiness_source"], "validated_current_owner_receipt");
    let public_json = owner.to_string();
    for private in [
        "scheduler-fixture-private-credential".to_owned(),
        crate::model::digest(b"scheduler-fixture-private-credential"),
        "a".repeat(64),
        "config_sha256".to_owned(),
        "token_hash".to_owned(),
        "unused-fixture-owner-token".to_owned(),
    ] {
        assert!(!public_json.contains(&private));
    }
    assert_eq!(scheduler_fixture_meta_rows(&db), before);
}

#[test]
fn scheduler_owner_status_departed_retains_identity_without_readiness() {
    let db = scheduler_owner_fixture();
    scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Departed);
    let owner = scheduler_owner_observation(&db);
    assert_eq!(owner["status"], "current");
    assert_eq!(owner["phase"], "departed");
    assert_eq!(owner["launch_id"], "fixture-launch-3");
    assert_eq!(owner["ready"], false);
}

#[test]
fn scheduler_owner_status_stale_generation_or_host_scope_cannot_be_ready() {
    use super::super::{meta, set_meta};
    use serde_json::json;

    for changed_identity in ["generation", "host_epoch", "controller_id"] {
        let db = scheduler_owner_fixture();
        scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Active);
        match changed_identity {
            "generation" => {
                let key = format!("client:{}", super::SERVICE_ID);
                let mut client = meta(&db, &key).unwrap().unwrap();
                client["automation_scheduler"]["scope"]["generation"] = json!(2);
                set_meta(&db, &key, &client).unwrap();
                set_meta(&db, super::GENERATION_KEY, &json!(2)).unwrap();
            }
            "host_epoch" => set_meta(&db, "host_epoch", &json!(4)).unwrap(),
            _ => set_meta(&db, "controller_id", &json!("another-host")).unwrap(),
        }
        let before = scheduler_fixture_meta_rows(&db);
        let owner = scheduler_owner_observation(&db);
        assert_eq!(owner["status"], "stale", "{changed_identity}");
        assert_eq!(owner["phase"], "active");
        assert_eq!(owner["scope"]["generation"], 1);
        assert_eq!(owner["host_scope"]["host_epoch"], 3);
        assert_eq!(
            owner["host_scope"]["controller_id"],
            "scheduler-fixture-host"
        );
        assert_eq!(owner["ready"], false);
        assert_eq!(scheduler_fixture_meta_rows(&db), before);
    }
}

#[test]
fn scheduler_owner_status_missing_or_damaged_owner_is_bounded_and_not_ready() {
    use super::super::{meta, set_meta};
    use serde_json::json;

    let db = scheduler_owner_fixture();
    scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Active);
    let valid = meta(&db, super::OWNER_KEY).unwrap().unwrap();
    db.execute("DELETE FROM meta WHERE key=?1", [super::OWNER_KEY])
        .unwrap();
    assert_eq!(scheduler_owner_observation(&db)["status"], "missing");
    assert_eq!(scheduler_owner_observation(&db)["ready"], false);

    let mut damaged = Vec::new();
    for (field, value) in [
        ("schema_version", json!(2)),
        ("launch_id", json!("private-secret/invalid")),
        ("phase", json!("private-secret")),
        ("group_identity", serde_json::Value::Null),
        ("private-secret", json!("private-secret")),
    ] {
        let mut owner = valid.clone();
        owner[field] = value;
        damaged.push(owner);
    }
    let mut owner = valid.clone();
    owner["group_identity"]["owner_token"] = json!("private-secret");
    damaged.push(owner);
    let mut owner = valid.clone();
    owner["scope"]["generation"] = json!(0);
    damaged.push(owner);
    let mut owner = valid.clone();
    owner["phase"] = json!("launching");
    damaged.push(owner);
    let mut owner = valid.clone();
    owner["host_scope"]["host_epoch"] = json!(0);
    damaged.push(owner);
    for value in damaged {
        set_meta(&db, super::OWNER_KEY, &value).unwrap();
        let before = scheduler_fixture_meta_rows(&db);
        let observation = scheduler_owner_observation(&db);
        assert_eq!(observation["status"], "invalid");
        assert_eq!(observation["ready"], false);
        assert!(!observation.to_string().contains("private-secret"));
        assert_eq!(scheduler_fixture_meta_rows(&db), before);
    }
    db.execute(
        "UPDATE meta SET value_json=?1 WHERE key=?2",
        rusqlite::params!["{\"private-secret\":", super::OWNER_KEY],
    )
    .unwrap();
    let before = scheduler_fixture_meta_rows(&db);
    assert_eq!(scheduler_owner_observation(&db)["status"], "invalid");
    assert_eq!(scheduler_fixture_meta_rows(&db), before);
}

#[test]
fn scheduler_owner_status_missing_disabled_or_damaged_registration_cannot_be_ready() {
    use super::super::{meta, set_meta};
    use serde_json::json;

    for damaged_field in [
        "missing",
        "disabled",
        "role",
        "token_hash",
        "method_scope",
        "generation",
    ] {
        let db = scheduler_owner_fixture();
        scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Active);
        let key = format!("client:{}", super::SERVICE_ID);
        let mut client = meta(&db, &key).unwrap().unwrap();
        match damaged_field {
            "missing" => {
                db.execute("DELETE FROM meta WHERE key=?1", [&key]).unwrap();
            }
            "method_scope" => {
                client["automation_scheduler"]["method_scope"] = json!(["host.status"]);
                set_meta(&db, &key, &client).unwrap();
            }
            "generation" => set_meta(&db, super::GENERATION_KEY, &json!(2)).unwrap(),
            field => {
                client[field] = match field {
                    "disabled" => json!(true),
                    "role" => json!("manager"),
                    _ => json!("private-secret"),
                };
                set_meta(&db, &key, &client).unwrap();
            }
        }
        let owner = scheduler_owner_observation(&db);
        assert_eq!(owner["status"], "scope_unavailable", "{damaged_field}");
        assert_eq!(owner["phase"], "active");
        assert_eq!(owner["ready"], false);
        assert!(!owner.to_string().contains("private-secret"));
    }
}

#[test]
fn scheduler_owner_status_old_receipt_without_host_scope_cannot_be_ready() {
    let db = scheduler_owner_fixture();
    scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Active);
    let mut owner = super::owner_record(&db).unwrap().unwrap();
    owner.host_scope = None;
    let tx = db.unchecked_transaction().unwrap();
    super::save_owner(&tx, &owner).unwrap();
    tx.commit().unwrap();
    let owner = scheduler_owner_observation(&db);
    assert_eq!(owner["status"], "host_scope_unavailable");
    assert_eq!(owner["phase"], "active");
    assert_eq!(owner["ready"], false);
}

#[test]
fn scheduler_owner_status_requires_current_running_host_and_supervisor_marker() {
    use super::super::{meta, set_meta};
    use serde_json::json;

    for not_ready in [
        "starting",
        "stopped",
        "failed",
        "wrong_epoch",
        "no_supervisor_marker",
        "missing_host_epoch",
        "damaged_controller_id",
        "invalid_current",
    ] {
        let db = scheduler_owner_fixture();
        scheduler_fixture_set_phase(&db, super::SchedulerOwnerPhase::Active);
        if not_ready == "no_supervisor_marker" {
            db.execute("DELETE FROM meta WHERE key='host:optional-workers:v1'", [])
                .unwrap();
        } else if not_ready == "missing_host_epoch" {
            db.execute("DELETE FROM meta WHERE key='host_epoch'", [])
                .unwrap();
        } else if not_ready == "damaged_controller_id" {
            set_meta(&db, "controller_id", &json!("private-secret/invalid\n")).unwrap();
        } else {
            let mut current = meta(&db, "host:lifecycle:v1").unwrap().unwrap();
            if not_ready == "wrong_epoch" {
                current["host_epoch"] = json!(4);
            } else if not_ready == "invalid_current" {
                current["schema_version"] = json!(2);
            } else {
                current["state"] = json!(not_ready);
            }
            set_meta(&db, "host:lifecycle:v1", &current).unwrap();
        }
        let lifecycle = super::super::host_lifecycle::status(&db).unwrap();
        let worker = &lifecycle["optional_workers"]["automation-scheduler"];
        assert_eq!(worker["owner"]["phase"], "active");
        assert_eq!(worker["owner"]["ready"], false, "{not_ready}");
        assert!(!worker["owner"].to_string().contains("private-secret"));
        if not_ready == "no_supervisor_marker" {
            assert!(worker.get("state").is_none());
        }
    }
}
