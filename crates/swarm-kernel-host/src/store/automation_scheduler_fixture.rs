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

#[tokio::test]
async fn cron_quarantine_reaches_store_outcome_while_goal_reminder_progresses() {
    use crate::{
        checks::model::{CheckProfile, Parser},
        config::Config,
        model::{self, Credential},
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, sync::Arc};

    const PROJECT_ID: &str = "scheduler-propagation-fixture";
    const CRON_ID: &str = "quarantined-calendar";
    const GOAL_ID: &str = "due-reminder";

    let directory = std::env::temp_dir().join(format!("scheduler-propagation-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let directory = root.path.clone();
    let operator_credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.automation_scheduler.enabled = true;
    config.checks.enabled = true;
    config.checks.profiles = vec![CheckProfile {
        profile_id: "strict".into(),
        profile_revision: "v1".into(),
        executable: std::env::current_exe().unwrap(),
        args: Vec::new(),
        parser: Parser::ExitCode,
        resource: "scheduler-propagation-checks".into(),
        environment: BTreeMap::new(),
        inherit_env: Vec::new(),
        expected_targets: Vec::new(),
        reproducible: false,
        fingerprint_env: Vec::new(),
        versioned_inputs: BTreeMap::new(),
    }];
    let owner = StoreOwner::start(root, Arc::new(config), operator_credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(operator_credential).await.unwrap();

    let manager_id = "scheduler-propagation-manager";
    let manager_token = format!("scheduler-propagation-{}", model::new_id());
    scheduler_fixture_call(
        &owner.store,
        &operator,
        "client.register",
        json!({
            "client_id":manager_id,
            "role":"manager",
            "token_hash":model::digest(manager_token.as_bytes()),
        }),
    )
    .await;
    let manager = owner
        .store
        .authenticate(Credential {
            client_id: manager_id.to_owned(),
            token: manager_token,
        })
        .await
        .unwrap();
    scheduler_fixture_call(
        &owner.store,
        &operator,
        "gm.handover",
        json!({"client_id":manager_id}),
    )
    .await;

    let task = scheduler_fixture_call(
        &owner.store,
        &operator,
        "task.create",
        json!({
            "project_id":PROJECT_ID,
            "spec":{
                "objective":"Exercise independent scheduler source progress",
                "phase":"verification",
                "owner_policy_id":"owner-policy-v1",
                "requirements":[{"id":"R1","statement":"Retain the exact scheduled source"}],
            }
        }),
    )
    .await;
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let attempt = scheduler_fixture_call(
        &owner.store,
        &manager,
        "task.claim",
        json!({"task_id":task_id,"expected_revision":1}),
    )
    .await;
    let attempt_id = attempt["attempt_id"].as_str().unwrap().to_owned();
    let candidate_ref = format!("source-{}", model::digest(task_id.as_bytes()));

    let applied = scheduler_fixture_call(
        &owner.store,
        &manager,
        "automation.config.apply",
        json!({
            "project_id":PROJECT_ID,
            "changes":[{
                "automation_id":CRON_ID,
                "expected_revision":0,
                "include_existing":true,
                "patch":{
                    "enabled":true,
                    "steps":["check_run"],
                    "cron":{
                        "calendar":{"expression":"0 0 0 1 1 *","timezone":"UTC","anchor_ms":0},
                        "action":{
                            "kind":"check_run",
                            "attempt_id":attempt_id,
                            "expected_task_revision":1,
                            "candidate_ref":candidate_ref,
                            "profile_id":"strict",
                            "profile_revision":"v1",
                        },
                    },
                },
            }],
        }),
    )
    .await;
    assert_eq!(applied["applied"], true);

    let future_reminder_at_ms = model::now_ms().unwrap().saturating_add(60_000);
    let created_goal = scheduler_fixture_call(
        &owner.store,
        &manager,
        "goal.create",
        json!({
            "project_id":PROJECT_ID,
            "task_id":task_id,
            "task_revision":1,
            "attempt_id":attempt_id,
            "goal_id":GOAL_ID,
            "expected_revision":0,
            "objective":"Advance the independent reminder source",
            "completion_evidence":{"kind":"task_acceptance"},
            "reminder":{"due_at_ms":future_reminder_at_ms,"cooldown_ms":0},
            "enabled":true,
        }),
    )
    .await;
    assert_eq!(created_goal["created"], true);

    // Move the valid reminder into the due window while keeping its record,
    // due-index key, and slot identity mutually consistent.
    let goal_record_key = format!(
        "goals:v1:record:{}",
        model::digest(
            model::canonical(&json!([PROJECT_ID, task_id, GOAL_ID]))
                .unwrap()
                .as_bytes()
        )
    );
    let goal_record_id = goal_record_key
        .strip_prefix("goals:v1:record:")
        .unwrap()
        .to_owned();
    let old_goal_due_key = format!("goals:v1:due:{future_reminder_at_ms:019}:{goal_record_id}");
    let goal_due_at_ms = 1_i64;
    let goal_due_key = format!("goals:v1:due:{goal_due_at_ms:019}:{goal_record_id}");
    let goal_slot_id = model::digest(
        model::canonical(&json!([PROJECT_ID, task_id, GOAL_ID, goal_due_at_ms]))
            .unwrap()
            .as_bytes(),
    );
    let record_key_for_db = goal_record_key.clone();
    let old_goal_due_key_for_db = old_goal_due_key.clone();
    let goal_due_key_for_db = goal_due_key.clone();
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let raw: String = tx.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [&record_key_for_db],
                |row| row.get(0),
            )?;
            let mut record: Value = serde_json::from_str(&raw)?;
            record["reminder"]["due_at_ms"] = json!(goal_due_at_ms);
            tx.execute("DELETE FROM meta WHERE key=?1", [&old_goal_due_key_for_db])?;
            super::super::set_meta(&tx, &record_key_for_db, &record)?;
            super::super::set_meta(
                &tx,
                &goal_due_key_for_db,
                &json!({
                    "schema_version":1,
                    "goal_id":GOAL_ID,
                    "record_key":record_key_for_db,
                    "due_at_ms":goal_due_at_ms,
                    "slot_id":goal_slot_id,
                }),
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    // The due index remains structurally valid. Its exact committed entry is
    // semantically damaged, so classification must cover entry decoding too.
    let damaged_entry_key =
        crate::automation::config::entry_key(manager_id, PROJECT_ID, CRON_ID).unwrap();
    owner
        .store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [&damaged_entry_key],
                |row| row.get(0),
            )?;
            let mut entry: Value = serde_json::from_str(&raw)?;
            entry["schema_version"] = json!(0);
            super::super::set_meta(db, &damaged_entry_key, &entry)
        })
        .await
        .unwrap();

    let cron_prefix = super::CRON_DUE_PREFIX.to_owned();
    let cron_upper = format!("{cron_prefix}~");
    let (cron_index_key, cron_index_raw) = owner
        .store
        .run(move |db| {
            db.query_row(
                "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT 1",
                rusqlite::params![cron_prefix, cron_upper],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    let key_digest = model::digest(cron_index_key.as_bytes());
    let expected_subject_identity = format!("cron-subject:meta-key-sha256:{key_digest}");
    let expected_source_pointer = format!("meta/key-sha256:{key_digest}");
    let expected_source_digest = model::digest(cron_index_raw.as_bytes());

    let worker_config = swarm_automation::read_worker_config_file(
        &owner.store.automation_scheduler_worker_config_path(),
    )
    .unwrap();
    let (worker_scope, worker_launch_id) = owner
        .store
        .begin_automation_scheduler_worker()
        .await
        .unwrap();
    let mut scheduler_owner = SchedulerFixtureChild::spawn(
        &owner.store.automation_scheduler_worker_config_path(),
        &directory.join("scheduler-owner-ready"),
    );
    let process_identity = swarm_process::spawned_identity(scheduler_owner.0.id()).unwrap();
    let group_identity = swarm_automation::service_owner_group_identity(
        &process_identity,
        &worker_config.service_owner_token,
    )
    .unwrap();
    owner
        .store
        .activate_automation_scheduler_worker(
            worker_scope.clone(),
            worker_launch_id.clone(),
            group_identity.clone(),
        )
        .await
        .unwrap();
    let scheduler = owner
        .store
        .authenticate(worker_config.credential.clone())
        .await
        .unwrap();
    let page = owner
        .store
        .call(
            scheduler.clone(),
            "automation.scheduler.page".into(),
            json!({"scope":worker_config.scope}),
        )
        .await
        .unwrap();
    let source_pages = page["sources"].as_array().unwrap();
    let calendar_page = source_pages
        .iter()
        .find(|source| source["kind"] == "manager_calendar")
        .unwrap();
    let goal_page = source_pages
        .iter()
        .find(|source| source["kind"] == "goal_reminder")
        .unwrap();
    assert_eq!(calendar_page["due_count"], 1);
    assert_eq!(goal_page["due_count"], 1);

    let scope = page["scope"].clone();
    let expected_snapshot_sha256 = page["snapshot_sha256"].as_str().unwrap().to_owned();
    let observed_at_ms = page["observed_at_ms"].as_i64().unwrap();
    let client_request_id = format!(
        "automation-pulse-{}",
        super::digest_json(&json!({
            "schema_version":1,
            "scope":scope,
            "snapshot_sha256":expected_snapshot_sha256,
        }))
        .unwrap()
    );
    let result = owner
        .store
        .call(
            scheduler,
            "automation.scheduler.admit".into(),
            json!({
                "client_request_id":client_request_id,
                "scope":scope,
                "expected_snapshot_sha256":expected_snapshot_sha256,
                "observed_at_ms":observed_at_ms,
            }),
        )
        .await
        .unwrap();

    assert_eq!(result["disposition"], "reconcilers_completed");
    assert_eq!(result["source_disposition"], "degraded");
    let source_outcomes = result["source_outcomes"].as_array().unwrap();
    let calendar_outcome = source_outcomes
        .iter()
        .find(|source| source["kind"] == "manager_calendar")
        .unwrap();
    assert_eq!(calendar_outcome["disposition"], "degraded");
    assert_eq!(calendar_outcome["cursor_advanced"], true);
    assert_eq!(
        calendar_outcome["damaged_subjects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        calendar_outcome["damaged_subjects"][0]["code"],
        "AUTOMATION_RECORD_VERSION"
    );
    assert_eq!(
        calendar_outcome["damaged_subjects"][0]["subject_identity"],
        expected_subject_identity
    );
    assert_eq!(
        calendar_outcome["damaged_subjects"][0]["source_pointer"],
        expected_source_pointer
    );
    assert_eq!(
        calendar_outcome["damaged_subjects"][0]["source_digest"],
        expected_source_digest
    );
    let goal_outcome = source_outcomes
        .iter()
        .find(|source| source["kind"] == "goal_reminder")
        .unwrap();
    assert_eq!(goal_outcome["disposition"], "progressed");
    assert_eq!(goal_outcome["cursor_advanced"], true);

    let quarantine_prefix = "automation:v1:cron:quarantine:".to_owned();
    let quarantine_upper = format!("{quarantine_prefix}~");
    let cron_index_key_for_db = cron_index_key.clone();
    let goal_due_key_for_check = goal_due_key.clone();
    let (quarantine_records, cron_index_retained, goal_index_retained) = owner
        .store
        .run(move |db| {
            let mut statement =
                db.prepare("SELECT value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key")?;
            let quarantine_records = statement
                .query_map(
                    rusqlite::params![quarantine_prefix, quarantine_upper],
                    |row| row.get::<_, String>(0),
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let cron_index_retained: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
                [&cron_index_key_for_db],
                |row| row.get(0),
            )?;
            let goal_index_retained: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
                [&goal_due_key_for_check],
                |row| row.get(0),
            )?;
            Ok((quarantine_records, cron_index_retained, goal_index_retained))
        })
        .await
        .unwrap();
    assert_eq!(quarantine_records.len(), 1);
    assert!(!cron_index_retained);
    assert!(!goal_index_retained);
    let quarantine = crate::automation::config::open_record(
        serde_json::from_str(&quarantine_records[0]).unwrap(),
        "automation quarantine fixture",
    )
    .unwrap();
    assert_eq!(quarantine["code"], "AUTOMATION_RECORD_VERSION");
    assert_eq!(
        quarantine["evidence"]["subject_identity"],
        expected_subject_identity
    );
    assert_eq!(
        quarantine["evidence"]["source_pointer"],
        expected_source_pointer
    );
    assert_eq!(
        quarantine["evidence"]["source_digest"],
        expected_source_digest
    );

    scheduler_owner.0.kill().unwrap();
    let direct_child_exit = scheduler_owner.0.wait().unwrap();
    let owner_pid = group_identity["pid"].clone();
    #[cfg(windows)]
    let retained_job_empty = scheduler_owner
        .1
        .as_ref()
        .map(swarm_process::Group::family_empty);
    let finish = owner
        .store
        .finish_automation_scheduler_worker(worker_scope, worker_launch_id, group_identity)
        .await;
    if let Err(error) = finish {
        #[cfg(windows)]
        panic!(
            "scheduler fixture owner finish failed after direct child wait: {error:?}; direct_child_exit={direct_child_exit:?}; owner_pid={owner_pid}; retained_job_empty={retained_job_empty:?}"
        );
        #[cfg(not(windows))]
        panic!(
            "scheduler fixture owner finish failed after direct child wait: {error:?}; direct_child_exit={direct_child_exit:?}; owner_pid={owner_pid}"
        );
    }
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

struct SchedulerFixtureChild(
    std::process::Child,
    #[cfg(windows)] Option<swarm_process::Group>,
);

impl SchedulerFixtureChild {
    fn spawn(config_path: &std::path::Path, ready_path: &std::path::Path) -> Self {
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "store::automation_scheduler::fixture::scheduler_fixture_owner_child",
            ])
            .env("SWARM_SCHEDULER_FIXTURE_CONFIG", config_path)
            .env("SWARM_SCHEDULER_FIXTURE_READY", ready_path)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        #[cfg(windows)]
        let mut owner = Self(child, None);
        #[cfg(not(windows))]
        let mut owner = Self(child);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !ready_path.exists() {
            assert!(
                owner.0.try_wait().unwrap().is_none(),
                "fixture owner exited"
            );
            assert!(
                std::time::Instant::now() < deadline,
                "fixture owner not ready"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        #[cfg(windows)]
        {
            let config = swarm_automation::read_worker_config_file(config_path).unwrap();
            let launched = swarm_process::spawned_identity(owner.0.id()).unwrap();
            let identity = swarm_automation::service_owner_group_identity(
                &launched,
                &config.service_owner_token,
            )
            .unwrap();
            owner.1 = Some(
                swarm_automation::retain_service_owner_family(
                    &identity,
                    &config.service_owner_token,
                )
                .expect(
                    "retain the ready fixture service's exact Job through child wait and finish",
                ),
            );
        }
        owner
    }
}

impl Drop for SchedulerFixtureChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn scheduler_fixture_owner_child() {
    let Some(config_path) = std::env::var_os("SWARM_SCHEDULER_FIXTURE_CONFIG") else {
        return;
    };
    let config =
        swarm_automation::read_worker_config_file(std::path::Path::new(&config_path)).unwrap();
    let _group =
        swarm_process::Group::enter_service(&config.service_owner_token, config.scope.purpose)
            .unwrap();
    let ready_path = std::env::var_os("SWARM_SCHEDULER_FIXTURE_READY").unwrap();
    std::fs::write(std::path::Path::new(&ready_path), b"ready").unwrap();
    std::thread::sleep(std::time::Duration::from_secs(20));
}

async fn scheduler_fixture_call(
    store: &crate::store::Store,
    principal: &crate::model::Principal,
    method: &str,
    mut request: serde_json::Value,
) -> serde_json::Value {
    request["client_request_id"] = serde_json::json!(crate::model::new_id());
    store
        .call(principal.clone(), method.to_owned(), request)
        .await
        .unwrap()
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

async fn scheduler_store_fixture() -> (crate::store::StoreOwner, std::path::PathBuf) {
    use crate::{
        config::Config,
        model,
        platform::{DataRoot, bootstrap_credential},
        store::StoreOwner,
    };
    use std::sync::Arc;

    let directory = std::env::temp_dir().join(format!("scheduler-owner-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let directory = root.path.clone();
    let credential = bootstrap_credential(&directory).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.automation_scheduler.enabled = true;
    let owner = StoreOwner::start(root, Arc::new(config), credential)
        .await
        .unwrap();
    (owner, directory)
}

fn scheduler_fixture_identity(
    child: &SchedulerFixtureChild,
    store: &crate::store::Store,
) -> serde_json::Value {
    let process_identity = swarm_process::spawned_identity(child.0.id()).unwrap();
    swarm_automation::service_owner_group_identity(
        &process_identity,
        store.scheduler_owner_token().unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn r48_owner_lost_store_results_hold_intent_and_process_capacity() {
    let (owner, directory) = scheduler_store_fixture().await;
    let store = &owner.store;

    store.lose_next_automation_scheduler_begin_reply_after_commit();
    let mut child_launch_calls = 0;
    let lost_begin = crate::host::begin_scheduler_worker_then_spawn(store, || {
        child_launch_calls += 1;
        Ok::<(), std::io::Error>(())
    })
    .await
    .unwrap_err();
    assert_eq!(lost_begin.code, "STORE_CLOSED");
    assert_eq!(child_launch_calls, 0);

    let retained = store
        .run(|db| super::owner_record(db))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.phase, super::SchedulerOwnerPhase::Launching);
    assert_eq!(
        retained.scope,
        store.run(|db| super::current_scope(db)).await.unwrap()
    );
    let retained_before = serde_json::to_value(&retained).unwrap();
    let retry = crate::host::begin_scheduler_worker_then_spawn(store, || {
        child_launch_calls += 1;
        Ok::<(), std::io::Error>(())
    })
    .await
    .unwrap_err();
    assert_eq!(retry.code, "AUTOMATION_SERVICE_OWNER_UNKNOWN");
    assert_eq!(child_launch_calls, 0);
    assert_eq!(
        serde_json::to_value(
            store
                .run(|db| super::owner_record(db))
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        retained_before
    );

    // The lost begin reply returned no launch tuple. Read back that exact
    // reservation and settle it only after the host launch closure proves it
    // was never called.
    store
        .abandon_automation_scheduler_worker(retained.scope, retained.launch_id)
        .await
        .unwrap();

    let ready_path = directory.join("r48-scheduler-owner-ready");
    let (scope, launch_id, mut child) =
        crate::host::begin_scheduler_worker_then_spawn(store, || {
            child_launch_calls += 1;
            Ok::<_, std::io::Error>(SchedulerFixtureChild::spawn(
                &store.automation_scheduler_worker_config_path(),
                &ready_path,
            ))
        })
        .await
        .unwrap();
    let identity = scheduler_fixture_identity(&child, store);

    store.lose_next_automation_scheduler_activate_reply_after_commit();
    let lost_activate = store
        .activate_automation_scheduler_worker(scope.clone(), launch_id.clone(), identity.clone())
        .await
        .unwrap_err();
    assert_eq!(lost_activate.code, "STORE_CLOSED");
    let active = store
        .run(|db| super::owner_record(db))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.phase, super::SchedulerOwnerPhase::Active);
    assert_eq!(active.scope, scope);
    assert_eq!(active.launch_id, launch_id);
    assert_eq!(active.group_identity, Some(identity.clone()));

    let blocked = crate::host::begin_scheduler_worker_then_spawn(store, || {
        child_launch_calls += 1;
        Ok::<(), std::io::Error>(())
    })
    .await
    .unwrap_err();
    assert_eq!(blocked.code, "AUTOMATION_SERVICE_OWNER_RETAINED");
    assert_eq!(child_launch_calls, 1);
    assert!(child.0.try_wait().unwrap().is_none());
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    store
        .finish_automation_scheduler_worker(scope, launch_id, identity)
        .await
        .unwrap();
    let (next_scope, next_launch_id) = store.begin_automation_scheduler_worker().await.unwrap();
    assert_eq!(next_scope, active.scope);
    assert_ne!(next_launch_id, active.launch_id);
    store
        .abandon_automation_scheduler_worker(next_scope, next_launch_id)
        .await
        .unwrap();

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(windows)]
#[tokio::test]
async fn r48_owner_query_retention_failure_stops_child_before_activation() {
    let (owner, directory) = scheduler_store_fixture().await;
    let store = &owner.store;
    let ready_path = directory.join("r48-retention-refusal-ready");
    let config_path = store.automation_scheduler_worker_config_path();
    let (scope, launch_id, mut child) =
        crate::host::begin_scheduler_worker_then_spawn(store, || {
            tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::automation_scheduler::fixture::scheduler_fixture_owner_child",
                ])
                .env("SWARM_SCHEDULER_FIXTURE_CONFIG", &config_path)
                .env("SWARM_SCHEDULER_FIXTURE_READY", &ready_path)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
        })
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !ready_path.exists() {
            assert!(child.try_wait().unwrap().is_none());
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the actual scheduler fixture enters its service Job");
    let launched = swarm_process::spawned_identity(child.id().unwrap()).unwrap();
    let identity = swarm_automation::service_owner_group_identity(
        &launched,
        store.scheduler_owner_token().unwrap(),
    )
    .unwrap();
    // This independent test observer lets final cleanup prove the real family
    // empty; it is never passed to the production retention-failure path.
    let observed = swarm_automation::retain_service_owner_family(
        &identity,
        store.scheduler_owner_token().unwrap(),
    )
    .unwrap();
    let wrong_token = format!("{}{}", crate::model::new_id(), crate::model::new_id());
    let error =
        match crate::host::retain_scheduler_family_or_stop(&mut child, &identity, &wrong_token)
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("an unavailable exact Job must not permit activation"),
        };
    assert_eq!(error.code, "AUTOMATION_WORKER_CLEANUP_UNKNOWN");
    assert!(
        error
            .secondary_codes
            .iter()
            .any(|code| code == "AUTOMATION_SERVICE_OWNER_PROOF_UNAVAILABLE")
    );
    assert!(child.try_wait().unwrap().is_some());
    let retained = store
        .run(|db| super::owner_record(db))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.phase, super::SchedulerOwnerPhase::Launching);
    assert_eq!(retained.launch_id, launch_id);
    let mut replayed = false;
    let retry = crate::host::begin_scheduler_worker_then_spawn(store, || {
        replayed = true;
        Ok::<(), std::io::Error>(())
    })
    .await
    .unwrap_err();
    assert_eq!(retry.code, "AUTOMATION_SERVICE_OWNER_UNKNOWN");
    assert!(!replayed);
    store
        .abandon_exited_automation_scheduler_worker(scope, launch_id, identity)
        .await
        .unwrap();
    drop(observed);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn r48_owner_stale_callbacks_keep_old_receipt_out_of_current_host_status() {
    use super::super::{meta, set_meta};
    use serde_json::json;

    let (owner, directory) = scheduler_store_fixture().await;
    let store = &owner.store;
    let ready_path = directory.join("r48-stale-scheduler-owner-ready");
    let mut child_launches = 0;
    let (old_scope, old_launch_id, mut old_child) =
        crate::host::begin_scheduler_worker_then_spawn(store, || {
            child_launches += 1;
            Ok::<_, std::io::Error>(SchedulerFixtureChild::spawn(
                &store.automation_scheduler_worker_config_path(),
                &ready_path,
            ))
        })
        .await
        .unwrap();
    let old_identity = scheduler_fixture_identity(&old_child, store);

    store
        .run(|db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            set_meta(&tx, "controller_id", &json!("scheduler-next-host"))?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    let launching_before = store
        .run(|db| super::owner_record(db))
        .await
        .unwrap()
        .unwrap();
    let delayed_activation = store
        .activate_automation_scheduler_worker(
            old_scope.clone(),
            old_launch_id.clone(),
            old_identity.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(delayed_activation.code, "AUTOMATION_SERVICE_OWNER_STALE");
    let launching_after = store
        .run(|db| super::owner_record(db))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&launching_after).unwrap(),
        serde_json::to_value(&launching_before).unwrap()
    );
    assert_eq!(launching_after.phase, super::SchedulerOwnerPhase::Launching);
    let status = store
        .run(|db| super::super::host_lifecycle::status(db))
        .await
        .unwrap();
    let owner_receipt = &status["optional_workers"]["automation-scheduler"]["owner"];
    assert_eq!(owner_receipt["status"], "stale");
    assert_eq!(owner_receipt["ready"], false);
    assert_eq!(owner_receipt["launch_id"], old_launch_id);

    let mut replacement_launches = 0;
    let blocked = crate::host::begin_scheduler_worker_then_spawn(store, || {
        replacement_launches += 1;
        Ok::<(), std::io::Error>(())
    })
    .await
    .unwrap_err();
    assert_eq!(blocked.code, "AUTOMATION_SERVICE_OWNER_UNKNOWN");
    assert_eq!(replacement_launches, 0);
    assert_eq!(child_launches, 1);
    assert!(old_child.0.try_wait().unwrap().is_none());

    old_child.0.kill().unwrap();
    old_child.0.wait().unwrap();
    store
        .abandon_exited_automation_scheduler_worker(old_scope, old_launch_id, old_identity)
        .await
        .unwrap();
    // Finish proved this old family empty. Release its observation handle
    // before the next fixture worker creates the same service-token Job.
    drop(old_child);

    let ready_path = directory.join("r48-current-scheduler-owner-ready");
    let (scope, launch_id, mut child) =
        crate::host::begin_scheduler_worker_then_spawn(store, || {
            child_launches += 1;
            Ok::<_, std::io::Error>(SchedulerFixtureChild::spawn(
                &store.automation_scheduler_worker_config_path(),
                &ready_path,
            ))
        })
        .await
        .unwrap();
    assert_eq!(child_launches, 2);
    let identity = scheduler_fixture_identity(&child, store);
    store
        .activate_automation_scheduler_worker(scope.clone(), launch_id.clone(), identity.clone())
        .await
        .unwrap();
    let active_before = store
        .run(|db| super::owner_record(db))
        .await
        .unwrap()
        .unwrap();

    store
        .run(|db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            set_meta(&tx, "host_epoch", &json!(4))?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let delayed_finish = store
        .finish_automation_scheduler_worker(scope, launch_id.clone(), identity)
        .await
        .unwrap_err();
    assert_eq!(delayed_finish.code, "AUTOMATION_SERVICE_OWNER_STALE");
    let active_after = store
        .run(|db| super::owner_record(db))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(&active_after).unwrap(),
        serde_json::to_value(&active_before).unwrap()
    );
    assert_eq!(active_after.phase, super::SchedulerOwnerPhase::Active);

    store
        .run(|db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let registration_key = format!("client:{}", super::SERVICE_ID);
            let mut registration = meta(&tx, &registration_key)?.unwrap();
            registration["automation_scheduler"]["scope"]["generation"] = json!(2);
            set_meta(&tx, &registration_key, &registration)?;
            set_meta(&tx, super::GENERATION_KEY, &json!(2))?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    let status = store
        .run(|db| super::super::host_lifecycle::status(db))
        .await
        .unwrap();
    let owner_receipt = &status["optional_workers"]["automation-scheduler"]["owner"];
    assert_eq!(owner_receipt["status"], "stale");
    assert_eq!(owner_receipt["ready"], false);
    assert_eq!(owner_receipt["launch_id"], launch_id);
    assert_eq!(owner_receipt["scope"]["generation"], 1);
    assert_eq!(
        owner_receipt["host_scope"]["controller_id"],
        "scheduler-next-host"
    );
    assert_eq!(
        owner_receipt["host_scope"]["host_epoch"].as_i64(),
        active_before
            .host_scope
            .as_ref()
            .map(|host_scope| host_scope.host_epoch)
    );

    let no_replacement = crate::host::begin_scheduler_worker_then_spawn(store, || {
        replacement_launches += 1;
        Ok::<(), std::io::Error>(())
    })
    .await
    .unwrap_err();
    assert_eq!(no_replacement.code, "AUTOMATION_SERVICE_OWNER_SCOPE_STALE");
    assert_eq!(replacement_launches, 0);
    assert_eq!(
        serde_json::to_value(
            store
                .run(|db| super::owner_record(db))
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(&active_before).unwrap()
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
