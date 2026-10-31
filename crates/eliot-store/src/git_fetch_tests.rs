use super::{
    ProjectRegistrationInput, RuleDefinitionInput, Store, TimerDefinitionInput, TimerSlotCommit,
};
use eliot_core::{
    CommandState, FrozenSetupFile, SubmitCommand,
    api::{
        FrozenGitFetch, GitCredentialHelper, GitFetchDefinitionArgs, GitFetchOutcome,
        ProjectGitView, TimerRunRequest, TimerSchedule,
    },
    canonical_json_sha256,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};
use ulid::Ulid;

fn now_ms() -> i64 {
    i64::try_from(Ulid::new().timestamp_ms()).unwrap()
}

fn store() -> Store {
    Store::open(Path::new(":memory:")).unwrap()
}

fn registration_input(
    id: &str,
    endpoint: &str,
    actions: Vec<String>,
    credential_helper: Option<GitCredentialHelper>,
) -> ProjectRegistrationInput {
    let checkout = std::env::temp_dir().join(format!("eliot-git-fetch-{id}"));
    ProjectRegistrationInput {
        id: id.to_owned(),
        checkout: checkout.display().to_string(),
        repo_common_dir: checkout.join(".git").display().to_string(),
        git: Some(ProjectGitView {
            fetch_remotes: BTreeMap::from([("origin".to_owned(), endpoint.to_owned())]),
            credential_helper,
        }),
        profiles: Vec::new(),
        actions,
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    }
}

fn register(
    store: &mut Store,
    input: &ProjectRegistrationInput,
    now: i64,
) -> eliot_core::api::ProjectRegistrationView {
    store
        .upsert_project_registration("owner", input, now)
        .unwrap()
        .value
}

fn frozen_fetch(
    registration: &eliot_core::api::ProjectRegistrationView,
    expected_old_oid: Option<String>,
    credential_helper: Option<FrozenSetupFile>,
) -> FrozenGitFetch {
    FrozenGitFetch {
        repository: registration.id.clone(),
        registration_revision: registration.revision,
        canonical_checkout: registration.checkout.clone(),
        common_dir: registration.repo_common_dir.clone(),
        remote: "origin".to_owned(),
        endpoint: registration
            .git
            .as_ref()
            .expect("Git fetch approval")
            .fetch_remotes["origin"]
            .clone(),
        source_ref: "refs/heads/main".to_owned(),
        advertised_oid: "a".repeat(40),
        tracking_ref: "refs/remotes/origin/main".to_owned(),
        expected_old_oid,
        timeout_ms: eliot_core::api::DEFAULT_GIT_FETCH_TIMEOUT_MS,
        credential_helper,
    }
}

fn manager_receipt() -> FrozenSetupFile {
    FrozenSetupFile {
        canonical_path: std::env::current_exe()
            .unwrap()
            .canonicalize()
            .unwrap()
            .display()
            .to_string(),
        size: 1,
        sha256: "ab".repeat(32),
    }
}

fn fetch_request(id: impl Into<String>, frozen: &FrozenGitFetch) -> SubmitCommand {
    let mut request = SubmitCommand::new(id, "git.fetch");
    request.args = serde_json::to_value(frozen).unwrap();
    request
}

fn fetch_rule(id: &str, repository: &str, trigger: &str) -> RuleDefinitionInput {
    let value = json!({
        "id": id,
        "enabled": true,
        "on": {"event": trigger},
        "do": [{
            "action": "git.fetch",
            "repository": repository,
            "remote": "origin",
            "source_ref": "refs/heads/main",
            "tracking_ref": "refs/remotes/origin/main",
        }],
        "limit": {"max": 4, "per": "rule", "window_ms": 60_000},
    });
    RuleDefinitionInput {
        id: id.to_owned(),
        canonical_definition: serde_json::to_string(&value).unwrap(),
        definition_digest: canonical_json_sha256(&value).unwrap(),
        enabled: true,
    }
}

fn ensure_once_timer(
    store: &mut Store,
    principal: &str,
    id: &str,
    now: i64,
) -> eliot_core::api::TimerStateView {
    let at = now + 1_000;
    store
        .ensure_timer_state(
            principal,
            &TimerDefinitionInput {
                id: id.to_owned(),
                schedule_revision: 1,
                schedule: TimerSchedule::Once { at },
                enabled: true,
                initial_next_due_at: Some(at),
            },
            now,
        )
        .unwrap()
}

fn dispatched_event_count(store: &Store, command_id: &str) -> i64 {
    store
        .connection
        .query_row(
            "SELECT COUNT(*) FROM events WHERE kind = 'command.dispatched'
             AND json_extract(data, '$.command_id') = ?1",
            [command_id],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
}

#[test]
fn owner_and_project_event_and_timer_actions_admit_the_same_targetless_command() {
    let mut store = store();
    let now = now_ms();
    let repository = "git-fetch-event-timer";
    let registration = registration_input(
        repository,
        "https://git.example.invalid/team/repository.git",
        vec!["git.fetch".to_owned()],
        None,
    );
    let registered = register(&mut store, &registration, now);
    let principal = format!("project:{repository}");

    let owner_event_rule = fetch_rule("owner-fetch-event", repository, "command.accepted");
    let project_event_rule = fetch_rule("project-fetch-event", repository, "command.accepted");
    let owner_timer_rule = fetch_rule("owner-fetch-timer", repository, "command.accepted");
    let owner_event_state = store
        .ensure_rule_state("owner", &owner_event_rule, now)
        .unwrap();
    let project_event_state = store
        .ensure_rule_state(&principal, &project_event_rule, now)
        .unwrap();
    store
        .ensure_rule_state("owner", &owner_timer_rule, now)
        .unwrap();
    let timer = ensure_once_timer(&mut store, "owner", &owner_timer_rule.id, now);

    let mut launch = SubmitCommand::new(Ulid::new().to_string(), "launch");
    launch.args = json!({
        "project": repository,
        "workspace": registered.checkout.clone(),
    });
    launch.expected_generation = Some(0);
    let launch = store.submit("owner", launch, now + 1).unwrap();
    let accepted_seq = launch
        .events
        .iter()
        .find(|event| event.kind == "command.accepted")
        .expect("the Launch admission supplies a committed event")
        .seq;
    let frozen = frozen_fetch(&registered, None, None);
    let owner_event = store
        .admit_rule_action(
            "owner",
            &owner_event_state,
            accepted_seq,
            fetch_request(String::new(), &frozen),
            now + 2,
        )
        .unwrap();
    let project_event = store
        .admit_rule_action(
            &principal,
            &project_event_state,
            accepted_seq,
            fetch_request(String::new(), &frozen),
            now + 3,
        )
        .unwrap();
    let scheduled_at_ms = timer.next_due_at.unwrap();
    let scheduled = store
        .commit_timer_slot(
            "owner",
            &timer,
            TimerSlotCommit {
                scheduled_at_ms,
                next_due_at_ms: None,
                action: Some(fetch_request(String::new(), &frozen)),
                skip_code: None,
            },
            scheduled_at_ms,
        )
        .unwrap();

    let owner_child = owner_event.child.expect("Owner event Git fetch");
    let project_child = project_event.child.expect("Project event Git fetch");
    let scheduled_child = scheduled.child.expect("Owner timer Git fetch");
    let serialized = serde_json::to_value(&frozen).unwrap();
    for child in [&owner_child, &project_child, &scheduled_child] {
        assert_eq!(child.command.verb, "git.fetch");
        assert!(child.command.target.is_none());
        assert!(child.command.generation.is_none());
        assert_eq!(child.command.args, serialized);
    }
    assert_eq!(owner_child.command.principal, "owner");
    assert_eq!(project_child.command.principal, principal);
}

#[test]
fn project_manual_timer_fetch_requires_frozen_registration_and_helper_policy() {
    let mut store = store();
    let now = now_ms();
    let project = "git-fetch-project-timer";
    let principal = format!("project:{project}");
    let registration = registration_input(
        project,
        "https://git.example.invalid/team/private.git",
        vec!["git.fetch".to_owned()],
        Some(GitCredentialHelper::Manager),
    );
    let registered = register(&mut store, &registration, now);
    let rule_id = "project-manual-fetch";
    let definition = fetch_rule(rule_id, project, "timer");
    let rule = store
        .ensure_rule_state(&principal, &definition, now + 1)
        .unwrap();
    let timer = ensure_once_timer(&mut store, &principal, rule_id, now + 2);
    let request = TimerRunRequest::new(Ulid::new().to_string(), rule_id).in_project(project);
    let without_helper = frozen_fetch(&registered, None, None);
    let missing_helper = store
        .admit_manual_timer_run(
            &principal,
            &request,
            &timer,
            rule.revision,
            fetch_request(String::new(), &without_helper),
            now + 3,
        )
        .unwrap_err();
    assert_eq!(missing_helper.code, "not_allowed");
    assert!(
        store
            .replay_timer_run(&principal, &request)
            .unwrap()
            .is_none()
    );

    let frozen = frozen_fetch(&registered, None, Some(manager_receipt()));
    let first = store
        .admit_manual_timer_run(
            &principal,
            &request,
            &timer,
            rule.revision,
            fetch_request(String::new(), &frozen),
            now + 4,
        )
        .unwrap();
    assert!(first.is_new);
    let child = first.child.as_ref().unwrap();
    let command_id = child.command.id.clone();
    assert_eq!(child.command.verb, "git.fetch");
    assert_eq!(child.command.principal, principal);
    assert!(child.command.target.is_none());
    assert_eq!(child.command.args, serde_json::to_value(&frozen).unwrap());

    let helper_changed_registration = registration_input(
        project,
        "https://git.example.invalid/team/private.git",
        vec!["git.fetch".to_owned()],
        None,
    );
    let current_helper = register(&mut store, &helper_changed_registration, now + 5);
    assert_ne!(current_helper.revision, registered.revision);
    let high_water = store.high_water().unwrap();
    let replay = store
        .admit_manual_timer_run(
            &principal,
            &request,
            &timer,
            rule.revision,
            fetch_request(String::new(), &frozen),
            now + 6,
        )
        .unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.child.as_ref().unwrap().command.id, command_id);
    assert_eq!(store.high_water().unwrap(), high_water);

    let stale_request = TimerRunRequest::new(Ulid::new().to_string(), rule_id).in_project(project);
    let stale = store
        .admit_manual_timer_run(
            &principal,
            &stale_request,
            &timer,
            rule.revision,
            fetch_request(String::new(), &frozen),
            now + 7,
        )
        .unwrap_err();
    assert_eq!(stale.code, "not_allowed");
    let mut helper_mismatch = frozen.clone();
    helper_mismatch.registration_revision = current_helper.revision;
    let helper_request = TimerRunRequest::new(Ulid::new().to_string(), rule_id).in_project(project);
    assert_eq!(
        store
            .admit_manual_timer_run(
                &principal,
                &helper_request,
                &timer,
                rule.revision,
                fetch_request(String::new(), &helper_mismatch),
                now + 8,
            )
            .unwrap_err()
            .code,
        "not_allowed"
    );
    let changed_registration = registration_input(
        project,
        "https://git.example.invalid/team/rotated.git",
        vec!["git.fetch".to_owned()],
        None,
    );
    let current = register(&mut store, &changed_registration, now + 9);
    assert_ne!(current.revision, current_helper.revision);
    let mut endpoint_mismatch = frozen.clone();
    endpoint_mismatch.registration_revision = current.revision;
    endpoint_mismatch.credential_helper = None;
    let endpoint_request =
        TimerRunRequest::new(Ulid::new().to_string(), rule_id).in_project(project);
    assert_eq!(
        store
            .admit_manual_timer_run(
                &principal,
                &endpoint_request,
                &timer,
                rule.revision,
                fetch_request(String::new(), &endpoint_mismatch),
                now + 10,
            )
            .unwrap_err()
            .code,
        "not_allowed"
    );
    let forged_request = TimerRunRequest::new(Ulid::new().to_string(), rule_id).in_project(project);
    let mut forged = frozen;
    forged.repository = "another-project".to_owned();
    assert_eq!(
        store
            .admit_manual_timer_run(
                &principal,
                &forged_request,
                &timer,
                rule.revision,
                fetch_request(String::new(), &forged),
                now + 11,
            )
            .unwrap_err()
            .code,
        "not_allowed"
    );
}

#[test]
fn owner_command_binding_replays_frozen_snapshot_but_conflicts_on_changed_selectors() {
    let mut store = store();
    let now = now_ms();
    let registration = registration_input(
        "git-fetch-idempotency",
        "https://git.example.invalid/team/one.git",
        Vec::new(),
        None,
    );
    let registered = register(&mut store, &registration, now);
    let frozen = frozen_fetch(&registered, None, None);
    let id = Ulid::new().to_string();
    let request = fetch_request(id.clone(), &frozen);
    let first = store.submit("owner", request.clone(), now + 1).unwrap();
    assert!(first.is_new);
    assert_eq!(first.command.state, CommandState::Accepted);

    let changed_registration = registration_input(
        "git-fetch-idempotency",
        "https://git.example.invalid/team/two.git",
        Vec::new(),
        None,
    );
    let current = register(&mut store, &changed_registration, now + 2);
    assert_ne!(current.revision, registered.revision);
    let replay = store.submit("owner", request, now + 3).unwrap();
    assert!(!replay.is_new);
    assert_eq!(replay.command.id, id);
    assert_eq!(replay.command.args, serde_json::to_value(&frozen).unwrap());

    let mut changed_snapshot = frozen.clone();
    changed_snapshot.registration_revision = current.revision;
    changed_snapshot.endpoint = current.git.as_ref().unwrap().fetch_remotes["origin"].clone();
    changed_snapshot.advertised_oid = "c".repeat(40);
    changed_snapshot.expected_old_oid = Some("b".repeat(40));
    let replay_snapshot = store
        .submit(
            "owner",
            fetch_request(id.clone(), &changed_snapshot),
            now + 4,
        )
        .unwrap();
    assert!(!replay_snapshot.is_new);
    assert_eq!(
        replay_snapshot.command.args,
        serde_json::to_value(&frozen).unwrap(),
        "dynamic frozen metadata cannot change the first retained intent"
    );

    let mut changed = frozen.clone();
    changed.source_ref = "refs/heads/release".to_owned();
    assert_eq!(
        store
            .submit("owner", fetch_request(id.clone(), &changed), now + 5)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    let mut changed = frozen.clone();
    changed.tracking_ref = "refs/remotes/origin/release".to_owned();
    assert_eq!(
        store
            .submit("owner", fetch_request(id.clone(), &changed), now + 6)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    let mut changed = frozen.clone();
    changed.remote = "upstream".to_owned();
    assert_eq!(
        store
            .submit("owner", fetch_request(id.clone(), &changed), now + 7)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    let mut changed = frozen.clone();
    changed.repository = "another-repository".to_owned();
    assert_eq!(
        store
            .submit("owner", fetch_request(id.clone(), &changed), now + 8)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    let mut changed = frozen.clone();
    changed.timeout_ms += 60_000;
    assert_eq!(
        store
            .submit("owner", fetch_request(id.clone(), &changed), now + 9)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    assert_eq!(
        store
            .submit(
                "owner",
                fetch_request(Ulid::new().to_string(), &frozen),
                now + 10,
            )
            .unwrap_err()
            .code,
        "not_allowed"
    );
    let mut endpoint_mismatch = frozen.clone();
    endpoint_mismatch.registration_revision = current.revision;
    assert_eq!(
        store
            .submit(
                "owner",
                fetch_request(Ulid::new().to_string(), &endpoint_mismatch),
                now + 11,
            )
            .unwrap_err()
            .code,
        "not_allowed"
    );
}

#[test]
fn raw_git_fetch_replay_canonicalizes_timeout_and_ignores_frozen_metadata() {
    let mut store = store();
    let now = now_ms();
    let registration = registration_input(
        "git-fetch-raw-replay",
        "https://git.example.invalid/team/replay.git",
        Vec::new(),
        None,
    );
    let registered = register(&mut store, &registration, now);
    let mut frozen = frozen_fetch(&registered, None, None);
    frozen.timeout_ms = 60_000;
    let id = Ulid::new().to_string();
    store
        .submit("owner", fetch_request(id.clone(), &frozen), now + 1)
        .unwrap();

    let mut changed_snapshot = frozen.clone();
    changed_snapshot.registration_revision += 1;
    changed_snapshot.endpoint = "https://git.example.invalid/team/changed.git".to_owned();
    changed_snapshot.advertised_oid = "b".repeat(40);
    changed_snapshot.expected_old_oid = Some("c".repeat(40));
    changed_snapshot.credential_helper = Some(manager_receipt());
    let metadata_replay = store
        .submit(
            "owner",
            fetch_request(id.clone(), &changed_snapshot),
            now + 2,
        )
        .unwrap();
    assert!(!metadata_replay.is_new);
    assert!(metadata_replay.events.is_empty());
    assert_eq!(
        metadata_replay.command.args,
        serde_json::to_value(&frozen).unwrap()
    );

    let definition = |timeout: Option<&str>| GitFetchDefinitionArgs {
        repository: registered.id.clone(),
        remote: "origin".to_owned(),
        source_ref: "refs/heads/main".to_owned(),
        tracking_ref: "refs/remotes/origin/main".to_owned(),
        timeout: timeout.map(str::to_owned),
    };
    let raw_request = |command_id: &str, definition: &GitFetchDefinitionArgs| {
        let mut request = SubmitCommand::new(command_id.to_owned(), "git.fetch");
        request.args = serde_json::to_value(definition).unwrap();
        request
    };
    let replay_minute = store
        .replay_git_fetch_submission("owner", &raw_request(&id, &definition(Some("1m"))), 60_000)
        .unwrap()
        .expect("first raw selector replay finds the frozen command");
    let replay_seconds = store
        .replay_git_fetch_submission("owner", &raw_request(&id, &definition(Some("60s"))), 60_000)
        .unwrap()
        .expect("equivalent duration has the same canonical request digest");
    assert_eq!(replay_minute.command.id, id);
    assert_eq!(replay_seconds.command.id, id);
    assert_eq!(
        replay_seconds.command.args,
        serde_json::to_value(&frozen).unwrap()
    );

    let changed_repository = GitFetchDefinitionArgs {
        repository: "another-registered-repository".to_owned(),
        ..definition(Some("1m"))
    };
    let changed_remote = GitFetchDefinitionArgs {
        remote: "upstream".to_owned(),
        ..definition(Some("1m"))
    };
    let changed_source = GitFetchDefinitionArgs {
        source_ref: "refs/heads/release".to_owned(),
        ..definition(Some("1m"))
    };
    let changed_tracking = GitFetchDefinitionArgs {
        tracking_ref: "refs/remotes/origin/release".to_owned(),
        ..definition(Some("1m"))
    };
    for (changed, timeout_ms) in [
        (changed_repository, 60_000),
        (changed_remote, 60_000),
        (changed_source, 60_000),
        (changed_tracking, 60_000),
        (definition(Some("2m")), 120_000),
    ] {
        assert_eq!(
            store
                .replay_git_fetch_submission("owner", &raw_request(&id, &changed), timeout_ms)
                .unwrap_err()
                .code,
            "id_conflict"
        );
    }
    assert_eq!(
        store
            .replay_git_fetch_submission(
                &format!("project:{}", registered.id),
                &raw_request(&id, &definition(Some("1m"))),
                60_000,
            )
            .unwrap_err()
            .code,
        "id_conflict"
    );

    let default_id = Ulid::new().to_string();
    let mut default_frozen = frozen.clone();
    default_frozen.timeout_ms = eliot_core::api::DEFAULT_GIT_FETCH_TIMEOUT_MS;
    store
        .submit(
            "owner",
            fetch_request(default_id.clone(), &default_frozen),
            now + 3,
        )
        .unwrap();
    let omitted_timeout = definition(None);
    let explicit_default = definition(Some("10m"));
    let replay_omitted = store
        .replay_git_fetch_submission(
            "owner",
            &raw_request(&default_id, &omitted_timeout),
            eliot_core::api::DEFAULT_GIT_FETCH_TIMEOUT_MS,
        )
        .unwrap()
        .expect("omitted timeout binds to the effective default");
    let replay_explicit = store
        .replay_git_fetch_submission(
            "owner",
            &raw_request(&default_id, &explicit_default),
            eliot_core::api::DEFAULT_GIT_FETCH_TIMEOUT_MS,
        )
        .unwrap()
        .expect("explicit default duration has the same canonical binding");
    assert_eq!(replay_omitted.command.id, default_id);
    assert_eq!(
        replay_explicit.command.args,
        serde_json::to_value(&default_frozen).unwrap()
    );
}

#[test]
fn same_repo_fetches_queue_through_unknown_restart_while_other_repos_proceed() {
    let now = now_ms();
    let path = std::env::temp_dir().join(format!("eliot-store-git-fetch-{}.db", Ulid::new()));
    let mut store = Store::open(&path).unwrap();
    let repo_a = register(
        &mut store,
        &registration_input(
            "git-fetch-repo-a",
            "https://git.example.invalid/team/a.git",
            Vec::new(),
            None,
        ),
        now,
    );
    let repo_b = register(
        &mut store,
        &registration_input(
            "git-fetch-repo-b",
            "https://git.example.invalid/team/b.git",
            Vec::new(),
            None,
        ),
        now + 1,
    );
    let frozen_a = frozen_fetch(&repo_a, None, None);
    let frozen_b = frozen_fetch(&repo_b, None, None);
    let first_id = Ulid::new().to_string();
    let second_id = Ulid::new().to_string();
    let other_id = Ulid::new().to_string();
    let first = store
        .submit("owner", fetch_request(first_id.clone(), &frozen_a), now + 2)
        .unwrap();
    assert!(first.is_new);
    store
        .submit(
            "owner",
            fetch_request(second_id.clone(), &frozen_a),
            now + 3,
        )
        .unwrap();
    store
        .submit("owner", fetch_request(other_id.clone(), &frozen_b), now + 4)
        .unwrap();

    assert_eq!(
        store.dispatch(&second_id, now + 5).unwrap().value.state,
        CommandState::Queued
    );
    assert_eq!(
        store.dispatch(&other_id, now + 6).unwrap().value.state,
        CommandState::Dispatched
    );
    assert_eq!(
        store.dispatch(&first_id, now + 7).unwrap().value.state,
        CommandState::Dispatched
    );
    store
        .settle(&first_id, CommandState::Unknown, None, now + 8)
        .unwrap();
    drop(store);

    let mut reopened = Store::open(&path).unwrap();
    assert_eq!(
        reopened.get_command(&first_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(
        reopened.dispatch(&first_id, now + 9).unwrap_err().code,
        "bad_state"
    );
    assert_eq!(
        reopened.dispatch(&second_id, now + 10).unwrap().value.state,
        CommandState::Queued
    );
    assert_eq!(
        dispatched_event_count(&reopened, &first_id),
        1,
        "reopen and Unknown do not redispatch the first fetch"
    );
    assert_eq!(dispatched_event_count(&reopened, &second_id), 0);
    assert_eq!(dispatched_event_count(&reopened, &other_id), 1);

    let wrong_readback = serde_json::to_value(GitFetchOutcome {
        tracking_ref: frozen_a.tracking_ref.clone(),
        tracking_oid: "c".repeat(40),
    })
    .unwrap();
    assert_eq!(
        reopened
            .settle(
                &first_id,
                CommandState::Applied,
                Some(wrong_readback),
                now + 11,
            )
            .unwrap_err()
            .code,
        "invalid"
    );
    assert_eq!(
        reopened.get_command(&first_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    reopened
        .settle(
            &first_id,
            CommandState::Applied,
            Some(
                serde_json::to_value(GitFetchOutcome {
                    tracking_ref: frozen_a.tracking_ref.clone(),
                    tracking_oid: frozen_a.advertised_oid.clone(),
                })
                .unwrap(),
            ),
            now + 12,
        )
        .unwrap();
    assert_eq!(
        reopened.dispatch(&second_id, now + 13).unwrap().value.state,
        CommandState::Dispatched
    );
    assert_eq!(dispatched_event_count(&reopened, &first_id), 1);
    assert_eq!(dispatched_event_count(&reopened, &second_id), 1);
    drop(reopened);
    for suffix in ["", "-wal", "-shm"] {
        let candidate = if suffix.is_empty() {
            path.clone()
        } else {
            PathBuf::from(format!("{}{}", path.display(), suffix))
        };
        let _ = fs::remove_file(candidate);
    }
}
