use super::{ProjectRegistrationInput, Store, WorkspaceGitReservation};
use eliot_core::{
    AgentState, CommandState, FrozenGitFetch, GitFetchOutcome, NativeEvent, SourceBatch,
    SubmitCommand, WorkspaceCreationOutcome, WorkspacePhase, WorkspacePreparation,
    WorkspacePreparationInput, WorkspaceRemovalOutcome,
    api::{DEFAULT_GIT_FETCH_TIMEOUT_MS, ProjectGitView, ProjectRegistrationView},
};
use serde_json::json;
use std::{collections::BTreeMap, path::Path};
use ulid::Ulid;

struct WorkspaceFixture {
    agent: String,
    start_command_id: String,
    input: WorkspacePreparationInput,
}

fn now_ms() -> i64 {
    i64::try_from(Ulid::new().timestamp_ms()).expect("ULID timestamp fits i64")
}

fn open_store() -> Store {
    Store::open(Path::new(":memory:")).expect("open real in-memory Store")
}

fn register_repository(store: &mut Store, now: i64) -> ProjectRegistrationView {
    let id = format!("repo-{}", Ulid::new().to_string().to_ascii_lowercase());
    let checkout = std::env::temp_dir()
        .join(format!("eliot-git-fence-{id}"))
        .join("checkout")
        .display()
        .to_string();
    let repo_common_dir = Path::new(&checkout).join(".git").display().to_string();
    let input = ProjectRegistrationInput {
        id: id.clone(),
        checkout: checkout.clone(),
        repo_common_dir: repo_common_dir.clone(),
        git: Some(ProjectGitView {
            fetch_remotes: BTreeMap::from([(
                "origin".to_owned(),
                format!("https://git.example.test/{id}.git"),
            )]),
            credential_helper: None,
        }),
        profiles: vec!["fake".to_owned()],
        actions: vec!["git.fetch".to_owned()],
        interpreters: BTreeMap::new(),
        run_env: Vec::new(),
    };
    store
        .upsert_project_registration("owner", &input, now)
        .expect("register approved Git fetch remote")
        .value
}

fn admit_workspace(
    store: &mut Store,
    registration: &ProjectRegistrationView,
    label: &str,
    now: i64,
) -> WorkspaceFixture {
    let worktree_path = Path::new(&registration.checkout)
        .parent()
        .expect("registered checkout has a parent")
        .join(format!("worktree-{label}"))
        .display()
        .to_string();
    let input = WorkspacePreparationInput {
        project: registration.checkout.clone(),
        repo_path: registration.checkout.clone(),
        repo_common_dir: registration.repo_common_dir.clone(),
        requested_branch: format!("codex/{label}"),
        base_ref: Some("HEAD".to_owned()),
        base_oid: "d".repeat(40),
        worktree_path,
        frozen_setup: Vec::new(),
    };
    let start_command_id = Ulid::new().to_string();
    let mut launch = SubmitCommand::new(start_command_id.clone(), "launch");
    launch.expected_generation = Some(0);
    launch.args = json!({
        "project": input.project,
        "workspace": input.repo_path,
        "worktree": input.requested_branch,
        "harness": "fake",
        "profile": "fake",
        "profile_snapshot": {},
    });
    let admission = store
        .submit_with_workspace("owner", launch, input.clone(), now)
        .expect("admit managed workspace launch");
    let agent = admission.command.target.expect("workspace Agent target");
    let preparation = store
        .get_workspace(&agent)
        .expect("read admitted workspace")
        .expect("admitted workspace row");
    assert_eq!(preparation.phase, WorkspacePhase::Creating);
    WorkspaceFixture {
        agent,
        start_command_id,
        input,
    }
}

fn workspace(store: &Store, fixture: &WorkspaceFixture) -> WorkspacePreparation {
    store
        .get_workspace(&fixture.agent)
        .expect("read managed workspace")
        .expect("managed workspace row")
}

fn settle_creation(
    store: &mut Store,
    fixture: &WorkspaceFixture,
    target_generation: i64,
    outcome: WorkspaceCreationOutcome,
    now: i64,
) {
    store
        .settle_workspace_creation(&fixture.start_command_id, target_generation, outcome, now)
        .expect("settle typed workspace creation/readback");
}

fn verified_creation(
    store: &mut Store,
    fixture: &WorkspaceFixture,
    target_generation: i64,
    now: i64,
) {
    settle_creation(
        store,
        fixture,
        target_generation,
        WorkspaceCreationOutcome::Verified {
            repo_path: fixture.input.repo_path.clone(),
            repo_common_dir: fixture.input.repo_common_dir.clone(),
            worktree_path: fixture.input.worktree_path.clone(),
            branch: fixture.input.requested_branch.clone(),
            head: fixture.input.base_oid.clone(),
        },
        now,
    );
}

fn ready_workspace(
    store: &mut Store,
    registration: &ProjectRegistrationView,
    label: &str,
    now: i64,
) -> WorkspaceFixture {
    let fixture = admit_workspace(store, registration, label, now);
    let creating = workspace(store, &fixture);
    assert!(matches!(
        store
            .begin_workspace_git_effect(&fixture.start_command_id, &creating, now + 1)
            .expect("reserve initial worktree creation"),
        WorkspaceGitReservation::Reserved
    ));
    verified_creation(store, &fixture, 1, now + 2);
    store
        .dispatch(&fixture.start_command_id, now + 3)
        .expect("dispatch Fake launch after worktree readback");
    let batch_id = Ulid::new().to_string();
    let native_scope = format!("timer-resume-{}-scope", fixture.agent);
    let native_ref = format!("timer-resume-{}-subject", fixture.agent);
    store
        .put_batch(
            SourceBatch {
                batch_id: batch_id.clone(),
                agent_id: fixture.agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".to_owned(),
                    native_id: batch_id,
                    data: json!({
                        "command_id":fixture.start_command_id,
                        "native_scope":native_scope,
                        "native_ref":native_ref,
                    }),
                }],
                cursor: None,
            },
            now + 4,
        )
        .expect("commit Fake session readiness");
    store
        .settle(
            &fixture.start_command_id,
            CommandState::Applied,
            None,
            now + 5,
        )
        .expect("settle Fake launch");
    assert_eq!(workspace(store, &fixture).phase, WorkspacePhase::Ready);
    fixture
}

fn end_session(store: &mut Store, fixture: &WorkspaceFixture, now: i64) {
    let batch_id = Ulid::new().to_string();
    store
        .put_batch(
            SourceBatch {
                batch_id: batch_id.clone(),
                agent_id: fixture.agent.clone(),
                generation: 1,
                source: "stream".to_owned(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ended".to_owned(),
                    native_id: batch_id,
                    data: json!({}),
                }],
                cursor: None,
            },
            now,
        )
        .expect("commit retained Fake session end");
    assert_eq!(
        store.get_agent(&fixture.agent).unwrap().unwrap().state,
        AgentState::Stopped
    );
}

fn dispatch_removal(
    store: &mut Store,
    fixture: &WorkspaceFixture,
    now: i64,
) -> (String, WorkspacePreparation) {
    end_session(store, fixture, now);
    let command_id = Ulid::new().to_string();
    let mut remove = SubmitCommand::new(command_id.clone(), "worktree.remove");
    remove.target = Some(fixture.agent.clone());
    remove.expected_generation = Some(1);
    store
        .submit("owner", remove, now + 1)
        .expect("admit exact terminal worktree removal");
    store
        .dispatch(&command_id, now + 2)
        .expect("dispatch removal before native Git boundary");
    let removing = workspace(store, fixture);
    assert_eq!(removing.phase, WorkspacePhase::Removing);
    (command_id, removing)
}

fn frozen_fetch(registration: &ProjectRegistrationView) -> FrozenGitFetch {
    let git = registration.git.as_ref().expect("approved Git view");
    FrozenGitFetch {
        repository: registration.id.clone(),
        registration_revision: registration.revision,
        canonical_checkout: registration.checkout.clone(),
        common_dir: registration.repo_common_dir.clone(),
        remote: "origin".to_owned(),
        endpoint: git
            .fetch_remotes
            .get("origin")
            .expect("approved origin endpoint")
            .clone(),
        source_ref: "refs/heads/main".to_owned(),
        advertised_oid: "b".repeat(40),
        tracking_ref: "refs/remotes/origin/main".to_owned(),
        expected_old_oid: None,
        timeout_ms: DEFAULT_GIT_FETCH_TIMEOUT_MS,
        credential_helper: None,
    }
}

fn dispatch_fetch(
    store: &mut Store,
    registration: &ProjectRegistrationView,
    now: i64,
) -> (String, FrozenGitFetch) {
    let frozen = frozen_fetch(registration);
    let command_id = Ulid::new().to_string();
    let mut request = SubmitCommand::new(command_id.clone(), "git.fetch");
    request.args = serde_json::to_value(&frozen).expect("serialize frozen fetch intent");
    let principal = format!("project:{}", registration.id);
    store
        .submit(&principal, request, now)
        .expect("admit registered-project git.fetch command");
    store
        .dispatch(&command_id, now + 1)
        .expect("dispatch ordinary targetless git.fetch command");
    (command_id, frozen)
}

fn settle_fetch_unknown(store: &mut Store, command_id: &str, now: i64) {
    store
        .settle(command_id, CommandState::Unknown, None, now)
        .expect("retain uncertain native fetch");
}

fn settle_fetch_applied(store: &mut Store, command_id: &str, frozen: &FrozenGitFetch, now: i64) {
    let outcome = GitFetchOutcome {
        tracking_ref: frozen.tracking_ref.clone(),
        tracking_oid: frozen.advertised_oid.clone(),
    };
    outcome.validate_for(frozen).unwrap();
    store
        .settle(
            command_id,
            CommandState::Applied,
            Some(serde_json::to_value(outcome).unwrap()),
            now,
        )
        .expect("settle fetch from exact tracking-ref readback");
}

fn assert_pending(store: &Store, command_id: &str) {
    let command = store.get_command(command_id).unwrap().unwrap();
    assert!(matches!(
        command.state,
        CommandState::Accepted | CommandState::Queued | CommandState::Dispatched
    ));
    assert_eq!(command.outcome, None);
}

#[test]
fn unknown_native_worktree_creation_defers_same_repo_fetch_until_readback() {
    let now = now_ms();
    let mut store = open_store();
    let registration = register_repository(&mut store, now);
    let creating = admit_workspace(&mut store, &registration, "uncertain-create", now + 20);
    let creating_view = workspace(&store, &creating);

    assert!(matches!(
        store
            .begin_workspace_git_effect(&creating.start_command_id, &creating_view, now + 21)
            .unwrap(),
        WorkspaceGitReservation::Reserved
    ));
    let reserved_view = workspace(&store, &creating);
    assert!(matches!(
        store
            .begin_workspace_git_effect(&creating.start_command_id, &reserved_view, now + 22)
            .unwrap(),
        WorkspaceGitReservation::MayHaveBegun
    ));
    settle_creation(
        &mut store,
        &creating,
        1,
        WorkspaceCreationOutcome::Unknown {
            code: "create_readback_unknown".to_owned(),
            message: "synthetic worktree creation uncertainty".to_owned(),
        },
        now + 23,
    );
    assert_eq!(workspace(&store, &creating).phase, WorkspacePhase::Unknown);

    let (fetch_id, _) = dispatch_fetch(&mut store, &registration, now + 24);
    assert_eq!(
        store.get_command(&fetch_id).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_pending(&store, &fetch_id);

    verified_creation(&mut store, &creating, 1, now + 27);
    let retried = store
        .dispatch(&fetch_id, now + 28)
        .expect("retry the retained fetch after create readback");
    assert_eq!(retried.value.state, CommandState::Dispatched);
}

#[test]
fn earlier_pending_workspace_create_orders_later_fetch_without_losing_intent() {
    let now = now_ms();
    let mut store = open_store();
    let registration = register_repository(&mut store, now);
    let completed = ready_workspace(&mut store, &registration, "existing", now + 10);
    let completed_view = workspace(&store, &completed);
    assert_eq!(completed_view.phase, WorkspacePhase::Ready);
    assert_eq!(
        completed_view
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched"),
        Some(&json!(true))
    );
    let pending_create = admit_workspace(&mut store, &registration, "pending-create", now + 20);
    let creating = workspace(&store, &pending_create);
    assert_eq!(
        creating
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched"),
        Some(&json!(false))
    );

    let (fetch_id, _) = dispatch_fetch(&mut store, &registration, now + 21);
    assert_eq!(
        store.get_command(&fetch_id).unwrap().unwrap().state,
        CommandState::Queued,
        "later fetch remains ordered behind the pending workspace writer"
    );
    assert_pending(&store, &fetch_id);

    assert!(matches!(
        store
            .begin_workspace_git_effect(&pending_create.start_command_id, &creating, now + 23,)
            .unwrap(),
        WorkspaceGitReservation::Reserved
    ));
    verified_creation(&mut store, &pending_create, 1, now + 24);
    assert_eq!(
        store.dispatch(&fetch_id, now + 25).unwrap().value.state,
        CommandState::Dispatched
    );
}

#[test]
fn unknown_fetch_defers_same_repo_create_but_not_unrelated_repo() {
    let now = now_ms();
    let mut store = open_store();
    let registration = register_repository(&mut store, now);
    let (fetch_id, frozen) = dispatch_fetch(&mut store, &registration, now + 20);
    assert_eq!(
        store.get_command(&fetch_id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
    settle_fetch_unknown(&mut store, &fetch_id, now + 24);

    let same_repo_create = admit_workspace(&mut store, &registration, "later-create", now + 25);
    let same_repo_view = workspace(&store, &same_repo_create);
    assert!(matches!(
        store
            .begin_workspace_git_effect(
                &same_repo_create.start_command_id,
                &same_repo_view,
                now + 26,
            )
            .unwrap(),
        WorkspaceGitReservation::Deferred
    ));
    assert_pending(&store, &same_repo_create.start_command_id);
    let deferred_workspace = workspace(&store, &same_repo_create);
    assert_eq!(
        deferred_workspace
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched"),
        Some(&json!(false))
    );

    let unrelated = register_repository(&mut store, now + 27);
    let unrelated_create = admit_workspace(&mut store, &unrelated, "independent-create", now + 28);
    let unrelated_view = workspace(&store, &unrelated_create);
    assert!(matches!(
        store
            .begin_workspace_git_effect(
                &unrelated_create.start_command_id,
                &unrelated_view,
                now + 29,
            )
            .unwrap(),
        WorkspaceGitReservation::Reserved
    ));

    settle_fetch_applied(&mut store, &fetch_id, &frozen, now + 30);
    let same_repo_view = workspace(&store, &same_repo_create);
    assert!(matches!(
        store
            .begin_workspace_git_effect(
                &same_repo_create.start_command_id,
                &same_repo_view,
                now + 31,
            )
            .unwrap(),
        WorkspaceGitReservation::Reserved
    ));
}

#[test]
fn unknown_fetch_queues_remove_without_marking_native_effect() {
    let now = now_ms();
    let mut store = open_store();
    let registration = register_repository(&mut store, now);
    let remove_workspace = ready_workspace(&mut store, &registration, "remove", now + 10);
    let (fetch_id, frozen) = dispatch_fetch(&mut store, &registration, now + 20);
    assert_eq!(
        store.get_command(&fetch_id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
    settle_fetch_unknown(&mut store, &fetch_id, now + 21);

    end_session(&mut store, &remove_workspace, now + 22);
    let remove_id = Ulid::new().to_string();
    let mut remove = SubmitCommand::new(remove_id.clone(), "worktree.remove");
    remove.target = Some(remove_workspace.agent.clone());
    remove.expected_generation = Some(1);
    store
        .submit("owner", remove, now + 23)
        .expect("admit exact same-repository removal");
    store
        .dispatch(&remove_id, now + 24)
        .expect("defer removal before its native Git boundary");
    assert_eq!(
        store.get_command(&remove_id).unwrap().unwrap().state,
        CommandState::Queued
    );
    assert_pending(&store, &remove_id);
    let still_ready = workspace(&store, &remove_workspace);
    assert_eq!(still_ready.phase, WorkspacePhase::Ready);
    assert_eq!(
        still_ready
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("stage"))
            .and_then(serde_json::Value::as_str),
        Some("creation")
    );
    assert_ne!(
        still_ready
            .outcome
            .as_ref()
            .and_then(|outcome| outcome.get("command_id"))
            .and_then(serde_json::Value::as_str),
        Some(remove_id.as_str())
    );

    settle_fetch_applied(&mut store, &fetch_id, &frozen, now + 25);
    assert_eq!(
        store.dispatch(&remove_id, now + 26).unwrap().value.state,
        CommandState::Dispatched
    );
    let removing = workspace(&store, &remove_workspace);
    assert_eq!(removing.phase, WorkspacePhase::Removing);
    assert!(matches!(
        store
            .begin_workspace_git_effect(&remove_id, &removing, now + 27)
            .unwrap(),
        WorkspaceGitReservation::Reserved
    ));
}

#[test]
fn retained_ready_resume_unknown_without_git_marker_does_not_fence_fetch() {
    let now = now_ms();
    let mut store = open_store();
    let registration = register_repository(&mut store, now);
    let resume_workspace = ready_workspace(&mut store, &registration, "resume", now + 10);
    end_session(&mut store, &resume_workspace, now + 30);

    let resume_id = Ulid::new().to_string();
    let mut resume = SubmitCommand::new(resume_id.clone(), "resume");
    resume.target = Some(resume_workspace.agent.clone());
    resume.expected_generation = Some(1);
    let admission = store
        .submit_with_workspace("owner", resume, resume_workspace.input.clone(), now + 31)
        .expect("admit retained Ready Resume");
    assert_eq!(admission.command.state, CommandState::Accepted);
    let resume_fixture = WorkspaceFixture {
        agent: resume_workspace.agent.clone(),
        start_command_id: resume_id.clone(),
        input: resume_workspace.input.clone(),
    };
    verified_creation(&mut store, &resume_fixture, 2, now + 32);
    settle_creation(
        &mut store,
        &resume_fixture,
        2,
        WorkspaceCreationOutcome::Unknown {
            code: "retained_inspection_unknown".to_owned(),
            message: "retained Ready workspace inspection did not complete".to_owned(),
        },
        now + 33,
    );
    assert_eq!(
        store.get_command(&resume_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(
        workspace(&store, &resume_workspace).phase,
        WorkspacePhase::Unknown
    );
    assert_eq!(
        workspace(&store, &resume_workspace)
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched"),
        Some(&json!(false))
    );

    let (fetch_id, _) = dispatch_fetch(&mut store, &registration, now + 34);
    assert_eq!(
        store.get_command(&fetch_id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
}

#[test]
fn predrain_remove_inspection_unknown_without_git_marker_does_not_fence_fetch() {
    let now = now_ms();
    let mut store = open_store();
    let registration = register_repository(&mut store, now);
    let remove_workspace = ready_workspace(&mut store, &registration, "remove", now + 10);
    let (remove_id, _) = dispatch_removal(&mut store, &remove_workspace, now + 30);
    let removing = workspace(&store, &remove_workspace);

    // This is the source-drain/read-only inspection result. The native Git
    // effect boundary is deliberately never entered for this removal command.
    store
        .settle_workspace_removal(
            &remove_id,
            removing.target_generation,
            WorkspaceRemovalOutcome::Unknown {
                code: "preflight_inspection_unknown".to_owned(),
                message: "read-only pre-removal inspection did not complete".to_owned(),
            },
            now + 33,
        )
        .expect("retain read-only removal inspection uncertainty");
    assert_eq!(
        store.get_command(&remove_id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(
        workspace(&store, &remove_workspace)
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched"),
        Some(&json!(false))
    );

    let (fetch_id, _) = dispatch_fetch(&mut store, &registration, now + 34);
    assert_eq!(
        store.get_command(&fetch_id).unwrap().unwrap().state,
        CommandState::Dispatched
    );
}

#[test]
fn legacy_absent_marker_is_conservative_but_new_false_unknown_is_read_only() {
    let now = now_ms();
    let mut legacy_store = open_store();
    let legacy_registration = register_repository(&mut legacy_store, now);
    let legacy_create =
        admit_workspace(&mut legacy_store, &legacy_registration, "legacy", now + 10);
    let legacy_creating = workspace(&legacy_store, &legacy_create);
    assert!(matches!(
        legacy_store
            .begin_workspace_git_effect(
                &legacy_create.start_command_id,
                &legacy_creating,
                now + 21,
            )
            .unwrap(),
        WorkspaceGitReservation::Reserved
    ));
    settle_creation(
        &mut legacy_store,
        &legacy_create,
        1,
        WorkspaceCreationOutcome::Unknown {
            code: "legacy_create_readback_unknown".to_owned(),
            message: "synthetic legacy creation uncertainty".to_owned(),
        },
        now + 22,
    );
    let legacy_unknown = workspace(&legacy_store, &legacy_create);
    let mut legacy_outcome = legacy_unknown
        .outcome
        .clone()
        .expect("legacy unknown workspace outcome");
    assert_eq!(
        legacy_outcome
            .as_object_mut()
            .expect("legacy outcome object")
            .remove("native_git_write_dispatched"),
        Some(json!(true))
    );
    let encoded_outcome = serde_json::to_string(&legacy_outcome).unwrap();
    let transaction = legacy_store
        .connection
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .expect("seed persisted pre-marker row");
    let changed = transaction
        .execute(
            "UPDATE workspaces SET outcome = ?1 WHERE agent = ?2 AND target_generation = ?3",
            rusqlite::params![
                encoded_outcome,
                eliot_core::parse_agent_id(&legacy_create.agent).unwrap(),
                legacy_unknown.target_generation,
            ],
        )
        .unwrap();
    assert_eq!(changed, 1);
    transaction.commit().unwrap();
    let legacy_unknown = workspace(&legacy_store, &legacy_create);
    assert!(
        legacy_unknown
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched")
            .is_none()
    );
    assert!(matches!(
        legacy_store
            .begin_workspace_git_effect(&legacy_create.start_command_id, &legacy_unknown, now + 23,)
            .unwrap(),
        WorkspaceGitReservation::MayHaveBegun
    ));
    let (legacy_fetch, _) = dispatch_fetch(&mut legacy_store, &legacy_registration, now + 24);
    assert_eq!(
        legacy_store
            .get_command(&legacy_fetch)
            .unwrap()
            .unwrap()
            .state,
        CommandState::Queued
    );

    let new_now = now_ms();
    let mut new_store = open_store();
    let new_registration = register_repository(&mut new_store, new_now);
    let new_create = admit_workspace(&mut new_store, &new_registration, "new-false", new_now + 10);
    let new_initial = workspace(&new_store, &new_create);
    assert_eq!(
        new_initial
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched"),
        Some(&json!(false))
    );
    settle_creation(
        &mut new_store,
        &new_create,
        1,
        WorkspaceCreationOutcome::Unknown {
            code: "new_preflight_unknown".to_owned(),
            message: "synthetic pre-native inspection uncertainty".to_owned(),
        },
        new_now + 21,
    );
    let new_unknown = workspace(&new_store, &new_create);
    assert_eq!(
        new_unknown
            .outcome
            .as_ref()
            .unwrap()
            .get("native_git_write_dispatched"),
        Some(&json!(false))
    );
    let (new_fetch, _) = dispatch_fetch(&mut new_store, &new_registration, new_now + 22);
    assert_eq!(
        new_store.get_command(&new_fetch).unwrap().unwrap().state,
        CommandState::Dispatched
    );
}
