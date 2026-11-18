use super::*;
use std::{fs, path::Path, sync::Barrier, thread};

fn register(store: &mut Store, now: i64) {
    register_project(store, now, "alpha", "/managed/alpha");
}

fn register_project(store: &mut Store, now: i64, id: &str, checkout: &str) {
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: id.into(),
                checkout: checkout.into(),
                repo_common_dir: format!("{checkout}/.git"),
                profiles: vec!["approved".into()],
                actions: vec!["launch".into(), CLAIM.into()],
                git: None,
                interpreters: BTreeMap::new(),
                run_env: Vec::new(),
            },
            now,
        )
        .unwrap();
}

fn launch(id: String, project: &str) -> SubmitCommand {
    let mut request = SubmitCommand::new(id, "launch");
    request.args = json!({
        "project": project,
        "workspace": "/managed/alpha",
        "harness": "codex",
        "profile": "approved",
        "profile_snapshot": {"control": "tree"},
    });
    request
}

/// A managed manager launch: the command runs from the registered checkout,
/// names its unique branch, and carries the frozen preparation for a fresh
/// opaque worktree. Mirrors what the daemon freezes before Store admission.
fn managed_launch(
    id: String,
    project: &str,
    worktree: &str,
) -> (SubmitCommand, WorkspacePreparationInput) {
    let repo = match project {
        "alpha" => "/managed/alpha",
        "beta" => "/managed/beta",
        _ => panic!("unknown test project"),
    };
    let mut request = SubmitCommand::new(id, "launch");
    request.args = json!({
        "project": repo,
        "workspace": repo,
        "harness": "codex",
        "profile": "approved",
        "profile_snapshot": {"control": "tree"},
        "worktree": "swarm/test-branch",
    });
    let input = WorkspacePreparationInput {
        project: repo.into(),
        repo_path: repo.into(),
        repo_common_dir: format!("{repo}/.git"),
        requested_branch: "swarm/test-branch".into(),
        base_ref: None,
        base_oid: "0123456789abcdef0123456789abcdef01234567".into(),
        worktree_path: worktree.into(),
        frozen_setup: Vec::new(),
    };
    (request, input)
}

fn managed_request(project: &str) -> ProjectAssignmentLaunchRequest {
    ProjectAssignmentLaunchRequest {
        id: Ulid::new().to_string(),
        project: project.into(),
        profile: "approved".into(),
        adopt_worktree: None,
    }
}

fn reserved_generation(store: &Store, agent: i64) -> Option<i64> {
    store
        .connection
        .query_row(
            "SELECT capacity_generation FROM agents WHERE id = ?1",
            [agent],
            |row| row.get(0),
        )
        .unwrap()
}

/// Drive a freshly admitted managed worktree to `ready`, standing in for the
/// daemon preparation chain which has no Store-level side effects to test.
fn mark_worktree_ready(store: &Store, agent: i64) {
    store
        .connection
        .execute(
            "UPDATE workspaces SET phase = 'ready' WHERE agent = ?1",
            [agent],
        )
        .unwrap();
}

fn ready(store: &mut Store, project: &str, now: i64) -> String {
    let id = Ulid::new().to_string();
    let start = store
        .submit("owner", launch(id.clone(), project), now)
        .unwrap();
    let agent = start.command.target.unwrap();
    store.dispatch(&id, now).unwrap();
    crate::tests::install_fixture_sources(store, &agent, &id, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: agent.clone(),
                generation: 1,
                source: "stream".into(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".into(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":id,"native_scope":"codex-test","native_ref":agent}),
                }],
                cursor: None,
            },
            now,
        )
        .unwrap();
    store.settle(&id, CommandState::Applied, None, now).unwrap();
    agent
}

fn claim_request() -> ProjectAssignmentClaimRequest {
    ProjectAssignmentClaimRequest {
        id: Ulid::new().to_string(),
        project: "alpha".into(),
        manager: None,
    }
}

fn release(store: &mut Store, epoch: i64, now: i64) -> ProjectAssignmentResult {
    store
        .release_project_manager(
            "owner",
            &ProjectAssignmentReleaseRequest {
                id: Ulid::new().to_string(),
                project: "alpha".into(),
                expected_epoch: epoch,
            },
            now,
        )
        .unwrap()
        .value
}

#[test]
fn concurrent_claims_have_one_winner_and_restart_keeps_the_binding() {
    let path = std::env::temp_dir().join(format!("swarm-project-assignment-{}", Ulid::new()));
    fs::create_dir(&path).unwrap();
    let db_path = path.join("eliot.db");
    let now = Ulid::new().timestamp_ms() as i64;
    let mut first = Store::open(&db_path).unwrap();
    register(&mut first, now);
    let first_agent = ready(&mut first, "alpha", now);
    let second_agent = ready(&mut first, "alpha", now);
    let second = Store::open(&db_path).unwrap();
    let barrier = Barrier::new(2);
    let [(first, first_retry), (second, second_retry)] = thread::scope(|scope| {
        let runners = [(first, first_agent), (second, second_agent)].map(|(mut store, agent)| {
            let barrier = &barrier;
            scope.spawn(move || {
                let principal = format!("agent:{agent}:1");
                let request = claim_request();
                barrier.wait();
                let initial = store
                    .claim_project_manager(&principal, &request, now)
                    .unwrap()
                    .value;
                let retry = store
                    .claim_project_manager(&principal, &request, now + 1)
                    .unwrap()
                    .value;
                (initial, retry)
            })
        });
        runners.map(|runner| runner.join().unwrap())
    });
    assert_ne!(first.outcome, second.outcome);
    assert!(matches!(
        first.outcome,
        ProjectAssignmentOutcome::Claimed | ProjectAssignmentOutcome::Occupied
    ));
    assert!(matches!(
        second.outcome,
        ProjectAssignmentOutcome::Claimed | ProjectAssignmentOutcome::Occupied
    ));
    assert_eq!(first.assignment, second.assignment);
    assert_eq!(first, first_retry);
    assert_eq!(second, second_retry);
    assert_eq!(first.assignment.epoch, 1);

    let mut restarted = Store::open(&db_path).unwrap();
    restarted.recover_on_restart(now + 2).unwrap();
    let current = restarted.project_assignment("owner", "alpha").unwrap();
    assert_eq!(current.manager, first.assignment.manager);
    assert_eq!(current.epoch, first.assignment.epoch);
    assert_eq!(current.revision, first.assignment.revision);
    drop(restarted);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn stale_admission_and_queued_dispatch_refuse_without_a_new_effect() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let agent = ready(&mut store, "alpha", now);
    let principal = format!("agent:{agent}:1");
    let claim = claim_request();
    let initial = store
        .claim_project_manager(&principal, &claim, now)
        .unwrap()
        .value;
    assert_eq!(initial.assignment.epoch, 1);

    let owner_launch = launch(Ulid::new().to_string(), "alpha");
    assert_eq!(
        store.submit("owner", owner_launch, now).unwrap_err().code,
        "assignment_occupied",
    );

    let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
    send.target = Some(agent.clone());
    send.expected_generation = Some(1);
    send.args = json!({"text": "queued", "assignment_epoch": 0});
    assert_eq!(
        store
            .submit(&principal, send.clone(), now)
            .unwrap_err()
            .code,
        "stale_assignment"
    );
    assert!(store.get_command(&send.id).unwrap().is_none());

    send.args["assignment_epoch"] = json!(1);
    store.submit(&principal, send.clone(), now).unwrap();
    let mut child = launch(Ulid::new().to_string(), "alpha");
    child.args["parent"] = json!(agent);
    let child_agent = store
        .submit(&principal, child.clone(), now)
        .unwrap()
        .command
        .target
        .unwrap();
    assert_eq!(
        release(&mut store, 1, now).outcome,
        ProjectAssignmentOutcome::Released
    );
    let retried = store
        .claim_project_manager(&principal, &claim, now)
        .unwrap()
        .value;
    assert_eq!(retried, initial);
    let replacement = store
        .claim_project_manager(&principal, &claim_request(), now)
        .unwrap()
        .value;
    assert_eq!(replacement.assignment.epoch, 3);

    assert_eq!(
        store.dispatch(&send.id, now).unwrap_err().code,
        "stale_assignment"
    );
    assert_eq!(
        store.dispatch(&child.id, now).unwrap_err().code,
        "stale_assignment"
    );
    assert_eq!(
        store.get_agent(&child_agent).unwrap().unwrap().generation,
        0
    );
    assert_eq!(store.get_agent(&agent).unwrap().unwrap().active_turn, None);
}

#[test]
fn manager_launch_reserves_atomically_and_legacy_launches_are_unchanged() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let request = managed_request("alpha");
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/alpha-1");
    let reserved = store
        .launch_project_manager(&request, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    assert_eq!(reserved.assignment.epoch, 1);
    assert_eq!(reserved.assignment.state, ProjectAssignmentState::Starting);
    assert_eq!(reserved.outcome, ProjectAssignmentOutcome::LaunchAdmitted);
    let manager = store
        .get_command(&request.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    assert_eq!(
        store
            .get_agent(&manager)
            .unwrap()
            .unwrap()
            .workspace
            .as_deref(),
        Some("/wt/alpha-1")
    );
    assert!(store.workspace_assignment_bound("/wt/alpha-1").unwrap());
    let competitor = ProjectAssignmentLaunchRequest {
        id: Ulid::new().to_string(),
        ..request.clone()
    };
    let (command, input) = managed_launch(competitor.id.clone(), "alpha", "/wt/alpha-2");
    let rejected = store
        .launch_project_manager(&competitor, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    assert_eq!(rejected.outcome, ProjectAssignmentOutcome::Occupied);
    assert_eq!(rejected.assignment, reserved.assignment);
    assert!(store.get_command(&competitor.id).unwrap().is_none());
    assert_eq!(
        store.replay_project_manager_launch(&request, now).unwrap(),
        Some(reserved)
    );

    assert_eq!(
        release(&mut store, 1, now).outcome,
        ProjectAssignmentOutcome::Released
    );
    assert_eq!(
        store.dispatch(&request.id, now).unwrap_err().code,
        "stale_assignment"
    );

    let agent = ready(&mut store, "legacy", now);
    let mut send = SubmitCommand::new(Ulid::new().to_string(), "send");
    send.target = Some(agent.clone());
    send.expected_generation = Some(1);
    send.args = json!({"text": "legacy"});
    store
        .submit(&format!("agent:{agent}:1"), send.clone(), now)
        .unwrap();
    assert_eq!(
        store.dispatch(&send.id, now).unwrap().value.state,
        CommandState::Dispatched
    );
}

#[test]
fn resume_rotates_the_epoch_and_uncertain_start_keeps_the_slot_occupied() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let agent = ready(&mut store, "alpha", now);
    let principal = format!("agent:{agent}:1");
    store
        .claim_project_manager(&principal, &claim_request(), now)
        .unwrap();

    crate::tests::install_live_custody_observations(&mut store, &agent, 1, now);
    let mut stop = SubmitCommand::new(Ulid::new().to_string(), "stop");
    stop.target = Some(agent.clone());
    store.submit("owner", stop.clone(), now).unwrap();
    store.dispatch(&stop.id, now).unwrap();
    store
        .settle(&stop.id, CommandState::Applied, None, now)
        .unwrap();
    crate::tests::simulate_normal_stop_proof(&mut store, &agent, 1, now);

    let mut resume = SubmitCommand::new(Ulid::new().to_string(), "resume");
    resume.target = Some(agent.clone());
    resume.expected_generation = Some(1);
    resume.args = json!({"text": "new linked input"});
    let admitted = store.submit("owner", resume.clone(), now).unwrap();
    assert_eq!(store.project_assignment("owner", "alpha").unwrap().epoch, 1);
    store.dispatch(&resume.id, now).unwrap();
    let current = store.project_assignment("owner", "alpha").unwrap();
    assert_eq!(current.epoch, 2);
    assert_eq!(
        current.manager,
        Some(ProjectManagerIdentity::Local {
            agent,
            generation: 2
        })
    );
    let linked = admitted.linked_command.unwrap();
    store.check_assignment_dispatch(&linked.id).unwrap();

    store
        .settle(&resume.id, CommandState::Unknown, None, now)
        .unwrap();
    assert_eq!(
        release(&mut store, 2, now).outcome,
        ProjectAssignmentOutcome::StartupUncertain
    );
    assert_eq!(
        store.project_assignment("owner", "alpha").unwrap().state,
        ProjectAssignmentState::Blocked
    );
    assert_eq!(
        store.dispatch(&resume.id, now).unwrap_err().code,
        "bad_state"
    );
}

#[test]
fn managed_launches_in_different_projects_get_distinct_worktrees() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    register_project(&mut store, now, "beta", "/managed/beta");
    let request_alpha = managed_request("alpha");
    let (command_alpha, input_alpha) =
        managed_launch(request_alpha.id.clone(), "alpha", "/wt/alpha-1");
    let admitted_alpha = store
        .launch_project_manager(&request_alpha, Ok((command_alpha, Some(input_alpha))), now)
        .unwrap()
        .value;
    assert_eq!(
        admitted_alpha.outcome,
        ProjectAssignmentOutcome::LaunchAdmitted
    );
    let request_beta = managed_request("beta");
    let (command_beta, input_beta) = managed_launch(request_beta.id.clone(), "beta", "/wt/beta-1");
    let admitted_beta = store
        .launch_project_manager(&request_beta, Ok((command_beta, Some(input_beta))), now)
        .unwrap()
        .value;
    assert_eq!(
        admitted_beta.outcome,
        ProjectAssignmentOutcome::LaunchAdmitted
    );
    let manager_alpha = store
        .get_command(&request_alpha.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    let manager_beta = store
        .get_command(&request_beta.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    assert_ne!(manager_alpha, manager_beta);
    let workspace_alpha = store
        .get_agent(&manager_alpha)
        .unwrap()
        .unwrap()
        .workspace
        .unwrap();
    let workspace_beta = store
        .get_agent(&manager_beta)
        .unwrap()
        .unwrap()
        .workspace
        .unwrap();
    assert_eq!(workspace_alpha, "/wt/alpha-1");
    assert_eq!(workspace_beta, "/wt/beta-1");
    assert!(store.workspace_assignment_bound("/wt/alpha-1").unwrap());
    assert!(store.workspace_assignment_bound("/wt/beta-1").unwrap());
    assert!(!store.workspace_assignment_bound("/wt/nowhere").unwrap());
}

#[test]
fn worker_inherits_assignment_and_workspace() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let request = managed_request("alpha");
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/manager");
    let admitted = store
        .launch_project_manager(&request, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    assert_eq!(admitted.outcome, ProjectAssignmentOutcome::LaunchAdmitted);
    let manager = store
        .get_command(&request.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    mark_worktree_ready(&store, parse_agent_id(&manager).unwrap());
    store.dispatch(&request.id, now).unwrap();
    // The manager must reach a live state before it can parent workers, just
    // as the daemon only dispatches children after the native harness is up.
    crate::tests::install_fixture_sources(&mut store, &manager, &request.id, 1, now);
    store
        .put_batch(
            SourceBatch {
                batch_id: Ulid::new().to_string(),
                agent_id: manager.clone(),
                generation: 1,
                source: "stream".into(),
                events: vec![NativeEvent {
                    ts: None,
                    kind: "session.ready".into(),
                    native_id: Ulid::new().to_string(),
                    data: json!({"command_id":request.id,"native_scope":"codex-test","native_ref":manager}),
                }],
                cursor: None,
            },
            now,
        )
        .unwrap();
    store
        .settle(&request.id, CommandState::Applied, None, now)
        .unwrap();
    let mut child = launch(Ulid::new().to_string(), "alpha");
    child.args["parent"] = json!(manager);
    // The Store also inherits the parent's project without a router hint.
    child.args["workspace"] = json!("/wt/manager");
    let principal = format!("agent:{manager}:1");
    let worker = store
        .submit(&principal, child, now)
        .unwrap()
        .command
        .target
        .unwrap();
    let worker_row = store.get_agent(&worker).unwrap().unwrap();
    let manager_row = store.get_agent(&manager).unwrap().unwrap();
    assert_eq!(worker_row.project, "alpha");
    assert_eq!(worker_row.workspace, manager_row.workspace);
    assert_eq!(worker_row.workspace.as_deref(), Some("/wt/manager"));
    assert_eq!(worker_row.parent.as_deref(), Some(manager.as_str()));
    assert_eq!(worker_row.root, manager_row.root);
    let artifact = core::api::ArtifactView {
        id: "worktree-test".into(),
        agent: manager,
        generation: 1,
        project: "alpha".into(),
        tree: Vec::new(),
        path: "/wt/manager".into(),
        kind: core::api::ArtifactKind::Worktree,
        purpose: "managed worktree".into(),
        identity: "test".into(),
        created_at: now,
        state: "registered".into(),
        git: None,
    };
    assert_eq!(
        store.cleanup_worktree_blocker(&artifact).unwrap(),
        Some("assignment_bound")
    );
    assert_eq!(
        release(&mut store, 1, now + 1).outcome,
        ProjectAssignmentOutcome::Released
    );
    assert_eq!(
        store.cleanup_worktree_blocker(&artifact).unwrap(),
        Some("workspace_borrowed")
    );
}

#[test]
fn same_id_retry_after_restart_creates_no_duplicate() {
    let path = std::env::temp_dir().join(format!("swarm-project-assignment-2b-{}", Ulid::new()));
    fs::create_dir(&path).unwrap();
    let db_path = path.join("eliot.db");
    let now = Ulid::new().timestamp_ms() as i64;
    let mut first = Store::open(&db_path).unwrap();
    register(&mut first, now);
    let request = managed_request("alpha");
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/alpha-1");
    let initial = first
        .launch_project_manager(&request, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    assert_eq!(initial.outcome, ProjectAssignmentOutcome::LaunchAdmitted);
    drop(first);
    let mut restarted = Store::open(&db_path).unwrap();
    restarted.recover_on_restart(now + 1).unwrap();
    let recovered = restarted.project_assignment("owner", "alpha").unwrap();
    assert_eq!(recovered.state, ProjectAssignmentState::Blocked);
    assert_eq!(
        recovered.start_command_id.as_deref(),
        Some(request.id.as_str())
    );
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/alpha-1");
    let retried = restarted
        .launch_project_manager(&request, Ok((command, Some(input))), now + 1)
        .unwrap()
        .value;
    assert_eq!(retried, initial);
    let agents: i64 = restarted
        .connection
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE project = 'alpha'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(agents, 1);
    let workspaces: i64 = restarted
        .connection
        .query_row("SELECT COUNT(*) FROM workspaces", [], |row| row.get(0))
        .unwrap();
    assert_eq!(workspaces, 1);
    drop(restarted);
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn pre_admission_refusal_does_not_reserve_assignment() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let count_agents = |store: &Store| {
        store
            .connection
            .query_row("SELECT COUNT(*) FROM agents", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
    };
    let count_workspaces = |store: &Store| {
        store
            .connection
            .query_row("SELECT COUNT(*) FROM workspaces", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap()
    };
    // A preflight error leaves no admitted assignment, Agent, or workspace.
    let request = managed_request("alpha");
    let failed: Result<_> = store.launch_project_manager(
        &request,
        Err(Error::new("unavailable", "frozen git is down")),
        now,
    );
    assert_eq!(failed.unwrap_err().code, "unavailable");
    let vacant = store.project_assignment("owner", "alpha").unwrap();
    assert_eq!(vacant.manager, None);
    assert_eq!(vacant.state, ProjectAssignmentState::Vacant);
    assert_eq!(count_agents(&store), 0);
    assert_eq!(count_workspaces(&store), 0);
    // The same ID stays reusable after the unknown failure.
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/alpha-1");
    let admitted = store
        .launch_project_manager(&request, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    assert_eq!(admitted.outcome, ProjectAssignmentOutcome::LaunchAdmitted);
    // A refused competitor sees the occupation and persists nothing.
    let competitor = managed_request("alpha");
    let (command, input) = managed_launch(competitor.id.clone(), "alpha", "/wt/alpha-2");
    let refused = store
        .launch_project_manager(&competitor, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    assert_eq!(refused.outcome, ProjectAssignmentOutcome::Occupied);
    assert!(store.get_command(&competitor.id).unwrap().is_none());
    assert_eq!(count_agents(&store), 1);
    assert_eq!(count_workspaces(&store), 1);
}

#[test]
fn failed_preparation_settles_launch_and_clears_assignment() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let request = managed_request("alpha");
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/failed");
    let admitted = store
        .launch_project_manager(&request, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    let manager = store
        .get_command(&request.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    let manager_num = parse_agent_id(&manager).unwrap();
    assert_eq!(reserved_generation(&store, manager_num), Some(1));
    assert!(store.workspace_assignment_bound("/wt/failed").unwrap());

    let failed = store
        .settle_workspace_creation(
            &request.id,
            1,
            core::WorkspaceCreationOutcome::Failed {
                code: "git_exit".into(),
                message: "worktree was not created".into(),
                failure_where: None,
                origin: None,
                origin_evidence: None,
            },
            now + 1,
        )
        .unwrap()
        .value;
    assert_eq!(failed.phase, core::WorkspacePhase::Failed);
    assert_eq!(
        store.get_command(&request.id).unwrap().unwrap().state,
        CommandState::Failed
    );
    assert_eq!(
        store.get_agent(&manager).unwrap().unwrap().state,
        AgentState::Failed
    );
    assert_eq!(reserved_generation(&store, manager_num), None);
    let vacant = store.project_assignment("owner", "alpha").unwrap();
    assert_eq!(vacant.state, ProjectAssignmentState::Vacant);
    assert_eq!(vacant.epoch, admitted.assignment.epoch + 1);
    assert_eq!(vacant.manager, None);
    assert_eq!(vacant.workspace, None);
    assert_eq!(vacant.start_command_id, None);
    assert!(!store.workspace_assignment_bound("/wt/failed").unwrap());
    assert_eq!(
        release(&mut store, vacant.epoch, now + 2).outcome,
        ProjectAssignmentOutcome::Vacant
    );
    assert_eq!(
        store
            .replay_project_manager_launch(&request, now + 3)
            .unwrap(),
        Some(admitted)
    );
    let replacement = managed_request("alpha");
    let (command, input) = managed_launch(replacement.id.clone(), "alpha", "/wt/replacement");
    assert_eq!(
        store
            .launch_project_manager(&replacement, Ok((command, Some(input))), now + 3)
            .unwrap()
            .value
            .outcome,
        ProjectAssignmentOutcome::LaunchAdmitted
    );
}

#[test]
fn unknown_preparation_is_readable_and_explicit_release_fences_readback() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let request = managed_request("alpha");
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/unknown");
    let verified = core::WorkspaceCreationOutcome::Verified {
        repo_path: input.repo_path.clone(),
        repo_common_dir: input.repo_common_dir.clone(),
        worktree_path: input.worktree_path.clone(),
        branch: input.requested_branch.clone(),
        head: input.base_oid.clone(),
    };
    let admitted = store
        .launch_project_manager(&request, Ok((command, Some(input))), now)
        .unwrap()
        .value;
    let manager = store
        .get_command(&request.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    let manager_num = parse_agent_id(&manager).unwrap();
    let unknown = store
        .settle_workspace_creation(
            &request.id,
            1,
            core::WorkspaceCreationOutcome::Unknown {
                code: "git_unknown".into(),
                message: "no verified worktree result".into(),
                failure_where: None,
                origin: None,
                origin_evidence: None,
            },
            now + 1,
        )
        .unwrap()
        .value;
    assert_eq!(unknown.phase, core::WorkspacePhase::Unknown);
    let blocked = store.project_assignment("owner", "alpha").unwrap();
    assert_eq!(blocked.state, ProjectAssignmentState::Blocked);
    assert_eq!(blocked.epoch, admitted.assignment.epoch);
    assert_eq!(blocked.workspace.as_deref(), Some("/wt/unknown"));
    assert_eq!(
        blocked.start_command_id.as_deref(),
        Some(request.id.as_str())
    );
    assert!(store.workspace_assignment_bound("/wt/unknown").unwrap());
    assert_eq!(
        store.get_command(&request.id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(reserved_generation(&store, manager_num), Some(1));
    let released = release(&mut store, blocked.epoch, now + 2);
    assert_eq!(released.outcome, ProjectAssignmentOutcome::Released);
    assert_eq!(released.assignment.state, ProjectAssignmentState::Vacant);
    assert_eq!(released.assignment.workspace, None);
    assert!(!store.workspace_assignment_bound("/wt/unknown").unwrap());
    assert_eq!(
        store
            .settle_workspace_creation(&request.id, 1, verified, now + 3)
            .unwrap_err()
            .code,
        "stale_assignment"
    );
    assert_eq!(
        store.get_workspace(&manager).unwrap().unwrap().phase,
        core::WorkspacePhase::Unknown
    );
    assert_eq!(
        store.get_command(&request.id).unwrap().unwrap().state,
        CommandState::Unknown
    );
    assert_eq!(reserved_generation(&store, manager_num), Some(1));
    assert_eq!(
        store
            .replay_project_manager_launch(&request, now + 3)
            .unwrap(),
        Some(admitted)
    );
}

#[test]
fn bound_worktree_refuses_removal_until_release() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let request = managed_request("alpha");
    let (command, input) = managed_launch(request.id.clone(), "alpha", "/wt/manager");
    store
        .launch_project_manager(&request, Ok((command, Some(input))), now)
        .unwrap();
    let manager = store
        .get_command(&request.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    let mut remove = SubmitCommand::new(Ulid::new().to_string(), "worktree.remove");
    remove.target = Some(manager.clone());
    assert_eq!(store.submit("owner", remove, now).unwrap_err().code, "busy");
    assert_eq!(
        release(&mut store, 1, now).outcome,
        ProjectAssignmentOutcome::Released
    );
    assert!(!store.workspace_assignment_bound("/wt/manager").unwrap());
    // The assignment gate lifted; the pre-existing terminal-owner rule still
    // guards removal of the unready created worktree.
    let mut remove = SubmitCommand::new(Ulid::new().to_string(), "worktree.remove");
    remove.target = Some(manager);
    assert_eq!(
        store.submit("owner", remove, now).unwrap_err().code,
        "bad_state"
    );
}

#[test]
fn explicit_adoption_binds_without_a_worktree_row() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    register_project(&mut store, now, "beta", "/managed/beta");
    let mut request = managed_request("alpha");
    request.adopt_worktree = Some("/adopted/alpha".into());
    let mut command = launch(request.id.clone(), "alpha");
    command.args["project"] = json!("/adopted/alpha");
    command.args["workspace"] = json!("/adopted/alpha");
    let admitted = store
        .launch_project_manager(&request, Ok((command, None)), now)
        .unwrap()
        .value;
    assert_eq!(admitted.outcome, ProjectAssignmentOutcome::LaunchAdmitted);
    let manager = store
        .get_command(&request.id)
        .unwrap()
        .unwrap()
        .target
        .unwrap();
    assert_eq!(
        store
            .get_agent(&manager)
            .unwrap()
            .unwrap()
            .workspace
            .as_deref(),
        Some("/adopted/alpha")
    );
    // Adoption records only the binding: no managed worktree row exists, so
    // cleanup can never list the human checkout as a removable artifact.
    assert!(store.get_workspace(&manager).unwrap().is_none());
    assert!(store.workspace_assignment_bound("/adopted/alpha").unwrap());
    // Another project cannot bind the same checkout, adopted or prepared.
    let mut competitor = managed_request("beta");
    competitor.adopt_worktree = Some("/adopted/alpha".into());
    let mut command = launch(competitor.id.clone(), "beta");
    command.args["project"] = json!("/adopted/alpha");
    command.args["workspace"] = json!("/adopted/alpha");
    assert_eq!(
        store
            .launch_project_manager(&competitor, Ok((command, None)), now)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    assert!(store.get_command(&competitor.id).unwrap().is_none());
}

#[test]
fn adoption_refuses_overlapping_unmanaged_agent_workspaces() {
    let now = Ulid::new().timestamp_ms() as i64;
    let mut store = Store::open(Path::new(":memory:")).unwrap();
    register(&mut store, now);
    let mut writer = launch(Ulid::new().to_string(), "/adopted/alpha/subdirectory");
    writer.args["workspace"] = json!("/adopted/alpha/subdirectory");
    store.submit("owner", writer, now).unwrap();

    let mut request = managed_request("alpha");
    request.adopt_worktree = Some("/adopted/alpha".into());
    let mut command = launch(request.id.clone(), "/adopted/alpha");
    command.args["workspace"] = json!("/adopted/alpha");
    assert_eq!(
        store
            .launch_project_manager(&request, Ok((command, None)), now)
            .unwrap_err()
            .code,
        "id_conflict"
    );
    assert!(store.get_command(&request.id).unwrap().is_none());
    assert_eq!(
        store.project_assignment("owner", "alpha").unwrap().state,
        ProjectAssignmentState::Vacant
    );
}
