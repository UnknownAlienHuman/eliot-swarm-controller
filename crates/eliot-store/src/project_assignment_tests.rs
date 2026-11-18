use super::*;
use std::{fs, path::Path, sync::Barrier, thread};

fn register(store: &mut Store, now: i64) {
    store
        .upsert_project_registration(
            "owner",
            &ProjectRegistrationInput {
                id: "alpha".into(),
                checkout: "/managed/alpha".into(),
                repo_common_dir: "/managed/alpha/.git".into(),
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
    let request = ProjectAssignmentLaunchRequest {
        id: Ulid::new().to_string(),
        project: "alpha".into(),
        profile: "approved".into(),
    };
    let prepared = launch(request.id.clone(), "alpha");
    let reserved = store
        .launch_project_manager(&request, Ok(prepared), now)
        .unwrap()
        .value;
    assert_eq!(reserved.assignment.epoch, 1);
    assert_eq!(reserved.assignment.state, ProjectAssignmentState::Starting);
    let competitor = ProjectAssignmentLaunchRequest {
        id: Ulid::new().to_string(),
        ..request.clone()
    };
    let rejected = store
        .launch_project_manager(&competitor, Ok(launch(competitor.id.clone(), "alpha")), now)
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
