use super::*;
use eliot_core::api::{OverviewCoverage, ProjectOverviewRequest, ScopeLimits};
use rusqlite::params;
use std::path::Path;

fn store() -> Store {
    Store::open(Path::new(":memory:")).expect("open overview fixture")
}

fn agent(store: &Store, id: i64, project: &str, state: &str, observe_only: bool) {
    store.connection.execute(
        "INSERT INTO agents(id,generation,project,harness,profile,token,state,created_at,updated_at,
         observe_only,control,capacity_generation)
         VALUES(?1,1,?2,'codex','fixture',CASE WHEN ?4=0 THEN 'token-' || ?1 END,
                ?3,1,1,?4,'project',CASE WHEN ?4=0 THEN 1 END)",
        params![id, project, state, observe_only as i64],
    ).expect("insert Agent");
    store
        .connection
        .execute("UPDATE agents SET root=id WHERE id=?1", [id])
        .expect("set root");
}

fn command(store: &Store, id: &str, target: i64, verb: &str, state: &str) {
    store.connection.execute(
        "INSERT INTO commands(id,principal,method,payload_digest,target,generation,verb,args,state,created_at,updated_at)
         VALUES(?1,'owner','command.submit',zeroblob(32),?2,1,?3,'{}',?4,1,1)",
        params![id, target, verb, state],
    ).expect("insert command");
}

#[test]
fn detail_access_fences_workers_and_released_managers() {
    let store = store();
    agent(&store, 1, "alpha", "idle", false);
    agent(&store, 2, "alpha", "idle", false);
    agent(&store, 3, "beta", "idle", false);
    agent(&store, 4, "beta", "unknown", true);
    store
        .connection
        .execute_batch(
            "UPDATE agents SET root=1,parent=1,depth=1 WHERE id=2;
         UPDATE agents SET root=3,parent=3,depth=1 WHERE id=4;
         INSERT INTO project_registrations(id,checkout,repo_common_dir,profiles,actions)
         VALUES('alpha','C:/alpha','C:/alpha/.git','[]','[]'),
               ('beta','C:/beta','C:/beta/.git','[]','[]');
         INSERT INTO project_assignments(project,manager,manager_generation,epoch,revision)
         VALUES('alpha',1,1,1,1),('beta',3,1,1,1);
         INSERT INTO native_child_registrations
         (producer,producer_generation,parent,parent_generation,registration_id,child,request,ack)
         VALUES(3,1,3,1,'foreign',4,'{}','{}');",
        )
        .unwrap();
    let overview = |principal| {
        store
            .project_overview(
                &ScopeLimits::default(),
                &ProjectOverviewRequest::default(),
                principal,
                1,
            )
            .unwrap()
    };
    let manager = overview("agent:a1:1");
    assert_eq!(manager.projects[0].workers[0].agent, "a2");
    assert!(manager.projects[0].assignment.is_some());
    assert!(!manager.projects[1].detail);
    assert!(manager.projects[1].manager.is_none());
    assert!(manager.projects[1].assignment.is_none());
    assert_eq!(manager.projects[1].native_child_total, 1);
    assert_eq!(manager.fleet.native_child_total, 1);
    assert!(manager.fleet.native_children.is_empty());
    assert!(!overview("agent:a2:1").projects[0].detail);
    store
        .connection
        .execute("UPDATE agents SET control='self' WHERE id=1", [])
        .unwrap();
    assert!(overview("agent:a1:1").projects[0].workers.is_empty());
    store.connection.execute("UPDATE project_assignments SET manager=NULL, manager_generation=NULL WHERE project='alpha'", []).unwrap();
    assert!(!overview("agent:a1:1").projects[0].detail);
}

#[test]
fn no_project_native_child_is_wire_level_fleet_observation_only() {
    let store = store();
    agent(&store, 1, "", "idle", false);
    agent(&store, 2, "", "unknown", true);
    store.connection.execute("INSERT INTO native_child_registrations(producer,producer_generation,parent,parent_generation,registration_id,child,request,ack) VALUES(1,1,1,1,'r1',2,'{}','{}')", []).unwrap();
    let response = store
        .project_overview(
            &ScopeLimits::default(),
            &ProjectOverviewRequest {
                project: None,
                after: None,
            },
            "owner",
            1,
        )
        .unwrap();
    assert_eq!(response.fleet.native_child_total, 1);
    assert_eq!(response.fleet.native_children.len(), 1);
    assert_eq!(response.fleet.native_children[0].project, None);
    assert!(response.projects.is_empty());
    assert!(
        serde_json::to_value(&response).unwrap()["fleet"]["native_children"][0]["project"]
            .is_null()
    );
}

#[test]
fn filtered_totals_include_empty_registered_projects_and_watermark() {
    let store = store();
    agent(&store, 1, "alpha", "idle", false);
    agent(&store, 2, "beta", "idle", false);
    store.connection.execute("INSERT INTO project_registrations(id,checkout,repo_common_dir,profiles,actions) VALUES('empty','C:/empty','C:/empty/.git','[]','[]')", []).unwrap();
    store
        .connection
        .execute(
            "INSERT INTO events(ts,kind,source,data) VALUES(10,'agent.ready','daemon','{}')",
            [],
        )
        .unwrap();
    let (fleet, coverage) = read_fleet(&store.connection, &ScopeLimits::default()).unwrap();
    assert_eq!(fleet.projects, 3);
    assert_eq!(coverage, OverviewCoverage::Partial);
    let response = store
        .project_overview(
            &ScopeLimits::default(),
            &ProjectOverviewRequest {
                project: Some("empty".into()),
                after: None,
            },
            "owner",
            11,
        )
        .unwrap();
    assert_eq!(response.fleet.projects, 3);
    assert_eq!(response.projects.len(), 1);
    assert_eq!(response.projects[0].project, "empty");
    assert!(response.event_watermark >= 1);
}

#[test]
fn project_cursor_reaches_every_large_team_worker_and_workspace_under_frame_bound() {
    let store = store();
    agent(&store, 1, "alpha", "idle", false);
    store.connection.execute("INSERT INTO project_assignments(project,manager,manager_generation,epoch,revision) VALUES('alpha',1,1,1,1)", []).unwrap();
    let path = "C:/".to_owned() + &"x".repeat(1_100);
    for id in 2..=1_005 {
        agent(&store, id, "alpha", "idle", false);
        let cid = format!("workspace-{id}");
        command(&store, &cid, id, "launch", "applied");
        store.connection.execute("INSERT INTO workspaces(agent,target_generation,start_command_id,project,repo_path,repo_common_dir,requested_branch,base_oid,worktree_path,frozen_setup,phase,updated_at) VALUES(?1,1,?2,'alpha',?3,?3,'main','oid',?4,'{}','ready',1)", params![id, cid, path, format!("{path}-{id}")]).unwrap();
    }
    let mut after = None;
    let mut workers = std::collections::BTreeSet::new();
    let mut workspaces = std::collections::BTreeSet::new();
    for _ in 0..20 {
        let response = store
            .project_overview(
                &ScopeLimits::default(),
                &ProjectOverviewRequest {
                    project: None,
                    after: after.clone(),
                },
                "owner",
                1,
            )
            .unwrap();
        assert!(serde_json::to_vec(&response).unwrap().len() < 1_536 * 1024);
        for project in &response.projects {
            for worker in &project.workers {
                assert!(workers.insert(worker.agent.clone()));
            }
            for workspace in &project.workspaces {
                assert!(workspaces.insert(workspace.agent.clone()));
            }
        }
        after = response.next_cursor;
        if after.is_none() {
            break;
        }
    }
    assert!(after.is_none());
    assert_eq!(workers.len(), 1_004);
    assert_eq!(workspaces.len(), 1_004);
}

#[test]
fn dispatched_start_is_starting_then_unknown_and_not_queued() {
    let store = store();
    agent(&store, 1, "alpha", "starting", false);
    command(&store, "start", 1, "launch", "dispatched");
    let starting = lifecycle(&store.connection, Some("alpha")).unwrap();
    assert_eq!(starting.starting, 1);
    assert_eq!(starting.queued, 0);
    store.connection.execute("UPDATE commands SET state='unknown', outcome='{\"native_dispatched\":0}' WHERE id='start'", []).unwrap();
    let lifecycle = lifecycle(&store.connection, Some("alpha")).unwrap();
    assert_eq!(lifecycle.starting, 0);
    assert_eq!(lifecycle.retained_unknown, 1);
    assert_eq!(lifecycle.queued, 0);
}
