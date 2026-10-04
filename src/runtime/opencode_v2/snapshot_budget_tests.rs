use crate::runtime::opencode_v2::{
    root_id,
    tests::{Fixture, Reply, child_events},
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, time::Duration};

#[tokio::test]
async fn optional_snapshot_budget_retains_completed_evidence_and_marks_unavailable_axes() {
    let fixture = Fixture::new().await;
    let service = fixture.service().await;
    let open = fixture.open(&service).await;
    let root = root_id(&open.binding_id, open.generation);
    let child = "ses_snapshot_budget_child";
    let form = json!({
        "id":"frm_snapshot_budget_pending",
        "sessionID":root,
        "title":"pending-secret-marker-7319",
        "fields":[]
    });
    let permission = json!({
        "id":"per_snapshot_budget_pending",
        "sessionID":root,
        "action":"shell",
        "resources":["fixture"]
    });
    {
        let mut world = fixture.world.lock().unwrap();
        world.sessions.insert(
            child.into(),
            json!({
                "id":child,
                "parentID":root,
                "projectID":"prj_fixture",
                "time":{"created":1,"updated":2}
            }),
        );
        world.logs.insert(
            child.into(),
            child_events(child, &root, &[("budget_child_run", Some("completed"))]),
        );
        world.forms.insert(root.clone(), vec![form]);
        world.permissions.insert(root.clone(), vec![permission]);
    }

    let bound = BTreeSet::from([child.to_owned()]);
    let prior = service
        .snapshot(&root, &Value::Null, &bound)
        .await
        .expect("healthy baseline snapshot");
    assert_eq!(prior.state["configuration"]["complete"], true);
    assert_eq!(prior.state["agent_configuration"]["complete"], true);
    assert_eq!(prior.state["model_configuration"]["complete"], true);
    let old_child = prior.state["observed_children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sessionId"] == child)
        .unwrap()
        .clone();
    assert_eq!(old_child["last_turn"]["terminal"], "completed");
    assert!(old_child["execution_scan"].is_object());
    assert!(
        prior.state["pending_requests"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["kind"] == "form")
    );
    assert!(
        prior.state["pending_requests"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["kind"] == "permission")
    );

    fixture.override_get("/api/model", Reply::Sse(String::new(), true));
    fixture.override_get(
        &format!("/api/session/{root}/form"),
        Reply::Sse(String::new(), true),
    );
    fixture.override_get(
        &format!("/api/session/{root}/permission"),
        Reply::Sse(String::new(), true),
    );
    fixture.override_get(
        &format!("/api/experimental/session/{child}/log"),
        Reply::Sse(String::new(), true),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        service.snapshot_with_budget(&root, &prior.state, &bound, Duration::from_millis(250)),
    )
    .await
    .expect("optional reads are bounded")
    .expect("an optional axis failure still yields a snapshot");

    let state = result.state;
    assert_eq!(state["session"]["sessionId"], root);
    assert_eq!(state["enumeration_complete"], true);
    assert_eq!(state["family_coverage"]["members_observed_now"], 1);
    assert_eq!(state["configuration"]["complete"], true);
    assert_eq!(state["agent_configuration"]["complete"], true);
    assert_eq!(state["model_configuration"]["complete"], false);
    assert_eq!(state["family_completeness"], "partial");

    let requests = state["pending_requests"].as_array().unwrap();
    for (kind, request_id) in [
        ("form", "frm_snapshot_budget_pending"),
        ("permission", "per_snapshot_budget_pending"),
    ] {
        let retained = requests
            .iter()
            .find(|p| p["kind"] == kind && p["request_id"] == request_id)
            .unwrap();
        assert_eq!(retained["observed_now"], false);
    }
    let current_child = state["observed_children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sessionId"] == child)
        .unwrap();
    assert_eq!(current_child["last_turn"], old_child["last_turn"]);
    assert_eq!(current_child["execution_scan"], old_child["execution_scan"]);
    assert_eq!(
        current_child["execution_disposition"],
        old_child["execution_disposition"]
    );

    let failures = state["failures"].as_array().unwrap();
    for source in ["session_model", "form", "permission", "child_execution_log"] {
        assert!(
            failures.iter().any(|failure| {
                failure["source"] == source && failure["code"] == "NATIVE_SNAPSHOT_BUDGET_EXHAUSTED"
            }),
            "missing safe budget failure for {source}: {failures:?}"
        );
    }
    assert_eq!(
        state["gaps"].as_u64().unwrap(),
        prior.state["gaps"].as_u64().unwrap().saturating_add(1)
    );
    assert!(
        !serde_json::to_string(failures)
            .unwrap()
            .contains("pending-secret-marker-7319")
    );
}

#[tokio::test]
async fn delayed_model_catalog_does_not_starve_child_execution_readback() {
    let fixture = Fixture::new().await;
    let service = fixture.service().await;
    let open = fixture.open(&service).await;
    let root = root_id(&open.binding_id, open.generation);
    let child = "ses_snapshot_fairness_child";
    {
        let mut world = fixture.world.lock().unwrap();
        world.sessions.insert(
            child.into(),
            json!({
                "id":child,
                "parentID":root,
                "projectID":"prj_fixture",
                "time":{"created":1,"updated":2}
            }),
        );
        world.logs.insert(
            child.into(),
            child_events(child, &root, &[("fairness_first_run", Some("completed"))]),
        );
    }

    let bound = BTreeSet::from([child.to_owned()]);
    let prior = service
        .snapshot(&root, &Value::Null, &bound)
        .await
        .expect("healthy baseline snapshot");
    let old_child = prior.state["observed_children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sessionId"] == child)
        .unwrap();
    assert_eq!(
        old_child["last_turn"]["turnId"],
        "evt_fairness_first_run_started"
    );
    assert_eq!(old_child["last_turn"]["terminal"], "completed");

    fixture.override_get("/api/model", Reply::Sse(String::new(), true));
    fixture.world.lock().unwrap().logs.insert(
        child.into(),
        child_events(
            child,
            &root,
            &[
                ("fairness_first_run", Some("completed")),
                ("fairness_second_run", Some("completed")),
            ],
        ),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        service.snapshot_with_budget(&root, &prior.state, &bound, Duration::from_millis(250)),
    )
    .await
    .expect("model catalog timeout does not hold the snapshot")
    .expect("a delayed optional axis still yields a snapshot");
    let state = result.state;
    assert_eq!(state["model_configuration"]["complete"], false);
    assert_eq!(state["configuration"]["complete"], true);
    assert_eq!(state["agent_configuration"]["complete"], true);

    let current_child = state["observed_children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sessionId"] == child)
        .unwrap();
    assert_eq!(
        current_child["last_turn"]["turnId"],
        "evt_fairness_second_run_started"
    );
    assert_eq!(current_child["last_turn"]["terminal"], "completed");
    assert_eq!(current_child["execution_disposition"], "completed");

    let failures = state["failures"].as_array().unwrap();
    assert!(failures.iter().any(|failure| {
        failure["source"] == "session_model"
            && failure["code"] == "NATIVE_SNAPSHOT_BUDGET_EXHAUSTED"
    }));
    assert!(
        !failures
            .iter()
            .any(|failure| failure["source"] == "child_execution_log")
    );
}

#[tokio::test]
async fn delayed_child_log_does_not_starve_new_pending_questions() {
    let fixture = Fixture::new().await;
    let service = fixture.service().await;
    let open = fixture.open(&service).await;
    let root = root_id(&open.binding_id, open.generation);
    let child = "ses_snapshot_pending_fairness_child";
    {
        let mut world = fixture.world.lock().unwrap();
        world.sessions.insert(
            child.into(),
            json!({
                "id":child,
                "parentID":root,
                "projectID":"prj_fixture",
                "time":{"created":1,"updated":2}
            }),
        );
        world.logs.insert(
            child.into(),
            child_events(child, &root, &[("pending_fairness_run", Some("completed"))]),
        );
    }

    let bound = BTreeSet::from([child.to_owned()]);
    let prior = service
        .snapshot(&root, &Value::Null, &bound)
        .await
        .expect("healthy baseline snapshot");
    let old_child = prior.state["observed_children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sessionId"] == child)
        .unwrap()
        .clone();
    assert_eq!(old_child["last_turn"]["terminal"], "completed");
    assert!(
        prior.state["pending_requests"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let form = json!({
        "id":"frm_fresh_after_snapshot",
        "sessionID":root,
        "title":"new form request",
        "fields":[]
    });
    let permission = json!({
        "id":"per_fresh_after_snapshot",
        "sessionID":root,
        "action":"shell",
        "resources":["fixture"]
    });
    {
        let mut world = fixture.world.lock().unwrap();
        world.forms.insert(root.clone(), vec![form]);
        world.permissions.insert(root.clone(), vec![permission]);
    }
    fixture.override_get(
        &format!("/api/experimental/session/{child}/log"),
        Reply::Sse(String::new(), true),
    );

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        service.snapshot_with_budget(&root, &prior.state, &bound, Duration::from_millis(250)),
    )
    .await
    .expect("child log timeout does not hold pending-request reads")
    .expect("a delayed child log still yields a snapshot");
    let state = result.state;

    let requests = state["pending_requests"].as_array().unwrap();
    for (kind, request_id) in [
        ("form", "frm_fresh_after_snapshot"),
        ("permission", "per_fresh_after_snapshot"),
    ] {
        let current = requests
            .iter()
            .find(|request| request["kind"] == kind && request["request_id"] == request_id)
            .unwrap();
        assert_eq!(current["observed_now"], true);
    }

    let current_child = state["observed_children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["sessionId"] == child)
        .unwrap();
    assert_eq!(current_child["last_turn"], old_child["last_turn"]);
    assert_eq!(current_child["execution_scan"], old_child["execution_scan"]);
    let failures = state["failures"].as_array().unwrap();
    assert!(failures.iter().any(|failure| {
        failure["source"] == "child_execution_log"
            && failure["code"] == "NATIVE_SNAPSHOT_BUDGET_EXHAUSTED"
    }));
}
