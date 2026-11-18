//! Project overview routing and its principal boundary.
use super::*;
use eliot_core::api::{ProjectOverviewRequest, discovery::decode_params};

impl Router {
    pub(super) async fn project_overview(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectOverviewRequest = decode_params(params.clone())?;
        request.validate()?;
        if !matches!(principal, Principal::Owner | Principal::Agent { .. }) {
            return Err(Error::new(
                "not_allowed",
                "project overview requires Owner or Agent authority",
            ));
        }
        let store = self.inner.store.lock().await;
        let limits = crate::config::load_scope_limits(&self.inner.data_root)?;
        let response =
            store.project_overview(&limits, &request, &principal_name(principal), now_ms())?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "project overview could not be encoded"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::Bus;
    use eliot_core::api::{OverviewCoverage, ProjectOverviewResponse};
    use eliot_core::{NativeEvent, SourceBatch, SubmitCommand};
    use tokio::sync::Mutex as AsyncMutex;

    const CAPACITY_SOURCE: &str = "[capacity]\nglobal_max_live=5\n[capacity.projects.team-a]\nmax_live=2\nmanager_max_live=3\n";

    fn overview_router() -> (Router, std::path::PathBuf) {
        let root =
            std::env::temp_dir().join(format!("eliot-overview-router-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("config.toml"), CAPACITY_SOURCE).unwrap();
        let store = eliot_store::Store::open(&root.join("eliot.db")).unwrap();
        let store = std::sync::Arc::new(AsyncMutex::new(store));
        let router = Router::new(root.clone(), "owner-token".to_owned(), store, Bus::new(16));
        (router, root)
    }

    async fn launch_idle(router: &Router, project: &str, parent: Option<&str>, now: i64) -> String {
        let store_handle = std::sync::Arc::clone(&router.inner.store);
        let mut store = store_handle.lock().await;
        let launch_id = ulid::Ulid::new().to_string();
        let mut args = json!({
            "project": project,
            "workspace": "synthetic-workspace",
            "harness": "codex",
            "profile": "fixture",
            "profile_snapshot": {"control":"tree"},
        });
        if let Some(parent) = parent {
            args["parent"] = json!(parent);
        }
        let admission = store
            .submit(
                "owner",
                SubmitCommand {
                    id: launch_id.clone(),
                    target: None,
                    selector: None,
                    verb: "launch".to_owned(),
                    args,
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                    probe: false,
                },
                now,
            )
            .expect("admit launch");
        let agent = admission.command.target.expect("launch target");
        store.dispatch(&launch_id, now).expect("dispatch launch");
        crate::test_support::install_fixture_sources(&mut store, &agent, &launch_id, 1, now);
        store
            .put_batch(
                SourceBatch {
                    batch_id: ulid::Ulid::new().to_string(),
                    agent_id: agent.clone(),
                    generation: 1,
                    source: "stream".to_owned(),
                    events: vec![NativeEvent {
                        ts: None,
                        kind: "session.ready".to_owned(),
                        native_id: ulid::Ulid::new().to_string(),
                        data: json!({"command_id": launch_id}),
                    }],
                    cursor: None,
                },
                now,
            )
            .expect("commit session ready");
        store
            .settle(&launch_id, eliot_core::CommandState::Applied, None, now)
            .expect("settle launch");
        agent
    }

    #[tokio::test]
    async fn owner_sees_full_teams_and_manager_sees_own_team() {
        let (router, root) = overview_router();
        let now = now_ms();
        {
            let store_handle = std::sync::Arc::clone(&router.inner.store);
            let mut store = store_handle.lock().await;
            for project in ["team-a", "team-b"] {
                let checkout = root.join(project);
                std::fs::create_dir_all(&checkout).unwrap();
                store
                    .upsert_project_registration(
                        "owner",
                        &eliot_store::ProjectRegistrationInput {
                            id: project.to_owned(),
                            checkout: checkout.to_str().unwrap().to_owned(),
                            repo_common_dir: checkout.join(".git").to_str().unwrap().to_owned(),
                            git: None,
                            profiles: vec!["fixture".to_owned()],
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
        }
        let manager = launch_idle(&router, "team-a", None, now).await;
        {
            let store_handle = std::sync::Arc::clone(&router.inner.store);
            let mut store = store_handle.lock().await;
            store
                .claim_project_manager(
                    &format!("agent:{manager}:1"),
                    &eliot_core::api::ProjectAssignmentClaimRequest {
                        id: ulid::Ulid::new().to_string(),
                        project: "team-a".to_owned(),
                        manager: None,
                    },
                    now,
                )
                .unwrap();
        }
        let worker = launch_idle(&router, "team-a", Some(&manager), now).await;
        let _other = launch_idle(&router, "team-b", None, now).await;

        // Owner: every team in full, Owner limits overlaid from config.toml.
        let value = router
            .project_overview(&json!({}), &Principal::Owner)
            .await
            .unwrap();
        let response: ProjectOverviewResponse = serde_json::from_value(value).unwrap();
        assert_eq!(response.coverage, OverviewCoverage::Partial);
        assert_eq!(response.projects.len(), 2);
        let team_a = &response.projects[0];
        let team_b = &response.projects[1];
        assert_eq!(team_a.project, "team-a");
        assert!(team_a.detail);
        assert_eq!(team_a.manager.as_ref().unwrap().agent, manager);
        assert_eq!(team_a.workers.len(), 1);
        assert_eq!(team_a.workers[0].agent, worker);
        assert_eq!(team_a.totals.live, 2);
        assert_eq!(response.fleet.live_harnesses, 3);
        assert_eq!(response.fleet.manager_total, 1);
        assert_eq!(response.fleet.worker_total, 2);
        assert_eq!(
            response.fleet.lifecycle.live + response.fleet.lifecycle.starting,
            3
        );
        assert_eq!(response.fleet.capacity.scope, "global:swarm");
        assert_eq!(response.fleet.capacity.limit, Some(5));
        assert_eq!(team_a.capacity[0].scope, "project:team-a");
        assert_eq!(team_a.capacity[0].limit, Some(2));
        assert_eq!(team_a.capacity[1].limit, Some(3));
        assert_eq!(team_b.capacity[0].limit, None);
        let unknown = router
            .project_overview(&json!({"project": "missing"}), &Principal::Owner)
            .await
            .unwrap_err();
        assert_eq!(unknown.code, "not_found");

        // Manager: its own team in full, aggregates for other projects.
        let caller = Principal::Agent {
            id: manager.clone(),
            generation: 1,
        };
        let value = router.project_overview(&json!({}), &caller).await.unwrap();
        let response: ProjectOverviewResponse = serde_json::from_value(value).unwrap();
        let team_a = &response.projects[0];
        let team_b = &response.projects[1];
        assert!(team_a.detail);
        assert_eq!(team_a.workers.len(), 1);
        assert!(!team_b.detail);
        assert!(team_b.workers.is_empty());
        assert!(team_b.workspaces.is_empty());
        assert_eq!(team_b.worker_total, 1);
        assert_eq!(team_b.totals.live, 1);
        assert_eq!(team_b.manager, None);

        // Explicit filters keep the same shaping: own project full, others
        // aggregates, unknown projects rejected.
        let value = router
            .project_overview(&json!({"project": "team-a"}), &caller)
            .await
            .unwrap();
        let response: ProjectOverviewResponse = serde_json::from_value(value).unwrap();
        assert_eq!(response.projects.len(), 1);
        assert!(response.projects[0].detail);
        assert_eq!(response.fleet.projects, 2);
        assert!(response.event_watermark > 0);
        let value = router
            .project_overview(&json!({"project": "team-b"}), &caller)
            .await
            .unwrap();
        let response: ProjectOverviewResponse = serde_json::from_value(value).unwrap();
        assert_eq!(response.projects.len(), 1);
        assert!(!response.projects[0].detail);
        let unknown = router
            .project_overview(&json!({"project": "missing"}), &caller)
            .await
            .unwrap_err();
        assert_eq!(unknown.code, "not_found");

        std::mem::drop(router);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
