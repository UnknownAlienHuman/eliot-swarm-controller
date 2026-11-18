use super::*;
use eliot_core::api::{
    ProjectAssignmentClaimRequest, ProjectAssignmentGetRequest, ProjectAssignmentLaunchRequest,
    ProjectAssignmentReleaseRequest, discovery::decode_params,
};

impl Router {
    pub(super) async fn project_assignment_get(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectAssignmentGetRequest = decode_params(params.clone())?;
        request.validate()?;
        let store = self.inner.store.lock().await;
        encode_assignment(store.project_assignment(&principal_name(principal), &request.project)?)
    }

    pub(super) async fn project_assignment_claim(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectAssignmentClaimRequest = decode_params(params.clone())?;
        let mut store = self.inner.store.lock().await;
        let change = store.claim_project_manager(&principal_name(principal), &request, now_ms())?;
        self.publish_locked(&change.events);
        self.signal_progress();
        encode_assignment(change.value)
    }

    pub(super) async fn project_assignment_release(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let request: ProjectAssignmentReleaseRequest = decode_params(params.clone())?;
        let mut store = self.inner.store.lock().await;
        let change =
            store.release_project_manager(&principal_name(principal), &request, now_ms())?;
        self.publish_locked(&change.events);
        self.signal_progress();
        encode_assignment(change.value)
    }

    pub(super) async fn project_assignment_launch(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ProjectAssignmentLaunchRequest = decode_params(params.clone())?;
        request.validate()?;
        let registration = {
            let store = self.inner.store.lock().await;
            if let Some(result) = store.replay_project_manager_launch(&request, now_ms())? {
                return encode_assignment(result);
            }
            store
                .get_project_registration("owner", &request.project)?
                .ok_or_else(|| Error::new("not_found", "project registration was not found"))?
        };
        let prepared = async {
            self.check_command_admission_mode("launch")?;
            if !registration.profiles.contains(&request.profile) {
                return Err(Error::new(
                    "not_allowed",
                    "manager launch profile is not approved for this project",
                ));
            }
            let (checkout, common_dir) = crate::workspace_git::WorkspaceGit::new()?
                .repository_identity(&registration.checkout)
                .await?;
            if checkout != registration.checkout || common_dir != registration.repo_common_dir {
                return Err(Error::new(
                    "not_allowed",
                    "registered repository identity changed",
                ));
            }
            let mut launch = SubmitCommand::new(request.id.clone(), "launch");
            launch.args = json!({"workspace": registration.checkout, "profile": request.profile});
            normalize_launch(&mut launch, principal)?;
            self.freeze_launch_admission(principal, &mut launch, false)
                .await?;
            if contains_known_secret(&launch.args, &self.inner.secrets) {
                return Err(Error::new(
                    "secret_in_payload",
                    "manager launch contains a known credential",
                ));
            }
            Ok(launch)
        }
        .await;
        let mut store = self.inner.store.lock().await;
        let prepared = prepared.and_then(|launch| {
            self.check_command_admission_mode("launch")?;
            Ok(launch)
        });
        let change = store.launch_project_manager(&request, prepared, now_ms())?;
        if !change.events.is_empty() {
            self.publish_locked(&change.events);
            if let Some(id) = &change.value.command_id {
                self.schedule_command(id.clone(), None);
            }
        }
        self.signal_progress();
        encode_assignment(change.value)
    }
}

fn encode_assignment(value: impl serde::Serialize) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "assignment result could not be encoded"))
}
