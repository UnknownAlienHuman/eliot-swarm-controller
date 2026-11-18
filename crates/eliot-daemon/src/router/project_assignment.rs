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
            let git = crate::workspace_git::WorkspaceGit::new()?;
            let (checkout, common_dir) = git.repository_identity(&registration.checkout).await?;
            if checkout != registration.checkout || common_dir != registration.repo_common_dir {
                return Err(Error::new(
                    "not_allowed",
                    "registered repository identity changed",
                ));
            }
            let mut launch = SubmitCommand::new(request.id.clone(), "launch");
            // A manager launch always runs in a project worktree. Without an
            // explicit adoption the daemon prepares a fresh opaque worktree on
            // a unique branch from the frozen base OID and runs the existing
            // preparation and setup chain before native dispatch. An explicit
            // adoption names an existing checkout of the same repository; the
            // daemon never adopts automatically, and an adopted checkout keeps
            // its environment with no setup side effects.
            let preparation = if let Some(adopt) = request.adopt_worktree.as_deref() {
                launch.args = json!({"workspace": adopt, "profile": request.profile});
                normalize_launch(&mut launch, principal)?;
                let canonical = required_text(&launch.args, "workspace")?;
                let (adopt_checkout, adopt_common) = git.repository_identity(canonical).await?;
                if adopt_checkout != canonical || adopt_common != registration.repo_common_dir {
                    return Err(Error::new(
                        "not_allowed",
                        "adopted checkout is outside this project repository",
                    ));
                }
                self.freeze_launch_profile(principal, &mut launch).await?;
                None
            } else {
                launch.args = json!({"workspace": registration.checkout, "profile": request.profile, "worktree": manager_branch(&request.id)});
                normalize_launch(&mut launch, principal)?;
                let input = self
                    .freeze_launch_admission(principal, &mut launch, false, false)
                    .await?;
                Some(input.ok_or_else(|| {
                    Error::new("internal", "manager worktree was not frozen")
                })?)
            };
            if contains_known_secret(&launch.args, &self.inner.secrets) {
                return Err(Error::new(
                    "secret_in_payload",
                    "manager launch contains a known credential",
                ));
            }
            Ok((launch, preparation))
        }
        .await;
        let mut store = self.inner.store.lock().await;
        let prepared = prepared.and_then(|prepared| {
            self.check_command_admission_mode("launch")?;
            if !store
                .get_project_registration("owner", &request.project)?
                .is_some_and(|current| current.revision == registration.revision)
            {
                return Err(Error::new(
                    "stale_assignment",
                    "manager launch registration changed",
                ));
            }
            Ok(prepared)
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

/// Unique branch for one manager worktree, derived from the launch operation
/// ID. Retries of the same ID reuse the same branch and read back the retained
/// preparation instead of preparing a second worktree.
fn manager_branch(id: &str) -> String {
    // Git refs can be case-insensitive on disk. Preserve the original case
    // in a numeric suffix while keeping canonical ULID branches unchanged.
    let case = id.bytes().fold(0_u32, |bits, byte| {
        (bits << 1) | u32::from(byte.is_ascii_lowercase())
    });
    let name = id.to_ascii_lowercase();
    if case == 0 {
        format!("swarm/{name}")
    } else {
        format!("swarm/{name}-{case:x}")
    }
}

#[cfg(test)]
mod tests {
    use super::manager_branch;

    #[test]
    fn manager_branches_keep_case_distinctions_on_windows() {
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let canonical = manager_branch(id);
        let lower = manager_branch(&id.to_ascii_lowercase());
        let mixed = manager_branch(&id.replacen('A', "a", 1));
        assert_eq!(canonical, format!("swarm/{}", id.to_ascii_lowercase()));
        assert_ne!(canonical, lower);
        assert_ne!(canonical, mixed);
        assert_ne!(lower, mixed);
        assert_eq!(manager_branch(id), canonical);
    }
}
