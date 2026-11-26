use super::*;

impl Router {
    pub(in crate::router) async fn artifact_reconcile(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ArtifactReconcileRequest = coordination::decode_coordination_request(
            params,
            &self.inner.secrets,
            "artifact.reconcile",
            ArtifactReconcileRequest::validate,
        )?;
        let payload = encoded(&request)?;
        let artifact = {
            let store = self.inner.store.lock().await;
            if let Some(replay) = store.cleanup_replay::<ArtifactView>(
                "owner",
                &request.id,
                "artifact.reconcile",
                &payload,
            )? {
                return encoded(replay);
            }
            self.check_cleanup_admission()?;
            let artifact = store.unknown_cleanup_artifact(&request.artifact)?;
            if !store
                .artifact_reconciliation_quiescent(&artifact, request.release_custody.is_some())?
            {
                return Err(Error::new(
                    "agent_not_quiescent",
                    "artifact Agent custody is still active",
                ));
            }
            artifact
        };
        if matches!(
            artifact.kind,
            ArtifactKind::Worktree | ArtifactKind::Branch | ArtifactKind::ProjectRegistration
        ) || artifact.purpose == SWARM_ARTIFACT_ROOT_PURPOSE
        {
            return Err(Error::new(
                "unsupported",
                "artifact has separate workspace, Git or generation-root custody",
            ));
        }
        let mut absence = None;
        let mut present = None;
        if request.release_custody.is_none() {
            if artifact.kind == ArtifactKind::NativeSession {
                return Err(Error::new(
                    "readback_unavailable",
                    "use --release-custody to keep the native session outside Swarm's cleanup custody",
                ));
            }
            let (_, absent) = self
                .cleanup_preflight(&artifact, CLEANUP_PLAN_MAX_PREFLIGHT)
                .await?;
            if absent {
                let observed = self
                    .observe_cleanup_directory_absence(&artifact)?
                    .ok_or_else(|| {
                        Error::new("identity_changed", "artifact directory reappeared")
                    })?;
                crate::workspace_git::wait_for_absence_grace(
                    Instant::now() + CLEANUP_ABSENCE_OBSERVATION_BUDGET,
                )
                .await?;
                absence = Some(observed);
            } else {
                present = self.require_cleanup_ownership(&artifact).await?;
            }
        }
        let mut store = self.inner.store.lock().await;
        self.check_cleanup_admission()?;
        if let Some((missing, parent)) = &absence {
            crate::workspace_git::confirm_worktree_absence(missing, parent)?;
        }
        if let Some(path) = &present {
            path.require_identity(&artifact.identity)?;
        }
        encoded(store.reconcile_cleanup_artifact(
            &artifact,
            &request,
            absence.is_some(),
            now_ms(),
        )?)
    }
}
