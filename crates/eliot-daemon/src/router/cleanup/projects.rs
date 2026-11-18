//! Owner cleanup candidates derived from the revisioned settings source.
use super::*;
use crate::config::settings;
use eliot_store::Store;
use std::io::ErrorKind;

impl Router {
    /// Project rows plus the settings revision they were read at. The
    /// revision pins continuation offsets to one project set: callers reject
    /// a continued page whose revision moved.
    pub(super) async fn plan_project_registrations(
        &self,
        request: &CleanupPlanRequest,
    ) -> Result<(Vec<CleanupCandidate>, Option<String>)> {
        // Settings entries have no creation timestamp for a cutoff.
        if request.scope == CleanupScope::SelfTree || request.older_than.is_some() {
            return Ok((Vec::new(), None));
        }
        let policy = self.secret_policy_snapshot();
        let store = self.inner.store.lock().await;
        let read = settings::read_internal(
            &self.inner.data_root,
            &store,
            Some(&[SettingsSection::Projects]),
            &policy,
        )?;
        let Some(revision) = &read.revision else {
            return Ok((Vec::new(), None));
        };
        let mut items = Vec::new();
        for (id, project) in project_entries(&read)? {
            if request
                .project
                .as_ref()
                .is_some_and(|project| project != id)
            {
                continue;
            }
            let path = project_path(project)?;
            let check = check_stale_project(&store, id, path);
            let (verdict, reason) = match check {
                Ok(()) => ("safe", "project_path_missing".to_owned()),
                Err(error) => ("keep", error.code),
            };
            items.push(CleanupCandidate {
                artifact: ArtifactView {
                    id: ulid::Ulid::new().to_string(),
                    agent: String::new(),
                    generation: 0,
                    project: id.clone(),
                    tree: Vec::new(),
                    path: path.to_owned(),
                    kind: ArtifactKind::ProjectRegistration,
                    purpose: "Project registration".to_owned(),
                    identity: revision.clone(),
                    created_at: 0,
                    state: "registered".to_owned(),
                    git: None,
                },
                bytes: None,
                verdict: verdict.to_owned(),
                reason,
            });
        }
        Ok((items, Some(revision.clone())))
    }

    /// The Store lock spans live-use rechecks, settings publication and the
    /// receipt update, so Agent admission cannot race the checked removal.
    pub(super) fn apply_project_registrations(
        &self,
        store: &mut Store,
        request: &CleanupApplyRequest,
        plan: &CleanupPlan,
        response: &mut CleanupApplyResponse,
    ) -> Result<()> {
        let attempted = plan
            .items
            .iter()
            .enumerate()
            .filter(|(index, item)| {
                item.artifact.kind == ArtifactKind::ProjectRegistration
                    && response.items[*index].outcome == "unknown"
            })
            .collect::<Vec<_>>();
        let Some((_, first)) = attempted.first() else {
            return Ok(());
        };

        let policy = self.secret_policy_snapshot();
        let revision = &first.artifact.identity;
        let result = (|| {
            let mut selected = Vec::new();
            let read = settings::read_internal(
                &self.inner.data_root,
                store,
                Some(&[SettingsSection::Projects]),
                &policy,
            )?;
            if read.revision.as_ref() != Some(revision) {
                return Err(Error::new(
                    "conflict",
                    "project settings changed since the cleanup plan",
                ));
            }
            let mut entries = project_entries(&read)?.clone();
            for &(index, item) in &attempted {
                let artifact = &item.artifact;
                let current_path = entries
                    .get(&artifact.project)
                    .map(project_path)
                    .transpose()?;
                if &artifact.identity != revision || current_path != Some(artifact.path.as_str()) {
                    project_outcome(&mut response.items[index], Err("identity_changed"));
                    continue;
                }
                if let Err(error) = check_stale_project(store, &artifact.project, &artifact.path) {
                    project_outcome(&mut response.items[index], Err(&error.code));
                    continue;
                }
                selected.push(artifact.project.clone());
                entries.remove(&artifact.project);
            }
            if selected.is_empty() {
                return Ok(());
            }
            settings::remove_stale_projects(
                &self.inner.data_root,
                store,
                revision,
                entries,
                &selected,
                &policy,
            )
        })();

        for (index, _) in attempted {
            let outcome = &mut response.items[index];
            if outcome.outcome == "unknown" {
                project_outcome(
                    outcome,
                    result
                        .as_ref()
                        .map(|_| ())
                        .map_err(|error| error.code.as_str()),
                );
            }
        }
        store.settle_project_cleanup(request, response)
    }
}

fn project_entries(read: &SettingsReadResponse) -> Result<&serde_json::Map<String, Value>> {
    read.sections
        .get(&SettingsSection::Projects)
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("source_integrity", "settings projects are not an object"))
}

fn project_path(entry: &Value) -> Result<&str> {
    entry
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .ok_or_else(|| Error::new("source_integrity", "project registration has no path"))
}

fn check_stale_project(store: &Store, id: &str, path: &str) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(_) => {
            return Err(Error::new(
                "project_path_present",
                "registered checkout still exists",
            ));
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(_) => {
            return Err(Error::new(
                "project_path_unverified",
                "checkout absence could not be verified",
            ));
        }
    }
    settings::require_project_unused(store, id, path)
}

fn project_outcome(outcome: &mut CleanupItemOutcome, result: std::result::Result<(), &str>) {
    match result {
        Ok(()) => {
            outcome.outcome = "removed".to_owned();
            outcome.reason = None;
        }
        Err(code) => {
            outcome.outcome = if code == "unknown" {
                "unknown"
            } else {
                "skipped"
            }
            .to_owned();
            outcome.reason = Some(code.to_owned());
        }
    }
}
