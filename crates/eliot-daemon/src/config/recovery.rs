//! Settings access when a bad config prevents normal daemon startup.
use std::path::Path;

use eliot_core::api::{
    SettingsReadRequest, SettingsReadResponse, SettingsSaveRequest, SettingsSaveResponse,
    SettingsSection,
};
use eliot_core::{Error, Result, SecretPolicy};

use super::{profiles_edit, settings};
use crate::platform::DataDir;
use crate::platform::data_dir::DaemonLock;

/// Read the settings document with the normal validator while the daemon is offline.
pub fn read_settings_for_repair(path: &Path) -> Result<SettingsReadResponse> {
    let (root, _lock, policy) = open_offline(path)?;
    let request = SettingsReadRequest::default();
    let snapshot = settings::read_snapshot(root.path(), None, &policy)?;
    let store = eliot_store::Store::open(&root.path().join("eliot.db"))?;
    let reply = snapshot.into_response(&store)?;
    request.validate_response(&reply)?;
    Ok(reply)
}

/// Validate or save a repair through the same revision-checked writer as settings.save.
pub async fn save_settings_for_repair(
    path: &Path,
    request: &SettingsSaveRequest,
) -> Result<SettingsSaveResponse> {
    request.validate()?;
    let (root, _lock, policy) = open_offline(path)?;
    let projects = resolve_projects_for_save(root.path(), request, &policy).await?;
    let mut store = eliot_store::Store::open(&root.path().join("eliot.db"))?;
    let (reply, _) = settings::save(root.path(), &mut store, request, &policy, projects.as_ref())?;
    request.validate_response(&reply).map_err(|_| {
        Error::new(
            "unknown",
            "settings save response could not be validated; reread the settings",
        )
    })?;
    Ok(reply)
}

fn open_offline(path: &Path) -> Result<(DataDir, DaemonLock, SecretPolicy)> {
    if !crate::daemon::client_is_owner(path)? {
        return Err(Error::new(
            "not_allowed",
            "settings repair requires the local Owner",
        ));
    }
    let root = DataDir::open_existing(path)?;
    let lock = root.acquire_daemon_lock()?;
    let policy = SecretPolicy::new([root.read_owner_token()?]);
    Ok((root, lock, policy))
}

/// Bind any Git resolution to the validated source and candidate revisions.
pub(crate) async fn resolve_projects_for_save(
    data_root: &Path,
    request: &SettingsSaveRequest,
    policy: &SecretPolicy,
) -> Result<Option<settings::ResolvedProjects>> {
    if !request.sections.contains_key(&SettingsSection::Projects) {
        return Ok(None);
    }
    let (source, candidate) = settings::prepare_candidate(
        data_root,
        request.expected_revision.as_deref(),
        &request.sections,
        policy,
    )?;
    Ok(Some(settings::ResolvedProjects {
        source_revision: source.as_deref().map(profiles_edit::revision_of),
        candidate_revision: profiles_edit::revision_of(&candidate),
        inputs: crate::automation::project_registration_inputs(
            settings::project_configs_for_settings(&candidate)?,
        )
        .await?,
    }))
}
