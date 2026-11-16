//! First-use configuration for a private Swarm installation.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use eliot_core::{
    Error, Result, SecretPolicy,
    api::{ProjectDiscoverResponse, ProjectRegistrationView},
};
use eliot_store::{ProjectRegistrationInput, Store};
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// Explicitly create or validate a user-selected private data directory.
///
/// Passing a fresh path is the safe remedy for an unsafe default root. This
/// never repairs, copies, or weakens the checks on an existing directory.
pub fn ensure_private_data_dir(path: impl AsRef<Path>) -> Result<PathBuf> {
    crate::platform::data_dir::ensure_private_data_dir(path)
}

/// Seed installed native-default profiles and one initial default route via
/// the unified settings writer. Existing profile and routing choices survive.
pub(crate) async fn ensure_hot_start(
    data_root: &Path,
    store: &Mutex<Store>,
    policy: &SecretPolicy,
    cwd: Option<&Path>,
) -> Result<()> {
    {
        let needs = {
            let mut store = store.lock().await;
            bootstrap_needed(data_root, &mut store, policy)?
        };
        let muse_cmd = if needs {
            let (cmd, skipped) = crate::adhoc::select_muse_hot_start_cmd().await;
            for cmd in &skipped {
                crate::logging::record(
                    policy,
                    "bootstrap.muse_empty_catalogue_skipped",
                    &[("cmd", cmd.as_str())],
                    None,
                );
            }
            cmd
        } else {
            None
        };
        let mut store = store.lock().await;
        ensure_bootstrap_settings(data_root, &mut store, policy, muse_cmd)?;
    }
    if let Some(cwd) = cwd
        && let Some((checkout, common_dir)) = workspace_identity(cwd).await?
    {
        let existing_id = matching_project_id(data_root, &checkout, &common_dir).await?;
        let mut store = store.lock().await;
        let _ = ensure_project_identity(
            data_root,
            &mut store,
            &checkout,
            &common_dir,
            existing_id.as_deref(),
            policy,
        )?;
    }
    Ok(())
}

/// Read and register an Owner-opened or launched Git checkout. A non-Git
/// directory is a no-op; existing project settings and their allowlists are
/// preserved.
pub(crate) async fn discover_project(
    data_root: &Path,
    store: &Mutex<Store>,
    workspace: &Path,
    policy: &SecretPolicy,
) -> Result<ProjectDiscoverResponse> {
    let Some((checkout, common_dir)) = workspace_identity(workspace).await? else {
        return Ok(ProjectDiscoverResponse { project: None });
    };
    let existing_id = matching_project_id(data_root, &checkout, &common_dir).await?;
    let registration = {
        let mut store = store.lock().await;
        ensure_project_identity(
            data_root,
            &mut store,
            &checkout,
            &common_dir,
            existing_id.as_deref(),
            policy,
        )?
    };
    Ok(ProjectDiscoverResponse {
        project: registration,
    })
}

async fn matching_project_id(
    data_root: &Path,
    checkout: &str,
    common_dir: &str,
) -> Result<Option<String>> {
    let projects = crate::config::load_project_registration_config(data_root)?;
    if projects.is_empty() {
        return Ok(None);
    }
    let git = crate::workspace_git::WorkspaceGit::new()?;
    for project in projects {
        match git.discover_repository_identity(&project.path).await {
            Ok((registered_checkout, registered_common))
                if same_git_path(&registered_checkout, checkout)
                    && same_git_path(&registered_common, common_dir) =>
            {
                return Ok(Some(project.id));
            }
            Ok(_) => {}
            Err(error) if error.code == "unavailable" => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

fn same_git_path(left: &str, right: &str) -> bool {
    #[cfg(windows)]
    {
        left.eq_ignore_ascii_case(right)
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

async fn workspace_identity(workspace: &Path) -> Result<Option<(String, String)>> {
    let path = workspace
        .to_str()
        .ok_or_else(|| Error::new("invalid", "project workspace path is not UTF-8"))?;
    let git = match crate::workspace_git::WorkspaceGit::new() {
        Ok(git) => git,
        Err(error) if error.code == "unavailable" => return Ok(None),
        Err(error) => return Err(error),
    };
    match git.discover_repository_identity(path).await {
        Ok(identity) => Ok(Some(identity)),
        Err(error) if error.code == "unavailable" => Ok(None),
        Err(error) => Err(error),
    }
}

fn bootstrap_needed(data_root: &Path, store: &mut Store, policy: &SecretPolicy) -> Result<bool> {
    use crate::config::settings::{self};
    use eliot_core::api::SettingsSection::{Profiles, Routing};
    let read = settings::read_internal(data_root, store, Some(&[Profiles, Routing]), policy)?;
    let current_profiles = read
        .sections
        .get(&Profiles)
        .cloned()
        .unwrap_or_else(|| json!({}));
    let profile_names = current_profiles
        .as_object()
        .map(|profiles| profiles.keys().len())
        .unwrap_or_default();
    Ok(read.revision.is_none() && profile_names == 0)
}

fn ensure_bootstrap_settings(
    data_root: &Path,
    store: &mut Store,
    policy: &SecretPolicy,
    muse_cmd: Option<String>,
) -> Result<()> {
    use crate::config::settings::{self, SettingsAuthor};
    use eliot_core::api::SettingsSection::{Profiles, Routing};

    let read = settings::read_internal(data_root, store, Some(&[Profiles, Routing]), policy)?;
    let current_profiles = read
        .sections
        .get(&Profiles)
        .cloned()
        .unwrap_or_else(|| json!({}));
    let current_routing = read
        .sections
        .get(&Routing)
        .cloned()
        .unwrap_or_else(|| json!({"default": []}));
    let mut sections = BTreeMap::new();
    let mut profile_names = current_profiles
        .as_object()
        .map(|profiles| profiles.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();

    if read.revision.is_none() && profile_names.is_empty() {
        let profiles = installed_default_profiles(muse_cmd);
        profile_names = profiles.keys().cloned().collect();
        if !profiles.is_empty() {
            sections.insert(Profiles, Value::Object(profiles));
        }
    }

    let default_candidates = current_routing
        .get("default")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if default_candidates.is_empty()
        && current_profiles
            .as_object()
            .is_none_or(|profiles| profiles.is_empty())
        && !profile_names.is_empty()
    {
        let default_profile = if profile_names.iter().any(|name| name == "codex") {
            "codex".to_owned()
        } else {
            profile_names[0].clone()
        };
        sections.insert(Routing, json!({"default": [default_profile]}));
    }

    if sections.is_empty() {
        return Ok(());
    }
    settings::save_internal(
        data_root,
        store,
        read.revision.as_deref(),
        &sections,
        SettingsAuthor::Bootstrap,
        false,
        policy,
    )?;
    Ok(())
}

fn installed_default_profiles(muse_cmd: Option<String>) -> serde_json::Map<String, Value> {
    let mut profiles = serde_json::Map::new();
    for harness in crate::adhoc::SUPPORTED_HARNESSES {
        if *harness == "muse" {
            let Some(cmd) = muse_cmd.clone() else {
                continue;
            };
            let mut profile = serde_json::Map::new();
            profile.insert("harness".to_owned(), Value::String("muse".to_owned()));
            profile.insert("mcp".to_owned(), Value::String("inherit".to_owned()));
            profile.insert("cmd".to_owned(), Value::String(cmd));
            profiles.insert("muse".to_owned(), Value::Object(profile));
            continue;
        }
        let discovered = crate::adhoc::harness_installation(harness);
        if discovered.get("installed").and_then(Value::as_bool) != Some(true) {
            continue;
        }
        let mut profile = serde_json::Map::new();
        profile.insert("harness".to_owned(), Value::String((*harness).to_owned()));
        // Explicit worker MCP policy per harness: the new default is
        // `minimal` wherever the harness implements an isolation mechanism
        // (Codex, Claude, OpenCode all serve only the Agent-scoped server);
        // Muse and Antigravity keep an explicit `inherit` opt-in because no
        // verified isolation mechanism exists for them.
        let mcp = match *harness {
            "muse" | "antigravity" => "inherit",
            _ => "minimal",
        };
        profile.insert("mcp".to_owned(), Value::String(mcp.to_owned()));
        if let Some(cmd) = discovered.get("cmd").and_then(Value::as_str) {
            profile.insert("cmd".to_owned(), Value::String(cmd.to_owned()));
        } else if *harness != "codex" {
            continue;
        }
        profiles.insert((*harness).to_owned(), Value::Object(profile));
    }
    profiles
}

fn ensure_project_identity(
    data_root: &Path,
    store: &mut Store,
    checkout: &str,
    common_dir: &str,
    existing_id: Option<&str>,
    policy: &SecretPolicy,
) -> Result<Option<ProjectRegistrationView>> {
    use crate::config::settings::{self, SettingsAuthor};
    use eliot_core::api::SettingsSection::{Profiles, Projects};

    let read = settings::read_internal(data_root, store, Some(&[Profiles, Projects]), policy)?;
    let profiles = read
        .sections
        .get(&Profiles)
        .cloned()
        .unwrap_or_else(|| json!({}));
    let profile_names = profiles
        .as_object()
        .ok_or_else(|| Error::new("source_integrity", "settings profiles are not an object"))?
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    if profile_names.is_empty() {
        return Ok(None);
    }
    let mut projects = read
        .sections
        .get(&Projects)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let id = existing_id
        .map(str::to_owned)
        .unwrap_or_else(|| project_id(checkout));
    if !projects.contains_key(&id) {
        let entry = json!({
            "path": checkout,
            "profiles": profile_names,
            "actions": ["launch", "send"],
        });
        projects.insert(id.clone(), entry);
        let mut sections = BTreeMap::new();
        sections.insert(Projects, Value::Object(projects));
        settings::save_internal(
            data_root,
            store,
            read.revision.as_deref(),
            &sections,
            SettingsAuthor::ProjectDiscovery,
            false,
            policy,
        )?;
    }

    let definition = crate::config::load_project_registration_config(data_root)?
        .into_iter()
        .find(|project| project.id == id)
        .ok_or_else(|| Error::new("source_integrity", "saved project registration disappeared"))?;
    // A concurrent settings save may have retargeted the matched ID while
    // Git discovery ran outside the Store lock. Never apply that path's
    // authority to the checkout from the earlier discovery.
    let configured_checkout = std::fs::canonicalize(&definition.path)
        .map_err(|_| Error::new("conflict", "project checkout changed during discovery"))?;
    if !configured_checkout
        .to_str()
        .is_some_and(|path| same_git_path(path, checkout))
    {
        return Err(Error::new(
            "conflict",
            "project registration changed during discovery",
        ));
    }
    let input = ProjectRegistrationInput {
        id,
        checkout: checkout.to_owned(),
        repo_common_dir: common_dir.to_owned(),
        git: definition.git,
        profiles: definition.profiles,
        actions: definition.actions,
        interpreters: definition.interpreters,
        run_env: definition.run_env,
    };
    store
        .upsert_project_registration("owner", &input, crate::common::now_ms())
        .map(|change| Some(change.value))
}

fn project_id(checkout: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(checkout.as_bytes());
    let digest = hasher.finalize();
    let mut id = String::from("project-");
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(id, "{byte:02x}");
    }
    id
}
