//! First-use configuration for a private Swarm installation.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use eliot_core::{
    Error, Result, SecretPolicy,
    api::{ProjectDiscoverResponse, ProjectRegistrationView, SUPPORTED_HARNESSES},
};
use eliot_store::{ProjectRegistrationInput, Store};
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// Where a new private data directory named `name` goes for this process:
/// beside `current`, unless Windows would redirect that location.
pub fn fresh_data_dir(current: &Path, name: &str) -> Result<PathBuf> {
    crate::platform::data_dir::fresh_data_dir(current, name)
}

/// Explicitly create or validate a user-selected private data directory.
///
/// Passing a fresh path is the safe remedy for an unsafe default root. This
/// never repairs, copies, or weakens the checks on an existing directory.
pub fn ensure_private_data_dir(path: impl AsRef<Path>) -> Result<PathBuf> {
    crate::platform::data_dir::ensure_private_data_dir(path)
}

/// Seed installed native-default profiles and initial default/worker routes via
/// the unified settings writer. Existing profile and routing choices survive.
///
/// Startup performs no project discovery: an automatic checkout
/// registration here would resurrect a project the Owner removed on every
/// restart. Explicit registration still runs through [`discover_project`]
/// (an Owner open, an Owner launch workspace, or the desktop's workspace
/// checkout).
pub(crate) async fn ensure_hot_start(
    data_root: &Path,
    store: &Mutex<Store>,
    policy: &SecretPolicy,
) -> Result<()> {
    {
        let needs = {
            let mut store = store.lock().await;
            bootstrap_needed(data_root, &mut store, policy)?
        };
        let profiles = if needs {
            let deadline = std::time::Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
            crate::adhoc::blocking_discovery(deadline, move || installed_default_profiles(deadline))
                .await?
        } else {
            serde_json::Map::new()
        };
        let mut store = store.lock().await;
        ensure_bootstrap_settings(data_root, &mut store, policy, profiles)?;
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
    use crate::config::settings;

    let Some((checkout, common_dir, linked)) = workspace_identity(workspace).await? else {
        return Ok(ProjectDiscoverResponse { project: None });
    };
    let (revision, configured) = settings::project_configs_snapshot(data_root)?;
    let matching = matching_project(configured, &checkout, &common_dir, linked).await?;
    let mut store = store.lock().await;
    if settings::current_revision(data_root)? != revision {
        return Err(Error::new(
            "conflict",
            "project settings changed during discovery",
        ));
    }
    let project = match matching {
        Some((definition, registered_checkout, registered_common)) => {
            Some(upsert_discovered_project(
                &mut store,
                definition,
                &registered_checkout,
                &registered_common,
            )?)
        }
        None => register_project(
            data_root,
            &mut store,
            &checkout,
            &common_dir,
            revision.as_deref(),
            policy,
        )?,
    };
    Ok(ProjectDiscoverResponse { project })
}

/// Resolve an existing registration for a launch that cannot discover projects.
pub(crate) async fn resolve_registered_project(
    data_root: &Path,
    workspace: &Path,
) -> Result<Option<String>> {
    use crate::config::settings;

    let (revision, configured) = settings::project_configs_snapshot(data_root)?;
    if configured.is_empty() {
        return Ok(None);
    }
    let Some((checkout, common_dir, linked)) = workspace_identity(workspace).await? else {
        return Ok(None);
    };
    let matching = matching_project(configured, &checkout, &common_dir, linked).await?;
    if settings::current_revision(data_root)? != revision {
        return Err(Error::new(
            "conflict",
            "project settings changed during resolution",
        ));
    }
    Ok(matching.map(|(project, _, _)| project.id))
}

async fn matching_project(
    projects: Vec<crate::config::ProjectRegistrationConfig>,
    checkout: &str,
    common_dir: &str,
    linked: bool,
) -> Result<Option<(crate::config::ProjectRegistrationConfig, String, String)>> {
    if projects.is_empty() {
        return Ok(None);
    }
    let git = crate::workspace_git::WorkspaceGit::new()?;
    let mut best = None;
    let mut rank = 0;
    let mut ambiguous = false;
    for project in projects {
        let (registered_checkout, registered_common, registered_linked) =
            match git.discover_repository_identity(&project.path).await {
                Ok(identity) => identity,
                Err(error) if error.code == "unavailable" => continue,
                Err(error) => return Err(error),
            };
        if !same_git_path(&registered_common, common_dir) {
            continue;
        }
        let candidate_rank = if linked && !registered_linked {
            // A legacy worktree entry cannot supersede the repository grant.
            2
        } else if linked || same_git_path(&registered_checkout, checkout) {
            1
        } else {
            continue;
        };
        if candidate_rank > rank {
            best = Some((project, registered_checkout, registered_common));
            rank = candidate_rank;
            ambiguous = false;
        } else if candidate_rank == rank {
            ambiguous = true;
        }
    }
    if ambiguous {
        return Err(Error::new(
            "conflict",
            "multiple project registrations match the checkout",
        ));
    }
    Ok(best)
}

fn same_git_path(left: &str, right: &str) -> bool {
    #[cfg(windows)]
    {
        // `fs::canonicalize` on Windows yields the verbatim \\?\ form while a
        // Git identity readback reports the drive-letter form. The same checkout
        // must compare equal however each side was produced, or a registration
        // that already matches is refused as changed and discovery fails.
        eliot_core::display_path(left).eq_ignore_ascii_case(&eliot_core::display_path(right))
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

async fn workspace_identity(workspace: &Path) -> Result<Option<(String, String, bool)>> {
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
    mut profiles: serde_json::Map<String, Value>,
) -> Result<()> {
    use crate::config::settings::{self, SettingsAuthor};
    use eliot_core::api::SettingsSection::{Profiles, Routing};

    let read = settings::read_internal(data_root, &*store, Some(&[Profiles, Routing]), policy)?;
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
        profile_names = profiles.keys().cloned().collect();
        // Each manager can take the default route, so each one delegates
        // to the other installed profiles.
        for manager in bootstrap_managers(&profile_names) {
            let workers = bootstrap_worker_profiles(&profile_names, &manager);
            let Some(profile) = profiles.get_mut(&manager) else {
                continue;
            };
            // Bootstrap records the safe policy explicitly for every native
            // manager; adapters without an enforcement mapping fail closed.
            profile["native_subagents"] = json!("deny");
            profile["child_profiles"] = json!(workers);
            profile["control"] = json!("tree");
        }
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
        // Routing walks this list in order, so a manager that cannot take
        // the task (quota used up, provider hold) falls through to the next.
        let candidates = bootstrap_default_candidates(&profile_names);
        let workers = bootstrap_worker_profiles(&profile_names, &candidates[0]);
        // With only one installed harness the inherited profile still works.
        let workers = if workers.is_empty() {
            vec![candidates[0].clone()]
        } else {
            workers
        };
        let mut routing = current_routing.clone();
        routing["default"] = json!(candidates);
        if routing.get("worker").is_none() {
            routing["worker"] = json!(workers);
        }
        sections.insert(Routing, routing);
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

/// The hot-start managers, in native subagent-denial preference order.
fn bootstrap_managers(names: &[String]) -> Vec<String> {
    let managers: Vec<String> = crate::config::NATIVE_SUBAGENT_DENY_HARNESSES
        .iter()
        .filter_map(|preferred| names.iter().find(|name| name.as_str() == *preferred))
        .cloned()
        .collect();
    if managers.is_empty() {
        names.first().cloned().into_iter().collect()
    } else {
        managers
    }
}

/// The default routing role: the managers, then the other installed
/// profiles in name order. `names` is nonempty, so the list is too.
fn bootstrap_default_candidates(names: &[String]) -> Vec<String> {
    let mut candidates = bootstrap_managers(names);
    let rest: Vec<String> = names
        .iter()
        .filter(|name| !candidates.contains(name))
        .cloned()
        .collect();
    candidates.extend(rest);
    candidates
}

/// A manager's workers in the default routing order, so the harnesses that
/// isolate a worker's MCP surface come before Muse and Antigravity.
fn bootstrap_worker_profiles(names: &[String], manager: &str) -> Vec<String> {
    bootstrap_default_candidates(names)
        .into_iter()
        .filter(|name| name.as_str() != manager)
        .collect()
}

fn installed_default_profiles(deadline: std::time::Instant) -> serde_json::Map<String, Value> {
    let mut profiles = serde_json::Map::new();
    for harness in SUPPORTED_HARNESSES {
        let discovered = crate::adhoc::harness_installation(harness, deadline);
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
        profiles.insert((*harness).to_owned(), Value::Object(profile));
    }
    profiles
}

fn register_project(
    data_root: &Path,
    store: &mut Store,
    checkout: &str,
    common_dir: &str,
    expected_revision: Option<&str>,
    policy: &SecretPolicy,
) -> Result<Option<ProjectRegistrationView>> {
    use crate::config::settings::{self, SettingsAuthor};
    use eliot_core::api::SettingsSection::{Profiles, Projects};

    let read = settings::read_internal(data_root, &*store, Some(&[Profiles, Projects]), policy)?;
    if read.revision.as_deref() != expected_revision {
        return Err(Error::new(
            "conflict",
            "project settings changed during discovery",
        ));
    }
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
    let id = project_id(checkout);
    if projects.contains_key(&id) {
        return Err(Error::new(
            "conflict",
            "project registration ID already belongs to another checkout",
        ));
    }
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

    let definition = crate::config::load_project_registration_config(data_root)?
        .into_iter()
        .find(|project| project.id == id)
        .ok_or_else(|| Error::new("source_integrity", "saved project registration disappeared"))?;
    upsert_discovered_project(store, definition, checkout, common_dir).map(Some)
}

fn upsert_discovered_project(
    store: &mut Store,
    definition: crate::config::ProjectRegistrationConfig,
    checkout: &str,
    common_dir: &str,
) -> Result<ProjectRegistrationView> {
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
        id: definition.id,
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
        .map(|change| change.value)
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
