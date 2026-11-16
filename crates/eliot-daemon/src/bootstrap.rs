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

/// Seed installed native-default profiles and initial default/worker routes via
/// the unified settings writer. Existing profile and routing choices survive.
pub(crate) async fn ensure_hot_start(
    data_root: &Path,
    store: &Mutex<Store>,
    policy: &SecretPolicy,
    cwd: Option<&Path>,
) -> Result<()> {
    {
        let mut store = store.lock().await;
        ensure_bootstrap_settings(data_root, &mut store, policy)?;
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

fn ensure_bootstrap_settings(
    data_root: &Path,
    store: &mut Store,
    policy: &SecretPolicy,
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
        let mut profiles = installed_default_profiles();
        profile_names = profiles.keys().cloned().collect();
        let default_profile = bootstrap_default_profile(&profile_names);
        if let Some(profile) = profiles.get_mut(&default_profile) {
            let workers = bootstrap_worker_profiles(&profile_names, &default_profile);
            // A manager whose harness cannot disable native subagents keeps
            // them explicitly; the derived `deny` would make the whole
            // bootstrap settings save (and daemon startup) fail.
            if !workers.is_empty() && !manager_supports_native_deny(&default_profile) {
                profile["native_subagents"] = json!("allow");
            }
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
        let default_profile = bootstrap_default_profile(&profile_names);
        let workers = bootstrap_worker_profiles(&profile_names, &default_profile);
        // With only one installed harness the inherited profile still works.
        let workers = if workers.is_empty() {
            vec![default_profile.clone()]
        } else {
            workers
        };
        let mut routing = current_routing.clone();
        routing["default"] = json!([default_profile]);
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

/// Harnesses whose native subagents can be turned off per session
/// (`native_subagents = "deny"`), in bootstrap manager preference order. A
/// manager on one of them delegates through Swarm by default.
const DENY_CAPABLE_MANAGERS: [&str; 3] = ["codex", "claude", "opencode"];

/// The hot-start manager: the first installed deny-capable harness, else the
/// first installed profile (Muse or Antigravity, which keep native subagents).
fn bootstrap_default_profile(names: &[String]) -> String {
    DENY_CAPABLE_MANAGERS
        .iter()
        .find_map(|preferred| names.iter().find(|name| name.as_str() == *preferred))
        .or_else(|| names.first())
        .cloned()
        .unwrap_or_default()
}

/// Whether a hot-start manager on this profile can disable native subagents.
fn manager_supports_native_deny(profile: &str) -> bool {
    DENY_CAPABLE_MANAGERS.contains(&profile)
}

fn bootstrap_worker_profiles(names: &[String], manager: &str) -> Vec<String> {
    names
        .iter()
        .filter(|name| name.as_str() != manager)
        .cloned()
        .collect()
}

fn installed_default_profiles() -> serde_json::Map<String, Value> {
    let mut profiles = serde_json::Map::new();
    for harness in crate::adhoc::SUPPORTED_HARNESSES {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_manager_prefers_deny_capable_harnesses() {
        let names = |list: &[&str]| list.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>();
        assert_eq!(
            bootstrap_default_profile(&names(&["antigravity", "codex", "muse"])),
            "codex"
        );
        assert_eq!(
            bootstrap_default_profile(&names(&["antigravity", "claude", "muse"])),
            "claude"
        );
        assert_eq!(
            bootstrap_default_profile(&names(&["antigravity", "muse", "opencode"])),
            "opencode"
        );
        // Only harnesses without a native-subagent switch: the first one
        // manages and keeps native subagents explicitly (see ensure_bootstrap_settings).
        let only = names(&["antigravity", "muse"]);
        assert_eq!(bootstrap_default_profile(&only), "antigravity");
        assert!(!manager_supports_native_deny("antigravity"));
        assert!(!manager_supports_native_deny("muse"));
        assert!(manager_supports_native_deny("codex"));
    }

    #[test]
    fn non_deny_manager_with_children_resolves_with_explicit_allow() {
        // The audited failure: an Antigravity manager granted a Muse child
        // derived `deny`, which Antigravity rejects, aborting startup. The
        // bootstrap now writes `native_subagents = "allow"` for it; the
        // resulting profiles must resolve, and the derived form must not.
        let root = std::env::temp_dir().join(format!("eliot-bootstrap-deny-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        let explicit = "[profiles.antigravity]
harness = \"antigravity\"
model = \"test-model\"
cmd = \"C:/Tools/Agy/agy.exe\"
child_profiles = [\"muse\"]
control = \"tree\"
native_subagents = \"allow\"
[profiles.muse]
harness = \"muse\"
model = \"m\"
cmd = \"C:/Tools/MuseCode/muse-bin-1.4.0-R4161.1.exe\"
";
        std::fs::write(root.join("config.toml"), explicit).unwrap();
        let snapshot = crate::config::resolve_profile(&root, "antigravity")
            .expect("explicit allow keeps the Antigravity manager valid");
        assert_eq!(snapshot["native_subagents"], "allow");
        let derived = explicit.replace(
            "native_subagents = \"allow\"
",
            "",
        );
        std::fs::write(root.join("config.toml"), derived).unwrap();
        let error = crate::config::resolve_profile(&root, "antigravity")
            .expect_err("derived deny is unsupported on Antigravity");
        assert_eq!(error.code, "unsupported");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn fresh_bootstrap_grants_only_installed_worker_profiles_and_preserves_edits() {
        let root = std::env::temp_dir().join(format!("eliot-bootstrap-{}", ulid::Ulid::new()));
        std::fs::create_dir_all(&root).unwrap();
        let mut store = Store::open(Path::new(":memory:")).unwrap();
        let policy = SecretPolicy::default();
        ensure_bootstrap_settings(&root, &mut store, &policy).unwrap();
        if root.join("config.toml").is_file() {
            let source = std::fs::read_to_string(root.join("config.toml")).unwrap();
            let config: toml::Value = toml::from_str(&source).unwrap();
            let profiles = config["profiles"].as_table().unwrap();
            let names = profiles.keys().cloned().collect::<Vec<_>>();
            let manager = bootstrap_default_profile(&names);
            let workers = bootstrap_worker_profiles(&names, &manager);
            assert_eq!(
                config["routing"]["default"][0].as_str(),
                Some(manager.as_str())
            );
            assert_eq!(profiles[&manager]["control"].as_str(), Some("tree"));
            let allowed: Vec<String> = profiles[&manager]["child_profiles"]
                .clone()
                .try_into()
                .unwrap();
            assert_eq!(allowed, workers);
            let routed: Vec<String> = config["routing"]["worker"].clone().try_into().unwrap();
            assert_eq!(
                routed,
                if workers.is_empty() {
                    vec![manager]
                } else {
                    workers
                }
            );
            // Restart never adds a new grant or overwrites an Owner edit.
            let edited = "[profiles.only]\nharness='codex'\nchild_profiles=[]\n[routing]\ndefault=['only']\nworker=['only']\n";
            std::fs::write(root.join("config.toml"), edited).unwrap();
            ensure_bootstrap_settings(&root, &mut store, &policy).unwrap();
            assert_eq!(
                std::fs::read_to_string(root.join("config.toml")).unwrap(),
                edited
            );
        }
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
}
