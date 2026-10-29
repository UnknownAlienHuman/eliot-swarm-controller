//! Owner-registered project authority and project-scoped rule loading.
//!
//! Project definitions are read only from the canonical owner-registered checkout.
//! Full project configuration overlays remain unsupported.

use crate::{
    config::{self, ProjectRegistrationConfig},
    git_monitor::reject_links,
    workspace_git::WorkspaceGit,
};
use eliot_core::{Error, Result};
use std::{fs, io::ErrorKind, path::Path};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RegistrationSnapshot {
    pub id: String,
    pub checkout: String,
    pub repo_common_dir: String,
    pub profiles: Vec<String>,
    pub actions: Vec<String>,
}

pub(super) struct LoadedProjectRules {
    pub principal: String,
    pub registration: RegistrationSnapshot,
    pub snapshot: super::reload::RuleSnapshot,
}

/// Resolve owner-configured registrations to canonical checkout and Git identity.
/// Call before Store admission; no Store lock is acquired during filesystem or Git work.
pub(super) async fn load_registrations(data_root: &Path) -> Result<Vec<RegistrationSnapshot>> {
    let configured = config::load_project_registration_config(data_root)?;
    if configured.is_empty() {
        return Ok(Vec::new());
    }

    let git = WorkspaceGit::new()?;
    let mut registrations = Vec::with_capacity(configured.len());
    for project in configured {
        let registration = resolve_registration(&git, project).await?;
        registrations.push(registration);
    }
    Ok(registrations)
}

/// Load one project's bounded rule files from its registered checkout and preserve
/// the originating authority principal beside the parsed definitions.
pub(super) async fn load_project_rules(
    data_root: &Path,
    registration: RegistrationSnapshot,
) -> Result<LoadedProjectRules> {
    verify_registration(data_root, &registration).await?;
    let checkout = Path::new(&registration.checkout);
    reject_links(checkout)?;
    reject_unsupported_project_overlay(checkout)?;
    reject_links(&checkout.join(".eliot").join("rules"))?;
    let snapshot = super::reload::load_project(checkout)?;
    verify_registration(data_root, &registration).await?;
    if !snapshot.definitions.is_empty()
        && !registration.actions.iter().any(|action| action == "stop")
    {
        return Err(Error::new(
            "not_allowed",
            "project rules require owner approval for the stop action",
        ));
    }

    Ok(LoadedProjectRules {
        principal: format!("project:{}", registration.id),
        registration,
        snapshot,
    })
}

/// Re-read owner policy and Git identity before registration-dependent dispatch.
/// A changed or invalid registration fails closed; callers compare this snapshot
/// with the Store readback/revision before allowing an effect.
pub(super) async fn verify_registration(
    data_root: &Path,
    expected: &RegistrationSnapshot,
) -> Result<()> {
    let configured = config::load_project_registration_config(data_root)?;
    let project = configured
        .into_iter()
        .find(|project| project.id == expected.id)
        .ok_or_else(|| Error::new("not_allowed", "project registration is no longer approved"))?;
    let git = WorkspaceGit::new()?;
    let current = resolve_registration(&git, project).await?;
    if current != *expected {
        return Err(Error::new(
            "not_allowed",
            "project registration or approved authority changed",
        ));
    }
    Ok(())
}

/// Verify a managed Agent workspace against both its frozen Store snapshot and
/// the owner-registered repository. Call outside Store locks, before dispatch.
pub(super) async fn verify_managed_workspace_identity(
    registration: &RegistrationSnapshot,
    workspace: &eliot_core::WorkspacePreparation,
    target_workspace: &str,
    expected_generation: i64,
) -> Result<()> {
    if workspace.project != registration.checkout
        || workspace.repo_path != registration.checkout
        || workspace.repo_common_dir != registration.repo_common_dir
        || workspace.worktree_path != target_workspace
        || workspace.phase != eliot_core::WorkspacePhase::Ready
        || workspace.target_generation != expected_generation
    {
        return Err(Error::new(
            "not_allowed",
            "managed project workspace snapshot differs from its registration",
        ));
    }

    let (canonical_root, repo_common_dir) = WorkspaceGit::new()?
        .repository_identity(target_workspace)
        .await?;
    if canonical_root != workspace.worktree_path
        || repo_common_dir != workspace.repo_common_dir
        || repo_common_dir != registration.repo_common_dir
    {
        return Err(Error::new(
            "not_allowed",
            "managed project workspace Git identity changed",
        ));
    }
    Ok(())
}

async fn resolve_registration(
    git: &WorkspaceGit,
    project: ProjectRegistrationConfig,
) -> Result<RegistrationSnapshot> {
    let (checkout, repo_common_dir) = git.repository_identity(&project.path).await?;
    Ok(RegistrationSnapshot {
        id: project.id,
        checkout,
        repo_common_dir,
        profiles: project.profiles,
        actions: project.actions,
    })
}

fn reject_unsupported_project_overlay(checkout: &Path) -> Result<()> {
    let config_path = checkout.join(".eliot").join("config.toml");
    reject_links(&config_path)?;
    match fs::symlink_metadata(&config_path) {
        Ok(_) => Err(Error::new(
            "unsupported",
            "project configuration overlays are not implemented",
        )),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(_) => Err(Error::new("unavailable", "cannot inspect project metadata")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{path::PathBuf, process::Command};

    struct ProjectFixture {
        base: PathBuf,
        root: PathBuf,
        data_root: PathBuf,
    }

    impl ProjectFixture {
        fn new() -> Self {
            let base = fs::canonicalize(std::env::temp_dir()).expect("canonical temp directory");
            let root = base.join(format!("eliot-project-loader-{}", ulid::Ulid::new()));
            fs::create_dir(&root).expect("create isolated project fixture");
            let data_root = root.join("owner-data");
            fs::create_dir(&data_root).expect("create owner data directory");
            Self {
                base,
                root,
                data_root,
            }
        }

        fn repository(&self, name: &str) -> PathBuf {
            let path = self.root.join(name);
            fs::create_dir(&path).expect("create repository directory");
            let output = Command::new("git")
                .args(["init", "--quiet"])
                .current_dir(&path)
                .output()
                .expect("start git init");
            assert!(
                output.status.success(),
                "git init failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            path
        }

        fn run_git(&self, repository: &Path, args: &[&str]) {
            let output = Command::new("git")
                .args(args)
                .current_dir(repository)
                .output()
                .expect("start Git fixture command");
            assert!(
                output.status.success(),
                "Git fixture command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        fn write_rule(&self, root: &Path, id: &str) {
            let directory = root.join(".eliot").join("rules");
            fs::create_dir_all(&directory).expect("create project rules directory");
            fs::write(
                directory.join("rule.toml"),
                format!(
                    "[[rule]]\nid = '{id}'\non = {{ event = 'turn.completed' }}\ndo = [{{ action = 'stop', target = 'a1', expected_generation = 1 }}]\n"
                ),
            )
            .expect("write project rule");
        }

        fn write_registration(
            &self,
            checkout: &Path,
            profiles: Option<&[&str]>,
            actions: Option<&[&str]>,
        ) {
            let mut source = format!(
                "[projects.app]\npath = {}\n",
                toml_string(&checkout.to_string_lossy())
            );
            if let Some(profiles) = profiles {
                source.push_str(&format!("profiles = {}\n", toml_array(profiles)));
            }
            if let Some(actions) = actions {
                source.push_str(&format!("actions = {}\n", toml_array(actions)));
            }
            fs::write(self.data_root.join("config.toml"), source)
                .expect("write owner project registration");
        }
    }

    impl Drop for ProjectFixture {
        fn drop(&mut self) {
            if self.root.parent() == Some(self.base.as_path())
                && self
                    .root
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("eliot-project-loader-"))
            {
                let _ = fs::remove_dir_all(&self.root);
            }
        }
    }

    fn toml_string(value: &str) -> String {
        serde_json::to_string(value).expect("encode fixture TOML string")
    }

    fn toml_array(values: &[&str]) -> String {
        format!(
            "[{}]",
            values
                .iter()
                .map(|value| toml_string(value))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn git_argument_path(path: &Path) -> String {
        crate::git_monitor::git_path_argument(path).expect("normalize Git fixture argument path")
    }

    #[cfg(windows)]
    fn cmd_argument_path(path: &Path) -> String {
        git_argument_path(path).replace('/', "\\")
    }

    async fn sole_registration(fixture: &ProjectFixture) -> RegistrationSnapshot {
        let mut registrations = load_registrations(&fixture.data_root)
            .await
            .expect("load owner registrations");
        assert_eq!(registrations.len(), 1);
        registrations.remove(0)
    }

    #[tokio::test]
    async fn owner_allowlists_default_to_deny_and_changes_fail_revalidation() {
        let fixture = ProjectFixture::new();
        let checkout = fixture.repository("registered");
        fixture.write_rule(&checkout, "project-stop");
        fixture.write_registration(&checkout, None, None);

        let configured = config::load_project_registration_config(&fixture.data_root)
            .expect("parse default-deny registration");
        assert!(configured[0].profiles.is_empty());
        assert!(configured[0].actions.is_empty());
        let unapproved = sole_registration(&fixture).await;
        let error = load_project_rules(&fixture.data_root, unapproved)
            .await
            .err()
            .expect("project rule without action approval must be denied");
        assert_eq!(error.code, "not_allowed");

        fixture.write_registration(&checkout, Some(&["fake"]), Some(&["stop"]));
        let approved = sole_registration(&fixture).await;
        fixture.write_registration(&checkout, Some(&[]), Some(&["stop"]));
        assert_eq!(
            verify_registration(&fixture.data_root, &approved)
                .await
                .expect_err("profile approval changes must invalidate the snapshot")
                .code,
            "not_allowed"
        );
        fixture.write_registration(&checkout, Some(&["fake"]), Some(&[]));
        assert_eq!(
            verify_registration(&fixture.data_root, &approved)
                .await
                .expect_err("action approval changes must invalidate the snapshot")
                .code,
            "not_allowed"
        );
    }

    #[tokio::test]
    async fn rules_come_only_from_the_canonical_owner_registered_checkout() {
        let fixture = ProjectFixture::new();
        let checkout = fixture.repository("registered");
        let unregistered_workspace = fixture.root.join("agent-worktree");
        fs::create_dir(&unregistered_workspace).expect("create unregistered workspace");
        fixture.write_rule(&checkout, "registered-rule");
        fixture.write_rule(&unregistered_workspace, "workspace-rule");
        fixture.write_registration(&checkout, Some(&["fake"]), Some(&["stop"]));

        let registration = sole_registration(&fixture).await;
        assert_eq!(
            registration.checkout.as_str(),
            fs::canonicalize(&checkout)
                .expect("canonical registered checkout")
                .to_string_lossy()
                .as_ref()
        );
        let loaded = load_project_rules(&fixture.data_root, registration)
            .await
            .expect("load registered checkout rules");
        assert_eq!(loaded.principal, "project:app");
        assert_eq!(loaded.snapshot.definitions.len(), 1);
        assert_eq!(loaded.snapshot.definitions[0].id, "registered-rule");
        assert_eq!(loaded.snapshot.inputs.len(), 1);
        assert_eq!(loaded.snapshot.inputs[0].id, "registered-rule");
    }

    #[tokio::test]
    async fn linked_rules_path_and_project_config_overlay_are_rejected() {
        let fixture = ProjectFixture::new();
        let checkout = fixture.repository("registered");
        let external_rules = fixture.root.join("external-rules");
        fs::create_dir(&external_rules).expect("create external rules directory");
        fs::write(
            external_rules.join("rule.toml"),
            "[[rule]]\nid = 'linked-rule'\non = { event = 'turn.completed' }\ndo = [{ action = 'stop', target = 'a1', expected_generation = 1 }]\n",
        )
        .expect("write external rule");
        let metadata = checkout.join(".eliot");
        fs::create_dir(&metadata).expect("create project metadata directory");
        let linked_rules = metadata.join("rules");
        create_directory_link(&external_rules, &linked_rules);
        fixture.write_registration(&checkout, Some(&["fake"]), Some(&["stop"]));

        let registration = sole_registration(&fixture).await;
        let error = load_project_rules(&fixture.data_root, registration.clone())
            .await
            .err()
            .expect("linked rules directory must be rejected");
        assert_eq!(error.code, "unavailable");

        fs::write(metadata.join("config.toml"), "overlay = true\n")
            .expect("write unsupported project overlay");
        let error = load_project_rules(&fixture.data_root, registration)
            .await
            .err()
            .expect("project config overlay must be rejected");
        assert_eq!(error.code, "unsupported");
    }

    #[tokio::test]
    async fn managed_worktree_common_directory_change_at_same_path_is_rejected() {
        let fixture = ProjectFixture::new();
        let checkout = fixture.repository("registered");
        fixture.run_git(
            &checkout,
            &[
                "-c",
                "user.name=Fixture",
                "-c",
                "user.email=fixture@example.invalid",
                "commit",
                "--allow-empty",
                "-m",
                "initial",
            ],
        );

        let worktree = fixture.root.join("managed-worktree");
        let worktree_text = worktree.to_string_lossy().into_owned();
        let worktree_git_arg = git_argument_path(&worktree);
        fixture.run_git(
            &checkout,
            &["worktree", "add", "--detach", &worktree_git_arg, "HEAD"],
        );

        let git = WorkspaceGit::new().expect("resolve Git");
        let (canonical_checkout, registered_common_dir) = git
            .repository_identity(&checkout.to_string_lossy())
            .await
            .expect("read registered repository identity");
        let (canonical_worktree, initial_common_dir) = git
            .repository_identity(&worktree_text)
            .await
            .expect("read managed worktree identity");
        assert_eq!(initial_common_dir, registered_common_dir);

        let registration = RegistrationSnapshot {
            id: "app".to_owned(),
            checkout: canonical_checkout.clone(),
            repo_common_dir: registered_common_dir.clone(),
            profiles: vec!["fake".to_owned()],
            actions: vec!["stop".to_owned()],
        };
        let workspace = eliot_core::WorkspacePreparation {
            agent: "a1".to_owned(),
            target_generation: 1,
            start_command_id: ulid::Ulid::new().to_string(),
            project: canonical_checkout.clone(),
            repo_path: canonical_checkout,
            repo_common_dir: registered_common_dir.clone(),
            requested_branch: "detached".to_owned(),
            base_ref: None,
            base_oid: String::new(),
            worktree_path: canonical_worktree.clone(),
            frozen_setup: Vec::new(),
            phase: eliot_core::WorkspacePhase::Ready,
            setup_index: 0,
            outcome: None,
            updated_at: 0,
        };

        verify_managed_workspace_identity(&registration, &workspace, &workspace.worktree_path, 1)
            .await
            .expect("accept original managed worktree identity");

        let mut setup_ready_next_generation = workspace.clone();
        setup_ready_next_generation.phase = eliot_core::WorkspacePhase::SetupReady;
        setup_ready_next_generation.target_generation = 2;
        assert_eq!(
            verify_managed_workspace_identity(
                &registration,
                &setup_ready_next_generation,
                &setup_ready_next_generation.worktree_path,
                1,
            )
            .await
            .expect_err("SetupReady next-generation workspace is not dispatch-ready")
            .code,
            "not_allowed"
        );

        let mut ready_wrong_generation = workspace.clone();
        ready_wrong_generation.target_generation = 2;
        assert_eq!(
            verify_managed_workspace_identity(
                &registration,
                &ready_wrong_generation,
                &ready_wrong_generation.worktree_path,
                1,
            )
            .await
            .expect_err("Ready workspace for another generation must be rejected")
            .code,
            "not_allowed"
        );

        fixture.run_git(
            &checkout,
            &["worktree", "remove", "--force", &worktree_git_arg],
        );
        let replacement_git_dir = fixture.root.join("replacement-git-dir");
        let separate_git_dir_arg = format!(
            "--separate-git-dir={}",
            git_argument_path(&replacement_git_dir)
        );
        let replacement = Command::new("git")
            .args(["init", "--quiet"])
            .arg(separate_git_dir_arg)
            .arg(&worktree_git_arg)
            .output()
            .expect("create replacement Git identity");
        assert!(
            replacement.status.success(),
            "replacement git init failed: {}",
            String::from_utf8_lossy(&replacement.stderr)
        );

        let (replacement_root, replacement_common_dir) = git
            .repository_identity(&worktree_text)
            .await
            .expect("read replacement repository identity");
        assert_eq!(replacement_root, canonical_worktree);
        assert_ne!(replacement_common_dir, registered_common_dir);
        assert_eq!(
            verify_managed_workspace_identity(
                &registration,
                &workspace,
                &workspace.worktree_path,
                1,
            )
            .await
            .expect_err("changed common directory must fail closed")
            .code,
            "not_allowed"
        );
    }

    #[cfg(unix)]
    fn create_directory_link(target: &Path, link: &Path) {
        std::os::unix::fs::symlink(target, link).expect("create project rules symlink");
    }

    #[cfg(windows)]
    fn create_directory_link(target: &Path, link: &Path) {
        let link = cmd_argument_path(link);
        let target = cmd_argument_path(target);
        let output = Command::new("cmd.exe")
            .args(["/d", "/c", "mklink", "/J", &link, &target])
            .output()
            .expect("create project rules junction");
        assert!(
            output.status.success(),
            "mklink failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
