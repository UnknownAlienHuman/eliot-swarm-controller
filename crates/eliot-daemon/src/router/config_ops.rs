//! Owner configuration screens' RPCs: the sanitized profile list, the
//! `[profiles.*]` source read and its validated save, and the rule list.
//!
//! Profiles resolve from `config.toml` at every launch admission against a
//! revision-keyed validated snapshot, so these reads report exactly what the
//! next launch will use; file validation runs once per content revision. The save
//! delegates to `config::profiles_edit`, which reuses the launch parser and
//! resolution. `rule.list` pages the stored rule states of one namespace with
//! their canonical definitions and timer states; it changes nothing.
use super::{PAGE_MAX, Principal, Router, require_owner, rule_principal};
use eliot_core::{
    Error, Result, SecretPolicy,
    api::{
        ProfileListRequest, ProfilesReadRequest, ProfilesSaveRequest, ProjectRegistrationView,
        RuleListEntry, RuleListRequest, RuleListResponse, ScriptListRequest, ScriptReadRequest,
        ScriptSaveRequest, SettingsReadRequest, SettingsSaveRequest,
    },
};
use serde_json::Value;

impl Router {
    fn apply_assignment_pauses_locked(
        store: &mut eliot_store::Store,
        config: crate::config::AutomationConfig,
    ) -> Result<bool> {
        let rules: Vec<_> = config
            .assignments
            .into_iter()
            .filter(|(_, assignment)| !assignment.enabled)
            .map(|(key, _)| format!("assignment-{key}"))
            .collect();
        store.set_paused_assignment_timers(&rules)
    }

    pub(crate) async fn refresh_assignment_pauses(&self) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        // Invalid source retains the last saved pause state.
        if let Ok(config) = crate::config::load_automation_config(&self.inner.data_root) {
            Self::apply_assignment_pauses_locked(&mut store, config)?;
        }
        Ok(())
    }

    fn apply_saved_log_level(data_root: &std::path::Path) {
        // Best effort: a read failure keeps the current level; the next
        // save or restart repairs it. Never fails the save itself.
        if let Ok(config) = crate::config::load_daemon_config(data_root)
            && let Ok(level) = crate::logging::LogLevel::parse(&config.log_level)
        {
            crate::logging::set_log_level(level);
        }
    }
}

/// A response produced after a mutation committed that fails local
/// validation: report `unknown`, never a pre-admission rejection code.
fn post_write_validation(what: &str, error: Error) -> Error {
    Error::new(
        "unknown",
        format!("{what} applied but the response failed validation: {error}"),
    )
}

fn encode<T: serde::Serialize>(value: &T, what: &str) -> Result<Value> {
    serde_json::to_value(value)
        .map_err(|_| Error::new("internal", format!("{what} could not be encoded")))
}

impl Router {
    pub(super) async fn profile_list(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ProfileListRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let reply = crate::config::profiles_edit::list(&self.inner.data_root, &policy)?;
        request.validate_response(&reply)?;
        encode(&reply, "profile list")
    }

    pub(super) async fn config_profiles_read(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ProfilesReadRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let reply = crate::config::profiles_edit::read(&self.inner.data_root, &policy)?;
        request.validate_response(&reply)?;
        encode(&reply, "profile source")
    }

    pub(super) async fn config_profiles_save(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ProfilesSaveRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let mut store = self.inner.store.lock().await;
        let reply = crate::config::profiles_edit::save(
            &self.inner.data_root,
            &mut store,
            &request,
            &policy,
        )?;
        request.validate_response(&reply)?;
        encode(&reply, "profile save")
    }

    pub(super) async fn settings_read(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: SettingsReadRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        let store = self.inner.store.lock().await;
        let reply = crate::config::settings::read_internal(
            &self.inner.data_root,
            &store,
            request.sections.as_deref(),
            &policy,
        )?;
        request.validate_response(&reply)?;
        encode(&reply, "settings read")
    }

    pub(super) async fn settings_save(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: SettingsSaveRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let policy = self.secret_policy_snapshot();
        // Resolve the candidate before publication, including dry runs. Keep
        // Git I/O outside the Store lock and bind its result to the source CAS.
        let projects = if request
            .sections
            .contains_key(&eliot_core::api::SettingsSection::Projects)
        {
            let (source, candidate) = crate::config::settings::prepare_candidate(
                &self.inner.data_root,
                request.expected_revision.as_deref(),
                &request.sections,
                &policy,
            )?;
            Some(crate::config::settings::ResolvedProjects {
                source_revision: source
                    .as_deref()
                    .map(crate::config::profiles_edit::revision_of),
                candidate_revision: crate::config::profiles_edit::revision_of(&candidate),
                inputs: crate::automation::project_registration_inputs(
                    crate::config::settings::project_configs(&candidate)?,
                )
                .await?,
            })
        } else {
            None
        };
        let mut store = self.inner.store.lock().await;
        let (reply, events) = crate::config::settings::save(
            &self.inner.data_root,
            &mut store,
            &request,
            &policy,
            projects.as_ref(),
        )?;
        self.publish_locked(&events);
        let pause_changed = if !request.dry_run
            && request
                .sections
                .contains_key(&eliot_core::api::SettingsSection::Automation)
        {
            crate::config::load_automation_config_snapshot(&self.inner.data_root)
                .and_then(|(revision, config)| {
                    if revision != reply.revision {
                        return Err(Error::new(
                            "conflict",
                            "settings changed before the pause applied",
                        ));
                    }
                    Self::apply_assignment_pauses_locked(&mut store, config)
                })
                .map_err(|_| {
                    Error::new(
                        "unknown",
                        "settings were saved but assignment pause readback is uncertain",
                    )
                })?
        } else {
            false
        };
        // `[daemon] log_level` applies live: refresh it from the saved
        // file so standard/debug switches without a restart. Other daemon
        // keys still report through `restart_required`.
        if reply.saved {
            Self::apply_saved_log_level(&self.inner.data_root);
        }
        // Publication and application share the Store writer lock. A later
        // save cannot publish or apply a newer level before this one applies.
        drop(store);
        if pause_changed || !events.is_empty() {
            self.signal_progress();
        }
        // The save committed above: a response that fails local validation
        // is an unknown effect, never a pre-admission rejection.
        request
            .validate_response(&reply)
            .map_err(|error| post_write_validation("settings save", error))?;
        encode(&reply, "settings save")
    }

    pub(super) async fn rule_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleListRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let namespace = rule_principal(request.project.as_deref());
        let limit = request.limit.min(PAGE_MAX);
        let store = self.inner.store.lock().await;
        let states =
            store.list_rule_processing_states(&namespace, request.after.as_deref(), limit)?;
        let mut rules = Vec::with_capacity(states.len());
        for state in states {
            let definition: Value =
                serde_json::from_str(&state.current_definition).map_err(|_| {
                    Error::new("source_integrity", "stored rule definition is not JSON")
                })?;
            let timer = store.get_timer_state(&namespace, &state.view.id)?;
            rules.push(RuleListEntry {
                state: state.view,
                source_present: state.source_present,
                timer,
                definition,
            });
        }
        drop(store);
        let policy = self.secret_policy_snapshot();
        let rules = withhold_unsafe_rules(rules, &policy);
        // Like `agent.list`: every nonempty page names its last id and the
        // caller continues until an empty page, so a byte-budget-shortened
        // page never hides the rest of the namespace.
        let next = rules.last().map(|entry| entry.state.id.clone());
        let reply = RuleListResponse { rules, next };
        request.validate_response(&reply)?;
        encode(&reply, "rule list")
    }

    /// Registered projects whose `.eliot/scripts/` roots scripts may live in.
    async fn script_registrations(
        &self,
        project: Option<&str>,
    ) -> Result<Vec<ProjectRegistrationView>> {
        match project {
            Some(id) => Ok(self
                .inner
                .store
                .lock()
                .await
                .get_project_registration("owner", id)?
                .into_iter()
                .collect()),
            None => Ok(Vec::new()),
        }
    }

    pub(super) async fn script_list(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ScriptListRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let (root_index, _) = request.position()?;
        let store = self.inner.store.lock().await;
        // Keep root inventory bounded too: retain only one Store page while
        // locating the numeric root. The cursor never carries a secret name.
        let mut after = None;
        let mut remaining = root_index.saturating_sub(1);
        let (registration, more) = loop {
            let page = store.list_project_registrations("owner", after.as_deref(), PAGE_MAX)?;
            if root_index == 0 {
                break (None, !page.is_empty());
            }
            if remaining < page.len() {
                let selected = page[remaining].clone();
                let more = remaining + 1 < page.len()
                    || !store
                        .list_project_registrations("owner", Some(&selected.id), 1)?
                        .is_empty();
                break (Some(selected), more);
            }
            if page.is_empty() {
                return encode(
                    &eliot_core::api::ScriptListResponse {
                        roots: Vec::new(),
                        next_cursor: None,
                    },
                    "script list",
                );
            }
            remaining -= page.len();
            after = page.last().map(|r| r.id.clone());
        };
        drop(store);
        let policy = self.secret_policy_snapshot();
        let reply = crate::scripts::edit::list(
            &self.inner.data_root,
            registration.as_ref(),
            &request,
            more,
            &policy,
        )?;
        request.validate_response(&reply)?;
        encode(&reply, "script list")
    }

    pub(super) async fn script_read(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ScriptReadRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let registrations = self
            .script_registrations(request.project.as_deref())
            .await?;
        let policy = self.secret_policy_snapshot();
        let reply =
            crate::scripts::edit::read(&self.inner.data_root, &registrations, &request, &policy)?;
        request.validate_response(&reply)?;
        encode(&reply, "script read")
    }

    pub(super) async fn script_save(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: ScriptSaveRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let registrations = self
            .script_registrations(request.project.as_deref())
            .await?;
        let policy = self.secret_policy_snapshot();
        let reply =
            crate::scripts::edit::save(&self.inner.data_root, &registrations, &request, &policy)?;
        request.validate_response(&reply)?;
        encode(&reply, "script save")
    }
}

/// Drop rule entries whose complete stored state, timer state and definition
/// carry a value the current policy knows (keys included). Stored text can
/// predate a secret rotation, so listing re-checks every entry; withheld
/// entries keep their ids out of `next`, and an all-withheld page ends the
/// listing like any empty page.
fn withhold_unsafe_rules(rules: Vec<RuleListEntry>, policy: &SecretPolicy) -> Vec<RuleListEntry> {
    rules
        .into_iter()
        .filter(|entry| {
            serde_json::to_value(entry).is_ok_and(|value| !policy.contains_value(&value))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::api::RuleStateView;
    use serde_json::json;

    struct SettingsRoot(std::path::PathBuf);

    impl SettingsRoot {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("eliot-project-settings-{}", ulid::Ulid::new()));
            crate::platform::DataDir::open(&path).unwrap();
            crate::platform::data_dir::replace_private_file(
                &path,
                "config.toml",
                b"[profiles.base]\nharness='codex'\n",
            )
            .unwrap();
            Self(path)
        }
    }
    impl Drop for SettingsRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn saved_assignment_pause_fences_a_stale_slot_with_a_malformed_rules_file() {
        use eliot_core::api::{SettingsSection, TimerRunRequest};
        let root = SettingsRoot::new();
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        let request = |enabled| {
            let revision = crate::config::settings::current_revision(&root.0).unwrap();
            serde_json::to_value(SettingsSaveRequest {
                expected_revision: revision,
                sections: [(
                    SettingsSection::Automation,
                    json!({"assignments":{"queue":{
                        "folder":root.0, "workspace":root.0,
                        "interval":"1h", "max_busy":1, "enabled":enabled
                    }}}),
                )]
                .into(),
                dry_run: false,
                client: eliot_core::api::SettingsClientSource::Desktop,
            })
            .unwrap()
        };
        router
            .settings_save(&request(true), &Principal::Owner)
            .await
            .unwrap();
        crate::automation::initialize_global_rules(&router, &root.0, &SecretPolicy::default())
            .await
            .unwrap();
        let before = router
            .get_rule_timer("owner", "assignment-queue")
            .await
            .unwrap()
            .unwrap();
        assert!(before.enabled);
        let assignment = router.assignment_status("queue", 1).await.unwrap();
        let rules = root.0.join("rules");
        std::fs::create_dir_all(&rules).unwrap();
        std::fs::write(rules.join("unrelated.toml"), "broken [").unwrap();

        let reply = router
            .settings_save(&request(false), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(reply["saved"], true);
        let after = router
            .get_rule_timer("owner", "assignment-queue")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after,
            eliot_core::api::TimerStateView {
                enabled: false,
                ..before.clone()
            }
        );

        let scheduled_at_ms = before.next_due_at.unwrap();
        let slot = eliot_store::TimerSlotCommit {
            scheduled_at_ms,
            next_due_at_ms: Some(scheduled_at_ms + 3_600_000),
            action: None,
            skip_code: Some("missed".to_owned()),
            skip_capacity_message: None,
        };
        let late = router
            .commit_rule_timer_slot("owner", None, &before, slot, None, scheduled_at_ms)
            .await
            .unwrap()
            .unwrap();
        assert!(late.parent.is_none() && !late.is_new && late.events.is_empty());
        assert_eq!(late.state, after);
        let manual = TimerRunRequest::new(ulid::Ulid::new().to_string(), "assignment-queue");
        let error = router
            .timer_run(&serde_json::to_value(manual).unwrap(), &Principal::Owner)
            .await
            .unwrap_err();
        assert_eq!(error.code, "bad_state");
        assert!(error.message.contains("disabled or paused"));
        assert_eq!(
            router.assignment_status("queue", 1).await.unwrap(),
            assignment
        );
        let list = router
            .rule_list(&json!({}), &Principal::Owner)
            .await
            .unwrap();
        let list: RuleListResponse = serde_json::from_value(list).unwrap();
        assert!(list.rules.iter().any(|entry| {
            entry.state.id == "assignment-queue.complete"
                && entry.state.enabled
                && entry.source_present
        }));
    }

    #[tokio::test]
    async fn rule_list_reports_removal_of_an_already_disabled_rule() {
        let root = SettingsRoot::new();
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        let rules = root.0.join("rules");
        std::fs::create_dir_all(&rules).unwrap();
        let path = rules.join("disabled.toml");
        std::fs::write(&path, "[[rule]]\nid='disabled'\nenabled=false\non={every='1h'}\ndo=[{action='stop',target='a1',expected_generation=1}]\n").unwrap();
        for present in [true, false] {
            if !present {
                std::fs::remove_file(&path).unwrap();
            }
            crate::automation::initialize_global_rules(&router, &root.0, &SecretPolicy::default())
                .await
                .unwrap();
            let list = router
                .rule_list(&json!({}), &Principal::Owner)
                .await
                .unwrap();
            let list: RuleListResponse = serde_json::from_value(list).unwrap();
            let entry = list
                .rules
                .iter()
                .find(|entry| entry.state.id == "disabled")
                .unwrap();
            assert!(!entry.state.enabled);
            assert_eq!(entry.source_present, present);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_log_level_saves_apply_in_publication_order() {
        let root = SettingsRoot::new();
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        let request = |path: &std::path::Path, level: &str| {
            let source = std::fs::read_to_string(path.join("config.toml")).unwrap();
            serde_json::to_value(SettingsSaveRequest {
                expected_revision: Some(crate::config::profiles_edit::revision_of(&source)),
                sections: [(
                    eliot_core::api::SettingsSection::Daemon,
                    json!({"log_level":level}),
                )]
                .into(),
                dry_run: false,
                client: eliot_core::api::SettingsClientSource::Cli,
            })
            .unwrap()
        };
        let a = router.clone();
        let b = router.clone();
        let first = tokio::spawn(async move {
            a.settings_save(&request(&a.inner.data_root, "debug"), &Principal::Owner)
                .await
        });
        let second = tokio::spawn(async move {
            b.settings_save(&request(&b.inner.data_root, "standard"), &Principal::Owner)
                .await
        });
        let results = [first.await.unwrap(), second.await.unwrap()];
        assert!(results.iter().any(Result::is_ok));
        for result in results {
            if let Err(error) = result {
                assert_eq!(error.code, "conflict");
            }
        }
        let config = crate::config::load_daemon_config(&root.0).unwrap();
        assert_eq!(crate::logging::is_debug(), config.log_level == "debug");
        crate::logging::set_log_level(crate::logging::LogLevel::Standard);
    }

    fn project_request(
        root: &SettingsRoot,
        checkout: &std::path::Path,
        action: &str,
    ) -> SettingsSaveRequest {
        let source = std::fs::read_to_string(root.0.join("config.toml")).unwrap();
        SettingsSaveRequest {
            expected_revision: Some(crate::config::profiles_edit::revision_of(&source)),
            sections: [(
                eliot_core::api::SettingsSection::Projects,
                json!({"sample":{"path":checkout,"profiles":["base"],"actions":[action]}}),
            )]
            .into(),
            dry_run: false,
            client: eliot_core::api::SettingsClientSource::Cli,
        }
    }

    #[tokio::test]
    async fn non_repository_project_is_rejected_before_dry_run_and_publication() {
        let root = SettingsRoot::new();
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        let original = std::fs::read(root.0.join("config.toml")).unwrap();
        let mut request = project_request(&root, &root.0, "launch");
        let mut errors = Vec::new();
        for dry_run in [true, false] {
            request.dry_run = dry_run;
            errors.push(
                router
                    .settings_save(&serde_json::to_value(&request).unwrap(), &Principal::Owner)
                    .await
                    .unwrap_err()
                    .code,
            );
            assert_eq!(std::fs::read(root.0.join("config.toml")).unwrap(), original);
            let store = router.inner.store.lock().await;
            assert!(store.list_settings_history(100).unwrap().is_empty());
            assert!(
                store
                    .list_project_registrations("owner", None, 100)
                    .unwrap()
                    .is_empty()
            );
        }
        assert_eq!(errors[0], errors[1]);
        assert_ne!(errors[0], "unknown");
    }

    #[tokio::test]
    async fn missing_project_folder_is_a_definite_invalid_refusal() {
        let root = SettingsRoot::new();
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        let original = std::fs::read(root.0.join("config.toml")).unwrap();
        let request = project_request(&root, &root.0.join("missing"), "launch");
        let error = router
            .settings_save(&serde_json::to_value(&request).unwrap(), &Principal::Owner)
            .await
            .unwrap_err();
        // Nothing was written: never `unavailable`, which clients must read
        // as an unknown outcome.
        assert_eq!(error.code, "invalid");
        assert!(error.message.contains("workspace cannot be resolved"));
        assert_eq!(std::fs::read(root.0.join("config.toml")).unwrap(), original);
    }

    #[tokio::test]
    async fn delayed_project_resolution_cannot_restore_stale_authority() {
        let root = SettingsRoot::new();
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        let checkout = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        let slow = project_request(&root, checkout, "launch");
        let policy = router.secret_policy_snapshot();
        let (source, candidate) = crate::config::settings::prepare_candidate(
            &root.0,
            slow.expected_revision.as_deref(),
            &slow.sections,
            &policy,
        )
        .unwrap();
        let resolved = crate::config::settings::ResolvedProjects {
            source_revision: source
                .as_deref()
                .map(crate::config::profiles_edit::revision_of),
            candidate_revision: crate::config::profiles_edit::revision_of(&candidate),
            inputs: crate::automation::project_registration_inputs(
                crate::config::settings::project_configs(&candidate).unwrap(),
            )
            .await
            .unwrap(),
        };
        // Deterministic interleaving: A's Git work finishes only after B publishes.
        let fast = project_request(&root, checkout, "notify");
        router
            .settings_save(&serde_json::to_value(&fast).unwrap(), &Principal::Owner)
            .await
            .unwrap();
        let published = std::fs::read(root.0.join("config.toml")).unwrap();
        let mut store = router.inner.store.lock().await;
        let latest = store
            .get_project_registration("owner", "sample")
            .unwrap()
            .unwrap();
        let error =
            crate::config::settings::save(&root.0, &mut store, &slow, &policy, Some(&resolved))
                .unwrap_err();
        assert_eq!(error.code, "conflict");
        assert_eq!(
            std::fs::read(root.0.join("config.toml")).unwrap(),
            published
        );
        assert_eq!(
            store
                .get_project_registration("owner", "sample")
                .unwrap()
                .unwrap(),
            latest
        );
        assert_eq!(latest.actions, ["notify"]);
        assert_eq!(store.list_settings_history(100).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn project_save_refuses_linked_checkout_admitted_after_resolution() {
        let root = SettingsRoot::new();
        let checkout = root.0.join("repo");
        let linked = root.0.join("linked");
        std::fs::create_dir(&checkout).unwrap();
        let git = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .current_dir(&checkout)
                .args(args)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        };
        git(&["init"]);
        git(&[
            "-c",
            "user.name=EliotTest",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--allow-empty",
            "-m",
            "root",
        ]);
        git(&[
            "worktree",
            "add",
            "-b",
            "linked-task",
            linked.to_str().unwrap(),
        ]);
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        let request = project_request(&root, &checkout, "launch");
        router
            .settings_save(&serde_json::to_value(request).unwrap(), &Principal::Owner)
            .await
            .unwrap();

        let mut removal = project_request(&root, &checkout, "launch");
        removal
            .sections
            .insert(eliot_core::api::SettingsSection::Projects, json!({}));
        let policy = router.secret_policy_snapshot();
        let (source, candidate) = crate::config::settings::prepare_candidate(
            &root.0,
            removal.expected_revision.as_deref(),
            &removal.sections,
            &policy,
        )
        .unwrap();
        let resolved = crate::config::settings::ResolvedProjects {
            source_revision: removal.expected_revision.clone(),
            candidate_revision: crate::config::profiles_edit::revision_of(&candidate),
            inputs: Vec::new(),
        };
        let mut launch = eliot_core::SubmitCommand::new(ulid::Ulid::new().to_string(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({
            "project": linked,
            "workspace": linked,
            "harness": "codex",
            "profile": "base",
            "profile_snapshot": {"harness": "codex"},
        });
        let mut store = router.inner.store.lock().await;
        store
            .submit("owner", launch, crate::common::now_ms())
            .unwrap();
        let error =
            crate::config::settings::save(&root.0, &mut store, &removal, &policy, Some(&resolved))
                .unwrap_err();
        assert_eq!(error.code, "project_in_use");
        assert_eq!(
            std::fs::read_to_string(root.0.join("config.toml")).unwrap(),
            source.unwrap()
        );
        assert!(
            store
                .get_project_registration("owner", "sample")
                .unwrap()
                .is_some()
        );
        assert_eq!(store.list_settings_history(100).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn selected_stale_cleanup_rechecks_restoration_and_leaves_other_missing_project() {
        use eliot_core::api::{ArtifactKind, CleanupApplyResponse, CleanupPlan};
        let root = SettingsRoot::new();
        let one = root.0.join("one");
        let two = root.0.join("two");
        let config = root.0.join("config.toml");
        let source = std::fs::read_to_string(&config).unwrap();
        std::fs::write(&config, format!("{source}\n[projects.one]\npath = {}\nprofiles = []\nactions = []\n\n[projects.two]\npath = {}\nprofiles = []\nactions = []\n", json!(one), json!(two))).unwrap();
        let router = crate::router::tests::router_with_data_root(root.0.clone());
        for restored in [true, false] {
            let plan: CleanupPlan = serde_json::from_value(
                router
                    .cleanup_plan(
                        &json!({"scope":"project","project":"one"}),
                        &Principal::Owner,
                    )
                    .await
                    .unwrap(),
            )
            .unwrap();
            let item = plan
                .items
                .iter()
                .find(|item| item.artifact.kind == ArtifactKind::ProjectRegistration)
                .unwrap();
            assert_eq!(item.artifact.project, "one");
            assert_eq!(item.verdict, "safe");
            if restored {
                std::fs::create_dir(&one).unwrap();
            }
            let reply: CleanupApplyResponse = serde_json::from_value(router.cleanup_apply(
                &json!({"id":ulid::Ulid::new().to_string(),"plan_id":plan.plan_id,"artifacts":[item.artifact.id]}),
                &Principal::Owner,
            ).await.unwrap()).unwrap();
            let result = reply
                .items
                .iter()
                .find(|result| result.artifact_id == item.artifact.id)
                .unwrap();
            assert_eq!(result.outcome, if restored { "skipped" } else { "removed" });
            let current = std::fs::read_to_string(&config).unwrap();
            let projects = crate::config::settings::project_configs(&current).unwrap();
            assert!(projects.iter().any(|project| project.id == "two"));
            assert_eq!(projects.iter().any(|project| project.id == "one"), restored);
            if restored {
                assert_eq!(result.reason.as_deref(), Some("project_path_present"));
                std::fs::remove_dir(&one).unwrap();
            }
        }
    }

    fn entry(id: &str, definition: Value) -> RuleListEntry {
        RuleListEntry {
            source_present: true,
            state: RuleStateView {
                id: id.to_owned(),
                project: None,
                revision: 1,
                current_cursor: 0,
                retiring_revision: None,
                retiring_cursor: None,
                retire_through: None,
                enabled: true,
                paused_reason: None,
            },
            timer: None,
            definition,
        }
    }

    #[test]
    fn post_write_validation_failure_reports_unknown() {
        use eliot_core::api::{
            SettingsClientSource, SettingsSaveRequest, SettingsSaveResponse, SettingsSection,
        };
        // A save that committed but whose reply is inconsistent fails
        // response validation with `invalid`; the daemon must report
        // `unknown` at its source so the CLI never reads it as a
        // pre-admission rejection.
        let request = SettingsSaveRequest {
            expected_revision: None,
            sections: [(SettingsSection::Automation, json!({}))]
                .into_iter()
                .collect(),
            dry_run: false,
            client: SettingsClientSource::Desktop,
        };
        let reply = SettingsSaveResponse {
            saved: true,
            revision: None,
            changed_sections: Vec::new(),
            restart_required: Vec::new(),
            history_revision: Some(1),
        };
        let error = request.validate_response(&reply).unwrap_err();
        assert_eq!(error.code, "invalid");
        let reported = post_write_validation("settings save", error);
        assert_eq!(reported.code, "unknown");
        assert!(
            reported.message.contains("applied"),
            "the message must say the write happened: {}",
            reported.message
        );
    }

    #[test]
    fn rule_list_withholds_entries_carrying_known_secrets() {
        let policy = SecretPolicy::new(vec!["rotated-owner-token-1".to_owned()]);
        let rules = vec![
            entry("clean", json!({"on": {}, "do": [{"text": "hello"}]})),
            entry(
                "leaky-value",
                json!({"on": {}, "do": [{"text": "token rotated-owner-token-1 here"}]}),
            ),
            entry("leaky-key", json!({"rotated-owner-token-1": "x"})),
        ];
        let kept = withhold_unsafe_rules(rules, &policy);
        let ids: Vec<&str> = kept.iter().map(|entry| entry.state.id.as_str()).collect();
        assert_eq!(ids, vec!["clean"]);
    }
}
