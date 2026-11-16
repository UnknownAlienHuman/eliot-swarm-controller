//! Owner configuration screens' RPCs: the sanitized profile list, the
//! `[profiles.*]` source read and its validated save, and the rule list.
//!
//! Profiles are resolved from `config.toml` at every launch admission, so
//! these reads report exactly what the next launch will use; the save
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
        let mut store = self.inner.store.lock().await;
        let reply = crate::config::settings::read_internal(
            &self.inner.data_root,
            &mut store,
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
        drop(store);
        // `[daemon] log_level` applies live: refresh it from the saved
        // file so standard/debug switches without a restart. Other daemon
        // keys still report through `restart_required`.
        if reply.saved {
            Self::apply_saved_log_level(&self.inner.data_root);
        }
        if !events.is_empty() {
            self.signal_progress();
        }
        request.validate_response(&reply)?;
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

    fn entry(id: &str, definition: Value) -> RuleListEntry {
        RuleListEntry {
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
