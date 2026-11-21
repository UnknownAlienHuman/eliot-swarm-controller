//! Owner configuration screens' RPCs: the sanitized profile list, the
//! settings read and save, and the rule list.
//!
//! Profiles resolve from `config.toml` at every launch admission against a
//! revision-keyed validated snapshot, so these reads report exactly what the
//! next launch will use; file validation runs once per content revision.
//! `profile.list` delegates to `config::profiles_edit`, which reuses the launch
//! parser and resolution. `rule.list` pages the stored rule states of one namespace with
//! their canonical definitions and timer states; it changes nothing.
use super::{PAGE_MAX, Principal, Router, require_owner, rule_principal};
use eliot_core::{
    Error, Result, SecretPolicy,
    api::{
        ProfileListRequest, ProjectRegistrationView, RuleDeleteRequest, RuleListEntry,
        RuleListRequest, RuleListResponse, RuleReadRequest, RuleSaveRequest, ScriptListRequest,
        ScriptReadRequest, ScriptSaveRequest, SettingsReadRequest, SettingsSaveRequest,
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
        // File read, parses and validation run before the Store lock; only
        // the author history is read under it.
        let snapshot = crate::config::settings::read_snapshot(
            &self.inner.data_root,
            request.sections.as_deref(),
            &policy,
        )?;
        let reply = snapshot.into_response(&*self.inner.store.lock().await)?;
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
        let projects = crate::config::recovery::resolve_projects_for_save(
            &self.inner.data_root,
            &request,
            &policy,
        )
        .await?;
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
                .or_else(|_| {
                    crate::config::settings::automation_config_snapshot_for_settings(
                        &self.inner.data_root,
                    )
                })
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
        if reply.saved && !request.dry_run {
            self.signal_launch_arbitration();
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
            let source_file = if state.source_present {
                self.rule_source_file(&namespace, &state.view.id)
            } else {
                None
            };
            rules.push(RuleListEntry {
                state: state.view,
                source_present: state.source_present,
                source_file,
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

    pub(super) async fn rule_read(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleReadRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        let policy = self.secret_policy_snapshot();
        let reply =
            crate::automation::rule_edit::read(&self.inner.data_root, &request, &policy).await?;
        encode(&reply, "rule read")
    }

    pub(super) async fn rule_save(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleSaveRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        let policy = self.secret_policy_snapshot();
        let reply =
            crate::automation::rule_edit::save(&self.inner.data_root, &request, &policy).await?;
        encode(&reply, "rule save")
    }

    pub(super) async fn rule_delete(&self, params: &Value, principal: &Principal) -> Result<Value> {
        require_owner(principal)?;
        let request: RuleDeleteRequest = eliot_core::api::discovery::decode_params(params.clone())?;
        let policy = self.secret_policy_snapshot();
        let reply =
            crate::automation::rule_edit::delete(&self.inner.data_root, &request, &policy).await?;
        encode(&reply, "rule delete")
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
