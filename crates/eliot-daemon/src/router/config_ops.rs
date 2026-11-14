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
    Error, Result,
    api::{
        ProfileListRequest, ProfilesReadRequest, ProfilesSaveRequest, RuleListEntry,
        RuleListRequest, RuleListResponse,
    },
};
use serde_json::Value;

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
        let reply = crate::config::profiles_edit::save(&self.inner.data_root, &request, &policy)?;
        request.validate_response(&reply)?;
        encode(&reply, "profile save")
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
        // Like `agent.list`: every nonempty page names its last id and the
        // caller continues until an empty page, so a byte-budget-shortened
        // page never hides the rest of the namespace.
        let next = rules.last().map(|entry| entry.state.id.clone());
        let reply = RuleListResponse { rules, next };
        request.validate_response(&reply)?;
        encode(&reply, "rule list")
    }
}
