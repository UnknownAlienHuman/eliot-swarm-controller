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
