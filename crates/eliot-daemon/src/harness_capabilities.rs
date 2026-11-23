//! Built-in profile parser rules, also used by offline desktop settings repair.

pub(crate) use eliot_core::api::HarnessCapabilities;
use eliot_core::api::native_profile::NativeSubagentsPolicy;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct ProfileCapabilities {
    /// Model-pin selectors and provider-mode flags are native metadata.
    pub model_pin_selectors: bool,
    /// Route-scope selectors must resolve before metadata is trusted.
    pub route_selectors_resolved: bool,
    /// Environment names that carry route scope and are refused in metadata.
    pub route_scope_secret_names: &'static [&'static str],
    /// `CLAUDE_CONFIG_DIR` is a credential-bearing route selector.
    pub config_dir_credential: bool,
    /// Accepts the `latest` command pin.
    pub latest_cmd: bool,
    /// Accepts a script-wrapper `cmd`.
    pub script_wrapper_cmd: bool,
    /// Accepts `local_provider` in a profile entry.
    pub local_provider: bool,
    /// Accepts `project_doc_max_bytes` in a profile entry.
    pub project_doc_max_bytes: bool,
    /// Native-subagent default and denial support.
    pub native_subagents_policy: Option<NativeSubagentsPolicy>,
}

impl ProfileCapabilities {
    pub(crate) fn of(harness: &str) -> Self {
        Self::for_harness(harness).unwrap_or_default()
    }

    pub(crate) fn for_harness(harness: &str) -> Option<Self> {
        Some(match harness {
            "codex" => Self {
                script_wrapper_cmd: true,
                project_doc_max_bytes: true,
                native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
                ..Self::default()
            },
            "claude" => Self {
                native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
                model_pin_selectors: true,
                route_selectors_resolved: true,
                route_scope_secret_names: &["CLAUDE_CODE_OAUTH_TOKEN"],
                config_dir_credential: true,
                latest_cmd: true,
                ..Self::default()
            },
            "opencode" => Self {
                native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
                route_scope_secret_names: &["OPENCODE_CONFIG_CONTENT"],
                local_provider: true,
                ..Self::default()
            },
            "muse" | "antigravity" => Self {
                native_subagents_policy: Some(NativeSubagentsPolicy::allow_by_default()),
                ..Self::default()
            },
            _ => return None,
        })
    }
}

/// Native-subagent policy shared with the offline settings editor.
pub fn native_subagents_policy(harness: &str) -> Option<NativeSubagentsPolicy> {
    ProfileCapabilities::for_harness(harness)?.native_subagents_policy
}
