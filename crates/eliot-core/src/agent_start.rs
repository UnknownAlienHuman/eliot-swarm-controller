//! Harness-neutral parameters shared by admission preflight and plugin dispatch.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Parameters for the private `agent.start` request.
///
/// Optional fields retain their nullable wire representation. A Store preflight
/// may omit runtime-only values; the host must check the completed encoded frame
/// after supplying the runtime environment and source readback, before sending.
#[derive(Clone, Serialize, Deserialize)]
pub struct AgentStartParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    #[serde(default)]
    pub profile: Option<Value>,
    /// Frozen owner rules supplied only to a native adapter's private start
    /// request; never stored in the public Agent/profile projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_rules: Option<String>,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub target_environment: Option<Value>,
    /// Identity passed only to harness-managed MCP servers, separate from
    /// the environment inherited by shell tools.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_environment: Option<Value>,
    /// Frozen profile choice controlling whether Agent identity reaches shell
    /// tools. An omitted value retains the pre-switch default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pass_agent_identity_to_shell: Option<bool>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub source_plan: Option<Value>,
    #[serde(default)]
    pub source_state: Option<Value>,
    #[serde(default)]
    pub resume_from: Option<Value>,
    #[serde(default)]
    pub initial: Option<Value>,
    /// Effective model for a `resume` running under an inherited
    /// `configure` overlay, resolved by the daemon from the resume
    /// command's retained configuration revision. `None` on a fresh
    /// launch or a resume without an overlay; the plugin then realigns
    /// the retained native session to the frozen profile exactly as
    /// before. Present only for harnesses with sticky native session
    /// state (OpenCode, Antigravity, Claude, Muse); Codex applies
    /// overlays per turn at send time and never reads these.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_override: Option<String>,
    /// Effective effort for such a resume, resolved like `model_override`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_override: Option<String>,
    /// Configuration revision the resume runs under. Present exactly when
    /// the overrides above apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_revision: Option<i64>,
}

impl AgentStartParams {
    pub fn new(
        agent_id: impl Into<String>,
        generation: i64,
        command_id: impl Into<String>,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            generation,
            command_id: command_id.into(),
            profile: None,
            owner_rules: None,
            workspace: None,
            target_environment: None,
            mcp_environment: None,
            pass_agent_identity_to_shell: None,
            note: None,
            source_plan: None,
            source_state: None,
            resume_from: None,
            initial: None,
            model_override: None,
            effort_override: None,
            config_revision: None,
        }
    }
}
