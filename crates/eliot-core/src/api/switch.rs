//! Owner launch-plus-handoff. Native launch and task delivery remain separate facts.
use super::{HandoffRequest, HandoffResponse};
use crate::{CommandState, Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SwitchLaunch {
    pub profile: Option<String>,
    pub harness: Option<String>,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub auth: Option<String>,
    pub cmd: Option<String>,
    pub access: Option<String>,
    /// Target execution project for B. Omitted keeps A's project. When set,
    /// the switch worker prepares the target's standard worktree (or verifies
    /// an explicit workspace against it); B still retains A's authorization
    /// scope so the handoff stays in one namespace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Omitted uses A's retained workspace (or the target project's checkout
    /// when `project` is set). Normal launch workspace checks apply.
    pub workspace: Option<String>,
    pub worktree: Option<String>,
    pub base: Option<String>,
}

impl SwitchLaunch {
    pub fn validate(&self) -> Result<()> {
        if self.profile.is_some() == self.harness.is_some()
            || (self.harness.is_none()
                && [
                    &self.model,
                    &self.effort,
                    &self.auth,
                    &self.cmd,
                    &self.access,
                ]
                .iter()
                .any(|value| value.is_some()))
            || (self.base.is_some() && self.worktree.is_none())
            || [
                &self.profile,
                &self.harness,
                &self.model,
                &self.effort,
                &self.auth,
                &self.cmd,
                &self.access,
                &self.project,
                &self.workspace,
                &self.worktree,
                &self.base,
            ]
            .iter()
            .any(|value| value.as_deref().is_some_and(|text| text.trim().is_empty()))
        {
            return Err(Error::new(
                "invalid",
                "switch requires a profile or ad-hoc harness and valid launch selectors",
            ));
        }
        if let Some(access) = self.access.as_deref()
            && !matches!(access.trim(), "ask" | "auto" | "full")
        {
            return Err(Error::new(
                "invalid",
                "switch access must be ask, auto or full",
            ));
        }
        if let Some(project) = self.project.as_deref()
            && (project.len() > 1024 || project.contains(char::is_control))
        {
            return Err(Error::new(
                "invalid",
                "switch project must be a name of at most 1024 bytes",
            ));
        }
        Ok(())
    }

    pub fn args(&self, workspace: &str) -> Result<Value> {
        let mut args = serde_json::to_value(self)
            .map_err(|_| Error::new("internal", "cannot encode switch launch"))?;
        let object = args.as_object_mut().unwrap();
        object.retain(|_, value| !value.is_null());
        // The target project is switch routing, never a launch arg: the
        // worker resolves the checkout and sets the launch scope itself.
        object.remove("project");
        object
            .entry("workspace")
            .or_insert_with(|| Value::String(workspace.to_owned()));
        Ok(args)
    }
}

/// Router-set launch arg naming the target registration revision the switch
/// worker verified before preparation. The Store admission transaction
/// rechecks it, so a revocation between preparation and admission refuses
/// instead of dispatching under stale authority.
pub const SWITCH_PROJECT_REVISION_ARG: &str = "switch_project_revision";
/// Router-set launch flag admitting the blank source namespace on B when a
/// switch continues a projectless source. Normal launches can never carry
/// it: `reject_launch_derived_keys` refuses it from callers.
pub const SWITCH_BLANK_SCOPE_ARG: &str = "switch_blank_scope";

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SwitchRequest {
    pub id: String,
    pub from: String,
    pub from_generation: i64,
    pub task_command: String,
    #[schemars(length(max = super::MAX_HANDOFF_SENDS), extend("uniqueItems" = true))]
    pub commands: Vec<String>,
    pub launch: SwitchLaunch,
}

impl SwitchRequest {
    pub fn validate(&self) -> Result<()> {
        super::handoff::validate_selection(
            &self.id,
            &self.from,
            self.from_generation,
            &self.task_command,
            &self.commands,
        )?;
        self.launch.validate()
    }

    pub fn handoff(&self, id: &str, to: &str, generation: i64) -> HandoffRequest {
        HandoffRequest {
            id: id.to_owned(),
            from: self.from.clone(),
            from_generation: self.from_generation,
            to: to.to_owned(),
            to_generation: generation,
            task_command: self.task_command.clone(),
            commands: self.commands.clone(),
        }
    }

    pub fn validate_response(&self, value: &SwitchResponse) -> Result<()> {
        if value.id != self.id
            || value.from != self.from
            || value.from_generation != self.from_generation
            || value.launch_command == value.handoff_command
            || value.launch_command == self.id
            || value.handoff_command == self.id
        {
            return Err(Error::new(
                "source_integrity",
                "switch response binding differs",
            ));
        }
        if let Some(handoff) = &value.handoff {
            let created = value.created.as_ref().ok_or_else(|| {
                Error::new("source_integrity", "switch handoff has no destination")
            })?;
            self.handoff(&value.handoff_command, &created.agent, created.generation)
                .validate_response(handoff)?;
        }
        if value.disposal_command.is_some() != value.disposal_state.is_some()
            || value.disposal_command.as_ref().is_some_and(|id| {
                value.state != CommandState::Failed
                    || value.created.is_none()
                    || value.handoff.is_some()
                    || id == &value.launch_command
                    || id == &value.handoff_command
                    || crate::validate_command_id(id).is_err()
            })
        {
            return Err(Error::new(
                "source_integrity",
                "switch successor stop has an invalid binding",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SwitchAgent {
    pub agent: String,
    pub generation: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SwitchResponse {
    pub id: String,
    pub from: String,
    pub from_generation: i64,
    pub state: CommandState,
    /// Frozen before launch preparation or any external work.
    pub launch_command: String,
    pub handoff_command: String,
    pub launch_state: Option<CommandState>,
    /// Agent allocation is distinct from proven session readiness.
    pub created: Option<SwitchAgent>,
    pub handoff: Option<HandoffResponse>,
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<String>,
    /// The ordinary generation-bound stop admitted for an unused successor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposal_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposal_state: Option<CommandState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disposal_reason: Option<String>,
    pub next_action: String,
}
