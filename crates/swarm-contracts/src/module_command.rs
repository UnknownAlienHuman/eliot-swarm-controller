//! Closed, data-only classification of Store-dispatched module commands.
//!
//! This registry describes compatibility only. It never grants application
//! authority, starts a module, or interprets a native result.

use crate::native_mcp::{
    NATIVE_MCP_ARM_METHOD, NATIVE_MCP_INSTALL_METHOD, NATIVE_MCP_OBSERVE_METHOD,
    NATIVE_MCP_READ_METHOD, NativeMcpPhase,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const COMMAND_RELEASE_EVIDENCE_METHOD: &str = "native.command.release_evidence";
pub const COMMAND_RELEASE_EVIDENCE_FIELD: &str = "capture_release_evidence";

/// Store's immutable readback proof authorizing deletion of one Command run's
/// native captures. The Operation envelope supplies release-operation identity
/// and binding authority; these fields bind the proof to the target and every
/// persisted output page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandCaptureReleaseEvidence {
    pub schema_version: u8,
    pub target_operation_id: String,
    pub target_input_sha256: String,
    pub target_outcome_sha256: String,
    pub outputs: Vec<CommandCaptureOutputCoverage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandCaptureOutputCoverage {
    pub native_output: String,
    pub target_command_output: Value,
    pub pages: Vec<CommandCaptureArtifactPage>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandCaptureArtifactPage {
    pub result_operation_id: String,
    pub result_input_sha256: String,
    pub artifact_ref: String,
    pub artifact_sha256: String,
    pub artifact_byte_length: u64,
    pub offset_bytes: u64,
    pub byte_length: u64,
    pub total_bytes: u64,
    pub page_sha256: String,
    pub eof: bool,
}

/// Every command that may require a selected module credential.
pub const MODULE_COMMAND_METHODS: [&str; 19] = [
    "agent.open",
    "task.dispatch",
    "agent.send",
    "agent.reply",
    "agent.configure",
    "agent.goal",
    "agent.background",
    "agent.refresh",
    "agent.reconcile",
    "agent.result",
    "agent.recover",
    "native.opencode.loop_step",
    "native.command.cancel_turn",
    "native.command.close_session",
    COMMAND_RELEASE_EVIDENCE_METHOD,
    NATIVE_MCP_INSTALL_METHOD,
    NATIVE_MCP_OBSERVE_METHOD,
    NATIVE_MCP_ARM_METHOD,
    NATIVE_MCP_READ_METHOD,
];

/// Commands selected by the module-demand page. Native-MCP phases reuse an
/// already-retained native session and do not create a second process demand.
pub const MODULE_DEMAND_METHODS: [&str; 15] = [
    "agent.open",
    "task.dispatch",
    "agent.send",
    "agent.reply",
    "agent.configure",
    "agent.goal",
    "agent.background",
    "agent.refresh",
    "agent.reconcile",
    "agent.result",
    "agent.recover",
    "native.opencode.loop_step",
    "native.command.cancel_turn",
    "native.command.close_session",
    COMMAND_RELEASE_EVIDENCE_METHOD,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCommandKind {
    AgentOpen,
    TaskDispatch,
    AgentSendNextTurn,
    AgentSendSteer,
    AgentReply,
    AgentConfigure,
    AgentGoal,
    AgentBackground,
    AgentRefresh,
    AgentReconcile,
    AgentResult,
    AgentRecover,
    OpenCodeLoopStep,
    CommandCancelTurn,
    CommandCloseSession,
    CommandReleaseEvidence,
    NativeMcp(NativeMcpPhase),
}

impl RuntimeCommandKind {
    pub const fn capability(self) -> &'static str {
        match self {
            Self::AgentOpen => "agent.open",
            Self::TaskDispatch => "task.dispatch",
            Self::AgentSendNextTurn => "agent.send/next_turn",
            Self::AgentSendSteer => "agent.send/steer",
            Self::AgentReply => "agent.reply",
            Self::AgentConfigure => "agent.configure",
            Self::AgentGoal => "agent.goal",
            Self::AgentBackground => "agent.background",
            Self::AgentRefresh => "agent.refresh",
            Self::AgentReconcile => "agent.reconcile",
            Self::AgentResult => "agent.result",
            Self::AgentRecover => "agent.recover",
            Self::OpenCodeLoopStep => "native.opencode.loop_step",
            Self::CommandCancelTurn => "native.command.cancel_turn",
            Self::CommandCloseSession => "native.command.close_session",
            Self::CommandReleaseEvidence => COMMAND_RELEASE_EVIDENCE_METHOD,
            Self::NativeMcp(phase) => phase.method(),
        }
    }

    pub const fn is_native_mcp(self) -> bool {
        matches!(self, Self::NativeMcp(_))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeCommandClassError {
    UnknownMethod,
    InvalidAgentSendDelivery,
    InvalidCommandReleaseEvidence,
}

impl RuntimeCommandClassError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnknownMethod => "MODULE_COMMAND_UNMAPPED",
            Self::InvalidAgentSendDelivery => "MODULE_COMMAND_INVALID",
            Self::InvalidCommandReleaseEvidence => "MODULE_COMMAND_INVALID",
        }
    }

    pub const fn message(self) -> &'static str {
        match self {
            Self::UnknownMethod => {
                "runtime command is absent from the closed module command registry"
            }
            Self::InvalidAgentSendDelivery => {
                "agent.send requires exact delivery next_turn or steer"
            }
            Self::InvalidCommandReleaseEvidence => {
                "Command capture release requires its exact target and Store artifact evidence"
            }
        }
    }
}

pub fn classify_runtime_command(
    method: &str,
    input: &Value,
) -> Result<RuntimeCommandKind, RuntimeCommandClassError> {
    Ok(match method {
        "agent.open" => RuntimeCommandKind::AgentOpen,
        "task.dispatch" => RuntimeCommandKind::TaskDispatch,
        "agent.send" => match input.get("delivery").and_then(Value::as_str) {
            Some("next_turn") => RuntimeCommandKind::AgentSendNextTurn,
            Some("steer") => RuntimeCommandKind::AgentSendSteer,
            _ => return Err(RuntimeCommandClassError::InvalidAgentSendDelivery),
        },
        "agent.reply" => RuntimeCommandKind::AgentReply,
        "agent.configure" => RuntimeCommandKind::AgentConfigure,
        "agent.goal" => RuntimeCommandKind::AgentGoal,
        "agent.background" => RuntimeCommandKind::AgentBackground,
        "agent.refresh" => RuntimeCommandKind::AgentRefresh,
        "agent.reconcile" => RuntimeCommandKind::AgentReconcile,
        "agent.result" => RuntimeCommandKind::AgentResult,
        "agent.recover" => RuntimeCommandKind::AgentRecover,
        "native.opencode.loop_step" => RuntimeCommandKind::OpenCodeLoopStep,
        "native.command.cancel_turn" => RuntimeCommandKind::CommandCancelTurn,
        "native.command.close_session" => RuntimeCommandKind::CommandCloseSession,
        COMMAND_RELEASE_EVIDENCE_METHOD => {
            let evidence = input.get(COMMAND_RELEASE_EVIDENCE_FIELD).and_then(|value| {
                serde_json::from_value::<CommandCaptureReleaseEvidence>(value.clone()).ok()
            });
            if input.get("target_operation_id").and_then(Value::as_str)
                != evidence
                    .as_ref()
                    .map(|value| value.target_operation_id.as_str())
            {
                return Err(RuntimeCommandClassError::InvalidCommandReleaseEvidence);
            }
            RuntimeCommandKind::CommandReleaseEvidence
        }
        NATIVE_MCP_INSTALL_METHOD => RuntimeCommandKind::NativeMcp(NativeMcpPhase::Install),
        NATIVE_MCP_OBSERVE_METHOD => RuntimeCommandKind::NativeMcp(NativeMcpPhase::Observe),
        NATIVE_MCP_ARM_METHOD => RuntimeCommandKind::NativeMcp(NativeMcpPhase::Arm),
        NATIVE_MCP_READ_METHOD => RuntimeCommandKind::NativeMcp(NativeMcpPhase::Read),
        _ => return Err(RuntimeCommandClassError::UnknownMethod),
    })
}

/// A historical broad `agent.send` capability remains compatible with either
/// exact delivery variant. Every other capability must match exactly.
pub fn capability_satisfies(advertised: &str, required: &str) -> bool {
    advertised == required || (required.starts_with("agent.send/") && advertised == "agent.send")
}
