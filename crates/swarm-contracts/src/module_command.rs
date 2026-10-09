//! Closed, data-only classification of Store-dispatched module commands.
//!
//! This registry describes compatibility only. It never grants application
//! authority, starts a module, or interprets a native result.

use crate::native_mcp::{
    NATIVE_MCP_ARM_METHOD, NATIVE_MCP_INSTALL_METHOD, NATIVE_MCP_OBSERVE_METHOD,
    NATIVE_MCP_READ_METHOD, NativeMcpPhase,
};
use serde_json::Value;

/// Every command that may require a selected module credential.
pub const MODULE_COMMAND_METHODS: [&str; 18] = [
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
    NATIVE_MCP_INSTALL_METHOD,
    NATIVE_MCP_OBSERVE_METHOD,
    NATIVE_MCP_ARM_METHOD,
    NATIVE_MCP_READ_METHOD,
];

/// Commands selected by the module-demand page. Native-MCP phases reuse an
/// already-retained native session and do not create a second process demand.
pub const MODULE_DEMAND_METHODS: [&str; 14] = [
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
}

impl RuntimeCommandClassError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnknownMethod => "MODULE_COMMAND_UNMAPPED",
            Self::InvalidAgentSendDelivery => "MODULE_COMMAND_INVALID",
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
