//! Data-only types shared across independently built Swarm processes.

pub mod concilium_limits;
pub mod coordination_limits;
pub mod credential;
pub mod declared_service_scope;
pub mod error;
pub mod mcp_catalog;
pub mod mcp_frontend;
pub mod method_policy;
pub mod module_catalog;
pub mod module_command;

pub mod module_contract;
/// Store-admitted native MCP command DTO and method declarations.
pub mod native_mcp;
pub mod native_usage;
pub mod provider_condition;
pub mod rpc;
pub mod runtime;
pub mod task_prompt;

pub use credential::Credential;
pub use declared_service_scope::{DeclaredServicePurpose, DeclaredServiceScope};
pub use error::{Error, NativeRpcRejectionClass, Result};
pub use native_mcp::{NativeMcpCommand, NativeMcpPhase, ProtectedArtifactRef};
pub use rpc::Request;
pub use runtime::{
    EffectOutcome, GoalContinuationAdmissionContext, GoalContinuationAdmissionReceipt,
    GoalContinuationLink, GoalTerminalEventRef, GoalTerminalEvidence, ModuleReceiptIdentity,
    NormalizedResultOriginContext, NormalizedResultPageSource, NormalizedResultProducerOrigin,
    RuntimeCommand, RuntimeOutcome, TaskDispatchAdmissionReceipt, TaskDispatchContext,
};
