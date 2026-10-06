//! Data-only types shared across independently built Swarm processes.

pub mod concilium_limits;
pub mod coordination_limits;
pub mod credential;
pub mod declared_service_scope;
pub mod error;
pub mod mcp_frontend;
pub mod method_policy;
pub mod module_catalog;

pub mod module_contract;
/// Store-admitted native MCP command DTO and method declarations.
pub mod native_mcp;
pub mod rpc;
pub mod runtime;

pub use credential::Credential;
pub use declared_service_scope::{DeclaredServicePurpose, DeclaredServiceScope};
pub use error::{Error, NativeRpcRejectionClass, Result};
pub use native_mcp::{NativeMcpCommand, NativeMcpPhase, ProtectedArtifactRef};
pub use rpc::Request;
pub use runtime::{
    EffectOutcome, ModuleReceiptIdentity, NormalizedResultOriginContext,
    NormalizedResultPageSource, NormalizedResultProducerOrigin, RuntimeCommand, RuntimeOutcome,
    TaskDispatchAdmissionReceipt, TaskDispatchContext,
};
