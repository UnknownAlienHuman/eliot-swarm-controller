//! Data-only types shared across independently built Swarm processes.

pub mod credential;
pub mod declared_service_scope;
pub mod error;
pub mod module_catalog;
pub mod module_contract;
pub mod rpc;
pub mod runtime;

pub use credential::Credential;
pub use declared_service_scope::{DeclaredServicePurpose, DeclaredServiceScope};
pub use error::{Error, NativeRpcRejectionClass, Result};
pub use rpc::Request;
pub use runtime::{EffectOutcome, RuntimeCommand, RuntimeOutcome};
