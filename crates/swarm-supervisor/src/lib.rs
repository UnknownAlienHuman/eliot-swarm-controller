//! Demand-driven module lifecycle primitives for independently built workers.
//!
//! The package owns no database and invents no RPC. The host supplies an
//! already-admitted demand and trusted Operation readback; module workers use
//! [`ModuleLink`] for the current `module.hello`, `module.next`, and
//! `module.outcome` methods.

mod descriptor;
mod module_link;
pub mod observation;
mod supervisor;

pub use descriptor::{
    ActivationPolicy, ArtifactIdentity, ArtifactSelector, BindingLaunchConfig, CapabilityId,
    DescriptorCatalog, EnvironmentVariable, LaunchSpec, LaunchValue, LifecycleOwnership,
    ModuleCatalog, ModuleDescriptor, ModuleId, ModuleOwnerExecutable, ProtectedRef,
    ProtectedResolver, ProtectedResolverContext, ProtocolRange, ProtocolVersion,
    ResolverMapDirectory, RestartPolicy, SchemaDescriptor, ServiceScope, Sha256Digest,
    load_installed_descriptor,
};
pub use module_link::module_contract_claim;
pub use observation::{
    ModuleEffectCertainty, ModuleFailureStage, ModuleSupervisorObservation, ModuleSupervisorPhase,
};
pub use supervisor::{
    AdmissionState, DemandCause, DemandLease, FailureSummary, KernelFault, LifecycleState,
    ModuleSupervisor, OperationReadback, OperationSnapshot, ProcessIdentity, RestartStatus,
    SupervisorRegistry, SupervisorStatus,
};

pub use swarm_contracts::error::{Error, Result};
pub use swarm_contracts::module_contract::ModuleContractClaim;
