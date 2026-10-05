//! Public, non-authoritative claim exchanged by `module.hello`.
//!
//! A claim is useful only when the Store compares it with the exact trusted
//! descriptor pinned to an authenticated binding. It is not registration,
//! installation evidence, or an authorization grant.

use crate::module_catalog::{
    ActivationPolicy, ArtifactId, ArtifactIdentity, ArtifactVersion, CapabilityId, CatalogError,
    LaunchSpec, LifecycleOwnership, ModuleDescriptor, ModuleId, ProtocolRange, ProtocolVersion,
    RestartPolicy, SchemaDescriptor, WorkspaceOptionContract, WorkspaceOptionSemantics,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MODULE_PROTOCOL_V1: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };
pub const RUNTIME_COMMAND_SCHEMA_ID: &str = "swarm.runtime_command";
pub const RUNTIME_OUTCOME_SCHEMA_ID: &str = "swarm.runtime_outcome";
pub const RUNTIME_SCHEMA_VERSION: &str = "1";

pub fn runtime_command_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: RUNTIME_COMMAND_SCHEMA_ID.to_owned(),
        version: RUNTIME_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

pub fn runtime_outcome_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: RUNTIME_OUTCOME_SCHEMA_ID.to_owned(),
        version: RUNTIME_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Declarative protocol portion of the first Rust Codex controller adapter.
/// The caller supplies launch policy and must register the resulting descriptor
/// only after its local installer verifies the exact artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleContractTemplate {
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub protocol: ProtocolRange,
    pub capabilities: BTreeSet<CapabilityId>,
    pub config_schema: Option<SchemaDescriptor>,
    pub workspace_option: Option<WorkspaceOptionContract>,
    pub command_schemas: BTreeSet<SchemaDescriptor>,
    pub event_schemas: BTreeSet<SchemaDescriptor>,
}

impl ModuleContractTemplate {
    pub fn codex_rust_controller_v1() -> Result<Self, CatalogError> {
        let module_id = ModuleId::new("codex")?;
        let artifact = ArtifactIdentity {
            artifact_id: ArtifactId::new("codex-rust-controller.1")?,
            version: ArtifactVersion::new("1")?,
            // The package has not yet been built/installed on this baseline.
            // Installer evidence may add a build ID and executable digest.
            build_id: None,
        };
        let capabilities = [
            "agent.open",
            "task.dispatch",
            "agent.send",
            "agent.reconcile",
        ]
        .into_iter()
        .map(CapabilityId::new)
        .collect::<Result<BTreeSet<_>, _>>()?;
        Ok(Self {
            module_id,
            artifact,
            protocol: ProtocolRange::exact(MODULE_PROTOCOL_V1),
            capabilities,
            config_schema: None,
            workspace_option: None,
            command_schemas: BTreeSet::from([runtime_command_schema()]),
            event_schemas: BTreeSet::from([runtime_outcome_schema()]),
        })
    }

    /// Workspace-contract release for the standalone Rust Codex adapter.
    /// Version 1 remains available for already retained descriptors.
    pub fn codex_rust_controller_v2() -> Result<Self, CatalogError> {
        let mut template = Self::codex_rust_controller_v1()?;
        template.artifact.version = ArtifactVersion::new("2")?;
        template.workspace_option = Some(WorkspaceOptionContract {
            schema_version: 1,
            native_options_pointer: "/workspaceRoot".to_owned(),
            semantics: WorkspaceOptionSemantics::ReplaceWithAdmittedAbsoluteWorkspace,
        });
        Ok(template)
    }

    pub fn descriptor(
        &self,
        launch: LaunchSpec,
        lifecycle: LifecycleOwnership,
        activation: ActivationPolicy,
        enabled: bool,
        restart: RestartPolicy,
    ) -> Result<ModuleDescriptor, CatalogError> {
        let descriptor = ModuleDescriptor {
            schema_version: 1,
            module_id: self.module_id.clone(),
            artifact: self.artifact.clone(),
            launch,
            config_schema: self.config_schema.clone(),
            workspace_option: self.workspace_option.clone(),
            command_schemas: self.command_schemas.clone(),
            event_schemas: self.event_schemas.clone(),
            protocol: self.protocol,
            capabilities: self.capabilities.clone(),
            lifecycle,
            activation,
            enabled,
            restart,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleContractClaim {
    pub schema_version: u16,
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    /// Exact protocol version the adapter selected for this Store connection.
    pub protocol: ProtocolVersion,
    /// Must be sorted and unique in serialized form.
    pub capabilities: Vec<CapabilityId>,
    pub config_schema: Option<SchemaDescriptor>,
    /// Must be sorted and unique in serialized form.
    pub command_schemas: Vec<SchemaDescriptor>,
    /// Must be sorted and unique in serialized form.
    pub event_schemas: Vec<SchemaDescriptor>,
}

impl ModuleContractClaim {
    /// Build the exact public handshake claim from a locally trusted
    /// descriptor. This is not proof of installation; Store compares it with
    /// the descriptor already pinned to the authenticated binding.
    pub fn from_descriptor(
        descriptor: &ModuleDescriptor,
        protocol: ProtocolVersion,
    ) -> Result<Self, CatalogError> {
        descriptor.validate()?;
        if !descriptor.protocol.contains(protocol) {
            return Err(CatalogError::InvalidDescriptor { field: "protocol" });
        }
        let claim = Self {
            schema_version: 1,
            module_id: descriptor.module_id.clone(),
            artifact: descriptor.artifact.clone(),
            protocol,
            capabilities: descriptor.capabilities.iter().cloned().collect(),
            config_schema: descriptor.config_schema.clone(),
            command_schemas: descriptor.command_schemas.iter().cloned().collect(),
            event_schemas: descriptor.event_schemas.iter().cloned().collect(),
        };
        claim
            .validate()
            .map_err(|_| CatalogError::InvalidDescriptor {
                field: "module_contract_claim",
            })?;
        Ok(claim)
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1 {
            return Err("unsupported module contract claim schema");
        }
        if !strictly_sorted(&self.capabilities)
            || !strictly_sorted(&self.command_schemas)
            || !strictly_sorted(&self.event_schemas)
        {
            return Err("module contract arrays must be sorted and unique");
        }
        Ok(())
    }
}

fn strictly_sorted<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::PathBuf;

    #[test]
    fn codex_v1_template_builds_the_exact_sorted_claim_without_binary_evidence() {
        let template = ModuleContractTemplate::codex_rust_controller_v1().unwrap();
        let launch = LaunchSpec {
            executable: if cfg!(windows) {
                PathBuf::from(r"C:\modules\codex-rust-controller.exe")
            } else {
                PathBuf::from("/modules/codex-rust-controller")
            },
            argv: Vec::new(),
            environment: Vec::new(),
            credential_ref: None,
            working_directory: None,
            inherited_environment_allowlist: BTreeSet::new(),
            executable_sha256: None,
        };
        let descriptor = template
            .descriptor(
                launch,
                LifecycleOwnership::OwnedService,
                ActivationPolicy::OnDemand,
                false,
                RestartPolicy::default(),
            )
            .unwrap();
        let claim = ModuleContractClaim::from_descriptor(&descriptor, MODULE_PROTOCOL_V1).unwrap();
        let value = serde_json::to_value(&claim).unwrap();
        assert_eq!(value["module_id"], "codex");
        assert_eq!(value["artifact"]["artifact_id"], "codex-rust-controller.1");
        assert_eq!(value["artifact"]["version"], "1");
        assert_eq!(value["protocol"], json!({"major":1,"minor":0}));
        assert_eq!(
            value["capabilities"],
            json!([
                "agent.open",
                "agent.reconcile",
                "agent.send",
                "task.dispatch"
            ])
        );
        assert_eq!(
            value["command_schemas"],
            json!([{"schema_id":"swarm.runtime_command","version":"1"}])
        );
        assert_eq!(
            value["event_schemas"],
            json!([{"schema_id":"swarm.runtime_outcome","version":"1"}])
        );
        assert!(value["artifact"].get("build_id").is_none());
        assert!(value["command_schemas"][0].get("sha256").is_none());
        assert_eq!(descriptor.enabled, false);
        assert!(descriptor.launch.executable_sha256.is_none());
    }
}
