//! Public, non-authoritative claim exchanged by `module.hello`.
//!
//! A claim is useful only when the Store compares it with the exact trusted
//! descriptor pinned to an authenticated binding. It is not registration,
//! installation evidence, or an authorization grant.

use crate::module_catalog::{
    ActivationPolicy, ArtifactId, ArtifactIdentity, ArtifactVersion, CapabilityId, CatalogError,
    LaunchSpec, LifecycleOwnership, ModuleDescriptor, ModuleId, PreInputOpenContract,
    ProtocolRange, ProtocolVersion, RestartPolicy, SchemaDescriptor, WorkspaceOptionContract,
    WorkspaceOptionSemantics,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MODULE_PROTOCOL_V1: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };
pub const RUNTIME_COMMAND_SCHEMA_ID: &str = "swarm.runtime_command";
pub const RUNTIME_OUTCOME_SCHEMA_ID: &str = "swarm.runtime_outcome";
pub const TASK_DISPATCH_CONTEXT_SCHEMA_ID: &str = "swarm.task_dispatch_context";
pub const TASK_DISPATCH_ADMISSION_SCHEMA_ID: &str = "swarm.task_dispatch_admission";
pub const TASK_PROMPT_SCHEMA_ID: &str = crate::task_prompt::TASK_PROMPT_SCHEMA_ID;
pub const NORMALIZED_RESULT_CONTEXT_SCHEMA_ID: &str = "swarm.normalized_result_context";
pub const NORMALIZED_RESULT_PAGE_SCHEMA_ID: &str = "swarm.normalized_result_page";
pub const GOAL_CONTINUATION_SCHEMA_ID: &str = "swarm.goal_continuation";
pub const GOAL_CONTINUATION_SCHEMA_VERSION: &str = "1";
/// Internal Store-enriched command context for a Codex Goal continuation.
/// This is transported through the existing authenticated RuntimeCommand and
/// does not advertise a native capability.
pub const GOAL_CONTINUATION_CONTEXT_SCHEMA_ID: &str = "swarm.goal_continuation_context";
pub const GOAL_CONTINUATION_CONTEXT_SCHEMA_VERSION: &str = "1";
/// Closed adapter receipt for the exact continuation input. This is distinct
/// from the task.dispatch admission event schema.
pub const GOAL_CONTINUATION_ADMISSION_SCHEMA_ID: &str = "swarm.goal_continuation_admission";
pub const GOAL_CONTINUATION_ADMISSION_SCHEMA_VERSION: &str = "1";
/// Versioned common terminal evidence retained by Store consumers.  This is
/// an evidence envelope, not a capability declaration or a continuation
/// grant; adapters opt in only by producing a validated retained event.
pub const GOAL_TERMINAL_EVIDENCE_SCHEMA_ID: &str = "swarm.goal_terminal_evidence";
pub const GOAL_TERMINAL_EVIDENCE_SCHEMA_VERSION: &str = "1";
/// Common bounded metadata envelope for descriptor-admitted operationless
/// Module events. The descriptor still decides which event stream/schema a
/// binding may publish; this schema only defines the closed projection fields.
pub const MODULE_EVENT_METADATA_SCHEMA_ID: &str = "swarm.module_event_metadata";
pub const RUNTIME_SCHEMA_VERSION: &str = "1";

pub use crate::native_mcp::{NATIVE_MCP_COMMAND_SCHEMA_ID, NATIVE_MCP_COMMAND_SCHEMA_VERSION};

/// Descriptor declaration for the bounded native MCP command carried inside
/// the existing authenticated RuntimeCommand transport. Native outcomes keep
/// using `swarm.runtime_outcome@1`; this helper intentionally adds no event
/// schema or receipt channel.
pub fn native_mcp_command_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: NATIVE_MCP_COMMAND_SCHEMA_ID.to_owned(),
        version: NATIVE_MCP_COMMAND_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

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

/// Descriptor declaration for the Store-enriched, immutable task-dispatch
/// context supplied to an adapter that implements normalized admission.
pub fn task_dispatch_context_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: TASK_DISPATCH_CONTEXT_SCHEMA_ID.to_owned(),
        version: RUNTIME_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Versioned opt-in for Store-produced exact native task prompt bytes.
pub fn task_prompt_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: TASK_PROMPT_SCHEMA_ID.to_owned(),
        version: crate::task_prompt::TASK_PROMPT_SCHEMA_VERSION.to_string(),
        sha256: None,
    }
}

/// Fail closed for malformed/unknown task-prompt declarations, but retain
/// historical rendering for descriptors that never selected this schema.
/// Uses the same immutable registered-descriptor fields in Store and adapters.
pub fn task_prompt_selected<'a>(
    command_schemas: impl Iterator<Item = &'a SchemaDescriptor>,
    event_schemas: impl Iterator<Item = &'a SchemaDescriptor>,
    capabilities: impl Iterator<Item = &'a CapabilityId>,
) -> Result<bool, &'static str> {
    let expected = task_prompt_schema();
    let runtime = runtime_command_schema();
    let context = task_dispatch_context_schema();
    let admission = task_dispatch_admission_schema();
    let mut selected = false;
    let mut has_runtime = false;
    let mut has_context = false;
    for schema in command_schemas {
        if schema.schema_id == TASK_PROMPT_SCHEMA_ID {
            if selected || *schema != expected {
                return Err("unsupported or duplicated selected TaskPrompt schema");
            }
            selected = true;
        }
        has_runtime |= *schema == runtime;
        has_context |= *schema == context;
    }
    if !selected {
        return Ok(false);
    }
    if !has_runtime
        || !has_context
        || !event_schemas.into_iter().any(|schema| *schema == admission)
        || !capabilities
            .into_iter()
            .any(|cap| cap.as_str() == "task.dispatch")
    {
        return Err("selected TaskPrompt requires task.dispatch and normalized admission pair");
    }
    Ok(true)
}

/// Descriptor declaration for the typed normalized dispatch receipt returned
/// by an adapter that implements `task_dispatch_context_schema`.
pub fn task_dispatch_admission_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: TASK_DISPATCH_ADMISSION_SCHEMA_ID.to_owned(),
        version: RUNTIME_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Descriptor declaration for Store-sealed normalized result origin context.
pub fn normalized_result_context_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: NORMALIZED_RESULT_CONTEXT_SCHEMA_ID.to_owned(),
        version: RUNTIME_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Descriptor declaration for typed normalized result pages.
pub fn normalized_result_page_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: NORMALIZED_RESULT_PAGE_SCHEMA_ID.to_owned(),
        version: RUNTIME_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Closed Store-owned linkage for one Goal continuation.  This is an
/// attribution schema, not a provider capability declaration: each adapter
/// must prove its own terminal evidence before the Store can use it.
pub fn goal_continuation_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: GOAL_CONTINUATION_SCHEMA_ID.to_owned(),
        version: GOAL_CONTINUATION_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Internal receipt declaration for Store validation of one task-bound
/// continuation. It is not inserted into a module capability set by this
/// helper; the existing Codex `agent.send` capability remains the only native
/// action.
pub fn goal_continuation_admission_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: GOAL_CONTINUATION_ADMISSION_SCHEMA_ID.to_owned(),
        version: GOAL_CONTINUATION_ADMISSION_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Descriptor for the common immutable terminal event envelope.  The current
/// Codex descriptor keeps its existing capability set; this schema is carried
/// in the Store's authenticated evidence path and does not advertise
/// `agent.goal`.
pub fn goal_terminal_evidence_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: GOAL_TERMINAL_EVIDENCE_SCHEMA_ID.to_owned(),
        version: GOAL_TERMINAL_EVIDENCE_SCHEMA_VERSION.to_owned(),
        sha256: None,
    }
}

/// Descriptor declaration for the common bounded operationless Module-event
/// metadata envelope. A descriptor must opt into this schema before its
/// Module may call the generic event producer path.
pub fn module_event_metadata_schema() -> SchemaDescriptor {
    SchemaDescriptor {
        schema_id: MODULE_EVENT_METADATA_SCHEMA_ID.to_owned(),
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
    pub pre_input_open: Option<PreInputOpenContract>,
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
            pre_input_open: None,
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

    /// Descriptor for the current normalized Codex controller contract.
    /// Version 3 remains available to existing registrations; version 4 adds
    /// exact normalized result-page schemas and the matching result capability.
    pub fn codex_rust_controller_v3() -> Result<Self, CatalogError> {
        let mut template = Self::codex_rust_controller_v2()?;
        template.artifact.version = ArtifactVersion::new("3")?;
        template.command_schemas =
            BTreeSet::from([runtime_command_schema(), task_dispatch_context_schema()]);
        template.event_schemas =
            BTreeSet::from([runtime_outcome_schema(), task_dispatch_admission_schema()]);
        Ok(template)
    }

    /// Descriptor for the current Codex adapter artifact, including its
    /// descriptor-gated normalized result selector and paged result schema.
    pub fn codex_rust_controller_v4() -> Result<Self, CatalogError> {
        let mut template = Self::codex_rust_controller_v3()?;
        template.artifact.version = ArtifactVersion::new("4")?;
        template
            .capabilities
            .insert(CapabilityId::new("agent.result")?);
        template.command_schemas = BTreeSet::from([
            runtime_command_schema(),
            task_dispatch_context_schema(),
            normalized_result_context_schema(),
        ]);
        template.event_schemas = BTreeSet::from([
            runtime_outcome_schema(),
            task_dispatch_admission_schema(),
            normalized_result_page_schema(),
        ]);
        Ok(template)
    }

    /// TaskPrompt v1 is an additive command contract on Codex artifact v5.
    /// Existing v1–v4 descriptor versions retain their immutable semantics.
    pub fn codex_rust_controller_v5() -> Result<Self, CatalogError> {
        let mut template = Self::codex_rust_controller_v4()?;
        template.artifact.version = ArtifactVersion::new("5")?;
        template.command_schemas.insert(task_prompt_schema());
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
            pre_input_open: self.pre_input_open,
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
    /// Exact descriptor-pinned prepared-open behavior, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_input_open: Option<PreInputOpenContract>,
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
            pre_input_open: descriptor.pre_input_open,
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
        if self
            .pre_input_open
            .as_ref()
            .is_some_and(|contract| contract.validate().is_err())
        {
            return Err("pre-input open contract is invalid");
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
