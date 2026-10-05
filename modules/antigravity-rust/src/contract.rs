//! Exact public protocol claim for the standalone Antigravity Rust artifact.

use std::collections::BTreeSet;

use swarm_contracts::{
    error::{Error, Result},
    module_catalog::{
        ArtifactId, ArtifactIdentity, ArtifactVersion, CapabilityId, ModuleId, ProtocolRange,
        WorkspaceOptionContract, WorkspaceOptionSemantics,
    },
    module_contract::{
        MODULE_PROTOCOL_V1, ModuleContractClaim, ModuleContractTemplate,
        runtime_command_schema, runtime_outcome_schema, task_dispatch_admission_schema,
        task_dispatch_context_schema,
    },
};

use crate::wire::{ARTIFACT_ID, ARTIFACT_VERSION};

/// The descriptor body shared by the trusted installer and this adapter's
/// `module.hello` claim. Launch paths remain installation-specific.
pub fn template() -> Result<ModuleContractTemplate> {
    let module_id = ModuleId::new("antigravity")
        .map_err(|_| Error::new("MODULE_CONTRACT_INVALID", "invalid Antigravity module id"))?;
    let artifact = ArtifactIdentity {
        artifact_id: ArtifactId::new(ARTIFACT_ID).map_err(|_| {
            Error::new("MODULE_CONTRACT_INVALID", "invalid Antigravity artifact id")
        })?,
        version: ArtifactVersion::new(ARTIFACT_VERSION).map_err(|_| {
            Error::new(
                "MODULE_CONTRACT_INVALID",
                "invalid Antigravity artifact version",
            )
        })?,
        build_id: None,
    };
    let capabilities = [
        "agent.open",
        "agent.reconcile",
        "agent.refresh",
        "agent.result",
        "agent.send/next_turn",
        "task.dispatch",
    ]
    .into_iter()
    .map(|value| {
        CapabilityId::new(value)
            .map_err(|_| Error::new("MODULE_CONTRACT_INVALID", "invalid Antigravity capability"))
    })
    .collect::<Result<BTreeSet<_>>>()?;

    Ok(ModuleContractTemplate {
        module_id,
        artifact,
        protocol: ProtocolRange::exact(MODULE_PROTOCOL_V1),
        capabilities,
        config_schema: None,
        workspace_option: Some(WorkspaceOptionContract {
            schema_version: 1,
            native_options_pointer: "/workspaceRoot".to_owned(),
            semantics: WorkspaceOptionSemantics::ReplaceWithAdmittedAbsoluteWorkspace,
        }),
        pre_input_open: None,
        command_schemas: BTreeSet::from([
            runtime_command_schema(),
            task_dispatch_context_schema(),
        ]),
        event_schemas: BTreeSet::from([
            runtime_outcome_schema(),
            task_dispatch_admission_schema(),
        ]),
    })
}

/// Read the exact descriptor-derived claim fixed by the guarded module owner.
/// The executable does not synthesize a build identity or authority from its
/// local config; Store negotiation still decides whether this claim matches
/// the selected trusted descriptor.
pub fn claim() -> Result<ModuleContractClaim> {
    let template = template()?;
    let encoded = std::env::var("ELIOT_SWARM_MODULE_CONTRACT").map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_REQUIRED",
            "guarded module launcher did not supply the descriptor claim",
        )
    })?;
    if encoded.len() > 64 * 1024 {
        return Err(Error::new(
            "MODULE_CONTRACT_INVALID",
            "descriptor claim exceeds the bounded hello envelope",
        ));
    }
    let claim: ModuleContractClaim = serde_json::from_str(&encoded).map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_INVALID",
            "guarded module descriptor claim is malformed",
        )
    })?;
    claim.validate().map_err(|_| {
        Error::new(
            "MODULE_CONTRACT_INVALID",
            "Antigravity protocol claim is not in canonical order",
        )
    })?;
    if claim.schema_version != 1
        || claim.module_id != template.module_id
        || claim.artifact.artifact_id != template.artifact.artifact_id
        || claim.artifact.version != template.artifact.version
        || claim
            .artifact
            .build_id
            .as_deref()
            .is_some_and(|value| !valid_build_id(value))
        || claim.protocol != MODULE_PROTOCOL_V1
        || claim.capabilities != template.capabilities.into_iter().collect::<Vec<_>>()
        || claim.config_schema != template.config_schema
        || !schemas_match(&claim)
    {
        return Err(Error::new(
            "MODULE_CONTRACT_MISMATCH",
            "guarded claim differs from the supported Antigravity .1 protocol",
        ));
    }
    Ok(claim)
}

pub fn normalized_dispatch_enabled(claim: &ModuleContractClaim) -> bool {
    claim
        .command_schemas
        .iter()
        .any(|schema| schema.schema_id == "swarm.task_dispatch_context" && schema.version == "1")
        && claim
            .event_schemas
            .iter()
            .any(|schema| schema.schema_id == "swarm.task_dispatch_admission" && schema.version == "1")
}

fn schemas_match(claim: &ModuleContractClaim) -> bool {
    let legacy_commands = vec![runtime_command_schema()];
    let legacy_events = vec![runtime_outcome_schema()];
    let normalized_commands = vec![
        runtime_command_schema(),
        task_dispatch_context_schema(),
    ];
    let normalized_events = vec![
        runtime_outcome_schema(),
        task_dispatch_admission_schema(),
    ];
    (claim.command_schemas == legacy_commands && claim.event_schemas == legacy_events)
        || (claim.command_schemas == normalized_commands
            && claim.event_schemas == normalized_events)
}

fn valid_build_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-+".contains(&byte))
}
