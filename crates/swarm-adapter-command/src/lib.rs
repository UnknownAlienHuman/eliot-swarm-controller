//! Independent Command Code module adapter.
//!
//! This package owns native invocation and its bounded receipt. Swarm keeps
//! ownership of admission, Task policy, binding identity, and final acceptance.

#![recursion_limit = "256"]

mod acp_prompt;
mod adapter;
mod journal;
mod module_host;
mod native;
mod result_page;

pub use adapter::run;

pub const RUNTIME: &str = "command";
pub const ARTIFACT_ID: &str = "eliot-command.rust-headless.1";
pub const ARTIFACT_VERSION: &str = "3";
pub const CONTRACT_REVISION: &str = "command-headless-module-v3";
pub const BATCH_V4_CONTRACT_REVISION: &str = "command-headless-module-v4";
pub const EXECUTION_SHAPE: &str = "sessionless_batch";

/// ACP is a distinct artifact and execution profile. The historical batch
/// artifact and its saved receipts continue to use the constants above.
pub const ACP_ARTIFACT_ID: &str = "eliot-command.acp-rust.1";
pub const ACP_ARTIFACT_VERSION: &str = "1";
pub const ACP_CONTRACT_REVISION: &str = "command-acp-module-v1";
pub const ACP_EXECUTION_SHAPE: &str = "command_acp_v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Profile {
    BatchV3,
    BatchV4,
    AcpV1,
}

impl Profile {
    pub(crate) fn artifact_id(self) -> &'static str {
        match self {
            Self::BatchV3 | Self::BatchV4 => ARTIFACT_ID,
            Self::AcpV1 => ACP_ARTIFACT_ID,
        }
    }

    pub(crate) fn artifact_version(self) -> &'static str {
        match self {
            Self::BatchV3 => ARTIFACT_VERSION,
            Self::BatchV4 => "4",
            Self::AcpV1 => ACP_ARTIFACT_VERSION,
        }
    }

    pub(crate) fn contract_revision(self) -> &'static str {
        match self {
            Self::BatchV3 => CONTRACT_REVISION,
            Self::BatchV4 => BATCH_V4_CONTRACT_REVISION,
            Self::AcpV1 => ACP_CONTRACT_REVISION,
        }
    }

    pub(crate) fn execution_shape(self) -> &'static str {
        match self {
            Self::BatchV3 | Self::BatchV4 => EXECUTION_SHAPE,
            Self::AcpV1 => ACP_EXECUTION_SHAPE,
        }
    }
}

/// Canonical LF digest of the existing pinned Command mod source. The `.3`
/// and `.4` JavaScript module artifacts remain separate and unchanged.
pub const MOD_SHA256: &str = "513eaa7d6034cc22b5abf14d080888cdd3e8782133e39f859b7703db123e1f80";
