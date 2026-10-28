mod fake;
mod file_identity;
pub mod peer;
pub mod protocol;
mod stderr;

pub use fake::{PluginError, run_fake};
pub use file_identity::{FileIdentity, file_final_path, file_identity};
pub use peer::{CommitSink, Peer, PeerClient};
pub use protocol::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, DiscoverCoverage,
    DiscoverPage, DiscoverParams, DiscoverSubject, InitializeParams, MAX_DISCOVER_PAGE_BYTES,
    MAX_DISCOVER_PAGE_ENTRIES, MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_OBSERVATION_CURSOR_BYTES,
    MAX_OBSERVATION_PAGE_BYTES, MAX_OBSERVATION_PAGE_ENTRIES, MAX_TEXT_BYTES, ObservationPage,
    ObservationQuery, ObserverAttachParams, ObserverDetachParams, ObserverSnapshotParams,
    PluginManifest, RpcError, RpcRequest, RpcResponse, fake_manifest,
};
pub use stderr::{StderrCapture, StderrSnapshot, StderrView, spawn_stderr};
