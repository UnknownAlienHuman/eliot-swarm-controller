mod fake;
pub mod peer;
pub mod protocol;

pub use fake::{PluginError, run_fake};
pub use peer::{CommitSink, Peer, PeerClient};
pub use protocol::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_TEXT_BYTES, PluginManifest, RpcError, RpcRequest,
    RpcResponse, fake_manifest,
};
