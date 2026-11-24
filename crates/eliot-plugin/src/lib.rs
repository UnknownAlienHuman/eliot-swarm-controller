mod bounded_file;
pub mod capabilities;
pub mod debug;
pub mod edit_path;
pub mod event_actor;
mod file_identity;
pub mod install;
pub mod native_child;
pub mod native_cli;
pub mod native_data;
pub mod native_lifecycle;
pub mod native_stdio;
pub mod native_transport;
pub mod owner_scan;
pub mod peer;
pub mod protocol;
pub mod quota;
pub mod resume;
pub mod retry;
pub mod session;
pub mod settle;
pub mod skill;
pub mod start_classification;
mod stderr;
pub mod sync;
pub mod text_inference;
pub mod usage;

pub use debug::{DebugRecord, parse_debug_line};
pub use file_identity::{FileIdentity, file_final_path, file_identity};
pub use peer::{
    CommitSink, Peer, PeerClient, SnapshotDispatch, is_definite_batch_rejection,
    preemptible_snapshot,
};
pub use protocol::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, DiscoverParams,
    InitializeParams, MAX_EVENT_BYTES, MAX_FRAME_BYTES, MAX_TEXT_BYTES, ObserverAttachParams,
    ObserverDetachParams, ObserverSnapshotParams, PluginManifest, RpcError, RpcRequest,
    RpcResponse, SessionUsageSampleAck, SessionUsageSampleRequest,
};
pub use stderr::{
    StderrCapture, StderrSnapshot, StderrView, harness_diagnostic, spawn_plugin_stderr,
    spawn_stderr, spawn_stderr_prefix,
};
