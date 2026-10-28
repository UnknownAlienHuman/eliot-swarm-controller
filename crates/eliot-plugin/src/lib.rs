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

#[cfg(test)]
mod native_page_imports {
    use std::any::TypeId;

    #[test]
    fn old_plugin_page_paths_are_the_same_core_types() {
        for (core, protocol, root) in [
            (
                TypeId::of::<eliot_core::api::DiscoverSubject>(),
                TypeId::of::<crate::protocol::DiscoverSubject>(),
                TypeId::of::<crate::DiscoverSubject>(),
            ),
            (
                TypeId::of::<eliot_core::api::DiscoverCoverage>(),
                TypeId::of::<crate::protocol::DiscoverCoverage>(),
                TypeId::of::<crate::DiscoverCoverage>(),
            ),
            (
                TypeId::of::<eliot_core::api::DiscoverPage>(),
                TypeId::of::<crate::protocol::DiscoverPage>(),
                TypeId::of::<crate::DiscoverPage>(),
            ),
            (
                TypeId::of::<eliot_core::api::ObservationQuery>(),
                TypeId::of::<crate::protocol::ObservationQuery>(),
                TypeId::of::<crate::ObservationQuery>(),
            ),
            (
                TypeId::of::<eliot_core::api::ObservationPage>(),
                TypeId::of::<crate::protocol::ObservationPage>(),
                TypeId::of::<crate::ObservationPage>(),
            ),
        ] {
            assert_eq!(core, protocol);
            assert_eq!(core, root);
        }
        let query: eliot_core::api::ObservationQuery = serde_json::from_value(serde_json::json!({
            "view": "subject", "limit": 100,
        }))
        .unwrap();
        let legacy: crate::protocol::ObservationQuery = query;
        let root: crate::ObservationQuery = legacy;
        assert_eq!(
            serde_json::to_value(root).unwrap(),
            serde_json::json!({
                "source": "native", "view": "subject", "turn_id": null, "cursor": null, "limit": 100,
            })
        );
    }

    #[test]
    fn old_plugin_page_limit_paths_keep_the_core_values() {
        assert_eq!(
            crate::MAX_DISCOVER_PAGE_ENTRIES,
            eliot_core::api::MAX_DISCOVER_PAGE_ENTRIES
        );
        assert_eq!(
            crate::MAX_DISCOVER_PAGE_BYTES,
            eliot_core::api::MAX_DISCOVER_PAGE_BYTES
        );
        assert_eq!(
            crate::MAX_OBSERVATION_PAGE_ENTRIES,
            eliot_core::api::MAX_OBSERVATION_PAGE_ENTRIES
        );
        assert_eq!(
            crate::MAX_OBSERVATION_PAGE_BYTES,
            eliot_core::api::MAX_OBSERVATION_PAGE_BYTES
        );
        assert_eq!(
            crate::MAX_OBSERVATION_CURSOR_BYTES,
            eliot_core::api::MAX_OBSERVATION_CURSOR_BYTES
        );
    }
}
