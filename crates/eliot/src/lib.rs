//! Composition shared by the controller and the desktop settings editor.

/// Register the built-in adapters' capabilities before using profile policy.
pub fn register_harness_capabilities() -> eliot_core::Result<()> {
    eliot_core::api::register_harness_capabilities([
        ("codex", eliot_plugin_codex::capabilities::declaration()),
        ("claude", eliot_plugin_claude::capabilities::declaration()),
        (
            "opencode",
            eliot_plugin_opencode::capabilities::declaration(),
        ),
        ("muse", eliot_plugin_muse::capabilities::declaration()),
        (
            "antigravity",
            eliot_plugin_antigravity::capabilities::declaration(),
        ),
    ])
}
