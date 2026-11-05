use eliot_core::{Result, SecretPolicy};
use eliot_plugin::native_transport::{
    NativeChild, NativeLabels, NativeLaunch, PumpAction, StreamState, spawn_pumped,
};
use tokio::sync::{mpsc, watch};

use super::AgyFrame;

pub(crate) async fn spawn(
    launch: NativeLaunch,
    policy: SecretPolicy,
) -> Result<(
    NativeChild,
    mpsc::Receiver<AgyFrame>,
    watch::Receiver<StreamState>,
)> {
    let (child, _, frames, stream) = spawn_pumped(
        launch,
        policy,
        NativeLabels {
            short: "Antigravity",
            server: "Antigravity",
        },
        "agy",
        |value| match serde_json::from_value(value) {
            Ok(frame) => PumpAction::Emit(frame),
            Err(_) => {
                PumpAction::Malformed("Antigravity frame did not match its event envelope".into())
            }
        },
    )
    .await?;
    Ok((child, frames, stream))
}
