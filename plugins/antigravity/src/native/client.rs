use eliot_core::{Result, SecretPolicy};
use eliot_plugin::native_transport::{
    NativeChild, NativeLabels, NativeLaunch, PumpAction, PumpMode, StreamState, spawn_pumped,
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
    // AGY stream-json has no native RPC replies (init/step_update/result
    // only), so bounded backpressure cannot deadlock a reply.
    let (child, _, frames, stream) = spawn_pumped(
        launch,
        policy,
        NativeLabels {
            short: "Antigravity",
            server: "Antigravity",
        },
        "agy",
        PumpMode::LosslessBackpressure,
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
