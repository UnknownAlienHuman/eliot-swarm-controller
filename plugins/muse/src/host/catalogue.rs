use super::{contextual_error, native_initialize_params};
use crate::native::{
    NativeClient, NativeLaunch, NativeProcess,
    types::{NativeInitializeResult, NativeModelListParams, NativeModelListResult},
};
use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::native_cli::{BoundedReadLabels, run_bounded_with_stderr};
use std::{path::Path, time::Duration};
#[expect(
    clippy::disallowed_types,
    reason = "Muse catalogue refresh uses an owned native CLI command."
)]
use tokio::process::Command;
use tokio::time::Instant;

const QUERY_BUDGET: Duration = Duration::from_secs(15);
const REFRESH_BUDGET: Duration = Duration::from_secs(12);
const CLOSE_BUDGET: Duration = Duration::from_secs(3);

pub(super) struct CatalogueHost {
    pub(super) native: NativeProcess,
    pub(super) identity: NativeInitializeResult,
    pub(super) catalogue: NativeModelListResult,
    refresh_error: Option<Error>,
}

impl CatalogueHost {
    pub(super) async fn open(
        launch: &NativeLaunch,
        instance_dir: &Path,
        policy: &SecretPolicy,
        observe_models: bool,
    ) -> Result<Self> {
        let mut host = Self::read(launch, policy, observe_models).await?;
        if !host.catalogue.models.is_empty() {
            return Ok(host);
        }

        // The current serve process took its snapshot at startup. Confirm its
        // exit before refreshing the cache and opening a fresh host.
        host.close().await?;
        let refresh_error = refresh(launch, instance_dir, policy).await.err();
        let mut host = Self::read(launch, policy, observe_models)
            .await
            .map_err(|error| with_refresh_failure(error, refresh_error.as_ref()))?;
        host.refresh_error = refresh_error;
        Ok(host)
    }

    async fn read(
        launch: &NativeLaunch,
        policy: &SecretPolicy,
        observe_models: bool,
    ) -> Result<Self> {
        let deadline = Instant::now() + QUERY_BUDGET;
        let mut native = NativeProcess::spawn(launch.clone(), policy.clone())
            .await
            .map_err(|error| {
                let message = format!("Muse session host could not be spawned ({})", error.code);
                error.with_message(message)
            })?;
        match read_snapshot(&native.client(), deadline, observe_models).await {
            Ok((identity, catalogue)) => Ok(Self {
                native,
                identity,
                catalogue,
                refresh_error: None,
            }),
            Err(error) => {
                close(&mut native).await?;
                Err(error)
            }
        }
    }

    pub(super) async fn close(&mut self) -> Result<()> {
        close(&mut self.native).await
    }

    pub(super) fn catalogue_error(&self, error: Error) -> Error {
        with_refresh_failure(error, self.refresh_error.as_ref())
    }

    pub(super) fn refresh_failed(&self) -> bool {
        self.refresh_error.is_some()
    }
}

async fn close(native: &mut NativeProcess) -> Result<()> {
    native
        .terminate(false, Instant::now() + CLOSE_BUDGET)
        .await?;
    Ok(())
}

async fn read_snapshot(
    client: &NativeClient,
    deadline: Instant,
    observe_models: bool,
) -> Result<(NativeInitializeResult, NativeModelListResult)> {
    let init = serde_json::to_value(native_initialize_params(observe_models))
        .map_err(|_| Error::new("internal", "Muse initialize params could not be encoded"))?;
    let identity = client
        .call("initialize", init, deadline)
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "profile_error",
                "Muse session host initialize failed",
            )
        })?;
    let identity = serde_json::from_value(identity)
        .map_err(|_| Error::new("profile_error", "Muse initialize response was invalid"))?;
    client
        .notify("initialized", None, deadline)
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "profile_error",
                "Muse session host did not accept initialized",
            )
        })?;
    let params = serde_json::to_value(NativeModelListParams { session_id: None })
        .map_err(|_| Error::new("internal", "Muse model/list params could not be encoded"))?;
    let models = client
        .call("model/list", params, deadline)
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "profile_error",
                "Muse model/list did not return a catalogue",
            )
        })?;
    let models = serde_json::from_value(models).map_err(|_| {
        Error::new(
            "profile_error",
            "Muse model/list result did not match the installed schema",
        )
    })?;
    Ok((identity, models))
}

async fn refresh(launch: &NativeLaunch, instance_dir: &Path, policy: &SecretPolicy) -> Result<()> {
    #[expect(
        clippy::disallowed_methods,
        reason = "This statement constructs an owned Muse catalogue process."
    )]
    #[expect(
        clippy::disallowed_types,
        reason = "This command runs Muse's read-only model listing."
    )]
    let mut command = Command::new(&launch.program);
    command
        .args(["exec", "--list-models"])
        .current_dir(instance_dir)
        .envs(launch.environment.iter().map(|(name, value)| (name, value)));
    run_bounded_with_stderr(
        command,
        64 * 1024,
        Instant::now() + REFRESH_BUDGET,
        &BoundedReadLabels {
            start: "Muse exec --list-models could not start",
            deadline: "Muse exec --list-models did not finish before its deadline",
            unreadable: "Muse exec --list-models output was unreadable",
            status: "Muse exec --list-models failed to refresh the provider catalogue",
        },
        policy,
    )
    .await?;
    Ok(())
}

fn with_refresh_failure(mut error: Error, refresh: Option<&Error>) -> Error {
    if let Some(refresh) = refresh {
        let message = format!("{}; {}", error.message, refresh.message);
        error = error.with_message(message);
        for diagnostic in refresh.diagnostics.iter() {
            if !diagnostic.snapshot.text.trim().is_empty() {
                error = error.with_stderr(diagnostic.source, diagnostic.snapshot.clone());
            }
        }
    }
    error
}
