use super::{
    args::*,
    support::{Failure, mutation_id, print_value},
};
use eliot_core::api::*;
use std::path::PathBuf;

pub(crate) async fn run_cleanup(
    args: CleanupArgs,
    data: PathBuf,
    json: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let value = match args.command {
        CleanupCommand::Plan {
            scope,
            project,
            older_than,
        } => {
            let request = CleanupPlanRequest {
                scope: match scope.as_str() {
                    "all" => CleanupScope::All,
                    "project" => CleanupScope::Project,
                    _ => CleanupScope::SelfTree,
                },
                project,
                older_than,
            };
            request
                .validate()
                .map_err(|e| Failure::from_core(e, false))?;
            eliot_daemon::client_call_typed::<_, CleanupPlan>(&data, "cleanup.plan", &request)
                .await
                .and_then(|r| {
                    serde_json::to_value(r)
                        .map_err(|_| eliot_core::Error::new("unknown", "invalid plan"))
                })
                .map_err(|e| Failure::from_core(e, false))?
        }
        CleanupCommand::Apply { plan_id } => {
            let request = CleanupApplyRequest {
                id: mutation_id(id)?,
                plan_id,
            };
            request
                .validate()
                .map_err(|e| Failure::from_core(e, true))?;
            eliot_daemon::client_call_typed::<_, CleanupApplyResponse>(
                &data,
                "cleanup.apply",
                &request,
            )
            .await
            .and_then(|r| {
                serde_json::to_value(r)
                    .map_err(|_| eliot_core::Error::new("unknown", "invalid cleanup result"))
            })
            .map_err(|e| Failure::from_core(e, true))?
        }
    };
    print_value(&value, json);
    Ok(0)
}

pub(crate) async fn run_artifact(
    args: ArtifactArgs,
    data: PathBuf,
    json: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let ArtifactCommand::Register {
        path,
        kind,
        purpose,
    } = args.command;
    let request = ArtifactRegisterRequest {
        id: mutation_id(id)?,
        path,
        purpose,
        kind: match kind.as_str() {
            "build" => ArtifactKind::Build,
            "data_dir" => ArtifactKind::DataDir,
            "temp" => ArtifactKind::Temp,
            _ => ArtifactKind::Other,
        },
    };
    request
        .validate()
        .map_err(|e| Failure::from_core(e, true))?;
    let result: ArtifactView =
        eliot_daemon::client_call_typed(&data, "artifact.register", &request)
            .await
            .map_err(|e| Failure::from_core(e, true))?;
    print_value(
        &serde_json::to_value(result).map_err(|_| Failure::new(4, "invalid artifact result"))?,
        json,
    );
    Ok(0)
}
