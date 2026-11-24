use super::{
    args::*,
    support::{Failure, mutation_id, print_value},
};
use eliot_core::api::*;
use std::path::PathBuf;

#[expect(
    clippy::disallowed_methods,
    reason = "Recursive directory removal is centralized in CLI cleanup."
)]
pub(super) fn remove_dir_all(path: &std::path::Path) -> std::io::Result<()> {
    std::fs::remove_dir_all(path)
}

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
            after,
        } => {
            let continuation = after
                .as_deref()
                .map(|token| PlanContinuation::decode(token).and_then(|page| page.encode()))
                .transpose()
                .map_err(Failure::for_read)?;
            let request = CleanupPlanRequest {
                scope: match scope.as_deref() {
                    Some("all") => CleanupScope::All,
                    Some("project") => CleanupScope::Project,
                    Some(_) => CleanupScope::SelfTree,
                    None => CleanupScope::Default,
                },
                project,
                older_than,
                continuation,
            };
            request.validate().map_err(Failure::for_read)?;
            eliot_daemon::client_call_typed::<_, CleanupPlan>(&data, "cleanup.plan", &request)
                .await
                .and_then(|r| {
                    serde_json::to_value(r)
                        .map_err(|_| eliot_core::Error::new("unknown", "invalid plan"))
                })
                .map_err(Failure::for_read)?
        }
        CleanupCommand::Apply {
            plan_id,
            artifacts,
            delete_remote,
        } => {
            let request = CleanupApplyRequest {
                id: mutation_id(id)?,
                plan_id,
                delete_remote,
                artifacts: (!artifacts.is_empty()).then_some(artifacts),
            };
            request.validate().map_err(Failure::for_mutation)?;
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
            .map_err(Failure::for_mutation)?
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
        ttl,
    } = args.command;
    let expires_at = ttl
        .as_deref()
        .map(|duration| {
            let ttl_ms = eliot_daemon::operator::parse_stand_ttl_ms(duration)
                .map_err(Failure::for_mutation)?;
            eliot_daemon::operator::stand_now_ms()
                .checked_add(ttl_ms)
                .ok_or_else(|| Failure::new(2, "TTL overflows epoch timestamp"))
        })
        .transpose()?;
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
        expires_at,
    };
    request.validate().map_err(Failure::for_mutation)?;
    let result: ArtifactView =
        eliot_daemon::client_call_typed(&data, "artifact.register", &request)
            .await
            .map_err(Failure::for_mutation)?;
    print_value(
        &serde_json::to_value(result).map_err(|_| Failure::new(4, "invalid artifact result"))?,
        json,
    );
    Ok(0)
}
