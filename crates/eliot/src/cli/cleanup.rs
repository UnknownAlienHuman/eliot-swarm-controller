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

/// Remove only verified, inactive images owned by the updater in this release
/// directory. Linux can unlink a mapped image while its running process keeps
/// the inode; Windows refuses deletion while a process has the image mapped.
/// Any uncertain or failed removal is retained and reported.
#[cfg(any(windows, target_os = "linux"))]
pub(super) fn prune_update_images(
    data_root: &std::path::Path,
    retained_sha256: &[&str],
    keep_desktop_images: bool,
) -> Result<(), String> {
    let directory = eliot_daemon::operator::update_images_directory(data_root)
        .map_err(|error| error.to_string())?;
    let entries = std::fs::read_dir(directory)
        .map_err(|error| format!("inspect staged image directory: {error}"))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("read staged image directory: {error}"))?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(digest) = name
            .strip_prefix(".swarm-release-")
            .or_else(|| name.strip_prefix(".swarm-desktop-release-"))
            .and_then(|value| value.strip_suffix(std::env::consts::EXE_SUFFIX))
        else {
            continue;
        };
        if name.starts_with(".swarm-desktop-release-") && keep_desktop_images {
            continue;
        }
        if retained_sha256.contains(&digest) {
            continue;
        }
        if eliot_daemon::operator::validate_update_image(&path).is_err() {
            continue;
        }
        let metadata = std::fs::symlink_metadata(&path)
            .map_err(|error| format!("inspect staged image {}: {error}", path.display()))?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            continue;
        }
        match eliot_daemon::operator::binary_sha256(&path) {
            Ok(hash) if hash == digest => {}
            _ => continue,
        }
        #[cfg(windows)]
        {
            let mut permissions = metadata.permissions();
            #[expect(
                clippy::permissions_set_readonly_false,
                reason = "Windows requires clearing the read-only attribute before pruning an inactive staged image"
            )]
            permissions.set_readonly(false);
            std::fs::set_permissions(&path, permissions).map_err(|error| {
                format!(
                    "prepare staged image {} for removal: {error}",
                    path.display()
                )
            })?;
        }
        if let Err(error) = std::fs::remove_file(&path) {
            #[cfg(windows)]
            {
                let mut permissions = std::fs::metadata(&path)
                .map_err(|restore| {
                    format!(
                        "remove inactive staged image {} failed: {error}; cannot restore its permissions: {restore}",
                        path.display()
                    )
                })?
                .permissions();
                permissions.set_readonly(true);
                std::fs::set_permissions(&path, permissions).map_err(|restore| {
                format!(
                    "remove inactive staged image {} failed: {error}; cannot restore read-only protection: {restore}",
                    path.display()
                )
            })?;
            }
            return Err(format!(
                "remove inactive staged image {}: {error}",
                path.display()
            ));
        }
    }
    Ok(())
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
        CleanupCommand::ApplyAll {
            scope,
            project,
            older_than,
        } => {
            let request = CleanupApplyAllRequest {
                id: mutation_id(id)?,
                scope: match scope.as_deref() {
                    Some("all") => CleanupScope::All,
                    Some("project") => CleanupScope::Project,
                    None => CleanupScope::Default,
                    Some(_) => {
                        return Err(Failure::new(2, "--scope must be all or project"));
                    }
                },
                project,
                older_than,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let reply: CleanupJobStatus =
                eliot_daemon::client_call_typed(&data, "cleanup.apply_all", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            if reply.id != request.id {
                return Err(Failure::invalid_response(eliot_core::Error::new(
                    "source_integrity",
                    "cleanup apply-all returned a different job ID",
                )));
            }
            let value = serde_json::to_value(&reply)
                .map_err(|_| Failure::new(4, "invalid cleanup job result"))?;
            print_cleanup_job(&value, &reply.id, &reply.state, json);
            return Ok(0);
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
            let reply: CleanupApplyResponse =
                eliot_daemon::client_call_typed(&data, "cleanup.apply", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            let value = serde_json::to_value(&reply)
                .map_err(|_| Failure::new(4, "invalid cleanup result"))?;
            print_value(&value, json);
            if let Some(state) = reply.state.as_ref() {
                print_cleanup_receipt(&request.id, state, json);
            }
            return Ok(0);
        }
        CleanupCommand::Status { id } => {
            let request = CleanupStatusRequest { id };
            let reply: CleanupJobStatus =
                eliot_daemon::client_call_typed(&data, "cleanup.status", &request)
                    .await
                    .map_err(Failure::for_read)?;
            if reply.id != request.id {
                return Err(Failure::invalid_response(eliot_core::Error::new(
                    "source_integrity",
                    "cleanup status returned a different job ID",
                )));
            }
            let value = serde_json::to_value(&reply)
                .map_err(|_| Failure::new(4, "invalid cleanup status"))?;
            print_cleanup_job(&value, &reply.id, &reply.state, json);
            return Ok(0);
        }
    };
    print_value(&value, json);
    Ok(0)
}

fn print_cleanup_job(value: &serde_json::Value, id: &str, state: &CleanupJobState, json: bool) {
    print_value(value, json);
    if !json {
        print_cleanup_receipt(id, state, false);
    }
}

fn print_cleanup_receipt(id: &str, state: &CleanupJobState, json: bool) {
    if json {
        return;
    }
    println!("Cleanup job {id}: state={}.", cleanup_state_name(state));
    if matches!(state, CleanupJobState::Running) {
        println!("Follow progress with: swarm cleanup status {id}");
    }
}

fn cleanup_state_name(state: &CleanupJobState) -> &'static str {
    match state {
        CleanupJobState::Running => "running",
        CleanupJobState::Completed => "completed",
        CleanupJobState::Failed => "failed",
        CleanupJobState::Interrupted => "interrupted",
    }
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
