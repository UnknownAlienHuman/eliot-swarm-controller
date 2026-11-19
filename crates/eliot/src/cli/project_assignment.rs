use super::args::{ProjectAssignmentCommand, ProjectTransferArgs, ProjectTransferCommand};
use super::support::{Failure, mutation_id, print_value};
use eliot_core::api::{
    PROJECT_TRANSFER_METHOD, ProjectAssignment, ProjectAssignmentClaimRequest,
    ProjectAssignmentGetRequest, ProjectAssignmentLaunchRequest, ProjectAssignmentOutcome,
    ProjectAssignmentReleaseRequest, ProjectAssignmentResult, ProjectManagerIdentity,
    ProjectTransferAction, ProjectTransferOutcome, ProjectTransferRequest, ProjectTransferResult,
};
use std::path::Path;

pub(super) async fn run(
    command: ProjectAssignmentCommand,
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let response: ProjectAssignmentResult = match command {
        ProjectAssignmentCommand::Get { project } => {
            let request = ProjectAssignmentGetRequest { project };
            let assignment: ProjectAssignment =
                eliot_daemon::client_call_typed(data_dir, "project.assignment.get", &request)
                    .await
                    .map_err(Failure::for_read)?;
            print_value(&serde_json::json!(assignment), json_output);
            return Ok(0);
        }
        ProjectAssignmentCommand::Claim {
            project,
            agent,
            generation,
            remote,
            grant_revision,
        } => {
            let local = agent
                .zip(generation)
                .map(|(agent, generation)| ProjectManagerIdentity::Local { agent, generation });
            let remote = remote
                .zip(grant_revision)
                .map(
                    |(instance, grant_revision)| ProjectManagerIdentity::Remote {
                        instance,
                        grant_revision,
                    },
                );
            let request = ProjectAssignmentClaimRequest {
                id: mutation_id(id)?,
                project,
                manager: local.or(remote),
            };
            call(data_dir, "project.assignment.claim", &request).await?
        }
        ProjectAssignmentCommand::Launch {
            project,
            profile,
            adopt_worktree,
        } => {
            let request = ProjectAssignmentLaunchRequest {
                id: mutation_id(id)?,
                project,
                profile,
                adopt_worktree,
            };
            call(data_dir, "project.assignment.launch", &request).await?
        }
        ProjectAssignmentCommand::Release { project, epoch } => {
            let request = ProjectAssignmentReleaseRequest {
                id: mutation_id(id)?,
                project,
                expected_epoch: epoch,
            };
            call(data_dir, "project.assignment.release", &request).await?
        }
    };
    print_value(&serde_json::json!(response), json_output);
    Ok(match response.outcome {
        ProjectAssignmentOutcome::Claimed
        | ProjectAssignmentOutcome::LaunchAdmitted
        | ProjectAssignmentOutcome::Released => 0,
        ProjectAssignmentOutcome::Occupied
        | ProjectAssignmentOutcome::StaleEpoch
        | ProjectAssignmentOutcome::Vacant
        | ProjectAssignmentOutcome::StartupUncertain => 3,
    })
}

pub(super) async fn transfer(
    args: ProjectTransferArgs,
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let action = match args.command {
        ProjectTransferCommand::Begin { saved_context } => {
            ProjectTransferAction::Begin { saved_context }
        }
        ProjectTransferCommand::Launch { intent, profile } => {
            ProjectTransferAction::Launch { intent, profile }
        }
        ProjectTransferCommand::Commit {
            intent,
            destination,
            generation,
        } => ProjectTransferAction::Commit {
            intent,
            destination: ProjectManagerIdentity::Local {
                agent: destination,
                generation,
            },
        },
        ProjectTransferCommand::Cancel { intent } => ProjectTransferAction::Cancel { intent },
    };
    let request = ProjectTransferRequest {
        id: mutation_id(id)?,
        project: args.project,
        expected_revision: args.revision,
        action,
    };
    request.validate().map_err(Failure::for_mutation)?;
    let response: ProjectTransferResult =
        eliot_daemon::client_call_typed(data_dir, PROJECT_TRANSFER_METHOD, &request)
            .await
            .map_err(Failure::for_mutation)?;
    if response.id != request.id || response.assignment.project != request.project {
        return Err(Failure::new(
            4,
            "project.transfer response does not match the request",
        ));
    }
    print_value(&serde_json::json!(response), json_output);
    Ok(match response.outcome {
        ProjectTransferOutcome::Blocked | ProjectTransferOutcome::WaitingCapacity => 3,
        _ => 0,
    })
}

async fn call(
    data_dir: &Path,
    method: &str,
    request: &impl serde::Serialize,
) -> Result<ProjectAssignmentResult, Failure> {
    eliot_daemon::client_call_typed(data_dir, method, request)
        .await
        .map_err(Failure::for_mutation)
}
