use super::args::ProjectAssignmentCommand;
use super::support::{Failure, mutation_id, print_value};
use eliot_core::api::{
    ProjectAssignment, ProjectAssignmentClaimRequest, ProjectAssignmentGetRequest,
    ProjectAssignmentLaunchRequest, ProjectAssignmentOutcome, ProjectAssignmentReleaseRequest,
    ProjectAssignmentResult, ProjectManagerIdentity,
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
                    .map_err(|error| Failure::from_core(error, false))?;
            print_value(&serde_json::json!(assignment), json_output);
            return Ok(0);
        }
        ProjectAssignmentCommand::Claim {
            project,
            agent,
            generation,
        } => {
            let request = ProjectAssignmentClaimRequest {
                id: mutation_id(id)?,
                project,
                manager: agent
                    .zip(generation)
                    .map(|(agent, generation)| ProjectManagerIdentity::Local { agent, generation }),
            };
            call(data_dir, "project.assignment.claim", &request).await?
        }
        ProjectAssignmentCommand::Launch { project, profile } => {
            let request = ProjectAssignmentLaunchRequest {
                id: mutation_id(id)?,
                project,
                profile,
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

async fn call(
    data_dir: &Path,
    method: &str,
    request: &impl serde::Serialize,
) -> Result<ProjectAssignmentResult, Failure> {
    eliot_daemon::client_call_typed(data_dir, method, request)
        .await
        .map_err(|error| Failure::from_core(error, true))
}
