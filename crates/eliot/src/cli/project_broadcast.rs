use super::support::{Failure, mutation_id, print_value_or_human};
use eliot_core::{CommandState, api::ProjectBroadcastRequest, api::ProjectBroadcastResponse};
use std::path::Path;

pub(super) async fn run(
    project: String,
    text: String,
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let request = ProjectBroadcastRequest {
        id: mutation_id(id)?,
        project,
        text,
    };
    request.validate().map_err(Failure::for_mutation)?;
    let response: ProjectBroadcastResponse =
        eliot_daemon::client_call_typed(data_dir, "project.broadcast", &request)
            .await
            .map_err(Failure::for_mutation)?;
    request
        .validate_response(&response)
        .map_err(|_| Failure::new(4, "project.broadcast response is invalid"))?;
    let value = serde_json::to_value(&response)
        .map_err(|_| Failure::new(4, "project.broadcast response is unavailable"))?;
    print_value_or_human(&value, json_output, || {
        if response.recipients.is_empty() {
            println!("No visible working Agents in project {}.", response.project);
        } else {
            println!(
                "Queued soonest context for {} Agents in project {}",
                response.recipients.len(),
                response.project
            );
            for recipient in &response.recipients {
                println!(
                    "{}  {} (turn {}, command {})",
                    recipient.agent,
                    command_state_name(recipient.state),
                    recipient.turn_id,
                    recipient.command_id
                );
            }
        }
    });
    Ok(0)
}

fn command_state_name(state: CommandState) -> &'static str {
    match state {
        CommandState::Accepted => "accepted",
        CommandState::Queued => "queued",
        CommandState::Dispatched => "dispatched",
        CommandState::Applied => "applied",
        CommandState::Rejected => "rejected",
        CommandState::Failed => "failed",
        CommandState::Unknown => "unknown",
        CommandState::Cancelled => "cancelled",
    }
}
