//! Headless chat and work organization commands (step 4a).
use super::args::{ChatAttachArgs, ChatCommand, ChatDetachArgs, ChatOrganizationArgs, ChatTagArgs};
use super::support::{Failure, mutation_id, print_value};
use eliot_core::api::{
    ArchivedFilter, ChatAttachRequest, ChatDetachRequest, ChatOrganizationMutationResponse,
    ChatOrganizationRequest, ChatOrganizationResponse, ChatTagRequest,
};
use std::path::Path;

pub(super) async fn run_chat(
    command: ChatCommand,
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        ChatCommand::Organization(ChatOrganizationArgs { project, archived }) => {
            let request = ChatOrganizationRequest {
                project,
                archived: if archived {
                    ArchivedFilter::Include
                } else {
                    ArchivedFilter::Exclude
                },
            };
            request.validate().map_err(Failure::for_read)?;
            let response: ChatOrganizationResponse =
                eliot_daemon::client_call_typed(data_dir, "chat.organization", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            print_value(
                &serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "invalid organization response"))?,
                json_output,
            );
            Ok(0)
        }
        ChatCommand::Attach(ChatAttachArgs { agents, project }) => {
            let request = ChatAttachRequest {
                id: mutation_id(id)?,
                agents,
                project,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: ChatOrganizationMutationResponse =
                eliot_daemon::client_call_typed(data_dir, "chat.attach", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            print_value(
                &serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "invalid attach response"))?,
                json_output,
            );
            Ok(0)
        }
        ChatCommand::Detach(ChatDetachArgs { agents }) => {
            let request = ChatDetachRequest {
                id: mutation_id(id)?,
                agents,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: ChatOrganizationMutationResponse =
                eliot_daemon::client_call_typed(data_dir, "chat.detach", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            print_value(
                &serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "invalid detach response"))?,
                json_output,
            );
            Ok(0)
        }
        ChatCommand::Tag(ChatTagArgs { agents, tag }) => {
            let request = ChatTagRequest {
                id: mutation_id(id)?,
                agents,
                tags: tag,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: ChatOrganizationMutationResponse =
                eliot_daemon::client_call_typed(data_dir, "chat.tag", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            print_value(
                &serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "invalid tag response"))?,
                json_output,
            );
            Ok(0)
        }
    }
}
