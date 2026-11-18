//! Headless chat and work organization commands (step 4a).
use super::args::{
    ChatAttachArgs, ChatCommand, ChatContinueArgs, ChatDetachArgs, ChatOrganizationArgs,
    ChatTagArgs,
};
use super::support::{Failure, mutation_id, print_value, response_exit_code};
use eliot_core::api::{
    ArchivedFilter, ChatAttachRequest, ChatContinueRequest, ChatDetachRequest,
    ChatOrganizationMutationResponse, ChatOrganizationRequest, ChatOrganizationResponse,
    ChatTagRequest, SwitchLaunch,
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
        ChatCommand::Continue(ChatContinueArgs {
            chat,
            project,
            profile,
            harness,
            model,
            effort,
            auth,
            cmd,
            access,
            workspace,
            worktree,
            base,
            text,
        }) => {
            let request = ChatContinueRequest {
                id: mutation_id(id)?,
                chat,
                project,
                launch: SwitchLaunch {
                    profile,
                    harness,
                    model,
                    effort,
                    auth,
                    cmd,
                    access,
                    workspace,
                    worktree,
                    base,
                },
                text,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: eliot_core::api::ChatContinueResponse =
                eliot_daemon::client_call_typed(data_dir, "chat.continue", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let exit = response_exit_code(response.state);
            print_value(
                &serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "invalid continue response"))?,
                json_output,
            );
            Ok(exit)
        }
    }
}
