use super::{
    args::{CheckpointArgs, CheckpointCommand},
    support::{Failure, mutation_id, print_value, print_value_or_human, read_and_print},
};
use eliot_core::api::{
    CheckpointGetRequest, CheckpointGetResponse, CheckpointListRequest, CheckpointListResponse,
    CheckpointRestoreRequest, CheckpointRestoreResponse,
};
use std::path::PathBuf;

pub(crate) async fn run(
    args: CheckpointArgs,
    data_dir: PathBuf,
    json_output: bool,
    supplied_id: Option<String>,
) -> Result<u8, Failure> {
    match args.command {
        CheckpointCommand::List {
            agent,
            generation,
            limit,
            cursor,
        } => {
            let cursor = cursor
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|_| {
                    Failure::new(2, "--cursor must be the JSON object returned as next")
                })?;
            let request = CheckpointListRequest {
                agent,
                generation,
                limit,
                cursor,
            };
            read_and_print!(
                &data_dir,
                json_output,
                "checkpoint.list",
                request,
                CheckpointListResponse
            )
        }
        CheckpointCommand::Get {
            agent,
            generation,
            checkpoint_ref,
            oid,
            limit,
            cursor,
        } => {
            let cursor = cursor
                .as_deref()
                .map(serde_json::from_str)
                .transpose()
                .map_err(|_| {
                    Failure::new(2, "--cursor must be the JSON object returned as next")
                })?;
            let request = CheckpointGetRequest {
                agent,
                generation,
                checkpoint_ref,
                oid,
                limit,
                cursor,
            };
            read_and_print!(
                &data_dir,
                json_output,
                "checkpoint.get",
                request,
                CheckpointGetResponse
            )
        }
        CheckpointCommand::Restore {
            agent,
            generation,
            checkpoint_ref,
            workspace,
        } => {
            let request = CheckpointRestoreRequest {
                id: mutation_id(supplied_id)?,
                agent,
                generation,
                checkpoint_ref,
                workspace,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: CheckpointRestoreResponse =
                eliot_daemon::client_call_typed(&data_dir, "checkpoint.restore", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            let value = serde_json::to_value(&response)
                .map_err(|_| Failure::new(4, "checkpoint restore response is invalid"))?;
            print_value_or_human(&value, json_output, || {
                println!(
                    "Restored {} paths at HEAD {}",
                    response.paths.len(),
                    response.head
                );
            });
            Ok(0)
        }
    }
}
