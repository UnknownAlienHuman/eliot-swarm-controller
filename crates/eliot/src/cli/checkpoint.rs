use super::{
    args::{CheckpointArgs, CheckpointCommand},
    support::{Failure, mutation_id, print_value_or_human},
};
use eliot_core::api::{CheckpointRestoreRequest, CheckpointRestoreResponse};
use std::path::PathBuf;

pub(crate) async fn run(
    args: CheckpointArgs,
    data_dir: PathBuf,
    json_output: bool,
    supplied_id: Option<String>,
) -> Result<u8, Failure> {
    match args.command {
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
