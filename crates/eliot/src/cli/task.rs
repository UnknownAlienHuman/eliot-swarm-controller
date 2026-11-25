use super::{
    args::{TaskArgs, TaskCommand},
    support::{Failure, print_value, read_and_print},
};
use eliot_core::api::{TaskGetRequest, TaskGetResponse, TaskListRequest, TaskListResponse};
use std::path::PathBuf;

pub(crate) async fn run(
    args: TaskArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    match args.command {
        TaskCommand::List {
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
                    Failure::new(
                        2,
                        "--cursor must be the JSON object returned as next_cursor",
                    )
                })?;
            let request = TaskListRequest {
                agent,
                generation,
                limit,
                cursor,
            };
            read_and_print!(
                &data_dir,
                json_output,
                "task.list",
                request,
                TaskListResponse
            )
        }
        TaskCommand::Get {
            agent,
            generation,
            task_command,
            section,
            revision,
            content_digest,
            offset,
        } => {
            let request = TaskGetRequest {
                agent,
                generation,
                task_command,
                section,
                revision,
                content_digest,
                offset,
            };
            read_and_print!(&data_dir, json_output, "task.get", request, TaskGetResponse)
        }
    }
}
