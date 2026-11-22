use super::{
    args::{GitCommand, GitIntegrateArgs},
    support::{
        Failure, command_state_result, print_value_or_human, response_exit_code, submit_and_print,
        submit_command, submit_request, wait_call, wait_deadline,
    },
};
use eliot_core::{
    CommandState,
    api::{
        CommandGetRequest, GitIntegrateCheckClassification, GitIntegrateDefinitionArgs,
        GitIntegrateOutcome, GitIntegrateStatus,
    },
    fence_untrusted_text, reason_words,
};
use serde_json::json;
use std::path::Path;

pub(crate) async fn run(
    command: GitCommand,
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let GitCommand::Integrate(args) = command;
    let GitIntegrateArgs {
        project,
        base,
        branches,
        fast_forward_to,
        timeout,
        wait,
        timeout_seconds,
    } = args;
    let definition = GitIntegrateDefinitionArgs {
        repository: project,
        base_branch: base,
        branches,
        fast_forward_to,
        timeout,
    };
    definition.validate().map_err(Failure::for_mutation)?;
    let payload = serde_json::to_value(definition)
        .map_err(|_| Failure::new(2, "could not encode integration request"))?;
    let request = submit_request(id, None, "git.integrate", payload)?;
    if !wait {
        return submit_and_print(data_dir, &request, json_output).await;
    }
    let deadline = wait_deadline(timeout_seconds)?;
    let submitted = submit_command(data_dir, &request).await?;
    let query = CommandGetRequest::new(&submitted.command_id);
    let command = wait_call(
        eliot_daemon::wait_command(data_dir, &query, deadline),
        deadline,
    )
    .await?
    .ok_or_else(Failure::timeout)?;
    let Some(outcome) = command
        .outcome
        .as_ref()
        .filter(|body| body.get("base_oid").is_some())
    else {
        command_state_result(&command)?;
        return Err(Failure::new(4, "integration result is unavailable"));
    };
    let result: GitIntegrateOutcome = serde_json::from_value(outcome.clone())
        .map_err(|_| Failure::new(4, "integration result is invalid"))?;
    let value = json!({
        "command_id": command.id,
        "state": command.state,
        "outcome": outcome,
    });
    print_value_or_human(&value, json_output, || {
        print_integration(&result, &command.id, command.state)
    });
    let command_exit = response_exit_code(command.state);
    Ok(match result.status {
        _ if command_exit != 0 => command_exit,
        GitIntegrateStatus::Merged => 0,
        GitIntegrateStatus::Conflict | GitIntegrateStatus::CheckFailed => 1,
        GitIntegrateStatus::Unknown => 4,
    })
}

fn print_integration(result: &GitIntegrateOutcome, command_id: &str, state: CommandState) {
    if result.cleanup_pending {
        println!("Integration scratch cleanup is pending");
    }
    match result.status {
        GitIntegrateStatus::Merged => {
            println!(
                "Merged and checked {}",
                result.integration_oid.as_deref().unwrap_or("")
            );
            if let Some(update) = &result.fast_forward {
                println!("Fast-forwarded local branch");
                println!("{}", fence_untrusted_text(&update.branch));
            }
            if let Some(code) = &result.failure_code {
                println!("{}", reason_words(code));
            }
        }
        GitIntegrateStatus::Conflict => {
            println!("Merge conflict");
            if !result.conflict_paths.is_empty() {
                println!(
                    "{}",
                    fence_untrusted_text(&result.conflict_paths.join("\n"))
                );
            }
            if result.conflict_paths_truncated {
                println!("Additional conflict paths omitted");
            }
        }
        GitIntegrateStatus::CheckFailed => {
            println!("Project check failed");
            if let Some(check) = &result.check_failure {
                if check.classification == GitIntegrateCheckClassification::CombinationOnly {
                    println!("The check passed for each branch in isolation");
                }
                if !check.failed_branches.is_empty() {
                    println!("Branches failing the project check");
                    println!(
                        "{}",
                        fence_untrusted_text(&check.failed_branches.join("\n"))
                    );
                }
                if !check.unknown_branches.is_empty() {
                    println!("Branches with an unconfirmed check result");
                    println!(
                        "{}",
                        fence_untrusted_text(&check.unknown_branches.join("\n"))
                    );
                }
                if !check.tail.is_empty() {
                    println!("{}", fence_untrusted_text(&check.tail));
                }
            }
        }
        GitIntegrateStatus::Unknown => {
            let summary = match state {
                CommandState::Rejected => "Integration declined",
                CommandState::Failed => "Integration failed",
                CommandState::Cancelled => "Integration cancelled",
                _ => "The integration result couldn't be confirmed",
            };
            println!("{summary}. Command {command_id}");
            if let Some(code) = &result.failure_code {
                println!("{}", reason_words(code));
            }
        }
    }
}
