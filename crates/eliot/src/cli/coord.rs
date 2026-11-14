//! Command and turn resolution, mail, variables, workers and claims.
use super::args::{
    ClaimArgs, ClaimCommand, ClaimListArgs, ClaimReleaseArgs, ClaimSetArgs, CmdArgs,
    CmdChildrenArgs, CmdCommand, CmdResolveArgs, CmdShowArgs, InboxArgs, MsgAckArgs, MsgArgs,
    MsgCommand, MsgSendArgs, MsgShowArgs, PageArgs, TurnArgs, TurnCommand, TurnResolveArgs,
    VarArgs, VarCommand, VarGetArgs, VarSetArgs, WhoArgs,
};
use super::support::{
    Failure, WAIT_POLL_INTERVAL, command_value, mutation_id, print_value, validate_limit,
};
use eliot_core::Command as CoreCommand;
use eliot_core::api::{
    ClaimListRequest, ClaimListResponse, ClaimReleaseRequest, ClaimReleaseResponse,
    ClaimSetRequest, ClaimSetResponse, CommandChildrenRequest, CommandChildrenResponse,
    CommandGetRequest, CommandResolution, CommandResolveRequest, CommandResolveResponse,
    MAX_VAR_VALUE_BYTES, MailAckRequest, MailAckResponse, MailDelivery, MailInboxRequest,
    MailInboxResponse, MailMode, MailReadRequest, MailSendRequest, MailSendResponse,
    TurnResolveRequest, TurnResolveResponse, TurnResolveStatus, VarGetRequest, VarGetResponse,
    VarSetRequest, VarSetResponse, WhoRequest, WhoResponse,
};
use serde_json::Value;
use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::time::{Instant, sleep};

pub(crate) async fn run_cmd(
    CmdArgs { command }: CmdArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        CmdCommand::Show(CmdShowArgs { id }) => {
            let params = CommandGetRequest::new(id);
            params
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: CoreCommand =
                eliot_daemon::client_call_typed(&data_dir, "command.get", &params)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            params
                .validate_response(&response)
                .map_err(|error| Failure::from_core(error, false))?;
            let result = command_value(response)?;
            print_value(&result, json_output);
            Ok(0)
        }
        CmdCommand::Children(CmdChildrenArgs {
            id,
            after,
            page: PageArgs { limit },
        }) => {
            validate_limit(limit)?;
            let request = CommandChildrenRequest { id, after, limit };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: CommandChildrenResponse =
                eliot_daemon::client_call_typed(&data_dir, "command.children", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "command.children response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "command.children response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        CmdCommand::Resolve(CmdResolveArgs {
            command_id,
            resolution,
            reason,
        }) => {
            let resolution = CommandResolution::parse(&resolution)
                .map_err(|_| Failure::new(2, "--as must be applied or failed"))?;
            let request = CommandResolveRequest {
                id: mutation_id(id)?,
                command_id,
                resolution,
                reason,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: CommandResolveResponse =
                eliot_daemon::client_call_typed(&data_dir, "command.resolve", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "command.resolve response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "command.resolve response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}

pub(crate) async fn run_turn(
    TurnArgs { command }: TurnArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        TurnCommand::Resolve(TurnResolveArgs {
            agent,
            generation,
            turn_id,
            status,
            reason,
        }) => {
            let status = TurnResolveStatus::parse(&status).map_err(|_| {
                Failure::new(2, "--status must be completed, failed, or interrupted")
            })?;
            let request = TurnResolveRequest {
                id: mutation_id(id)?,
                agent,
                generation,
                turn_id,
                status,
                reason,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: TurnResolveResponse =
                eliot_daemon::client_call_typed(&data_dir, "turn.resolve", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "turn.resolve response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "turn.resolve response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}

pub(crate) async fn run_msg(
    MsgArgs { command }: MsgArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        MsgCommand::Send(MsgSendArgs {
            to,
            text,
            thread,
            reply_to,
            mode,
        }) => {
            let mode: MailMode = serde_json::from_value(Value::String(mode))
                .map_err(|_| Failure::new(2, "mail mode must be queue, send, steer or inject"))?;
            let request = MailSendRequest {
                id: mutation_id(id)?,
                to,
                mode,
                text,
                thread,
                reply_to_delivery: reply_to,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: MailSendResponse =
                eliot_daemon::client_call_typed(&data_dir, "mail.send", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "mail.send response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "mail.send response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        MsgCommand::Show(MsgShowArgs { delivery, agent }) => {
            let request = MailReadRequest {
                agent,
                delivery_id: delivery,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: MailDelivery =
                eliot_daemon::client_call_typed(&data_dir, "mail.read", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "mail.read response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "mail.read response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        MsgCommand::Ack(MsgAckArgs { delivery, agent }) => {
            let request = MailAckRequest {
                id: mutation_id(id)?,
                agent,
                deliveries: delivery,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: MailAckResponse =
                eliot_daemon::client_call_typed(&data_dir, "mail.ack", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "mail.ack response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "mail.ack response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}

pub(crate) async fn run_inbox(
    InboxArgs {
        agent,
        limit,
        after,
        wait,
    }: InboxArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let request = MailInboxRequest {
        agent,
        limit,
        after,
    };
    request
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let deadline = wait.map(|secs| Instant::now() + Duration::from_secs(secs));
    loop {
        let response: MailInboxResponse =
            eliot_daemon::client_call_typed(&data_dir, "mail.inbox", &request)
                .await
                .map_err(|error| Failure::from_core(error, false))?;
        request
            .validate_response(&response)
            .map_err(|_| Failure::new(4, "mail.inbox response is invalid"))?;
        let done = match deadline {
            None => true,
            Some(end) => !response.deliveries.is_empty() || Instant::now() >= end,
        };
        if done {
            let value = serde_json::to_value(&response)
                .map_err(|_| Failure::new(4, "mail.inbox response is invalid"))?;
            print_value(&value, json_output);
            if deadline.is_some() && response.deliveries.is_empty() {
                return Ok(5);
            }
            return Ok(0);
        }
        sleep(WAIT_POLL_INTERVAL).await;
    }
}

pub(crate) async fn run_var(
    VarArgs { command }: VarArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        VarCommand::Set(VarSetArgs {
            key,
            value,
            value_file,
            scope,
            as_json,
            expect_version,
            note,
        }) => {
            let raw_value = match (value, value_file) {
                (Some(value), None) => value,
                (None, Some(path)) => read_var_value_file(&path)?,
                _ => {
                    return Err(Failure::new(
                        2,
                        "provide exactly one of VALUE or --value-file",
                    ));
                }
            };
            let value = if as_json {
                serde_json::from_str(&raw_value)
                    .map_err(|_| Failure::new(2, "--as-json value must be valid JSON"))?
            } else {
                Value::String(raw_value)
            };
            let request = VarSetRequest {
                id: mutation_id(id)?,
                scope,
                key,
                value,
                expect_version,
                note,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: VarSetResponse =
                eliot_daemon::client_call_typed(&data_dir, "var.set", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "var.set response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "var.set response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        VarCommand::Get(VarGetArgs { key, scope }) => {
            let request = VarGetRequest { scope, key };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: VarGetResponse =
                eliot_daemon::client_call_typed(&data_dir, "var.get", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "var.get response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "var.get response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}

pub(crate) async fn run_who(
    WhoArgs { project, history }: WhoArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    run_who_command(&data_dir, json_output, project, history).await
}

pub(crate) async fn run_claim(
    ClaimArgs { command }: ClaimArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    run_claim_command(&data_dir, json_output, id, command).await
}

pub(crate) async fn run_who_command(
    data_dir: &Path,
    json_output: bool,
    project: Option<String>,
    history: bool,
) -> Result<u8, Failure> {
    let request = WhoRequest {
        project,
        history: Some(history),
    };
    request
        .validate()
        .map_err(|error| Failure::from_core(error, false))?;
    let response: WhoResponse = eliot_daemon::client_call_typed(data_dir, "who", &request)
        .await
        .map_err(|error| Failure::from_core(error, false))?;
    request
        .validate_response(&response)
        .map_err(|_| Failure::new(4, "who response is invalid"))?;
    let value =
        serde_json::to_value(response).map_err(|_| Failure::new(4, "who response is invalid"))?;
    print_value(&value, json_output);
    Ok(0)
}

pub(crate) async fn run_claim_command(
    data_dir: &Path,
    json_output: bool,
    id: Option<String>,
    command: ClaimCommand,
) -> Result<u8, Failure> {
    match command {
        ClaimCommand::Set(ClaimSetArgs {
            glob,
            agent,
            note,
            ttl_secs,
        }) => {
            let request = ClaimSetRequest {
                id: mutation_id(id)?,
                agent,
                glob,
                note,
                ttl_secs,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: ClaimSetResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.set", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "claim.set response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "claim.set response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        ClaimCommand::List(ClaimListArgs { project, agent }) => {
            let request = ClaimListRequest { project, agent };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, false))?;
            let response: ClaimListResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.list", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, false))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "claim.list response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "claim.list response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        ClaimCommand::Release(ClaimReleaseArgs { glob, agent }) => {
            let request = ClaimReleaseRequest {
                id: mutation_id(id)?,
                agent,
                glob,
            };
            request
                .validate()
                .map_err(|error| Failure::from_core(error, true))?;
            let response: ClaimReleaseResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.release", &request)
                    .await
                    .map_err(|error| Failure::from_core(error, true))?;
            request
                .validate_response(&response)
                .map_err(|_| Failure::new(4, "claim.release response is invalid"))?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "claim.release response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
    }
}

pub(crate) fn read_var_value_file(path: &Path) -> Result<String, Failure> {
    let file = File::open(path).map_err(|_| Failure::new(2, "value file could not be read"))?;
    let mut bounded = file.take(MAX_VAR_VALUE_BYTES as u64 + 1);
    let mut bytes = Vec::new();
    let byte_count = bounded
        .read_to_end(&mut bytes)
        .map_err(|_| Failure::new(2, "value file could not be read"))?;
    if byte_count > MAX_VAR_VALUE_BYTES {
        return Err(Failure::new(
            2,
            format!("value file exceeds the {MAX_VAR_VALUE_BYTES} byte limit"),
        ));
    }
    String::from_utf8(bytes).map_err(|_| Failure::new(2, "value file must be UTF-8"))
}
