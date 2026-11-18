//! Command and turn resolution, mail, variables, workers and claims.
use super::args::{
    ClaimArgs, ClaimCommand, ClaimListArgs, ClaimReleaseArgs, ClaimSetArgs, CmdArgs,
    CmdChildrenArgs, CmdCommand, CmdResolveArgs, CmdShowArgs, InboxArgs, MsgAckArgs, MsgArgs,
    MsgCommand, MsgSendArgs, MsgShowArgs, PageArgs, TurnArgs, TurnCommand, TurnResolveArgs,
    VarArgs, VarCommand, VarGetArgs, VarSetArgs, WhoArgs,
};
use super::support::{
    Failure, command_value, mutation_id, print_value, sleep_before_deadline, validate_limit,
    wait_deadline,
};
use eliot_core::Command as CoreCommand;
use eliot_core::api::discovery::{IdentityRequest, IdentityResponse};
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
};
use tokio::time::{Instant, timeout_at};

pub(crate) async fn run_cmd(
    CmdArgs { command }: CmdArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    match command {
        CmdCommand::Cancel(CmdShowArgs { command_id }) => {
            let request = eliot_core::api::CommandCancelRequest {
                id: mutation_id(id)?,
                command_id,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: eliot_core::api::CommandCancelResponse =
                eliot_daemon::client_call_typed(&data_dir, "command.cancel", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            if response.command_id != request.command_id
                || response.state != eliot_core::CommandState::Cancelled
            {
                return Err(Failure::new(4, "cancel response binding differs"));
            }
            print_value(
                &serde_json::to_value(response)
                    .map_err(|_| Failure::new(4, "invalid cancel response"))?,
                json_output,
            );
            Ok(0)
        }
        CmdCommand::Show(CmdShowArgs { command_id }) => {
            let params = CommandGetRequest::new(command_id);
            params.validate().map_err(Failure::for_read)?;
            let response: CoreCommand =
                eliot_daemon::client_call_typed(&data_dir, "command.get", &params)
                    .await
                    .map_err(Failure::for_read)?;
            params
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let result = command_value(response)?;
            print_value(&result, json_output);
            Ok(0)
        }
        CmdCommand::Children(CmdChildrenArgs {
            command_id,
            after,
            page: PageArgs { limit },
        }) => {
            validate_limit(limit)?;
            let request = CommandChildrenRequest {
                id: command_id,
                after,
                limit,
            };
            request.validate().map_err(Failure::for_read)?;
            let response: CommandChildrenResponse =
                eliot_daemon::client_call_typed(&data_dir, "command.children", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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
            request.validate().map_err(Failure::for_mutation)?;
            let response: CommandResolveResponse =
                eliot_daemon::client_call_typed(&data_dir, "command.resolve", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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
            request.validate().map_err(Failure::for_mutation)?;
            let response: TurnResolveResponse =
                eliot_daemon::client_call_typed(&data_dir, "turn.resolve", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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
            request.validate().map_err(Failure::for_mutation)?;
            let response: MailSendResponse =
                eliot_daemon::client_call_typed(&data_dir, "mail.send", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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
            request.validate().map_err(Failure::for_read)?;
            let response: MailDelivery =
                eliot_daemon::client_call_typed(&data_dir, "mail.read", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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
            request.validate().map_err(Failure::for_mutation)?;
            let response: MailAckResponse =
                eliot_daemon::client_call_typed(&data_dir, "mail.ack", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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
        thread,
        reply_to,
        wait,
    }: InboxArgs,
    data_dir: PathBuf,
    json_output: bool,
) -> Result<u8, Failure> {
    let mut request = MailInboxRequest {
        agent,
        limit,
        after,
        wait_ms: 0,
        thread,
        reply_to,
    };
    request.validate().map_err(Failure::for_read)?;
    // One checked absolute deadline bounds every wait RPC and sleep; a short
    // requested wait can no longer last until the full RPC timeout, and an
    // extreme value is rejected instead of panicking on `Instant` overflow.
    // The last validated page is kept so expiry prints it instead of
    // dropping the JSON result.
    let (deadline, waited) = inbox_deadline(wait)?;
    let mut last: Option<MailInboxResponse> = None;
    loop {
        // Each wait RPC long-polls the server for the time left under the
        // deadline (the server clamps past 30 seconds); the client loop
        // stays for the exit-5 accounting and the kept page.
        request.wait_ms = match deadline {
            Some(end) => end
                .checked_duration_since(Instant::now())
                .map(|left| left.as_millis().min(30_000) as u64)
                .unwrap_or(0),
            None => 0,
        };
        let response: MailInboxResponse = match deadline {
            Some(end) => {
                match timeout_at(
                    end,
                    eliot_daemon::client_call_typed(&data_dir, "mail.inbox", &request),
                )
                .await
                {
                    Ok(Ok(response)) => response,
                    // A completed inbox RPC failed before any wait expiry:
                    // the read-only path wrote nothing, so this is a plain
                    // read error (exit 1), not an uncertain command outcome.
                    // Exit 5 stays for deadline expiry below.
                    Ok(Err(error)) => return Err(Failure::for_read(error)),
                    Err(_) => {
                        if let Some(page) = last {
                            print_inbox_page(&page, json_output)?;
                            return Ok(5);
                        }
                        return Err(Failure::inbox_timeout());
                    }
                }
            }
            None => eliot_daemon::client_call_typed(&data_dir, "mail.inbox", &request)
                .await
                .map_err(Failure::for_read)?,
        };
        request
            .validate_response(&response)
            .map_err(Failure::invalid_response)?;
        let done = match deadline {
            None => true,
            Some(end) => !response.deliveries.is_empty() || Instant::now() >= end,
        };
        if done {
            print_inbox_page(&response, json_output)?;
            if waited && response.deliveries.is_empty() {
                return Ok(5);
            }
            return Ok(0);
        }
        last = Some(response);
        match deadline {
            Some(end) => {
                if sleep_before_deadline(end).await.is_err() {
                    // The deadline expired between validated reads: print
                    // the kept page, then report the timeout.
                    if let Some(page) = last {
                        print_inbox_page(&page, json_output)?;
                    }
                    return Ok(5);
                }
            }
            // Without a wait every iteration is already done; this is only
            // defensive against a future loop change.
            None => return Ok(0),
        }
    }
}

/// Print one validated inbox page; serialization failure stays exit 4.
fn print_inbox_page(response: &MailInboxResponse, json_output: bool) -> Result<(), Failure> {
    let value = serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "mail.inbox response is invalid"))?;
    print_value(&value, json_output);
    Ok(())
}

/// Deadline plan for `inbox --wait`: no `--wait` reads once and exits 0
/// even when the inbox is empty; `--wait 0` reads once and reports an empty
/// inbox as a timeout (exit 5); a positive wait bounds every RPC and sleep
/// by one checked absolute deadline. Extreme values are rejected instead of
/// panicking on `Instant` overflow.
pub(crate) fn inbox_deadline(wait: Option<u64>) -> Result<(Option<Instant>, bool), Failure> {
    match wait {
        None => Ok((None, false)),
        Some(0) => Ok((None, true)),
        Some(secs) => Ok((
            Some(wait_deadline(secs).map_err(|_| Failure::new(2, "--wait value is too large"))?),
            true,
        )),
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
            request.validate().map_err(Failure::for_mutation)?;
            let response: VarSetResponse =
                eliot_daemon::client_call_typed(&data_dir, "var.set", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "var.set response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        VarCommand::Get(VarGetArgs {
            key,
            scope,
            after_version,
            wait,
        }) => {
            if wait.unwrap_or(0) > 0 && after_version.is_none() {
                return Err(Failure::new(2, "--wait needs --after-version"));
            }
            let mut request = VarGetRequest {
                scope,
                key,
                after_version,
                wait_ms: 0,
            };
            request.validate().map_err(Failure::for_read)?;
            // Same deadline plan as `inbox --wait`: no wait reads once and
            // exits 0; a wait exits 5 when the version never moves. Each wait
            // RPC long-polls the server for the time left under the deadline.
            let (deadline, waited) = inbox_deadline(wait)?;
            let mut last: Option<VarGetResponse> = None;
            loop {
                request.wait_ms = match deadline {
                    Some(end) => end
                        .checked_duration_since(Instant::now())
                        .map(|left| left.as_millis().min(30_000) as u64)
                        .unwrap_or(0),
                    None => 0,
                };
                let response: VarGetResponse = match deadline {
                    Some(end) => {
                        match timeout_at(
                            end,
                            eliot_daemon::client_call_typed(&data_dir, "var.get", &request),
                        )
                        .await
                        {
                            Ok(Ok(response)) => response,
                            Ok(Err(error)) => return Err(Failure::for_read(error)),
                            Err(_) => {
                                if let Some(read) = last {
                                    let rendered = serde_json::to_value(read).map_err(|_| {
                                        Failure::new(4, "var.get response is invalid")
                                    })?;
                                    print_value(&rendered, json_output);
                                    return Ok(5);
                                }
                                return Err(Failure::new(
                                    5,
                                    "variable wait timed out before a read completed",
                                ));
                            }
                        }
                    }
                    None => eliot_daemon::client_call_typed(&data_dir, "var.get", &request)
                        .await
                        .map_err(Failure::for_read)?,
                };
                request
                    .validate_response(&response)
                    .map_err(Failure::invalid_response)?;
                let changed = after_version.is_none_or(|baseline| response.version != baseline);
                let done = match deadline {
                    None => true,
                    Some(end) => changed || Instant::now() >= end,
                };
                if done {
                    let value = serde_json::to_value(response)
                        .map_err(|_| Failure::new(4, "var.get response is invalid"))?;
                    print_value(&value, json_output);
                    if waited && !changed {
                        return Ok(5);
                    }
                    return Ok(0);
                }
                last = Some(response);
                if sleep_before_deadline(deadline.expect("wait has a deadline"))
                    .await
                    .is_err()
                {
                    if let Some(read) = last {
                        let rendered = serde_json::to_value(read)
                            .map_err(|_| Failure::new(4, "var.get response is invalid"))?;
                        print_value(&rendered, json_output);
                    }
                    return Ok(5);
                }
            }
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

pub(crate) async fn run_identity(data_dir: PathBuf, json_output: bool) -> Result<u8, Failure> {
    let request = IdentityRequest {};
    request.validate().map_err(Failure::for_read)?;
    let response: IdentityResponse =
        eliot_daemon::client_call_typed(&data_dir, "api.identity", &request)
            .await
            .map_err(Failure::for_read)?;
    let value = serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "api.identity response is invalid"))?;
    print_value(&value, json_output);
    Ok(0)
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
    request.validate().map_err(Failure::for_read)?;
    let response: WhoResponse = eliot_daemon::client_call_typed(data_dir, "who", &request)
        .await
        .map_err(Failure::for_read)?;
    request
        .validate_response(&response)
        .map_err(Failure::invalid_response)?;
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
            request.validate().map_err(Failure::for_mutation)?;
            let response: ClaimSetResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.set", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
            let value = serde_json::to_value(response)
                .map_err(|_| Failure::new(4, "claim.set response is invalid"))?;
            print_value(&value, json_output);
            Ok(0)
        }
        ClaimCommand::List(ClaimListArgs { project, agent }) => {
            let request = ClaimListRequest { project, agent };
            request.validate().map_err(Failure::for_read)?;
            let response: ClaimListResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.list", &request)
                    .await
                    .map_err(Failure::for_read)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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
            request.validate().map_err(Failure::for_mutation)?;
            let response: ClaimReleaseResponse =
                eliot_daemon::client_call_typed(data_dir, "claim.release", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            request
                .validate_response(&response)
                .map_err(Failure::invalid_response)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inbox_deadline_plans_zero_wait_and_rejects_overflow() {
        let (deadline, waited) = inbox_deadline(None).expect("no wait reads once");
        assert!(deadline.is_none());
        assert!(!waited);
        let (deadline, waited) = inbox_deadline(Some(0)).expect("zero wait reads once");
        assert!(deadline.is_none());
        assert!(waited, "an empty zero-wait read reports a timeout");
        let (deadline, waited) = inbox_deadline(Some(5)).expect("small wait has a deadline");
        assert!(deadline.unwrap() > Instant::now());
        assert!(waited);
        assert!(inbox_deadline(Some(u64::MAX)).is_err());
    }
}
