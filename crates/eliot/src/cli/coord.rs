//! Command and turn resolution, mail, variables, workers and claims.
use super::args::{
    ClaimArgs, ClaimCommand, ClaimListArgs, ClaimReleaseArgs, ClaimSetArgs, CmdArgs,
    CmdChildrenArgs, CmdCommand, CmdResolveArgs, CmdSettleStoppedArgs, CmdShowArgs, InboxArgs,
    MsgAckArgs, MsgArgs, MsgCommand, MsgSendArgs, MsgShowArgs, OptionalAgent, OwnerInboxArgs,
    OwnerInboxCommand, PageArgs, TurnArgs, TurnCommand, TurnResolveArgs, VarArgs, VarCommand,
    VarGetArgs, VarSetArgs, WhoArgs,
};
use super::reads::resolve_agent_target;
use super::support::{
    Failure, command_value, mutation_id, print_value, print_value_or_human, sleep_before_deadline,
    validate_limit, wait_deadline,
};
use eliot_core::Command as CoreCommand;
use eliot_core::api::discovery::{IdentityRequest, IdentityResponse};
use eliot_core::api::{
    ClaimListRequest, ClaimListResponse, ClaimReleaseRequest, ClaimReleaseResponse,
    ClaimSetRequest, ClaimSetResponse, CommandChildrenRequest, CommandChildrenResponse,
    CommandGetRequest, CommandResolution, CommandResolveRequest, CommandResolveResponse,
    MAX_BULK_OWNER_DECISIONS, MAX_VAR_VALUE_BYTES, MailAckRequest, MailAckResponse, MailDelivery,
    MailInboxRequest, MailInboxResponse, MailMode, MailReadRequest, MailSendRequest,
    MailSendResponse, OwnerBulkDecideRequest, OwnerBulkDecideResponse, OwnerDecideRequest,
    OwnerDecideResponse, OwnerInboxItemRef, OwnerInboxKind, OwnerInboxRequest, OwnerInboxResponse,
    SettleStoppedRequest, SettleStoppedResponse, TurnResolveRequest, TurnResolveResponse,
    TurnResolveStatus, VarGetRequest, VarGetResponse, VarSetRequest, VarSetResponse, WhoRequest,
    WhoResponse,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashSet},
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
        CmdCommand::SettleStopped(CmdSettleStoppedArgs { apply, after }) => {
            let request = SettleStoppedRequest {
                id: mutation_id(id)?,
                after,
                review: apply,
            };
            request.validate().map_err(Failure::for_mutation)?;
            let response: SettleStoppedResponse =
                eliot_daemon::client_call_typed(&data_dir, "command.settle_stopped", &request)
                    .await
                    .map_err(Failure::for_mutation)?;
            if response.id != request.id
                || response.applied != request.review.is_some()
                || request
                    .review
                    .as_ref()
                    .is_some_and(|review| review != &response.review)
            {
                return Err(Failure::new(4, "stop settlement response binding differs"));
            }
            let value = serde_json::to_value(&response)
                .map_err(|_| Failure::new(4, "stop settlement response is invalid"))?;
            print_value_or_human(&value, json_output, || {
                println!(
                    "Historical settlement {}: {} proven stop(s), {} exact request-reply receipt(s), {} stop(s) still awaiting proof.",
                    if response.applied {
                        "applied"
                    } else {
                        "preview"
                    },
                    response.stops.len(),
                    response.request_replies.len(),
                    response.awaiting_proof.len()
                );
                println!("Review digest: {}", response.review);
                for stop in &response.stops {
                    println!(
                        "  Agent {} generation {} · command {} · mark stopped: {}{}",
                        stop.agent,
                        stop.generation,
                        stop.command_id,
                        stop.mark_stopped,
                        stop.interrupted_turn
                            .as_deref()
                            .map(|turn| format!(" · interrupted turn {turn}"))
                            .unwrap_or_default()
                    );
                }
                for command in &response.awaiting_proof {
                    println!("  Awaiting process or reader proof: {command}");
                }
                for reply in &response.request_replies {
                    println!(
                        "  Agent {} generation {} · {} command {} · request {} · turn {} · native receipt event {}",
                        reply.agent,
                        reply.generation,
                        reply.verb,
                        reply.command_id,
                        reply.request_id,
                        reply.turn_id,
                        reply.receipt_seq
                    );
                }
                if let Some(next) = response.next.as_deref() {
                    println!("Continue with --after {next}");
                }
            });
            Ok(0)
        }
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
            let result = command_value(response.clone())?;
            print_value_or_human(&result, json_output, || {
                println!(
                    "Command {} · {} · {}",
                    response.id,
                    response.verb,
                    format!("{:?}", response.state).to_lowercase()
                );
                println!("Method: {}", response.method);
                if let Some(target) = response.target.as_deref() {
                    println!("Target: {target}");
                }
                if let Some(generation) = response.generation {
                    println!("Generation: {generation}");
                }
                if let Some(turn) = response.turn_id.as_deref() {
                    println!("Turn: {turn}");
                }
                if let Some(request) = response.request_id.as_deref() {
                    println!("Request: {request}");
                }
                if let Some(outcome) = &response.outcome {
                    println!("Outcome: {outcome}");
                }
            });
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
            status,
            legacy_as,
            reason,
        }) => {
            let resolution = match (status.as_deref(), legacy_as.as_deref()) {
                (Some(_), Some(_)) => {
                    return Err(Failure::new(2, "--status and --as cannot be used together"));
                }
                (Some("completed"), None) | (None, Some("applied")) => CommandResolution::Applied,
                (Some("failed"), None) | (None, Some("failed")) => CommandResolution::Failed,
                (None, None) => {
                    return Err(Failure::new(2, "--status or legacy --as is required"));
                }
                _ => {
                    return Err(Failure::new(
                        2,
                        "--status must be completed or failed; --as must be applied or failed",
                    ));
                }
            };
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
        MsgCommand::Inbox(args) => run_mail_inbox(args, data_dir, json_output).await,
        MsgCommand::Send(MsgSendArgs {
            to,
            text,
            thread,
            reply_to,
            mode,
            no_wake,
        }) => {
            let mode: MailMode = serde_json::from_value(Value::String(mode))
                .map_err(|_| Failure::new(2, "mail mode must be queue, send, steer or inject"))?;
            let request = MailSendRequest {
                id: mutation_id(id)?,
                to,
                mode,
                wake: !no_wake,
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
        MsgCommand::Show(MsgShowArgs {
            delivery,
            agent:
                OptionalAgent {
                    agent: agent_positional,
                    legacy_agent,
                },
        }) => {
            let request = MailReadRequest {
                agent: resolve_agent_target(agent_positional, legacy_agent)?,
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
        MsgCommand::Ack(MsgAckArgs {
            delivery,
            agent:
                OptionalAgent {
                    agent: agent_positional,
                    legacy_agent,
                },
        }) => {
            let agent = resolve_agent_target(agent_positional, legacy_agent)?;
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
    OwnerInboxArgs { page, command }: OwnerInboxArgs,
    data_dir: PathBuf,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    if let Some(command) = command {
        match command {
            OwnerInboxCommand::Decide { item, choice, args } => {
                return decide_inbox(&data_dir, &item, choice, args, json_output, id).await;
            }
            OwnerInboxCommand::Snooze { item } => {
                return decide_inbox(
                    &data_dir,
                    &item,
                    "snooze".to_owned(),
                    Vec::new(),
                    json_output,
                    id,
                )
                .await;
            }
            OwnerInboxCommand::Apply { kind, items, yes } => {
                let InboxArgs {
                    agent,
                    limit,
                    after,
                    thread,
                    reply_to,
                    wait,
                } = page;
                if agent.is_some() || thread.is_some() || reply_to.is_some() || wait.is_some() {
                    return Err(Failure::new(
                        2,
                        "inbox apply accepts Owner inbox paging only",
                    ));
                }
                return bulk_decide_inbox(
                    &data_dir,
                    kind,
                    items,
                    yes,
                    OwnerInboxRequest { after, limit },
                    json_output,
                    id,
                )
                .await;
            }
        }
    }
    let InboxArgs {
        agent,
        limit,
        after,
        thread,
        reply_to,
        wait,
    } = page;
    // Agent's historical unflagged invocation remains its mailbox. Owner
    // calls default to the unified decision queue; mail-specific selectors
    // retain their original meaning for Owner mail access.
    if agent.is_none()
        && thread.is_none()
        && reply_to.is_none()
        && wait.is_none()
        && std::env::var_os("ELIOT_AGENT_ID").is_none()
    {
        let request = OwnerInboxRequest { after, limit };
        let response: OwnerInboxResponse =
            eliot_daemon::client_call_typed(&data_dir, "owner.inbox", &request)
                .await
                .map_err(Failure::for_read)?;
        return print_owner_inbox(&response, json_output);
    }
    run_mail_inbox(
        InboxArgs {
            agent,
            limit,
            after,
            thread,
            reply_to,
            wait,
        },
        data_dir,
        json_output,
    )
    .await
}

async fn decide_inbox(
    data_dir: &Path,
    item: &str,
    choice: String,
    args: Vec<String>,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let item = parse_owner_inbox_item_ref(item)?;
    let mut fields = serde_json::Map::new();
    for arg in args {
        let (key, value) = arg
            .split_once('=')
            .filter(|(key, _)| !key.is_empty())
            .ok_or_else(|| Failure::new(2, "--arg must use KEY=VALUE"))?;
        let value = if key == "answers" {
            serde_json::from_str(value)
                .map_err(|_| Failure::new(2, "--arg answers must be valid JSON"))?
        } else {
            Value::String(value.to_owned())
        };
        if fields.insert(key.to_owned(), value).is_some() {
            return Err(Failure::new(2, "duplicate --arg key"));
        }
    }
    let request = OwnerDecideRequest {
        id: mutation_id(id)?,
        item,
        choice,
        args: (!fields.is_empty()).then_some(Value::Object(fields)),
    };
    request.validate().map_err(Failure::for_mutation)?;
    let response: OwnerDecideResponse =
        eliot_daemon::client_call_typed(data_dir, "owner.decide", &request)
            .await
            .map_err(Failure::for_mutation)?;
    let value = serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "owner.decide response is invalid"))?;
    print_value(&value, json_output);
    Ok(0)
}

async fn bulk_decide_inbox(
    data_dir: &Path,
    kind: Option<String>,
    items: Vec<String>,
    yes: bool,
    page: OwnerInboxRequest,
    json_output: bool,
    id: Option<String>,
) -> Result<u8, Failure> {
    let OwnerInboxRequest { after, limit } = page;
    let refs = match kind {
        Some(kind) if items.is_empty() => {
            if !(1..=MAX_BULK_OWNER_DECISIONS).contains(&limit.unwrap_or(100)) {
                return Err(Failure::new(2, "--limit must be between 1 and 100"));
            }
            let kind = parse_owner_inbox_kind(&kind)
                .ok_or_else(|| Failure::new(2, "unknown Owner inbox item kind"))?;
            let mut refs = BTreeMap::new();
            let mut cursor = after;
            let mut seen = HashSet::new();
            loop {
                let request = OwnerInboxRequest {
                    after: cursor.clone(),
                    limit: Some(limit.unwrap_or(100)),
                };
                request.effective_limit().map_err(Failure::for_read)?;
                let response: OwnerInboxResponse =
                    eliot_daemon::client_call_typed(data_dir, "owner.inbox", &request)
                        .await
                        .map_err(Failure::for_read)?;
                for item in response.items.into_iter().filter(|item| item.kind == kind) {
                    if !yes
                        && item.recommended.as_ref().is_some_and(|action| {
                            action.method == "command.submit"
                                && action.args["verb"].as_str() == Some("stop")
                        })
                    {
                        return Err(Failure::new(
                            2,
                            "kind-wide apply includes Stop recommendations; pass --yes or use reviewed --item refs",
                        ));
                    }
                    refs.insert(
                        (item.kind, item.subject.clone()),
                        OwnerInboxItemRef {
                            kind: item.kind,
                            subject: item.subject,
                            revision: item.revision,
                        },
                    );
                }
                if refs.len() > MAX_BULK_OWNER_DECISIONS {
                    return Err(Failure::new(
                        2,
                        "this kind has more than 100 current items; narrow with --after or pass reviewed --item refs",
                    ));
                }
                match response.next {
                    Some(next) => {
                        if !seen.insert(next.clone()) {
                            return Err(Failure::new(
                                4,
                                "owner.inbox repeated a page cursor while collecting current items",
                            ));
                        }
                        cursor = Some(next);
                    }
                    None => break,
                }
            }
            refs.into_values().collect()
        }
        None if !items.is_empty() => {
            if after.is_some() || limit.is_some() {
                return Err(Failure::new(
                    2,
                    "--after and --limit apply only with --kind",
                ));
            }
            items
                .iter()
                .map(|item| parse_owner_inbox_item_ref(item))
                .collect::<Result<Vec<_>, _>>()?
        }
        Some(_) => {
            return Err(Failure::new(2, "choose --kind or repeat --item, not both"));
        }
        None => {
            return Err(Failure::new(
                2,
                "inbox apply requires --kind or at least one --item",
            ));
        }
    };
    if refs.is_empty() {
        return print_owner_bulk_decisions(&OwnerBulkDecideResponse::default(), json_output);
    }
    let request = OwnerBulkDecideRequest {
        id: mutation_id(id)?,
        items: refs,
    };
    request.validate().map_err(Failure::for_mutation)?;
    let response: OwnerBulkDecideResponse =
        eliot_daemon::client_call_typed(data_dir, "owner.decide.bulk", &request)
            .await
            .map_err(Failure::for_mutation)?;
    print_owner_bulk_decisions(&response, json_output)
}

fn parse_owner_inbox_item_ref(item: &str) -> Result<OwnerInboxItemRef, Failure> {
    let invalid_item =
        || Failure::new(2, "expected the inbox item reference KIND/SUBJECT@REVISION");
    let (cursor, revision) = item.rsplit_once('@').ok_or_else(invalid_item)?;
    let (kind, subject) = cursor.split_once('/').ok_or_else(invalid_item)?;
    let kind = parse_owner_inbox_kind(kind).ok_or_else(invalid_item)?;
    Ok(OwnerInboxItemRef {
        kind,
        subject: subject.to_owned(),
        revision: revision.to_owned(),
    })
}

fn parse_owner_inbox_kind(value: &str) -> Option<OwnerInboxKind> {
    OwnerInboxKind::ALL
        .into_iter()
        .find(|candidate| candidate.as_str() == value)
}

fn print_owner_bulk_decisions(
    response: &OwnerBulkDecideResponse,
    json_output: bool,
) -> Result<u8, Failure> {
    let value = serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "owner.decide.bulk response is invalid"))?;
    if json_output {
        print_value(&value, true);
    } else {
        println!(
            "Bulk owner decisions: {} applied, {} skipped, {} failed.",
            response.applied.len(),
            response.skipped.len(),
            response.failed.len()
        );
        for item in &response.applied {
            println!(
                "  Applied {}/{}@{} · {} · id={}",
                item.item.kind.as_str(),
                item.item.subject,
                item.item.revision,
                item.outcome,
                item.mutation_id
            );
        }
        for item in &response.skipped {
            println!(
                "  Skipped {}/{}@{} · {} · id={}",
                item.item.kind.as_str(),
                item.item.subject,
                item.item.revision,
                item.reason,
                item.mutation_id
            );
        }
        for item in &response.failed {
            let certainty = if item.uncertain { "unknown" } else { "failed" };
            println!(
                "  {certainty} {}/{}@{} · {}: {} · id={}",
                item.item.kind.as_str(),
                item.item.subject,
                item.item.revision,
                item.code,
                item.message,
                item.mutation_id
            );
        }
    }
    if response.failed.iter().any(|item| item.uncertain) {
        Ok(4)
    } else if !response.failed.is_empty() {
        Ok(1)
    } else {
        Ok(0)
    }
}

async fn run_mail_inbox(
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

fn print_owner_inbox(response: &OwnerInboxResponse, json_output: bool) -> Result<u8, Failure> {
    let value = serde_json::to_value(response)
        .map_err(|_| Failure::new(4, "owner.inbox response is invalid"))?;
    if json_output {
        print_value(&value, true);
    } else {
        if response.items.is_empty() {
            println!("Owner inbox is clear.");
        }
        for item in &response.items {
            let seconds = item.age_ms / 1000;
            let age = if seconds >= 86_400 {
                format!("{}d {}h", seconds / 86_400, seconds / 3_600 % 24)
            } else if seconds >= 3_600 {
                format!("{}h {}m", seconds / 3_600, seconds / 60 % 60)
            } else if seconds >= 60 {
                format!("{}m", seconds / 60)
            } else {
                format!("{seconds}s")
            };
            println!("{}@{} · {age}", item.cursor(), item.revision);
            println!("  {}", item.reason);
            if let Some(instructions) = &item.instructions {
                println!("  Run:");
                for line in instructions.lines() {
                    println!("    {line}");
                }
            }
            if let Some(action) = &item.recommended {
                println!("  Recommended: {} {}", action.method, action.args);
            } else if item.kind == OwnerInboxKind::Stuck {
                println!("  No safe recommended action.");
            }
            for action in &item.choices {
                if item.recommended.as_ref() == Some(action) {
                    continue;
                }
                println!(
                    "  Choice: {} → {} {}",
                    action.choice, action.method, action.args
                );
            }
        }
        if let Some(next) = &response.next {
            println!("Next: {next}");
        }
    }
    Ok(0)
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
            agent:
                OptionalAgent {
                    agent: agent_positional,
                    legacy_agent,
                },
            note,
            ttl_secs,
        }) => {
            let agent = resolve_agent_target(agent_positional, legacy_agent)?;
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
        ClaimCommand::List(ClaimListArgs {
            project,
            agent:
                OptionalAgent {
                    agent: agent_positional,
                    legacy_agent,
                },
        }) => {
            let agent = resolve_agent_target(agent_positional, legacy_agent)?;
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
        ClaimCommand::Release(ClaimReleaseArgs {
            glob,
            agent:
                OptionalAgent {
                    agent: agent_positional,
                    legacy_agent,
                },
        }) => {
            let agent = resolve_agent_target(agent_positional, legacy_agent)?;
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
