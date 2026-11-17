//! Agent actor lifecycle and stop handling.
//!
//! Owns the live run_agent_inner message loop: start sequencing, steer and
//! snapshot fan-out, queued-stop admission and force settlement. Shared
//! ordering and generation ownership stay with super; this module only
//! hosts the loop so host.rs no longer concentrates it alongside
//! authenticated source-request intake.

use super::super::connection::MAX_PENDING_CALLS;
use super::{
    AgentActorConfig, MAX_PENDING_SNAPSHOT_READS, PendingFailure, PendingSnapshotFuture,
    PendingSteerFuture, PluginClient, PluginCommandCompletion, PluginSnapshotCompletion, RPC_GRACE,
    RuntimeMessage, STOP_GRACE, agent_command_params, cleanup_actor, cleanup_startup,
    complete_plugin_command, custody_label, deadline, fence_command_routing,
    fence_runtime_generation, has_committed_session_ended, initialize_agent,
    is_failed_start_result, mark_lost_state, mark_lost_state_with_exit, prepare_queued_stop_call,
    record_plugin_error, redact_native_error, resolve_resume_config, resolve_send_config,
    retire_failed_start, settle_drained_source_exit, settle_owned_source_exit,
    shutdown_and_wait_child, snapshot_future, start_deadline, validate_applied,
    validate_start_result, wait_child, wait_stop_child,
};
use crate::platform::OwnedChild;
use eliot_core::{AgentStartParams, Error, Result};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_json::{Value, json};
use std::sync::Arc;
#[cfg(windows)]
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout_at};

pub(super) async fn run_agent_inner(
    actor: AgentActorConfig,
    mut child: OwnedChild,
    client: PluginClient,
    mut event_task: tokio::task::JoinHandle<()>,
    mut messages: mpsc::Receiver<RuntimeMessage>,
    started: oneshot::Sender<Result<Value>>,
    pending_failure: &mut Option<PendingFailure>,
) {
    let initialize_started = Instant::now();
    let source_plan = match initialize_agent(&actor, &client).await {
        Ok(source_plan) => source_plan,
        Err(error) => {
            let proven_no_dispatch = error.code == "native_not_dispatched";
            let _ = started.send(Err(error));
            cleanup_startup(
                &actor,
                child,
                &mut event_task,
                pending_failure,
                proven_no_dispatch,
            )
            .await;
            return;
        }
    };
    let initialized_at = Instant::now();
    // A resume that inherited a `configure` overlay carries its effective
    // values as the native realignment target: the retained session
    // already runs them, so the plugin must prove them instead of
    // reverting to the frozen snapshot.
    let resume_config = match resolve_resume_config(&actor).await {
        Ok(resume_config) => resume_config,
        Err(error) => {
            let _ = started.send(Err(error));
            cleanup_startup(&actor, child, &mut event_task, pending_failure, true).await;
            return;
        }
    };
    let (model_override, effort_override, config_revision) = match resume_config {
        Some((model, effort, revision)) => (model, effort, Some(revision)),
        None => (None, None, None),
    };
    let start_params = AgentStartParams {
        agent_id: actor.agent.id.clone(),
        generation: actor.agent.generation,
        command_id: actor.start_command.id.clone(),
        profile: Some(actor.agent.profile_snapshot.clone()),
        workspace: actor.agent.workspace.clone(),
        target_environment: Some(actor.target_environment.clone()),
        note: None,
        source_plan: Some(source_plan),
        source_state: actor.source_state.clone(),
        resume_from: actor.resume_from.clone(),
        initial: None,
        model_override,
        effort_override,
        config_revision,
    };
    let native_started = Instant::now();
    let start = match serde_json::to_value(start_params) {
        // PluginClient validates the exact completed RPC frame, including its
        // allocated request ID and newline, before any bytes reach the writer.
        Ok(params) => client.call("agent.start", params, start_deadline()).await,
        Err(_) => Err(Error::new(
            "internal",
            "agent.start params could not be encoded",
        )),
    };
    crate::logging::record_debug(
        &eliot_core::SecretPolicy::new(actor.source_credentials.iter()),
        "agent.handshake_timing",
        &[
            ("agent_id", &actor.agent.id),
            ("command_id", &actor.start_command.id),
        ],
        &[
            (
                "initialize_ms",
                (initialized_at - initialize_started)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            ),
            (
                "start_prepare_ms",
                (native_started - initialized_at)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            ),
            (
                "native_start_ms",
                native_started
                    .elapsed()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            ),
        ],
    );
    match start {
        Ok(value) if validate_start_result(&value, &actor.agent, &actor.start_command).is_ok() => {
            if is_failed_start_result(&value) {
                // The plugin's authoritative proven-no-effect verdict: never
                // enter the live actor loop. Retire and drain this exact
                // plugin actor and process tree first so Store sees the
                // generation's processes exit and the owned-exit proof path
                // releases the capacity reservation, then deliver the
                // authoritative Failed outcome. No re-dispatch.
                retire_failed_start(
                    &client,
                    &mut child,
                    &mut event_task,
                    &actor,
                    pending_failure,
                )
                .await;
                let _ = started.send(Ok(value));
                return;
            }
            let _ = started.send(Ok(value));
        }
        Ok(_) => {
            let error = Error::new("unknown", "plugin start did not prove session readiness");
            let _ = started.send(Err(error));
        }
        Err(error) => {
            let error = redact_native_error(error, &actor.source_credentials);
            let native_endpoint_exited =
                error.code == eliot_plugin::native_stdio::NATIVE_ENDPOINT_EXITED;
            let error = if native_endpoint_exited {
                // Endpoint death settles Agent lifecycle, not the dispatched
                // native effect: the caller still receives Unknown.
                error.with_code("unknown")
            } else {
                error
            };
            let uncertain = error.code == "unknown";
            let proven_no_dispatch = error.code == "native_not_dispatched";
            let _ = started.send(Err(error));
            if !uncertain || native_endpoint_exited {
                cleanup_startup(
                    &actor,
                    child,
                    &mut event_task,
                    pending_failure,
                    proven_no_dispatch,
                )
                .await;
                return;
            }
        }
    }

    let AgentActorConfig {
        state,
        monitoring,
        key,
        agent,
        start_command,
        plugin_credential,
        source_credentials,
        stderr,
        ..
    } = actor;
    let mut last_command_id = start_command.id.clone();
    let mut planned_exit = false;
    let mut pending_steers: FuturesUnordered<PendingSteerFuture> = FuturesUnordered::new();
    let mut pending_snapshots: FuturesUnordered<PendingSnapshotFuture> = FuturesUnordered::new();
    loop {
        tokio::select! {
            status = child.wait() => {
                // The plugin process exit is process evidence: its numeric
                // code (and NTSTATUS for crash statuses) with the redacted
                // stderr tail is recorded on `agent.lost` below. A killed
                // process has no code; the tail still says what it last
                // reported.
                let exit = eliot_core::ProcessExit::from_code(
                    status.as_ref().ok().and_then(|status| status.code()),
                );
                let stderr_tail = stderr.snapshot().text;
                if let Some(state) = state.upgrade() {
                    fence_command_routing(&state, &key);
                }
                if status.is_ok() {
                    let _ = monitoring.sample_generation(&agent).await;
                }
                // Keep source authorization intact throughout the bounded drain.
                let (gap_reason, drained) = match timeout_at(
                    Instant::now() + STOP_GRACE,
                    &mut event_task,
                ).await {
                    Ok(Ok(())) => ("root_exit_without_session_terminal", true),
                    Ok(Err(_)) => ("root_exit_source_failed", true),
                    Err(_) => {
                        event_task.abort();
                        let drained = timeout_at(
                            Instant::now() + RPC_GRACE,
                            &mut event_task,
                        ).await.is_ok();
                        ("root_exit_drain_timeout", drained)
                    }
                };
                #[cfg(windows)]
                let control_gone = match child.wait_family_empty(Duration::ZERO).await {
                    Ok(true) => true,
                    Ok(false) => {
                        child.terminate().is_ok()
                            && matches!(child.wait_family_empty(STOP_GRACE).await, Ok(true))
                    }
                    Err(_) => false,
                };
                // Unix custody remains degraded: leader exit proves native
                // control loss, not that every descendant has been contained.
                #[cfg(not(windows))]
                let control_gone = status.is_ok();
                if control_gone {
                    let _ = monitoring.sample_generation(&agent).await;
                }
                let source_settled = settle_drained_source_exit(
                    &state, &key, drained, control_gone, gap_reason,
                ).await.is_ok();
                if source_settled && control_gone && !planned_exit {
                    mark_lost_state_with_exit(
                        &state,
                        &key,
                        Some(exit),
                        Some(stderr_tail),
                    )
                    .await;
                }
                break;
            }
            event_result = &mut event_task => {
                if let Some(state) = state.upgrade() {
                    fence_command_routing(&state, &key);
                }
                let gap_reason = if event_result.is_ok() {
                    "stream_eof_without_session_terminal"
                } else {
                    "stream_task_failed"
                };
                let control_gone = child.terminate().is_ok()
                    && wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                let source_settled = settle_drained_source_exit(
                    &state, &key, true, control_gone, gap_reason,
                ).await.is_ok();
                if source_settled && control_gone {
                    mark_lost_state(&state, &key).await;
                }
                planned_exit = true;
                break;
            }
            completion = pending_snapshots.next(), if !pending_snapshots.is_empty() => {
                if let Some(completion) = completion {
                    if let Err(error) = &completion.result {
                        record_plugin_error(
                            error,
                            &completion.agent_id,
                            completion.generation,
                            &completion.command_id,
                            &source_credentials,
                        );
                    }
                    let _ = completion.reply.send(completion.result);
                }
            }
            completion = pending_steers.next(), if !pending_steers.is_empty() => {
                if let Some(completion) = completion {
                    complete_plugin_command(
                        completion,
                        &agent,
                        &stderr,
                        &source_credentials,
                        pending_failure,
                        client.reader_closed(),
                    );
                }
            }
            message = messages.recv() => {
                let Some(message) = message else {
                    let exited = child.terminate().is_ok()
                        && wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                    if settle_owned_source_exit(
                        &state, &key, Some(&mut event_task), exited,
                        "plugin_owner_closed_without_session_terminal",
                    ).await.is_ok() {
                        mark_lost_state(&state, &key).await;
                    }
                    break;
                };
                match message {
                    RuntimeMessage::Command {
                        command,
                        dispatch_seq,
                        reply,
                    } => {
                        last_command_id = command.id.clone();
                        let config = match resolve_send_config(&state, &command).await {
                            Ok(config) => config,
                            Err(error) => {
                                complete_plugin_command(
                                    PluginCommandCompletion {
                                        command,
                                        reply,
                                        result: Err(error),
                                    },
                                    &agent,
                                    &stderr,
                                    &source_credentials,
                                    pending_failure,
                                    client.reader_closed(),
                                );
                                continue;
                            }
                        };
                        match agent_command_params(&agent, &command, dispatch_seq, config.as_ref()) {
                            Err(error) => complete_plugin_command(
                                PluginCommandCompletion {
                                    command,
                                    reply,
                                    result: Err(error),
                                },
                                &agent,
                                &stderr,
                                &source_credentials,
                                pending_failure,
                                client.reader_closed(),
                            ),
                            Ok(params) if command.verb == "steer" => {
                                let pending_steer_limit =
                                    MAX_PENDING_CALLS.saturating_sub(pending_snapshots.len() + 1);
                                if pending_steers.len() >= pending_steer_limit {
                                    complete_plugin_command(
                                        PluginCommandCompletion {
                                            command,
                                            reply,
                                            result: Err(Error::new(
                                                "native_not_dispatched",
                                                "plugin has reached its bounded in-flight request limit",
                                            )),
                                        },
                                        &agent,
                                        &stderr,
                                        &source_credentials,
                                        pending_failure,
                                        client.reader_closed(),
                                    );
                                } else {
                                    match client.begin_call("agent.command", params, deadline()).await {
                                        Ok(pending_call) => {
                                            pending_steers.push(Box::pin(async move {
                                                let result = pending_call.finish(None).await;
                                                PluginCommandCompletion {
                                                    command,
                                                    reply,
                                                    result,
                                                }
                                            }));
                                        }
                                        Err(error) => complete_plugin_command(
                                            PluginCommandCompletion {
                                                command,
                                                reply,
                                                result: Err(error),
                                            },
                                            &agent,
                                            &stderr,
                                            &source_credentials,
                                            pending_failure,
                                            client.reader_closed(),
                                        ),
                                    }
                                }
                            }
                            Ok(params) => {
                                let result = client.call("agent.command", params, deadline()).await;
                                complete_plugin_command(
                                    PluginCommandCompletion {
                                        command,
                                        reply,
                                        result,
                                    },
                                    &agent,
                                    &stderr,
                                    &source_credentials,
                                    pending_failure,
                                    client.reader_closed(),
                                );
                            }
                        }
                    }
                    RuntimeMessage::Snapshot { agent_id, generation, command_id, turn_id, reply } => {
                        if pending_snapshots.len() >= MAX_PENDING_SNAPSHOT_READS {
                            let _ = reply.send(Err(Error::new(
                                "busy",
                                "Agent status snapshot is already in flight",
                            )));
                        } else {
                            pending_snapshots.push(snapshot_future(
                                client.clone(),
                                turn_id,
                                stderr.clone(),
                                Arc::clone(&source_credentials),
                                PluginSnapshotCompletion {
                                    agent_id,
                                    generation,
                                    command_id,
                                    reply,
                                    // Identity pre-built by the caller; the future
                                    // fills the diagnosed result before resolving.
                                    result: Err(Error::new("internal", "snapshot result unset")),
                                },
                            ));
                        }
                    }
                    RuntimeMessage::Stop { command, force, deadlines, reply } => {
                        let params = json!({
                            "agent_id": agent.id,
                            "generation": agent.generation,
                            "command_id": command.id,
                            "force": force,
                        });
                        let stop_call = match prepare_queued_stop_call(&client, params, deadlines) {
                            Ok(call) => call,
                            Err(error) => {
                                let _ = reply.send(Err(error));
                                continue;
                            }
                        };
                        let snapshot_command_id = last_command_id.clone();
                        last_command_id = command.id.clone();
                        let cooperative = stop_call
                            .await
                            .map_err(|error| redact_native_error(error, &source_credentials))
                            .and_then(validate_applied);
                        match cooperative {
                            Ok(result) => {
                                let guarantee = child.guarantee();
                                let (graceful, exited) = shutdown_and_wait_child(
                                    &client,
                                    &mut child,
                                    &monitoring,
                                    &agent,
                                    deadlines,
                                )
                                .await;
                                if !exited {
                                    drop(child);
                                    let _ = settle_owned_source_exit(
                                        &state, &key, Some(&mut event_task), false,
                                        "stop_exit_without_session_terminal",
                                    ).await;
                                    if let Some(state) = state.upgrade() {
                                        fence_runtime_generation(&state, &key);
                                    }
                                    planned_exit = true;
                                    let _ = reply.send(Err(Error::new(
                                        "unknown",
                                        "stop could not confirm owned process-family exit",
                                    )));
                                    break;
                                }
                                let source_exit = settle_owned_source_exit(
                                    &state, &key, Some(&mut event_task), true,
                                    "stop_exit_without_session_terminal",
                                ).await;
                                if let Some(state) = state.upgrade() {
                                    fence_runtime_generation(&state, &key);
                                }
                                planned_exit = true;
                                if let Err(error) = source_exit {
                                    let _ = reply.send(Err(error));
                                    break;
                                }
                                if Instant::now() > deadlines.source_until {
                                    let _ = reply.send(Err(Error::new("unknown", "stop source settlement exceeded its absolute deadline")));
                                    break;
                                }
                                let _ = reply.send(Ok(json!({
                                    "state": "applied",
                                    "custody": custody_label(guarantee),
                                    "graceful": graceful,
                                    "native": result,
                                })));
                                break;
                            }
                            Err(_) if force => {
                                let snapshot_command = snapshot_command_id.clone();
                                let snapshot = if Instant::now() < deadlines.rpc_until { client.call(
                                    "agent.snapshot",
                                    json!({
                                        "agent_id": agent.id,
                                        "generation": agent.generation,
                                        "command_id": snapshot_command,
                                    }),
                                    deadlines.rpc_deadline(STOP_GRACE),
                                ).await.ok() } else { None };
                                let guarantee = child.guarantee();
                                let terminated = child.terminate().is_ok();
                                let exited = wait_stop_child(&mut child, &monitoring, &agent, deadlines.custody_until).await;
                                if !terminated || !exited {
                                    drop(child);
                                    let _ = settle_owned_source_exit(
                                        &state, &key, Some(&mut event_task), false,
                                        "force_stop_exit_without_session_terminal",
                                    ).await;
                                    if let Some(state) = state.upgrade() {
                                        fence_runtime_generation(&state, &key);
                                    }
                                    planned_exit = true;
                                    let _ = reply.send(Err(Error::new(
                                        "unknown",
                                        format!(
                                            "force-stop could not confirm owned custody termination ({})",
                                            custody_label(guarantee)
                                        ),
                                    )));
                                    break;
                                }
                                let source_exit = settle_owned_source_exit(
                                    &state, &key, Some(&mut event_task), true,
                                    "force_stop_exit_without_session_terminal",
                                ).await;
                                if let Some(state) = state.upgrade() {
                                    fence_runtime_generation(&state, &key);
                                }
                                planned_exit = true;
                                if let Err(error) = source_exit {
                                    let _ = reply.send(Err(error));
                                    break;
                                }
                                if Instant::now() > deadlines.source_until {
                                    let _ = reply.send(Err(Error::new("unknown", "stop source settlement exceeded its absolute deadline")));
                                    break;
                                }
                                let mut response = json!({
                                    "state": "applied",
                                    "custody": custody_label(guarantee),
                                    "terminated": true,
                                    "cooperative": "unknown",
                                });
                                if let Some(snapshot) = snapshot {
                                    response["snapshot"] = snapshot;
                                }
                                let _ = reply.send(Ok(response));
                                break;
                            }
                            Err(error) => {
                                if error.code == "unknown" && Instant::now() < deadlines.rpc_until {
                                    let _ = client.call(
                                        "agent.snapshot",
                                        json!({
                                            "agent_id": agent.id,
                                            "generation": agent.generation,
                                            "command_id": snapshot_command_id,
                                        }),
                                        deadlines.rpc_deadline(STOP_GRACE),
                                    ).await;
                                }
                                if error.code == "unknown"
                                    && has_committed_session_ended(&state, &key, deadlines.custody_until).await
                                {
                                    // Durable logical End is committed but the stop RPC
                                    // outcome is unknown. Run the Applied physical
                                    // sequence so the owning plugin/Job and descendants
                                    // cannot leak, then return the ORIGINAL unknown:
                                    // ack uncertainty is separate from physical closure
                                    // and no Applied is invented.
                                    let (_, exited) = shutdown_and_wait_child(
                                        &client,
                                        &mut child,
                                        &monitoring,
                                        &agent,
                                        deadlines,
                                    )
                                    .await;
                                    if !exited {
                                        drop(child);
                                        let _ = settle_owned_source_exit(
                                            &state, &key, Some(&mut event_task), false,
                                            "unknown_stop_exit_with_session_terminal",
                                        ).await;
                                        if let Some(state) = state.upgrade() {
                                            fence_runtime_generation(&state, &key);
                                        }
                                        planned_exit = true;
                                        let _ = reply.send(Err(error));
                                        break;
                                    }
                                    let _ = settle_owned_source_exit(
                                        &state, &key, Some(&mut event_task), true,
                                        "unknown_stop_exit_with_session_terminal",
                                    ).await;
                                    if let Some(state) = state.upgrade() {
                                        fence_runtime_generation(&state, &key);
                                    }
                                    planned_exit = true;
                                    let _ = reply.send(Err(error));
                                    break;
                                }
                                let _ = reply.send(Err(error));
                            }
                        }
                    }
                    RuntimeMessage::DaemonForce { reply } => {
                        let guarantee = child.guarantee();
                        let terminated = child.terminate().is_ok();
                        let exited = wait_child(&mut child, STOP_GRACE, &monitoring, &agent).await;
                        if !terminated || !exited {
                            drop(child);
                            let _ = settle_owned_source_exit(
                                &state, &key, Some(&mut event_task), false,
                                "daemon_force_exit_without_session_terminal",
                            ).await;
                            if let Some(state) = state.upgrade() {
                                fence_runtime_generation(&state, &key);
                            }
                            planned_exit = true;
                            let _ = reply.send(Err(Error::new(
                                "unknown",
                                format!(
                                    "daemon force could not confirm owned custody termination ({})",
                                    custody_label(guarantee)
                                ),
                            )));
                            break;
                        }
                        let source_exit = settle_owned_source_exit(
                            &state, &key, Some(&mut event_task), true,
                            "daemon_force_exit_without_session_terminal",
                        ).await;
                        if let Err(error) = source_exit {
                            if let Some(state) = state.upgrade() {
                                fence_runtime_generation(&state, &key);
                            }
                            planned_exit = true;
                            let _ = reply.send(Err(error));
                            break;
                        }
                        mark_lost_state(&state, &key).await;
                        planned_exit = true;
                        let _ = reply.send(Ok(()));
                        break;
                    }
                }
            }
        }
    }
    // These read-only futures are actor-owned. Dropping them cancels every
    // pending snapshot RPC before actor cleanup; they cannot outlive the child.
    drop(pending_snapshots);
    cleanup_actor(&state, &key, &plugin_credential, planned_exit).await;
    if !pending_steers.is_empty() {
        // Stop/force/source custody above owns teardown. Only then wait for the
        // transport to close and settle the retained correlated responses.
        let _ = timeout_at(Instant::now() + STOP_GRACE, client.wait_closed()).await;
        // A response may already have been routed even if the close signal
        // misses its bounded wait. Drain ready completions either way.
        let drain_until = Instant::now() + RPC_GRACE;
        while !pending_steers.is_empty() {
            let completion = match timeout_at(drain_until, pending_steers.next()).await {
                Ok(Some(completion)) => completion,
                Ok(None) | Err(_) => break,
            };
            complete_plugin_command(
                completion,
                &agent,
                &stderr,
                &source_credentials,
                pending_failure,
                client.reader_closed(),
            );
        }
    }
}
