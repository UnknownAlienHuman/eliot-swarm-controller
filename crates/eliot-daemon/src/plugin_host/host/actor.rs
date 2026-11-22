//! Agent actor lifecycle and stop handling.
//!
//! Owns the live run_agent_inner message loop: start sequencing, steer and
//! snapshot fan-out, queued-stop admission and force settlement. Shared
//! ordering and generation ownership stay with super; this module only
//! hosts the loop so host.rs no longer concentrates it alongside
//! authenticated source-request intake.

use super::super::connection::{MAX_PENDING_CALLS, PLUGIN_TRANSPORT_WHERE};
use super::{
    AgentActorConfig, COMPACT_RPC_DEADLINE, MAX_PENDING_SNAPSHOT_READS, PendingFailure,
    PendingSnapshotFuture, PendingSteerFuture, PluginClient, PluginCommandCompletion,
    PluginSnapshotCompletion, RPC_GRACE, RuntimeMessage, STOP_GRACE, agent_command_params,
    cleanup_actor, cleanup_startup, complete_plugin_command, custody_label, deadline,
    fence_command_routing, fence_runtime_generation, initialize_agent, is_failed_start_result,
    mark_lost_state, mark_lost_state_with_exit, prepare_queued_stop_call, record_plugin_error,
    redact_native_error, resolve_resume_config, resolve_send_config, retire_failed_start,
    settle_drained_source_exit, settle_owned_source_exit, shutdown_and_wait_child, snapshot_future,
    start_deadline, validate_applied, validate_start_result, wait_child, wait_for_stop_exit_proof,
    wait_stop_child,
};
use crate::monitor::Monitoring;
use crate::platform::OwnedChild;
use eliot_core::{Agent, AgentStartParams, Error, FailureOrigin, Result};
use eliot_plugin::{StderrCapture, StderrView};
use eliot_store::AgentLossEvidence;
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_json::{Value, json};
use std::sync::{Arc, Weak};
#[cfg(windows)]
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, timeout_at};

/// Linux spontaneous root-exit observation only: report the plugin process
/// exit without reaping while its birth identity still anchors group
/// signals. The `select!` caller fences routing once this wins, then runs
/// the bounded group drain and the reap to completion outside the select so
/// a queued message cannot cancel the drain and restart the grace period.
/// Never signals a bare reused PGID: every group signal goes through the
/// birth-checked leader identity.
#[cfg(target_os = "linux")]
async fn spontaneous_root_wait(
    child: &mut OwnedChild,
    _monitoring: &Monitoring,
    _agent: &Agent,
) -> Result<()> {
    child.wait_exit_unreaped().await
}

/// Non-Linux spontaneous root-exit wait: plain reap; group proof stays with
/// the platform observer (Windows Job) or is unavailable (other Unix).
#[cfg(not(target_os = "linux"))]
async fn spontaneous_root_wait(
    child: &mut OwnedChild,
    _: &Monitoring,
    _: &Agent,
) -> (Result<std::process::ExitStatus>, bool) {
    (child.wait().await, false)
}

/// Arbitrate a finished reader against an observed root exit: an exit the OS
/// already reported wins over EOF or task failure, so the reader arm never
/// masks spontaneous-exit evidence with `stream_eof_without_session_terminal`.
/// Returns the source-gap reason and whether the shared root-exit settlement
/// path must run instead of the reader-loss path.
pub(super) fn arbitrate_reader_close(root_exited: bool, reader_ok: bool) -> (&'static str, bool) {
    if root_exited {
        ("root_exit_without_session_terminal", true)
    } else if reader_ok {
        ("stream_eof_without_session_terminal", false)
    } else {
        ("stream_task_failed", false)
    }
}

/// Shared spontaneous root-exit settlement: custody drain, then source and
/// loss settlement with the `root_exited` cause and its exit evidence. Linux
/// keeps #702's drain-before-reap order: the leader stays unreaped while the
/// owned group drains, and only the bounded reap clears the birth-checked
/// identity. Both the spontaneous-exit arm and the reader-EOF arbitration run
/// this path, so EOF can never mask an observed root exit. This runs only
/// from terminal arms where no stop was planned, matching the old
/// `!planned_exit` guard at the call sites.
#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
pub(super) async fn settle_observed_root_exit(
    state: &Weak<super::HostState>,
    key: &super::AgentKey,
    child: &mut OwnedChild,
    monitoring: &Monitoring,
    agent: &Agent,
    event_task: &mut tokio::task::JoinHandle<()>,
    completed_reader: Option<std::result::Result<(), tokio::task::JoinError>>,
    stderr: &StderrView,
) {
    // Fence routing before the bounded cleanup so no racing dispatch enters.
    if let Some(state) = state.upgrade() {
        fence_command_routing(&state, key);
    }
    // Linux: observe without reaping while the birth identity still anchors
    // group signals, then run the bounded group drain and the reap to
    // completion outside the select so a queued message cannot cancel the
    // drain and restart the grace period. Never signals a bare reused PGID:
    // every group signal goes through the birth-checked leader identity.
    #[cfg(target_os = "linux")]
    let (status, _group_drained) = {
        let observed = child.try_wait_exit_unreaped();
        let _ = monitoring.sample_generation(agent).await;
        // A failed observation proves nothing about the group: still
        // terminate and drain before reaping so surviving descendants get a
        // signal, then report unknown via drained=false below.
        let group_drained = if observed.is_ok_and(|exited| exited) {
            super::linux_drain_owned_group(child, STOP_GRACE).await
        } else {
            let _ = child.terminate();
            let _ = super::linux_drain_owned_group(child, STOP_GRACE).await;
            false
        };
        if group_drained {
            let _ = monitoring.sample_generation(agent).await;
        }
        // Reap only after the bounded cleanup: the numeric PGID cannot be
        // reused between the final observation and this reap because the
        // exited leader is still unreaped. The reap itself is bounded: a
        // failed observation may leave the child live (/proc loss also
        // defeats birth-checked termination), and an unbounded wait would
        // hang before source settlement. A timeout keeps status Err so the
        // unknown-custody settlement below reports it.
        (child.wait_bounded(STOP_GRACE).await, group_drained)
    };
    #[cfg(not(target_os = "linux"))]
    let (status, _group_drained) = (child.wait().await, false);
    // The plugin process exit is process evidence: its numeric code (and
    // NTSTATUS for crash statuses) with the redacted stderr tail is recorded
    // on `agent.lost` below. A killed process has no code; the tail still
    // says what it last reported.
    let exit =
        eliot_core::ProcessExit::from_code(status.as_ref().ok().and_then(|status| status.code()));
    let stderr_tail = stderr.snapshot().text;
    if status.is_ok() {
        let _ = monitoring.sample_generation(agent).await;
    }
    // Keep source authorization intact throughout the bounded drain. A reader
    // arm that already consumed the JoinHandle result passes it here so this
    // path never polls the completed handle a second time (Tokio panics on
    // poll-after-completion). Only a still-pending reader is awaited.
    let (gap_reason, drained) = match completed_reader {
        Some(Ok(())) => ("root_exit_without_session_terminal", true),
        Some(Err(_)) => ("root_exit_source_failed", true),
        None => match timeout_at(Instant::now() + STOP_GRACE, &mut *event_task).await {
            Ok(Ok(())) => ("root_exit_without_session_terminal", true),
            Ok(Err(_)) => ("root_exit_source_failed", true),
            Err(_) => {
                event_task.abort();
                let drained = timeout_at(Instant::now() + RPC_GRACE, event_task)
                    .await
                    .is_ok();
                ("root_exit_drain_timeout", drained)
            }
        },
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
    // Unix custody remains degraded: leader exit proves native control loss,
    // not that every descendant has been contained.
    #[cfg(all(unix, not(target_os = "linux")))]
    let control_gone = status.is_ok();
    #[cfg(target_os = "linux")]
    let control_gone = status.is_ok() && _group_drained;
    if control_gone {
        let _ = monitoring.sample_generation(agent).await;
    }
    let source_settled = settle_drained_source_exit(state, key, drained, control_gone, gap_reason)
        .await
        .is_ok();
    if source_settled && control_gone {
        // A crash report bound to this spawn attributes the exit to Swarm.
        let crash = super::plugin_crash_marker(&stderr_tail, stderr.crash_line_prefix());
        if let Some(marker) = crash.as_ref() {
            super::record_plugin_crash(state, marker);
        }
        mark_lost_state_with_exit(
            state,
            key,
            eliot_core::PLUGIN_ROOT_EXITED_REASON,
            AgentLossEvidence {
                exit: Some(exit),
                stderr_tail: Some(stderr_tail),
                origin: crash.as_ref().map(|_| FailureOrigin::Swarm),
                origin_evidence: crash.map(|marker| marker.summary()),
            },
        )
        .await;
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
pub(super) async fn run_agent_inner(
    actor: AgentActorConfig,
    mut child: Option<OwnedChild>,
    client: PluginClient,
    mut event_task: tokio::task::JoinHandle<()>,
    mut messages: mpsc::Receiver<RuntimeMessage>,
    started: oneshot::Sender<Result<Value>>,
    stderr_capture: &mut Option<StderrCapture>,
    pending_failure: &mut Option<PendingFailure>,
) {
    // Drop order is load-bearing: every handle stays owned by this frame and
    // the phase helpers only borrow it. Locals drop in reverse declaration
    // order, so a surviving child drops after the command receiver, event
    // task, monitor and client, exactly as before the split. The child is
    // taken only where the inline code moved or dropped it.
    if !run_agent_startup(
        &actor,
        &client,
        &mut child,
        &mut event_task,
        started,
        stderr_capture,
        pending_failure,
    )
    .await
    {
        return;
    }
    let AgentActorConfig {
        state,
        monitoring,
        key,
        agent,
        plugin_credential,
        source_credentials,
        stderr,
        ..
    } = actor;
    let mut pending_steers: FuturesUnordered<PendingSteerFuture> = FuturesUnordered::new();
    let mut pending_snapshots: FuturesUnordered<PendingSnapshotFuture> = FuturesUnordered::new();
    let planned_exit = drive_agent_dispatch(AgentDispatch {
        state: &state,
        monitoring: &monitoring,
        key: &key,
        agent: &agent,
        source_credentials: &source_credentials,
        stderr: &stderr,
        client: &client,
        child: &mut child,
        event_task: &mut event_task,
        messages: &mut messages,
        pending_steers: &mut pending_steers,
        pending_snapshots: &mut pending_snapshots,
        pending_failure,
    })
    .await;
    settle_agent_actor(
        &state,
        &key,
        &plugin_credential,
        planned_exit,
        &mut pending_steers,
        pending_snapshots,
        &client,
        &agent,
        &stderr,
        &source_credentials,
        pending_failure,
    )
    .await;
}

/// Borrowed dispatch-loop handles. Every handle stays owned by
/// `run_agent_inner` so the drop order there never moves; the loop only
/// borrows, and takes `child` exactly where the inline code dropped it.
struct AgentDispatch<'a> {
    state: &'a Weak<super::HostState>,
    monitoring: &'a super::Monitoring,
    key: &'a super::AgentKey,
    agent: &'a super::Agent,
    source_credentials: &'a Arc<[String]>,
    stderr: &'a super::StderrView,
    client: &'a PluginClient,
    child: &'a mut Option<OwnedChild>,
    event_task: &'a mut tokio::task::JoinHandle<()>,
    messages: &'a mut mpsc::Receiver<RuntimeMessage>,
    pending_steers: &'a mut FuturesUnordered<PendingSteerFuture>,
    pending_snapshots: &'a mut FuturesUnordered<PendingSnapshotFuture>,
    pending_failure: &'a mut Option<PendingFailure>,
}

/// Start sequencing: initialize the native source, resolve resume overlays,
/// and run the `agent.start` handshake. Every handle is borrowed: the child
/// is taken only where the inline code moved it into cleanup, so a cancelled
/// startup drops the handles in the actor frame's original order. Success
/// leaves the live child and event task in place; every failure path answers
/// `started` and cleans up exactly as before, returning `false`.
async fn run_agent_startup(
    actor: &AgentActorConfig,
    client: &PluginClient,
    child: &mut Option<OwnedChild>,
    event_task: &mut tokio::task::JoinHandle<()>,
    started: oneshot::Sender<Result<Value>>,
    stderr_capture: &mut Option<StderrCapture>,
    pending_failure: &mut Option<PendingFailure>,
) -> bool {
    let initialize_started = Instant::now();
    let source_plan = match initialize_agent(actor, client).await {
        Ok(source_plan) => source_plan,
        Err(error) => {
            let proven_no_dispatch = error.code == "native_not_dispatched";
            let origin = startup_loss_origin(&error);
            cleanup_startup(
                actor,
                child
                    .take()
                    .expect("actor owns the starting child until cleanup"),
                &mut *event_task,
                stderr_capture,
                pending_failure,
                proven_no_dispatch,
                origin,
            )
            .await;
            let _ = started.send(Err(error));
            return false;
        }
    };
    let initialized_at = Instant::now();
    // A resume that inherited a `configure` overlay carries its effective
    // values as the native realignment target: the retained session
    // already runs them, so the plugin must prove them instead of
    // reverting to the frozen snapshot.
    let resume_config = match resolve_resume_config(actor).await {
        Ok(resume_config) => resume_config,
        Err(error) => {
            let origin = startup_loss_origin(&error);
            cleanup_startup(
                actor,
                child
                    .take()
                    .expect("actor owns the starting child until cleanup"),
                &mut *event_task,
                stderr_capture,
                pending_failure,
                true,
                origin,
            )
            .await;
            let _ = started.send(Err(error));
            return false;
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
        owner_rules: actor.owner_rules.clone(),
        profile: Some({
            let mut profile = actor.agent.profile_snapshot.clone();
            if let Some(values) = profile.as_object_mut() {
                values.remove(crate::command_guard::SNAPSHOT_FIELD);
                values.remove("pass_agent_identity_to_shell");
            }
            profile
        }),
        workspace: actor.agent.workspace.clone(),
        target_environment: Some(actor.target_environment.clone()),
        mcp_environment: actor.mcp_environment.clone(),
        pass_agent_identity_to_shell: Some(actor.pass_agent_identity_to_shell),
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
                    client,
                    child
                        .as_mut()
                        .expect("actor owns the starting child until retire"),
                    &mut *event_task,
                    actor,
                    pending_failure,
                )
                .await;
                let _ = started.send(Ok(value));
                return false;
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
            let origin = startup_loss_origin(&error);
            let _ = started.send(Err(error));
            if !uncertain || native_endpoint_exited {
                cleanup_startup(
                    actor,
                    child
                        .take()
                        .expect("actor owns the starting child until cleanup"),
                    &mut *event_task,
                    stderr_capture,
                    pending_failure,
                    proven_no_dispatch,
                    origin,
                )
                .await;
                return false;
            }
        }
    }
    true
}

fn startup_loss_origin(error: &Error) -> Option<FailureOrigin> {
    // The daemon's RPC classification describes the broken pipe. The loss
    // event describes the owned plugin process and keeps its own evidence.
    if error.failure_where.as_deref() == Some(PLUGIN_TRANSPORT_WHERE) {
        None
    } else {
        error.origin
    }
}

/// Typed process/source/control message handling: supervise the plugin child,
/// source stream, and queued commands, stops, snapshots, and daemon force
/// until the loop exits, then report whether the exit was planned. Every
/// handle is borrowed from the actor frame; `child` is taken only where the
/// inline code dropped it.
async fn drive_agent_dispatch(dispatch: AgentDispatch<'_>) -> bool {
    let AgentDispatch {
        state,
        monitoring,
        key,
        agent,
        source_credentials,
        stderr,
        client,
        child: child_slot,
        event_task,
        messages,
        pending_steers,
        pending_snapshots,
        pending_failure,
    } = dispatch;
    let mut planned_exit = false;
    // while_let_loop does not apply: the child borrow must end before
    // take() on break paths, which a while-let condition cannot express.
    #[allow(
        clippy::while_let_loop,
        reason = "A while-let borrow would overlap the required take on loop exit."
    )]
    loop {
        let Some(child) = child_slot.as_mut() else {
            // take() below only runs on break paths; the loop never re-enters
            // without a child.
            break;
        };
        tokio::select! {
            _ = spontaneous_root_wait(child, monitoring, agent) => {
                // The exit observation alone won the select. The reader is
                // still pending, so settlement awaits it below.
                settle_observed_root_exit(state, key, child, monitoring, agent, event_task, None, stderr).await;
                break;
            }
            event_result = &mut *event_task => {
                if let Some(state) = state.upgrade() {
                    fence_command_routing(&state, key);
                }
                // Arbitration: a root exit the OS already reported wins over
                // EOF or task failure, so this arm never masks
                // spontaneous-exit evidence with a stream gap reason. The
                // select already consumed the JoinHandle result; passing it
                // avoids polling the completed handle again inside settlement.
                let (gap_reason, is_root_exit) =
                    arbitrate_reader_close(child.try_observe_root_exit(), event_result.is_ok());
                if is_root_exit {
                    settle_observed_root_exit(state, key, child, monitoring, agent, event_task, Some(event_result), stderr).await;
                    break;
                }
                let control_gone = child.terminate().is_ok()
                    && wait_child(&mut *child, STOP_GRACE, monitoring, agent).await;
                let source_settled = settle_drained_source_exit(
                    state, key, true, control_gone, gap_reason,
                ).await.is_ok();
                if source_settled && control_gone {
                    mark_lost_state(state, key, gap_reason).await;
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
                            source_credentials,
                        );
                    }
                    let _ = completion.reply.send(completion.result);
                }
            }
            completion = pending_steers.next(), if !pending_steers.is_empty() => {
                if let Some(completion) = completion {
                    complete_plugin_command(
                        completion,
                        agent,
                        stderr,
                        source_credentials,
                        pending_failure,
                        client.reader_closed(),
                    );
                }
            }
            message = messages.recv() => {
                let Some(message) = message else {
                    let exited = child.terminate().is_ok()
                        && wait_child(&mut *child, STOP_GRACE, monitoring, agent).await;
                    if settle_owned_source_exit(
                        state, key, Some(&mut *event_task), exited,
                        "plugin_owner_closed_without_session_terminal",
                    ).await.is_ok() {
                        mark_lost_state(state, key, "plugin_owner_closed_without_session_terminal").await;
                    }
                    break;
                };
                match message {
                    RuntimeMessage::Command {
                        command,
                        context,
                        reply,
                    } => {
                        let config = match resolve_send_config(state, &command).await {
                            Ok(config) => config,
                            Err(error) => {
                                complete_plugin_command(
                                    PluginCommandCompletion {
                                        command,
                                        reply,
                                        result: Err(error),
                                    },
                                    agent,
                                    stderr,
                                    source_credentials,
                                    pending_failure,
                                    client.reader_closed(),
                                );
                                continue;
                            }
                        };
                        match agent_command_params(agent, &command, context, config.as_ref()) {
                            Err(error) => complete_plugin_command(
                                PluginCommandCompletion {
                                    command,
                                    reply,
                                    result: Err(error),
                                },
                                agent,
                                stderr,
                                source_credentials,
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
                                        agent,
                                        stderr,
                                        source_credentials,
                                        pending_failure,
                                        client.reader_closed(),
                                    );
                                } else {
                                    match client.begin_call("agent.command", params, deadline()).await {
                                        Ok(pending_call) => {
                                            let response_deadline = deadline();
                                            pending_steers.push(Box::pin(async move {
                                                // Dispatch is already confirmed. Expiry is unknown,
                                                // and PendingCall releases the correlation slot; this
                                                // command must never be sent again.
                                                let result = pending_call.finish(response_deadline).await;
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
                                            agent,
                                            stderr,
                                            source_credentials,
                                            pending_failure,
                                            client.reader_closed(),
                                        ),
                                    }
                                }
                            }
                            Ok(params) => {
                                let command_deadline = if command.verb == "compact" { Instant::now() + COMPACT_RPC_DEADLINE } else { deadline() };
                                let result = client.call("agent.command", params, command_deadline).await;
                                complete_plugin_command(
                                    PluginCommandCompletion {
                                        command,
                                        reply,
                                        result,
                                    },
                                    agent,
                                    stderr,
                                    source_credentials,
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
                                Arc::clone(source_credentials),
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
                        let guarantee = child.guarantee();
                        let (native, graceful, exited) = if force {
                            // Windows force goes directly to the named Job. The
                            // actor provides the equivalent weaker Unix custody.
                            if Instant::now() >= deadlines.queued_until {
                                let _ = reply.send(Err(Error::new(
                                    "native_not_dispatched",
                                    "stop queue deadline elapsed before custody termination",
                                )));
                                continue;
                            }
                            let exited = child.terminate().is_ok()
                                && wait_stop_child(child, monitoring, agent, deadlines.custody_until).await;
                            (Ok(None), false, exited)
                        } else {
                            let params = json!({
                                "agent_id": agent.id,
                                "generation": agent.generation,
                                "command_id": command.id,
                            });
                            let stop_call = match prepare_queued_stop_call(client, params, deadlines) {
                                Ok(call) => call,
                                Err(error) => {
                                    let _ = reply.send(Err(error));
                                    continue;
                                }
                            };
                            let native = stop_call
                                .await
                                .map_err(|error| redact_native_error(error, source_credentials))
                                .and_then(validate_applied)
                                .map(Some);
                            match native {
                                Ok(_) => {
                                    let (graceful, exited) = shutdown_and_wait_child(
                                        client, child, monitoring, agent, deadlines,
                                    ).await;
                                    (native, graceful, exited)
                                }
                                Err(ref error) if error.code == "unknown" => {
                                    // The effect is uncertain. Retire owned custody
                                    // without calling the plugin again.
                                    let exited = child.terminate().is_ok()
                                        && wait_stop_child(child, monitoring, agent, deadlines.custody_until).await;
                                    (native, false, exited)
                                }
                                Err(error) => {
                                    let _ = reply.send(Err(error));
                                    continue;
                                }
                            }
                        };
                        if !exited {
                            drop(child_slot.take());
                        }
                        let source_exit = settle_owned_source_exit(
                            state, key, Some(&mut *event_task), exited,
                            if force {
                                "force_stop_exit_without_session_terminal"
                            } else {
                                "stop_exit_without_session_terminal"
                            },
                        ).await;
                        if let Some(state) = state.upgrade() {
                            fence_runtime_generation(&state, key);
                        }
                        planned_exit = true;
                        let source_error = source_exit.err();
                        let proof = if exited && source_error.is_none() {
                            Some(
                                wait_for_stop_exit_proof(state, agent, deadlines.source_until)
                                    .await,
                            )
                        } else {
                            None
                        };
                        let (native_result, native_stop_error_code) = match native {
                            Ok(native_result) => (native_result, None),
                            Err(error) => (None, Some(error.code)),
                        };
                        let native_session_end_observed = native_result.is_some();
                        let result = match (exited, source_error, proof) {
                            (false, _, _) => Err(Error::new(
                                "unknown",
                                format!(
                                    "stop process custody exit is unproven ({})",
                                    custody_label(guarantee),
                                ),
                            )),
                            (true, Some(error), _) => Err(error),
                            (true, None, Some(Err(error))) => Err(error),
                            (true, None, Some(Ok(proof)))
                                if Instant::now() <= deadlines.source_until =>
                            {
                                Ok(json!({
                                    "state": "applied",
                                    "custody": custody_label(guarantee),
                                    "graceful": graceful,
                                    "native": native_result,
                                    "native_session_end_observed": native_session_end_observed,
                                    "native_stop_error_code": native_stop_error_code,
                                    "native_stop_reason": if native_session_end_observed {
                                        Value::Null
                                    } else {
                                        json!("native_session_end_unobserved")
                                    },
                                    "process_custody_exit_proven": proof.process_custody_exited,
                                    "native_source_drain_proven": proof.native_source_drained,
                                }))
                            }
                            _ => Err(Error::new(
                                "unknown",
                                "stop source settlement exceeded its absolute deadline",
                            )),
                        };
                        if result.is_err() {
                            // The actor is exiting and control is fenced even if cleanup
                            // could not prove exit. Loss does not attest custody closure.
                            let reason = if exited {
                                "stop_exit_without_session_terminal"
                            } else {
                                "stop_process_exit_unproven"
                            };
                            mark_lost_state(state, key, reason).await;
                        }
                        let _ = reply.send(result);
                        break;
                    }
                    RuntimeMessage::DaemonForce { restart, reply } => {
                        let guarantee = child.guarantee();
                        let terminated = child.terminate().is_ok();
                        let exited = wait_child(&mut *child, STOP_GRACE, monitoring, agent).await;
                        if restart && terminated && exited {
                            // Preserve the idle projection and its custody reservation.
                            // Boot recovery proves the prior generation's exit before
                            // it decides whether this session can continue.
                            planned_exit = true;
                            let _ = reply.send(Ok(()));
                            break;
                        }
                        if !terminated || !exited {
                            drop(child_slot.take());
                            let _ = settle_owned_source_exit(
                                state, key, Some(&mut *event_task), false,
                                "daemon_force_exit_without_session_terminal",
                            ).await;
                            if let Some(state) = state.upgrade() {
                                fence_runtime_generation(&state, key);
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
                            state, key, Some(&mut *event_task), true,
                            "daemon_force_exit_without_session_terminal",
                        ).await;
                        if let Err(error) = source_exit {
                            if let Some(state) = state.upgrade() {
                                fence_runtime_generation(&state, key);
                            }
                            planned_exit = true;
                            let _ = reply.send(Err(error));
                            break;
                        }
                        mark_lost_state(state, key, "daemon_force_exit_without_session_terminal").await;
                        planned_exit = true;
                        let _ = reply.send(Ok(()));
                        break;
                    }
                }
            }
        }
    }
    planned_exit
}

/// Final family settlement: drop pending snapshot reads, clean up the actor,
/// then drain retained steer responses within bounded waits. Every handle is
/// borrowed from the actor frame, so the drop order there never moves; the
/// snapshot set moves in only to be dropped first, exactly as before.
#[allow(
    clippy::too_many_arguments,
    reason = "The operation's distinct inputs remain explicit at its established call boundary."
)]
async fn settle_agent_actor(
    state: &Weak<super::HostState>,
    key: &super::AgentKey,
    plugin_credential: &str,
    planned_exit: bool,
    pending_steers: &mut FuturesUnordered<PendingSteerFuture>,
    pending_snapshots: FuturesUnordered<PendingSnapshotFuture>,
    client: &PluginClient,
    agent: &super::Agent,
    stderr: &super::StderrView,
    source_credentials: &Arc<[String]>,
    pending_failure: &mut Option<PendingFailure>,
) {
    // These read-only futures are actor-owned. Dropping them cancels every
    // pending snapshot RPC before actor cleanup; they cannot outlive the child.
    drop(pending_snapshots);
    cleanup_actor(state, key, plugin_credential, planned_exit).await;
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
                agent,
                stderr,
                source_credentials,
                pending_failure,
                client.reader_closed(),
            );
        }
    }
}
