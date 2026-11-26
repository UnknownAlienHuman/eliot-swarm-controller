use super::operator::stand_owner_call_typed;
use super::support::{Failure, WAIT_POLL_INTERVAL};
use eliot_core::api::{
    CommandGetRequest, MAX_SWARM_WAIT_MS, SwarmWaitRequest, SwarmWaitResponse, WaitEntryResult,
    WaitTurnRef,
};
use eliot_core::{Command, CommandState, ContextWaitRequest};
use serde::{Serialize, de::DeserializeOwned};
use std::{path::Path, time::Duration};
use tokio::time::{Instant, sleep_until, timeout_at};

#[derive(Clone, Copy)]
pub(super) enum WaitClient<'a> {
    Caller(&'a Path),
    StandOwner(&'a Path),
}

impl WaitClient<'_> {
    async fn call<P: Serialize, R: DeserializeOwned>(
        self,
        method: &str,
        params: &P,
    ) -> eliot_core::Result<R> {
        match self {
            Self::Caller(path) => eliot_daemon::client_call_typed(path, method, params).await,
            Self::StandOwner(path) => stand_owner_call_typed(path, method, params).await,
        }
    }

    pub(super) async fn wait_set(
        self,
        request: SwarmWaitRequest,
        deadline: Instant,
    ) -> Result<Option<SwarmWaitResponse>, Failure> {
        let mut bindings = vec![None; request.commands.len()];
        self.wait_set_bound(request, &mut bindings, deadline).await
    }

    async fn wait_set_bound(
        self,
        request: SwarmWaitRequest,
        bindings: &mut [Option<WaitTurnRef>],
        deadline: Instant,
    ) -> Result<Option<SwarmWaitResponse>, Failure> {
        request.validate().map_err(Failure::for_read)?;
        wait_slices(
            deadline,
            |wait_ms| {
                let mut request = request.clone();
                request.wait_ms = wait_ms;
                async move { self.call("swarm.wait", &request).await }
            },
            |reply: &SwarmWaitResponse| {
                request
                    .validate_response(reply)
                    .map_err(Failure::readback)?;
                for (binding, entry) in bindings.iter_mut().zip(&reply.results) {
                    if let Some(observed) = turn_binding(entry) {
                        if binding.get_or_insert_with(|| observed.clone()) != &observed {
                            return Err(binding_mismatch());
                        }
                    } else if binding.is_some()
                        && !matches!(
                            entry.status.as_str(),
                            "unknown" | "not_found" | "not_visible"
                        )
                    {
                        return Err(binding_mismatch());
                    }
                }
                for (turn, entry) in request.turns.iter().zip(&reply.results[bindings.len()..]) {
                    if turn_binding(entry).is_some_and(|observed| observed != *turn) {
                        return Err(binding_mismatch());
                    }
                }
                Ok(reply.completed || reply.mail_wake || request.mailbox)
            },
        )
        .await
    }

    pub(super) async fn command(self, id: &str, deadline: Instant) -> Result<Command, Failure> {
        let query = CommandGetRequest::new(id);
        query.validate().map_err(Failure::readback)?;
        let subject = format!("command {id}");
        let request = SwarmWaitRequest::new(vec![id.to_owned()], Vec::new(), "all", 0);
        let mut bindings = [None];
        let reply = self
            .wait_set_bound(request, &mut bindings, deadline)
            .await?
            .ok_or_else(|| unsettled(&subject, "no validated snapshot"))?;
        let entry = &reply.results[0];
        if !reply.completed {
            return Err(unsettled(&subject, &entry.status));
        }
        let [mut binding] = bindings;
        // The turn already woke swarm.wait. Only the exact command row can
        // now show whether settlement caught up.
        self.command_readback(&query, deadline, |command| {
            if let Some(bound) = &binding
                && (command.target.as_deref() != Some(bound.agent.as_str())
                    || command.generation != Some(bound.generation)
                    || (command.state != CommandState::Unknown
                        && command.turn_id.as_deref() != Some(bound.turn_id.as_str())))
            {
                return Err(binding_mismatch());
            }
            if command.state.is_terminal() || command.state == CommandState::Unknown {
                return Ok(Some(command));
            }
            if binding.is_none()
                && let (Some(agent), Some(generation), Some(turn)) =
                    (&command.target, command.generation, &command.turn_id)
            {
                binding = Some(WaitTurnRef::new(agent, generation, turn));
            }
            Ok(None)
        })
        .await
    }

    pub(super) async fn command_readback<T>(
        self,
        query: &CommandGetRequest,
        deadline: Instant,
        mut settled: impl FnMut(Command) -> Result<Option<T>, Failure>,
    ) -> Result<T, Failure> {
        let subject = format!("command {}", query.id);
        let mut observation = "no exact command read";
        loop {
            let command = wait_read(
                self.call("command.get", query),
                deadline,
                &subject,
                observation,
            )
            .await?;
            query
                .validate_response(&command)
                .map_err(Failure::readback)?;
            if let Some(result) = settled(command)? {
                return Ok(result);
            }
            observation = "exact command settlement still open";
            pause(deadline).await;
        }
    }

    pub(super) async fn inject(self, id: String, deadline: Instant) -> Result<Command, Failure> {
        let query = CommandGetRequest::new(&id);
        let reply = wait_slices(
            deadline,
            |wait_ms| {
                let request = ContextWaitRequest {
                    id: id.clone(),
                    wait_ms,
                };
                async move { self.call("inject.wait", &request).await }
            },
            |command: &Command| {
                query
                    .validate_response(command)
                    .map_err(Failure::readback)?;
                if command.verb != "inject" {
                    return Err(binding_mismatch());
                }
                Ok(injection_reportable(command))
            },
        )
        .await?;
        reply.ok_or_else(|| unsettled(&format!("command {}", query.id), "no validated snapshot"))
    }
}

fn injection_reportable(command: &Command) -> bool {
    command.state.is_terminal()
        || command.state == CommandState::Unknown
        || command
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome["delivery"] == "moved" || outcome["moved"] == true)
}

async fn wait_slices<T, F: Future<Output = eliot_core::Result<T>>>(
    deadline: Instant,
    mut poll: impl FnMut(u64) -> F,
    mut settled: impl FnMut(&T) -> Result<bool, Failure>,
) -> Result<Option<T>, Failure> {
    let mut last = None;
    loop {
        let started = Instant::now();
        let remaining = deadline.saturating_duration_since(started);
        if remaining.is_zero() {
            break;
        }
        let wait_ms = slice_ms(remaining);
        let Ok(reply) = timeout_at(deadline, poll(wait_ms)).await else {
            break;
        };
        let reply = match reply {
            Ok(reply) => reply,
            Err(error) if retryable_wait_read(&error) => {
                pause(deadline).await;
                continue;
            }
            Err(error) => return Err(Failure::readback(error)),
        };
        if settled(&reply)? {
            return Ok(Some(reply));
        }
        last = Some(reply);
        if wait_ms == 0 || started.elapsed() < Duration::from_millis(wait_ms) {
            // Pace snapshots that came back before the requested wait elapsed.
            sleep_until((Instant::now() + Duration::from_millis(25)).min(deadline)).await;
        }
    }
    Ok(last)
}

fn retryable_wait_read(error: &eliot_core::Error) -> bool {
    matches!(error.code.as_str(), "unavailable" | "busy")
        || (error.code == "unknown"
            && error
                .message
                .starts_with("daemon RPC exceeded its absolute deadline"))
}

fn slice_ms(remaining: Duration) -> u64 {
    // Leave time for the response and exact readback in the final slice.
    remaining
        .saturating_sub(WAIT_POLL_INTERVAL)
        .as_millis()
        .min(u128::from(MAX_SWARM_WAIT_MS)) as u64
}

pub(super) async fn wait_read<T>(
    call: impl Future<Output = eliot_core::Result<T>>,
    deadline: Instant,
    subject: &str,
    observation: &str,
) -> Result<T, Failure> {
    timeout_at(deadline, call)
        .await
        .map_err(|_| unsettled(subject, observation))?
        .map_err(Failure::readback)
}

pub(super) fn unsettled(subject: &str, observation: &str) -> Failure {
    Failure::new(
        4,
        format!(
            "{subject} outcome is unknown at the wait deadline (last observation: {observation})"
        ),
    )
}

fn turn_binding(entry: &WaitEntryResult) -> Option<WaitTurnRef> {
    Some(WaitTurnRef::new(
        entry.agent.as_ref()?,
        entry.generation?,
        entry.turn_id.as_ref()?,
    ))
}

fn binding_mismatch() -> Failure {
    Failure::new(
        4,
        "unknown: read response does not match the requested binding",
    )
}

async fn pause(deadline: Instant) {
    sleep_until((Instant::now() + WAIT_POLL_INTERVAL).min(deadline)).await;
}
