//! Pure classification of retained work: an engine is moving it, an owner was
//! asked, it is parked on purpose, it is stuck, or it has remained live in one
//! state too long.

use crate::api::{GoalStatus, LaunchIntentState, OwnerInboxKind};
use crate::{AgentState, CommandState, ProviderHoldState, ProviderRestrictionKind, TurnState};

const HOUR_MS: i64 = 60 * 60 * 1_000;
const DAY_MS: i64 = 24 * HOUR_MS;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LivenessObject {
    Agent {
        state: AgentState,
        since: i64,
        owner_item: bool,
        launch_queued: bool,
        active_turn: bool,
        wait_evidence: Option<AgentWaitEvidence>,
    },
    Command {
        state: CommandState,
        since: i64,
        owner_item: bool,
        dispatcher_active: bool,
        effect_active: bool,
    },
    Turn {
        state: TurnState,
        since: i64,
        owner_item: bool,
        executor_active: bool,
    },
    Goal {
        state: GoalStatus,
        since: i64,
        owner_item: bool,
        round_active: bool,
        timer_due: Option<i64>,
    },
    Hold {
        state: ProviderHoldState,
        kind: ProviderRestrictionKind,
        since: i64,
        owner_item: bool,
        probe_active: bool,
        reset_at: Option<i64>,
    },
    LaunchIntent {
        state: LaunchIntentState,
        since: i64,
        command_active: bool,
        owner_item: bool,
    },
    AssignmentWait {
        since: i64,
        due: i64,
        timer_active: bool,
        owner_linked: bool,
    },
    HarnessRequest {
        since: i64,
    },
    Timer {
        since: i64,
        due: Option<i64>,
        active: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AgentWaitEvidence {
    SelfWake,
    Timer,
}

impl AgentWaitEvidence {
    const fn reason(self) -> &'static str {
        match self {
            Self::SelfWake => "armed_agent_wake",
            Self::Timer => "agent_wake_timer",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Mover {
    Moving {
        reason: &'static str,
        by: i64,
    },
    Asked {
        reason: &'static str,
        item: OwnerInboxKind,
    },
    Parked {
        reason: &'static str,
    },
    Stuck {
        reason: &'static str,
    },
    Long {
        reason: &'static str,
    },
}

/// Classify an object using only persisted evidence supplied by its Store owner.
/// No fallback state or synthetic deadline can make work appear live.
pub fn mover(object: &LivenessObject, now: i64) -> Mover {
    match object {
        LivenessObject::Agent {
            state,
            since,
            owner_item,
            launch_queued,
            active_turn,
            wait_evidence,
        } => match state {
            AgentState::Created => {
                if *launch_queued {
                    moving_or_long(*since, now, HOUR_MS, "queued_launch", None)
                } else {
                    Mover::Stuck {
                        reason: "created_without_dispatch",
                    }
                }
            }
            AgentState::Starting => {
                if *launch_queued {
                    moving_or_long(*since, now, HOUR_MS, "launch_dispatch", None)
                } else if *owner_item {
                    Mover::Asked {
                        reason: "starting_agent_in_owner_inbox",
                        item: OwnerInboxKind::HarnessRequest,
                    }
                } else {
                    Mover::Stuck {
                        reason: "starting_without_dispatch",
                    }
                }
            }
            AgentState::Idle => {
                if *owner_item {
                    Mover::Asked {
                        reason: "idle_agent_owner_question",
                        item: OwnerInboxKind::HarnessRequest,
                    }
                } else if let Some(evidence) = wait_evidence {
                    Mover::Parked {
                        reason: evidence.reason(),
                    }
                } else {
                    moving_or_long(
                        *since,
                        now,
                        7 * DAY_MS,
                        "idle_agent_awaiting_assignment_or_automation",
                        None,
                    )
                }
            }
            AgentState::Busy => {
                if *active_turn {
                    moving_or_long(*since, now, 7 * DAY_MS, "active_turn", None)
                } else if *owner_item {
                    Mover::Asked {
                        reason: "busy_agent_owner_question",
                        item: OwnerInboxKind::HarnessRequest,
                    }
                } else {
                    Mover::Stuck {
                        reason: "busy_without_active_turn",
                    }
                }
            }
            AgentState::Waiting => {
                if *owner_item {
                    Mover::Asked {
                        reason: "waiting_agent_owner_question",
                        item: OwnerInboxKind::HarnessRequest,
                    }
                } else if let Some(evidence) = wait_evidence {
                    Mover::Parked {
                        reason: evidence.reason(),
                    }
                } else if *active_turn {
                    moving_or_long(*since, now, DAY_MS, "active_waiting_turn", None)
                } else {
                    Mover::Stuck {
                        reason: "waiting_without_turn_or_question",
                    }
                }
            }
            AgentState::Unknown => Mover::Stuck {
                reason: "unknown_agent_state",
            },
            AgentState::Stopped | AgentState::Failed | AgentState::Lost => {
                if *owner_item {
                    Mover::Asked {
                        reason: "terminal_agent_in_owner_inbox",
                        item: OwnerInboxKind::Agent,
                    }
                } else {
                    Mover::Stuck {
                        reason: "terminal_agent_in_liveness_input",
                    }
                }
            }
        },
        LivenessObject::Command {
            state,
            since,
            owner_item,
            dispatcher_active,
            effect_active,
        } => match state {
            CommandState::Accepted | CommandState::Queued => {
                if *dispatcher_active {
                    moving_or_long(*since, now, DAY_MS, "command_dispatch_queue", None)
                } else if *owner_item {
                    Mover::Asked {
                        reason: "command_in_owner_inbox",
                        item: OwnerInboxKind::UnknownCommand,
                    }
                } else {
                    Mover::Stuck {
                        reason: "command_without_dispatcher",
                    }
                }
            }
            CommandState::Dispatched => {
                if *effect_active {
                    moving_or_long(*since, now, DAY_MS, "command_effect_or_receipt", None)
                } else if *owner_item {
                    Mover::Asked {
                        reason: "dispatched_command_owner_item",
                        item: OwnerInboxKind::UnknownCommand,
                    }
                } else {
                    Mover::Stuck {
                        reason: "dispatched_without_effect_or_receipt",
                    }
                }
            }
            CommandState::Unknown => {
                if *owner_item {
                    Mover::Asked {
                        reason: "unknown_command_in_owner_inbox",
                        item: OwnerInboxKind::UnknownCommand,
                    }
                } else {
                    Mover::Stuck {
                        reason: "unknown_command_without_owner_item",
                    }
                }
            }
            CommandState::Applied
            | CommandState::Rejected
            | CommandState::Failed
            | CommandState::Cancelled => Mover::Stuck {
                reason: "terminal_command_in_liveness_input",
            },
        },
        LivenessObject::Turn {
            state,
            since,
            owner_item,
            executor_active,
        } => match state {
            TurnState::Running => {
                if *executor_active {
                    moving_or_long(*since, now, 7 * DAY_MS, "turn_executor", None)
                } else if *owner_item {
                    Mover::Asked {
                        reason: "running_turn_owner_item",
                        item: OwnerInboxKind::UnknownTurn,
                    }
                } else {
                    Mover::Stuck {
                        reason: "running_turn_without_executor",
                    }
                }
            }
            TurnState::Unknown => {
                if *owner_item {
                    Mover::Asked {
                        reason: "unknown_turn_in_owner_inbox",
                        item: OwnerInboxKind::UnknownTurn,
                    }
                } else {
                    Mover::Stuck {
                        reason: "unknown_turn_without_owner_item",
                    }
                }
            }
            TurnState::Completed => Mover::Stuck {
                reason: "terminal_turn_in_liveness_input",
            },
        },
        LivenessObject::Goal {
            state,
            since,
            owner_item,
            round_active,
            timer_due,
        } => match state {
            GoalStatus::Active => {
                if *round_active {
                    moving_or_long(*since, now, 7 * DAY_MS, "active_goal_round", None)
                } else if let Some(due) = (*timer_due).filter(|due| *due > now) {
                    moving_or_long(*since, now, 7 * DAY_MS, "goal_timer", Some(due))
                } else if *owner_item {
                    Mover::Asked {
                        reason: "active_goal_owner_item",
                        item: OwnerInboxKind::BlockedGoal,
                    }
                } else {
                    Mover::Stuck {
                        reason: "active_goal_without_round_or_timer",
                    }
                }
            }
            GoalStatus::Blocked => Mover::Asked {
                reason: "blocked_goal_in_owner_inbox",
                item: OwnerInboxKind::BlockedGoal,
            },
            GoalStatus::Paused => {
                if *owner_item {
                    Mover::Asked {
                        reason: "paused_goal_owner_item",
                        item: OwnerInboxKind::BlockedGoal,
                    }
                } else {
                    Mover::Stuck {
                        reason: "paused_goal_without_owner_item",
                    }
                }
            }
            GoalStatus::Cleared | GoalStatus::Completed => Mover::Stuck {
                reason: "terminal_goal_in_liveness_input",
            },
        },
        LivenessObject::Hold {
            state,
            kind,
            since,
            owner_item,
            probe_active,
            reset_at,
        } => match state {
            ProviderHoldState::Held => match (kind, reset_at, owner_item) {
                (
                    ProviderRestrictionKind::RateLimit | ProviderRestrictionKind::Quota,
                    Some(at),
                    _,
                ) if *at > now => {
                    moving_or_long(*since, now, 7 * DAY_MS, "provider_reset", Some(*at))
                }
                (_, _, true) => Mover::Asked {
                    reason: "provider_hold_in_owner_inbox",
                    item: OwnerInboxKind::Hold,
                },
                (_, _, false) => Mover::Stuck {
                    reason: "provider_hold_without_probe_reset_or_owner_item",
                },
            },
            ProviderHoldState::Probing => {
                if *probe_active {
                    moving_or_long(*since, now, DAY_MS, "provider_probe_command", None)
                } else if *owner_item {
                    Mover::Asked {
                        reason: "provider_probe_owner_item",
                        item: OwnerInboxKind::Hold,
                    }
                } else {
                    Mover::Stuck {
                        reason: "provider_probe_without_active_command",
                    }
                }
            }
        },
        LivenessObject::LaunchIntent {
            state,
            since,
            command_active,
            owner_item,
        } => match state {
            LaunchIntentState::Queued => {
                if *owner_item {
                    Mover::Asked {
                        reason: "owner_linked_launch_wait",
                        item: OwnerInboxKind::LaunchQueue,
                    }
                } else if *command_active {
                    moving_or_long(*since, now, DAY_MS, "queued_launch_arbitration", None)
                } else {
                    Mover::Stuck {
                        reason: "queued_launch_without_arbitration",
                    }
                }
            }
            LaunchIntentState::Admitted => {
                if *command_active {
                    moving_or_long(*since, now, DAY_MS, "admitted_launch_command", None)
                } else {
                    Mover::Stuck {
                        reason: "admitted_launch_without_command",
                    }
                }
            }
            LaunchIntentState::Rejected => Mover::Stuck {
                reason: "terminal_launch_intent_in_liveness_input",
            },
        },
        LivenessObject::AssignmentWait {
            since,
            due,
            timer_active,
            owner_linked,
        } => {
            if *owner_linked {
                Mover::Asked {
                    reason: "owner_linked_assignment_wait",
                    item: OwnerInboxKind::LaunchQueue,
                }
            } else if *timer_active && *due > now {
                moving_or_long(*since, now, 7 * DAY_MS, "assignment_timer", Some(*due))
            } else {
                Mover::Stuck {
                    reason: "assignment_wait_without_active_timer",
                }
            }
        }
        LivenessObject::HarnessRequest { .. } => Mover::Asked {
            reason: "harness_request_in_owner_inbox",
            item: OwnerInboxKind::HarnessRequest,
        },
        LivenessObject::Timer { since, due, active } => {
            if *active && due.is_some_and(|at| at > now) {
                moving_or_long(*since, now, 7 * DAY_MS, "scheduled_timer", *due)
            } else {
                Mover::Stuck {
                    reason: "timer_without_active_future_schedule",
                }
            }
        }
    }
}

fn moving_or_long(
    since: i64,
    now: i64,
    threshold: i64,
    reason: &'static str,
    by: Option<i64>,
) -> Mover {
    if now.saturating_sub(since) > threshold {
        Mover::Long { reason }
    } else {
        Mover::Moving {
            reason,
            by: by.unwrap_or_else(|| since.saturating_add(threshold)),
        }
    }
}
