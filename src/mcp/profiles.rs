use crate::config::McpToolProfile;

use super::subscriptions::Category;

/// Explicit profile allowlists. A new method is unavailable to every
/// restricted profile until it is named here; Full is the opt-in compatibility
/// surface for the complete local tool table.
pub(super) fn allows_method(profile: McpToolProfile, method: &str) -> bool {
    if profile == McpToolProfile::Full {
        return true;
    }

    let observer_read = matches!(
        method,
        "swarm.tools.search"
            | "host.status"
            | "swarm.dashboard"
            | "task.get"
            | "task.list"
            | "task.submission"
            | "task.acceptance"
            | "attempt.get"
            | "operation.get"
            | "operation.list"
            | "agent.state"
            | "agent.list"
            | "agent.family"
            | "check.get"
            | "check.profiles"
            | "artifact.get"
            | "artifact.read"
            | "artifact.parts"
            | "report.delta"
            | "report.attention"
            | "report.capacity"
            | "message.read"
    );
    if observer_read
        && matches!(
            profile,
            McpToolProfile::Observer
                | McpToolProfile::Reviewer
                | McpToolProfile::Manager
                | McpToolProfile::Gm
        )
    {
        return true;
    }

    match profile {
        McpToolProfile::Observer => false,
        McpToolProfile::Reviewer => method == "task.request_changes",
        McpToolProfile::Participant => matches!(
            method,
            "swarm.tools.search"
                | "swarm.context.get"
                | "coordination.peer.find"
                | "coordination.work_card.get"
                | "coordination.work_card.list"
                | "coordination.work_card.publish"
                | "coordination.work_card.withdraw"
                | "coordination.contract_card.get"
                | "coordination.contract_card.list"
                | "coordination.contract_card.publish"
                | "coordination.contract_card.withdraw"
                | "coordination.send"
                | "coordination.inbox"
                | "review.get"
                | "review.list"
                | "swarm.review.context"
                | "review.submit"
                | "task.submission"
                | "check.get"
                | "artifact.read"
                | "operation.get"
        ),
        McpToolProfile::AssignedReviewer => matches!(
            method,
            "swarm.tools.search"
                | "swarm.review.context"
                | "review.get"
                | "review.list"
                | "review.submit"
                | "task.submission"
                | "check.get"
                | "artifact.read"
                | "operation.get"
        ),
        McpToolProfile::Manager => matches!(
            method,
            "task.request_changes"
                | "task.create"
                | "task.revise"
                | "task.claim"
                | "task.dispatch"
                | "task.submit"
                | "attempt.release"
                | "attempt.bind_producer"
                | "operation.cancel"
                | "agent.open"
                | "agent.send"
                | "agent.reply"
                | "agent.configure"
                | "agent.goal"
                | "agent.background"
                | "agent.refresh"
                | "agent.reconcile"
                | "agent.recover"
                | "agent.result"
                | "message.send"
                | "message.cancel"
                | "coordination.participant.register"
                | "coordination.participant.disable"
                | "coordination.participant.get"
                | "coordination.participant.list"
                | "swarm.context.get"
                | "coordination.peer.find"
                | "coordination.work_card.get"
                | "coordination.work_card.list"
                | "coordination.contract_card.get"
                | "coordination.contract_card.list"
                | "review.assign"
                | "review.get"
                | "review.list"
                | "swarm.review.context"
                | "automation.config.get"
                | "automation.config.preview"
                | "automation.config.apply"
                | "automation.config.explain"
                | "swarm.queue.get"
                | "swarm.agent.inspect"
                | "swarm.exceptions.get"
        ),
        McpToolProfile::Gm => {
            (allows_method(McpToolProfile::Manager, method)
                && !matches!(
                    method,
                    "automation.config.get"
                        | "automation.config.preview"
                        | "automation.config.apply"
                        | "automation.config.explain"
                ))
                || matches!(
                    method,
                    "client.list"
                        | "client.register"
                        | "host.mode"
                        | "task.accept"
                        | "task.invalidate_acceptance"
                        | "forge.publish_ref"
                        | "gm.handover"
                )
        }
        McpToolProfile::Full => true,
    }
}

pub(super) fn allows_subscription_category(profile: McpToolProfile, category: Category) -> bool {
    allows_method(profile, "report.delta")
        && match category {
            Category::Reports => true,
            Category::Mailbox => allows_method(profile, "message.read"),
            Category::Operations => allows_method(profile, "operation.get"),
        }
}

pub(super) fn allows_task_get(profile: McpToolProfile) -> bool {
    allows_method(profile, "operation.get") && allows_method(profile, "report.attention")
}

pub(super) fn allows_task_cancel(profile: McpToolProfile) -> bool {
    allows_method(profile, "operation.get") && allows_method(profile, "operation.cancel")
}
