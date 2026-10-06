use crate::config::McpToolProfile;

use super::subscriptions::Category;

/// Frontend exposure filters. A new method is not exposed through a restricted profile
/// until it is named here. The host independently
/// authorizes every forwarded request from the authenticated credential.
pub(super) fn exposes_method(profile: McpToolProfile, method: &str) -> bool {
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

    if method == "module.catalog.get"
        && matches!(profile, McpToolProfile::Manager | McpToolProfile::Gm)
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
                | "swarm.overlap.check"
                | "coordination.work_card.get"
                | "coordination.work_card.list"
                | "coordination.work_card.publish"
                | "coordination.work_card.withdraw"
                | "coordination.contract_card.get"
                | "coordination.contract_card.list"
                | "coordination.contract_card.publish"
                | "coordination.contract_card.withdraw"
                | "coordination.send"
                | "coordination.sync_integration"
                | "coordination.consult"
                | "coordination.inbox"
                | "coordination.watch.create"
                | "coordination.watch.list"
                | "coordination.watch.cancel"
                | "review.get"
                | "review.list"
                | "swarm.review.context"
                | "review.submit"
                | "source.capture"
                | "task.submit"
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
            "module.route.select"
                | "task.request_changes"
                | "task.create"
                | "task.revise"
                | "task.claim"
                | "task.dispatch"
                | "task.submit"
                | "task.submit.recover"
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
                | "automation.config.transfer"
                | "automation.config.explain"
                | "bus.events.page"
                | "bus.consumer.admit"
                | "schedule.run_now"
                | "hook.source.get"
                | "hook.source.revoke"
                | "goal.create"
                | "goal.revise"
                | "goal.enable"
                | "goal.disable"
                | "goal.readback"
                | "goal.get"
                | "goal.list"
                | "script.register"
                | "script.revise"
                | "script.validate"
                | "script.activate"
                | "script.run"
                | "script.get"
                | "script.list"
                | "swarm.queue.get"
                | "swarm.agent.inspect"
                | "swarm.exceptions.get"
                | "swarm.launch.preview"
                | "swarm.launch"
                | "swarm.overlap.check"
                | "github.effect.managed_label"
                | "github.pull_request.update_description"
                | "github.pull_request.reconcile_description"
                | "coordination.watch.create"
                | "coordination.watch.list"
                | "coordination.watch.cancel"
        ),
        McpToolProfile::Gm => {
            (exposes_method(McpToolProfile::Manager, method)
                && !matches!(
                    method,
                    "automation.config.preview" | "automation.config.apply"
                ))
                || matches!(
                    method,
                    "client.list"
                        | "client.register"
                        | "host.mode"
                        | "task.accept"
                        | "task.invalidate_acceptance"
                        | "forge.publish_ref"
                        | "github.source.inspect"
                        | "github.source.get"
                        | "github.work_pool.preview"
                        | "github.work_pool.apply"
                        | "github.effect.managed_label"
                        | "github.effect.reconcile_managed_label"
                        | "github.pull_request.update_description"
                        | "github.pull_request.reconcile_description"
                        | "gm.handover"
                )
        }
        McpToolProfile::Full => true,
    }
}

pub(super) fn allows_subscription_category(profile: McpToolProfile, category: Category) -> bool {
    exposes_method(profile, "report.delta")
        && match category {
            Category::Reports => true,
            Category::Mailbox => exposes_method(profile, "message.read"),
            Category::Operations => exposes_method(profile, "operation.get"),
        }
}

pub(super) fn allows_task_get(profile: McpToolProfile) -> bool {
    exposes_method(profile, "operation.get") && exposes_method(profile, "report.attention")
}

pub(super) fn allows_task_cancel(profile: McpToolProfile) -> bool {
    exposes_method(profile, "operation.get") && exposes_method(profile, "operation.cancel")
}
