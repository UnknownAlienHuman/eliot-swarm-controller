use crate::config::McpToolProfile;

use super::subscriptions::Category;

/// Frontend method exposure is shared data. The host still authorizes every
/// forwarded request; this wrapper keeps subscription code on its existing API.
pub(super) fn exposes_method(profile: McpToolProfile, method: &str) -> bool {
    swarm_contracts::mcp_catalog::exposes_method(profile, method)
}
#[derive(Debug, Clone, Copy)]
pub(super) struct SubscriptionMethodRequirement {
    pub(super) all: &'static [&'static str],
    pub(super) any: &'static [&'static str],
}

pub(super) fn subscription_method_requirement(category: Category) -> SubscriptionMethodRequirement {
    match category {
        Category::Reports => SubscriptionMethodRequirement {
            all: &["report.delta"],
            any: &[],
        },
        Category::Mailbox => SubscriptionMethodRequirement {
            all: &["report.delta", "message.read"],
            any: &[],
        },
        Category::Operations => SubscriptionMethodRequirement {
            all: &["report.delta", "operation.get"],
            any: &[],
        },
        Category::Concilium => SubscriptionMethodRequirement {
            all: &["report.delta"],
            any: &["concilium.get", "concilium.list"],
        },
        Category::Coordination => SubscriptionMethodRequirement {
            all: &[
                "report.delta",
                "coordination.thread.get",
                "coordination.thread.list",
                "coordination.contract.get",
                "coordination.contract.list",
            ],
            any: &[],
        },
    }
}

pub(super) fn task_get_required_methods() -> &'static [&'static str] {
    &["operation.get", "report.attention"]
}

pub(super) fn task_cancel_required_methods() -> &'static [&'static str] {
    &["operation.get", "operation.cancel"]
}

fn exposes_all(profile: McpToolProfile, methods: &[&str]) -> bool {
    methods.iter().all(|method| exposes_method(profile, method))
}

pub(super) fn allows_subscription_category(profile: McpToolProfile, category: Category) -> bool {
    let requirement = subscription_method_requirement(category);
    exposes_all(profile, requirement.all)
        && (requirement.any.is_empty()
            || requirement
                .any
                .iter()
                .any(|method| exposes_method(profile, method)))
}

pub(super) fn allows_task_get(profile: McpToolProfile) -> bool {
    exposes_all(profile, task_get_required_methods())
}

pub(super) fn allows_task_cancel(profile: McpToolProfile) -> bool {
    exposes_all(profile, task_cancel_required_methods())
}
