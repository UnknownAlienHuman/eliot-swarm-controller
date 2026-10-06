//! Data-only positive method policy shared by Store, CLI and MCP.
//!
//! This table classifies methods that already have source handlers. Profile
//! and object checks may narrow these methods, but no profile can manufacture
//! a method or replace the Store's ownership/GM/revision checks.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MethodClass {
    ReadOnly,
    Mutation,
    /// A local frontend facade that never reaches Store.
    FacadeOnly,
    /// A trusted internal path that is not an application/MCP method.
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParticipantAccess {
    None,
    Read,
    Mutation,
    CoordinationRead,
    CoordinationMutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MethodPolicy {
    pub method: &'static str,
    pub class: MethodClass,
    /// The independent MCP frontend has a typed schema for this method.
    pub mcp: bool,
    /// Assignment-bound Participant admission, if any.
    pub participant: ParticipantAccess,
}

/// Positive registry of the current typed MCP inventory plus closed Store-only
/// service calls. Unknown names do not acquire a default writer path.
pub const METHOD_REGISTRY: &[MethodPolicy] = &[
    MethodPolicy {
        method: "swarm.tools.search",
        class: MethodClass::FacadeOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "swarm.context.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "swarm.dashboard",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "monitor.snapshot",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "swarm.queue.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "swarm.agent.inspect",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "swarm.exceptions.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "swarm.launch.preview",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "swarm.launch",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "coordination.participant.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "coordination.participant.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "coordination.peer.find",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "swarm.overlap.check",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "coordination.work_card.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "coordination.work_card.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "coordination.contract_card.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "coordination.contract_card.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "coordination.inbox",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "coordination.watch.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "review.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::Read,
    },
    MethodPolicy {
        method: "review.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::Read,
    },
    MethodPolicy {
        method: "swarm.review.context",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::Read,
    },
    MethodPolicy {
        method: "automation.config.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "automation.config.preview",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "automation.config.explain",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "bus.events.page",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "host.status",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "route.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.catalog.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "client.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.submission",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::Read,
    },
    MethodPolicy {
        method: "task.acceptance",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "attempt.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "operation.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::CoordinationRead,
    },
    MethodPolicy {
        method: "logging.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "operation.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.state",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.family",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "check.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::Read,
    },
    MethodPolicy {
        method: "check.profiles",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "artifact.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "artifact.read",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::Read,
    },
    MethodPolicy {
        method: "artifact.parts",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "report.delta",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "monitor.follow",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "report.attention",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "report.capacity",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "message.read",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "coordination.participant.register",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "coordination.participant.disable",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "coordination.work_card.publish",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.work_card.withdraw",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.contract_card.publish",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.contract_card.withdraw",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.send",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.sync_integration",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.consult",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.watch.create",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "coordination.watch.cancel",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::CoordinationMutation,
    },
    MethodPolicy {
        method: "review.assign",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "review.submit",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::Mutation,
    },
    MethodPolicy {
        method: "automation.config.apply",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "logging.set",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "event.emit",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "bus.consumer.admit",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "schedule.run_now",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "automation.config.transfer",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "host.mode",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.route.select",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "client.register",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "source.capture",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::Mutation,
    },
    MethodPolicy {
        method: "check.run",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "check.cancel",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.create",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.revise",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.claim",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.dispatch",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.submit",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::Mutation,
    },
    MethodPolicy {
        method: "task.submit.recover",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.request_changes",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.accept",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "forge.publish_ref",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "task.invalidate_acceptance",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "attempt.release",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "attempt.bind_producer",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.open",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.send",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.reply",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.configure",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.goal",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.refresh",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.reconcile",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.recover",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.result",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "artifact.assemble",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "operation.cancel",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "gm.handover",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "agent.background",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "message.send",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "message.cancel",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "hook.source.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "hook.source.revoke",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "goal.create",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "goal.revise",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "goal.enable",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "goal.disable",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "goal.readback",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "goal.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "goal.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "script.register",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "script.revise",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "script.validate",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "script.activate",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "script.run",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "script.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "script.list",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.source.inspect",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.source.setup",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.source.get",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.source.poll",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.work_pool.preview",
        class: MethodClass::ReadOnly,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.work_pool.apply",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.effect.managed_label",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.effect.reconcile_managed_label",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.pull_request.update_description",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "github.pull_request.reconcile_description",
        class: MethodClass::Mutation,
        mcp: true,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "mcp.authorization",
        class: MethodClass::ReadOnly,
        mcp: false,
        participant: ParticipantAccess::Read,
    },
    MethodPolicy {
        method: "doctor.inspect",
        class: MethodClass::ReadOnly,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "hook.emit",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "hook.source.setup",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.hello",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.next",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.result",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.observe",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.outcome",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.event",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "bus.consumer.register",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "bus.consumer.revoke",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "automation.scheduler.page",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "automation.scheduler.admit",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.descriptor.register",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.admission",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.demand.page",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.scope.readback",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.credential.ensure",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.credential.ready",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.recovery.reconcile",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.observation.record",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
    MethodPolicy {
        method: "module.supervisor.health.record",
        class: MethodClass::Internal,
        mcp: false,
        participant: ParticipantAccess::None,
    },
];

pub fn policy(method: &str) -> Option<&'static MethodPolicy> {
    METHOD_REGISTRY.iter().find(|entry| entry.method == method)
}

pub fn class(method: &str) -> Option<MethodClass> {
    policy(method).map(|entry| entry.class)
}

/// Return the request-ID/read-only class for implemented application methods.
/// Facade-only and trusted-internal paths intentionally return None.
pub fn read_only(method: &str) -> Option<bool> {
    match class(method) {
        Some(MethodClass::ReadOnly) => Some(true),
        Some(MethodClass::Mutation) => Some(false),
        Some(MethodClass::FacadeOnly | MethodClass::Internal) | None => None,
    }
}

pub fn is_read_only(method: &str) -> bool {
    class(method) == Some(MethodClass::ReadOnly)
}

pub fn is_mcp_method(method: &str) -> bool {
    policy(method).is_some_and(|entry| entry.mcp)
}

pub fn participant_allowed(method: &str) -> bool {
    policy(method).is_some_and(|entry| entry.participant != ParticipantAccess::None)
}

pub fn participant_read(method: &str) -> bool {
    policy(method).is_some_and(|entry| {
        matches!(
            entry.participant,
            ParticipantAccess::Read | ParticipantAccess::CoordinationRead,
        )
    })
}

pub fn participant_mutation(method: &str) -> bool {
    policy(method).is_some_and(|entry| {
        matches!(
            entry.participant,
            ParticipantAccess::Mutation | ParticipantAccess::CoordinationMutation,
        )
    })
}

pub fn participant_coordination_read(method: &str) -> bool {
    policy(method).is_some_and(|entry| entry.participant == ParticipantAccess::CoordinationRead)
}

pub fn participant_coordination_mutation(method: &str) -> bool {
    policy(method).is_some_and(|entry| entry.participant == ParticipantAccess::CoordinationMutation)
}
