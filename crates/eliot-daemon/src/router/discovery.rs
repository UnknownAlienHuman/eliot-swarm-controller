//! Descriptions beside Router registration. Wire shapes belong to Core DTOs.
use super::{Principal, Router};
use eliot_core::api::inference::*;
use eliot_core::{
    Command, ContextWaitRequest, Error, Result, Turn,
    api::{discovery::*, *},
};
use serde_json::{Value, json};
use std::sync::OnceLock;

struct Method {
    name: &'static str,
    summary: &'static str,
    owner_only: bool,
    mutation: bool,
    input: fn() -> Value,
    output: fn() -> Value,
    example: fn() -> Value,
}

const EXAMPLE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

macro_rules! method {
    ($name:literal, $request:ty, $response:ty, $owner:literal, $write:literal, $doc:literal, $example:tt) => {
        Method {
            name: $name,
            summary: $doc,
            owner_only: $owner,
            mutation: $write,
            input: request_schema::<$request>,
            output: response_schema::<$response>,
            example: || json!($example),
        }
    };
}

// Every Router match arm is described here. The coverage regression below makes
// omission deliberate: new methods must be described before they can ship.
const METHODS: &[Method] = &[
    method!(
        "owner.inbox",
        OwnerInboxRequest,
        OwnerInboxResponse,
        true,
        false,
        "Read the Owner decision queue and offered actions.",
        {}
    ),
    method!(
        "checkpoint.restore",
        CheckpointRestoreRequest,
        CheckpointRestoreResponse,
        true,
        true,
        "Restore both staged and worktree checkpoint layers into a clean worktree at the captured HEAD.",
        {"id":EXAMPLE_ID,"agent":"a1","generation":1,"checkpoint_ref":"refs/eliot/checkpoints/a1/g1/0000000000000001-01ARZ3NDEKTSV4RRFFQ69G5FAV","workspace":"C:/work"}
    ),
    method!("owner.decide", OwnerDecideRequest, OwnerDecideResponse, true, true, "Execute a revision-bound choice or snooze the item until its subject changes.", {"id":EXAMPLE_ID,"item":{"kind":"agent","subject":"a1","revision":"example"},"choice":"snooze"}),
    method!("owner.decide.bulk", OwnerBulkDecideRequest, OwnerBulkDecideResponse, true, true, "Apply recommended actions for explicit revision-bound Owner inbox refs.", {"id":EXAMPLE_ID,"items":[{"kind":"agent","subject":"a1","revision":"example"}]}),
    method!("launch.enqueue", LaunchQueueRequest, LaunchIntentView, false, true, "Enqueue an interactive launch intent for fair admission.", {"id":EXAMPLE_ID,"args":{"profile":"worker","workspace":"C:/work"}}),
    method!("launch.intent.get", LaunchIntentGetRequest, LaunchIntentView, false, false, "Read the current state of a queued interactive launch intent.", {"id":EXAMPLE_ID}),
    method!(
        "launch.queue",
        LaunchQueueListRequest,
        LaunchQueueView,
        false,
        false,
        "Read scoped interactive launch intents and assignment wait reasons.",
        {}
    ),
    method!("project.budget.set", ManagerBudgetRequest, ManagerBudgetView, false, true, "Change free budget or priority within the current manager's Owner grant.", {"id":EXAMPLE_ID,"project":"project-1","expected_revision":1,"max_live":2}),
    method!("agent.archive", AgentArchiveRequest, AgentTreeResponse, true, true, "Archive or unarchive an idle or ended Agent tree (ended: custody resolved); never stops it.", {"id":EXAMPLE_ID,"agent":"a1","archived":true}),
    method!("agent.delete", AgentDeleteRequest, AgentTreeResponse, true, true, "Delete stopped or failed Agent trees and clean up their eligible managed worktrees and native sessions.", {"id":EXAMPLE_ID,"agent":"a1"}),
    method!("artifact.register", ArtifactRegisterRequest, ArtifactView, false, true, "Register an existing Agent-owned artifact directory with exact generation and opened identity.", {"id":EXAMPLE_ID,"path":"C:/scratch/build","kind":"build","purpose":"assignment build"}),
    method!(
        "chat.organization",
        ChatOrganizationRequest,
        ChatOrganizationResponse,
        true,
        false,
        "Grouped work projection by assignment, manager, repo and ancestry with event-watermark readback.",
        {}
    ),
    method!("chat.attach", ChatAttachRequest, ChatOrganizationMutationResponse, true, true, "Place chats under a retained project; execution scope is untouched.", {"id":EXAMPLE_ID,"agents":["a1"],"project":"app"}),
    method!("chat.detach", ChatDetachRequest, ChatOrganizationMutationResponse, true, true, "Return chats to the no-project bucket; execution scope is untouched.", {"id":EXAMPLE_ID,"agents":["a1"]}),
    method!("chat.tag", ChatTagRequest, ChatOrganizationMutationResponse, true, true, "Replace tag groups of Agents or chats with revision readback.", {"id":EXAMPLE_ID,"agents":["a1"],"tags":["inbox"]}),
    method!("cleanup.plan", CleanupPlanRequest, CleanupPlan, false, false, "Plan bounded cleanup: Agents own subtree; Owner project or all. No deletion.", {"scope":"self_tree"}),
    method!("cleanup.apply", CleanupApplyRequest, CleanupApplyResponse, false, true, "Consume one plan, rechecking safe entries; replay returns retained outcomes.", {"id":EXAMPLE_ID,"plan_id":EXAMPLE_ID}),
    method!(
        "client.identity",
        InferenceIdentityRequest,
        InferenceIdentityResponse,
        false,
        false,
        "Read the authenticated Memory installation identity; Memory principal only.",
        {}
    ),
    method!("inference.client.install", InferenceCredentialRequest, InferenceCredentialResponse, true, true, "Install one private Memory credential; Owner only; exact replay reconciles publication.", {"id":EXAMPLE_ID,"client_id":"memory:01ARZ3NDEKTSV4RRFFQ69G5FAV","expected_version":0}),
    method!("inference.client.rotate", InferenceCredentialRequest, InferenceCredentialResponse, true, true, "Stage one pending Memory credential; Owner only; no version allocation.", {"id":EXAMPLE_ID,"client_id":"memory:01ARZ3NDEKTSV4RRFFQ69G5FAV","expected_version":1}),
    method!("inference.client.promote", InferenceCredentialRequest, InferenceCredentialResponse, true, true, "Promote the pending credential; retain one previous credential.", {"id":EXAMPLE_ID,"client_id":"memory:01ARZ3NDEKTSV4RRFFQ69G5FAV","expected_version":1}),
    method!("inference.client.retire", InferenceCredentialRequest, InferenceCredentialResponse, true, true, "Retire the previous credential after restart and release.", {"id":EXAMPLE_ID,"client_id":"memory:01ARZ3NDEKTSV4RRFFQ69G5FAV","expected_version":2}),
    method!("inference.text", InferenceTextRequest, InferenceReceipt, false, true, "Admit one bounded tool-free text inference; Memory principal only.", {"id":EXAMPLE_ID,"profile":"memory-text-v1","origin":"episode:1","disclosure":"provider_allowed","text":"hello","input_sha256":"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824","input_bytes":5,"response_format":"text","max_result_bytes":1024,"deadline_ms":30000}),
    method!("inference.get", InferenceGetRequest, InferenceCall, false, false, "Read current own-call physical state; Memory principal only.", {"call_id":EXAMPLE_ID}),
    method!("inference.write.get", InferenceWriteGetRequest, InferenceWriteResponse, false, false, "Read one exact own mutation binding without admission.", {"id":EXAMPLE_ID}),
    method!(
        "inference.high_water",
        InferenceHighWaterRequest,
        InferenceHighWaterResponse,
        false,
        false,
        "Capture own monotonic call/write high-waters and retained membership cuts.",
        {}
    ),
    method!("inference.list", InferenceListRequest, InferenceListResponse, false, false, "Page own call/write membership; pruned cuts return history_gap.", {"call_after_seq":0,"call_through_seq":1,"write_after_seq":0,"write_through_seq":1,"limit":20,"cursor":null}),
    method!("inference.retention.advance", InferenceRetentionRequest, InferenceRetentionResponse, false, true, "Advance Memory-asserted safe cuts; preserves live write replay.", {"id":EXAMPLE_ID,"observed_call_high_water_seq":1,"observed_write_high_water_seq":1,"safe_retain_after_seq":1,"retain_after_seq":1,"safe_write_retain_after_seq":1,"write_retain_after_seq":1,"limit":20}),
    method!("inference.result.read", InferenceResultRequest, InferenceResultResponse, false, false, "Read one exact own-result byte range.", {"call_id":EXAMPLE_ID,"result_sha256":"0000000000000000000000000000000000000000000000000000000000000000","offset":0,"length":1024}),
    method!("inference.cancel", InferenceCancelRequest, InferenceMutationResponse, false, true, "Cancel an own queued call; sending calls honestly report too_late.", {"id":EXAMPLE_ID,"call_id":EXAMPLE_ID}),
    method!("inference.release", InferenceReleaseRequest, InferenceMutationResponse, false, true, "Fence publication and clean owned copies after backup classification.", {"id":EXAMPLE_ID,"call_id":EXAMPLE_ID,"disposition":"abandon_for_erasure","result_sha256":null,"backup_dependency":"no_backup_dependencies"}),
    method!("api.methods", MethodsRequest, MethodsResponse, false, false, "List/search callable canonical methods in bounded pages.", {"query":"mail", "limit":20}),
    method!("api.help", MethodHelpRequest, MethodHelpResponse, false, false, "Read compact DTO fields and an example; full schemas are opt-in.", {"method":"mail.send"}),
    method!(
        "api.identity",
        IdentityRequest,
        IdentityResponse,
        false,
        false,
        "Read your authenticated identity and allowed scopes, without credentials.",
        {}
    ),
    method!(
        "daemon.status",
        DaemonStatusRequest,
        DaemonStatusResponse,
        true,
        false,
        "Read daemon liveness, custody and worker health.",
        {}
    ),
    method!("daemon.shutdown", DaemonShutdownRequest, DaemonShutdownResponse, true, true, "Request drain or force shutdown; acknowledgement is not process-exit proof.", {"id":EXAMPLE_ID,"mode":"drain"}),
    method!(
        "autonomy.get",
        AutonomyRequest,
        AutonomyResponse,
        true,
        false,
        "Read windowed Swarm autonomy counts and source coverage.",
        {}
    ),
    #[cfg(debug_assertions)]
    method!(
        "daemon.panic_test",
        DaemonPanicTestRequest,
        DaemonPanicTestResponse,
        true,
        false,
        "Debug builds only (Owner): acknowledge, capture a daemon panic, then abort.",
        {}
    ),
    method!(
        "maintenance.run",
        MaintenanceRunRequest,
        MaintenanceRunResponse,
        true,
        false,
        "Run the bounded retention pass now and report pruned counts.",
        {}
    ),
    method!(
        "maintenance.purge_legacy_attention_advisories",
        MaintenancePurgeLegacyAttentionRequest,
        MaintenancePurgeLegacyAttentionResponse,
        true,
        true,
        "Preview the exact count of owner-sent legacy advisory-attention mail, or purge it after an exact-count confirmation.",
        {}
    ),
    method!("command.submit", CommandSubmitRequest, CommandSubmitResponse, false, true, "Submit a command with launch/send/wait child verbs.", {"id":EXAMPLE_ID,"verb":"send","target":"a1","args":{"text":"Report current progress."}}),
    method!("command.get", CommandGetRequest, Command, false, false, "Read a retained command in your authorized scope.", {"id":EXAMPLE_ID}),
    method!("inject.wait", ContextWaitRequest, Command, false, false, "Wait up to 30 seconds for an exact insertion to be delivered, moved or cancelled.", {"id":EXAMPLE_ID,"wait_ms":30000}),
    method!("command.children", CommandChildrenRequest, CommandChildrenResponse, false, false, "Page the retained children of one command.", {"id":EXAMPLE_ID,"limit":20}),
    method!("command.resolve", CommandResolveRequest, CommandResolveResponse, true, true, "Record an owner resolution of an uncertain command; never replay its effect.", {"id":EXAMPLE_ID,"command_id":"previous-command","as":"failed","reason":"Owner verified failure."}),
    method!("command.settle_stopped", SettleStoppedRequest, SettleStoppedResponse, true, true, "Preview or settle historical stops backed by owned exit and reader drain, plus exact native request-reply receipts.", {"id":EXAMPLE_ID}),
    method!("agent.get", AgentGetRequest, AgentGetResponse, false, false, "Read Agent state, native capabilities and source health.", {"id":"a1"}),
    method!("agent.discover", AgentDiscoverRequest, DiscoverPage, true, false, "Discover native metadata through an installed profile without creating Agents.", {"profile":"codex","limit":20}),
    method!(
        "harness.list",
        HarnessListRequest,
        HarnessListResponse,
        true,
        false,
        "List installed harness executables with versions.",
        {}
    ),
    method!("harness.models", HarnessModelsRequest, HarnessModelsResponse, true, false, "Read a harness native model catalogue (read-only).", {"harness":"codex"}),
    method!("harness.quota", HarnessQuotaRequest, HarnessQuotaResponse, true, false, "Read provider-supported account quota when supported.", {"harness":"codex"}),
    method!("models.catalog", ModelsCatalogRequest, ModelsCatalogResponse, true, false, "Search one unified model catalogue across every installed harness (read-only).", {"query":"luna"}),
    method!("agent.history", AgentHistoryRequest, ObservationPage, true, false, "Read a bounded native history view advertised by agent.get.", {"id":"a1","source":"native","view":"turns","limit":20}),
    method!("agent.list", AgentListRequest, AgentListResponse, false, false, "List scoped compact Agent summaries with filters and counts.", {"limit":20,"state":"busy"}),
    method!("agent.result", AgentResultRequest, AgentResultResponse, false, false, "Page through one Agent's retained turns and their generation origins.", {"id":"a1","limit":20}),
    method!("agent.processes", AgentProcessesRequest, AgentProcessesResponse, false, false, "Read bounded process observations for one Agent.", {"id":"a1"}),
    method!("agent.stats", AgentStatsRequest, AgentStatsResponse, false, false, "Read sampled Agent statistics with freshness metadata.", {"id":"a1"}),
    method!(
        "observe.heavy",
        ObserveHeavyRequest,
        ObserveHeavyResponse,
        false,
        false,
        "Read the windowed heavy-work episode rollup per Agent generation.",
        {}
    ),
    method!(
        "harness.doctor",
        HarnessDoctorRequest,
        HarnessDoctorResponse,
        true,
        false,
        "Read installed-vs-verified harness versions, contract markers and missing profile executables.",
        {}
    ),
    method!("usage.summary", UsageSummaryRequest, UsageSummaryResponse, false, false, "Read bounded usage totals over one Agent, project or tree scope.", {"agent":"a1"}),
    method!("agent.git", AgentGitRequest, AgentGitResponse, false, false, "Read Git observations for one Agent's workspace.", {"id":"a1"}),
    method!("agent.patch_digest", AgentPatchDigestRequest, AgentPatchDigestResponse, false, false, "Digest an Agent's patch for review: files, packages, new dependencies, public interface, checker changes and suppressions.", {"id":"a1"}),
    method!("agent.edits", AgentEditsRequest, AgentEditsResponse, false, false, "Read a bounded page of observed workspace edits.", {"id":"a1","limit":20}),
    method!("turn.get", TurnGetRequest, Turn, false, false, "Read one exact Agent/generation/turn binding.", {"agent":"a1","generation":1,"turn_id":"turn-1"}),
    method!("turn.resolve", TurnResolveRequest, TurnResolveResponse, true, true, "Record an owner resolution for one uncertain exact turn.", {"id":EXAMPLE_ID,"agent":"a1","generation":1,"turn_id":"turn-1","status":"completed","reason":"Owner verified completion."}),
    method!("diagnose", DiagnoseRequest, DiagnoseResponse, true, false, "Explain a failed command, Agent or agent:turn: origin, where, exits, stderr tail (Owner).", {"target":"a1"}),
    method!(
        "events.bounds",
        EventsBoundsRequest,
        EventsBoundsResponse,
        false,
        false,
        "Read retained event bounds in your visible scope.",
        {}
    ),
    method!("events.read", EventsReadRequest, EventsReadResponse, false, false, "Read a bounded event page; history gaps remain explicit.", {"after":0,"limit":20}),
    method!("events.subscribe", EventsSubscribeRequest, EventsSubscribeResponse, false, false, "Long-poll a bounded event page, at most 30 seconds.", {"after":0,"limit":20,"wait_ms":1000}),
    method!("child.run", ChildRunRequest, ChildRunResponse, false, true, "Launch one child with its first input and return the exact result; wait_ms:0 registers a durable completion wake.", {"id":EXAMPLE_ID,"text":"Review the requested source and summarize the result.","wait_ms":0}),
    method!("agent.wake", AgentWakeRequest, AgentWakeResponse, false, true, "Arm one self-scoped wake; the exact caller turn settles before one fact is sent. argv_exit0 names an already authorized Run receipt.", {"id":EXAMPLE_ID,"condition":{"kind":"seconds","seconds":5},"timeout_secs":60}),
    method!("swarm.wait", SwarmWaitRequest, SwarmWaitResponse, false, false, "Wait up to 30 s for frozen child commands/turns (all/any); completed turns return final_text.", {"commands":[EXAMPLE_ID],"turns":[],"mode":"all","wait_ms":30000}),
    method!("swarm.digest", SwarmDigestRequest, SwarmDigestResponse, false, true, "Register a durable completion digest with the standard mail wake.", {"id":EXAMPLE_ID,"to":"a1","mode":"all","commands":[EXAMPLE_ID],"turns":[]}),
    method!("swarm.reparent", ReparentRequest, ReparentResponse, true, true, "Move one Agent subtree or all children to a controlled manager; Owner only.", {"id":EXAMPLE_ID,"agent":"a2","expected_generation":1,"parent":"a3","parent_generation":1}),
    method!("swarm.switch", SwitchRequest, SwitchResponse, false, true, "Move a task to a new session in its worktree; Owner anywhere, manager for a worker in scope; launch.project continues a chat in a project.", {"id":EXAMPLE_ID,"from":"a1","from_generation":1,"task_command":"retained-task","commands":[],"launch":{"harness":"antigravity","model":"gemini-4-hydrogen-low"}}),
    method!("swarm.handoff", HandoffRequest, HandoffResponse, false, true, "Transfer caller-issued inputs only; Owner with children, manager in scope never moves them.", {"id":EXAMPLE_ID,"from":"a1","from_generation":1,"to":"a2","to_generation":1,"task_command":"retained-task","commands":[]}),
    method!("command.cancel", CommandCancelRequest, CommandCancelResponse, false, true, "Withdraw one undispatched SEND; Owner for handoff, issuer-manager in scope incl. selector child.", {"id":EXAMPLE_ID,"command_id":"retained-handoff/send-0"}),
    method!("project.transfer", ProjectTransferRequest, ProjectTransferResult, true, true, "Drain, transfer or cancel a registered project manager assignment. Owner only.", {"id":EXAMPLE_ID,"project":"project-1","expected_revision":1,"action":{"kind":"begin"}}),
    method!("project.assignment.get", ProjectAssignmentGetRequest, ProjectAssignment, false, false, "Read a registered project's current manager and epoch.", {"project":"project-1"}),
    method!("project.assignment.claim", ProjectAssignmentClaimRequest, ProjectAssignmentResult, false, true, "Claim a project for an eligible local manager under Owner policy.", {"id":EXAMPLE_ID,"project":"project-1","manager":{"kind":"local","agent":"a1","generation":1}}),
    method!("project.assignment.launch", ProjectAssignmentLaunchRequest, ProjectAssignmentResult, true, true, "Reserve a free project and admit a manager launch in one transaction.", {"id":EXAMPLE_ID,"project":"project-1","profile":"chk-codex"}),
    method!("project.assignment.release", ProjectAssignmentReleaseRequest, ProjectAssignmentResult, false, true, "Release the current manager by expected epoch.", {"id":EXAMPLE_ID,"project":"project-1","expected_epoch":1}),
    method!("project.broadcast", ProjectBroadcastRequest, ProjectBroadcastResponse, false, true, "Queue soonest context for each visible working Agent in a project; Owner or current manager.", {"id":EXAMPLE_ID,"project":"project-1","text":"Please report progress."}),
    method!("project.get", ProjectGetRequest, ProjectRegistrationView, true, false, "Read one approved project registration.", {"id":"project-1"}),
    method!("project.overview", ProjectOverviewRequest, ProjectOverviewResponse, false, false, "Read one project team or every team with workspaces, queue, capacity and whole-fleet totals.", {"project":"project-1"}),
    method!("workspace.list", WorkspaceListRequest, WorkspaceListResponse, true, false, "Page the managed worktrees Swarm created, with total and cursor.", {"limit":100}),
    method!(
        "project.list",
        ProjectListRequest,
        Vec<ProjectRegistrationView>,
        true,
        false,
        "Read approved project registrations, bounded by the daemon page limit.",
        {}
    ),
    method!(
        "project.discover",
        ProjectDiscoverRequest,
        ProjectDiscoverResponse,
        true,
        true,
        "Register an Owner-opened Git checkout; non-Git paths are a no-op.",
        {"workspace":"C:/work/repo"}
    ),
    method!(
        "profile.list",
        ProfileListRequest,
        ProfileListResponse,
        true,
        false,
        "List configured profiles as sanitized summaries with resolvability.",
        {}
    ),
    method!(
        "settings.read",
        SettingsReadRequest,
        SettingsReadResponse,
        true,
        false,
        "Read typed settings sections, revision, restart classification and bounded metadata history.",
        {}
    ),
    method!(
        "settings.save",
        SettingsSaveRequest,
        SettingsSaveResponse,
        true,
        true,
        "Validate (dry_run) or save complete typed settings sections.",
        {"expected_revision":null,"sections":{"routing":{"default":["codex"]}},"dry_run":true,"client":"cli"}
    ),
    method!(
        "routing.preview",
        RoutingPreviewRequest,
        RoutingPreviewResponse,
        true,
        false,
        "Preview the routing decision for a role; advisory only.",
        {"role":"worker"}
    ),
    method!(
        "script.list",
        ScriptListRequest,
        ScriptListResponse,
        true,
        false,
        "Page existing script entries under approved Run roots.",
        {}
    ),
    method!("script.read", ScriptReadRequest, ScriptReadResponse, true, false, "Read one existing script entry through a verified handle.", {"project":null,"path":"check.ps1"}),
    method!("script.save", ScriptSaveRequest, ScriptSaveResponse, true, true, "Validate (dry_run) or replace one existing script entry guarded by its revision; never executes.", {"project":null,"path":"check.ps1","expected_revision":"0000000000000000000000000000000000000000000000000000000000000000","content":"Write-Output ok\n","dry_run":true}),
    method!("rule.list", RuleListRequest, RuleListResponse, true, false, "Page one rule namespace's stored states and timer states.", {"limit":20}),
    method!("rule.get", RuleGetRequest, RuleShowView, true, false, "Read one rule's cursors, latest action outcome and latest refusal.", {"id":"rule-1"}),
    method!("rule.read", RuleReadRequest, RuleReadResponse, true, false, "Read one approved rule source file and its byte revision.", {"path":"notify.toml"}),
    method!("rule.save", RuleSaveRequest, RuleSaveResponse, true, true, "Create or replace a rule source file against its expected revision.", {"path":"notify.toml","expected_revision":null,"content":"[[rule]]\nid = 'notify-finished'\non = { event = 'turn.completed' }\ndo = [{ action = 'notify', message = 'Task done' }]\n"}),
    method!("rule.delete", RuleDeleteRequest, RuleDeleteResponse, true, true, "Remove one rule source file against its expected revision.", {"path":"notify.toml","expected_revision":"0000000000000000000000000000000000000000000000000000000000000000"}),
    method!("rule.catch_up", RuleCatchUpRequest, Command, true, true, "Catch up a rule through a finite retained event interval.", {"id":EXAMPLE_ID,"rule":"rule-1","after_seq":0,"through_seq":10}),
    method!("timer.get", TimerGetRequest, TimerStateView, true, false, "Read one timer's state and next occurrence.", {"id":"timer-1"}),
    method!("timer.run", TimerRunRequest, Command, true, true, "Run one explicit retained timer occurrence.", {"id":EXAMPLE_ID,"rule":"timer-1"}),
    method!("provider.holds", ProviderHoldsRequest, ProviderHoldsResponse, true, false, "Page provider holds without credential values.", {"limit":20}),
    method!("provider.clear_hold", ProviderClearHoldRequest, ProviderClearHoldResponse, true, true, "Record corrective evidence and clear a provider hold.", {"id":EXAMPLE_ID,"scope":"profile:codex","corrective_evidence":"owner_verified","expected_probe_command":null}),
    method!("goal.set", GoalSetRequest, GoalView, true, true, "Set a bounded Eliot Goal; native mode is unsupported and max_cost needs the Agent's native session cost in its unit.", {"id":EXAMPLE_ID,"agent":"a1","definition":{"text":"Complete the assigned task.","check":["git","diff","--exit-code"],"max_rounds":1}}),
    method!("goal.get", GoalGetRequest, GoalView, true, false, "Read the Eliot Goal for one Agent.", {"agent":"a1"}),
    method!("goal.pause", GoalPauseRequest, GoalView, true, true, "Pause an Eliot Goal without replaying an uncertain round.", {"id":EXAMPLE_ID,"agent":"a1","reason":"Owner pause."}),
    method!("goal.clear", GoalClearRequest, GoalView, true, true, "Clear one Eliot Goal.", {"id":EXAMPLE_ID,"agent":"a1"}),
    method!("goal.resume", GoalResumeRequest, GoalView, true, true, "Resume a paused Eliot Goal.", {"id":EXAMPLE_ID,"agent":"a1"}),
    method!("goal.round", GoalRoundRequest, GoalRoundResponse, true, true, "Request one exact idle-event or cadence-slot Goal round.", {"id":EXAMPLE_ID,"agent":"a1","idle_seq":1}),
    method!("mail.send", MailSendRequest, MailSendResponse, false, true, "Queue addressed mail to 1..100 concrete Agents; acknowledgement is separate.", {"id":EXAMPLE_ID,"to":["a1"],"mode":"queue","text":"Please report your progress."}),
    method!("mail.inbox", MailInboxRequest, MailInboxResponse, false, false, "Page unread mail, long-polling up to 30 s with wait_ms; reading does not acknowledge.", {"limit":20,"wait_ms":30000}),
    method!("mail.read", MailReadRequest, MailDelivery, false, false, "Read one exact delivery id without acknowledging it.", {"delivery_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV/a1"}),
    method!("mail.ack", MailAckRequest, MailAckResponse, false, true, "Acknowledge 1..100 exact delivery ids in your mailbox.", {"id":EXAMPLE_ID,"deliveries":["previous-write/a1"]}),
    method!("var.set", VarSetRequest, VarSetResponse, false, true, "Set a shared variable, optionally using compare-and-swap.", {"id":EXAMPLE_ID,"key":"progress","value":"working","expect_version":0}),
    method!("var.get", VarGetRequest, VarGetResponse, false, false, "Read a shared variable or wait for a version after after_version; absent keys are version 0.", {"key":"progress"}),
    method!(
        "who",
        WhoRequest,
        WhoResponse,
        false,
        false,
        "Read advisory Git/workspace attribution and work claims.",
        {}
    ),
    method!("claim.set", ClaimSetRequest, ClaimSetResponse, false, true, "Set an expiring advisory work claim for an Agent.", {"id":EXAMPLE_ID,"glob":"src/**"}),
    method!(
        "claim.list",
        ClaimListRequest,
        ClaimListResponse,
        false,
        false,
        "List bounded current work claims.",
        {}
    ),
    method!("claim.release", ClaimReleaseRequest, ClaimReleaseResponse, false, true, "Release one advisory work claim.", {"id":EXAMPLE_ID,"glob":"src/**"}),
];

static MUTATION_ID_METHODS: OnceLock<Vec<&'static str>> = OnceLock::new();

/// Canonical names of mutation methods whose request DTO carries a
/// caller-retained `id`. Derived from this module's descriptors (mutation
/// flag plus an `id` property in the request schema); MCP id minting follows
/// this list so the two can never drift.
pub fn mutation_id_methods() -> &'static [&'static str] {
    MUTATION_ID_METHODS.get_or_init(|| {
        METHODS
            .iter()
            .filter(|method| method.mutation && request_takes_id(method))
            .map(|method| method.name)
            .collect()
    })
}

/// Whether `name` is a mutation method whose request DTO carries a
/// caller-retained `id`. Same derivation as [`mutation_id_methods`].
pub fn is_mutation_id_method(name: &str) -> bool {
    mutation_id_methods().contains(&name)
}

pub(super) fn is_mutation(name: &str) -> bool {
    METHODS
        .iter()
        .any(|method| method.name == name && method.mutation)
}

pub(super) fn canonical_method(name: &str) -> Option<&'static str> {
    METHODS
        .iter()
        .find(|method| method.name == name)
        .map(|method| method.name)
}

fn request_takes_id(method: &Method) -> bool {
    (method.input)()
        .get("properties")
        .and_then(|properties| properties.get("id"))
        .is_some()
}

fn method_suggestions(requested: &str, principal: &Principal) -> Vec<&'static str> {
    if requested.len() > 128 {
        return Vec::new();
    }
    let normalized = requested.strip_prefix("swarm.").unwrap_or(requested);
    let mut candidates = METHODS
        .iter()
        .filter(|method| visible(method, principal))
        .filter_map(|method| {
            let distance =
                edit_distance(normalized, method.name).min(edit_distance(requested, method.name));
            (distance <= 3).then_some((distance, method.name))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(distance, name)| (*distance, *name));
    candidates
        .into_iter()
        .take(3)
        .map(|(_, name)| name)
        .collect()
}

pub(super) fn unavailable_method(requested: &str, principal: &Principal) -> Error {
    let suggestions = method_suggestions(requested, principal);
    let message = if suggestions.is_empty() {
        "Method unavailable. Use api.methods for callable names.".to_owned()
    } else {
        format!(
            "Method unavailable. Did you mean {}?",
            suggestions.join(", ")
        )
    };
    Error::new("unsupported", message)
}

fn edit_distance(left: &str, right: &str) -> usize {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    for (row, left_char) in left.iter().enumerate() {
        let mut current = Vec::with_capacity(right.len() + 1);
        current.push(row + 1);
        for (column, right_char) in right.iter().enumerate() {
            let substitution = previous[column] + if left_char == right_char { 0 } else { 1 };
            current.push(
                (current[column] + 1)
                    .min(previous[column + 1] + 1)
                    .min(substitution),
            );
        }
        previous = current;
    }
    previous[right.len()]
}

pub(super) fn owner_only_read(name: &str) -> bool {
    METHODS
        .iter()
        .any(|method| method.name == name && method.owner_only && !method.mutation)
}

fn visible(method: &Method, principal: &Principal) -> bool {
    if method.name == "child.run" && !matches!(principal, Principal::Agent { .. }) {
        return false;
    }
    // Discovery lists callable methods, so it shares the RPC gate's own
    // principal/method classification: Memory-only entries such as
    // `client.identity` and `inference.*` (except `inference.client.*`) are
    // hidden from Owner exactly as invoking them is rejected.
    if super::auth::authorize_rpc_method(principal, method.name).is_err() {
        return false;
    }
    match principal {
        Principal::Owner => true,
        Principal::Agent { .. } => !method.owner_only,
        // These principals have no discovery RPC authority. Keep the helper
        // conservative even when used internally; do not broaden their gate.
        Principal::Run { .. }
        | Principal::ProjectRule { .. }
        | Principal::MemoryInference { .. }
        | Principal::Swarm(_) => false,
    }
}

impl Router {
    pub(super) fn profile_command_issues(
        &self,
        missing: Vec<(String, String, String)>,
        installations: &[(&str, Value)],
    ) -> Vec<ProfileCommandIssue> {
        use eliot_core::api::harness::HarnessCandidate;

        let policy = self.secret_policy_snapshot();
        missing
            .into_iter()
            .filter_map(|(profile, harness, configured_cmd)| {
                let replacement = installations
                    .iter()
                    .find(|(name, _)| *name == harness)
                    .and_then(|(_, installation)| {
                        let cmd = installation.get("cmd")?.as_str()?.to_owned();
                        let version = installation
                            .get("version")
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                        Some(HarnessCandidate { cmd, version })
                    })
                    .filter(|candidate| !policy.contains_value(&json!(candidate)));
                let issue = ProfileCommandIssue {
                    profile,
                    harness,
                    configured_cmd,
                    replacement,
                };
                let data = serde_json::to_value(&issue).ok()?;
                (issue.validate().is_ok() && !policy.contains_value(&data)).then_some(issue)
            })
            .take(eliot_core::api::MAX_LISTED_PROFILES)
            .collect()
    }

    pub(super) async fn methods(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MethodsRequest = decode_params(params.clone())?;
        request.validate()?;
        let query = request.query.as_deref().unwrap_or("");
        let mut selected: Vec<_> = METHODS
            .iter()
            .filter(|m| visible(m, principal))
            .filter(|m| m.name.contains(query) || m.summary.contains(query))
            .filter(|m| request.after.as_deref().is_none_or(|after| m.name > after))
            .collect();
        selected.sort_by_key(|m| m.name);
        let limit = request.limit.unwrap_or(20);
        let more = selected.len() > limit;
        selected.truncate(limit);
        let next = more.then(|| {
            selected
                .last()
                .expect("positive page limit")
                .name
                .to_owned()
        });
        encode(MethodsResponse {
            methods: selected
                .into_iter()
                .map(|m| MethodSummary {
                    method: m.name.to_owned(),
                    summary: request.full.then(|| m.summary.to_owned()),
                })
                .collect(),
            next,
        })
    }

    async fn identity(&self, principal: &Principal) -> Result<IdentityResponse> {
        match principal {
            Principal::Owner => Ok(IdentityResponse {
                principal:"owner".to_owned(), agent_id:None, generation:None,
                allowed_scopes:vec!["global".to_owned(), "project:<name>".to_owned(), "mailbox:<agent>".to_owned()],
                limits:"Owner authority; native actions still require the target generation's advertised capabilities.".to_owned(),
            }),
            Principal::Agent { id, generation } => {
                let agent = self.inner.store.lock().await.get_agent(id)?
                    .ok_or_else(|| Error::new("not_found", "authenticated Agent no longer exists"))?;
                Ok(IdentityResponse {
                    principal:"agent".to_owned(), agent_id:Some(id.clone()), generation:Some(*generation),
                    allowed_scopes:vec!["global".to_owned(), format!("project:{}", agent.project_scope()), format!("mailbox:{id}")],
                    limits:"Variables default to your project. Global variables are read-only for Agents; other-project variables reject. agent.list follows your frozen control scope (self, subtree, or project); event visibility is self-scoped. Mail send addresses concrete Agents; mailbox reads/acks are self-only. Agent control/read visibility and approved child profiles remain checked at the existing authority boundaries.".to_owned(),
                })
            }
            _ => Err(Error::new("not_allowed", "principal has no discovery authority")),
        }
    }

    pub(super) async fn caller_identity(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let _: IdentityRequest = decode_params(params.clone())?;
        encode(self.identity(principal).await?)
    }

    pub(super) async fn method_help(&self, params: &Value, principal: &Principal) -> Result<Value> {
        let request: MethodHelpRequest = decode_params(params.clone())?;
        request.validate()?;
        let Some(method) = METHODS
            .iter()
            .find(|m| m.name == request.method && visible(m, principal))
        else {
            return Err(unavailable_method(&request.method, principal));
        };
        let full = request.full.unwrap_or(false);
        let identity = self.identity(principal).await?;
        let aliases: &[&str] = match method.name {
            "api.identity" => &["swarm identity"],
            "project.transfer" => &["swarm project transfer"],
            "project.assignment.get" => &["swarm project assignment get"],
            "project.assignment.claim" => &["swarm project assignment claim"],
            "project.assignment.launch" => &["swarm project assignment launch"],
            "project.assignment.release" => &["swarm project assignment release"],
            "project.broadcast" => &["swarm project broadcast"],
            "who" => &["swarm who"],
            "claim.set" => &["swarm claim set"],
            "claim.list" => &["swarm claim list"],
            "claim.release" => &["swarm claim release"],
            "mail.send" => &["swarm msg send"],
            "mail.inbox" => &["swarm msg inbox", "swarm inbox (Agent)"],
            "owner.inbox" => &["swarm inbox"],
            "checkpoint.restore" => &["swarm checkpoint restore"],
            "owner.decide.bulk" => &["swarm inbox apply"],
            "mail.read" => &["swarm msg show"],
            "mail.ack" => &["swarm msg ack"],
            "var.set" => &["swarm var set"],
            "var.get" => &["swarm var get"],
            "swarm.wait" => &["swarm wait"],
            "swarm.digest" => &["swarm digest"],
            "swarm.reparent" => &["swarm reparent"],
            "swarm.handoff" => &["swarm handoff"],
            "swarm.switch" => &["swarm switch"],
            "chat.organization" => &["swarm chat organization"],
            "chat.attach" => &["swarm chat attach"],
            "chat.detach" => &["swarm chat detach"],
            "chat.tag" => &["swarm chat tag"],
            "command.cancel" => &["swarm cmd cancel"],
            "rule.list" => &["swarm rule list"],
            "rule.read" => &["swarm rule read"],
            "rule.save" => &["swarm rule save"],
            "rule.delete" => &["swarm rule delete"],
            _ => &[],
        };
        let mut unsupported = vec!["A method listing grants no additional authority. Unsupported native verbs reject; inspect agent.get for the exact target generation's capabilities.".to_owned()];
        if method.name.starts_with("mail.") {
            unsupported.push(
                "mail.send modes send, steer and inject are unsupported; only queue is connected."
                    .to_owned(),
            );
        }
        if method.name.starts_with("var.") {
            unsupported.push("tree variable scopes are unsupported.".to_owned());
        }
        if method.name.starts_with("goal.") {
            unsupported.push("native Goal mode is unsupported.".to_owned());
        }
        let mut example = (method.example)();
        // Examples use the caller's actual Agent where a concrete self target
        // is useful. IDs for retained resources must be replaced by a real id.
        if let Some(id) = identity.agent_id.as_deref()
            && method.name == "mail.send"
        {
            example["to"] = json!([id]);
        }
        if matches!(principal, Principal::Owner) && method.name.starts_with("var.") {
            example["scope"] = json!("global");
        }
        if matches!(principal, Principal::Owner)
            && matches!(
                method.name,
                "mail.inbox" | "mail.read" | "mail.ack" | "claim.set" | "claim.release"
            )
        {
            example["agent"] = json!("a1");
        }
        let scopes = if full {
            method_scopes(method.name, principal, &identity)
        } else {
            Vec::new()
        };
        // Compact by default: compact field descriptors (requiredness,
        // types, enums, nested shapes and documented constraints) stand in
        // for the full JSON Schemas (fields, one example, limits).
        // `full: true` restores both schemas for callers that need exact
        // shapes.
        let fields: Vec<String> = compact_field_descriptors(&(method.input)());
        let (request_schema, response_schema) = if full {
            (Some((method.input)()), Some((method.output)()))
        } else {
            (None, None)
        };
        encode(MethodHelpResponse {
            method: method.name.to_owned(),
            summary: method.summary.to_owned(),
            cli_aliases: if full {
                aliases.iter().map(|s| (*s).to_owned()).collect()
            } else {
                Vec::new()
            },
            fields,
            request_schema,
            response_schema,
            example: json!({"method":method.name,"params":example}),
            mutation_id: if full || method.name.starts_with("inference.") {
                mutation_id_help(method).to_owned()
            } else if is_mutation_id_method(method.name) {
                "Direct callers supply a fresh ULID in id. MCP can mint one. Reuse the same id for exact retries.".to_owned()
            } else {
                String::new()
            },
            scopes,
            limits: method_limits(method.name).to_owned(),
            unsupported: if full { unsupported } else { Vec::new() },
        })
    }
}

fn mutation_id_help(method: &Method) -> &'static str {
    if !method.mutation {
        "Read-only operation."
    } else if !is_mutation_id_method(method.name) {
        "The expected revision guards this edit. Read back the file after an uncertain result."
    } else if method.name.starts_with("inference.") {
        "Supply a caller-retained literal ULID before send; do not omit it. After a lost reply retry with that same id and identical semantic payload, or read inference.write.get. Never allocate another id to recover an uncertain mutation."
    } else {
        "id is optional on MCP: omit it and the server mints a ULID and returns it in the reply; retry with that same id. Otherwise supply a fresh caller-retained ULID in id before the first write. Retry only with that same id, principal, method and identical semantic payload. Never mint another id after an uncertain outcome; read back the retained result. The example id is illustrative; replace it for a new write."
    }
}

/// Compact field descriptors for a request DTO schema: one entry per
/// top-level property, sorted by name, each carrying the field name,
/// requiredness, type, enum values, nested shape and documented
/// constraints, derived from the schemars DTO schema (draft 2020-12).
/// Compact help carries these instead of the full JSON Schemas; `full:
/// true` restores both schemas for callers that need exact shapes.
fn compact_field_descriptors(schema: &Value) -> Vec<String> {
    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        return Vec::new();
    };
    let mut names: Vec<&String> = properties.keys().collect();
    names.sort();
    names
        .iter()
        .map(|name| describe_field(schema, properties, name))
        .collect()
}

/// One `name (required|optional): type [bounds] - documented constraints`
/// descriptor. Descriptions collapse to one line; nested objects render
/// their property names with requiredness marks one level deep.
fn describe_field(root: &Value, properties: &serde_json::Map<String, Value>, name: &str) -> String {
    let required = root
        .get("required")
        .and_then(Value::as_array)
        .is_some_and(|required| required.iter().any(|entry| entry.as_str() == Some(name)));
    let property = properties.get(name);
    // Types resolve through `$ref`, but documented constraints live on the
    // property beside the reference (schemars emits `description` there),
    // so descriptions and bounds prefer the property and fall back to the
    // resolved target.
    let node = property.map(|node| resolve_ref(root, node));
    let mut descriptor = format!(
        "{name} ({}): {}",
        if required { "required" } else { "optional" },
        node.map(|node| describe_type(root, node, 0))
            .unwrap_or_else(|| "any".to_owned())
    );
    let bounds = property
        .and_then(describe_bounds)
        .or_else(|| node.and_then(describe_bounds));
    if let Some(bounds) = bounds {
        descriptor.push_str(&format!(" [{bounds}]"));
    }
    let description = property
        .and_then(|node| node.get("description"))
        .and_then(Value::as_str)
        .or_else(|| {
            node.and_then(|node| node.get("description"))
                .and_then(Value::as_str)
        })
        .map(single_line)
        .filter(|description| !description.is_empty());
    if let Some(description) = description {
        descriptor.push_str(&format!(" - {description}"));
    }
    descriptor
}

/// Follow a local `$ref` (JSON pointer under `#`) to its target, up to a
/// small depth. An unresolvable reference returns the node itself.
fn resolve_ref<'a>(root: &'a Value, node: &'a Value) -> &'a Value {
    let mut current = node;
    for _ in 0..8 {
        let Some(target) = current.get("$ref").and_then(Value::as_str) else {
            return current;
        };
        let Some(path) = target.strip_prefix('#') else {
            return current;
        };
        let mut resolved = root;
        let mut ok = true;
        for segment in path.split('/').filter(|segment| !segment.is_empty()) {
            let segment = segment.replace("~1", "/").replace("~0", "~");
            match resolved.get(&segment) {
                Some(next) => resolved = next,
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            return current;
        }
        current = resolved;
    }
    current
}

/// A readable type for one schema node: enums render their closed value
/// set, unions join their branches (explicit null branches are dropped:
/// the required marker already says whether the field may be omitted),
/// arrays name their item type and objects name their nested properties.
fn describe_type(root: &Value, node: &Value, depth: usize) -> String {
    let node = resolve_ref(root, node);
    if let Some(values) = node.get("enum").and_then(Value::as_array) {
        let mut rendered: Vec<String> = values.iter().map(render_enum_value).collect();
        rendered.sort();
        rendered.dedup();
        return format!("enum({})", rendered.join("|"));
    }
    if node.get("const").is_some() {
        return format!("const({})", node.get("const").expect("const value"));
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(branches) = node.get(key).and_then(Value::as_array) {
            let mut parts = Vec::new();
            for branch in branches {
                let branch = resolve_ref(root, branch);
                if branch.get("type").and_then(Value::as_str) == Some("null")
                    || branch == &Value::Bool(false)
                {
                    continue;
                }
                if branch == &Value::Bool(true) {
                    parts.push("any".to_owned());
                    continue;
                }
                parts.push(describe_type(root, branch, depth + 1));
            }
            parts.sort();
            parts.dedup();
            if parts.is_empty() {
                return "null".to_owned();
            }
            if parts.len() == 1 {
                return parts.pop().expect("single branch");
            }
            return parts.join("|");
        }
    }
    let mut names = Vec::new();
    match node.get("type") {
        Some(Value::String(name)) if name != "null" => names.push(name.clone()),
        Some(Value::Array(list)) => {
            for name in list.iter().filter_map(Value::as_str) {
                if name != "null" {
                    names.push(name.to_owned());
                }
            }
        }
        _ => {}
    }
    if names.is_empty() {
        if node.get("properties").and_then(Value::as_object).is_some() {
            names.push("object".to_owned());
        } else if node.get("items").is_some() || node.get("prefixItems").is_some() {
            names.push("array".to_owned());
        } else {
            return "any".to_owned();
        }
    }
    let mut parts = Vec::new();
    for name in names {
        parts.push(match name.as_str() {
            "array" => format!("array<{}>", describe_items(root, node, depth)),
            "object" => describe_object_shape(root, node),
            other => other.to_owned(),
        });
    }
    parts.sort();
    parts.dedup();
    parts.join("|")
}

/// Item type for one array node: `prefixItems` renders as a tuple, a
/// missing items schema renders as `any`.
fn describe_items(root: &Value, node: &Value, depth: usize) -> String {
    if depth > 3 {
        return "any".to_owned();
    }
    if let Some(prefix) = node.get("prefixItems").and_then(Value::as_array) {
        let mut parts: Vec<String> = prefix
            .iter()
            .map(|item| describe_type(root, item, depth + 1))
            .collect();
        parts.sort();
        return format!("tuple({})", parts.join(", "));
    }
    match node.get("items") {
        None | Some(Value::Bool(false)) => "any".to_owned(),
        Some(Value::Bool(true)) => "any".to_owned(),
        Some(items) => describe_type(root, items, depth + 1),
    }
}

/// Nested shape for one object node: `{a, b*}` property names with
/// requiredness marks, one level deep.
fn describe_object_shape(root: &Value, node: &Value) -> String {
    let node = resolve_ref(root, node);
    let Some(properties) = node.get("properties").and_then(Value::as_object) else {
        return "object".to_owned();
    };
    let mut names: Vec<String> = properties.keys().cloned().collect();
    names.sort();
    for name in names.iter_mut() {
        let nested = node
            .get("required")
            .and_then(Value::as_array)
            .is_some_and(|required| required.iter().any(|entry| entry.as_str() == Some(name)));
        if nested {
            *name = format!("{name}*");
        }
    }
    format!("object{{{}}}", names.join(", "))
}

/// Structured bounds carried by one schema node: string/array/numeric
/// ranges, Eliot byte budgets, uniqueness, format and pattern. Present
/// keys render verbatim; absent keys stay absent.
fn describe_bounds(node: &Value) -> Option<String> {
    let integer = |key: &str| node.get(key).and_then(Value::as_i64);
    let mut bounds = Vec::new();
    match (integer("minLength"), integer("maxLength")) {
        (Some(min), Some(max)) => bounds.push(format!("len {min}..{max}")),
        (Some(min), None) => bounds.push(format!("len {min}..")),
        (None, Some(max)) => bounds.push(format!("len ..{max}")),
        (None, None) => {}
    }
    match (integer("minItems"), integer("maxItems")) {
        (Some(min), Some(max)) => bounds.push(format!("{min}..{max} items")),
        (Some(min), None) => bounds.push(format!("{min}.. items")),
        (None, Some(max)) => bounds.push(format!("..{max} items")),
        (None, None) => {}
    }
    match (node.get("minimum"), node.get("maximum")) {
        (Some(min), Some(max)) => bounds.push(format!("{min}..{max}")),
        (Some(min), None) => bounds.push(format!("{min}..")),
        (None, Some(max)) => bounds.push(format!("..{max}")),
        (None, None) => {}
    }
    if let Some(max) = integer("x-maxBytes") {
        bounds.push(format!("<={max}B"));
    }
    if let Some(min) = integer("x-minBytes") {
        bounds.push(format!(">={min}B"));
    }
    if node.get("uniqueItems") == Some(&Value::Bool(true)) {
        bounds.push("unique".to_owned());
    }
    if let Some(format) = node.get("format").and_then(Value::as_str) {
        bounds.push(format!("fmt={format}"));
    }
    if let Some(pattern) = node.get("pattern").and_then(Value::as_str) {
        bounds.push(format!("re={pattern}"));
    }
    if bounds.is_empty() {
        None
    } else {
        Some(bounds.join(", "))
    }
}

/// One enum value as compact JSON.
fn render_enum_value(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Collapse a documented constraint to one line for the descriptor.
fn single_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}
fn method_scopes(name: &str, principal: &Principal, identity: &IdentityResponse) -> Vec<String> {
    identity
        .allowed_scopes
        .iter()
        .filter(|scope| match name {
            "var.get" => scope.as_str() == "global" || scope.starts_with("project:"),
            "var.set" => {
                scope.starts_with("project:")
                    || (scope.as_str() == "global" && matches!(principal, Principal::Owner))
            }
            "mail.inbox" | "mail.read" | "mail.ack" => scope.starts_with("mailbox:"),
            // Native/process/provider selectors are described by their DTO fields;
            // they do not select one of the caller's coordination authority scopes.
            _ => false,
        })
        .cloned()
        .collect()
}

fn method_limits(name: &str) -> &'static str {
    match name {
        "owner.inbox" => {
            "Owner only. Page at most 100 current items. Continue with next, when present."
        }
        "checkpoint.restore" => {
            "Owner only. Restore only the exact retained Agent-generation checkpoint into a clean worktree at the checkpoint's original HEAD and Git common directory. Refuses an active Agent workspace, dirty target, secret path/content, changed HEAD or repository mismatch."
        }
        "owner.decide" => {
            "Owner only. Use the displayed kind, subject and revision with an offered choice, recommended, or snooze. Arguments fill only the chosen action's input fields."
        }
        "owner.decide.bulk" => {
            "Owner only. Apply recommended actions for 1 to 100 explicit refs, including each displayed revision. The batch ID binds the exact ordered refs; each item uses a deterministic owner.decide ID. Changed or unavailable recommendations are skipped, while per-item failures preserve their error and uncertainty."
        }
        "chat.attach" | "chat.detach" | "chat.tag" => {
            "Owner only. Placement and tags only; execution scope is untouched. At most 16 Agents per call. Retry the same id for the retained readback."
        }
        "chat.organization" => {
            "Owner only. Ready grouped projection with event-watermark readback; no client-side Git discovery."
        }
        "swarm.switch" => {
            "Owner anywhere, or a manager for a worker in its controlled scope, choosing a profile or role in the worker's worktree with authority equal or narrower. Exact source generation; at most 64 selected plain SENDs. Reserve child IDs before launch preparation. Choose profile, role or ad-hoc harness/model/effort/auth/cmd. No launch prompt. launch.project continues a chat in a registered project: B runs in the target standard worktree while retaining A's authorization scope, and B's organization link records continued_from. Exact session readiness precedes atomic H2 handoff. Failure retains created B; stop separately. Same-ID replay never launches again. Finish an untransferred ready B with ordinary swarm.handoff."
        }
        "swarm.handoff" => {
            "Owner anywhere, or a manager between two workers (never itself) in its own controlled scope for caller-issued inputs only. Distinct existing Agents in the same project; exact generations and retained task SEND required. At most 64 selected plain accepted/queued SENDs and configured selector bound. A quiescent, B cleanly idle. Owner transfers all live claims, a quiescent Goal (paused), and all children atomically; Agent handoffs never move children, claims or Goals. Brief first; failed/unknown brief holds successors queued until resolution or cancellation. No native resend on retry."
        }
        "command.cancel" => {
            "Owner withdraws accepted/queued SEND children of swarm.handoff; a manager cancels a SEND it issued itself while accepted/queued in its controlled scope (handoff child, ordinary SEND or own selector SEND child). Dispatched or unknown inputs cannot be cancelled. Retry the same id and command_id."
        }
        "swarm.reparent" => {
            "Owner only. Name exactly one of agent or children_of; source and parent generations are required. Same project, controlled nonterminal destination, no observe-only moved rows or cycles. Frozen profiles stay unchanged. Configured hierarchy and encoded retained-result bounds apply atomically. Retry the identical payload with the same id."
        }
        "mail.send" => {
            "Queue only. At most 100 distinct recipients, 32768 body bytes. Recipient generation is frozen at admission; configured unread mailbox limits may reject the entire fanout. Sender identity is inherited."
        }
        "mail.inbox" => {
            "At most 100 deliveries per page; Owner must supply agent, Agents may omit for self. Reading never acknowledges."
        }
        "mail.read" | "mail.ack" => {
            "Owner must supply agent, Agents may omit for self. Acknowledge at most 100 distinct exact delivery ids."
        }
        "var.set" | "var.get" => {
            "Scopes: global or project:<name>; Owner must specify scope, Agent omission selects its actual project. Only Owner writes global; Agents can read global. Key: 256 bytes; value: 16384 encoded JSON bytes; note: 1024 bytes. CAS uses expect_version; an absent key has version zero."
        }
        "agent.discover" | "agent.history" => {
            "Metadata/history only; native support and view availability are capability-dependent. History views are subject, turns or items. No Agent is created; hook callbacks are private plugin traffic."
        }
        "command.submit" => {
            "args is the verb-specific DTO object. Inspect agent.get for the exact target generation's native capabilities; unsupported operations reject. Turn/request-sensitive operations require exact bindings. CLI wait is client-side, not an agent.wait method. Agent child launch (verb launch with parent): omit profile_snapshot (the daemon freezes it from the named profile); omit profile to reuse the parent snapshot verbatim or name an approved child_profiles entry (same harness allows its own model, effort, and context ceiling with inherited native authority); workspace defaults to the parent workspace (a different workspace rejects); optional worktree creates a managed branch in the same repository, with base defaulting to the parent HEAD and limited to its branch history; parent defaults to self; project/control/capacity inherited. The Owner may override project Goal defaults individually. Other callers may only tighten an inherited policy. Executable check overrides and escalation require the Owner. args.no_policy records a reason and skips both the policy Goal and the fast check."
        }
        "events.read" | "events.subscribe" => {
            "At most 1000 events per page and the encoded event-page byte budget; subscribe waits at most 30000 ms. Agent events remain self-scoped. History gaps do not authorize replay."
        }
        "rule.read" | "rule.save" | "rule.delete" => {
            "Owner only. Use a top-level .toml filename in the global or registered project rule root. A null expected revision on save means create only. Replace and delete require the revision from rule.read."
        }
        "goal.set" | "goal.round" => {
            "Eliot mode only. Automatic idle driving requires max_turns or max_rounds unless a cadence is set. A round names exactly one idle_seq or slot; unknown effects are not replayed."
        }
        "claim.set" | "claim.list" | "claim.release" | "who" => {
            "Advisory attribution only; claims grant no write authority. Owner must select the subject Agent for claim writes; Agents act as self. Results retain configured claim/attribution bounds."
        }
        _ => {
            "Live generation, visibility, retained-resource existence and native capability checks remain at the existing transaction/effect boundaries."
        }
    }
}

fn encode(value: impl serde::Serialize) -> Result<Value> {
    let value = serde_json::to_value(value)
        .map_err(|_| Error::new("internal", "discovery response could not be encoded"))?;
    if serde_json::to_vec(&value).map_or(true, |b| b.len() > MAX_METHOD_SCHEMA_BYTES) {
        return Err(Error::new(
            "frame_too_large",
            "method help exceeds its 256 KiB response bound",
        ));
    }
    Ok(value)
}
