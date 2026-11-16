//! Descriptions beside Router registration. Wire shapes belong to Core DTOs.
use super::{Principal, Router};
use eliot_core::api::inference::*;
use eliot_core::{
    Command, Error, Result, Turn,
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
    #[cfg(test)]
    decode: fn(Value) -> Result<()>,
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
            #[cfg(test)]
            decode: |params| {
                let request: $request = decode_params(params)?;
                request.validate()
            },
        }
    };
}

// Every Router match arm is described here. The coverage regression below makes
// omission deliberate: new methods must be described before they can ship.
const METHODS: &[Method] = &[
    method!("artifact.register", ArtifactRegisterRequest, ArtifactView, false, true, "Register an existing Agent-owned artifact directory with exact generation and opened identity.", {"id":EXAMPLE_ID,"path":"C:/scratch/build","kind":"build","purpose":"assignment build"}),
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
    method!("api.help", MethodHelpRequest, MethodHelpResponse, false, false, "Read a method's DTO schemas, example and limits.", {"method":"mail.send"}),
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
    #[cfg(debug_assertions)]
    method!(
        "daemon.panic_test",
        DaemonPanicTestRequest,
        DaemonPanicTestResponse,
        true,
        false,
        "Debug builds only: force a daemon panic to test crash capture (Owner).",
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
    method!("command.submit", CommandSubmitRequest, CommandSubmitResponse, false, true, "Submit a command with launch/send/wait child verbs.", {"id":EXAMPLE_ID,"verb":"send","target":"a1","args":{"text":"Report current progress."}}),
    method!("command.get", CommandGetRequest, Command, false, false, "Read a retained command in your authorized scope.", {"id":EXAMPLE_ID}),
    method!("command.children", CommandChildrenRequest, CommandChildrenResponse, false, false, "Page the retained children of one command.", {"id":EXAMPLE_ID,"limit":20}),
    method!("command.resolve", CommandResolveRequest, CommandResolveResponse, true, true, "Record an owner resolution of an uncertain command; never replay its effect.", {"id":EXAMPLE_ID,"command_id":"previous-command","as":"failed","reason":"Owner verified failure."}),
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
    method!("agent.result", AgentResultRequest, AgentResultResponse, false, false, "Read retained turns/results for one Agent.", {"id":"a1","limit":20}),
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
        "Read installed-vs-verified harness versions and contract markers without launching.",
        {}
    ),
    method!("usage.summary", UsageSummaryRequest, UsageSummaryResponse, false, false, "Read bounded usage totals over one Agent, project or tree scope.", {"agent":"a1"}),
    method!("agent.git", AgentGitRequest, AgentGitResponse, false, false, "Read Git observations for one Agent's workspace.", {"id":"a1"}),
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
    method!("swarm.wait", SwarmWaitRequest, SwarmWaitResponse, false, false, "Wait up to 30 seconds for a frozen set of child commands/turns with all/any semantics.", {"commands":[EXAMPLE_ID],"turns":[],"mode":"all","wait_ms":1000}),
    method!("swarm.digest", SwarmDigestRequest, SwarmDigestResponse, false, true, "Register a durable completion digest with the standard mail wake.", {"id":EXAMPLE_ID,"to":"a1","mode":"all","commands":[EXAMPLE_ID],"turns":[]}),
    method!("swarm.reparent", ReparentRequest, ReparentResponse, true, true, "Move one Agent subtree or all children to a controlled manager; Owner only.", {"id":EXAMPLE_ID,"agent":"a2","expected_generation":1,"parent":"a3","parent_generation":1}),
    method!("swarm.switch", SwitchRequest, SwitchResponse, true, true, "Launch a new Agent session, then atomically hand off to it; Owner only.", {"id":EXAMPLE_ID,"from":"a1","from_generation":1,"task_command":"retained-task","commands":[],"launch":{"harness":"antigravity","model":"gemini-4-hydrogen-low"}}),
    method!("swarm.handoff", HandoffRequest, HandoffResponse, true, true, "Transfer selected undispatched inputs and children to an Agent; Owner only.", {"id":EXAMPLE_ID,"from":"a1","from_generation":1,"to":"a2","to_generation":1,"task_command":"retained-task","commands":[]}),
    method!("command.cancel", CommandCancelRequest, CommandCancelResponse, true, true, "Withdraw one undispatched handoff input with no native effect; Owner only.", {"id":EXAMPLE_ID,"command_id":"retained-handoff/send-0"}),
    method!("project.get", ProjectGetRequest, ProjectRegistrationView, true, false, "Read one approved project registration.", {"id":"project-1"}),
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
        "config.profiles.read",
        ProfilesReadRequest,
        ProfilesReadResponse,
        true,
        false,
        "Read the verbatim [profiles.*] sections and the file revision.",
        {}
    ),
    method!("config.profiles.save", ProfilesSaveRequest, ProfilesSaveResponse, true, true, "Validate (dry_run) or replace the [profiles.*] sections, guarded by revision.", {"expected_revision":null,"source":"[profiles.codex]
harness = \"codex\"
model = \"gpt-6-luna\"
","dry_run":true}),
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
    method!("rule.get", RuleGetRequest, RuleStateView, true, false, "Read one rule's current and retiring cursors.", {"id":"rule-1"}),
    method!("rule.catch_up", RuleCatchUpRequest, Command, true, true, "Catch up a rule through a finite retained event interval.", {"id":EXAMPLE_ID,"rule":"rule-1","after_seq":0,"through_seq":10}),
    method!("timer.get", TimerGetRequest, TimerStateView, true, false, "Read one timer's state and next occurrence.", {"id":"timer-1"}),
    method!("timer.run", TimerRunRequest, Command, true, true, "Run one explicit retained timer occurrence.", {"id":EXAMPLE_ID,"rule":"timer-1"}),
    method!("provider.holds", ProviderHoldsRequest, ProviderHoldsResponse, true, false, "Page provider holds without credential values.", {"limit":20}),
    method!("provider.clear_hold", ProviderClearHoldRequest, ProviderClearHoldResponse, true, true, "Record corrective evidence and clear a provider hold.", {"id":EXAMPLE_ID,"scope":"profile:codex","corrective_evidence":"owner_verified","expected_probe_command":null}),
    method!("goal.set", GoalSetRequest, GoalView, true, true, "Set a bounded Eliot Goal; native mode and cost limits are unsupported.", {"id":EXAMPLE_ID,"agent":"a1","definition":{"text":"Complete the assigned task.","check":["git","diff","--exit-code"],"max_rounds":1}}),
    method!("goal.get", GoalGetRequest, GoalView, true, false, "Read the Eliot Goal for one Agent.", {"agent":"a1"}),
    method!("goal.pause", GoalPauseRequest, GoalView, true, true, "Pause an Eliot Goal without replaying an uncertain round.", {"id":EXAMPLE_ID,"agent":"a1","reason":"Owner pause."}),
    method!("goal.clear", GoalClearRequest, GoalView, true, true, "Clear one Eliot Goal.", {"id":EXAMPLE_ID,"agent":"a1"}),
    method!("goal.resume", GoalResumeRequest, GoalView, true, true, "Resume a paused Eliot Goal.", {"id":EXAMPLE_ID,"agent":"a1"}),
    method!("goal.round", GoalRoundRequest, GoalRoundResponse, true, true, "Request one exact idle-event or cadence-slot Goal round.", {"id":EXAMPLE_ID,"agent":"a1","idle_seq":1}),
    method!("mail.send", MailSendRequest, MailSendResponse, false, true, "Queue addressed mail to 1..100 concrete Agents; acknowledgement is separate.", {"id":EXAMPLE_ID,"to":["a1"],"mode":"queue","text":"Please report your progress."}),
    method!("mail.inbox", MailInboxRequest, MailInboxResponse, false, false, "Page unread mail; reading does not acknowledge delivery.", {"limit":20}),
    method!("mail.read", MailReadRequest, MailDelivery, false, false, "Read one exact delivery id without acknowledging it.", {"delivery_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV/a1"}),
    method!("mail.ack", MailAckRequest, MailAckResponse, false, true, "Acknowledge 1..100 exact delivery ids in your mailbox.", {"id":EXAMPLE_ID,"deliveries":["previous-write/a1"]}),
    method!("var.set", VarSetRequest, VarSetResponse, false, true, "Set a shared variable, optionally using compare-and-swap.", {"id":EXAMPLE_ID,"key":"progress","value":"working","expect_version":0}),
    method!("var.get", VarGetRequest, VarGetResponse, false, false, "Read a shared variable; an absent key has version zero.", {"key":"progress"}),
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

fn request_takes_id(method: &Method) -> bool {
    (method.input)()
        .get("properties")
        .and_then(|properties| properties.get("id"))
        .is_some()
}

fn visible(method: &Method, principal: &Principal) -> bool {
    match principal {
        Principal::Owner => true,
        Principal::Agent { .. } => {
            !method.owner_only
                && method.name != "client.identity"
                && !method.name.starts_with("inference.")
        }
        // These principals have no discovery RPC authority. Keep the helper
        // conservative even when used internally; do not broaden their gate.
        Principal::Run { .. }
        | Principal::ProjectRule { .. }
        | Principal::MemoryInference { .. } => false,
    }
}

impl Router {
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
                    summary: m.summary.to_owned(),
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
                    allowed_scopes:vec!["global".to_owned(), format!("project:{}", agent.project), format!("mailbox:{id}")],
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
        let method = METHODS
            .iter()
            .find(|m| m.name == request.method && visible(m, principal))
            .ok_or_else(|| {
                Error::new(
                    "unsupported",
                    "method is unavailable; use api.methods for callable canonical names",
                )
            })?;
        let identity = self.identity(principal).await?;
        let aliases: &[&str] = match method.name {
            "api.identity" => &["swarm identity"],
            "who" => &["swarm who"],
            "claim.set" => &["swarm claim set"],
            "claim.list" => &["swarm claim list"],
            "claim.release" => &["swarm claim release"],
            "mail.send" => &["swarm msg send"],
            "mail.inbox" => &["swarm inbox"],
            "mail.read" => &["swarm msg show"],
            "mail.ack" => &["swarm msg ack"],
            "var.set" => &["swarm var set"],
            "var.get" => &["swarm var get"],
            "swarm.wait" => &["swarm wait"],
            "swarm.digest" => &["swarm digest"],
            "swarm.reparent" => &["swarm reparent"],
            "swarm.handoff" => &["swarm handoff"],
            "swarm.switch" => &["swarm switch"],
            "command.cancel" => &["swarm cmd cancel"],
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
            unsupported.push("native Goal mode and max_cost are unsupported.".to_owned());
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
        let scopes = method_scopes(method.name, principal, &identity);
        // Compact by default: compact field descriptors (requiredness,
        // types, enums, nested shapes and documented constraints) stand in
        // for the full JSON Schemas (fields, one example, limits).
        // `full: true` restores both schemas for callers that need exact
        // shapes.
        let fields: Vec<String> = compact_field_descriptors(&(method.input)());
        let (request_schema, response_schema) = if request.full.unwrap_or(false) {
            ((method.input)(), (method.output)())
        } else {
            (Value::Null, Value::Null)
        };
        encode(MethodHelpResponse {
            method:method.name.to_owned(), summary:method.summary.to_owned(),
            cli_aliases:aliases.iter().map(|s| (*s).to_owned()).collect(),
            fields,
            request_schema, response_schema,
            example:json!({"method":method.name,"params":example}),
            mutation_id:if method.name.starts_with("inference.") && method.mutation { "Supply a caller-retained literal ULID before send; do not omit it. After a lost reply retry with that same id and identical semantic payload, or read inference.write.get. Never allocate another id to recover an uncertain mutation." } else if method.mutation { "id is optional on MCP: omit it and the server mints a ULID and returns it in the reply; retry with that same id. Otherwise supply a fresh caller-retained ULID in id before the first write. Retry only with that same id, principal, method and identical semantic payload. Never mint another id after an uncertain outcome; read back the retained result. The example id is illustrative; replace it for a new write." } else { "Read-only; no mutation id is required." }.to_owned(),
            scopes, limits:format!("{} Responses retain their DTO pagination and daemon 4 MiB frame bounds; one help result is at most 256 KiB. Examples for existing resources require their actual retained ids/profile/source/view.", method_limits(method.name)), unsupported,
        })
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
        "swarm.switch" => {
            "Owner only. Exact source generation; at most 64 selected plain SENDs. Reserve child IDs before launch preparation. Choose profile or ad-hoc harness/model/effort/auth/cmd. No launch prompt. Exact session readiness precedes atomic H2 handoff. Failure retains created B; stop separately. Same-ID replay never launches again. Finish an untransferred ready B with ordinary swarm.handoff."
        }
        "swarm.handoff" => {
            "Owner only. Distinct existing Agents in the same project; exact generations and retained task SEND required. At most 64 selected plain accepted/queued SENDs and configured selector bound. A quiescent, B cleanly idle. All live claims, a quiescent Goal (paused), and all children transfer atomically. Brief first; failed/unknown brief holds successors queued until Owner resolution or cancellation. No native resend on retry."
        }
        "command.cancel" => {
            "Owner only. Accepted/queued SEND children of swarm.handoff only; dispatched or unknown inputs cannot be cancelled. Retry the same id and command_id."
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
            "args is the verb-specific DTO object. Inspect agent.get for the exact target generation's native capabilities; unsupported operations reject. Turn/request-sensitive operations require exact bindings. CLI wait is client-side, not an agent.wait method. Agent child launch (verb launch with parent): omit profile_snapshot (the daemon freezes it from the named profile); omit profile to reuse the parent snapshot verbatim or name an approved child_profiles entry (same harness, only model/effort may differ); omit workspace to share the parent workspace (a different workspace rejects); parent defaults to self; project/control/capacity inherited; no worktree."
        }
        "events.read" | "events.subscribe" => {
            "At most 1000 events per page and the encoded event-page byte budget; subscribe waits at most 30000 ms. Agent events remain self-scoped. History gaps do not authorize replay."
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[tokio::test]
    async fn audit_390_help_scopes_are_method_specific() {
        let router = super::super::tests::router();
        // A retained row is enough to exercise metadata; no plugin or native
        // process is started, and these calls do not qualify authentication.
        let mut launch = CommandSubmitRequest::new(ulid::Ulid::new().to_string(), "launch");
        launch.expected_generation = Some(0);
        launch.args = json!({"project":"audit-project","workspace":"audit-workspace","harness":"codex","profile":"fixture","profile_snapshot":{}});
        let agent_id = router
            .inner
            .store
            .lock()
            .await
            .submit("owner", launch, crate::common::now_ms())
            .unwrap()
            .command
            .target
            .unwrap();
        let agent_mailbox = format!("mailbox:{agent_id}");
        let callers = [
            (Principal::Owner, "project:<name>", "mailbox:<agent>"),
            (
                Principal::Agent {
                    id: agent_id,
                    generation: 0,
                },
                "project:audit-project",
                agent_mailbox.as_str(),
            ),
        ];
        for (principal, project, mailbox) in callers {
            let identity = router
                .caller_identity(&json!({}), &principal)
                .await
                .unwrap();
            assert_eq!(
                identity["allowed_scopes"],
                json!(["global", project, mailbox])
            );
            for method in METHODS.iter().filter(|m| visible(m, &principal)) {
                let help = router
                    .method_help(&json!({"method":method.name}), &principal)
                    .await
                    .unwrap();
                let expected = match method.name {
                    "var.get" => vec!["global", project],
                    "var.set" if matches!(principal, Principal::Owner) => vec!["global", project],
                    "var.set" => vec![project],
                    "mail.inbox" | "mail.read" | "mail.ack" => vec![mailbox],
                    _ => Vec::new(),
                };
                assert_eq!(help["scopes"], json!(expected), "{}", method.name);
                assert!(
                    !help["limits"]
                        .as_str()
                        .unwrap()
                        .contains("Variables default")
                );
                if matches!(method.name, "mail.inbox" | "mail.read" | "mail.ack") {
                    assert!(help["request_schema"]["properties"].get("scope").is_none());
                }
            }
        }
    }

    #[test]
    fn descriptors_cover_router_methods_and_examples_decode() {
        let source = include_str!("../router.rs");
        let table = source
            .split("let result = match method {")
            .nth(1)
            .unwrap()
            .split("_ => Err(")
            .next()
            .unwrap();
        let registered: BTreeSet<_> = table
            .lines()
            .filter_map(|line| {
                let line = line.trim();
                line.strip_prefix('"')
                    .and_then(|s| s.split_once('"'))
                    .map(|(name, _)| name)
            })
            .collect();
        let described: BTreeSet<_> = METHODS.iter().map(|m| m.name).collect();
        assert_eq!(registered, described);
        assert_eq!(described.len(), METHODS.len());
        let mut errors = Vec::new();
        for method in METHODS {
            if let Err(error) = (method.decode)((method.example)()) {
                errors.push(format!("{}: {error}", method.name));
            }
            assert!((method.input)().is_object());
            assert!((method.output)().is_object());
        }
        assert!(errors.is_empty(), "{}", errors.join("\n"));
    }

    #[tokio::test]
    async fn audit_390_mutation_help_states_the_mcp_omission_rule() {
        let router = super::super::tests::router();
        for method in METHODS.iter().filter(|m| m.mutation) {
            let help = router
                .method_help(&json!({"method": method.name}), &Principal::Owner)
                .await
                .unwrap();
            let text = help["mutation_id"].as_str().expect("mutation help text");
            assert!(
                text.contains("omit") && text.contains("retry with that same id"),
                "{} must state the MCP omission and retry rule",
                method.name
            );
        }
        let help = router
            .method_help(&json!({"method": "var.get"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(
            help["mutation_id"],
            json!("Read-only; no mutation id is required.")
        );
    }

    #[test]
    fn compact_descriptors_render_nested_shapes_and_unions() {
        let schema = json!({
            "type": "object",
            "required": ["id", "mode"],
            "properties": {
                "id": {"type": "string", "description": "Caller id."},
                "mode": {"$ref": "#/$defs/Mode"},
                "filter": {
                    "anyOf": [
                        {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}, "required": ["b"]},
                        {"type": "null"}
                    ]
                },
                "tags": {"type": "array", "items": {"type": "string"}, "minItems": 1, "description": "Tags."},
                "pair": {"type": "array", "prefixItems": [{"type": "string"}, {"type": "integer"}]},
                "code": {"type": "string", "pattern": "^[A-Z]+$", "format": "upper", "description": "Code."}
            },
            "$defs": {
                "Mode": {"type": "string", "enum": ["y", "x"], "description": "Closed set."}
            }
        });
        assert_eq!(
            compact_field_descriptors(&schema),
            vec![
                "code (optional): string [fmt=upper, re=^[A-Z]+$] - Code.".to_owned(),
                "filter (optional): object{a, b*}".to_owned(),
                "id (required): string - Caller id.".to_owned(),
                "mode (required): enum(x|y) - Closed set.".to_owned(),
                "pair (optional): array<tuple(integer, string)>".to_owned(),
                "tags (optional): array<string> [1.. items] - Tags.".to_owned(),
            ]
        );
        // A schema without properties carries no descriptors.
        assert!(compact_field_descriptors(&json!({"type": "object"})).is_empty());
    }

    #[tokio::test]
    async fn help_is_compact_by_default_and_full_on_request() {
        let router = super::super::tests::router();
        let compact = router
            .method_help(&json!({"method": "mail.send"}), &Principal::Owner)
            .await
            .unwrap();
        // Compact help keeps requiredness, types, enums and documented
        // constraints (not bare names): the `to` descriptor carries the
        // recipient shape and constraints, `mode` the closed value set.
        assert_eq!(
            compact["fields"],
            json!([
                "id (required): string - Caller-retained mutation id; MCP callers may omit id and the server mints a ULID, returned for retry. Otherwise use a fresh ULID for a new write.",
                "mode (required): enum(inject|queue|send|steer) - Only queue is implemented; send/steer/inject parse but return unsupported.",
                "reply_to_delivery (optional): string - Exact <write-id>/<recipient-agent> delivery id, at most 512 bytes, no NUL.",
                "text (required): string [<=32768B] - Mail body, at most 32768 UTF-8 bytes, no NUL; empty text is allowed.",
                "thread (optional): string [<=256B] - Optional nonblank thread label, no control characters.",
                "to (required): array<string> [1..100 items, unique] - Distinct concrete canonical a<positive i64> Agent ids."
            ])
        );
        assert!(compact["request_schema"].is_null());
        assert!(compact["response_schema"].is_null());
        assert!(compact.get("example").is_some());
        assert!(compact.get("limits").is_some());
        let wire = serde_json::to_vec(&compact).unwrap();
        // Descriptors carry the full documented constraints and still
        // fit the ~600-token compact budget (measured 2161 bytes on
        // 2026-11-16; completeness did not require raising it).
        assert!(
            wire.len() <= 2400,
            "compact mail.send help exceeds 600 tokens: {} bytes",
            wire.len()
        );
        let full = router
            .method_help(
                &json!({"method": "mail.send", "full": true}),
                &Principal::Owner,
            )
            .await
            .unwrap();
        assert_eq!(full["fields"], compact["fields"]);
        assert!(full["request_schema"].is_object());
        assert!(full["response_schema"].is_object());
        assert!(full["request_schema"]["properties"].get("to").is_some());
    }

    #[tokio::test]
    async fn method_pages_carry_one_line_summaries() {
        let router = super::super::tests::router();
        // Owner sees every method; walk every page so all summaries hold.
        let mut after: Option<String> = None;
        let mut pages = 0;
        let mut seen = 0;
        loop {
            let mut params = serde_json::Map::new();
            params.insert("limit".to_owned(), json!(50));
            if let Some(cursor) = after {
                params.insert("after".to_owned(), json!(cursor));
            }
            let page = router
                .methods(&Value::Object(params), &Principal::Owner)
                .await
                .unwrap();
            pages += 1;
            let methods = page["methods"].as_array().expect("methods page");
            assert!(!methods.is_empty());
            for entry in methods {
                seen += 1;
                let summary = entry["summary"].as_str().expect("summary text");
                assert!(
                    summary.len() <= 100,
                    "{} summary exceeds one line: {summary:?}",
                    entry["method"]
                );
            }
            after = page["next"].as_str().map(str::to_owned);
            if after.is_none() {
                break;
            }
        }
        assert!(pages >= 2, "expected more than one methods page");
        assert!(seen >= 80, "expected the full method catalogue: {seen}");
    }

    #[test]
    fn command_submit_help_states_agent_child_launch_shape() {
        assert!(
            METHODS
                .iter()
                .find(|m| m.name == "command.submit")
                .expect("command.submit described")
                .summary
                .contains("launch"),
            "command.submit must stay discoverable via api.methods query launch"
        );
        let limits = method_limits("command.submit");
        for needle in [
            "omit profile_snapshot",
            "child_profiles",
            "omit workspace",
            "parent defaults to self",
            "no worktree",
        ] {
            assert!(
                limits.contains(needle),
                "command.submit limits must state {needle}"
            );
        }
    }

    #[test]
    fn agent_catalog_hides_owner_methods_without_exposing_run_authority() {
        let agent = Principal::Agent {
            id: "a1".to_owned(),
            generation: 1,
        };
        for method in METHODS {
            assert_eq!(
                visible(method, &agent),
                !method.owner_only
                    && method.name != "client.identity"
                    && !method.name.starts_with("inference.")
            );
            assert!(!visible(
                method,
                &Principal::Run {
                    run_command_id: "run-1".to_owned()
                }
            ));
        }
    }
}
