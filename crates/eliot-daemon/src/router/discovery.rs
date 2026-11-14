//! Descriptions beside Router registration. Wire shapes belong to Core DTOs.
use super::{Principal, Router};
use eliot_core::{
    Command, Error, Result, Turn,
    api::{discovery::*, *},
};
use serde_json::{Value, json};

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
    method!(
        "maintenance.run",
        MaintenanceRunRequest,
        MaintenanceRunResponse,
        true,
        false,
        "Run the bounded retention pass now and report pruned counts.",
        {}
    ),
    method!("command.submit", CommandSubmitRequest, CommandSubmitResponse, false, true, "Submit a command using the target's advertised native capabilities.", {"id":EXAMPLE_ID,"verb":"send","target":"a1","args":{"text":"Report current progress."}}),
    method!("command.get", CommandGetRequest, Command, false, false, "Read a retained command in your authorized scope.", {"id":EXAMPLE_ID}),
    method!("command.children", CommandChildrenRequest, CommandChildrenResponse, false, false, "Page the retained children of one command.", {"id":EXAMPLE_ID,"limit":20}),
    method!("command.resolve", CommandResolveRequest, CommandResolveResponse, true, true, "Record an owner resolution of an uncertain command; never replay its effect.", {"id":EXAMPLE_ID,"command_id":"previous-command","as":"failed","reason":"Owner verified failure."}),
    method!("agent.get", AgentGetRequest, AgentGetResponse, false, false, "Read Agent state, native capabilities and source health.", {"id":"a1"}),
    method!("agent.discover", AgentDiscoverRequest, DiscoverPage, true, false, "Discover native metadata through an installed profile without creating Agents.", {"profile":"codex","limit":20}),
    method!("agent.history", AgentHistoryRequest, ObservationPage, true, false, "Read a bounded native history view advertised by agent.get.", {"id":"a1","source":"native","view":"turns","limit":20}),
    method!("agent.list", AgentListRequest, AgentListResponse, false, false, "List scoped compact Agent summaries with indexed state/project/harness/parent/root filters, counts and explicit truncation.", {"limit":20,"state":"busy"}),
    method!("agent.result", AgentResultRequest, AgentResultResponse, false, false, "Read retained turns/results for one Agent.", {"id":"a1","limit":20}),
    method!("agent.processes", AgentProcessesRequest, AgentProcessesResponse, false, false, "Read bounded process observations for one Agent.", {"id":"a1"}),
    method!("agent.stats", AgentStatsRequest, AgentStatsResponse, false, false, "Read sampled Agent statistics with freshness metadata.", {"id":"a1"}),
    method!("usage.summary", UsageSummaryRequest, UsageSummaryResponse, false, false, "Read bounded usage totals with explicit coverage over one Agent, project or tree scope.", {"agent":"a1"}),
    method!("agent.git", AgentGitRequest, AgentGitResponse, false, false, "Read Git observations for one Agent's workspace.", {"id":"a1"}),
    method!("agent.edits", AgentEditsRequest, AgentEditsResponse, false, false, "Read a bounded page of observed workspace edits.", {"id":"a1","limit":20}),
    method!("turn.get", TurnGetRequest, Turn, false, false, "Read one exact Agent/generation/turn binding.", {"agent":"a1","generation":1,"turn_id":"turn-1"}),
    method!("turn.resolve", TurnResolveRequest, TurnResolveResponse, true, true, "Record an owner resolution for one uncertain exact turn.", {"id":EXAMPLE_ID,"agent":"a1","generation":1,"turn_id":"turn-1","status":"completed","reason":"Owner verified completion."}),
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

fn visible(method: &Method, principal: &Principal) -> bool {
    match principal {
        Principal::Owner => true,
        Principal::Agent { .. } => !method.owner_only,
        // These principals have no discovery RPC authority. Keep the helper
        // conservative even when used internally; do not broaden their gate.
        Principal::Run { .. } | Principal::ProjectRule { .. } => false,
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
            "mail.send" => &["swarm msg send"],
            "mail.inbox" => &["swarm inbox"],
            "mail.read" => &["swarm msg show"],
            "mail.ack" => &["swarm msg ack"],
            "var.set" => &["swarm var set"],
            "var.get" => &["swarm var get"],
            "swarm.wait" => &["swarm wait"],
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
        encode(MethodHelpResponse {
            method:method.name.to_owned(), summary:method.summary.to_owned(),
            cli_aliases:aliases.iter().map(|s| (*s).to_owned()).collect(),
            request_schema:(method.input)(), response_schema:(method.output)(),
            example:json!({"method":method.name,"params":example}),
            mutation_id:if method.mutation { "id is optional on MCP: omit it and the server mints a ULID and returns it in the reply; retry with that same id. Otherwise supply a fresh caller-retained ULID in id before the first write. Retry only with that same id, principal, method and identical semantic payload. Never mint another id after an uncertain outcome; read back the retained result. The example id is illustrative; replace it for a new write." } else { "Read-only; no mutation id is required." }.to_owned(),
            scopes, limits:format!("{} Responses retain their DTO pagination and daemon 4 MiB frame bounds; one help result is at most 256 KiB. Examples for existing resources require their actual retained ids/profile/source/view.", method_limits(method.name)), unsupported,
        })
    }
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
            "args is the verb-specific DTO object. Inspect agent.get for the exact target generation's native capabilities; unsupported operations reject. Turn/request-sensitive operations require exact bindings. CLI wait is client-side, not an agent.wait method."
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
    fn agent_catalog_hides_owner_methods_without_exposing_run_authority() {
        let agent = Principal::Agent {
            id: "a1".to_owned(),
            generation: 1,
        };
        for method in METHODS {
            assert_eq!(visible(method, &agent), !method.owner_only);
            assert!(!visible(
                method,
                &Principal::Run {
                    run_command_id: "run-1".to_owned()
                }
            ));
        }
    }
}
