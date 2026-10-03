# ELIOT Gemini Spark Integration
## Official MCP remote-agent program for Google Gemini Spark

**Revision:** 1 — 2026-10-02  
**Repository baseline:** `8c9fcfe8896f14dc9341e5d81b36ec4ac109dda4`  
**Status:** documentation and implementation handoff. This document does not claim that an installed Spark account, OAuth flow, Cloudflare route, remote MCP endpoint or write action has been live-qualified.

## 0. Decision

Google Gemini Spark is a first-class remote-agent candidate for ELIOT because Google officially supports custom connected apps through a remote Model Context Protocol server URL.

Spark is not a new ELIOT runtime module and does not own native Muse Code, OpenCode, Codex, Zed or other worker sessions. It is an external ELIOT client:

```text
Gemini Spark
  -> Gemini custom Connected App (MCP client)
  -> HTTPS remote MCP endpoint
  -> Cloudflare edge/authentication
  -> cloudflared
  -> loopback Remote Agent Gateway
  -> local ELIOT IPC
  -> ELIOT host
```

The initial role is `observer`, followed by an independently qualified `reviewer`. Mutating manager and GM profiles are enabled only after exact OAuth, retry, confirmation, idempotency and handover behavior is demonstrated on the installed Spark product.

ELIOT remains the only authority for:

- Task, Attempt and Operation state;
- native binding/session ownership;
- request receipts and unknown outcomes;
- immutable candidate/result artifacts;
- CheckRunner evidence;
- acceptance;
- GM epoch and handover.

Spark tasks, schedules, skills, browser progress and MCP confirmations are client-side orchestration facts. They do not become ELIOT acceptance or native terminal evidence.

## 1. Product identity

### 1.1 Gemini Spark is not Gemini CLI or Antigravity

Gemini Spark is Google's personal cloud agent inside Gemini Apps. Google describes it as a long-running task and schedule system that can use Connected Apps, skills, chats, signed-in websites, Personal Intelligence, location, a remote browser and remote code execution.

Google's launch material describes Spark as running on dedicated Google Cloud virtual machines, based on Gemini and the Antigravity harness, and able to execute multi-step work independently of the user's laptop.

This does **not** make Spark:

- the Gemini CLI;
- Google Antigravity desktop;
- an ELIOT worker runtime;
- a replacement for the existing Antigravity/agy maintenance route;
- a native session owner for another harness;
- a durable replacement for ELIOT Operations or scheduled-work registry.

### 1.2 Official custom MCP support

The current Gemini Apps help contract says a user can:

1. add a custom app by entering its MCP server URL in Gemini Connected Apps;
2. connect through standard MCP;
3. use Dynamic Client Registration when supported;
4. enter credentials through Advanced features when DCR is unavailable;
5. explicitly select the app with `@` in a prompt.

The custom app is linked in the Gemini web app and then available in Gemini web and mobile.

Google's public help currently requires, for custom apps:

- age 18 or over;
- location in the United States;
- a personal Google Account, not a work or school account;
- Keep Activity enabled;
- English;
- a remote MCP server URL following standard MCP specifications.

Gemini Spark itself currently requires a personal Google Account, Google AI Pro or Ultra, Keep Activity enabled and a supported region. Spark's general regional availability is broader than the current custom-app availability; those two matrices must not be conflated.

### 1.3 Moving product boundary

The Spark model, browser and product surface may change independently of ELIOT. ELIOT therefore identifies this client by:

```text
client product: google-gemini-spark
connection contract revision
Google account class
custom-app capability
observed MCP protocol version
ELIOT principal/profile
qualification record
```

A displayed model name is not a durable route identity.

## 2. Official Spark behavior relevant to ELIOT

### 2.1 Tasks, schedules and skills

Google defines:

- **Task** — a high-level goal or project managed by Spark.
- **Schedule** — a time- or event-triggered execution of a task.
- **Skill** — reusable instructions and context describing how to perform work.

These concepts remain outside ELIOT authority. A Spark task may call ELIOT tools, but it does not create a second Task store. A Spark schedule may request an ELIOT read or typed Operation, but it does not replace the controller schedule registry.

### 2.2 Long-running and background work

Spark can continue work in a remote browser or remote computer and can run schedules in the background. The current help contract also states:

- up to 15 Spark tasks can run concurrently;
- a schedule does not run when 15 tasks are already running;
- some actions pause for user confirmation or browser takeover;
- turning off Spark pauses tasks and schedules rather than deleting them;
- stopping browser activity may cause Spark to continue through another tool or remote browser;
- completely stopping the Spark task is a separate action.

Consequences for ELIOT:

- Spark silence is not ELIOT terminal evidence;
- a closed browser or disconnected MCP session does not cancel an admitted ELIOT Operation;
- a Spark stop does not imply `agent.interrupt`, `message.cancel`, `attempt.release` or Task cancellation;
- an ELIOT cancellation remains an exact, separately authorized Operation;
- ELIOT schedules cannot rely on Spark's 15-task queue or account availability.

### 2.3 Connected data and privacy

Google states that a custom connected app may receive information from:

- the current chat;
- other Connected Apps;
- Personal Intelligence;
- skills;
- tasks;
- logged-in websites;
- other sources available to Spark.

Google also states that it does not control, monitor or secure third-party MCP servers. The user is responsible for trusting and supervising them.

For ELIOT this means:

- the MCP server must request only the data needed for one call;
- tool descriptions must not encourage broad context transfer;
- tool outputs must not echo unrelated Google data;
- ELIOT never asks Spark for Google account credentials, cookies, passwords or payment data;
- repository prompts and tool results are treated as untrusted content for prompt-injection purposes;
- a remote-agent support bundle excludes chat history and Google Connected App data.

### 2.4 Write confirmations

Google currently requires manual confirmation for custom-app write actions. This is an additional user-facing protection, not ELIOT authorization.

The controller still enforces:

- principal and role;
- named tool profile;
- exact Task/binding/generation scope;
- caller-owned `client_request_id`;
- ownership and current GM epoch;
- pending-request fingerprint;
- application-specific acceptance rules.

No design may infer that a Google confirmation makes the caller ELIOT GM or turns a tool result into Task acceptance.

## 3. Relationship to the existing Remote Agent Gateway

The canonical gateway architecture is [remote-agent-gateway.md](remote-agent-gateway.md). Spark adds one official MCP client surface; it does not add another gateway implementation.

```text
external client
  -> client-specific authentication
  -> named ELIOT remote profile
  -> filtered MCP discovery
  -> pre-dispatch allowlist
  -> local IPC
  -> existing application API
  -> existing durable Operation/read model
```

The gateway:

- never opens SQLite;
- never attaches directly to native worker sessions;
- never owns Spark's task or browser;
- never restarts `cloudflared` or Spark because an MCP call failed;
- never exposes a universal JSON-RPC passthrough;
- never exposes an arbitrary shell or filesystem tool.

Issue #20 remains the prerequisite for server-enforced remote profiles and projection of the already implemented Phase-B methods.

## 4. Network and transport contract

### 4.1 Remote URL is required

Unlike OpenAI Secure MCP Tunnel, Gemini custom apps are configured with a remote MCP server URL. The initial ELIOT Spark path therefore uses the general Cloudflare ingress:

```text
https://YOUR_DOMAIN/swarm/mcp
  -> Cloudflare
  -> cloudflared
  -> 127.0.0.1:<gateway-port>/mcp
  -> ELIOT IPC
```

No router port forward and no public local origin are introduced.

### 4.2 Streamable HTTP first

The new endpoint uses standard remote MCP over Streamable HTTP. It must not be built around the old filesystem SSE bridge.

The first live connection records:

- HTTP transport actually used;
- Spark `initialize` protocol version;
- client capabilities;
- server capabilities;
- session/stateless behavior;
- pagination behavior;
- cancellation/progress behavior;
- whether resources, prompts and MCP Tasks extensions are consumed;
- response/body limits;
- retry behavior.

Google's help says “standard MCP” but does not publish an exact supported protocol revision or full transport matrix. The implementation must not claim support for an MCP revision or extension merely because the ELIOT/RMCP side implements it.

A compatibility path may support the older 2025 Streamable HTTP handshake. Legacy HTTP+SSE is not the design target and is added only if the installed Spark client demonstrably requires it.

### 4.3 Do not expose ELIOT MCP Tasks automatically

ELIOT already projects Operations as MCP Tasks. Spark support for the MCP Tasks extension is not established by Google's custom-app documentation.

Until qualified:

- expose ordinary tools and bounded reads;
- do not assume Spark can poll ELIOT MCP Tasks;
- do not map a Spark task ID to an ELIOT Operation ID;
- do not create hidden Operations from Spark task progress events.

If Spark advertises and correctly uses the extension, record the exact protocol/capabilities and enable it as a separate compatibility decision.

## 5. Authentication and account linking

### 5.1 Preferred candidate: Cloudflare Access Managed OAuth

Spark supports DCR, and Cloudflare Access Managed OAuth supports standards-based OAuth for MCP server applications. Therefore the preferred candidate is:

```text
Spark MCP client
  -> OAuth discovery / DCR
  -> Cloudflare Access login and consent
  -> access token
  -> Cloudflare edge
  -> origin request with Cf-Access-Jwt-Assertion
  -> gateway validates JWT
  -> maps identity to one local ELIOT principal/profile
```

This is a **candidate architecture**, not yet a qualified fact.

The pilot must prove:

- Spark follows protected-resource and authorization-server metadata;
- DCR reaches the expected endpoint;
- redirect URI can be safely allowlisted;
- PKCE/code exchange completes;
- refresh works;
- revocation/disconnect works;
- Cloudflare token audience/issuer are validated at origin;
- a second Google account does not reuse the first account's ELIOT principal;
- profile selection cannot be supplied or widened by the MCP caller.

### 5.2 Alternative: gateway-owned MCP OAuth

If Access Managed OAuth cannot interoperate with Spark, the remote MCP gateway may implement the MCP OAuth 2.1 resource-server contract and delegate authentication to a reviewed authorization provider.

In that topology Cloudflare Tunnel remains transport/WAF/DDoS protection. Do not stack two incompatible OAuth owners or enable Access Managed OAuth in front of server code that already owns an OAuth flow unless the combined contract is explicitly designed and tested.

### 5.3 Advanced credentials fallback

Google exposes Advanced features for servers without DCR. This may permit a pre-registered client ID/secret.

Use it only after the installed product reveals the exact fields and callback contract. Credentials stay in the local Spark/Google account connection and authorization service, not in:

- the repository;
- ELIOT Task or Operation payloads;
- MCP tool schemas/results;
- logs;
- diagnostics;
- prompts.

A static shared bearer token is not the preferred final design. It lacks per-user OAuth consent, clean revocation and narrow identity mapping.

### 5.4 Current interoperability warning

Public field reports describe DCR, OAuth callback/token-exchange and post-token initialization failures with some Spark custom MCP integrations. These reports are not authoritative product specifications, but they are sufficient to require a real connection test before implementation is declared ready.

No fallback may silently weaken the security model merely to make the Connected Apps screen turn green.

## 6. Tool profiles

Issue #20 defines server-enforced profiles. Spark gets distinct credentials and profiles; it never shares Dot or Muse credentials.

### 6.1 `spark-observer`

Initial surface:

- host/readiness/Doctor reads;
- Task, Attempt and Operation reads;
- report, attention and capacity reads;
- family projections;
- check/submission/acceptance reads;
- bounded artifact metadata and bounded text excerpts.

Not exposed:

- Task mutation;
- message send/cancel;
- agent send/configure/reply/background/interrupt;
- acceptance/invalidation;
- client administration;
- host mode;
- GM handover;
- forge;
- module update;
- shell;
- arbitrary filesystem.

Both gates are mandatory:

1. hidden tools are absent from `tools/list`;
2. a manually addressed hidden tool is rejected before local IPC write.

### 6.2 `spark-reviewer`

Added only after observer qualification:

- exact submission read;
- exact candidate/evidence read;
- policy-selected `task.request_changes`;
- no acceptance by default;
- no native agent control.

A review references exact Task revision, Attempt, submission and candidate. Spark's natural-language task/thread identity is not review identity.

### 6.3 `spark-manager`

Added one mutation at a time after OAuth and retry qualification. Candidate operations:

1. addressed `message.send`;
2. one Task create/revise path selected by policy;
3. addressed `agent.send`;
4. exact pending `agent.reply`.

Every mutating schema requires caller-minted `client_request_id`. The server does not generate a new ID after an unknown result.

### 6.4 `spark-gm`

Disabled by default.

Promotion requires:

- successful observer and reviewer pilots;
- safe mutation retry/readback;
- stale-GM rejection;
- current epoch projection;
- exact `gm.handover`;
- verified successor resync;
- no automatic promotion on tunnel, OAuth or Spark task availability.

Only one remote agent is current GM. Dot, Muse and Spark cannot all mutate as GM simultaneously.

## 7. Spark task and schedule policy

### 7.1 Read-only schedules first

The first scheduled Spark workflows may only read bounded ELIOT projections and produce a summary to the user.

Examples:

- daily attention summary;
- stalled/unknown Operation report;
- upcoming schedule/maintenance report;
- quota/capacity incident summary;
- candidate/review queue summary.

They do not:

- answer forms;
- accept Tasks;
- send native prompts;
- cancel work;
- publish code;
- change host mode;
- perform GM handover.

### 7.2 Scheduled custom-app behavior is unqualified

A Google Help Community report states that a third-party MCP connector worked interactively but not in a scheduled Spark execution. Another report describes scheduled tasks waiting for repeated tool confirmation. Community reports may be incomplete or stale, but they identify exact tests that must be run.

The project must not promise unattended scheduled MCP access until the installed account proves:

- the custom app is available in background runs;
- OAuth tokens are available and refreshed;
- read-only calls do not unexpectedly require user presence;
- write confirmations pause rather than skip or fabricate success;
- the task reports the tool failure rather than returning a misleading “nothing changed” conclusion;
- schedule retries do not duplicate ELIOT mutations.

### 7.3 ELIOT scheduler remains authoritative

Spark schedules are user-facing clients of ELIOT. The controller's scheduled-work registry owns durable controller schedules.

A Spark schedule may request an existing typed Operation; it may not become a second schedule database or compute authoritative missed-slot/catch-up state.

## 8. Delivery and unknown outcomes

### 8.1 Read calls

Read results include:

- source;
- observation time;
- freshness;
- coverage;
- gaps;
- bounded cursor/page information.

Missing/partial data is reported as unknown/partial, never zero or empty success.

### 8.2 Mutations

Mutation flow:

```text
Spark chooses exact tool
  -> user confirmation when Google requires it
  -> caller supplies client_request_id
  -> gateway validates profile and input
  -> ELIOT persists intent
  -> native/controller effect
  -> durable Operation outcome
  -> tool returns Operation/result reference
```

If the HTTP/MCP response is lost after dispatch:

- Spark must not call again with a new request ID;
- the original Operation is read by request/operation identity;
- an unknown external effect is reconciled by the existing adapter/forge contract;
- ELIOT never fabricates rejection from network failure.

The pilot must determine whether Spark preserves tool arguments during client retries. Until then, write profiles remain off.

### 8.3 Cancellation

Three separate actions:

- cancel/stop the Spark task;
- close/revoke the custom app or OAuth grant;
- request an exact ELIOT cancellation/interrupt.

They are never inferred from one another.

## 9. Prompt injection and tool-output safety

Google explicitly identifies prompt injection as an agentic risk. ELIOT treats every repository document, Issue, email-derived instruction, website, tool description and remote tool output as potentially hostile data.

Gateway rules:

- tool descriptions are static, reviewed and versioned;
- no tool output can define new tools or profiles;
- text from Issues/artifacts is data, not an instruction to the gateway;
- URLs in tool results are not automatically fetched;
- artifact references do not trigger filesystem/network reads;
- external text cannot select an ELIOT credential, role or GM epoch;
- write tools show exact target identity and bounded arguments;
- result payloads contain no credentials, endpoint secrets or local private paths.

Spark's confirmation UI is not relied upon as the only prompt-injection defense.

## 10. Cloudflare integration

### 10.1 Separate route

Use a separate Swarm endpoint and application/profile. Do not reuse the legacy filesystem route or its tool allowlist.

```text
remote Swarm MCP
workspace read/search
other MCP services
```

remain independent capabilities.

### 10.2 Origin requirements

- bind gateway to loopback only;
- exact `/mcp` route;
- catch-all deny;
- bounded request and response sizes;
- bounded handshake/auth timeouts;
- no stale-on-error authoritative cache;
- validate OAuth/Access token issuer, audience, expiry and scopes;
- map one external identity to one configured local ELIOT principal;
- redact endpoint/auth material;
- retain only bounded security/audit facts;
- route-level 403/disable killswitch does not stop ELIOT host/native work.

### 10.3 Cloudflare MCP Portal

Cloudflare MCP Portals can aggregate remote HTTP MCP servers and apply tool/prompt policy. This may be useful later, but the first Spark path should connect directly to the narrow Swarm MCP server application.

A portal must not become:

- Task/Operation authority;
- a second tool-profile database;
- the holder of a broad shared admin credential;
- a way to combine workspace files and Swarm mutations into one excessive profile.

## 11. Privacy and local installation

The real deployment domain and infrastructure identifiers are local machine configuration, not repository content.

Repository examples use only:

```text
https://YOUR_DOMAIN/swarm/mcp
https://mcp.example.com/swarm/mcp
${ELIOT_SPARK_MCP_URL}
${ELIOT_CLOUDFLARE_ACCESS_AUD}
${ELIOT_CLOUDFLARE_TEAM}
```

Do not commit or print:

- real domains/subdomains;
- Cloudflare account, zone, tunnel, application or audience IDs;
- Google account identifiers;
- OAuth client secrets, refresh tokens or grants;
- local usernames or private absolute paths;
- ELIOT credentials;
- browser cookies or Google session data.

Local setup collects:

- connection channel `gemini-spark-mcp`;
- remote URL reference;
- authentication mode;
- local ELIOT principal/credential reference;
- named tool profile;
- OAuth issuer/audience/scopes;
- approval policy;
- optional future GM-candidate flag;
- qualification record.

Store it in a user-restricted local profile outside the repository. Agents receive profile identity and capabilities, not the deployment hostname or secret.

## 12. Implementation program

### S0 — documentation and privacy

- add this product identity and integration boundary;
- preserve the domain-placeholder rule;
- record official requirements and unverified fields;
- add no runtime or service change.

### S1 — complete issue #20

- project `message.cancel`, current mailbox fields and `agent.background`;
- implement named server-enforced profiles;
- filter both discovery and dispatch;
- preserve application authorization.

### S2 — remote Streamable HTTP gateway

- expose the existing MCP/application surface over loopback Streamable HTTP;
- no SQLite access;
- bounded transport;
- no generic passthrough;
- record protocol/capability facts.

### S3 — OAuth/Cloudflare candidate

- configure a dedicated MCP server application;
- test Access Managed OAuth/DCR;
- validate Access JWT at origin;
- record exact redirect and token behavior;
- retain route killswitch;
- fall back only to a reviewed gateway-owned OAuth design or pre-registered client.

### S4 — `spark-observer`

- add custom app through Gemini web;
- explicitly select it with `@`;
- verify catalog and read calls;
- verify partial/gap semantics;
- verify disconnect/reconnect and revocation;
- verify no write discovery or dispatch.

### S5 — schedules and background

- test an interactive read;
- test the same read in a scheduled task;
- test OAuth refresh;
- test 15-task saturation;
- test revoked app and disabled route;
- keep schedules read-only until evidence is recorded.

### S6 — `spark-reviewer`

- exact submission/candidate review;
- bounded evidence;
- one policy-selected request-changes mutation if confirmation/retry behavior is safe;
- no acceptance.

### S7 — controlled manager mutations

- one mutation type per slice;
- caller-owned request ID;
- lost-response reconciliation;
- manual confirmation behavior;
- stale target and stale GM epoch rejection;
- no generic shell.

### S8 — GM pilot

- operator/current-GM initiated handover only;
- exact epoch;
- successor authoritative resync;
- former GM loses mutation rights immediately;
- network/Spark task loss does not change designation.

### S9 — production decision

Record:

```text
SUPPORTED_INTERACTIVE_READ
SUPPORTED_SCHEDULED_READ
SUPPORTED_CONFIRMED_WRITE
SUPPORTED_MANAGER
SUPPORTED_GM
UNSUPPORTED
UNKNOWN
```

per operation/profile, with account/product version/date/evidence.

## 13. Qualification matrix

### Account and availability

```text
[ ] age/account/subscription/region requirements satisfied
[ ] personal account; no claim of Workspace-account support
[ ] Keep Activity requirement recorded
[ ] English custom-app limitation recorded
[ ] custom app visible in web and usable in expected clients
```

### MCP transport

```text
[ ] remote URL discovery
[ ] initialize protocol version captured
[ ] Streamable HTTP behavior
[ ] 2025 compatibility if required
[ ] tools/list pagination and annotations
[ ] resources/prompts support or explicit absence
[ ] MCP Tasks extension support or explicit absence
[ ] cancellation/progress behavior
[ ] body and item limits
[ ] reconnect/session/stateless behavior
```

### OAuth

```text
[ ] protected-resource metadata
[ ] authorization-server metadata
[ ] DCR or exact pre-registration path
[ ] redirect URI validation
[ ] PKCE/code exchange
[ ] token audience/issuer/scope
[ ] refresh
[ ] revocation
[ ] second-account isolation
[ ] no credential in logs/artifacts
```

### Authorization

```text
[ ] observer hidden write tools absent from discovery
[ ] manually addressed hidden tool rejected before IPC
[ ] profile cannot elevate ELIOT role
[ ] distinct Spark/Dot/Muse principals
[ ] exact Task/binding/generation scope
[ ] stale GM epoch rejected
```

### Read behavior

```text
[ ] status/report
[ ] attention/capacity
[ ] Task/Attempt/Operation read
[ ] family partial/gap
[ ] bounded artifact read
[ ] no stale-on-error success
```

### Write behavior

```text
[ ] manual confirmation observed
[ ] request ID retained before dispatch
[ ] same request/payload returns retained receipt
[ ] different payload under same ID conflicts
[ ] lost reply reconciled without duplicate
[ ] stale request/fingerprint rejected
[ ] Spark cancellation does not imply ELIOT cancellation
```

### Background schedules

```text
[ ] custom MCP available in scheduled execution
[ ] read-only call works without unattended write privilege
[ ] auth refresh in background
[ ] approval-required call pauses visibly
[ ] failure is not reported as successful no-op
[ ] 15-task saturation behavior
[ ] no duplicate mutation after schedule retry
```

### Security

```text
[ ] prompt-injection payload in tool output
[ ] hostile tool arguments
[ ] excessive data request rejected/minimized
[ ] no Google chat/Workspace data echoed unexpectedly
[ ] Cloudflare route killswitch
[ ] OAuth revocation
[ ] logs/support bundle redact endpoint and identity material
[ ] repository scan contains no deployment identifiers
```

## 14. Comparison with Dot and Meta Muse

| Surface | OpenAI Dot | Meta Muse personal agent | Google Gemini Spark |
|---|---|---|---|
| Official arbitrary MCP path | Yes through supported OpenAI MCP surfaces | Not established in reviewed public Meta connector docs | **Yes: custom app by MCP URL** |
| Private no-domain path | Secure MCP Tunnel | Not established | Not established; remote URL currently required |
| General typed connector | Plugins/MCP/local computer | Custom Connectors | Connected Apps + custom MCP |
| Background tasks | Dot/Work/Codex surface dependent | Muse tasks/cloud browser | Native Spark tasks/schedules |
| Main caveat | plan-dependent write MCP | connector wire/auth still needs qualification | custom apps currently US/personal/English; OAuth/background behavior needs qualification |
| Initial ELIOT role | observer, then GM candidate | observer/reviewer/standby | observer, then reviewer |
| GM eligibility | after write/handover qualification | after typed action/handover qualification | after OAuth/write/retry/handover qualification |

Spark's official MCP support makes it a more direct typed integration candidate than browser automation. It still does not supersede the ELIOT authority or make its scheduled agent loop safe for unqualified writes.

## 15. Field reports to test, not inherit

The following public reports are `USER`/`UNVERIFIED`, not product contracts:

- third-party MCP available interactively but unavailable in a scheduled Spark task;
- write/custom-app permission requests stalling unattended schedules;
- DCR failure falling back to unavailable static client credentials;
- successful authorization redirect without token exchange;
- token issued but no subsequent MCP initialization;
- a server working with other MCP clients but failing during Spark connection.

They define qualification cases. They do not justify workarounds that weaken OAuth, disable ELIOT authorization or fabricate successful background execution.

## 16. Non-goals

- no Spark model API adapter;
- no Gemini CLI route;
- no replacement of Antigravity/agy maintenance;
- no second Task/schedule/acceptance store;
- no direct Spark attachment to native worker sessions;
- no browser scraping as the primary integration;
- no arbitrary shell/filesystem tool;
- no shared credential across remote agents;
- no automatic GM election;
- no automatic controller re-prompt loop;
- no domain, account, tunnel or secret in Git;
- no claim of scheduled MCP support before live evidence.

## 17. Source index

### Google official

- [Gemini Apps Help: Use Gemini Spark to manage your tasks & workflows](https://support.google.com/gemini/answer/17094507?hl=en)
- [Gemini Apps Help: Connect & manage custom apps](https://support.google.com/gemini/answer/17209137?hl=en)
- [Google I/O 2026 keynote: Spark dedicated cloud VMs, Antigravity and MCP direction](https://blog.google/intl/de-de/unternehmen/technologie/sundar-pichai-io-2026/)
- [Google Blog, 2026-06-30: Gemini Spark updates — connected apps and custom MCP](https://blog.google/innovation-and-ai/products/gemini-app/gemini-spark-updates-june-2026/)
- [Google Blog, 2026-07-30: Gemini Spark integrates with Chrome](https://blog.google/innovation-and-ai/products/gemini-app/gemini-spark-updates-july-2026/)

### Cloudflare official

- [Cloudflare Access Managed OAuth](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/managed-oauth/)
- [Secure MCP servers with Cloudflare Access](https://developers.cloudflare.com/cloudflare-one/access-controls/ai-controls/secure-mcp-servers/)
- [MCP server portals](https://developers.cloudflare.com/agents/model-context-protocol/cloudflare/mcp-portal/)
- [Cloudflare MCP authorization guidance](https://developers.cloudflare.com/agents/model-context-protocol/protocol/authorization/)

### MCP official

- [MCP 2026-07-28 revision notes](https://blog.modelcontextprotocol.io/posts/2026-07-28/)
- [MCP TypeScript SDK: Streamable HTTP and OAuth client behavior](https://ts.sdk.modelcontextprotocol.io/client)

Google's current Spark documentation explicitly refers to DCR, while the MCP 2026-07-28 revision deprecates DCR in favor of Client ID Metadata Documents. The ELIOT endpoint must qualify the exact Spark behavior and retain compatibility rather than assuming that Spark already implements the newest registration path.

### ELIOT

- [Remote Agent Gateway](remote-agent-gateway.md)
- [Owner Decisions](owner-decisions.md)
- [Issue #20: MCP Phase-B projection and named tool profiles](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/20)

### Field reports — `USER` / `UNVERIFIED`

- [Google Help Community: custom connector unavailable in scheduled Spark execution](https://support.google.com/gemini/thread/456529684/gemini-spark-doesn-t-run-3rd-party-connectors-in-the-scheduled-automated-execution?hl=en)
- [Google Help Community: repeated confirmation stalls a scheduled custom-app task](https://support.google.com/gemini/thread/470675687/how-to-tell-gemini-spark-to-stop-asking-permission-before-using-my-custom-app-mcp?hl=en)
- [Provider report: Spark DCR falls back to unavailable static credentials](https://github.com/firecrawl/firecrawl-mcp-server/issues/345)
- [Google AI Developers Forum: OAuth callback/token-exchange and post-token initialization reports](https://discuss.ai.google.dev/t/gemini-spark-custom-mcp-oauth-stops-after-302-callback-and-never-calls-token/177327)
