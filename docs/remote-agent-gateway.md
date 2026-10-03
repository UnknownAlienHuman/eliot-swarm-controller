# ELIOT Remote Agent Gateway
## OpenAI Dot, Meta Muse Agent and Cloudflare integration program

**Revision:** 1 — 2026-10-02  
**Repository baseline:** `0ff7129d11a15a71a94e1ed2c265f7a3eeb0e1db`  
**Status:** documentation and implementation handoff. No remote control path described here is qualified merely because this document exists.

## 0. Decision

ELIOT keeps one Task/Attempt/Operation authority, one physical-session owner, one GM epoch and one protected acceptance path.

Remote agents do not receive a second scheduler, direct database access, shell passthrough or a competing session owner. They reach the existing application API through a narrow **Remote Agent Gateway** with a dedicated principal and a server-enforced tool profile.

The recommended connectivity split is:

```text
OpenAI Dot
  preferred private path:
    OpenAI Secure MCP Tunnel
      -> local tunnel-client
      -> existing `swarm mcp` stdio facade
      -> local ELIOT IPC
      -> ELIOT host

Meta Muse personal agent
  current supported path:
    Muse Secure VM browser
      -> Cloudflare Access
      -> thin web operator surface / Streamable HTTP gateway
      -> local ELIOT IPC
      -> ELIOT host

Other external agents
  optional public path:
    HTTPS Streamable MCP
      -> Cloudflare Access
      -> cloudflared
      -> loopback Remote Agent Gateway
      -> local ELIOT IPC
      -> ELIOT host
```

OpenAI Secure MCP Tunnel is the default for Dot because it keeps the MCP server private and needs no public domain. Cloudflare remains the general external ingress for Muse and other clients. Connectivity is not authorization: every path terminates in the same ELIOT principal, method allowlist, idempotency and GM checks.

## 1. Current repository stage

### 1.1. Implemented and preserved

The following product boundaries exist on the baseline and must not be rewritten:

- durable Tasks, Attempts, Operations and caller-owned request receipts;
- immutable source/result artifacts, submissions, review and acceptance;
- fixed-source CheckRunner with process-group ownership and conservative recovery;
- local authenticated IPC; no public controller listener;
- stdio RMCP facade over the same application API as the CLI;
- MCP Tasks projection over existing Operations, without a second task authority;
- bounded session subscriptions over committed report/mailbox/Operation facts, with explicit lag and read-resync;
- GM designation, epoch and explicit handover;
- OpenCode V2 direct HTTP adapter, exact input/log correlation, child reads, result export and addressed `agent.background`;
- Muse Code SDK bridge with exact steer, settings, goal/replies, recovery checkpoint and recorded durability/host-death/gap observations;
- OpenCodex 2.75 Management API observer/configuration bridge;
- typed mailbox delivery identity, payload digest, replies and cancellation;
- bounded report/family projections;
- durable active+reserved capacity accounting and attention projections.

These are controller facts, not live qualification of the installed external products.

### 1.2. Still unqualified or incomplete

- actual Muse Code Max inference, Windows launch, resume and child recovery;
- actual installed OpenCode restart/continuation behavior;
- Codex write/send route and host registration;
- complete native family coverage where upstream APIs are non-atomic;
- full GM wake path; current safe mode remains checkpoint polling;
- CheckRunner reproducible cache and reverse-dependency scope;
- scheduled-work registry;
- forge publication;
- automatic module/service installation;
- remote HTTP/Streamable MCP endpoint;
- OpenAI MCP Events;
- Meta Muse custom connector/MCP contract.

### 1.3. Documentation drift

`docs/documentation-program-implementation-review.md` is a dated review of baseline `c99a71f`. Several items it listed as Phase B work have since landed. It remains historical evidence, not the current readiness matrix.

The README still contains a sentence saying the Documentation Program path will exist only after PR #13 is merged. PR #13 is already merged. That sentence should be removed in the next small documentation cleanup.

## 2. Product identities must not be conflated

### 2.1. Muse Code

`modules/muse/` integrates **Muse Code** through the official Muse Code SDK/MSP. This is a native coding harness and an ELIOT runtime module.

### 2.2. Meta Muse personal agent

Meta Muse is a separate cloud personal-agent product running in a Muse Secure VM with its own browser and connected apps. The reviewed official Meta material describes first-party and partner connectors, user approvals and browser/computer work.

No reviewed official Meta source currently defines a public custom-MCP, custom-connector or skill SDK for arbitrary private services. Therefore:

- direct `Meta Muse -> custom MCP` is `UNVERIFIED`;
- the Muse Code SDK is not a control API for the personal Muse agent;
- a browser path through a protected operator surface is the first implementable pilot;
- a typed connector path may be added only when Meta publishes an official contract or an installed product exposes and qualifies one.

### 2.3. OpenAI Dot

Dot is a personal OpenAI agent that can use plugins shared with ChatGPT/Work/Codex and can optionally access a connected local computer. Dot and the Responses API are not the same runtime, but both can use an MCP server through supported OpenAI surfaces.

The current OpenAI plan boundary matters. On ChatGPT Pro, developer-mode custom MCP is currently limited to read/search. Full write MCP is documented for Business and Enterprise/Edu. Therefore the initial Dot plan is:

1. read-only Swarm visibility through Secure MCP Tunnel;
2. write/control through a dedicated local-computer skill or Codex/Work task, with the normal ELIOT credential and exact CLI/application methods;
3. migrate writes to typed MCP only when the account/workspace supports full MCP actions.

Do not promise Dot-as-full-GM through custom write MCP on a plan that does not expose it.

## 3. Invariants

### 3.1. One authority

```text
ELIOT Store
  owns Task / Attempt / Operation / Submission / Acceptance

Remote Agent Gateway
  authenticates, filters and forwards

OpenAI / Meta / Cloudflare
  transport or agent runtime only
```

A tunnel acknowledgement, browser click, MCP tool result or agent statement never becomes Task acceptance by itself.

### 3.2. One physical-session owner

For each native Muse Code, OpenCode, Codex or other physical session, exactly one ELIOT runtime adapter/backend owns control. Dot and Meta Muse control ELIOT; they do not attach directly as competing owners to native worker sessions.

### 3.3. One GM epoch

At most one external agent is designated GM. Another external agent may be observer, reviewer or standby. Promotion uses existing explicit `gm.handover`; no hostname, connector login or recent activity grants GM authority.

### 3.4. Transport is not policy

Cloudflare Tunnel, OpenAI Secure MCP Tunnel and HTTPS solve reachability. ELIOT decides:

- authenticated principal;
- role and GM epoch;
- visible tools;
- method admission;
- binding and generation scope;
- request identity;
- budgets and capacity;
- approvals;
- result evidence;
- audit;
- shutdown and handover.

### 3.5. Unknown remains unknown

Loss of an MCP response, tunnel disconnect, browser timeout or Access session expiry does not prove that a mutation failed. A caller that performs a mutation creates and retains `client_request_id` before dispatch, then reconciles the resulting Operation instead of inventing a new request.

## 4. Remote Agent Gateway boundary

### 4.1. Initial implementation shape

The first gateway is not another daemon with its own state machine. It is a client of the running ELIOT host, like the CLI and current MCP facade.

```text
external connection
  -> authenticated remote profile
  -> method/tool allowlist
  -> local IPC client
  -> existing application API
  -> durable Operation
```

No gateway component opens the SQLite database.

### 4.2. Two transport forms

#### Private stdio profile

Used by OpenAI Secure MCP Tunnel:

```text
tunnel-client --mcp-command
  -> `swarm mcp --credential <local profile credential>`
```

This reuses current RMCP stdio and adds no inbound listener.

#### Loopback Streamable HTTP profile

Used behind Cloudflare:

```text
cloudflared
  -> 127.0.0.1:<gateway-port>/mcp
  -> Streamable HTTP MCP facade
  -> local IPC
```

This is not currently implemented. Prefer native RMCP Streamable HTTP support or a small reviewed complete bridge. Do not maintain another hand-written SSE/JSON-RPC state machine if an upstream package supplies the transport.

The HTTP listener binds loopback only. `cloudflared` is the only expected ingress peer.

### 4.3. Tool filtering must occur twice

A remote profile:

1. exposes only allowed tools in `tools/list`;
2. rejects every non-allowed method before local IPC dispatch.

Filtering only the manifest is cosmetic and is not a security boundary.

Application role/GM checks remain mandatory after the gateway allowlist.

### 4.4. Separate source-reading service

A read-only filesystem/code search MCP is a different service from Swarm control. Keep separate:

```text
swarm-control
workspace-read
```

They use separate endpoints, credentials, tool catalogs and audit records. The Remote Agent Gateway does not add arbitrary filesystem, shell or Git tools to the Swarm control catalog.

## 5. OpenAI Dot integration

### 5.1. Preferred read-only path: Secure MCP Tunnel

Use OpenAI's outbound-only `tunnel-client` next to the local controller. Point it to the existing stdio MCP command. The local server remains private and no domain is required.

Local setup facts:

- tunnel identity belongs in the local tunnel-client profile;
- the tunnel runtime API key belongs in a protected local secret/environment reference;
- the MCP command names a local ELIOT credential file;
- ChatGPT/Plugin setup receives only the tunnel association/ID;
- the repository contains no tunnel IDs, organization IDs, workspace IDs, API keys or local endpoint values.

Initial Dot principal: `observer`.

Initial Dot tools:

- `host.status`
- `route.list`
- `task.get`, `task.list`, `task.submission`, `task.acceptance`
- `attempt.get`
- `operation.get`, `operation.list`
- `agent.state`, `agent.list`, `agent.family`
- `check.get`, `check.profiles`
- `artifact.get`, `artifact.read`, `artifact.parts`
- `report.delta`, `report.capacity`, `report.attention`
- `message.read`
- `doctor.inspect`

Actual names must follow the application API/tool registry on the implementing commit. A tool not present in code is not advertised from this document.

### 5.2. Pro write-path fallback: local computer skill

Until full custom write MCP is available to the user's plan, Dot can use its explicitly connected local computer to run a narrow local skill that invokes the existing `swarm` CLI/application methods.

Requirements:

- separate manager credential, never operator credential;
- no generic shell instruction exposed to Dot;
- the skill maps named actions to exact `swarm` commands and closed JSON schemas;
- every mutation requires caller-minted `client_request_id`;
- high-impact methods require user/local approval;
- stdout/stderr and Operation IDs are returned without secrets;
- local computer access remains optional and revocable.

This is a compatibility path, not the long-term transport. Once full MCP writes are available, the same action schemas move behind the MCP manager profile.

### 5.3. Future full-MCP manager profile

Candidate write methods:

- task creation/revision/claim/dispatch;
- addressed message send/cancel;
- addressed agent input/background/reply/goal;
- source capture/check start;
- request changes and submission reads.

Keep out of unattended default:

- `task.accept`;
- acceptance invalidation;
- `gm.handover`;
- host admission mode;
- cancellation of active native work;
- forge publication/merge/push;
- module install/update;
- service lifecycle;
- credential/client administration.

Those operations require explicit policy and normally a user approval or local operator.

### 5.4. Dot rules

Dot Custom Rules are defense in depth, not the authorization boundary. Recommended rules:

- use only tools exposed by the selected ELIOT profile;
- never invent a new request ID when the previous mutation outcome is unknown;
- do not accept or publish work without the configured human/local approval;
- do not change host mode, GM designation, credentials or services;
- do not treat silence, idle or a disconnected tunnel as completion;
- hand off consequential or ambiguous actions.

Server-side policy must remain correct if these prose rules are ignored.

## 6. Meta Muse personal-agent integration

### 6.1. Current supported pilot: protected browser surface

Because no public custom-MCP contract was established, the first Muse path is a thin web operator surface accessible from Muse's cloud browser through Cloudflare Access.

The UI is not a new scheduler. It renders typed ELIOT projections and submits the same closed application methods.

First pilot is read-only:

- host readiness;
- open Tasks and Attempts;
- current Operations;
- attention queue;
- capacity/quota incidents;
- pending native questions;
- check/submission/acceptance state;
- artifact excerpts through bounded reads.

The page must expose structured, accessible labels and stable identifiers. Do not make Muse scrape terminal output or infer control state from colors.

### 6.2. Controlled write pilot

Only after read-only qualification:

- one exact addressed message;
- one Task create/revise operation;
- one addressed agent input;
- one reply to an exact current native request.

Every action page shows target identity, generation, scope and expected result before submission. Consequential operations require local/user confirmation.

Muse browser automation is not called a typed connector. Its result must be read back from ELIOT.

### 6.3. Future connector path

If Meta publishes a custom connector/MCP/OAuth contract:

- prefer Streamable HTTP MCP;
- authenticate through the official connector flow;
- map connector identity to a dedicated ELIOT principal;
- reuse the same server-side tool profiles;
- qualify admission, unknown outcome, approvals and reconnect;
- remove browser automation only after equivalent typed behavior is demonstrated.

Do not build against reverse-engineered internal Muse APIs.

### 6.4. Muse approvals

Meta's own Sentinel/user approval does not replace ELIOT approval. It is an outer safeguard. ELIOT still enforces principal, GM epoch, tool allowlist, target generation and method-specific rules.

## 7. Cloudflare path

### 7.1. Role

Cloudflare provides:

- outbound tunnel from the Windows host;
- TLS, DDoS/WAF and hostname routing;
- Access identity/service authentication;
- an immediate route-level killswitch.

It does not own ELIOT Tasks, agents, sessions or authorization policy.

### 7.2. Current local runbook status

The supplied local runbook records:

- `cloudflared` service is running;
- the MCP ingress is intentionally disabled with an HTTP 403 route;
- the local gateway and upstream filesystem MCP are stopped;
- the previous filesystem backend binary is absent and must be replaced.

That local runbook contains private infrastructure identifiers. It must not be copied into this repository.

### 7.3. Authentication

Preferred choices:

#### Human/browser Muse access

Cloudflare Access browser authentication. The origin validates `Cf-Access-Jwt-Assertion`, including signature, audience and expiry.

#### OAuth-capable MCP client

Cloudflare Access Managed OAuth for an MCP server application, provided the client supports the required OAuth flow and the origin validates the Access JWT.

#### Headless machine client

Access service token with a Service Auth policy. Use the normal two headers, or the documented single-header mode only when a client cannot send both. Test the exact client before relying on it.

In all cases, Access identity maps to a fixed ELIOT principal/profile. The origin may also require an application-level credential as defense in depth.

### 7.4. Protocol

New public MCP work uses Streamable HTTP. Historical SSE aliases or an existing SSE bridge do not justify maintaining deprecated transport semantics.

Required endpoint shape in repository examples:

```text
https://YOUR_DOMAIN/swarm/mcp
```

The actual hostname is local deployment data.

### 7.5. Gateway hardening

- bind loopback only;
- mandatory Access validation;
- closed request schemas and body limits;
- post-auth per-principal rate/concurrency budgets;
- no arbitrary upstream URL;
- no arbitrary headers passed to native tools;
- server-side tool allowlist before dispatch;
- no raw database, filesystem or shell passthrough;
- redact tokens, headers, endpoints and native diagnostic secrets;
- preserve Operation ID and `client_request_id`;
- record Cloudflare subject/service-token identity separately from ELIOT principal;
- return typed gaps rather than fabricated empty states.

## 8. Local enrollment and domain privacy

### 8.1. Repository rule

Public repository content may contain only placeholders:

```text
https://YOUR_DOMAIN/swarm/mcp
https://mcp.example.com/swarm/mcp
${ELIOT_REMOTE_MCP_URL}
${ELIOT_CLOUDFLARE_ACCESS_AUD}
${ELIOT_CLOUDFLARE_TEAM}
${ELIOT_DOT_TUNNEL_ID}
```

Forbidden in source, documentation, examples, commits, PR bodies, fixtures and test snapshots:

- the operator's real domain or subdomains;
- Cloudflare account/zone/tunnel/team identifiers;
- Access audience values;
- OpenAI tunnel, organization or workspace IDs;
- API keys, service-token IDs/secrets or bearer tokens;
- local usernames and credential paths copied from a real installation.

### 8.2. Install-time profile

Proposed local-only profile location:

```text
%LOCALAPPDATA%\Eliot\remote-agents\<profile>\
  profile.toml
  credential.json
  transport\
```

Exact storage may reuse existing ELIOT configuration conventions. The invariant is that it is outside the repository, user-restricted and excluded from source capture.

A local setup command or installer asks the operator for:

- channel: `openai_tunnel`, `cloudflare_browser`, `cloudflare_mcp`;
- principal/profile;
- local host data directory/config;
- tunnel ID or external endpoint;
- secret references, never literal secrets in generated public examples;
- allowed tool profile;
- approval policy;
- whether this agent may be a GM candidate.

It validates connectivity locally, then provides setup instructions to the external product. Agents receive endpoint/tunnel association during local installation, not from repository text or Task prompts.

### 8.3. Logging

Persist:

- remote profile ID;
- transport kind;
- authenticated external subject hash/stable ID;
- ELIOT principal;
- tool/method;
- request and Operation IDs;
- decision/outcome;
- timestamp.

Do not persist:

- raw secrets;
- authorization headers;
- full private endpoint when a profile ID suffices;
- browser cookies;
- Cloudflare tunnel credentials;
- OpenAI runtime API keys.

## 9. Tool profiles and principals

### 9.1. `remote_observer`

Read-only. No mutation is merely hidden: it is rejected before dispatch.

### 9.2. `remote_manager`

Can create/claim/dispatch work and send addressed communication under normal ownership rules. Cannot become GM by profile alone.

### 9.3. `remote_gm_candidate`

Same tool catalog as the accepted GM policy permits, but GM-only application methods work only after explicit `gm.handover` advances the current epoch to this client.

### 9.4. `remote_reviewer`

Reads exact candidate/check/submission evidence and may issue an addressed review verdict only if the existing application role/acceptance policy permits it. It does not inherit writer credentials.

### 9.5. Profile implementation

Current MCP tool catalog is static. Add an explicit profile filter to the facade:

```text
configured profile
  -> tool catalog
  -> pre-dispatch method allowlist
  -> application authorization
```

Do not fork separate copies of every application method or create a second remote API.

## 10. GM ownership and failover

Recommended initial deployment:

```text
Dot        = primary GM candidate after qualification
Meta Muse  = observer/reviewer/standby
local user = operator and ultimate recovery authority
```

Reasons:

- Dot has an official private MCP tunnel and shared plugin permissions;
- Meta Muse currently lacks a reviewed custom private-service connector contract;
- only one external agent should mutate the controller as GM.

Promotion sequence:

1. disable new high-impact remote actions;
2. reconcile outstanding Operations;
3. inspect attention/mailbox;
4. explicitly hand over GM;
5. verify new GM epoch;
6. re-enable only the accepted profile;
7. former GM becomes observer or is disconnected.

Network failover does not perform GM handover automatically.

## 11. Wake and events

### 11.1. Initial mode

Use the implemented `checkpoint_poll`:

- `report.delta`;
- `report.attention`;
- `message.read`;
- `operation.get`.

No hidden model heartbeat is introduced.

### 11.2. OpenAI MCP Events

A future Dot plugin may expose typed events such as:

```text
attention.created
operation.terminal
submission.ready
check.terminal
gm.handover.required
quota.changed
```

MCP Events require the standard event catalog/subscription contract, durable webhook subscriptions, verified HTTPS callbacks, signing, expiration/refresh, retry/backoff and replay/gap semantics. Current ELIOT RMCP stdio facade implements its own bounded session-scoped freshness subscriptions over committed facts; those are useful for connected MCP clients but are not the OpenAI MCP Events webhook contract.

Event delivery is notification, not acceptance or a new Operation.

### 11.3. Muse wake

Until an official connector event contract exists, Muse uses browser refresh/polling or a supported connected communication channel. Natural-language messages are not workflow authority and cannot carry implicit approval for high-impact actions.

## 12. Lifecycle and killswitch

### 12.1. Dot

- revoke/disable the Dot plugin connection or tunnel association;
- stop `tunnel-client`;
- revoke its runtime API key;
- revoke the dedicated ELIOT credential;
- remove GM designation through explicit handover if applicable.

### 12.2. Cloudflare/Muse

- disable the Access policy/service token;
- change the ingress route to an explicit denial;
- stop the loopback gateway;
- revoke the dedicated ELIOT credential;
- leave unrelated local runtimes and already admitted work untouched.

### 12.3. Controller

`host.mode new_work=disabled` stops new work admission; it does not kill ongoing native families. Emergency cancellation remains an explicit, addressed operation with the existing ownership/evidence rules.

## 13. Implementation program

### G0 — documentation and privacy

**Status:** this document.

Acceptance:

- no private domain or infrastructure identifier in repository content;
- Dot, Meta Muse personal agent and Muse Code are distinct;
- transport and authority are distinct;
- plan/feature limitations are explicit.

### G1 — MCP tool profiles

Add profile selection to `swarm mcp`:

- closed named profiles;
- filtered `tools/list`;
- pre-dispatch rejection;
- same application API;
- dedicated credential;
- profile reported in server info/audit.

Negative cases:

- observer invokes mutation by guessed tool name;
- remote manager invokes operator/GM-only method;
- profile changes while a process is running;
- unknown profile.

### G2 — Dot read-only Secure MCP Tunnel pilot

No controller network listener.

Qualification:

- local stdio MCP through `tunnel-client`;
- exact tool discovery;
- tunnel reconnect;
- 256 KiB/64 KiB projection boundaries;
- read-only enforcement;
- plugin disconnect does not stop host or native work;
- no domain or runtime key in logs/repository;
- current Pro plan behavior recorded.

### G3 — Dot local skill control pilot

- narrow local skill/Codex task;
- exact command/schema mapping;
- dedicated manager credential;
- caller-owned request IDs;
- approval gates;
- unknown outcome reconciliation;
- no general shell surface.

This slice may be retired when full write MCP is available and qualified.

### G4 — loopback Streamable HTTP gateway

- reuse RMCP transport or reviewed complete bridge;
- loopback only;
- local IPC client;
- Cloudflare identity mapping;
- Access JWT validation;
- tool profiles;
- bounded request/result bodies;
- no DB access;
- disconnect-independent controller work.

### G5 — Cloudflare/Muse read-only browser pilot

- Access-protected semantic operator view;
- exact read projections only;
- no secrets/domain in generated artifacts;
- browser session expiry/relogin;
- prompt-injection probes;
- stale/partial/unknown rendered honestly.

### G6 — Muse controlled actions

Only after G5:

- one addressed message;
- one Task mutation;
- one addressed input;
- one native-request reply;
- readback after every action;
- explicit local/user approval for consequential actions.

No GM role yet.

### G7 — GM pilot and handover

- Dot as first GM candidate;
- Muse remains standby;
- exact epoch and stale former-GM denial;
- disconnect does not imply handover;
- pending Operations remain addressable;
- local operator can recover.

This depends on the unresolved GM rotation contract in issue #8.

### G8 — typed events

- MCP Events for Dot when protocol/runtime support is selected;
- durable subscription identity and expiration;
- callback verification/signing;
- delivery retry and explicit gaps;
- event-specific authorization;
- no duplicate model work from repeated delivery.

## 14. Acceptance matrix

### Privacy

```text
[ ] repository scan contains no deployment hostname, account/tunnel/audience ID or secret
[ ] examples use only placeholders
[ ] source capture excludes local profiles
[ ] diagnostics redact endpoint/auth material
[ ] support bundle contains profile IDs, not secrets
```

### Authority

```text
[ ] one ELIOT Task/Operation authority
[ ] one session owner per native binding
[ ] one current GM epoch
[ ] gateway cannot bypass application authorization
[ ] tool manifest and dispatch allowlists agree
[ ] network disconnect does not settle work
```

### Dot

```text
[ ] Secure MCP Tunnel reaches local stdio MCP without public ingress
[ ] observer profile is genuinely read-only
[ ] Pro plan limitation recorded
[ ] local skill write path has no generic shell
[ ] caller retains request ID before mutation
[ ] lost reply is reconciled, not replayed with a new ID
[ ] plugin/tunnel disconnect leaves host and admitted work running
```

### Cloudflare

```text
[ ] cloudflared ingress reaches loopback gateway only
[ ] catch-all denies unknown routes
[ ] Access authentication required
[ ] Access JWT signature/audience/expiry validated at origin
[ ] service-token header mode qualified against exact client
[ ] Streamable HTTP initialization and reconnect qualified
[ ] route-level denial is an effective killswitch
```

### Meta Muse

```text
[ ] no claim of custom MCP support without official/installed evidence
[ ] browser view is read-only in first pilot
[ ] stable identifiers and explicit stale/partial/unknown states
[ ] action target and result readback are shown
[ ] Sentinel/user approval is not substituted for ELIOT authorization
[ ] prompt-injection and stale-browser-session cases exercised
[ ] Muse never competes for native worker-session ownership
```

### GM

```text
[ ] explicit handover only
[ ] old epoch denied at admission and begin-send
[ ] no automatic handover on outage
[ ] mailbox/attention behavior across rotation follows issue #8 decision
[ ] high-impact actions require accepted policy/approval
```

## 15. Source registry

### ELIOT

- [README](../README.md)
- [Architecture](agent_swarm.md)
- [Module contract](agent_swarm.module-contract-v2.md)
- [Implementation plan](agent_swarm.implementation-v6.md)
- [Documentation Program](documentation-program.md)
- [Implementation Review](documentation-program-implementation-review.md)
- [MCP facade](../src/mcp.rs)
- [Muse Code module](../modules/muse/README.md)

### OpenAI

- [Getting started with your dot](https://help.openai.com/en/articles/20001530-getting-started-with-your-dot)
- [Dots privacy, security, and safety](https://help.openai.com/en/articles/20001529-dots-privacy-security-and-safety-faqs)
- [Secure MCP Tunnel](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels)
- [MCP servers](https://developers.openai.com/api/docs/guides/tools-connectors-mcp)
- [Developer mode and MCP apps](https://help.openai.com/en/articles/12584461-developer-mode-and-full-mcp-connectors-in-chatgpt-beta)
- [MCP Events](https://developers.openai.com/plugins/build/mcp-events)

### Meta

- [Introducing Muse](https://about.fb.com/news/2026/09/introducing-muse-personal-ai-agent/)
- [Muse for Small Business](https://about.fb.com/news/2026/09/introducing-muse-small-business/)

### Cloudflare

- [Cloudflare Tunnel](https://developers.cloudflare.com/tunnel/)
- [Managed OAuth for MCP applications](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/managed-oauth/)
- [Service tokens](https://developers.cloudflare.com/cloudflare-one/access-controls/service-credentials/service-tokens/)
- [Validate Access JWTs](https://developers.cloudflare.com/cloudflare-one/access-controls/applications/http-apps/authorization-cookie/validating-json/)
- [Cloudflare MCP transport guidance](https://developers.cloudflare.com/agents/model-context-protocol/cloudflare/servers-for-cloudflare/)

## 16. Final recommendation

Implement in this order:

```text
1. MCP tool profiles
2. Dot observer through OpenAI Secure MCP Tunnel
3. Dot local-skill manager pilot
4. loopback Streamable HTTP gateway behind Cloudflare Access
5. Muse read-only browser pilot
6. controlled Muse actions
7. one explicit GM handover pilot
8. typed events
```

Do not block completion of the local controller on remote-agent integration. Remote agents are additional clients of the existing authority, not a replacement core.
