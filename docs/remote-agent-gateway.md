# ELIOT Remote Agent Gateway
## OpenAI Dot, Meta Muse Agent and Cloudflare integration program

**Revision:** 3 — 2026-10-02  
**Repository baseline:** `0ff7129d11a15a71a94e1ed2c265f7a3eeb0e1db`  
**Status:** documentation and implementation handoff. Nothing here is live-qualified merely because this document exists.

## 0. Decision

ELIOT keeps one Task/Attempt/Operation authority, one physical-session owner, one GM epoch and one protected acceptance path.

Remote agents do not receive a second scheduler, direct database access, arbitrary shell passthrough or a competing native-session owner. They reach the existing application API through a narrow **Remote Agent Gateway** with a dedicated ELIOT principal and a server-enforced tool profile.

Recommended connectivity:

```text
OpenAI Dot
  OpenAI Secure MCP Tunnel
    -> local tunnel client
    -> existing `swarm mcp` stdio facade
    -> local ELIOT IPC
    -> ELIOT host

Meta Muse personal agent
  preferred typed pilot:
    Muse Custom Connector
      -> Cloudflare-protected narrow HTTPS API
      -> loopback Remote Agent Gateway
      -> local ELIOT IPC
      -> ELIOT host
  fallback:
    Muse Secure VM browser
      -> Cloudflare Access
      -> thin read-only operator surface

Other remote clients
  HTTPS Streamable MCP
    -> Cloudflare Access
    -> cloudflared
    -> loopback Remote Agent Gateway
    -> local ELIOT IPC
    -> ELIOT host
```

OpenAI Secure MCP Tunnel is preferred for Dot because the MCP server stays private and needs no public hostname. Cloudflare is the general external ingress for Muse and other clients. Connectivity is not authorization: every path terminates in the same ELIOT application checks, request receipts, binding generations and GM epoch.

## 1. Current repository stage

### 1.1 Implemented and preserved

Current `main` already contains:

- durable Tasks, Attempts, Operations and caller-owned request receipts;
- immutable source/result artifacts, submissions, independent review and acceptance;
- fixed-source CheckRunner with process-group ownership and conservative recovery;
- authenticated local IPC; no public controller listener;
- stdio RMCP facade over the same application API as the CLI;
- MCP Tasks projected from existing Operations, without a second task authority;
- bounded MCP subscriptions over committed report/mailbox/Operation facts, with explicit lag and authoritative read-resync;
- GM designation, epoch and explicit handover;
- OpenCode V2 direct HTTP adapter with exact input/log correlation, child reads, result export and addressed `agent.background`;
- Muse Code SDK bridge.7 with exact steer/settings/goal/replies, checkpoint recovery, explicit child freshness and durability/host-death/gap observations;
- OpenCodex 2.75 observer/configuration bridge.3;
- typed mailbox delivery identity, reply binding and cancellation;
- bounded report/family projections;
- durable active+reserved capacity accounting and attention projections.

Do not redesign these foundations for remote-agent access.

### 1.2 Still incomplete or unqualified

- actual Muse Code Max/Windows/resume/children qualification;
- installed OpenCode restart/continuation behavior;
- admitted Codex write/send route and host registration;
- complete family coverage where upstream APIs are non-atomic;
- full GM wake path; safe mode remains checkpoint polling;
- CheckRunner reusable cache and reverse-dependency scope;
- scheduled work, forge publication and automatic module installation;
- loopback Streamable HTTP/narrow REST gateway;
- OpenAI MCP Events;
- installed Meta Muse Custom Connector schema/auth/retry qualification.

### 1.3 Owner decisions and Issue audit

[Owner Decisions](owner-decisions.md) publishes Owner Policy v1 and resolves the contract questions from issues #2, #6, #7, #8, #10, #12, #14 and #15.

- #2 and #15 are decision-complete.
- #6, #7, #8, #10, #12 and #14 are now narrow implementation/qualification tasks.
- #3, #4, #5, #9 and #11 remain evidence/live-qualification work.
- #19 stays gated because this gateway is MCP/HTTP, not an ACP consumer.
- #20 is the immediate MCP-facade dependency for remote profiles.
- #1 now tracks only OpenCodex native-route composition and mixed-provider qualification; the observer/configuration module already exists.

## 2. Product identities

### 2.1 Muse Code

`modules/muse/` integrates **Muse Code** through the official Muse Code SDK/MSP. It is a native coding harness and an ELIOT runtime module.

### 2.2 Meta Muse personal agent

Meta Muse personal agent is a separate cloud product running in a Muse Secure VM with browser/computer capabilities and connected apps.

Official Meta material establishes two connector paths:

- a partner Connector Platform for reviewed/directory-distributed connectors;
- user-created **Custom Connectors**, which Muse can construct after retrieving API information and whose credentials are held in Muse's Secure Credentials Store.

Public Meta documentation does not establish that Custom Connectors use MCP and does not publish a complete connector wire/schema/auth contract. Therefore:

- preferred pilot: `Muse Custom Connector -> narrow ELIOT HTTPS API`;
- direct `Muse -> custom MCP` remains **UNVERIFIED** until the installed product proves it;
- the Muse Code SDK is not a control API for the personal Muse agent;
- the partner platform is unnecessary for a private single-operator connector;
- browser automation is fallback only when the Custom Connector cannot express the required API/authentication.

### 2.3 OpenAI Dot

Dot is a personal OpenAI agent that can use shared plugins and an optional connected local computer. Dot is not the Responses API runtime, but supported OpenAI surfaces can reach MCP servers.

Current plan boundary:

- on ChatGPT Pro, custom MCP in developer mode is read/search only;
- full custom MCP actions are documented for supported Business/Enterprise/Edu workspaces.

Initial plan:

1. Dot observer through Secure MCP Tunnel;
2. narrow local-computer skill or Codex/Work task for exact ELIOT mutations;
3. move writes to typed MCP only when the account/workspace supports them.

Do not promise Dot-as-full-GM through write MCP on a plan that does not expose it.

## 3. Invariants

### 3.1 One authority

```text
ELIOT Store
  owns Task / Attempt / Operation / Submission / Acceptance

Remote Agent Gateway
  authenticates, filters and forwards

OpenAI / Meta / Cloudflare
  transport or remote-agent runtime only
```

A tunnel acknowledgement, browser click, connector response or MCP tool result is never Task acceptance by itself.

### 3.2 One native owner

Dot and Meta Muse control ELIOT; they do not attach as competing controllers to Muse Code, OpenCode, Codex or another physical worker session.

### 3.3 One GM epoch

At most one external agent is current GM. Promotion uses `gm.handover`. Hostname, connector login, recent activity or tunnel reconnect does not grant GM authority.

### 3.4 Unknown remains unknown

Lost MCP/HTTP response, tunnel disconnect, browser timeout or expired Access session does not prove a mutation failed. Mutating callers retain `client_request_id` before dispatch and reconcile the resulting Operation.

### 3.5 Transport is not policy

Cloudflare Tunnel and OpenAI Secure MCP Tunnel solve reachability. ELIOT decides:

- principal and role;
- visible tools and method admission;
- Task/binding/generation scope;
- request identity and idempotency;
- budgets/capacity;
- approval and attention rights;
- evidence, acceptance and handover.

## 4. Gateway boundary

The gateway is a client of the running ELIOT host, like the CLI and existing MCP facade.

```text
external transport
  -> remote profile and identity mapping
  -> discovery allowlist
  -> pre-dispatch allowlist
  -> local IPC client
  -> existing application API
  -> durable Operation/read model
```

The gateway never opens SQLite.

### 4.1 Private stdio path

OpenAI Secure MCP Tunnel launches/connects to:

```text
swarm mcp --credential <local-profile-credential>
```

No inbound listener is introduced.

### 4.2 Loopback HTTP path

Cloudflare path:

```text
cloudflared
  -> 127.0.0.1:<gateway-port>
  -> Streamable HTTP MCP or narrow REST surface
  -> local IPC
```

Requirements:

- loopback bind only;
- bounded bodies/timeouts;
- no stale-on-error cache for authoritative reads;
- no arbitrary method passthrough;
- controller work survives gateway disconnect;
- exact identity/profile mapping;
- route-level killswitch independent from ELIOT host lifecycle.

## 5. Tool profiles

Issue #20 implements profiles at the existing MCP facade. Filtering must occur twice:

1. `tools/list` exposes only allowed tools;
2. pre-dispatch gate rejects a manually addressed hidden tool before local IPC write.

Initial profiles:

| Profile | Purpose | Allowed surface |
|---|---|---|
| `observer` | Dot/Muse read-only pilot | status, Tasks/Attempts/Operations reads, reports, attention/capacity, family, checks, submissions/acceptance reads, bounded artifacts |
| `reviewer` | independent review | observer + explicitly selected review/request-changes methods |
| `manager` | controlled remote manager | selected Task/message/agent methods; no generic shell/forge/module/admin |
| `gm` | current designated GM | manager + current GM-authorized methods; still subject to epoch/application checks |

A profile cannot elevate the ELIOT role. Both profile and application authorization must pass.

No universal shell, generic JSON-RPC passthrough or arbitrary filesystem tool is added.

## 6. Cloudflare plan

### 6.1 Current local state from the supplied runbook

- `cloudflared` service is installed/running;
- the MCP ingress is deliberately disabled with a 403 route;
- local gateway and old filesystem upstream are stopped;
- the old filesystem backend is absent and needs replacement if that separate route is restored.

Do not revive the filesystem backend as a prerequisite for Swarm. Add a separate Swarm route/profile.

### 6.2 Authentication

Cloudflare provides edge identity, not ELIOT authority.

Supported choices after exact client qualification:

- browser operator: Cloudflare Access interactive login;
- OAuth-capable MCP client: Access Managed OAuth;
- headless client: service token only if the client can send the required headers;
- origin validates Access JWT signature, audience and expiry;
- optional MCP Server Portal may aggregate external MCP servers, but it never becomes Task/Operation authority.

The existing simple bearer gateway is not copied as the final Swarm authorization layer. If retained temporarily, bearer-to-principal mapping must still terminate in an ELIOT credential/profile and never bypass application checks.

### 6.3 Route separation

Use separate routes/profiles for:

```text
swarm-observer/control
workspace-read/search
```

Filesystem/code-search access is not implicitly granted to a Swarm manager and vice versa.

## 7. Privacy and local setup

The real deployment domain and infrastructure identifiers are local installation values, never repository content.

Repository examples use only:

```text
https://YOUR_DOMAIN/swarm/mcp
https://mcp.example.com/swarm/mcp
${ELIOT_REMOTE_MCP_URL}
${ELIOT_CLOUDFLARE_ACCESS_AUD}
${ELIOT_CLOUDFLARE_TEAM}
${ELIOT_DOT_TUNNEL_ID}
```

Do not commit/log:

- real domains/subdomains;
- Cloudflare account, zone, tunnel, team or audience IDs;
- OpenAI workspace/tunnel IDs;
- local usernames/absolute private paths;
- service tokens, credentials or private keys.

Local installer/setup obtains:

- channel type;
- local profile/principal;
- endpoint/tunnel association;
- secret references;
- tool profile;
- approval policy;
- optional GM-candidate flag.

Store values in a user-restricted local profile outside the repository. Tasks and agent prompts receive profile identity/capabilities, not the deployment hostname or secret.

## 8. OpenAI Dot program

### G1 prerequisite

Finish issue #20: project Phase-B methods and add server-enforced tool profiles.

### Dot observer

- create dedicated observer principal/credential;
- expose observer profile through Secure MCP Tunnel;
- verify tools/resources/task projections;
- verify reconnect/resync and lag behavior;
- verify no write tool is discoverable or dispatchable;
- plugin/tunnel disconnect must not stop host/native work.

### Dot controlled mutations

Until write MCP is available on the selected plan, use a narrow local skill/Codex task:

```text
named action
  -> closed JSON schema
  -> exact swarm CLI/application method
  -> caller-minted client_request_id
  -> durable Operation
  -> readback/reconciliation
```

No general shell command. No operator credential. No automatic mutation retry after a lost response.

## 9. Meta Muse program

### Muse Custom Connector read-only pilot

1. Expose a narrow read-only HTTPS API/profile.
2. On the installed Muse product, record exact Custom Connector request, authentication, response and retry behavior.
3. Map connector identity to a dedicated observer principal.
4. Return typed bounded projections, not terminal logs or raw DB rows.
5. Test hostile connector data/prompt injection, stale state and partial observations.
6. Keep browser/Access surface as tested fallback.

Initial reads:

- readiness/Doctor;
- Tasks/Attempts/Operations;
- attention and capacity/quota incidents;
- pending native questions;
- checks/submissions/acceptance;
- bounded artifact excerpts.

### Controlled actions

Only after read-only qualification, add one at a time:

1. addressed message;
2. one Task mutation;
3. one addressed agent input;
4. one reply to an exact pending native request.

Each action shows target identity/generation, uses caller-owned request ID and performs ELIOT readback. Muse/Sentinel approval is additive; it is not ELIOT authorization.

Muse remains observer/reviewer/standby until explicit GM qualification.

## 10. GM pilot

Default candidates:

```text
OpenAI Dot = first remote GM candidate after qualification
Meta Muse  = observer/reviewer/standby
local user = operator and recovery authority
```

Promotion sequence:

1. stop new high-impact remote admissions;
2. reconcile outstanding Operations/attention;
3. execute explicit `gm.handover`;
4. verify new epoch and stale former-GM rejection;
5. successor resyncs authoritative report/mailbox cursors;
6. former GM becomes observer or disconnects.

Tunnel/browser/connector loss never performs handover automatically. Owner Policy v1 and issue #8 define the remaining contract.

## 11. Implementation order

| Slice | Work |
|---|---|
| G0 | documentation, privacy boundary, owner decisions |
| G1 | issue #20 Phase-B MCP projection + named tool profiles |
| G2 | Dot observer through Secure MCP Tunnel |
| G3 | narrow Dot local-skill mutation pilot |
| G4 | loopback Streamable HTTP/narrow REST gateway behind Cloudflare Access |
| G5 | Muse Custom Connector read-only pilot; browser fallback |
| G6 | four controlled Muse actions |
| G7 | explicit GM handover pilot under Owner Policy v1 |
| G8 | standard durable MCP Events when selected OpenAI surface supports them |

Remote integration does not block completion of the local controller.

## 12. Negative acceptance cases

### Privacy

- repository scan contains no real deployment hostname, account/tunnel/audience ID or secret;
- support bundle contains profile IDs, not secrets;
- diagnostics redact endpoint/auth material.

### Authorization

- observer cannot discover or manually invoke a mutation;
- profile cannot elevate application role;
- stale former-GM epoch is denied;
- connector/tunnel identity does not imply GM.

### Delivery

- caller retains request ID before mutation;
- lost reply is reconciled, not replayed with a new ID;
- gateway disconnect leaves admitted controller/native work running;
- stale native request fingerprint is rejected.

### Cloudflare

- origin validates Access JWT when Access is selected;
- catch-all denies unknown routes;
- service-token header mode is qualified for the exact client;
- route 403/disable is an effective killswitch without stopping ELIOT host.

### Muse

- connector schema/auth/retry behavior is recorded from installed product;
- no claim of MCP without evidence;
- first connector is genuinely read-only;
- browser fallback remains read-only first;
- prompt injection/hostile connector data does not widen tools;
- Muse never competes for native worker-session ownership.

### Dot

- private tunnel reaches local stdio MCP without public ingress;
- Pro plan limitation is recorded;
- local skill has no generic shell;
- plugin/tunnel disconnect does not stop controller/native work.

## 13. Donor decisions

- Use ELIOT Store/CheckRunner/submission authority as-is.
- Use OpenAI Secure MCP Tunnel as a complete connectivity unit for Dot.
- Use Cloudflare Tunnel/Access/optional MCP Portal as transport and edge policy only.
- Use Meta Custom Connector as the preferred typed Muse pilot after installed-contract qualification.
- Borrow Agent of Empires' manifest/reapproval and active+reserved ideas, not its unsandboxed plugin host.
- Borrow Paseo live/canonical/gap projection semantics, not its second control plane.
- Borrow CCCC delivery identity/reply binding, already reflected in the mailbox.
- Keep ACPX gated until a real ACP consumer exists.
- Do not attach Multica, Poracode, Waku, Claw or another cockpit as a competing owner of the same native sessions.

## 14. Source registry

### ELIOT

- [Architecture](agent_swarm.md)
- [Module contract](agent_swarm.module-contract-v2.md)
- [Implementation plan](agent_swarm.implementation-v6.md)
- [Documentation Program](documentation-program.md)
- [Owner Decisions](owner-decisions.md)
- [MCP facade](../src/mcp.rs)
- [Muse Code module](../modules/muse/README.md)

### OpenAI

- Getting started with Dot
- Dot privacy/security/safety
- Secure MCP Tunnel
- MCP servers and developer-mode/full-MCP plan documentation
- MCP Events

### Meta

- Introducing Muse
- Muse for Small Business
- How Muse works with Connectors
- Muse Connector Platform

### Cloudflare

- Cloudflare Tunnel
- Access Managed OAuth
- service tokens and Access JWT validation
- MCP transport guidance and MCP Server Portals

## 15. Final recommendation

Implement in this order:

```text
1. issue #20 MCP projection gaps + named tool profiles
2. Dot observer through OpenAI Secure MCP Tunnel
3. narrow Dot local-skill manager pilot
4. loopback Streamable HTTP / narrow REST gateway behind Cloudflare
5. Muse Custom Connector read-only pilot
6. controlled Muse connector actions; browser fallback only when required
7. explicit GM handover pilot
8. typed durable events
```

Remote agents remain additional clients of ELIOT authority, not a replacement core.
