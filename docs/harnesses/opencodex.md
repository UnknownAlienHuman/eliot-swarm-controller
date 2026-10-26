# OpenCodex provider proxy

OpenCodex is an inference proxy, not a harness/session authority. Eliot treats it as an
attached or managed shared [service](../spec/configuration.md#proxies-services-and-webhooks);
agent sessions remain owned by their harness plugins. Layer L2.7.

Facts checked 2026-10-25 against the then-current OpenCodex release/source. Recheck the
owner's installed version and Management API before implementation.

## Ownership

Owner install/login/init remains owner work. `attach` means health/read-only management
only and Eliot never stops/reconfigures it. `launch` means a separately supervised
managed service with its own custody; one agent stop never kills the proxy.

Client/admin keys, home paths and provider credentials are named environment references.
They never enter resolved config, rules, events or logs. Binding beyond loopback follows
OpenCodex's own auth requirements; Eliot never opens firewall/listen addresses.

## Harness wiring

| Harness | Route |
| --- | --- |
| Codex | named Responses provider at `<origin>/v1` ([Codex permissions/proxy](codex.md#permissions-and-proxy)) |
| Claude | explicit documented base URL/auth/custom headers in that per-agent minimal environment |

Eliot never runs global client integration installers or edits owner global configs.
Requested URL is not proof: plugin reports effective endpoint/provider/model/account,
and L7 verifies main plus auxiliary/title/compaction/subagent calls separately.

## Management and holds

Read-only management data may feed proxy health, provider/model availability, usage and
limitation scope. Exact paths/headers are installed-version facts. Management timeout
only makes that source stale; it never changes live agent state, retries input or
selects fallback.

OpenCodex auth mode determines which subscription/key/account spends. Profiles refer to
an explicit Eliot account scope shared across relevant profiles. Provider limitation
coverage is preserved; unknown coverage holds only the exact route, not every account.
A future probe is a real user turn, never a hidden model call.

## L7 cases

- client vs admin credential separation/masking;
- attached service never stopped and managed service lifecycle isolated;
- effective billing/route for Codex and Claude main + auxiliary requests;
- 409/stale plan and ambiguous write response never blindly retried;
- proxy `activeTurnCount=0` never used as native child/session completion;
- account/proxy cap and persisted hold recovery across daemon restart.
