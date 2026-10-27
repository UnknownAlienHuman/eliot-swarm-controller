# Harnesses

The [roadmap](../roadmap.md) owns build order: useful direct Codex first; monitoring,
automation and coordination on Codex next; Claude/OpenCode and selected optional adapters
in L6. A catalogue entry is not a requirement to build that adapter before L2 works.

Provider services are separate: [OpenCodex](opencodex.md), [local models](local-models.md).

## Common plugin contract

The owning [plugin protocol](../spec/plugin-protocol.md) defines generation, source
acknowledgement, unknown outcomes and custody once. Each implemented adapter supplies only
its native mapping: exact subject/turn/request ids, process mode, effective settings,
readback/cursor limitations and child environment. It does not duplicate core machinery.

Declare only implemented capabilities. A vendor's documented method is a candidate,
not proof of a working Eliot integration. Historical source-check dates on adapter pages
are not fresh installed-version verification. Update a page while integrating that native
route, not by conducting a vendor-wide research campaign before every layer.

## Capability matrix

This is a design/reference matrix, **not runtime qualification**. A question mark means
unresolved until the selected installed protocol is inspected. All operations still pass
the common exact-identity and authority checks.

| Feature | [Codex](codex.md) | [Claude](claude-code.md) | [Muse](muse.md) | [OpenCode](opencode.md) | [Antigravity](antigravity.md) | [ACP](acp.md) |
| --- | --- | --- | --- | --- | --- | --- |
| Transport | app-server stdio | SDK / stream-json | MSP stdio | HTTP/SSE | agy stream-json | ACP stdio |
| Initial mode | per-Agent | per-Agent | per-Agent | per-Agent server unless shared isolation is demonstrated | per-Agent | per-Agent |
| Native subject | store + thread | config/project + session | host/store + session | server/store + session | config + conversation | command/store + session |
| Controlled source | app-server stream | SDK stream | MSP stream | exact-session SSE | stdout | ACP connection |
| Steer | exact turn | unsupported unless an exact native method is demonstrated | exact turn | ? exact native method | unsupported | unsupported in generic v1 |
| Inject | steer / next send | next supported hook safe point | steer / next send | ? / next send | next send | next prompt |
| Interrupt | exact turn | active query | exact turn | verify session/execution scope | ? | active prompt cancel |
| Requests | native approval/input ids | SDK callbacks | native choice/request ids | installed permission/input ids | no known headless reply channel | negotiated permission/input |
| Resume | exact thread | exact session | exact session | installed attach/resume | conversation | negotiated load/resume |
| Native Goal | installed method | ? | installed method | no assumed support | none assumed | none generic |
| Child identity | shared parent environment | shared parent environment | ? | ? | shared parent environment | agent-specific |
| Proxy | named provider | base URL/auth env | none assumed | provider base URL | none assumed | agent-specific |

`additionalContext` delivered by a later Claude hook is **inject**, not steer. A native
abort that leaves a session reusable is **interrupt**, not stop. There is no silent
renaming to make the matrix look complete.

## Implementing an adapter

Read its native page and the relevant donor-map entry. Connect launch/input/result first;
add real controls next. Generate/use installed schemas where provided. Retain exact
unknown outcomes; do not build guessed fallback APIs. Use short manual native runs only
to establish the current path. Full common conformance/recording/platform runs are L7.

Future APIs/tables from the complete specification are not first-adapter prerequisites.
The first plugin needs one per-Agent stream; source handoff, discovery, children and
shared pools enter only with their actual callers.
