# Eliot Memory OS: conditional compile-packet integration

**Status:** requested, runtime integration not implemented. **Owner decision:** 3 October 2026. Eliot is not ready on the owner machine; commit the compile-packet request now, or enable it when Eliot Memory OS is connected. Standalone controller development and ordinary verification continue without an Eliot packet.

## Requested capability

When a real Eliot Memory OS connection exposes `eliot_compile_packet_l3`, request context for the concrete work goal before a material change. The requested tool argument is:

```json
{
  "goal": "The concrete requested change, its repository scope and expected observable result"
}
```

The capability name and argument above are an integration request derived from the installed Eliot work skill. They are not a new controller RPC, a verified Memory OS endpoint or a promise about an unconnected server's schema. Verify the connected server's advertised tool schema before calling it.

Read the returned packet and verifier. If it supplies a `frame_stub`, preserve its server-provided revision and fields while editing only the fields the server permits. Run the supplied verifier against the actual changed source. An absent, stale, rejected or unverifiable packet must never become a fabricated successful packet.

## Connection boundary

| State | Behavior |
| --- | --- |
| Eliot connection disabled or not configured | Use the standalone controller, explicit repository context and normal build/check evidence. Report no Eliot packet qualification. |
| Connection explicitly enabled and verified | Discover the actual capability, authenticate, verify the project scope, request a packet and honor its verifier. |
| Enabled connection unavailable, unauthorized, scope-mismatched, or capability absent | Report the concrete integration failure. Do not silently fall back for work that explicitly requires an Eliot packet; ordinary independent checks and runtime readback remain available. |

Configuration or binary presence alone does not establish a connection. Readiness requires a response from the intended authenticated service and a verified project identity. Use the real configured instance and data store. Do not initialize an empty Memory OS database, reuse an unrelated legacy database, guess an endpoint or synthesize a packet to satisfy this contract.

Eliot supplies context and verification requirements. Existing controller Task, Attempt, Operation, immutable source/check evidence and acceptance rules remain authoritative. A packet, model response or successful transport does not accept a Task or authorize replay of an uncertain native input.

## Acceptance for the future implementation

Qualification must establish all of the following:

1. Disabled integration starts and executes ordinary controller work without contacting Eliot.
2. An explicitly connected real service advertises the capability; its authentication and exact project scope are verified before the goal is sent.
3. The response's revision and verifier are preserved, and verifier results bind to the exact changed source.
4. Unavailable service, missing capability, scope mismatch, stale packet and verifier failure remain explicit failures of the requested integration.
5. Secrets are excluded from persisted task text, diagnostics and packet request receipts; native-input no-replay and independent Task acceptance remain unchanged.

Until those observations exist, documentation must retain `requested` / `not implemented` status. This contract allows continued standalone work; it does not claim that Eliot is working.
