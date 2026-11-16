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

## Implementing an adapter

Read its native page and the relevant donor-map entry. Connect launch/input/result first;
add real controls next. Generate/use installed schemas where provided. Retain exact
unknown outcomes; do not build guessed fallback APIs. Use short manual native runs only
to establish the current path. Full common conformance/recording/platform runs are L7.

Future APIs/tables from the complete specification are not first-adapter prerequisites.
The first plugin needs one per-Agent stream; source handoff, discovery, children and
shared pools enter only with their actual callers.

## Verified versions and `swarm doctor`

`crates/eliot-core/src/observe.rs` pins the verified contract per harness.
`swarm doctor` compares the live installed version and contract marker against that
table and reports drift without blocking launch:

| Harness | Verified version | Contract marker |
| --- | --- | --- |
| codex | 0.167.1 | app-server schema v2 |
| claude | 2.1.278 | stream-json records |
| muse | 1.4.0-R4161.1 | MSP v1 host |
| opencode | 2.2.7 | native schema |
| antigravity | 1.5.9 | ACP v1 `7ec5185c.json`, AGY 1.5.2 |

A drifted install still launches; the mismatch is annotated on `swarm doctor`, the
`harness.drift` event and the launch flag. Historical source-check dates on adapter
pages are not fresh verification; `swarm doctor` is the live readback.
