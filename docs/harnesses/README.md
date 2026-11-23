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

`crates/eliot-core/src/observe.rs` records the integration-time diagnostic baseline
per harness. The table is not a vendor executable pin or a launch gate. `swarm doctor`
compares the live installed version and contract marker against it and reports drift
without blocking launch:

| Harness | Verified version | Contract marker |
| --- | --- | --- |
| codex | 0.167.1 | app-server schema v2 |
| claude | 2.1.325 | stream-json records |
| muse | 1.4.0-R4161.1 | MSP v1 host |
| opencode | 2.2.7 | native schema |
| antigravity | 1.5.9 | ACP v1 `7ec5185c.json`, AGY 1.5.2 |

A drifted install still launches; the mismatch is annotated on `swarm doctor`, the
`harness.drift` event and the launch flag. Every profile with an explicitly pinned
executable path that has disappeared is reported with the configured path, an
installed replacement and repair command when available; `swarm status` exposes
the same profile warnings. A missing pin never falls back silently. `cmd` may be
omitted from any of the five harness profiles; the profile and ad-hoc launch use the
same harness-specific discovery, and only the launched generation freezes its
selected executable. Historical source-check dates on adapter pages are not fresh
verification; `swarm doctor` is the live readback.

## Current versions and mechanisms

Installed versions observed while integrating each native route (see the harness page
for the dated check, including the skill and MCP mechanism k61/k62 implemented). These
are historical diagnostic snapshots, not constraints on vendor updates or profile
executable selection:

| Harness | Installed version | Worker MCP default | `eliot-swarm` skill delivery |
| --- | --- | --- | --- |
| [Codex](codex.md) | codex-cli 0.167.1 | `minimal`; `mcp_allow` supported (requires `minimal`) | app-server `skills/extraRoots/set` before input |
| [Claude Code](claude-code.md) | 2.1.325 | `minimal`; non-empty `mcp_allow` is `unsupported` | `--plugin-dir <data-dir>/skills` per spawn |
| [OpenCode](opencode.md) | 2.2.7 | `minimal`; non-empty `mcp_allow` is `unsupported` | copied into the isolated per-Agent global skills directory |
| [Muse](muse.md) | 1.4.0 (R4161.1; the 1.4.20 catalogue is empty) | `inherit` only | first-turn system-context fallback (no per-session skill flag) |
| [Antigravity](antigravity.md) | agy 1.5.9 | `inherit` only | written into the `--add-dir` hook discovery root |

Every worker carries exactly one generic `swarm` MCP tool (`{ method, params }`); Agents
discover methods through `api.methods` and `api.help` and prove identity with `api.identity`.
