# Source-audit coverage

This registry prevents “170 findings” from being mistaken for “the repository was fully read.” It maps the current Rust workspace and adjacent source surfaces to existing AR evidence. `Partial` means some caller-to-result paths were traced; it does not mean every file or failure mode was inspected.

Inspected repository basis for this inventory: merged `main` at `797ea93f1cfcaad8c8dde2629d666e64efb131c2`. The detailed AR traces remain tied to their immutable commits in the [audit index](audit-index.md).

## Workspace packages

| Package / surface | Existing evidence | Coverage | Untraced or insufficient boundary |
| --- | --- | --- | --- |
| root `src/{main,lib}.rs` / host entry | runtime/restart snapshots and later package traces | `partial` | Full command/entrypoint/public-surface inventory and retired compatibility paths are not recorded as one chain. |
| `swarm-kernel-host` | AR-001–170 touch many Store/runtime/launcher/effect paths | `broad partial` | No file-level coverage ledger; large portions are only reached indirectly. Whole-package completion is not established. |
| `swarm-adapter-codex` | AR-025–035, 051–058; implementation N | `partial` | Full current command/action matrix, recovery and old-path disposition remain open. |
| `swarm-adapter-command` | AR-025–035; implementation O | `partial` | Current result/release/reconcile and retired compatibility surfaces need exact trace. |
| `swarm-adapter-claude` | AR-025–035; implementation M | `partial` | Current native transport, recovery, Goal/actions and historical formats are not exhaustively mapped. |
| `swarm-adapter-opencode` | AR-025–035, 051–058; implementation Q | `partial` | Service ownership, event/result coverage, steering, recovery and duplicate paths remain open. |
| `swarm-adapter-muse` | AR-032–035 plus historical operational evidence; implementation P | `partial` | Node/Rust split, pending requests, result provenance and package retirement are not exhaustively mapped. |
| `swarm-contracts` | cross-cutting AR findings and module API | `partial` | Public/internal/vendor method inventory and every retained DTO consumer are not enumerated. |
| `swarm-client` | AR-127–141 | `partial source trace` | All disconnect/reconnect/concurrency consumers and public API compatibility are not closed. |
| `swarm-cli` | scattered status/docs | `untraced as package` | Command inventory, authorization/readback semantics and obsolete surfaces need a dedicated trace. |
| `swarm-checks` | AR-065–070 | `partial source trace` | Full process/capture/cancel/platform boundary and legacy cutover remain open. |
| `swarm-observer` | AR-142–159 | `partial source trace` | Collector coverage, gaps, unmatched subjects and old-surface deletion remain open. |
| `swarm-forge-worker` | AR-079–088 | `partial source trace` | Complete effect/recovery/process-hold and publication-finalization chain remains open. |
| `swarm-bus` | mailbox and IPC findings touch consumers | `untraced as package boundary` | Ownership, persistence/transport division and duplicate communication surfaces need a dedicated trace. |
| `swarm-automation` | AR-051–058, 079–088 and repair findings | `partial` | Full trigger-to-action loop, budgets, restart, external-effect recovery and duplicate engines remain open. |
| `swarm-kernel` | dispatch/effect code referenced by several traces | `untraced as package boundary` | Public API, duplicated kernel/host responsibility and all consumers need an exact map. |
| `swarm-mcp` | AR-127–141 and gateway profile evidence | `partial source trace` | Tool catalog/profile/public-surface completeness, lock removal, exact reads and load behavior remain open. |
| `swarm-process` | AR-017–019, 065–078, 142–170 | `partial` | Full file/process/group/Windows-Unix contract and every caller are not exhaustively mapped. |
| `swarm-script-worker` | AR-071–078 | `partial source trace` | Action/readback/cancel/restart and legacy execution cut remain open. |
| `swarm-scripts` | AR-071–078 | `partial source trace` | Typed action vocabulary and all consumers/compatibility paths remain open. |
| `swarm-store` | AR-115–126 | `partial source trace` | Numeric migration implementation, extension verification, all reader cutovers and corruption paths remain open. |
| `swarm-supervisor` | AR-017–019, 142–159 | `partial` | Complete owner/helper/adapter family lifecycle and replacement proof remain open. |
| `swarm-telemetry` | scattered observability references | `untraced as package boundary` | Inputs, disclosure, authority separation, consumers and deletion/retention policy need a dedicated trace. |
| `swarm-gateway` | AR-160–170 | `partial source trace` | V0–V3 repair and connected security/session/readback qualification remain open. |
| `modules/antigravity-rust` | AR-001–005 and A1a/A1b implementation evidence | `partial` | Durable delivery, crash/restart, old-boot completion, full action matrix and installed harness remain open. |

## Non-workspace source surfaces

| Surface | Coverage | Required audit |
| --- | --- | --- |
| `migrations/**` | selected migrations inspected in AR-012–016, 100–126 | Enumerate every migration, current writer/reader, downgrade/corruption/data disposition and deletion plan. |
| `config/**` | route/provider/gateway fragments inspected | Build one key-to-owner/consumer/default/deprecation map; reject stale configuration families. |
| `tools/**` and `.github/workflows/**` | individual qualification/build paths inspected | Inventory active vs one-shot vs historical workflows, write authority, credentials and exact artifacts. |
| package/module descriptors and installers | A0/module-installation evidence | Enumerate every active artifact coordinate, retained historical identity, installer/selector and obsolete package. |
| `tests/**`, inline tests and qualification scripts | many focused receipts, no whole map | Map each current invariant to executable evidence; classify mocks/source checks separately from connected/native/load cases. |
| `vendor/**` / retained upstream snapshots | hash/build policy only | Record exact imported version, local patches, license/provenance, consumers and update/qualification boundary. |
| documentation and external source files | documentation-readiness audit | Classify every file; recover unavailable external evidence or mark the requirement mapping unverified. |

## Audit completion criteria

The source audit is complete only when:

1. every current package and non-workspace surface has a named inspected revision;
2. every file is owned by one responsibility or explicitly classified as generated/vendor/historical;
3. every public command, Store transition, external effect, recovery path and result consumer has a caller-to-result trace;
4. every deletion names remaining consumers and data disposition;
5. every confirmed historical requirement/failure maps to an AR/PF invariant or is explicitly rejected with evidence;
6. unresolved or uninspected boundaries are listed, not silently treated as clean.

## Next exact audit sequence

Do not continue implementation A2 as a substitute for this audit. Continue source review in manager-owned, non-overlapping clusters:

1. **Entrypoints and public surfaces:** root `src`, `swarm-cli`, `swarm-kernel`, `swarm-bus`, `swarm-telemetry`; enumerate commands/APIs and ownership overlaps.
2. **Shared boundaries:** `swarm-contracts`, `swarm-client`, `swarm-process`; enumerate every current consumer and compatibility format.
3. **Store completeness:** file-level `swarm-kernel-host/src/store`, `swarm-store`, all migrations and read models; reconcile against AR-001–170.
4. **Adapters:** one adapter at a time, exact action/recovery/result matrix plus old package/data disposition.
5. **Effects and observation:** Checks, Scripts, Forge/GitHub, automation, supervisor, observer and gateway.
6. **Tooling/qualification/docs:** active workflows, installers, descriptors, fixtures, evidence type and external-source availability.

Each cluster updates this table, the current audit register, audit index and failure-prevention register before another cluster starts.
