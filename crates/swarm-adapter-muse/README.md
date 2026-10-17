# Muse Rust transport foundation

This library owns bounded bidirectional stdio transport for the prospective Rust
Muse adapter. It has no executable or module descriptor. The current product
route remains [Node bridge.13](../../modules/muse/README.md); full Rust migration
is incomplete.

The complete `jsonrpsee-core 0.26.1` async client and public server router own RPC
correlation and method dispatch. Complete `tokio-util 0.7.19` supplies bounded
newline framing. Local glue separates the client/server ID spaces and orders
submissions before stdin close. The caller supplies pipes from an already owned
child; existing module-owner membership is verified. EOF never proves child or
descendant departure. A submission receipt proves flushed bytes, not native
admission, execution or Task completion. No request is automatically replayed.
An RPC request timeout ends transport admission before releasing its slot,
bounding the library's retained requests. Successful write receipts remain
valid evidence of attempted work; the native effect remains unknown.

The official Muse SDK `1.3.0` at
`a7c10c5dd3f66be412077d29f9d11111af70317b` remains the public wire/conformance
reference. No SDK `Session`, `PendingCommandSet` or `GapFiller` is copied. Generic
RPC adoption is recorded in the [donor inventory](../../docs/agent_swarm.donors-20260929.toml)
and `lib.rs`; the exact jsonrpsee license is retained under `third-party/`.
Native Muse retains authentication, provider configuration and inference.

Twenty local boundary fixtures and strict all-target Clippy passed on
2026-10-16. They cover typed IDs, server requests, split UTF-8, frame bounds,
malformed/truncated input, pressure, cancelled receipts, write failure and close
ordering. They use synthetic pipes and do not qualify a native Muse executable.

Complete command/TaskPrompt/model controls, private checkpoints, host delivery,
children, result paging, recovery, authorized streams, packaging and installed
native qualification remain outstanding. Native MCP inventory remains
unavailable; this library does not authorize dispatch. The eventual worker must
keep jsonrpsee payload trace targets disabled in its logging configuration.

## Native result reader

`results` validates exact session/item/revision selectors and uses only
`view/page` and `item/readOutput`. It does not consume `subagent/readResult`,
follow stored paths/URLs, select the latest assistant message or accept a Task.
The caller must authorize the current binding/family and any supplied cursor;
that predicate is rechecked around native reads and before returning bytes.

A historical lookup reads at most 64 native pages per call, keeping one page
and bounded cursor digests. If more history remains, it returns the exact
observed continuation rather than claiming the revision is unavailable.
The continuation itself grants no read rights. Byte pages are capped at 65536;
the reader validates source references, range/EOF/base64 and whole digests when
available. Unknown item statuses do not establish terminal results.

Structured subagent results use explicitly versioned
`rust_muse_result_json_v1` bytes, preserving native JSON integer values. This is
not a relabelling of historical Node canonical bytes or a migration of their
bindings. Message UTF-8 and native stored-output digest semantics are retained.
The fourteen local reader fixtures do not qualify installed Muse or host result
delivery; facade/journal integration remains required.
