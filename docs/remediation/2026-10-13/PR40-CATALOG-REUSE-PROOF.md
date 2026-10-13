# PR40 bounded catalog verification patch

Source baseline: `a7b5415cd1b34489ff9ec392113cde9f77022c4b`, isolated branch
`codex/swarm-catalog-20261013`. Review input: retained `pr-40.json`, including
all four comments; `pr-40-review-comments.json` is empty.

The existing per-ToolSpec OnceLock slots now have six test-only factory
counters: optional/required input construction, optional/required input byte
serialization, output construction, and output byte serialization. Isolated
descriptors use the same cache seam without resetting process-global slots.
No registry, authorization cache, dynamic cache key or wire format was added.

The contract tests assert cold counts `[1,1,1,1,1,1]`, no further construction
over 64 warm lookup rounds, identical retained Arc values across eight concurrent
first callers, cached absent output, and closed unknown method/profile lookup.
These are expected assertions, **not executed results**.

MCP tests compare direct input/output bytes against paged catalog schemas for
every visible method under all seven profiles, including method ordering and
input digest parity. Required-ID and Full optional-ID variants remain distinct.
A warmed catalog must reevaluate its live membership callback even under the
same authorization revision. Invalid exact method discovery remains a miss.

Historical source was located immediately before extraction at
`88264832b7cde56b917728830666f27b29fc515e` (`7dc8617^`). Retained Git blobs:

- `crates/swarm-mcp/src/mcp/mod.rs`: `128fe97bcf2a366a119ce39f52a0726f5c0253e8`
- `crates/swarm-mcp/src/mcp/catalog.rs`: `07842427ee029f54437e1e00417bdf97eba3d6f5`

The separate `7ab8690d32c8d2554c21e9c90566bd57992a0f52` repair intentionally
changed `gm.handover` and `attempt.bind_producer` semantic prerequisite wording
and introduced exact required input metadata. That wording affects V1 metadata
framing and must be accounted for separately from extraction. This patch does
not replace a historical snapshot or claim an unchanged historical digest.

## Validation and remaining gaps

Direct rustfmt of the three touched Rust files and `git diff --check` passed.
SHA-256 of the formatted sources:

- `mcp_catalog.rs`: `FA1FD9824882D8080B1120A631078CF504BA0B04B40D4E62F7A15BB37501FC5D`
- `mcp_catalog_cache_tests.rs`: `1EC98FCE33DC5E58F09D07556AE10D4E20E0A0C465EE54763C001D50CC20E1AF`
- MCP `catalog_tests.rs`: `F484EB9EC1C839A80B5D6C8F3BF3A1187931ED8190886837CBF1EBDB7702DEFF`

Cargo, builds, tests and Clippy were not run: the parent owns execution.
Parent commands (same filters on Windows and Linux):

```text
cargo test -p swarm-contracts --lib mcp_catalog::cache_tests
cargo test -p swarm-mcp --lib catalog_direct_schema_bytes_and_profile_membership_match
cargo test -p swarm-mcp --lib warm_catalog_rechecks_live_membership_and_rejects_unknown_exact_method
```

Still unverified: retained pre-extraction wire fixture bytes/preimages and
old/new Catalog V1 digests; all profile/audience historical framing parity;
exact cold/warm counts during full catalog framing rather than direct lookup;
separate authorization/dispatch cost; runtime target IPC membership enforcement.
The counters are compiled only for contracts unit tests, so MCP tests do not
claim cross-crate factory instrumentation. No throughput measurement is claimed.

Semantic project query (`project=eliot-swarm-controller`), connected ELIOT
packet/verifier, and Luna delegation could not run: no matching tools were
callable in this session. Exact lexical/source fallback and pre-edit
CodeUnderstandingProof were retained in `.catalog-proof/proof.txt` before edits:
candidate symbols were ToolSpec/input_schema/input_schema_bytes/output_schema/
output_schema_bytes/list_tools_page/digest_entries; exact anchors were
`mcp_catalog.rs:70,2171,4072` and `catalog.rs:108,363` at the baseline. The verifier
was explicitly parent-owned focused contracts/MCP tests, and the unknowns were
historical wire bytes/digests and unavailable semantic/ELIOT tools. This note
retains that proof after scratch cleanup.
Standalone scope follows the user's explicit instruction. Completion status:
`PARTIAL_PROGRESS`; PR40 closure remains with the parent after verification.
