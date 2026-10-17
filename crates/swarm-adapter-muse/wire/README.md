# Public MSP stable schema provenance

`msp.schema.json` is copied byte-for-byte from the public
[`meta-models/muse-code-sdk` schema/msp/stable/msp.schema.json](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/stable/msp.schema.json)
at upstream commit `a7c10c5dd3f66be412077d29f9d11111af70317b`. The upstream
schema identifies itself as generated and says not to edit it.

| Fact | Value |
| --- | --- |
| Upstream Git blob SHA-1 | `0d2f59cdb894fabe4d3e5535f6787e7f9e73df28` |
| Exact file SHA-256, lowercase hex without prefix | `ed442d494cf8cdb85f0b1487f8d0f54b7570548561c6c92d6def09ce107e1664` |
| Upstream root license | MIT, Copyright Meta Platforms, Inc. and affiliates |
| Local license copy | [`LICENSE`](LICENSE), copied byte-for-byte from the same upstream commit |

## Initialize contract anchors

The exact upstream schema records these facts:

- `methods.initialize` points to `$defs.InitializeParams` and
  `$defs.InitializeResult` ([source](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/stable/msp.schema.json#L6285-L6293)).
- `$defs.InitializeParams` requires `clientInfo`; `$defs.InitializeResult`
  requires `schema` and the other listed result fields ([source](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/stable/msp.schema.json#L1422-L1493)).
- `$defs.SchemaInfo.version` is an integer, described as envelope schema version
  `1` in v1. `$defs.SchemaInfo.fingerprint` uses the upstream pattern
  `^sha256:[0-9a-f]{64}$` and describes the value as a content hash of the
  server binary's stable-surface bundle; a mismatch is a warning, not an error
  ([source](https://github.com/meta-models/muse-code-sdk/blob/a7c10c5dd3f66be412077d29f9d11111af70317b/schema/msp/stable/msp.schema.json#L2572-L2590)).

The schema file SHA-256 above is an artifact integrity fact. It is not a
replacement or derivation of the server-provided `initialize` fingerprint.
`src/schema.rs` exposes both field schemas from the upstream document and keeps
the file digest distinctly named.

## Integration boundary

The root integrator owns adding `pub mod schema;` to `src/lib.rs` and the
existing locked validator dependency to this crate:

```toml
jsonschema = { version = "=0.58.6", default-features = false }
```

`schema.rs` uses `jsonschema::options().offline().build_map(...)` to compile
local `$defs` and method-parameter validators once. No file or HTTP reference
retrieval is enabled. Public entry points are `definition_schema`,
`method_parameters_schema`, `validate_definition`,
`validate_method_parameters`, `initialize_schema_version_schema`, and
`initialize_schema_fingerprint_schema`.

The focused negative vectors live in `src/schema.rs` and cover missing
initialize client information, an invalid client identifier, malformed
fingerprint syntax, a non-integer envelope version, unknown targets, and a
known method with no params schema. After adding the module export and dependency,
the root-owned focused verifier is `cargo test -p swarm-adapter-muse
schema::tests --lib`. This scoped contribution has not run Cargo, tests, builds,
formatters, or native processes.
