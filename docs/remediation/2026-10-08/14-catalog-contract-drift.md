# R14 companion. Catalog metadata must not invent request fields

**Status:** implementation handoff. Production MCP schemas, CLI mapping, Store validation and catalog metadata are unchanged on this branch.

**Evidence baseline:** `40591a295af94b1541ec2ba30afe8e3247701a71` (`main`, 2026-10-08).

## 1. Audit correction and confirmed defect

The broad allegation “MCP and CLI define mutually incompatible parameters for `gm.handover` and `attempt.bind_producer`” is too broad.

For both methods, the direct MCP `ToolSpec`, CLI mapping and Store request validator agree.

### `gm.handover`

Current executable request shape:

```text
client_id                 required
binding_id                optional, paired
binding_generation        optional, paired
client_request_id         mutation envelope
```

CLI exposes `client_id` plus an optional `binding_id`/`generation` pair. MCP requires `client_id` and refines the optional binding pair. Store admits the same fields.

The **catalog metadata** instead advertises:

```text
target_client_id
expected_revision
```

Neither field is accepted by the executable method.

### `attempt.bind_producer`

Current executable request shape:

```text
attempt_id
assignment_id
native_session_id
native_run_id
observation_id
client_request_id         mutation envelope
```

CLI `task bind`, direct MCP schema and Store validator agree on those fields.

The **catalog metadata** instead advertises:

```text
attempt_id
expected_revision
binding_id
binding_generation
```

Three advertised fields are not accepted, while four required executable fields are absent.

Therefore the confirmed defect is:

```text
TOOLS/direct schema + CLI + Store agree
but deferred/search catalog required_context describes a different API
```

An agent discovering either method through `swarm.tools.search` can construct a request that is guaranteed to fail, even though direct `tools/list` is correct.

## 2. Root cause

`ToolSpec.required` and `ToolMetadata.required_context` are maintained independently.

`required_context` is:

- returned to catalog/search clients;
- included in the catalog digest;
- included in search ranking text;
- currently populated sometimes with semantic prerequisites and sometimes with field names.

This mixed meaning makes drift inevitable. It is not fixed by changing two strings and keeping two parameter registries forever.

## 3. First connected repair

Correct the two existing metadata entries immediately, without changing method names or request schemas.

Recommended values keep `required_context` semantic rather than pretending it is a second schema.

### `gm.handover`

```text
current GM or verified local Operator authority
registered enabled Manager target client_id
optional exact binding_id + binding_generation pair
caller-owned client_request_id before dispatch
```

### `attempt.bind_producer`

```text
exact current attempt_id and owner authority
exact retained assignment_id
observed native_session_id + native_run_id
positive retained observation_id
caller-owned client_request_id before dispatch
```

Do not put `expected_revision`, `target_client_id` or binding fields into these entries unless the executable method is deliberately changed end-to-end in a separate product decision.

## 4. Remove the second request contract

R14 should make the distinction explicit:

```text
ToolSpec.fields / ToolSpec.required
    = executable wire request schema

ToolMetadata.required_context
    = semantic prerequisites only
```

Catalog projection already has access to the selected `ToolSpec` and generated input schema. It must not restate required input names from `ToolMetadata`.

Minimal implementation:

1. keep the V1 catalog wire shape and digest framing unchanged;
2. correct the two bad metadata rows;
3. add an internal validation pass over every metadata row and matching `ToolSpec`;
4. reject field-like context entries that claim an unsupported request key;
5. during R14 registry extraction, colocate metadata and `ToolSpec` in one data-only descriptor so adding a method cannot update one without the other.

Do not add another schema DSL. The current `ToolSpec` remains the executable source until the typed method registry replaces it.

## 5. Validation without guessing semantic prose

A generic test cannot assume every `required_context` string is a request field: many entries deliberately contain phrases such as `operator scope` or `local module catalog`.

Use an explicit convention for field claims. Two acceptable minimal approaches:

### Approach A — structured internal context item

```rust
enum RequiredContextItem {
    InputField(&'static str),
    Semantic(&'static str),
}
```

Projection still emits strings in the same order and framing. Validation checks every `InputField` against the matching `ToolSpec.fields` and conditional rules.

### Approach B — field prefix in internal metadata only

```text
field:attempt_id
semantic:exact retained assignment
```

Strip the prefix when building the unchanged V1 projection. This is less type-safe than A but still machine-checkable.

Prefer A if it does not enlarge the diff materially. Do not parse arbitrary English to infer fields.

## 6. Exact fixtures

1. `catalog_gm_handover_context_matches_executable_contract`
   - catalog contains `client_id`/optional binding semantics;
   - no `target_client_id` or `expected_revision`;
   - direct schema still requires `client_id` only and pairs binding fields.

2. `catalog_attempt_bind_producer_context_matches_executable_contract`
   - catalog names attempt/assignment/session/run/observation semantics;
   - no `expected_revision`, `binding_id`, `binding_generation` request claim.

3. `catalog_input_field_context_must_exist_in_toolspec`
   - synthetic metadata naming an unknown field is rejected at registry construction/test time.

4. `cli_mcp_store_shapes_match_for_gm_handover`
   - map CLI arguments;
   - validate with direct MCP schema and Store request validator;
   - optional binding pair both absent or both present.

5. `cli_mcp_store_shapes_match_for_attempt_bind_producer`
   - exact five domain fields survive mapping unchanged;
   - positive observation ID enforced;
   - unknown catalog-era fields rejected.

6. `catalog_digest_v1_framing_is_preserved_after_registry_colocation`
   - same ordered fields, descriptions and schema bytes for unaffected methods;
   - changed digest only for the two corrected metadata rows, with the expected reason recorded.

Use the real public CLI mapper and catalog/direct-schema producers where practical. Helper-only string equality is insufficient for the end-to-end shape claims.

## 7. Simplification and deletion

After R14 registry extraction:

- delete separate method-name matching between `TOOLS` and `TOOL_METADATA`;
- delete duplicated method rows that can exist without a matching executable schema;
- delete tests that merely assert equal list lengths;
- retain semantic search metadata, but not a second manually maintained field contract;
- keep Store validation authoritative at final dispatch.

Do not move application authority into the catalog. A schema or matching metadata row is never permission to execute.

## 8. Ownership and order

R14/#40 owns frontend descriptor colocation and catalog/schema consistency.

R24/#50 owns live authorization before target IPC. R29/#55 owns caller-known request IDs. R31/#57 owns native command mapping. None of them should create another catalog registry.

Recommended order:

```text
1. correct the two metadata rows
2. add machine-checkable context-item distinction
3. colocate ToolSpec + metadata in the minimum data-only owner
4. switch direct list and catalog search to that owner
5. delete old parallel arrays/matches
6. preserve Store reauthorization at dispatch
```

## 9. Gate

After connected code:

```sh
cargo fmt --all -- --check
cargo clippy --locked \
  -p swarm-mcp \
  -p swarm-cli \
  -p swarm-kernel-host \
  --lib --bins -- -D warnings
```

Then run the exact catalog/CLI/direct-schema fixtures above. Broad MCP/native qualification remains later.
