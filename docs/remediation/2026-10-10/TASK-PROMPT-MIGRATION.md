# Built-in OpenCode TaskPrompt migration

New built-in OpenCode bindings use the exact `opencode_v2` runtime and
`eliot-opencode-v2.http.2` artifact. Store retains the TaskPrompt envelope and
dispatch context with the Operation. The effect validates identity, UTF-8 byte
lengths and digests, sends the exact retained prompt and returns a typed
admission receipt. Readback verifies the same native input and prompt. Missing
or invalid TaskPrompt data cannot fall back to raw Task snapshot rendering.

Built-in `.1` keeps its immutable historical prompt and readback decoder, but
new `.1` bindings are rejected. New trusted Command bindings reject only the
exact `eliot-command.rust-headless.1` version `3` coordinate; V4 and ACP continue
to use TaskPrompt. Existing V3 Operations remain readable.

The operator examples and built-in demand/service predicates include `.2`.
The standalone OpenCode `eliot-opencode-v2.rust-http.1` contract is unchanged.
The [consumer inventory](TASK-PROMPT-CONSUMERS.md) identifies retained decoders
and their deletion conditions.

On 2026-10-10 a separate clean candidate based on `c1560215` passed Windows
Rust 1.98.1 scoped rustfmt, whitespace checks and host production Clippy
(`--lib --bins --no-deps -- -D warnings`). Host test targets compiled with zero
errors and 21 existing warnings. Source fixtures cover exact current bytes and
receipt identity, malformed envelopes, historical `.1` prompt/readback and
exact Command version retirement.

Fixture execution, full tests and native/model qualification remain pending
until source and Clippy assembly completes. The migration establishes a source
contract; project status remains `PARTIAL_PROGRESS`.
