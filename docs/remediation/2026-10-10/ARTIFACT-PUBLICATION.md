# Durable artifact publication

Host byte artifacts publish directly to their final destination through the
existing no-clobber durable file primitive. A destination collision requires
exact retained-byte verification. Streaming CheckRun log sealing and multipart
assembly preserve their zero-copy publication and sync the destination parent
on Unix. Assembly removes its temporary file through durable removal; cleanup
failure remains visible, including as a secondary error after publication
failure. Windows retains the primitive's documented platform guarantees.

The frozen two-file host candidate passed scoped format, whitespace checks and
Windows Rust 1.98.1 production Clippy with `-D warnings` on 2026-10-10. Host test
targets compiled without errors. Execution remains deferred until the full
source assembly. Script worker publication belongs to the connected capture
slice; full and native/model qualification remain pending.
