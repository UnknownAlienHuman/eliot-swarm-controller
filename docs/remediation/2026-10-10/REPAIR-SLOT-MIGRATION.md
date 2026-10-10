# Repair slot package identity migration

New direct and automated correction slots bind the full retained findings
package digest, including singleton packages. New slots and links use schema 2.
The existing storage prefixes remain unchanged. Retained schema 1 slots and
Operations are read without rewriting them or sending a second correction.

The retained decoder preserves both the package renderer and the historical
singleton renderer from `3fff155`. Historical automatic Operations keep their
original nonce, finding-only cause and sealed transfer provenance. Resolution
validates the immutable request, binding, Operation and link before reuse.
Simultaneous current and historical slots produce `REPAIR_SLOT_AMBIGUOUS`;
a proved different package on a historical singleton slot produces a conflict.
Malformed receipts, changed immutable text and mismatched links remain errors.

On 2026-10-10 the exact four-file candidate passed scoped rustfmt, whitespace
checks and Windows Rust 1.98.1 production Clippy for `swarm-kernel-host`
(`--lib --bins --no-deps -- -D warnings`) in a separate delivery checkout.
Host test targets compiled with zero errors and 21 existing warnings.
The dedicated slot fixtures include independent historical golden bytes,
changed-package conflicts, ambiguity and immutable-record tampering.

Fixture execution is deferred until the source and Clippy assembly completes,
as requested by the owner. Automatic transfer regression assembly and the full
test/native/model gates remain pending. This is source delivery evidence;
project status remains `PARTIAL_PROGRESS`.
