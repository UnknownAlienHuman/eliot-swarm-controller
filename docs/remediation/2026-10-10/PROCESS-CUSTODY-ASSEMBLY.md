# Process capture and custody assembly

Checks and Scripts share synchronous bounded pipe capture. Output is streamed
to private files and synced before final publication; unfinished readers are
closed with incomplete evidence. No detached capture reader owns a hidden
unbounded output buffer. Windows zero-byte nonblocking writes remain pending.
The ordinary Rust ChildStdin path still needs execution evidence.

The worker keeps its actual process Group through receipt publication and
through publication failure while departure or disarm remains unconfirmed.
Store retains actual CheckRun and ScriptRun Child handles through launch
persistence errors, retry and shutdown. Unknown direct exit cannot establish a
passed CheckRun. Pre-spawn failure retains a terminalization retry in the same
CheckRun and Operation, without authorizing another launch.

Module-supervisor observations bind a host-admitted actor UUID. Superseded
actors cannot overwrite current lifecycle state. Exact permanent rejection
isolates its scope; unrelated lifecycle delivery continues. Hello failures are
visible. Helper drain uses bounded family observation and retains its Child.

The host owns joinable shutdown custody for optional workers and supervisor
reapers. It drains both Store Child maps and registered supervisor reapers
before closing Store or returning to the runtime owner. Bounded actor stop can
report pending, while final host shutdown keeps the actual owner alive until
departure is confirmed. Wait-observation errors retain the same Child and
remain visible; a durable unknown status does not replace the handle.

The frozen assembly passed scoped owned-package formatting and strict Rust
1.98.1 production Clippy for all 24 owned packages on Windows and Linux on
2026-10-10, with zero errors and zero warnings. Test compilation and execution,
fresh installed native/model qualification and the public R48 fault/recovery
evidence are tracked separately. Source delivery does not establish those
acceptance items; project status remains `PARTIAL_PROGRESS`.
