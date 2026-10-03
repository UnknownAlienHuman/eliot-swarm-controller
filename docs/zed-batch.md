# Zed batch runtime

The Zed adapter models `eval-cli` as a one-shot executor. `agent.open` checks the configured executable, work directory, model, and declared environment-key presence; it starts no native session. `task.dispatch` freezes the exact instruction with the immutable Task snapshot, writes an Operation-scoped intent before launch, and records the validated native result plus output artifact pages. The Operation ID is the dispatch identity. A vendor session value, if a future result exposes one, remains per-run evidence and never becomes a binding root or turn.

Exit 0 is successful native execution only when `result.json` validates and reports `completed`. It does not accept the Task. An observed, matching native `error`, `timeout`, or `interrupted` result rejects the dispatch; a missing result, contradictory result/exit pair, launch marker without terminal receipt, unreadable receipt, or interrupted host observation stays Unknown and holds capacity. Host restart and `agent.reconcile` read the exact saved Operation intent, terminal receipt, and immutable artifacts. They never rerun a dispatch. `agent.refresh` reports the controller's saved binding and Operation snapshot.

`agent.result` accepts only a `batch_output` selector naming the exact terminal dispatch Operation and one of `result.json`, `thread.md`, or `thread.json`. Its byte range resolves to immutable artifact page IDs and per-page ranges; callers use the normal artifact read API to fetch selected bytes. A missing output or an out-of-range request is returned as a rejected read, without a filesystem path.

The installed `eval-cli` binary has not been qualified by this adapter. Preflight can establish that the configured entrypoint exists and that the route can be described; it does not upgrade `installed_runtime_verified`. Native result fields are cross-checked against the configured model, timeout, and exit code. Raw logs and native error text are not included in Operation details. Output files remain private controller artifacts and are exposed only through an exact admitted result read.

## Operator sequence

1. Configure the `zed` route with an explicit provider/model, an absolute work directory, a bounded timeout, an executable, and only the environment variable names the executor needs. Values are inherited for the child process but are never copied into controller state or logs.
2. Open a binding and inspect its Operation. A successful open means rootless executor preflight completed; it does not mean a native session was created or the installed binary was live-qualified.
3. Dispatch one authorized Task. Inspect the exact dispatch Operation's state, native result subtype, exit code, prompt/snapshot digests, stable batch run ID, and retained artifact references. Task acceptance remains a separate owner decision.
4. Read an output by selecting the dispatch Operation ID and one allowlisted output name. Follow the returned artifact references and page offsets through the artifact read API.
5. If a dispatch is Unknown, reconcile that exact Operation. Already-authorized exact readback and reconciliation may continue autonomously. Reconciliation can settle a saved terminal receipt; if the receipt is absent or invalid, it returns unresolved and preserves the capacity hold. Never replay the same Operation. Start another native effect only when the current Task/Attempt authorization covers it and the prior Unknown has been resolved; a change of route or scope needs a new explicit decision.

The adapter intentionally does not emulate persistent control, resume, send/steer, goals, replies, or session families. A future capability needs a native contract and its own explicit runtime boundary.
