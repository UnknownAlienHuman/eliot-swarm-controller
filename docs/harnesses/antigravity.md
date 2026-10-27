# Antigravity CLI (`agy`)

Optional L6 adapter. One warm `agy` process belongs to one Eliot Agent generation and
custody unit. Source: [google-antigravity/antigravity-cli](https://github.com/google-antigravity/antigravity-cli).
Inherited notes were checked 2026-10-25; verify installed flags/defaults and behavior
when this adapter is selected, not before Codex bring-up.

## Native identity and transport

Subject: `(canonical auth/config scope, exact conversation_id)`.

```text
agy --input-format stream-json --output-format stream-json
    [--model M] [--mode accept-edits|auto|plan] [--effort E]
    [--conversation ID]
```

One user line and at most one input in flight; stdout init/step_update/result. Results
without input ids are correlated only within that same uninterrupted process and its
single outstanding prompt. Resume starts a new generation with exact conversation id,
never the old prompt. Late old stdout is stale. Children share parent environment unless
installed evidence proves otherwise; no fake independent child token.

Whether init arrives before input is unresolved. If not, use the common
`start_requires_input` linked command contract. Arm stdout before the initial write.

## Native mapping

Launch/send use the native input stream; local write acceptance is not turn completion.
Steer is unsupported; inject may prepend to the next send and reports next-turn semantics.
Interrupt remains unsupported until an exact native method is observed. Stop closes stdin
and waits; force targets only owned custody. Configure uses launch flags/next-session
settings. No known headless approval/question reply channel is invented. Eliot Goal is
separate from native turn control. Slash/foreign control messages are not sent into a
stream that does not accept them.

## Facts and recovery

The stdout stream owns supplied session/turn/message/tool/child/usage families. Exact
terminal result can settle its single outstanding prompt even if no intermediate update
was emitted. SUCCESS/ERROR/INVALID/CANCELED/INTERRUPTED map from the installed schema;
WAITING/RUNNING are nonterminal. A soft-denied tool remains visible even under a successful
final result. Cumulative usage replaces the previous session counter.

There is no assumed replay cursor/history API. Loss means explicit gap and only available
exact current-state readback. Do not replay conversation history or infer success from
silence/process exit; drain buffered stdout first. Unknown input/result is not retried.

## Permissions and final checks

Use explicit native permission settings and effective init facts. No documented provider
base-URL override is assumed; never substitute another subscription/API route. L7 checks
init ordering, single-input correlation, late output, gaps, soft denial, children and
actual permissions/model. Common wire/ack/custody behavior stays in the plugin spec.
