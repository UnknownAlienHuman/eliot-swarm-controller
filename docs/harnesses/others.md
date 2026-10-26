# Other harnesses (L4.7, only when needed)

These are risk notes from v1, not active contracts. None is built until a concrete need
and fresh installed/official verification. A similar wire format is not permission to
reuse another plugin without proving the full Eliot contract: source-before-effect,
identity/readback, pagination, input/turn mapping, environment/routing and isolated stop.

## Command Code

- Old docs: [mods](https://commandcode.ai/docs/mods),
  [headless](https://commandcode.ai/docs/headless),
  [CLI](https://commandcode.ai/docs/reference/cli),
  [sessions](https://commandcode.ai/docs/sessions),
  [goal](https://commandcode.ai/docs/goal); recheck before implementation.
- Potential routes: its own headless NDJSON plugin or ACP through the generic ACP plugin
  **only if the installed ACP capabilities satisfy the selected contract**.
- v1 observations: mod exception could leave CLI running; `queueMessage` steer applied
  only after a tool batch; Goal did not survive resume; `turn_end` could precede the
  final Git commit. These become named L7 cases, not assumptions.
- Before any native process/input, open authoritative source epochs. Determine
  `start_requires_input`, stable session/input/turn/request ids, replay mode, paging and
  child-environment capability from a live recording.

## Kilo Code

- OpenCode-derived CLI may expose HTTP/SSE routes resembling OpenCode. Do not select the
  OpenCode plugin merely because paths look similar.
- Implementation must fetch the installed OpenAPI/version and prove one complete API
  generation, per-session environment/tool attribution, exact input→execution mapping,
  paged events/readback and isolated stop. Otherwise use a separate per-Agent plugin or
  observe-only attach.
- v1 observations: unscoped abort could stop descendants; queued prompts were
  in-process/lost on restart; titles/summaries/compaction/update checks could reach
  external services or silently use Kilo Gateway. Disable or explicitly allow every
  auxiliary route and verify effective billing/endpoint.
- Shared server PID remains service-owned. One Agent stop never kills it.

## Zed

Zed is an ACP client/editor, not a native Agent server to drive. Its old `eval-cli` was a
one-shot evaluator, not a control session. No plugin is planned unless a current headless
server protocol appears and satisfies the same exact contract.

## Candidates noted in v1

CodeGraff/Harness, Sinew, Pi-Go, OpenChamber, Delta, Agent Teams AI, Vicoa,
JetBrains Air and SWE-agent remain historical leads at the frozen v1 commit. They are not
implementation inputs until one concrete missing harness is selected and re-researched.
