// Pure mapping from the documented Antigravity CLI stream-json protocol to
// the compact controller observation. This file imports nothing: bridge.mjs
// feeds it the live `agy` stdout stream and selftest.mjs feeds it the
// recorded fixtures, so the fixture path and the live path cannot drift.
//
// Protocol basis (official docs, accessed 2026-10-02):
//   https://antigravity.google/docs/cli/headless/  (AG-HEADLESS)
//   https://antigravity.google/docs/subagents/     (AG-CHILDREN)
// The repo's runtime matrix selects the `native_warm_stream` entrypoint:
// `agy --input-format stream-json --output-format stream-json`. Output lines
// are NDJSON objects discriminated by `event` (never the Claude `type`).
//
// Mapping boundaries (implementation plan C08 / runtime notes AG-*):
// - Identity is the native conversation_id carried by the init event and
//   repeated by every payload. Children are observed only through
//   step_update.subagent_info, addressed by their own conversation_id.
// - A child that goes idle is NOT released: subagents re-awaken on message
//   (AG-CHILDREN lifecycle Running/Idle/Killed), and the stream carries no
//   release event, so observed children keep status `observed` forever.
// - result SUCCESS does not prove every tool succeeded: tools can be
//   soft-denied in headless mode while the run exits 0. A tool step whose
//   tool_info carries an error is kept as addressed evidence in
//   `tool_errors` and never dropped when the turn result is SUCCESS.
// - result counters (num_turns, duration_seconds, usage) are cumulative
//   over the whole session per the vendor docs: the latest result replaces
//   the previous snapshot; results are never summed.
// - Unknown event names are tolerated exactly like the native CLI tolerates
//   them on input (skipped, counted in `other_events`): a newer CLI event
//   must not break the codec.
// - No goal field exists in this protocol and none is invented here.

const MAX_STEPS = 200;
const MAX_CHILDREN = 100;
const MAX_TURNS = 32;
const MAX_TOOL_ERRORS = 32;

export function createStreamState() {
  return {
    phase: 'awaiting_init', // awaiting_init | ready | init_failed | stream_ended | stream_failed
    init: null,
    init_failure: null,
    execution: 'not_started', // not_started | running | turn_completed | turn_failed | turn_waiting | turn_open | init_failed | stream_ended | stream_failed
    steps: new Map(), // step_index -> step record, insertion ordered
    children: new Map(), // child conversation_id -> child record
    turns: [],
    usage: null,
    tool_errors: [],
    native_events_seen: 0,
    other_events: 0,
    gaps: 0,
    stderr_tail: '',
    stream_error: null,
    exit_code: null,
  };
}

// The only input message this adapter ever encodes. The native protocol
// accepts exactly one prompt shape on the warm channel; control_request /
// control_response and slash input are documented as unsupported there and
// are never produced by this module. Content is the plain string form; the
// documented text-block array form is never needed to send a prompt.
export function encodeUserMessage(text) {
  if (typeof text !== 'string' || !text.trim()) throw new Error('PROMPT_TEXT_REQUIRED');
  return { event: 'user', message: { content: text } };
}

function usageNumbers(value) {
  if (!value || typeof value !== 'object') return null;
  const out = {};
  for (const key of ['input_tokens', 'output_tokens', 'thinking_tokens', 'cache_read_tokens', 'total_tokens']) {
    if (typeof value[key] === 'number') out[key] = value[key];
  }
  return Object.keys(out).length ? out : null;
}

function applyInit(state, event) {
  const payload = event.init && typeof event.init === 'object' ? event.init : {};
  if (state.init) { state.gaps++; return; } // init is emitted once per stream
  const conversationId = typeof event.conversation_id === 'string' && event.conversation_id
    ? event.conversation_id
    : null;
  if (!conversationId) { state.gaps++; return; }
  state.init = {
    conversation_id: conversationId,
    cwd: typeof payload.cwd === 'string' ? payload.cwd : null,
    tools: Array.isArray(payload.tools) ? payload.tools.filter(t => typeof t === 'string') : [],
    permission_mode: typeof payload.permission_mode === 'string' ? payload.permission_mode : null,
    // model/agent appear in init only when overridden at launch; an absent
    // field is reported as null, never as a guessed effective model.
    model: typeof payload.model === 'string' ? payload.model : null,
    agent: typeof payload.agent === 'string' ? payload.agent : null,
  };
  if (state.phase === 'awaiting_init') state.phase = 'ready';
  if (state.execution === 'not_started') state.execution = 'running';
}

function stepRecord(state, index) {
  let record = state.steps.get(index);
  if (!record) {
    if (state.steps.size >= MAX_STEPS) {
      state.steps.delete(state.steps.keys().next().value);
      state.gaps++;
    }
    record = {
      step_index: index,
      state: null, // ACTIVE | DONE as reported natively
      step_type: null,
      tool_name: null,
      text_chars: 0,
      tool_error: null,
      subagent_ids: [],
    };
    state.steps.set(index, record);
  }
  return record;
}

function childRecord(state, entry) {
  const id = typeof entry?.conversation_id === 'string' && entry.conversation_id ? entry.conversation_id : null;
  if (!id) { state.gaps++; return null; }
  let child = state.children.get(id);
  if (!child) {
    if (state.children.size >= MAX_CHILDREN) { state.gaps++; return null; }
    child = {
      conversation_id: id,
      type_name: null,
      role: null,
      workspace_uris: [],
      log_uri: null,
      // The stream reports the invocation, not a lifecycle: an idle child
      // can wake again, so no terminal or released state is ever inferred.
      status: 'observed',
    };
    state.children.set(id, child);
  }
  if (typeof entry.type_name === 'string') child.type_name = entry.type_name;
  if (typeof entry.role === 'string') child.role = entry.role;
  if (typeof entry.log_uri === 'string') child.log_uri = entry.log_uri;
  if (Array.isArray(entry.workspace_uris)) child.workspace_uris = entry.workspace_uris.filter(u => typeof u === 'string');
  return child;
}

function applyStepUpdate(state, event) {
  const payload = event.step_update && typeof event.step_update === 'object' ? event.step_update : null;
  if (!payload || typeof payload.step_index !== 'number') { state.gaps++; return; }
  const carrier = typeof payload.conversation_id === 'string' ? payload.conversation_id : null;
  if (carrier && state.init && carrier !== state.init.conversation_id) {
    // Steps of the root stream carry the root conversation_id. A foreign id
    // is not silently merged into the root's step sequence.
    state.gaps++;
    return;
  }
  const record = stepRecord(state, payload.step_index);
  if (typeof payload.state === 'string') record.state = payload.state;
  if (typeof payload.step_type === 'string') record.step_type = payload.step_type;
  if (typeof payload.tool_name === 'string') record.tool_name = payload.tool_name;
  if (typeof payload.text_delta === 'string') record.text_chars += payload.text_delta.length;
  const toolInfo = payload.tool_info && typeof payload.tool_info === 'object' ? payload.tool_info : null;
  if (toolInfo?.error && typeof toolInfo.error === 'object' && !record.tool_error) {
    // A failed or soft-denied tool step: addressed evidence that survives
    // any later SUCCESS result for the turn.
    record.tool_error = {
      type: typeof toolInfo.error.type === 'string' ? toolInfo.error.type : 'unknown',
      message: typeof toolInfo.error.message === 'string' ? toolInfo.error.message.slice(0, 256) : null,
    };
    state.tool_errors.push({
      step_index: record.step_index,
      tool_name: record.tool_name ?? (typeof toolInfo.name === 'string' ? toolInfo.name : null),
      error_type: record.tool_error.type,
    });
    if (state.tool_errors.length > MAX_TOOL_ERRORS) {
      state.tool_errors.splice(0, state.tool_errors.length - MAX_TOOL_ERRORS);
      state.gaps++;
    }
  }
  const subagents = payload.subagent_info?.subagents;
  if (Array.isArray(subagents)) {
    for (const entry of subagents) {
      const child = childRecord(state, entry);
      if (child && !record.subagent_ids.includes(child.conversation_id)) record.subagent_ids.push(child.conversation_id);
    }
  }
}

function applyResult(state, event) {
  const payload = event.result && typeof event.result === 'object' ? event.result : null;
  if (!payload || typeof payload.status !== 'string') { state.gaps++; return; }
  const turn = {
    status: payload.status,
    error: typeof payload.error === 'string' ? payload.error : null,
    num_turns: typeof payload.num_turns === 'number' ? payload.num_turns : null,
    duration_seconds: typeof payload.duration_seconds === 'number' ? payload.duration_seconds : null,
    response_chars: typeof payload.response === 'string' ? payload.response.length : 0,
  };
  state.turns.push(turn);
  if (state.turns.length > MAX_TURNS) { state.turns.splice(0, state.turns.length - MAX_TURNS); state.gaps++; }
  const usage = usageNumbers(payload.usage);
  if (usage) {
    // Cumulative session counters: replace, never add to the previous turn.
    state.usage = { basis: 'native_cumulative_session', source: 'latest_result_event', ...usage };
  }
  if (state.phase === 'awaiting_init') {
    // A result before any init is the documented failure envelope for a run
    // that never started (for example an unrecognized --model): an init
    // failure, distinct from a failed turn, with no identity invented.
    state.phase = 'init_failed';
    state.init_failure = { status: turn.status, error: turn.error, native_responded: true };
    state.execution = 'init_failed';
    return;
  }
  switch (turn.status) {
    case 'SUCCESS': state.execution = 'turn_completed'; break;
    case 'WAITING': state.execution = 'turn_waiting'; break;
    case 'RUNNING': state.execution = 'turn_open'; break;
    default: state.execution = 'turn_failed'; break; // ERROR | CANCELED | INTERRUPTED | INVALID | unknown
  }
}

export function applyNativeEvent(state, event) {
  if (!event || typeof event !== 'object' || typeof event.event !== 'string') { state.gaps++; return state; }
  state.native_events_seen++;
  switch (event.event) {
    case 'init': applyInit(state, event); break;
    case 'step_update': applyStepUpdate(state, event); break;
    case 'result': applyResult(state, event); break;
    default: state.other_events++; break; // future/unknown event: skipped, like the native CLI skips unknown input
  }
  return state;
}

export function noteMalformedLine(state) {
  // A stdout line that is not a JSON event object: counted as a coverage
  // gap, never parsed by guessing and never fatal to the stream mapping.
  state.gaps++;
}

export function noteStderr(state, chunk) {
  if (typeof chunk !== 'string' || !chunk) return;
  state.stderr_tail = (state.stderr_tail + chunk).slice(-2048);
}

export function noteStreamEnd(state) {
  if (state.phase === 'awaiting_init') {
    state.phase = 'init_failed';
    state.init_failure = { status: null, error: 'stream_ended_before_init', native_responded: false };
  } else if (state.phase === 'ready') {
    state.phase = 'stream_ended';
  }
  if (state.execution === 'running' || state.execution === 'not_started' || state.execution === 'turn_open') {
    state.execution = 'stream_ended';
  }
}

export function noteStreamFailure(state, error) {
  const message = error instanceof Error ? error.message : String(error);
  if (state.phase === 'awaiting_init') {
    state.phase = 'init_failed';
    state.init_failure = { status: null, error: message.slice(0, 512), native_responded: false };
  } else {
    state.phase = 'stream_failed';
  }
  state.execution = 'stream_failed';
  state.stream_error = message.slice(0, 512);
}

export function noteProcessExit(state, code) {
  state.exit_code = typeof code === 'number' ? code : null;
  if (state.phase === 'ready' && code !== 0 && code !== null) {
    // Non-zero exit after a clean stream is still a native fact: the exit
    // code is retained in the snapshot instead of being smoothed over.
    state.stream_error = `native_process_exit_${code}`;
  }
}

export function snapshot(state) {
  const steps = [...state.steps.values()];
  return {
    adapter: {
      entrypoint: 'antigravity_cli_warm_stream',
      phase: state.phase,
      // The native stream reports no executor version; none is invented.
      executor_version: null,
      model: state.init?.model ?? null,
      agent: state.init?.agent ?? null,
      permission_mode: state.init?.permission_mode ?? null,
      tools_count: state.init ? state.init.tools.length : null,
      cwd: state.init?.cwd ?? null,
    },
    execution: state.execution,
    // One root stream proves only the children it carried; discovery of
    // the native family stays partial and no child is ever marked released.
    family_completeness: 'partial',
    observed_children: [...state.children.values()].slice(-MAX_CHILDREN),
    steps: steps.slice(-50),
    steps_total: steps.length,
    turns: state.turns,
    usage: state.usage,
    // Tool failures/soft-denials stay listed even when every turn result
    // is SUCCESS; a SUCCESS result alone is not tool success evidence.
    tool_errors: state.tool_errors,
    tools_with_errors: state.tool_errors.length,
    init_failure: state.init_failure,
    stream_error: state.stream_error,
    exit_code: state.exit_code,
    stderr_tail: state.stderr_tail ? state.stderr_tail.slice(-512) : null,
    native_events_seen: state.native_events_seen,
    other_events: state.other_events,
    gaps: state.gaps,
  };
}
