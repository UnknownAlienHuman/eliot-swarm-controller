// Pure mapping from pinned Claude Agent SDK stream messages to the compact
// controller observation. This file imports nothing: bridge.mjs feeds it the
// live query() stream and selftest.mjs feeds it the recorded fixtures, so the
// fixture path and the live path cannot drift apart.
//
// Mapping boundaries (implementation plan C08 / runtime notes CL-*):
// - One API assistant turn arrives as several assistant frames that share one
//   message.id, each carrying the block it delivers (frame-local content
//   arrays restart at index 0, so frame position is not block identity).
//   Blocks are appended in arrival order under that id; tool blocks dedupe by
//   their native tool id. A frame is never deduplicated as a whole message,
//   so a repeated message.id cannot lose a tool block.
// - A replayed frame (same frame uuid) is applied once.
// - stream_event partials are token deltas: counted, never inventoried as
//   messages or children. Child linkage comes only from complete frames via
//   parent_tool_use_id and Task/Agent tool_use blocks.
// - Result usage is the SDK's cumulative estimate for the query: the latest
//   result replaces the previous snapshot; results are never summed.
// - An error result before init is an init failure, kept distinct from a
//   failed turn and from an empty successful start.

const MAX_MESSAGES = 200;
const MAX_CHILDREN = 100;
const MAX_TURNS = 32;
const MAX_PERMISSION_REQUESTS = 32;
const MAX_TASK_NOTIFICATIONS = 32;

export function createStreamState() {
  return {
    phase: 'awaiting_init', // awaiting_init | ready | init_failed | stream_ended | stream_failed
    init: null,
    init_failure: null,
    execution: 'not_started', // not_started | running | turn_completed | turn_failed | init_failed | stream_ended | stream_failed
    messages: new Map(), // message.id -> record, insertion ordered
    frame_uuids: new Set(),
    children: new Map(), // Task/Agent tool_use id -> child record
    turns: [],
    usage: null,
    permission_denials: [],
    permission_denials_advisory: [],
    permission_requests: [],
    task_notifications: [],
    native_events_seen: 0,
    partial_events_seen: 0,
    other_events: 0,
    gaps: 0,
    stderr_tail: '',
  };
}

function blockSummary(block) {
  if (!block || typeof block !== 'object') return { type: 'unknown' };
  switch (block.type) {
    case 'text':
      return { type: 'text', chars: typeof block.text === 'string' ? block.text.length : 0 };
    case 'thinking':
      return { type: 'thinking', chars: typeof block.thinking === 'string' ? block.thinking.length : 0 };
    case 'tool_use':
      return { type: 'tool_use', id: typeof block.id === 'string' ? block.id : null, name: typeof block.name === 'string' ? block.name : null };
    case 'tool_result':
      return { type: 'tool_result', tool_use_id: typeof block.tool_use_id === 'string' ? block.tool_use_id : null, is_error: block.is_error === true };
    default:
      return { type: typeof block.type === 'string' ? block.type : 'unknown' };
  }
}

function blockIdentity(summary) {
  if (summary.type === 'tool_use' && summary.id) return `tool_use:${summary.id}`;
  if (summary.type === 'tool_result' && summary.tool_use_id) return `tool_result:${summary.tool_use_id}`;
  return null;
}

function messageRecord(state, msg) {
  const id = typeof msg.message?.id === 'string' && msg.message.id ? msg.message.id : null;
  if (!id) { state.gaps++; return null; }
  let record = state.messages.get(id);
  if (!record) {
    if (state.messages.size >= MAX_MESSAGES) {
      state.messages.delete(state.messages.keys().next().value);
      state.gaps++;
    }
    record = {
      id,
      session_id: typeof msg.session_id === 'string' ? msg.session_id : null,
      model: typeof msg.message?.model === 'string' ? msg.message.model : null,
      parent_tool_use_id: typeof msg.parent_tool_use_id === 'string' ? msg.parent_tool_use_id : null,
      subagent_type: typeof msg.subagent_type === 'string' ? msg.subagent_type : null,
      task_description: typeof msg.task_description === 'string' ? msg.task_description.slice(0, 256) : null,
      blocks: [], // block summaries in arrival order across all frames of this id
      block_positions: new Map(), // native tool identity -> position in blocks
      stop_reason: null,
      error: null,
      frames: 0,
    };
    state.messages.set(id, record);
  }
  return record;
}

function childRecord(state, toolUseId) {
  let child = state.children.get(toolUseId);
  if (!child) {
    if (state.children.size >= MAX_CHILDREN) { state.gaps++; return null; }
    child = {
      tool_use_id: toolUseId,
      subagent_type: null,
      task_description: null,
      status: 'observed', // observed | running | completed | failed | stopped
      frames: 0,
      result_seen: false,
      result_is_error: null,
    };
    state.children.set(toolUseId, child);
  }
  return child;
}

function applyAssistant(state, msg) {
  const record = messageRecord(state, msg);
  if (!record) return;
  record.frames++;
  if (typeof msg.error === 'string') record.error = msg.error;
  if (typeof msg.message?.stop_reason === 'string') record.stop_reason = msg.message.stop_reason;
  const parent = typeof msg.parent_tool_use_id === 'string' ? msg.parent_tool_use_id : null;
  if (parent) {
    const child = childRecord(state, parent);
    if (child) { child.frames++; if (child.status === 'observed') child.status = 'running'; }
  }
  const blocks = Array.isArray(msg.message?.content) ? msg.message.content : [];
  for (const block of blocks) {
    const summary = blockSummary(block);
    const identity = blockIdentity(summary);
    const known = identity ? record.block_positions.get(identity) : undefined;
    if (known !== undefined) {
      // Same native tool block delivered again: refresh it in place, keep order.
      record.blocks[known] = { ...summary, index: known };
    } else {
      summary.index = record.blocks.length;
      record.blocks.push(summary);
      if (identity) record.block_positions.set(identity, summary.index);
    }
    if (!parent && summary.type === 'tool_use' && (summary.name === 'Task' || summary.name === 'Agent') && summary.id) {
      const child = childRecord(state, summary.id);
      if (child) {
        child.status = child.result_seen ? child.status : 'running';
        if (typeof block.input?.subagent_type === 'string') child.subagent_type = block.input.subagent_type;
        if (typeof block.input?.description === 'string') child.task_description = block.input.description.slice(0, 256);
      }
    }
  }
}

function applyUser(state, msg) {
  const parent = typeof msg.parent_tool_use_id === 'string' ? msg.parent_tool_use_id : null;
  if (parent) {
    const child = childRecord(state, parent);
    if (child) { child.frames++; if (child.status === 'observed') child.status = 'running'; }
  }
  const blocks = Array.isArray(msg.message?.content) ? msg.message.content : [];
  for (const block of blocks) {
    if (!block || block.type !== 'tool_result' || typeof block.tool_use_id !== 'string') continue;
    // Attach the result to the exact tool_use block that requested it.
    for (const record of state.messages.values()) {
      for (const summary of record.blocks) {
        if (summary.type === 'tool_use' && summary.id === block.tool_use_id) {
          summary.result_seen = true;
          summary.result_is_error = block.is_error === true;
        }
      }
    }
    if (!parent) {
      const child = state.children.get(block.tool_use_id);
      if (child) {
        child.result_seen = true;
        child.result_is_error = block.is_error === true;
        child.status = block.is_error === true ? 'failed' : 'completed';
      }
    }
  }
}

function compactUsageNumbers(value) {
  if (!value || typeof value !== 'object') return null;
  const out = {};
  for (const [key, entry] of Object.entries(value)) {
    if (typeof entry === 'number') out[key] = entry;
  }
  return Object.keys(out).length ? out : null;
}

function applyResult(state, msg) {
  const subtype = typeof msg.subtype === 'string' ? msg.subtype : 'unknown';
  if (state.phase === 'awaiting_init' && subtype === 'error_during_execution') {
    state.phase = 'init_failed';
    state.init_failure = {
      subtype,
      errors: Array.isArray(msg.errors) ? msg.errors.filter(e => typeof e === 'string').slice(0, 8) : [],
    };
  }
  const turn = {
    subtype,
    is_error: msg.is_error === true,
    num_turns: typeof msg.num_turns === 'number' ? msg.num_turns : null,
    stop_reason: typeof msg.stop_reason === 'string' ? msg.stop_reason : null,
    duration_ms: typeof msg.duration_ms === 'number' ? msg.duration_ms : null,
    queued_turn_count: typeof msg.queued_turn_count === 'number' ? msg.queued_turn_count : null,
    result_index: typeof msg.result_index === 'number' ? msg.result_index : null,
  };
  state.turns.push(turn);
  if (state.turns.length > MAX_TURNS) { state.turns.splice(0, state.turns.length - MAX_TURNS); state.gaps++; }
  if (state.phase === 'init_failed' && state.execution === 'not_started') state.execution = 'init_failed';
  else state.execution = subtype === 'success' ? 'turn_completed' : 'turn_failed';
  const modelUsage = {};
  if (msg.modelUsage && typeof msg.modelUsage === 'object') {
    for (const [model, entry] of Object.entries(msg.modelUsage)) {
      const compact = compactUsageNumbers(entry);
      if (compact) modelUsage[model] = compact;
    }
  }
  // Cumulative for the whole query: replace, never add to the previous frame.
  state.usage = {
    basis: 'sdk_cumulative_estimate',
    source: 'latest_result_frame',
    total_cost_usd: typeof msg.total_cost_usd === 'number' ? msg.total_cost_usd : null,
    model_usage: modelUsage,
  };
  if (Array.isArray(msg.permission_denials)) {
    state.permission_denials = msg.permission_denials.slice(0, MAX_PERMISSION_REQUESTS).map(d => ({
      tool_name: typeof d?.tool_name === 'string' ? d.tool_name : null,
      tool_use_id: typeof d?.tool_use_id === 'string' ? d.tool_use_id : null,
    }));
  }
}

function applySystem(state, msg) {
  if (msg.subtype === 'init') {
    if (state.phase === 'awaiting_init') state.phase = 'ready';
    state.init = {
      session_id: typeof msg.session_id === 'string' ? msg.session_id : null,
      claude_code_version: typeof msg.claude_code_version === 'string' ? msg.claude_code_version : null,
      model: typeof msg.model === 'string' ? msg.model : null,
      permission_mode: typeof msg.permissionMode === 'string' ? msg.permissionMode : null,
      cwd: typeof msg.cwd === 'string' ? msg.cwd : null,
      api_key_source: typeof msg.apiKeySource === 'string' ? msg.apiKeySource : null,
      tools_count: Array.isArray(msg.tools) ? msg.tools.length : null,
      effort: typeof msg.effort === 'string' ? msg.effort : null,
    };
    if (state.execution === 'not_started') state.execution = 'running';
    return;
  }
  if (msg.subtype === 'permission_denied') {
    // Advisory only (result.permission_denials is authoritative): an auto
    // denial that never reached a permission prompt.
    state.permission_denials_advisory.push({
      tool_name: typeof msg.tool_name === 'string' ? msg.tool_name : null,
      tool_use_id: typeof msg.tool_use_id === 'string' ? msg.tool_use_id : null,
    });
    if (state.permission_denials_advisory.length > MAX_PERMISSION_REQUESTS) {
      state.permission_denials_advisory.splice(0, state.permission_denials_advisory.length - MAX_PERMISSION_REQUESTS);
      state.gaps++;
    }
    return;
  }
  if (msg.subtype === 'task_notification') {
    state.task_notifications.push({
      task_id: typeof msg.task_id === 'string' ? msg.task_id : null,
      tool_use_id: typeof msg.tool_use_id === 'string' ? msg.tool_use_id : null,
      status: typeof msg.status === 'string' ? msg.status : null,
    });
    if (state.task_notifications.length > MAX_TASK_NOTIFICATIONS) {
      state.task_notifications.splice(0, state.task_notifications.length - MAX_TASK_NOTIFICATIONS);
      state.gaps++;
    }
    if (typeof msg.tool_use_id === 'string') {
      const child = state.children.get(msg.tool_use_id);
      if (child && typeof msg.status === 'string') {
        child.status = msg.status === 'completed' ? 'completed' : msg.status === 'failed' ? 'failed' : msg.status === 'stopped' ? 'stopped' : child.status;
      }
    }
  }
}

export function applySdkMessage(state, msg) {
  if (!msg || typeof msg !== 'object' || typeof msg.type !== 'string') { state.gaps++; return state; }
  state.native_events_seen++;
  if (typeof msg.uuid === 'string') {
    if (state.frame_uuids.has(msg.uuid)) return state; // replayed frame: apply once
    state.frame_uuids.add(msg.uuid);
    if (state.frame_uuids.size > 8192) {
      // Bounded replay memory; dropping old uuids can only re-apply an old
      // frame, whose blocks merge by index and do not duplicate content.
      state.frame_uuids.delete(state.frame_uuids.values().next().value);
    }
  }
  switch (msg.type) {
    case 'system': applySystem(state, msg); break;
    case 'assistant': applyAssistant(state, msg); break;
    case 'user': applyUser(state, msg); break;
    case 'result': applyResult(state, msg); break;
    case 'stream_event': state.partial_events_seen++; break;
    default: state.other_events++; break;
  }
  return state;
}

export function notePermissionRequest(state, request) {
  state.permission_requests.push({
    tool_name: typeof request?.tool_name === 'string' ? request.tool_name : null,
    tool_use_id: typeof request?.tool_use_id === 'string' ? request.tool_use_id : null,
    disposition: 'denied_no_reply_path_in_this_artifact',
  });
  if (state.permission_requests.length > MAX_PERMISSION_REQUESTS) {
    state.permission_requests.splice(0, state.permission_requests.length - MAX_PERMISSION_REQUESTS);
    state.gaps++;
  }
}

export function noteStderr(state, chunk) {
  if (typeof chunk !== 'string' || !chunk) return;
  state.stderr_tail = (state.stderr_tail + chunk).slice(-2048);
}

export function noteStreamEnd(state) {
  if (state.phase === 'awaiting_init') {
    state.phase = 'init_failed';
    state.init_failure = { subtype: 'stream_ended_before_init', errors: [] };
  } else if (state.phase === 'ready') {
    state.phase = 'stream_ended';
  }
  if (state.execution === 'running' || state.execution === 'not_started') state.execution = 'stream_ended';
}

export function noteStreamFailure(state, error) {
  const message = error instanceof Error ? error.message : String(error);
  if (state.phase === 'awaiting_init') {
    state.phase = 'init_failed';
    state.init_failure = { subtype: 'stream_failed_before_init', errors: [message.slice(0, 512)] };
  } else {
    state.phase = 'stream_failed';
  }
  state.execution = 'stream_failed';
  state.stream_error = message.slice(0, 512);
}

function messageSnapshot(record) {
  return {
    id: record.id,
    model: record.model,
    parent_tool_use_id: record.parent_tool_use_id,
    subagent_type: record.subagent_type,
    task_description: record.task_description,
    blocks: record.blocks,
    stop_reason: record.stop_reason,
    error: record.error,
    frames: record.frames,
  };
}

export function snapshot(state) {
  const messages = [...state.messages.values()];
  return {
    adapter: {
      entrypoint: 'claude_agent_sdk_streaming_input',
      phase: state.phase,
      executor_version: state.init?.claude_code_version ?? null,
      model: state.init?.model ?? null,
      permission_mode: state.init?.permission_mode ?? null,
      tools_count: state.init?.tools_count ?? null,
      effort_observed: state.init?.effort ?? null,
    },
    execution: state.execution,
    // One stream proves only the frames it carried; discovery stays partial.
    family_completeness: 'partial',
    observed_children: [...state.children.values()].slice(-MAX_CHILDREN),
    // Permission requests are answered (denied) immediately in this artifact,
    // so nothing stays pending; the decided requests are listed separately.
    pending_requests: [],
    permission_requests: state.permission_requests,
    permission_denials: state.permission_denials,
    permission_denials_advisory: state.permission_denials_advisory,
    messages: messages.slice(-20).map(messageSnapshot),
    messages_total: messages.length,
    turns: state.turns,
    usage: state.usage,
    task_notifications: state.task_notifications,
    init_failure: state.init_failure,
    stream_error: state.stream_error ?? null,
    stderr_tail: state.stderr_tail ? state.stderr_tail.slice(-512) : null,
    native_events_seen: state.native_events_seen,
    partial_events_seen: state.partial_events_seen,
    other_events: state.other_events,
    gaps: state.gaps,
  };
}
