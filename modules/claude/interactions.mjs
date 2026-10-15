import { createHash } from 'node:crypto';

const MAX_PENDING = 8;
const MAX_REQUEST_BYTES = 32 * 1024;
const MAX_REPLY_BYTES = 16 * 1024;
const MAX_EVENTS = 32;

function codedError(code) {
  const error = new Error(code);
  error.diagnosticCode = code;
  return error;
}

function record(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function boundedText(value, maximum) {
  return typeof value === 'string' && value.length > 0 && value.length <= maximum
    && !/[\u0000-\u001f\u007f]/.test(value) ? value : null;
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (record(value)) {
    const keys = Object.keys(value).sort();
    return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw codedError('INTERACTION_JSON_INVALID');
  return encoded;
}

function sha256(value) {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

export function interactionDigest(value) {
  return sha256(canonicalJson(value));
}

function cloneJson(value) {
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw codedError('INTERACTION_JSON_INVALID');
  return JSON.parse(encoded);
}

function redactedText(value, secrets, maximum) {
  if (typeof value !== 'string') return null;
  let text = value;
  for (const secret of secrets) {
    if (secret) text = text.split(secret).join('[redacted]');
  }
  text = text
    .replace(/\b(?:Bearer|Basic)\s+[^\s,;]+/gi, '[redacted credential]')
    .replace(/\b(?:sk-ant-|gh[pousr]_|github_pat_|AKIA)[A-Za-z0-9_-]{12,}\b/g, '[redacted token]')
    .replace(/([?&](?:access_token|api[_-]?key|password|secret|token)=)[^&#\s]*/gi, '$1[redacted]')
    .replace(/\b[A-Za-z]:\\(?:[^\\\s]+\\)*[^\\\s]*/g, '[redacted path]')
    .replace(/\/(?:Users|home)\/[^\s"'<>]+/g, '[redacted path]');
  return text.slice(0, maximum);
}

function validQuestionInput(input) {
  const questions = input?.questions;
  return Array.isArray(questions) && questions.length >= 1 && questions.length <= 4
    && new Set(questions.map(question => question?.question)).size === questions.length
    && questions.every(question => record(question)
      && boundedText(question.question, 4096) !== null
      && boundedText(question.header, 64) !== null
      && typeof question.multiSelect === 'boolean'
      && Array.isArray(question.options) && question.options.length >= 2 && question.options.length <= 4
      && question.options.every(option => record(option)
        && boundedText(option.label, 256) !== null
        && boundedText(option.description, 2048) !== null
        && (option.preview === undefined || typeof option.preview === 'string')));
}

function safeQuestionProjection(input, secrets) {
  return {
    questions: input.questions.map(question => ({
      question: redactedText(question.question, secrets, 4096),
      header: redactedText(question.header, secrets, 64),
      multiSelect: question.multiSelect,
      options: question.options.map(option => ({
        label: redactedText(option.label, secrets, 256),
        description: redactedText(option.description, secrets, 2048),
      })),
    })),
  };
}

function permissionProjection(input) {
  const fields = Object.values(input).slice(0, 64).map((value, index) => ({
    name: `field_${index + 1}`,
    type: Array.isArray(value) ? 'array'
      : value === null ? 'null'
        : typeof value === 'object' ? 'object' : typeof value,
  }));
  return { field_count: Object.keys(input).length, fields };
}

function safeDisplay(options, secrets) {
  const display = {};
  for (const [source, target, limit] of [
    ['title', 'title', 1024],
    ['displayName', 'display_name', 256],
    ['description', 'description', 2048],
    ['decisionReason', 'decision_reason', 1024],
  ]) {
    const value = redactedText(options?.[source], secrets, limit);
    if (value !== null) display[target] = value;
  }
  if (typeof options?.defaultToNo === 'boolean') display.default_to_no = options.defaultToNo;
  if (typeof options?.suppressAlwaysAllowRule === 'boolean') {
    display.suppress_always_allow_rule = options.suppressAlwaysAllowRule;
  }
  return display;
}

function callbackDeny(message, toolUseID = null) {
  return {
    behavior: 'deny',
    message,
    ...(toolUseID ? { toolUseID } : {}),
  };
}

function validateReply(request, publicRequest, reply) {
  if (!record(reply) || Buffer.byteLength(canonicalJson(reply), 'utf8') > MAX_REPLY_BYTES) return false;
  const keys = Object.keys(reply);
  if (request.kind === 'permission') {
    if (reply.type !== 'permission') return false;
    if (reply.decision === 'allow') {
      return keys.every(key => ['type', 'decision', 'updated_input'].includes(key))
        && (reply.updated_input === undefined || record(reply.updated_input));
    }
    return reply.decision === 'deny'
      && keys.length === 3 && keys.every(key => ['type', 'decision', 'message'].includes(key))
      && boundedText(reply.message, 1024) !== null;
  }
  if (request.kind !== 'question' || request.tool_name !== 'AskUserQuestion'
      || keys.length !== 2 || !keys.every(key => ['type', 'updated_input'].includes(key))
      || reply.type !== 'question' || !record(reply.updated_input)
      || Object.keys(reply.updated_input).length !== 2
      || !Array.isArray(reply.updated_input.questions)
      || canonicalJson(reply.updated_input.questions) !== canonicalJson(publicRequest.input.questions)
      || !record(reply.updated_input.answers)) return false;
  const answers = reply.updated_input.answers;
  return Object.keys(answers).length === publicRequest.input.questions.length
    && publicRequest.input.questions.every(question => {
      const answer = answers[question.question];
      if (typeof answer === 'string') return boundedText(answer, 4096) !== null;
      return question.multiSelect === true && Array.isArray(answer)
        && answer.length >= 1 && answer.length <= 16
        && answer.every(value => boundedText(value, 4096) !== null);
    });
}

function sdkCallbackResult(request, publicRequest, reply, toolUseID) {
  if (request.kind === 'permission' && reply.decision === 'deny') {
    return { behavior: 'deny', message: reply.message, toolUseID };
  }
  if (request.kind === 'question') {
    const answers = {};
    for (let index = 0; index < request.input.questions.length; index++) {
      const originalQuestion = request.input.questions[index].question;
      const displayedQuestion = publicRequest.input.questions[index].question;
      answers[originalQuestion] = reply.updated_input.answers[displayedQuestion];
    }
    return {
      behavior: 'allow',
      updatedInput: { questions: request.input.questions, answers },
      toolUseID,
    };
  }
  return {
    behavior: 'allow',
    updatedInput: reply.updated_input === undefined ? request.input : reply.updated_input,
    toolUseID,
  };
}

export function createInteractionManager({
  bindingId,
  generation,
  bridgeBootId,
  getNativeIdentity,
  onRequest = () => {},
  onCancel = () => {},
  onChanged = () => {},
  secretValues = [],
}) {
  const pending = new Map();
  const toolGuards = new Map();
  const events = [];
  let eventCount = 0;
  let eventsTruncated = false;
  const secrets = secretValues.filter(value => typeof value === 'string' && value.length > 0);
  let retired = false;

  function currentIdentity() {
    const identity = getNativeIdentity?.();
    if (!record(identity) || typeof identity.rootId !== 'string' || !identity.rootId.trim()
        || typeof identity.scopeKey !== 'string' || !identity.scopeKey.trim()) return null;
    return identity;
  }

  function recordEvent(event) {
    eventCount++;
    if (events.length === MAX_EVENTS) {
      events.shift();
      eventsTruncated = true;
    }
    events.push({ sequence: eventCount, ...event });
  }

  function preToolUse(input) {
    const deny = () => ({
      continue: true,
      hookSpecificOutput: {
        hookEventName: 'PreToolUse',
        permissionDecision: 'deny',
        permissionDecisionReason: 'ELIOT could not validate this exact tool request.',
      },
    });
    if (retired || !record(input) || input.hook_event_name !== 'PreToolUse') return deny();
    const toolName = boundedText(input.tool_name, 256);
    const toolUseID = boundedText(input.tool_use_id, 256);
    if (toolName === null || toolUseID === null || !record(input.tool_input)) return deny();
    let toolInput;
    try { toolInput = cloneJson(input.tool_input); }
    catch { return deny(); }
    const digest = interactionDigest({ tool_name: toolName, tool_use_id: toolUseID, input: toolInput });
    const previous = toolGuards.get(toolUseID);
    if ((previous !== undefined && previous !== digest) || toolGuards.size >= 64) return deny();
    toolGuards.set(toolUseID, digest);
    return {
      continue: true,
      hookSpecificOutput: {
        hookEventName: 'PreToolUse',
        permissionDecision: 'ask',
        permissionDecisionReason: 'ELIOT requires an exact one-shot permission decision.',
      },
    };
  }

  function canUseTool(toolNameValue, inputValue, options) {
    const toolUseID = boundedText(options?.toolUseID, 256);
    const deny = message => callbackDeny(message, toolUseID);
    if (retired) return Promise.resolve(deny('The Claude bridge is retired.'));
    const identity = currentIdentity();
    const toolName = boundedText(toolNameValue, 256);
    const requestID = boundedText(options?.requestId, 128);
    const signal = options?.signal;
    if (!identity || toolName === null || toolUseID === null || requestID === null
        || !record(inputValue) || !signal
        || typeof signal.addEventListener !== 'function'
        || typeof signal.removeEventListener !== 'function') {
      return Promise.resolve(deny('The Claude SDK callback identity is unavailable.'));
    }
    let input;
    try { input = cloneJson(inputValue); }
    catch { return Promise.resolve(deny('The Claude SDK callback input is not JSON-safe.')); }
    const requestBytes = Buffer.byteLength(canonicalJson(input), 'utf8');
    if (requestBytes > MAX_REQUEST_BYTES) {
      return Promise.resolve(deny('The Claude SDK callback request exceeds its size boundary.'));
    }
    const guardDigest = toolGuards.get(toolUseID);
    const expectedGuard = interactionDigest({ tool_name: toolName, tool_use_id: toolUseID, input });
    if (guardDigest !== expectedGuard) {
      toolGuards.delete(toolUseID);
      return Promise.resolve(deny('The request did not pass its matching PreToolUse guard.'));
    }
    toolGuards.delete(toolUseID);
    if (signal.aborted) return Promise.resolve(deny('The Claude SDK callback was cancelled.'));
    if (pending.has(requestID) || pending.size >= MAX_PENDING) {
      return Promise.resolve(deny('The Claude SDK callback identity is already pending or capacity is full.'));
    }
    const kind = toolName === 'AskUserQuestion' ? 'question' : 'permission';
    if (kind === 'question' && !validQuestionInput(input)) {
      return Promise.resolve(deny('The Claude question request does not match its typed contract.'));
    }
    const request = {
      schema_version: 1,
      request_id: requestID,
      tool_use_id: toolUseID,
      kind,
      tool_name: toolName,
      input,
      binding_id: bindingId,
      generation,
      bridge_boot_id: bridgeBootId,
      native_root_id: identity.rootId,
      native_scope_key: identity.scopeKey,
    };
    request.request_sha256 = interactionDigest(request);
    let projectedInput;
    try {
      projectedInput = kind === 'question'
        ? safeQuestionProjection(input, secrets)
        : permissionProjection(input);
    } catch {
      return Promise.resolve(deny('The Claude SDK callback projection is ambiguous.'));
    }
    if (kind === 'question'
        && new Set(projectedInput.questions.map(question => question.question)).size
          !== projectedInput.questions.length) {
      return Promise.resolve(deny('The redacted Claude question labels are ambiguous.'));
    }
    const publicRequest = {
      schema_version: request.schema_version,
      request_id: request.request_id,
      tool_use_id: request.tool_use_id,
      kind: request.kind,
      tool_name: request.tool_name,
      request_sha256: request.request_sha256,
      binding_id: request.binding_id,
      generation: request.generation,
      bridge_boot_id: request.bridge_boot_id,
      native_root_id: request.native_root_id,
      native_scope_key: request.native_scope_key,
      input: projectedInput,
      input_redacted: true,
      display: safeDisplay(options, secrets),
      status: 'pending',
    };
    return new Promise(resolve => {
      const entry = { request, publicRequest, resolve, signal, toolUseID, abort: null };
      entry.abort = () => retireOne(requestID, entry, 'SDK_CALLBACK_ABORTED');
      pending.set(requestID, entry);
      recordEvent({
        kind: 'interaction_pending',
        request_id: requestID,
        request_sha256: request.request_sha256,
        interaction_kind: kind,
        tool_use_id: toolUseID,
        bridge_boot_id: bridgeBootId,
        native_root_id: identity.rootId,
        native_scope_key: identity.scopeKey,
      });
      onRequest(publicRequest);
      onChanged();
      signal.addEventListener('abort', entry.abort, { once: true });
      if (signal.aborted) entry.abort();
    });
  }

  function retireOne(requestID, entry, reason) {
    if (pending.get(requestID) !== entry) return;
    pending.delete(requestID);
    entry.signal?.removeEventListener('abort', entry.abort);
    onCancel({
      request_id: requestID,
      request_sha256: entry.request.request_sha256,
      kind: entry.request.kind,
      tool_use_id: entry.toolUseID,
      bridge_boot_id: entry.request.bridge_boot_id,
      native_root_id: entry.request.native_root_id,
      native_scope_key: entry.request.native_scope_key,
      diagnostic_code: reason,
    });
    recordEvent({
      kind: 'interaction_cancelled',
      request_id: requestID,
      request_sha256: entry.request.request_sha256,
      interaction_kind: entry.request.kind,
      tool_use_id: entry.toolUseID,
      diagnostic_code: reason,
    });
    entry.resolve(callbackDeny('This Claude SDK callback was cancelled before a matching user reply arrived.', entry.toolUseID));
    onChanged();
  }

  function reply(command) {
    const input = command?.input;
    const identity = currentIdentity();
    if (retired || !identity || !record(input)
        || command.binding_id !== bindingId || command.generation !== generation
        || command.native_root_id !== identity.rootId) {
      throw codedError('INTERACTION_REPLY_IDENTITY');
    }
    const requestID = boundedText(input.request_id, 128);
    const entry = requestID === null ? null : pending.get(requestID);
    const replyValue = input.reply;
    if (!entry || entry.request.bridge_boot_id !== bridgeBootId
        || entry.request.native_root_id !== identity.rootId
        || entry.request.native_scope_key !== identity.scopeKey
        || input.request_sha256 !== entry.request.request_sha256
        || input.interaction_kind !== entry.request.kind
        || !validateReply(entry.request, entry.publicRequest, replyValue)) {
      throw codedError('INTERACTION_REPLY_MISMATCH');
    }
    const replySha256 = interactionDigest(replyValue);
    if (input.reply_sha256 !== undefined && input.reply_sha256 !== replySha256) {
      throw codedError('INTERACTION_REPLY_MISMATCH');
    }
    pending.delete(requestID);
    entry.signal?.removeEventListener('abort', entry.abort);
    entry.resolve(sdkCallbackResult(entry.request, entry.publicRequest, replyValue, entry.toolUseID));
    recordEvent({
      kind: 'interaction_reply_acknowledged',
      request_id: requestID,
      request_sha256: entry.request.request_sha256,
      interaction_kind: entry.request.kind,
      reply_sha256: replySha256,
      tool_use_id: entry.toolUseID,
    });
    onChanged();
    return {
      request_id: requestID,
      request_sha256: entry.request.request_sha256,
      interaction_kind: entry.request.kind,
      reply_sha256: replySha256,
      bridge_boot_id: bridgeBootId,
      native_root_id: identity.rootId,
      native_scope_key: identity.scopeKey,
    };
  }

  function retire(reason) {
    if (retired) return;
    retired = true;
    toolGuards.clear();
    for (const [requestID, entry] of [...pending.entries()]) retireOne(requestID, entry, reason);
    onChanged();
  }

  return {
    preToolUse,
    canUseTool,
    reply,
    snapshot: () => [...pending.values()].map(entry => cloneJson(entry.publicRequest)),
    events: () => cloneJson(events),
    eventCount: () => eventCount,
    eventsTruncated: () => eventsTruncated,
    retire,
  };
}
