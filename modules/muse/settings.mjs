// Native setter mappings for SDK 1.3.0. The controller does not know these keys.
export function configuration(settings, sessionId) {
  if (!settings || typeof settings !== 'object' || Array.isArray(settings)) throw new Error('SETTINGS_OBJECT_REQUIRED');
  const keys = Object.keys(settings);
  // Each native setter is one independently idempotent command. Do not report
  // an atomic multi-setting change which the native protocol cannot perform.
  if (keys.length !== 1) throw new Error('USE_ONE_NATIVE_SETTER_PER_CONFIGURE');
  const key = keys[0], desired = settings[key];
  const base = { sessionId };
  if (key === 'reasoningEffort') {
    if (!['none','minimal','low','medium','high','xhigh','max','ultra'].includes(desired)) throw new Error('INVALID_REASONING_EFFORT');
    return { key, desired, method:'session/setReasoningEffort', params:{...base,reasoningEffort:desired} };
  }
  if (key === 'approvalMode') {
    if (!['allowAll','promptUnmatched','onRequest','denyUnmatched'].includes(desired)) throw new Error('INVALID_APPROVAL_MODE');
    return { key, desired, method:'session/setApprovalMode', params:{...base,mode:desired} };
  }
  if (key === 'model') {
    if (!desired || typeof desired !== 'object' || Array.isArray(desired)
      || typeof desired.modelId !== 'string' || !desired.modelId.trim()) throw new Error('INVALID_MODEL_SELECTION');
    for (const name of Object.keys(desired)) if (!['modelId','providerId'].includes(name)) throw new Error(`UNSUPPORTED_MODEL_FIELD_${name}`);
    if (desired.providerId !== undefined && (typeof desired.providerId !== 'string' || !desired.providerId.trim())) throw new Error('INVALID_PROVIDER');
    return { key, desired, method:'session/setModel', params:{...base,model:{...desired}} };
  }
  throw new Error(`UNSUPPORTED_NATIVE_SETTING_${key}`);
}

export function modelMatches(session, selection) {
  // An omitted modelId selects the installed native default: it constrains
  // nothing before readback. After session/start the bridge fills the route
  // defaults from the readback, so later checks compare concrete values.
  if (selection?.modelId === undefined) return true;
  return session?.modelId === selection.modelId
    && (selection.providerId === undefined || selection.providerId === null || session.providerId === selection.providerId);
}

export function goalCommand(input, sessionId) {
  const action = input.action;
  if (!['set','edit','pause','resume','clear'].includes(action)) throw new Error('INVALID_GOAL_ACTION');
  const params = { sessionId };
  if (action === 'set' || action === 'edit') {
    if (typeof input.objective !== 'string' || !input.objective.trim()) throw new Error('GOAL_OBJECTIVE_REQUIRED');
    params.objective = input.objective;
  } else if (input.objective !== undefined) throw new Error('UNEXPECTED_GOAL_OBJECTIVE');
  return { method:`goal/${action}`, params, startsWork:['set','edit','resume'].includes(action) };
}

// Session-scoped native MCP servers for SDK 1.3.0 session/start config.
//
// The pinned facade omits config from StartSessionOptions until its
// sessionMcp grant check lands, so the bridge forwards this shape over the
// raw Connection only after a granted initialize (see bridge.mjs). The shape
// mirrors the installed schema closed SessionMcpServerConfig union: an
// undeclared transport arm is rejected here before it reaches native decode.
// Command and URL values are admitted verbatim but never logged; only server
// names travel to readback labels, and no default is invented for mode.
const MCP_STDIO_FIELDS = ['transport','command','args','env','framing','mode'];
const MCP_HTTP_FIELDS = ['transport','url','headers','mode'];
function mcpStringMap(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return null;
  for (const entry of Object.values(value)) if (typeof entry !== 'string') return null;
  return { ...value };
}
export function nativeMcpServers(value) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new Error('MCP_SERVERS_OBJECT_REQUIRED');
  const names = Object.keys(value);
  const admitted = {};
  for (const name of names) {
    const server = value[name];
    if (server === null || typeof server !== 'object' || Array.isArray(server)) throw new Error('MCP_SERVER_CONFIG_INVALID');
    const mode = server.mode;
    if (mode !== undefined && mode !== 'required' && mode !== 'optional') throw new Error('MCP_SERVER_MODE_INVALID');
    if (server.transport === 'stdio') {
      for (const field of Object.keys(server)) if (!MCP_STDIO_FIELDS.includes(field)) throw new Error('MCP_SERVER_FIELD_INVALID');
      if (typeof server.command !== 'string' || !server.command) throw new Error('MCP_SERVER_COMMAND_REQUIRED');
      if (server.args !== undefined && (!Array.isArray(server.args) || server.args.some(a => typeof a !== 'string'))) throw new Error('MCP_SERVER_ARGS_INVALID');
      const env = server.env === undefined ? undefined : mcpStringMap(server.env);
      if (server.env !== undefined && env === null) throw new Error('MCP_SERVER_ENV_INVALID');
      if (server.framing !== undefined && !['auto','contentLength','lineDelimitedJson'].includes(server.framing)) throw new Error('MCP_SERVER_FRAMING_INVALID');
      const entry = { transport:'stdio', command:server.command };
      if (server.args !== undefined) entry.args = [...server.args];
      if (env !== undefined) entry.env = env;
      if (server.framing !== undefined) entry.framing = server.framing;
      if (mode !== undefined) entry.mode = mode;
      Object.defineProperty(admitted,name,{value:entry,writable:true,enumerable:true,configurable:true});
    } else if (server.transport === 'streamableHttp') {
      for (const field of Object.keys(server)) if (!MCP_HTTP_FIELDS.includes(field)) throw new Error('MCP_SERVER_FIELD_INVALID');
      if (typeof server.url !== 'string' || !server.url) throw new Error('MCP_SERVER_URL_REQUIRED');
      const headers = server.headers === undefined ? undefined : mcpStringMap(server.headers);
      if (server.headers !== undefined && headers === null) throw new Error('MCP_SERVER_HEADERS_INVALID');
      const entry = { transport:'streamableHttp', url:server.url };
      if (headers !== undefined) entry.headers = headers;
      if (mode !== undefined) entry.mode = mode;
      Object.defineProperty(admitted,name,{value:entry,writable:true,enumerable:true,configurable:true});
    } else {
      throw new Error('MCP_SERVER_TRANSPORT_UNSUPPORTED');
    }
  }
  return admitted;
}

// Keep request construction, admission marking, and the response-derived ACK
// in one testable seam. Forwarding here does not establish server liveness.
export async function startNativeMcpSession(
  connection, params, commandOptions, mcpServers, state, markAdmissionPossible,
) {
  if (mcpServers !== undefined) {
    params.config = { mcpServers };
    state.mcp_servers_config_forwarded = Object.keys(mcpServers);
    state.mcp_servers_admission_ack = null;
  }
  markAdmissionPossible();
  const response = await connection.command('session/start', params, commandOptions);
  const sessionId = response?.session?.sessionId;
  if (mcpServers !== undefined && typeof sessionId === 'string' && sessionId)
    state.mcp_servers_admission_ack = { session_id: sessionId };
  return response;
}
