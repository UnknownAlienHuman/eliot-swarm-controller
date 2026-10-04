import { createHash } from "node:crypto";
import { Buffer } from "node:buffer";
import { readFileSync, realpathSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Effect } from "effect";
import { Service as McpService } from "@opencode/core/mcp/index";
import { Plugin, Rpc } from "@opencode/plugin/effect";

const PLUGIN_ID = "eliot.native-mcp-proof.v1";
const RPC_ID = PLUGIN_ID;
const SERVICE_VERSION = "2.0.7";
const MAX_ACTIVE_CHALLENGES = 8;
const MAX_CHALLENGE_TTL_MS = 120_000;
const MAX_TOOLS = 512;
const MAX_JSON_BYTES = 512 * 1024;
const MAX_DESCRIPTION_BYTES = 16 * 1024;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const SHA256_RE = /^[0-9a-f]{64}$/;

const MODULE_PATH = realpathSync(fileURLToPath(import.meta.url));
const MODULE_SHA256 = createHash("sha256").update(readFileSync(MODULE_PATH)).digest("hex");

const JsonObject = { type: "object", additionalProperties: true };
const Text = { type: "string", minLength: 1, maxLength: 512 };
const PositiveInteger = { type: "integer", minimum: 1 };
const ArmInput = {
  type: "object",
  required: [
    "challenge_id", "nonce", "service_id", "service_pid", "service_version",
    "module_sha256", "directory", "session_id", "model", "assignment", "issued_at_ms", "expires_at_ms",
  ],
  additionalProperties: false,
  properties: {
    challenge_id: { type: "string", pattern: UUID_RE.source },
    nonce: { type: "string", pattern: UUID_RE.source },
    service_id: Text,
    service_pid: PositiveInteger,
    service_version: { type: "string", const: SERVICE_VERSION },
    module_sha256: { type: "string", pattern: SHA256_RE.source },
    directory: Text,
    session_id: { type: "string", pattern: "^ses_[A-Za-z0-9_-]{1,252}$" },
    model: JsonObject,
    assignment: JsonObject,
    issued_at_ms: PositiveInteger,
    expires_at_ms: PositiveInteger,
  },
};
const ReadInput = {
  type: "object",
  required: ["challenge_id", "nonce"],
  additionalProperties: false,
  properties: {
    challenge_id: { type: "string", pattern: UUID_RE.source },
    nonce: { type: "string", pattern: UUID_RE.source },
  },
};
const AnyObject = { type: "object", additionalProperties: true };
const RpcDefinition = Rpc.define({
  id: RPC_ID,
  methods: {
    arm: { input: ArmInput, output: AnyObject },
    read: { input: ReadInput, output: AnyObject },
  },
  events: {},
});

function isRecord(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function onlyKeys(value, keys) {
  if (!isRecord(value)) return false;
  const allowed = new Set(keys);
  return Object.keys(value).every((key) => allowed.has(key));
}

function boundedText(value, limit = 512) {
  return typeof value === "string"
    && value.length > 0
    && value.length <= limit
    && !/[\u0000-\u001f\u007f]/u.test(value);
}

function jsonObject(value, limit = MAX_JSON_BYTES) {
  const encoded = JSON.stringify(value);
  if (typeof encoded !== "string" || Buffer.byteLength(encoded, "utf8") > limit) {
    throw new Error("schema_bound");
  }
  const copy = JSON.parse(encoded);
  if (!isRecord(copy)) throw new Error("schema_object_required");
  return copy;
}

function sameModel(left, right) {
  return isRecord(left)
    && left.id === right.id
    && left.providerID === right.providerID
    && left.variant === right.variant;
}

function validAssignment(scope, sessionID) {
  const keys = [
    "task_id", "task_revision", "attempt_id", "binding_id", "binding_generation",
    "native_session_id", "participant_id", "mcp_profile", "grant_revision",
    "participation_basis", "assignment_id", "review_assignment_id",
  ];
  if (!onlyKeys(scope, keys) || Object.keys(scope).length !== keys.length) return false;
  if (!["task_id", "attempt_id", "binding_id", "native_session_id", "participant_id"]
    .every((key) => boundedText(scope[key], 256))) return false;
  if (scope.native_session_id !== sessionID
      || !Number.isSafeInteger(scope.task_revision) || scope.task_revision < 1
      || !Number.isSafeInteger(scope.binding_generation) || scope.binding_generation < 1
      || !Number.isSafeInteger(scope.grant_revision) || scope.grant_revision < 1) return false;

  if (scope.participation_basis === "attempt_owner") {
    return scope.mcp_profile === "participant"
      && scope.assignment_id === null
      && scope.review_assignment_id === null;
  }
  if (scope.participation_basis === "producer_ref") {
    return scope.mcp_profile === "participant"
      && boundedText(scope.assignment_id, 128)
      && scope.review_assignment_id === null;
  }
  if (scope.participation_basis === "sponsored_reviewer") {
    return scope.mcp_profile === "assigned_reviewer"
      && scope.assignment_id === null
      && boundedText(scope.review_assignment_id, 128);
  }
  return false;
}

function validateArmInput(input, pluginOptions, location) {
  if (!onlyKeys(input, [
    "challenge_id", "nonce", "service_id", "service_pid", "service_version",
    "module_sha256", "directory", "session_id", "model", "assignment", "issued_at_ms", "expires_at_ms",
  ])) throw new Error("input_schema");
  const now = Date.now();
  const moduleSha = input.module_sha256;
  const normalizedExpected = path.resolve(String(pluginOptions.directory ?? location.directory));
  const normalizedLocation = path.resolve(location.directory);
  const sameDirectory = process.platform === "win32"
    ? normalizedExpected.toLowerCase() === normalizedLocation.toLowerCase()
    : normalizedExpected === normalizedLocation;
  if (!UUID_RE.test(input.challenge_id)
      || !UUID_RE.test(input.nonce)
      || !boundedText(input.service_id, 128)
      || !Number.isSafeInteger(input.service_pid) || input.service_pid !== process.pid
      || input.service_version !== SERVICE_VERSION
      || !SHA256_RE.test(moduleSha)
      || moduleSha !== MODULE_SHA256
      || pluginOptions.moduleSha256 !== MODULE_SHA256
      || pluginOptions.serviceId !== input.service_id
      || pluginOptions.serviceVersion !== SERVICE_VERSION
      || !boundedText(input.directory, 4096)
      || !sameDirectory
      || (process.platform === "win32"
        ? path.resolve(input.directory).toLowerCase() !== normalizedLocation.toLowerCase()
        : path.resolve(input.directory) !== normalizedLocation)
      || !boundedText(input.session_id, 256)
      || !isRecord(input.model)
      || !onlyKeys(input.model, ["id", "providerID", "variant"])
      || !["id", "providerID", "variant"].every((key) => boundedText(input.model[key], 256))
      || !validAssignment(input.assignment, input.session_id)
      || !Number.isSafeInteger(input.issued_at_ms)
      || !Number.isSafeInteger(input.expires_at_ms)
      || input.issued_at_ms <= 0
      || input.issued_at_ms > now
      || input.expires_at_ms <= input.issued_at_ms
      || input.expires_at_ms - input.issued_at_ms > MAX_CHALLENGE_TTL_MS
      || input.expires_at_ms <= now
      || input.expires_at_ms > now + MAX_CHALLENGE_TTL_MS) {
    throw new Error("scope_rejected");
  }
  return {
    challenge_id: input.challenge_id,
    nonce: input.nonce,
    service_id: input.service_id,
    service_pid: input.service_pid,
    service_version: input.service_version,
    module_sha256: moduleSha,
    module_path: MODULE_PATH,
    directory: input.directory,
    session_id: input.session_id,
    model: { id: input.model.id, providerID: input.model.providerID, variant: input.model.variant },
    assignment: jsonObject(input.assignment, 16 * 1024),
    issued_at_ms: input.issued_at_ms,
    expires_at_ms: input.expires_at_ms,
  };
}

function canonical(value) {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (!isRecord(value)) return JSON.stringify(value);
  return "{" + Object.keys(value).sort().map((key) =>
    JSON.stringify(key) + ":" + canonical(value[key])).join(",") + "}";
}

function sameChallenge(left, right) {
  return canonical(left) === canonical(right);
}

function pruneChallenges(states, now) {
  for (const [id, state] of states) {
    if (state.challenge.expires_at_ms <= now) states.delete(id);
  }
}

function schemaCopy(value) {
  return jsonObject(value, MAX_JSON_BYTES);
}

function normalizeNativeTools(items) {
  if (!Array.isArray(items) || items.length > MAX_TOOLS) throw new Error("tool_inventory_bound");
  const output = items.map((item) => {
    if (!isRecord(item)
        || !boundedText(item.server, 256)
        || !boundedText(item.name, 256)
        || (item.description !== undefined
          && (typeof item.description !== "string"
            || Buffer.byteLength(item.description, "utf8") > MAX_DESCRIPTION_BYTES))
        || !isRecord(item.inputSchema)) {
      throw new Error("tool_inventory_schema");
    }
    return {
      server: item.server,
      name: item.name,
      description: item.description ?? "",
      input_schema: schemaCopy(item.inputSchema),
      codemode: typeof item.codemode === "boolean" ? item.codemode : null,
    };
  });
  output.sort((a, b) => a.server < b.server ? -1 : a.server > b.server ? 1
    : a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  if (Buffer.byteLength(JSON.stringify(output), "utf8") > MAX_JSON_BYTES) {
    throw new Error("tool_inventory_bound");
  }
  return output;
}

function normalizeContextTools(tools) {
  if (!isRecord(tools)) throw new Error("context_tools_schema");
  const entries = Object.entries(tools);
  if (entries.length > MAX_TOOLS) throw new Error("context_tools_bound");
  const output = entries.map(([name, value]) => {
    if (!boundedText(name, 256)
        || !isRecord(value)
        || typeof value.description !== "string"
        || Buffer.byteLength(value.description, "utf8") > MAX_DESCRIPTION_BYTES
        || !isRecord(value.input)) throw new Error("context_tools_schema");
    return {
      name,
      description: value.description,
      input_schema: schemaCopy(value.input),
    };
  });
  output.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
  if (Buffer.byteLength(JSON.stringify(output), "utf8") > MAX_JSON_BYTES) {
    throw new Error("context_tools_bound");
  }
  return output;
}

function normalizeProviderTool(value) {
  if (!isRecord(value)) throw new Error("provider_tool_schema");
  let name;
  let description = "";
  let inputSchema;
  if (value.type === "function" && isRecord(value.function)) {
    name = value.function.name;
    description = value.function.description ?? "";
    inputSchema = value.function.parameters;
  } else if (Array.isArray(value.functionDeclarations)) {
    return value.functionDeclarations.map((item) => normalizeProviderTool(item)).flat();
  } else {
    name = value.name;
    description = value.description ?? "";
    inputSchema = value.input_schema ?? value.parameters;
  }
  if (!boundedText(name, 256)
      || typeof description !== "string"
      || Buffer.byteLength(description, "utf8") > MAX_DESCRIPTION_BYTES
      || !isRecord(inputSchema)) throw new Error("provider_tool_schema");
  return [{
    name,
    description,
    input_schema: schemaCopy(inputSchema),
  }];
}

function extractProviderTools(body) {
  if (!isRecord(body)) return { status: "unsupported", tools: [], reason_code: "request_not_object" };
  let raw = body.tools;
  if (!Array.isArray(raw) && Array.isArray(body.functionDeclarations)) {
    raw = [{ functionDeclarations: body.functionDeclarations }];
  }
  if (!Array.isArray(raw)) {
    return { status: "unsupported", tools: [], reason_code: "tools_field_unrecognized" };
  }
  if (raw.length > MAX_TOOLS) {
    return { status: "unsupported", tools: [], reason_code: "tool_count_limit" };
  }
  try {
    const tools = raw.flatMap(normalizeProviderTool);
    if (tools.length > MAX_TOOLS
        || Buffer.byteLength(JSON.stringify(tools), "utf8") > MAX_JSON_BYTES) {
      return { status: "unsupported", tools: [], reason_code: "tool_schema_limit" };
    }
    tools.sort((a, b) => a.name < b.name ? -1 : a.name > b.name ? 1 : 0);
    return { status: "observed", tools, reason_code: null };
  } catch {
    return { status: "unsupported", tools: [], reason_code: "tool_schema_unrecognized" };
  }
}

function matchingState(states, event) {
  const sessionID = event?.sessionID;
  const model = event?.model;
  const agent = event?.agent;
  if (!boundedText(sessionID, 256)
      || !boundedText(agent, 256)
      || !isRecord(model)) return null;
  for (const state of states.values()) {
    if (state.challenge.expires_at_ms > Date.now()
        && state.challenge.session_id === sessionID
        && sameModel(state.challenge.model, model)
        && (state.agent === null || state.agent === agent)) {
      return { state, agent };
    }
  }
  return null;
}

function captureContext(states, event) {
  const match = matchingState(states, event);
  if (!match || match.state.context !== null) return;
  try {
    const tools = normalizeContextTools(event.tools);
    match.state.agent = match.agent;
    match.state.context = {
      status: "observed",
      stage: "session_context_hook",
      observed_at_ms: Date.now(),
      agent: match.agent,
      tools,
    };
    match.state.observation_sequence += 1;
  } catch {
    match.state.agent = match.agent;
    match.state.context = {
      status: "unsupported",
      stage: "session_context_hook",
      observed_at_ms: Date.now(),
      agent: match.agent,
      tools: [],
      reason_code: "context_schema_unrecognized",
    };
    match.state.observation_sequence += 1;
  }
}

function captureProvider(states, event, transport, parsed) {
  const match = matchingState(states, event);
  if (!match || match.state.provider_request !== null || event.kind !== "primary") return;
  match.state.agent = match.agent;
  match.state.provider_request = {
    status: parsed.status,
    transport,
    stage: "before_transport",
    kind: event.kind,
    observed_at_ms: Date.now(),
    agent: match.agent,
    model: {
      id: event.model.id,
      providerID: event.model.providerID,
      variant: event.model.variant,
    },
    tools: parsed.tools,
    reason_code: parsed.reason_code,
  };
  match.state.observation_sequence += 1;
}

async function readRequestTextBounded(request) {
  const clone = request.clone();
  if (!clone.body) return "";
  const reader = clone.body.getReader();
  const chunks = [];
  let bytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > MAX_JSON_BYTES) {
        await reader.cancel();
        return null;
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const joined = new Uint8Array(bytes);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder("utf-8", { fatal: true }).decode(joined);
}

function readback(state, options) {
  const context = state.context ?? {
    status: "unknown",
    stage: null,
    observed_at_ms: null,
    agent: null,
    tools: [],
    reason_code: null,
  };
  const provider = state.provider_request ?? {
    status: "unknown",
    transport: null,
    stage: null,
    kind: null,
    observed_at_ms: null,
    agent: null,
    model: null,
    tools: [],
    reason_code: null,
  };
  return {
    plugin_id: PLUGIN_ID,
    module_path: MODULE_PATH,
    module_sha256: MODULE_SHA256,
    service_id: options.serviceId,
    service_pid: process.pid,
    service_version: SERVICE_VERSION,
    directory: state.challenge.directory,
    challenge_id: state.challenge.challenge_id,
    nonce: state.challenge.nonce,
    issued_at_ms: state.challenge.issued_at_ms,
    expires_at_ms: state.challenge.expires_at_ms,
    assignment: state.challenge.assignment,
    expected_model: state.challenge.model,
    session_id: state.challenge.session_id,
    observation_sequence: state.observation_sequence,
    receipt_observed_at_ms: Date.now(),
    native_discovered: {
      status: "observed",
      observed_at_ms: state.native_observed_at_ms,
      tools: state.native_tools,
    },
    session_context: context,
    provider_request: provider,
    model_consumed: "unknown",
  };
}

export default Plugin.define({
  id: PLUGIN_ID,
  effect(context) {
    const states = new Map();
    return Effect.gen(function* () {
      yield* context.session.hook("context", (event) =>
        Effect.sync(() => captureContext(states, event)));

      yield* context.session.hook("http.request", (event) =>
        Effect.promise(async () => {
          const candidate = matchingState(states, event);
          if (!candidate || candidate.state.provider_request !== null || event.kind !== "primary") return;
          try {
            const text = await readRequestTextBounded(event.request);
            if (text === null) {
              captureProvider(states, event, "http", {
                status: "unsupported",
                tools: [],
                reason_code: "request_body_limit",
              });
              return;
            }
            let body;
            try {
              body = JSON.parse(text);
            } catch {
              captureProvider(states, event, "http", {
                status: "unsupported",
                tools: [],
                reason_code: "request_json_invalid",
              });
              return;
            }
            captureProvider(states, event, "http", extractProviderTools(body));
          } catch {
            captureProvider(states, event, "http", {
              status: "unsupported",
              tools: [],
              reason_code: "request_unreadable",
            });
          }
        }));

      yield* context.session.hook("experimental.ws.send", (event) =>
        Effect.sync(() => {
          const candidate = matchingState(states, event);
          if (!candidate || candidate.state.provider_request !== null || event.kind !== "primary") return;
          if (typeof event.frame !== "string"
              || Buffer.byteLength(event.frame, "utf8") > MAX_JSON_BYTES) {
            captureProvider(states, event, "websocket", {
              status: "unsupported",
              tools: [],
              reason_code: "frame_limit",
            });
            return;
          }
          let frame;
          try {
            frame = JSON.parse(event.frame);
          } catch {
            captureProvider(states, event, "websocket", {
              status: "unsupported",
              tools: [],
              reason_code: "frame_json_invalid",
            });
            return;
          }
          const parsed = extractProviderTools(frame);
          if (parsed.status === "unsupported" && parsed.reason_code === "tools_field_unrecognized") return;
          captureProvider(states, event, "websocket", parsed);
        }));

      yield* context.rpc.register(RpcDefinition, {
        arm: (input) => Effect.gen(function* () {
          const now = Date.now();
          pruneChallenges(states, now);
          const challenge = validateArmInput(input, context.options, context.location);
          const previous = states.get(challenge.challenge_id);
          if (previous) {
            if (!sameChallenge(previous.challenge, challenge)) throw new Error("challenge_conflict");
            return {
              accepted: true,
              challenge_id: challenge.challenge_id,
              nonce: challenge.nonce,
              module_sha256: MODULE_SHA256,
              service_id: challenge.service_id,
              service_pid: process.pid,
              service_version: SERVICE_VERSION,
            };
          }
          for (const current of states.values()) {
            if (current.challenge.session_id === challenge.session_id) {
              throw new Error("session_challenge_active");
            }
          }
          if (states.size >= MAX_ACTIVE_CHALLENGES) throw new Error("challenge_capacity");

          // RPC handlers run in the request location context, which provides MCP.Service.
          const mcp = yield* McpService;
          const nativeTools = normalizeNativeTools(yield* mcp.tools());
          const state = {
            challenge,
            native_tools: nativeTools,
            native_observed_at_ms: Date.now(),
            context: null,
            provider_request: null,
            agent: null,
            observation_sequence: 0,
          };
          states.set(challenge.challenge_id, state);
          return {
            accepted: true,
            challenge_id: challenge.challenge_id,
            nonce: challenge.nonce,
            module_sha256: MODULE_SHA256,
            service_id: challenge.service_id,
            service_pid: process.pid,
            service_version: SERVICE_VERSION,
          };
        }),
        read: (input) => Effect.sync(() => {
          if (!onlyKeys(input, ["challenge_id", "nonce"])
              || !UUID_RE.test(input.challenge_id)
              || !UUID_RE.test(input.nonce)) throw new Error("read_schema");
          pruneChallenges(states, Date.now());
          const state = states.get(input.challenge_id);
          if (!state
              || state.challenge.nonce !== input.nonce
              || state.challenge.expires_at_ms <= Date.now()) {
            throw new Error("challenge_unavailable");
          }
          return readback(state, context.options);
        }),
      });
    });
  },
});
