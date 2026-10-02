#!/usr/bin/env node
// OpenCodex provider-service adapter: attach to an explicitly
// configured, already-running OpenCodex service, observe it through
// read-only Management API reads, and — slice 2 — execute single,
// operator-requested configuration changes as saved Operations.
// OpenCodex is a provider/protocol proxy, not a session owner:
// execution stays with the native Codex backend (C07); this module
// observes the service and applies the configuration changes the
// operator explicitly selects, nothing else.
//
// Boundaries (issue #1, module contract, audit):
// - Reads never start, restart, install or reconfigure the service.
//   Mutations happen only inside `configure`, only for the one change
//   named by the operator's request file, and only through the documented
//   Management API writers. No request is ever sent to the data plane.
// - Client-integration changes (apply/disable/overwrite/restore,
//   including single Aside profiles) are applied ONLY through the
//   documented preview + planFingerprint flow: `preview` returns the
//   upstream plan, `configure` requires the operator's confirmed
//   operation + planFingerprint (both-or-neither, enforced locally
//   before any mutation is sent), and a 409 integration_preview_stale
//   is returned with the fresh plan for a new operator decision —
//   never retried blindly. The Aside bulk PUT is never sent: upstream
//   refuses plan bindings for it ("a confirmed plan applies to one
//   profile"), so it cannot meet the issue's confirmation rule.
// - Partial results are parsed per element; `saved` is never read as
//   `applied` — every configure finishes with readback GETs, and an
//   unknown (lost) mutation outcome is reconciled by reading, never
//   by re-sending the mutation.
// - Forbidden by the issue and absent here: global PATH/config edits
//   by ELIOT itself, automatic shim restore, compaction changes,
//   account-pool rotation, ELIOT-side fallback chains (upstream's own
//   stored subagent-model fallback chain is upstream configuration,
//   set only when the operator selects that Operation), sidecars.
// - shutdown is detach only: the client is dropped and the token
//   reference forgotten. An externally owned shared proxy is NEVER
//   stopped, restarted or signalled by this adapter.
// - The Management (admin) credential is a separate credential from any
//   data-plane/proxy admission key and from the Codex app-server token.
//   It is referenced by environment-variable name only, read at attach
//   time, held in memory only, sent only as X-OpenCodex-API-Key, and
//   never passed to Codex/model tools, never persisted, never printed.
// - Failed or absent observation is `unknown`, never an empty healthy
//   fleet. The service version is operator-managed and is NOT pinned
//   by this project: the operator runs and updates the service himself
//   at upstream current, and this adapter's contract baseline follows
//   upstream current (UPDATE.md). An observed version that differs
//   from the baseline is recorded as a fact — `observedVersion` vs
//   `expectedVersion`, `versionComparison` — and nothing more: it is
//   not a failure, not a pass, never a gate; it neither degrades
//   readiness nor blocks a mutation.
//   activeTurnCount is forwarded as observed (0 is not proof that
//   native Codex children stopped).
// - send/reply stay honestly unavailable: execution and replies
//   belong to the native Codex backend (C07).
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  billingFor, compareVersion, findModelRowSettings, isObject,
  mapHttpError, normalizeAsideProfileState, normalizeAsideProfiles,
  normalizeEffortCaps, normalizeHealth, normalizeInjectionModel,
  normalizeIntegrationJournal, normalizeIntegrationOutcome,
  normalizeIntegrationOutcomes, normalizeIntegrationPlan,
  normalizeIntegrationState, normalizeIntegrationStates, normalizeMemory,
  normalizeModelSettingsReceipt, normalizeModels, normalizeProtocol,
  normalizeProtocolSettings, normalizeProviders, normalizeSubagentFallback,
  normalizeSubagentModels, normalizeUsage, normalizeV2Receipt, normalizeV2State,
  redactValue, str, stringList, unknownSection,
} from './control.mjs';

export const MODULE_ARTIFACT_ID = 'opencodex-2.75.0-bridge.3';
export const ENTRYPOINT = 'opencodex_management_api';
export const UPSTREAM = {
  repo: 'lidge-jun/opencodex',
  release: 'v2.75.0',
  commit: 'ef0297f86c4540c7d757c8595170d66f9c584aec',
  license: 'MIT',
};
const REQUEST_TIMEOUT_MS = 5000;
const MAX_PROTOCOL_PROVIDERS = 16;
const MAX_MODEL_ROWS = 256;
const CAPABILITIES = {
  describe: 'implemented',
  open: 'implemented',
  attach: 'implemented',
  snapshot: 'implemented',
  send: 'unavailable',
  configure: 'implemented',
  preview: 'implemented',
  reply: 'unavailable',
  shutdown: 'detach_only',
};
// Saved-Operation contract vocabulary (module contract §4; the host
// stores this block in effective_request_json). One configure call is
// one change, one Operation; a lost mutation response is reconciled
// by readback only, never replayed.
const CONTRACT_REVISION = 1;
function operationContract(effectScope, applicationBoundary) {
  return {
    effect_scope: effectScope,
    order_scope: 'single_change',
    completion_condition: 'native_configuration_applied',
    replay_policy: 'readback_only_no_mutation_replay',
    fallback_used: false,
    contract_revision: CONTRACT_REVISION,
    application_boundary: applicationBoundary,
  };
}

function configError(code) {
  const error = new Error(code);
  error.code = code;
  return error;
}

export async function loadConfig(configPath) {
  const raw = JSON.parse(await readFile(configPath, 'utf8'));
  return validateConfig(raw);
}

export function validateConfig(raw) {
  if (typeof raw?.endpoint !== 'string' || !/^https?:\/\//.test(raw.endpoint)) {
    throw configError('CONFIG_ENDPOINT_REQUIRED');
  }
  if (raw.moduleArtifactId !== MODULE_ARTIFACT_ID) throw configError('CONFIG_ARTIFACT_MISMATCH');
  if (!str(raw.expectedVersion)) throw configError('CONFIG_EXPECTED_VERSION_REQUIRED');
  if (!str(raw.adminTokenEnv)) throw configError('CONFIG_ADMIN_TOKEN_ENV_REQUIRED');
  // Endpoint, expected version, credential reference and lifecycle owner
  // are four separate facts. Only an externally owned service is
  // accepted; a binding that claims ELIOT owns the lifecycle is refused.
  const lifecycleOwner = raw.lifecycleOwner ?? 'external';
  if (lifecycleOwner !== 'external') throw configError('CONFIG_LIFECYCLE_OWNER_NOT_EXTERNAL');
  return {
    endpoint: raw.endpoint.replace(/\/+$/, ''),
    moduleArtifactId: raw.moduleArtifactId,
    expectedVersion: raw.expectedVersion,
    adminTokenEnv: raw.adminTokenEnv,
    lifecycleOwner,
  };
}

// ---------------------------------------------------------------------------
// Configure request validation. One request file names exactly one
// operator-selected change. Validation is strict (unknown keys are
// refused, never silently dropped) and mirrors upstream's own request
// rules where upstream states them, so a rejected request is refused
// locally with the reason recorded and no mutation is sent.

const ROLLOUT_KEYS = ['nativeChatCombos', 'managedMessagesNative', 'managedMessagesNativeOAuth', 'directEncoders', 'shadowPlan'];
const MODALITIES = ['text', 'image', 'audio'];
const V2_KEYS = ['enabled', 'maxConcurrentThreadsPerSession', 'multiAgentMode', 'keepNativeChatGptOnV1',
  'agentsEnabled', 'agentsMaxDepth', 'subagentDeveloperInstructions', 'multiAgentModeHintText',
  'multiAgentSurfaceAdvisoryAcknowledged'];

function rejected(reason) {
  return { ok: false, reason };
}
function accepted(request) {
  return { ok: true, request };
}
function unknownKeys(raw, allowed) {
  return Object.keys(raw).filter((key) => !allowed.includes(key));
}
function isInt(value) {
  return typeof value === 'number' && Number.isSafeInteger(value);
}
function modelList(value, { allowEmpty = false } = {}) {
  if (!Array.isArray(value)) return null;
  if (!allowEmpty && value.length === 0) return null;
  const out = [];
  for (const entry of value) {
    if (typeof entry !== 'string' || entry.trim().length === 0) return null;
    out.push(entry);
  }
  return out;
}

export function validateRequest(raw, mode) {
  if (!isObject(raw)) return rejected('request_required');
  const kind = str(raw.kind);
  const kinds = ['protocol_settings', 'model_settings', 'subagent_v2', 'injection_model',
    'effort_caps', 'subagent_models', 'subagent_model_fallback',
    'client_integration', 'client_integration_restore', 'aside_profile', 'aside_profile_restore'];
  if (!kind || !kinds.includes(kind)) return rejected('unknown_kind');
  const integrationKind = kind.startsWith('client_integration') || kind.startsWith('aside_profile');
  if (mode === 'preview' && integrationKind && (raw.operation !== undefined || raw.planFingerprint !== undefined)) {
    return rejected('plan_binding_in_preview_request');
  }
  if (mode === 'configure' && integrationKind) {
    // Both-or-neither, enforced before any request is sent: a supplied
    // confirmation must never be dropped on the floor by this client.
    if ((raw.operation === undefined) !== (raw.planFingerprint === undefined)) {
      return rejected('plan_binding_both_or_neither');
    }
    if (raw.operation === undefined) return rejected('plan_binding_required');
    if (!str(raw.planFingerprint)) return rejected('plan_fingerprint_required');
  }
  const binding = integrationKind && mode === 'configure'
    ? { operation: raw.operation, planFingerprint: raw.planFingerprint }
    : null;

  if (kind === 'protocol_settings') {
    if (unknownKeys(raw, ['kind', 'patch']).length) return rejected('unknown_request_field');
    const patch = raw.patch;
    if (!isObject(patch) || unknownKeys(patch, ['messagesEnabled', 'unrepresentable', 'rollout']).length) {
      return rejected('invalid_patch');
    }
    let leaves = 0;
    if (patch.messagesEnabled !== undefined) {
      if (typeof patch.messagesEnabled !== 'boolean') return rejected('invalid_messages_enabled');
      leaves += 1;
    }
    if (patch.unrepresentable !== undefined) {
      if (patch.unrepresentable !== 'legacy' && patch.unrepresentable !== 'reject') {
        return rejected('invalid_unrepresentable');
      }
      leaves += 1;
    }
    if (patch.rollout !== undefined) {
      if (!isObject(patch.rollout) || unknownKeys(patch.rollout, ROLLOUT_KEYS).length) {
        return rejected('invalid_rollout');
      }
      for (const value of Object.values(patch.rollout)) {
        if (typeof value !== 'boolean') return rejected('invalid_rollout');
        leaves += 1;
      }
    }
    if (leaves === 0) return rejected('empty_patch');
    return accepted({ kind, patch });
  }

  if (kind === 'model_settings') {
    const axes = ['contextWindow', 'inputModalities', 'reasoningEfforts', 'defaultReasoningEffort'];
    if (unknownKeys(raw, ['kind', 'provider', 'modelId', ...axes]).length) return rejected('unknown_request_field');
    const provider = str(raw.provider);
    const modelId = str(raw.modelId);
    if (!provider || !modelId) return rejected('provider_and_model_required');
    // Upstream addresses per-model settings only on routed providers:
    // "openai" is the native passthrough lane, "combo" synthetic.
    if (provider === 'openai' || provider === 'combo') return rejected('provider_not_routed');
    const request = { kind, provider, modelId };
    let present = 0;
    if (raw.contextWindow !== undefined) {
      if (raw.contextWindow !== null && !(isInt(raw.contextWindow) && raw.contextWindow > 0)) {
        return rejected('invalid_context_window');
      }
      request.contextWindow = raw.contextWindow;
      present += 1;
    }
    if (raw.inputModalities !== undefined) {
      if (raw.inputModalities !== null) {
        if (!Array.isArray(raw.inputModalities)
          || raw.inputModalities.some((entry) => !MODALITIES.includes(entry))) {
          return rejected('invalid_input_modalities');
        }
        request.inputModalities = [...new Set(raw.inputModalities)];
      } else request.inputModalities = null;
      present += 1;
    }
    if (raw.reasoningEfforts !== undefined) {
      if (raw.reasoningEfforts !== null) {
        const ladder = stringList(raw.reasoningEfforts, 16);
        if (!ladder || ladder.length !== raw.reasoningEfforts.length) return rejected('invalid_reasoning_efforts');
        // An empty array is upstream's explicit "no rungs" override, NOT
        // a clear; it is preserved as-is.
        request.reasoningEfforts = raw.reasoningEfforts.slice();
      } else request.reasoningEfforts = null;
      present += 1;
    }
    if (raw.defaultReasoningEffort !== undefined) {
      if (raw.defaultReasoningEffort !== null && !str(raw.defaultReasoningEffort)) {
        return rejected('invalid_default_reasoning_effort');
      }
      request.defaultReasoningEffort = raw.defaultReasoningEffort;
      present += 1;
    }
    if (present === 0) return rejected('no_settings_axes');
    return accepted(request);
  }

  if (kind === 'subagent_v2') {
    if (unknownKeys(raw, ['kind', 'settings']).length) return rejected('unknown_request_field');
    const settings = raw.settings;
    if (!isObject(settings) || unknownKeys(settings, V2_KEYS).length) return rejected('invalid_settings');
    if (Object.keys(settings).length === 0) return rejected('empty_settings');
    if (settings.enabled !== undefined && typeof settings.enabled !== 'boolean') return rejected('invalid_enabled');
    if (settings.maxConcurrentThreadsPerSession !== undefined
      && !(isInt(settings.maxConcurrentThreadsPerSession) && settings.maxConcurrentThreadsPerSession >= 1)) {
      return rejected('invalid_max_concurrent_threads');
    }
    if (settings.multiAgentMode !== undefined
      && !['v1', 'default', 'v2'].includes(settings.multiAgentMode)) return rejected('invalid_multi_agent_mode');
    if (settings.keepNativeChatGptOnV1 !== undefined && typeof settings.keepNativeChatGptOnV1 !== 'boolean') {
      return rejected('invalid_keep_native');
    }
    if (settings.agentsEnabled !== undefined && settings.agentsEnabled !== null
      && typeof settings.agentsEnabled !== 'boolean') return rejected('invalid_agents_enabled');
    if (settings.agentsMaxDepth !== undefined && settings.agentsMaxDepth !== null
      && !(isInt(settings.agentsMaxDepth)
        && settings.agentsMaxDepth >= -2147483648 && settings.agentsMaxDepth <= 2147483647)) {
      return rejected('invalid_agents_max_depth');
    }
    if (settings.subagentDeveloperInstructions !== undefined
      && settings.subagentDeveloperInstructions !== null
      && typeof settings.subagentDeveloperInstructions !== 'string') {
      return rejected('invalid_subagent_instructions');
    }
    if (settings.multiAgentModeHintText !== undefined && settings.multiAgentModeHintText !== null
      && (typeof settings.multiAgentModeHintText !== 'string'
        || settings.multiAgentModeHintText.trim().length === 0)) {
      return rejected('invalid_mode_hint_text');
    }
    if (settings.multiAgentSurfaceAdvisoryAcknowledged !== undefined
      && typeof settings.multiAgentSurfaceAdvisoryAcknowledged !== 'boolean') {
      return rejected('invalid_advisory_ack');
    }
    return accepted({ kind, settings });
  }

  if (kind === 'injection_model') {
    const keys = ['multiAgentGuidanceEnabled', 'syncCodexSubagentDefaults', 'model', 'effort', 'prompt'];
    if (unknownKeys(raw, ['kind', 'settings']).length) return rejected('unknown_request_field');
    const settings = raw.settings;
    if (!isObject(settings) || unknownKeys(settings, keys).length) return rejected('invalid_settings');
    if (Object.keys(settings).length === 0) return rejected('empty_settings');
    for (const flag of ['multiAgentGuidanceEnabled', 'syncCodexSubagentDefaults']) {
      if (settings[flag] !== undefined && typeof settings[flag] !== 'boolean') return rejected(`invalid_${flag}`);
    }
    if (settings.model !== undefined && settings.model !== null && !str(settings.model)) {
      return rejected('invalid_model');
    }
    if (settings.effort !== undefined && settings.effort !== null && !str(settings.effort)) {
      return rejected('invalid_effort');
    }
    if (settings.prompt !== undefined && settings.prompt !== null && typeof settings.prompt !== 'string') {
      return rejected('invalid_prompt');
    }
    return accepted({ kind, settings });
  }

  if (kind === 'effort_caps') {
    if (unknownKeys(raw, ['kind', 'effortCap', 'subagentEffortCap', 'modelPinnedEfforts']).length) {
      return rejected('unknown_request_field');
    }
    // Deliberate slice boundary: the per-model pin map is not written
    // by this Operation; the refusal is recorded, never silent.
    if (raw.modelPinnedEfforts !== undefined) return rejected('model_pinned_efforts_not_in_slice');
    const request = { kind };
    let present = 0;
    for (const key of ['effortCap', 'subagentEffortCap']) {
      if (raw[key] !== undefined) {
        if (raw[key] !== null && !str(raw[key])) return rejected('invalid_effort_cap');
        request[key] = raw[key];
        present += 1;
      }
    }
    if (present === 0) return rejected('no_caps');
    return accepted(request);
  }

  if (kind === 'subagent_models') {
    if (unknownKeys(raw, ['kind', 'models', 'pickerOrder', 'pickerOrderMode']).length) {
      return rejected('unknown_request_field');
    }
    // Picker order is a separate Models-page concern, not written by
    // this roster Operation.
    if (raw.pickerOrder !== undefined || raw.pickerOrderMode !== undefined) {
      return rejected('picker_order_not_in_slice');
    }
    const models = modelList(raw.models);
    if (!models) return rejected('invalid_models');
    // Upstream truncates a longer roster to five SILENTLY; this client
    // refuses instead of letting a deliberate roster lose entries.
    if (models.length > 5) return rejected('roster_over_limit');
    return accepted({ kind, models });
  }

  if (kind === 'subagent_model_fallback') {
    if (unknownKeys(raw, ['kind', 'models', 'pollMs']).length) return rejected('unknown_request_field');
    const request = { kind };
    let present = 0;
    if (raw.models !== undefined) {
      const models = modelList(raw.models, { allowEmpty: true });
      if (!models) return rejected('invalid_models');
      request.models = models;
      present += 1;
    }
    if (raw.pollMs !== undefined) {
      if (raw.pollMs !== null && !(isInt(raw.pollMs) && raw.pollMs >= 5000 && raw.pollMs <= 600000)) {
        return rejected('invalid_poll_ms');
      }
      request.pollMs = raw.pollMs;
      present += 1;
    }
    if (present === 0) return rejected('empty_request');
    return accepted(request);
  }

  // Integration kinds.
  const integrationKeys = ['kind', 'clientId', 'profileId', 'enabled', 'overwriteConflict',
    'opId', 'confirmDrift', 'operation', 'planFingerprint'];
  if (unknownKeys(raw, integrationKeys).length) return rejected('unknown_request_field');
  const confirmDrift = raw.confirmDrift === undefined ? false : raw.confirmDrift;
  if (typeof confirmDrift !== 'boolean') return rejected('invalid_confirm_drift');

  if (kind === 'client_integration' || kind === 'aside_profile') {
    const request = { kind, binding };
    if (kind === 'client_integration') {
      const clientId = str(raw.clientId);
      if (!clientId) return rejected('client_id_required');
      // Aside is a set of profiles; the unscoped routes refuse it and
      // the canonical per-profile kind exists below.
      if (clientId === 'aside') return rejected('use_aside_profile_kind');
      request.clientId = clientId;
    } else {
      if (!isInt(raw.profileId) || raw.profileId < 0) return rejected('profile_id_required');
      request.profileId = raw.profileId;
    }
    if (typeof raw.enabled !== 'boolean') return rejected('enabled_required');
    if (raw.overwriteConflict !== undefined && typeof raw.overwriteConflict !== 'boolean') {
      return rejected('invalid_overwrite_conflict');
    }
    if (raw.overwriteConflict === true && raw.enabled === false) return rejected('invalid_overwrite_conflict');
    request.enabled = raw.enabled;
    request.overwriteConflict = raw.overwriteConflict === true;
    request.operation = request.enabled
      ? (request.overwriteConflict ? 'overwrite' : 'apply')
      : 'disable';
    if (binding && binding.operation !== request.operation) return rejected('plan_operation_mismatch');
    return accepted(request);
  }

  // Restore kinds.
  const request = { kind, binding, confirmDrift };
  if (kind === 'client_integration_restore') {
    const clientId = str(raw.clientId);
    if (!clientId) return rejected('client_id_required');
    if (clientId === 'aside') return rejected('use_aside_profile_kind');
    request.clientId = clientId;
  } else {
    if (!isInt(raw.profileId) || raw.profileId < 0) return rejected('profile_id_required');
    request.profileId = raw.profileId;
  }
  if (!str(raw.opId)) return rejected('op_id_required');
  request.opId = raw.opId;
  request.operation = 'restore';
  if (binding && binding.operation !== 'restore') return rejected('plan_operation_mismatch');
  return accepted(request);
}

export function createAdapter(config, env = process.env) {
  let token = null;
  let identity = null;

  function resolveToken() {
    const value = env[config.adminTokenEnv];
    return typeof value === 'string' && value.length > 0 ? value : null;
  }

  // The read helper of this module (slice 1, unchanged semantics).
  async function get(pathname) {
    let response;
    try {
      response = await fetch(`${config.endpoint}${pathname}`, {
        method: 'GET',
        headers: { 'X-OpenCodex-API-Key': token, Accept: 'application/json' },
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (error) {
      const reason = error?.name === 'TimeoutError' ? 'read_timeout' : 'service_unreachable';
      return { ok: false, status: null, code: null, reason, body: null };
    }
    let body = null;
    try {
      body = await response.json();
    } catch {
      return { ok: false, status: response.status, code: null, reason: 'malformed_response', body: null };
    }
    if (!response.ok) {
      const { reason, code } = mapHttpError(response.status, body);
      return { ok: false, status: response.status, code, reason, body: null };
    }
    return { ok: true, status: response.status, code: null, reason: null, body };
  }

  // The single mutation helper. It exists only for `configure`, takes
  // an explicit method + body from the per-kind executors below, and
  // distinguishes a responded request from a LOST one (network error
  // or timeout): a lost mutation outcome is unknown and is reconciled
  // by reading — this helper is never called twice for one change.
  async function send(method, pathname, body) {
    let response;
    try {
      response = await fetch(`${config.endpoint}${pathname}`, {
        method,
        headers: {
          'X-OpenCodex-API-Key': token,
          Accept: 'application/json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
    } catch (error) {
      const reason = error?.name === 'TimeoutError' ? 'mutation_timeout' : 'mutation_response_lost';
      return { responded: false, status: null, code: null, reason, body: null };
    }
    let parsed = null;
    try {
      parsed = await response.json();
    } catch {
      // A non-JSON mutation response leaves the outcome unproven either
      // way; the status is real evidence, so keep it and let the
      // executor's readback decide what can be claimed.
      return { responded: true, status: response.status, code: null, reason: 'malformed_response', body: null };
    }
    return { responded: true, status: response.status, code: null, reason: null, body: parsed };
  }

  function evidenceFor(read, result) {
    const entry = { read, state: result.ok ? 'observed' : 'unknown' };
    if (result.status !== null) entry.status = result.status;
    if (result.code) entry.code = result.code;
    if (result.reason) entry.reason = result.reason;
    return entry;
  }

  function mutationEvidenceFor(label, result) {
    const entry = { read: label, state: result.responded ? 'responded' : 'unknown' };
    if (result.status !== null) entry.status = result.status;
    if (result.reason) entry.reason = result.reason;
    return entry;
  }

  // Map a responded mutation failure to this module's reason vocabulary.
  // The upstream `code` is preserved as its own evidence field.
  function mutationFailure(result) {
    const code = isObject(result.body) ? str(result.body.code) : null;
    let reason;
    if (result.status === 400) reason = code ?? 'invalid_request';
    else if (result.status === 401) reason = 'admin_auth_rejected';
    else if (result.status === 403) reason = 'origin_blocked';
    else if (result.status === 404) reason = code ?? 'not_found';
    else if (result.status === 409) reason = code ?? 'conflict';
    else if (result.status === 410) reason = code ?? 'gone';
    else if (result.status === 413) reason = 'body_too_large';
    else if (result.status === 500) reason = code ?? 'write_failed';
    else if (result.status === 502) reason = code ?? 'upstream_write_failed';
    else if (result.status === 503) reason = code ?? 'management_unavailable';
    else reason = `http_${result.status}`;
    return { reason, code };
  }

  async function attach() {
    token = resolveToken();
    if (!token) {
      identity = null;
      return {
        state: 'unknown',
        reason: 'admin_token_env_missing',
        identity: null,
        expectedVersion: config.expectedVersion,
        observedVersion: null,
        versionComparison: 'unknown',
        readiness: 'unknown',
        evidence: [{ read: 'credentials', state: 'unknown', reason: 'admin_token_env_missing' }],
      };
    }
    const evidence = [];
    const healthResult = await get('/api/system/health');
    evidence.push(evidenceFor('/api/system/health', healthResult));
    if (!healthResult.ok) {
      identity = null;
      return {
        state: 'unknown',
        reason: healthResult.reason,
        status: healthResult.status,
        code: healthResult.code,
        identity: null,
        expectedVersion: config.expectedVersion,
        observedVersion: null,
        versionComparison: 'unknown',
        readiness: 'unknown',
        evidence,
      };
    }
    const health = normalizeHealth(healthResult.body);
    if (!health || !health.version || health.pid === null) {
      identity = null;
      return {
        state: 'unknown',
        reason: 'health_schema',
        identity: null,
        expectedVersion: config.expectedVersion,
        observedVersion: health?.version ?? null,
        versionComparison: 'unknown',
        readiness: 'unknown',
        evidence,
      };
    }
    // Identity is endpoint + observed pid + observed version. The pid is
    // an observation, never an identity to act on (no signalling, ever).
    identity = { endpoint: config.endpoint, pid: health.pid, version: health.version };
    const versionComparison = compareVersion(health.version, config.expectedVersion);
    const memoryResult = await get('/api/system/memory');
    evidence.push(evidenceFor('/api/system/memory', memoryResult));
    const memory = memoryResult.ok ? normalizeMemory(memoryResult.body) : null;
    return {
      state: 'attached',
      identity,
      expectedVersion: config.expectedVersion,
      observedVersion: health.version,
      versionComparison,
      readiness: memory ? 'observed' : 'unknown',
      health,
      memory: memory ?? unknownSection(memoryResult.reason ?? 'memory_schema', {
        status: memoryResult.status, code: memoryResult.code,
      }),
      evidence,
    };
  }

  // Every configuration read, each subsection degrading independently
  // to unknown. These are reads only; a snapshot never mutates.
  async function configurationSection(evidence) {
    const section = {};
    const protocolsResult = await get('/api/protocols');
    evidence.push(evidenceFor('/api/protocols', protocolsResult));
    const protocols = protocolsResult.ok ? normalizeProtocolSettings(protocolsResult.body) : null;
    section.protocols = protocols ?? unknownSection(protocolsResult.reason ?? 'protocols_schema', {
      status: protocolsResult.status, code: protocolsResult.code,
    });

    const integrationsResult = await get('/api/client-integrations');
    evidence.push(evidenceFor('/api/client-integrations', integrationsResult));
    const integrations = integrationsResult.ok ? normalizeIntegrationStates(integrationsResult.body) : null;
    section.clientIntegrations = integrations ?? unknownSection(integrationsResult.reason ?? 'integrations_schema', {
      status: integrationsResult.status, code: integrationsResult.code,
    });

    const asideResult = await get('/api/client-integrations/aside/profiles');
    evidence.push(evidenceFor('/api/client-integrations/aside/profiles', asideResult));
    const aside = asideResult.ok ? normalizeAsideProfiles(asideResult.body) : null;
    section.asideProfiles = aside ?? unknownSection(asideResult.reason ?? 'aside_profiles_schema', {
      status: asideResult.status, code: asideResult.code,
    });

    const surface = {};
    const surfaceReads = [
      ['v2', '/api/v2', normalizeV2State],
      ['injectionModel', '/api/injection-model', normalizeInjectionModel],
      ['effortCaps', '/api/effort-caps', normalizeEffortCaps],
      ['subagentModels', '/api/subagent-models', normalizeSubagentModels],
      ['subagentModelFallback', '/api/subagent-model-fallback', normalizeSubagentFallback],
    ];
    for (const [key, pathname, normalize] of surfaceReads) {
      const result = await get(pathname);
      evidence.push(evidenceFor(pathname, result));
      const normalized = result.ok ? normalize(result.body) : null;
      surface[key] = normalized ?? unknownSection(result.reason ?? `${key}_schema`, {
        status: result.status, code: result.code,
      });
    }
    section.subagentSurface = surface;
    return section;
  }

  async function snapshot() {
    const attached = await attach();
    const evidence = [...attached.evidence];
    const base = {
      observedAt: new Date().toISOString(),
      endpoint: config.endpoint,
      lifecycleOwner: 'external',
      moduleArtifactId: MODULE_ARTIFACT_ID,
      expectedVersion: config.expectedVersion,
      observedVersion: attached.observedVersion,
      versionComparison: attached.versionComparison,
    };
    if (!token) {
      const missing = () => unknownSection('admin_token_env_missing');
      return redactValue({
        ...base, readiness: 'unknown',
        health: missing(), memory: missing(), providers: missing(),
        models: missing(), usage: missing(), configuration: missing(), evidence,
      }, [token]);
    }
    const health = attached.state === 'attached'
      ? attached.health
      : unknownSection(attached.reason, { status: attached.status ?? null, code: attached.code ?? null });
    const memory = attached.state === 'attached' ? attached.memory : unknownSection(attached.reason);

    const providersResult = await get('/api/providers');
    evidence.push(evidenceFor('/api/providers', providersResult));
    let providers;
    if (providersResult.ok) {
      const rows = normalizeProviders(providersResult.body);
      if (!rows) {
        providers = unknownSection('providers_schema');
      } else {
        providers = [];
        for (const row of rows.slice(0, MAX_PROTOCOL_PROVIDERS)) {
          const entry = { ...row, adapterSource: null, upstream: null };
          const protocolResult = await get(`/api/protocols?provider=${encodeURIComponent(row.id)}`);
          evidence.push(evidenceFor(`/api/protocols?provider=${row.id}`, protocolResult));
          if (protocolResult.ok) {
            const protocol = normalizeProtocol(protocolResult.body);
            if (protocol) {
              entry.adapter = protocol.adapter ?? entry.adapter;
              entry.adapterSource = protocol.adapterSource;
              entry.authMode = protocol.authMode ?? entry.authMode;
              entry.upstream = protocol.upstream;
              entry.modelOverrides = protocol.modelOverrides;
            }
          }
          // The label rests on provider + authMode evidence only (§D);
          // billing is derived from authMode and from nothing else.
          entry.billing = billingFor(entry.authMode, entry.id);
          entry.billingTierReported = entry.authMode === 'oauth' ? false : null;
          entry.evidenceLevel = 'observed';
          providers.push(entry);
        }
      }
    } else {
      providers = unknownSection(providersResult.reason, {
        status: providersResult.status, code: providersResult.code,
      });
    }

    const modelsResult = await get('/api/models');
    evidence.push(evidenceFor('/api/models', modelsResult));
    let models;
    if (modelsResult.ok) {
      const rows = normalizeModels(modelsResult.body);
      models = rows ? rows.slice(0, MAX_MODEL_ROWS) : unknownSection('models_schema');
    } else {
      // e.g. 503 catalog_busy: the section degrades alone; no retry
      // inside a snapshot (Retry-After is honoured by not retrying).
      models = unknownSection(modelsResult.reason, {
        status: modelsResult.status, code: modelsResult.code,
      });
    }

    const usageResult = await get('/api/usage');
    evidence.push(evidenceFor('/api/usage', usageResult));
    let usage;
    if (usageResult.ok) {
      const normalized = normalizeUsage(usageResult.body);
      usage = normalized
        ? {
            state: 'observed',
            ...normalized,
            // The upstream figure is a configured-pricing estimate,
            // never an invoice or subscription charge.
            estimatedCostLabel: normalized.estimatedCostUsd !== null
              ? 'configured-pricing estimate — not an invoice or subscription charge'
              : null,
          }
        : unknownSection('usage_schema');
    } else {
      usage = unknownSection(usageResult.reason, {
        status: usageResult.status, code: usageResult.code,
      });
    }

    const configuration = await configurationSection(evidence);

    const readiness = attached.state !== 'attached' ? 'unknown' : 'observed';
    return redactValue({
      ...base, readiness, health, memory, providers, models, usage, configuration, evidence,
    }, [token]);
  }

  function describe() {
    return {
      entrypoint: ENTRYPOINT,
      moduleArtifactId: MODULE_ARTIFACT_ID,
      upstream: UPSTREAM,
      lifecycleOwner: 'external',
      capabilities: CAPABILITIES,
      // The service version is an observation (health read), not an
      // executor version; nothing is claimed before an attach.
      serviceVersionObserved: identity?.version ?? null,
      identity,
    };
  }

  function unavailable(operation) {
    return {
      outcome: 'unavailable',
      capability: operation,
      reason: 'execution and replies belong to the native Codex backend (C07); this module only observes the provider service and applies operator-requested configuration',
    };
  }

  // --- Saved-Operation machinery (slice 2) ---------------------------------
  // A record is the Operation's result: the host saves it as-is. Every
  // record carries the contract block, the recorded scope, what was
  // requested, what was sent (at most one mutation), the normalised
  // receipt, per-element outcomes where the upstream envelope has
  // them, the readback evidence and a verification verdict that keeps
  // `saved` and `applied` apart.

  function newRecord(request, effectScope, applicationBoundary) {
    return {
      outcome: null,
      kind: request.kind,
      operation_contract: operationContract(effectScope, applicationBoundary),
      scope: {},
      requested: null,
      plan: null,
      mutation: { performed: false, method: null, path: null, status: null, code: null, reason: null },
      receipt: null,
      elements: null,
      readback: null,
      verification: { state: 'not_performed', fields: {} },
      evidence: [],
    };
  }

  function finish(record) {
    return redactValue(record, [token]);
  }

  // The attach gate: a configure/preview runs only against an
  // attached service with a resolved admin credential. The observed
  // service version is NOT part of the gate: the service is
  // operator-managed and unpinned, and a version differing from the
  // contract baseline is recorded on the record as an observation
  // (serviceVersion), never treated as a defect or a blocker.
  async function gate(record) {
    const attached = await attach();
    record.evidence.push(...attached.evidence);
    record.serviceVersion = {
      observed: attached.observedVersion,
      baseline: config.expectedVersion,
      comparison: attached.versionComparison,
    };
    if (!token) return 'admin_token_env_missing';
    if (attached.state !== 'attached') return attached.reason ?? 'not_attached';
    return null;
  }

  async function readFor(record, pathname, label = pathname) {
    const result = await get(pathname);
    record.evidence.push(evidenceFor(label, result));
    return result;
  }

  async function mutateFor(record, method, pathname, body) {
    const result = await send(method, pathname, body);
    record.evidence.push(mutationEvidenceFor(`${method} ${pathname}`, result));
    record.mutation = {
      performed: true,
      method,
      path: pathname,
      status: result.status,
      code: null,
      reason: result.responded ? null : result.reason,
    };
    return result;
  }

  function refuseFromResponse(record, result) {
    const { reason, code } = mutationFailure(result);
    record.mutation.code = code;
    record.mutation.reason = reason;
    record.outcome = 'refused';
    record.reason = reason;
    if (code) record.code = code;
    return record;
  }

  function staleFromResponse(record, result) {
    const { code } = mutationFailure(result);
    record.mutation.code = code;
    record.mutation.reason = 'integration_preview_stale';
    record.outcome = 'stale';
    record.reason = 'integration_preview_stale';
    record.code = 'integration_preview_stale';
    // The fresh plan is the operator's next decision input. It is
    // returned verbatim (normalised); the confirmed fingerprint is
    // never reused and the mutation is never re-sent.
    record.plan = normalizeIntegrationPlan(isObject(result.body) ? result.body.plan : null);
    record.nextStep = 'review the fresh plan and, if the change is still wanted, save a new Operation confirming the new planFingerprint';
    return record;
  }

  function isStale(result) {
    return result.responded && result.status === 409
      && isObject(result.body) && result.body.code === 'integration_preview_stale';
  }

  function verifyFields(fields) {
    const values = Object.values(fields);
    const state = values.includes('mismatch') ? 'mismatch'
      : values.length > 0 && values.every((value) => value === 'verified') ? 'verified'
      : 'observed';
    return { state, fields };
  }

  function setEqual(a, b) {
    if (!Array.isArray(a) || !Array.isArray(b)) return false;
    const left = [...a].sort();
    const right = [...b].sort();
    return left.length === right.length && left.every((value, index) => value === right[index]);
  }

  // --- protocol_settings ---------------------------------------------------

  function protocolLeaves(patch) {
    const leaves = {};
    if (patch.messagesEnabled !== undefined) leaves.messagesEnabled = patch.messagesEnabled;
    if (patch.unrepresentable !== undefined) leaves.unrepresentable = patch.unrepresentable;
    if (patch.rollout) {
      for (const key of ROLLOUT_KEYS) {
        if (patch.rollout[key] !== undefined) leaves[`rollout.${key}`] = patch.rollout[key];
      }
    }
    return leaves;
  }

  function protocolVerification(leaves, observed) {
    const fields = {};
    for (const [leaf, wanted] of Object.entries(leaves)) {
      if (!observed) {
        fields[leaf] = 'not_checked';
      } else if (leaf === 'messagesEnabled') {
        fields[leaf] = observed.messagesEnabled === wanted ? 'verified' : 'mismatch';
      } else if (leaf === 'unrepresentable') {
        fields[leaf] = observed.unrepresentable === wanted ? 'verified' : 'mismatch';
      } else if (leaf === 'rollout.managedMessagesNativeOAuth') {
        // Resolved upstream as native && stored: the expected resolved
        // value folds the native lane in.
        const expected = wanted === true && observed.rollout.managedMessagesNative === true;
        fields[leaf] = observed.rollout.managedMessagesNativeOAuth === expected ? 'verified' : 'mismatch';
      } else {
        const key = leaf.slice('rollout.'.length);
        fields[leaf] = observed.rollout[key] === wanted ? 'verified' : 'mismatch';
      }
    }
    return verifyFields(fields);
  }

  async function protocolCurrent(record) {
    const result = await readFor(record, '/api/protocols');
    return result.ok ? normalizeProtocolSettings(result.body) : null;
  }

  async function runProtocolSettings(request, mode) {
    const record = newRecord(request, 'shared_service:protocol_settings', 'service_config_immediate');
    record.scope = { family: 'protocols' };
    record.requested = { patch: request.patch };
    const leaves = protocolLeaves(request.patch);
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    const before = await protocolCurrent(record);
    record.readback = { before };
    if (mode === 'preview') {
      record.outcome = 'preview';
      record.differingFields = Object.entries(leaves)
        .filter(([leaf, wanted]) => {
          if (!before) return true;
          if (leaf === 'messagesEnabled') return before.messagesEnabled !== wanted;
          if (leaf === 'unrepresentable') return before.unrepresentable !== wanted;
          return before.rollout[leaf.slice('rollout.'.length)] !== wanted;
        })
        .map(([leaf]) => leaf);
      return finish(record);
    }
    // Upstream refuses managedMessagesNativeOAuth=true unless the merged
    // rollout keeps managedMessagesNative on; check against the merged
    // state locally so the refusal is recorded without a wasted write.
    if (request.patch.rollout?.managedMessagesNativeOAuth === true) {
      const mergedNative = request.patch.rollout.managedMessagesNative
        ?? before?.rollout?.managedMessagesNative;
      if (mergedNative !== true) {
        record.outcome = 'rejected';
        record.reason = 'rollout_dependency';
        return finish(record);
      }
    }
    const result = await mutateFor(record, 'PATCH', '/api/protocols/settings', request.patch);
    if (!result.responded) {
      record.outcome = 'unknown';
      record.reason = result.reason;
      const after = await protocolCurrent(record);
      record.readback = { before, after };
      if (after) record.verification = protocolVerification(leaves, after);
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (result.status !== 200) return finish(refuseFromResponse(record, result));
    record.receipt = normalizeProtocolSettings(result.body);
    const after = await protocolCurrent(record);
    record.readback = { before, after };
    record.verification = protocolVerification(leaves, after);
    record.outcome = record.verification.state === 'mismatch' ? 'partial' : 'applied';
    return finish(record);
  }

  // --- model_settings --------------------------------------------------------

  async function modelReadback(record, provider, modelId) {
    const modelsResult = await readFor(record, '/api/models');
    const row = modelsResult.ok ? findModelRowSettings(modelsResult.body, provider, modelId) : null;
    const integrationsResult = await readFor(record, '/api/client-integrations');
    const integrations = integrationsResult.ok
      ? normalizeIntegrationStates(integrationsResult.body)
      : null;
    return { model: row, clientIntegrations: integrations };
  }

  function modelVerification(request, readback, receipt) {
    const fields = {};
    const row = readback?.model;
    if (request.contextWindow !== undefined) {
      fields.contextWindow = row?.found
        ? (row.contextWindowDeclared === request.contextWindow ? 'verified' : 'mismatch')
        : 'not_checked';
    }
    if (request.inputModalities !== undefined) {
      const wanted = request.inputModalities === null || request.inputModalities.length === 0
        ? null
        : request.inputModalities;
      fields.inputModalities = row?.found
        ? ((wanted === null && row.inputModalitiesDeclared === null)
          || (wanted !== null && setEqual(row.inputModalitiesDeclared ?? [], wanted))
          ? 'verified' : 'mismatch')
        : 'not_checked';
    }
    // The ladder and default have no declared readback field on the
    // model row (row values are effective); the receipt's stored-state
    // echo is the evidence for those two axes.
    if (request.reasoningEfforts !== undefined) {
      fields.reasoningEfforts = receipt
        ? (setEqual(receipt.reasoningEfforts ?? [], request.reasoningEfforts ?? [])
          || (request.reasoningEfforts === null && receipt.reasoningEfforts === null)
          ? 'verified' : 'mismatch')
        : 'not_checked';
    }
    if (request.defaultReasoningEffort !== undefined) {
      fields.defaultReasoningEffort = receipt
        ? (receipt.defaultReasoningEffort === request.defaultReasoningEffort ? 'verified' : 'mismatch')
        : 'not_checked';
    }
    return verifyFields(fields);
  }

  async function runModelSettings(request, mode) {
    const record = newRecord(request, `provider_model:${request.provider}/${request.modelId}`, 'service_config_immediate');
    record.scope = { provider: request.provider, modelId: request.modelId };
    const body = { provider: request.provider, modelId: request.modelId };
    for (const axis of ['contextWindow', 'inputModalities', 'reasoningEfforts', 'defaultReasoningEffort']) {
      if (request[axis] !== undefined) body[axis] = request[axis];
    }
    record.requested = { ...body };
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    if (mode === 'preview') {
      const readback = await modelReadback(record, request.provider, request.modelId);
      record.readback = { before: readback };
      record.outcome = 'preview';
      return finish(record);
    }
    const result = await mutateFor(record, 'PUT', '/api/model-settings', body);
    if (!result.responded) {
      // Never re-send: reconcile the unknown outcome by reading the
      // stored declarations back.
      record.outcome = 'unknown';
      record.reason = result.reason;
      const readback = await modelReadback(record, request.provider, request.modelId);
      record.readback = readback;
      record.verification = modelVerification(request, readback, null);
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (result.status !== 200) {
      // e.g. 500: upstream states the save failed and live config is
      // unchanged; the refusal is recorded, nothing is retried.
      return finish(refuseFromResponse(record, result));
    }
    record.receipt = normalizeModelSettingsReceipt(result.body);
    record.elements = normalizeIntegrationOutcomes(result.body);
    const readback = await modelReadback(record, request.provider, request.modelId);
    record.readback = readback;
    record.verification = modelVerification(request, readback, record.receipt);
    const elementFailures = (record.elements ?? []).some((entry) => entry.ok === false);
    const refreshFailed = record.receipt?.catalogRefresh?.status === 'failed';
    record.outcome = elementFailures || refreshFailed || record.verification.state === 'mismatch'
      ? 'partial'
      : 'applied';
    // saved (this request published config) and applied (per-client
    // integration files after catalog convergence) stay separate
    // facts; neither is inferred from the other.
    record.savedVsApplied = {
      saved: record.receipt?.saved ?? null,
      changed: record.receipt?.changed ?? null,
      hasOverrides: record.receipt?.hasOverrides ?? null,
      catalogRefresh: record.receipt?.catalogRefresh ?? null,
      clientIntegrationsObserved: readback.clientIntegrations !== null,
    };
    return finish(record);
  }

  // --- sub-agent surface -----------------------------------------------------
  // All five settings families apply at the upstream-defined boundary:
  // new sessions only; existing sessions keep their binding/surface.

  const NEW_SESSIONS = 'new_sessions_only';

  async function runSubagentV2(request, mode) {
    const record = newRecord(request, 'shared_service:subagent_surface:v2', NEW_SESSIONS);
    record.scope = { surface: 'v2' };
    record.requested = { settings: request.settings };
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    const beforeResult = await readFor(record, '/api/v2');
    const before = beforeResult.ok ? normalizeV2State(beforeResult.body) : null;
    record.readback = { before };
    if (mode === 'preview') {
      record.outcome = 'preview';
      return finish(record);
    }
    const settings = request.settings;
    // Upstream's own enabled/mode consistency rules, checked against
    // the merged state so the refusal is recorded without a write.
    const effectiveMode = settings.multiAgentMode ?? before?.multiAgentMode ?? 'default';
    const effectiveKeepNative = settings.keepNativeChatGptOnV1 !== undefined
      ? settings.keepNativeChatGptOnV1 === true
      : before?.keepNativeChatGptOnV1 === true;
    const hybridPinActive = effectiveMode === 'v2' && effectiveKeepNative;
    const modeFlag = settings.multiAgentMode === 'v2' ? !hybridPinActive
      : settings.multiAgentMode === 'v1' ? false
      : undefined;
    if (settings.enabled !== undefined && modeFlag !== undefined && settings.enabled !== modeFlag) {
      record.outcome = 'rejected';
      record.reason = 'enabled_mode_conflict';
      return finish(record);
    }
    if (settings.enabled === true && hybridPinActive) {
      record.outcome = 'rejected';
      record.reason = 'enabled_keep_native_conflict';
      return finish(record);
    }
    const result = await mutateFor(record, 'PUT', '/api/v2', settings);
    const verify = (observed) => {
      const fields = {};
      for (const [key, wanted] of Object.entries(settings)) {
        if (!observed) fields[key] = 'not_checked';
        else if (key === 'subagentDeveloperInstructions' || key === 'multiAgentModeHintText') {
          const observedText = observed[key];
          const wantedSet = wanted !== null;
          fields[key] = observedText.set === wantedSet
            && (!wantedSet || observedText.length === String(wanted).length)
            ? 'verified' : 'mismatch';
        } else if (wanted === null && (key === 'agentsEnabled' || key === 'agentsMaxDepth')) {
          // null unsets the key; the GET readers resolve a default, so
          // the landed value is observed, not equality-checked.
          fields[key] = 'observed';
        } else {
          fields[key] = observed[key] === wanted ? 'verified' : 'mismatch';
        }
      }
      return verifyFields(fields);
    };
    if (!result.responded) {
      record.outcome = 'unknown';
      record.reason = result.reason;
      const afterResult = await readFor(record, '/api/v2');
      const after = afterResult.ok ? normalizeV2State(afterResult.body) : null;
      record.readback = { before, after };
      if (after) record.verification = verify(after);
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (result.status === 200) {
      record.receipt = normalizeV2Receipt(result.body);
      const afterResult = await readFor(record, '/api/v2');
      const after = afterResult.ok ? normalizeV2State(afterResult.body) : null;
      record.readback = { before, after };
      record.verification = verify(after);
      record.outcome = record.verification.state === 'mismatch' ? 'partial' : 'applied';
      return finish(record);
    }
    if (result.status === 502) {
      // A mid-sequence write failure: earlier fields may already have
      // landed (upstream's error names them). The record shows the
      // readback, never the upstream prose.
      refuseFromResponse(record, result);
      const afterResult = await readFor(record, '/api/v2');
      const after = afterResult.ok ? normalizeV2State(afterResult.body) : null;
      record.readback = { before, after };
      if (after) {
        record.verification = verify(after);
        if (record.verification.state === 'verified') record.outcome = 'partial';
      }
      return finish(record);
    }
    return finish(refuseFromResponse(record, result));
  }

  async function runInjectionModel(request, mode) {
    const record = newRecord(request, 'shared_service:subagent_surface:injection_model', NEW_SESSIONS);
    record.scope = { surface: 'injection_model' };
    record.requested = { settings: request.settings };
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    const beforeResult = await readFor(record, '/api/injection-model');
    const before = beforeResult.ok ? normalizeInjectionModel(beforeResult.body) : null;
    record.readback = { before };
    if (mode === 'preview') {
      record.outcome = 'preview';
      return finish(record);
    }
    const settings = request.settings;
    // The effort ladder is served by the GET; an effort outside it is
    // refused locally (upstream answers the same request with a 400).
    if (typeof settings.effort === 'string' && before?.efforts
      && !before.efforts.includes(settings.effort)) {
      record.outcome = 'rejected';
      record.reason = 'invalid_effort';
      return finish(record);
    }
    if (settings.syncCodexSubagentDefaults === true) {
      const model = settings.model !== undefined ? settings.model : before?.model;
      if (!model) {
        record.outcome = 'rejected';
        record.reason = 'sync_requires_model';
        return finish(record);
      }
    }
    const result = await mutateFor(record, 'PUT', '/api/injection-model', settings);
    const verify = (observed) => {
      const fields = {};
      for (const [key, wanted] of Object.entries(settings)) {
        if (!observed) fields[key] = 'not_checked';
        else if (key === 'prompt') fields[key] = observed.promptSet === (wanted !== null && wanted !== '') ? 'verified' : 'mismatch';
        else if (key === 'model') fields[key] = (observed.model ?? null) === (wanted === '' ? null : wanted) ? 'verified' : 'mismatch';
        else if (key === 'effort') fields[key] = (observed.effort ?? null) === (wanted === '' ? null : wanted) ? 'verified' : 'mismatch';
        else fields[key] = observed[key] === wanted ? 'verified' : 'mismatch';
      }
      return verifyFields(fields);
    };
    if (!result.responded) {
      record.outcome = 'unknown';
      record.reason = result.reason;
      const afterResult = await readFor(record, '/api/injection-model');
      const after = afterResult.ok ? normalizeInjectionModel(afterResult.body) : null;
      record.readback = { before, after };
      if (after) record.verification = verify(after);
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (result.status !== 200) return finish(refuseFromResponse(record, result));
    record.receipt = normalizeInjectionModel(result.body);
    const afterResult = await readFor(record, '/api/injection-model');
    const after = afterResult.ok ? normalizeInjectionModel(afterResult.body) : null;
    record.readback = { before, after };
    record.verification = verify(after);
    record.outcome = record.verification.state === 'mismatch' ? 'partial' : 'applied';
    return finish(record);
  }

  async function runEffortCaps(request, mode) {
    const record = newRecord(request, 'shared_service:subagent_surface:effort_caps', NEW_SESSIONS);
    record.scope = { surface: 'effort_caps' };
    const body = {};
    if (request.effortCap !== undefined) body.effortCap = request.effortCap;
    if (request.subagentEffortCap !== undefined) body.subagentEffortCap = request.subagentEffortCap;
    record.requested = { ...body };
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    const beforeResult = await readFor(record, '/api/effort-caps');
    const before = beforeResult.ok ? normalizeEffortCaps(beforeResult.body) : null;
    record.readback = { before };
    if (mode === 'preview') {
      record.outcome = 'preview';
      return finish(record);
    }
    for (const [key, wanted] of Object.entries(body)) {
      if (typeof wanted === 'string' && before?.efforts && !before.efforts.includes(wanted)) {
        record.outcome = 'rejected';
        record.reason = 'invalid_effort';
        record.field = key;
        return finish(record);
      }
    }
    const result = await mutateFor(record, 'PUT', '/api/effort-caps', body);
    const verify = (observed) => {
      const fields = {};
      for (const [key, wanted] of Object.entries(body)) {
        fields[key] = !observed ? 'not_checked'
          : (observed[key] ?? null) === wanted ? 'verified' : 'mismatch';
      }
      return verifyFields(fields);
    };
    if (!result.responded) {
      record.outcome = 'unknown';
      record.reason = result.reason;
      const afterResult = await readFor(record, '/api/effort-caps');
      const after = afterResult.ok ? normalizeEffortCaps(afterResult.body) : null;
      record.readback = { before, after };
      if (after) record.verification = verify(after);
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (result.status !== 200) return finish(refuseFromResponse(record, result));
    record.receipt = normalizeEffortCaps(result.body);
    const afterResult = await readFor(record, '/api/effort-caps');
    const after = afterResult.ok ? normalizeEffortCaps(afterResult.body) : null;
    record.readback = { before, after };
    record.verification = verify(after);
    record.outcome = record.verification.state === 'mismatch' ? 'partial' : 'applied';
    return finish(record);
  }

  async function runSubagentModels(request, mode) {
    const record = newRecord(request, 'shared_service:subagent_surface:subagent_models', NEW_SESSIONS);
    record.scope = { surface: 'subagent_models' };
    record.requested = { models: request.models };
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    const beforeResult = await readFor(record, '/api/subagent-models');
    const before = beforeResult.ok ? normalizeSubagentModels(beforeResult.body) : null;
    record.readback = { before };
    if (mode === 'preview') {
      record.outcome = 'preview';
      return finish(record);
    }
    const result = await mutateFor(record, 'PUT', '/api/subagent-models', { models: request.models });
    const verify = (observed) => verifyFields({
      models: !observed?.chosen ? 'not_checked'
        : JSON.stringify(observed.chosen) === JSON.stringify(request.models) ? 'verified' : 'mismatch',
    });
    if (!result.responded) {
      record.outcome = 'unknown';
      record.reason = result.reason;
      const afterResult = await readFor(record, '/api/subagent-models');
      const after = afterResult.ok ? normalizeSubagentModels(afterResult.body) : null;
      record.readback = { before, after };
      if (after) record.verification = verify(after);
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (result.status !== 200) return finish(refuseFromResponse(record, result));
    record.receipt = normalizeSubagentModels(result.body);
    const afterResult = await readFor(record, '/api/subagent-models');
    const after = afterResult.ok ? normalizeSubagentModels(afterResult.body) : null;
    record.readback = { before, after };
    record.verification = verify(after);
    record.outcome = record.verification.state === 'mismatch' ? 'partial' : 'applied';
    return finish(record);
  }

  async function runSubagentModelFallback(request, mode) {
    const record = newRecord(request, 'shared_service:subagent_surface:subagent_model_fallback', NEW_SESSIONS);
    record.scope = { surface: 'subagent_model_fallback' };
    const body = {};
    if (request.models !== undefined) body.models = request.models;
    if (request.pollMs !== undefined) body.pollMs = request.pollMs;
    record.requested = { ...body };
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    const beforeResult = await readFor(record, '/api/subagent-model-fallback');
    const before = beforeResult.ok ? normalizeSubagentFallback(beforeResult.body) : null;
    record.readback = { before };
    if (mode === 'preview') {
      record.outcome = 'preview';
      return finish(record);
    }
    const result = await mutateFor(record, 'PUT', '/api/subagent-model-fallback', body);
    const verify = (observed) => {
      const fields = {};
      if (body.models !== undefined) {
        fields.models = !observed?.models ? 'not_checked'
          : JSON.stringify(observed.models) === JSON.stringify(body.models) ? 'verified' : 'mismatch';
      }
      if (body.pollMs !== undefined) {
        // A cleared pollMs resolves to the upstream default on read.
        fields.pollMs = !observed ? 'not_checked'
          : body.pollMs === null ? 'observed'
          : observed.pollMs === body.pollMs ? 'verified' : 'mismatch';
      }
      return verifyFields(fields);
    };
    if (!result.responded) {
      record.outcome = 'unknown';
      record.reason = result.reason;
      const afterResult = await readFor(record, '/api/subagent-model-fallback');
      const after = afterResult.ok ? normalizeSubagentFallback(afterResult.body) : null;
      record.readback = { before, after };
      if (after) record.verification = verify(after);
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (result.status !== 200) return finish(refuseFromResponse(record, result));
    record.receipt = normalizeSubagentFallback(result.body);
    const afterResult = await readFor(record, '/api/subagent-model-fallback');
    const after = afterResult.ok ? normalizeSubagentFallback(afterResult.body) : null;
    record.readback = { before, after };
    record.verification = verify(after);
    record.outcome = record.verification.state === 'mismatch' ? 'partial' : 'applied';
    return finish(record);
  }

  // --- client integrations ---------------------------------------------------
  // Apply/rollback only through preview + planFingerprint, only for
  // the client or single Aside profile the operator selected.

  function integrationPaths(request) {
    // In preview mode there is no binding yet; the apply body is only
    // assembled for configure, where validation guaranteed one.
    const bindingFields = request.binding
      ? { operation: request.binding.operation, planFingerprint: request.binding.planFingerprint }
      : {};
    if (request.kind === 'client_integration') {
      return {
        previewPath: '/api/client-integrations/preview',
        previewBody: { clientId: request.clientId, operation: request.operation },
        applyMethod: 'PUT',
        applyPath: `/api/client-integrations/${encodeURIComponent(request.clientId)}`,
        applyBody: {
          enabled: request.enabled,
          ...(request.overwriteConflict ? { overwriteConflict: true } : {}),
          ...bindingFields,
        },
        statePath: `/api/client-integrations/${encodeURIComponent(request.clientId)}`,
        expectedState: request.operation === 'disable' ? 'absent' : 'current',
      };
    }
    if (request.kind === 'aside_profile') {
      const base = `/api/client-integrations/aside/profiles/${request.profileId}`;
      return {
        previewPath: `${base}/preview`,
        previewBody: { operation: request.operation },
        applyMethod: 'PUT',
        applyPath: base,
        applyBody: {
          enabled: request.enabled,
          ...(request.overwriteConflict ? { overwriteConflict: true } : {}),
          ...bindingFields,
        },
        statePath: base,
        expectedState: request.operation === 'disable' ? 'absent' : 'current',
      };
    }
    if (request.kind === 'client_integration_restore') {
      return {
        previewPath: '/api/client-integrations/restore/preview',
        previewBody: { opId: request.opId, confirmDrift: request.confirmDrift },
        applyMethod: 'POST',
        applyPath: '/api/client-integrations/restore',
        applyBody: {
          opId: request.opId,
          confirmDrift: request.confirmDrift,
          ...bindingFields,
        },
        statePath: `/api/client-integrations/${encodeURIComponent(request.clientId)}`,
        expectedState: null,
      };
    }
    const base = `/api/client-integrations/aside/profiles/${request.profileId}`;
    return {
      previewPath: `${base}/preview`,
      previewBody: { operation: 'restore', opId: request.opId, confirmDrift: request.confirmDrift },
      applyMethod: 'POST',
      applyPath: `${base}/restore`,
      applyBody: {
        opId: request.opId,
        confirmDrift: request.confirmDrift,
        ...bindingFields,
      },
      statePath: base,
      expectedState: null,
    };
  }

  async function integrationStateRead(record, request, paths) {
    const result = await readFor(record, paths.statePath);
    if (!result.ok) return null;
    return request.kind.startsWith('aside')
      ? normalizeAsideProfileState(result.body)
      : normalizeIntegrationState(result.body);
  }

  async function runIntegration(request, mode) {
    const isAside = request.kind.startsWith('aside');
    const effectScope = request.kind === 'client_integration'
      ? `client_integration:${request.clientId}`
      : request.kind === 'client_integration_restore'
        ? `client_integration_restore:${request.clientId}`
        : request.kind === 'aside_profile'
          ? `aside_profile:${request.profileId}`
          : `aside_profile_restore:${request.profileId}`;
    const record = newRecord(request, effectScope, 'client_files_immediate');
    record.scope = request.kind.includes('restore')
      ? { opId: request.opId, ...(isAside ? { profileId: request.profileId } : { clientId: request.clientId }) }
      : (isAside ? { profileId: request.profileId } : { clientId: request.clientId });
    record.requested = request.kind.includes('restore')
      ? { opId: request.opId, confirmDrift: request.confirmDrift }
      : { operation: request.operation, enabled: request.enabled, overwriteConflict: request.overwriteConflict };
    const paths = integrationPaths(request);
    const gateReason = await gate(record);
    if (gateReason) {
      record.outcome = 'unknown';
      record.reason = gateReason;
      return finish(record);
    }
    if (mode === 'preview') {
      const result = await mutateFor(record, 'POST', paths.previewPath, paths.previewBody);
      // A preview POST writes nothing upstream (it is the documented
      // planning read), so `performed` describes the planning call,
      // never a configuration change.
      record.mutation.performed = false;
      record.mutation.reason = null;
      if (!result.responded) {
        record.outcome = 'unknown';
        record.reason = result.reason;
        return finish(record);
      }
      if (result.status === 200) {
        record.plan = normalizeIntegrationPlan(result.body);
        record.outcome = 'preview';
        return finish(record);
      }
      const { reason, code } = mutationFailure(result);
      record.mutation.code = code;
      if (code === 'integration_preview_unavailable') {
        // Upstream's documented remedy is a state read; include it as
        // evidence for the operator's next decision.
        record.outcome = 'unknown';
        record.reason = reason;
        record.code = code;
        const statesResult = await readFor(record, '/api/client-integrations');
        record.readback = statesResult.ok
          ? { clientIntegrations: normalizeIntegrationStates(statesResult.body) }
          : null;
        record.nextStep = 'the service retains no usable model roster for planning; read the integration state and retry the preview once a roster exists';
        return finish(record);
      }
      return finish(refuseFromResponse(record, result));
    }
    // configure: the binding was validated locally (both-or-neither,
    // operation agreement) before this point.
    const result = await mutateFor(record, paths.applyMethod, paths.applyPath, paths.applyBody);
    if (!result.responded) {
      record.outcome = 'unknown';
      record.reason = result.reason;
      const after = await integrationStateRead(record, request, paths);
      record.readback = { after };
      if (after && paths.expectedState) {
        record.verification = verifyFields({
          state: after.state === paths.expectedState ? 'verified' : 'observed',
        });
      }
      record.verification.reconciledByRead = true;
      return finish(record);
    }
    if (isStale(result)) {
      // Return the fresh plan for a new operator decision; the stale
      // fingerprint is never reused and nothing is re-sent.
      const after = await integrationStateRead(record, request, paths);
      record.readback = { after };
      return finish(staleFromResponse(record, result));
    }
    if (result.status !== 200) {
      const refused = refuseFromResponse(record, result);
      refused.receipt = normalizeIntegrationOutcome(result.body);
      refused.elements = normalizeIntegrationOutcomes(result.body);
      const after = await integrationStateRead(record, request, paths);
      refused.readback = { after };
      return finish(refused);
    }
    record.receipt = normalizeIntegrationOutcome(result.body);
    record.elements = normalizeIntegrationOutcomes(result.body);
    const after = await integrationStateRead(record, request, paths);
    record.readback = { after };
    if (after && record.receipt?.state) {
      record.verification = verifyFields({
        state: after.state === record.receipt.state ? 'verified' : 'mismatch',
      });
    }
    const elementFailures = (record.elements ?? []).some((entry) => entry.ok === false);
    record.outcome = elementFailures || record.verification.state === 'mismatch' ? 'partial' : 'applied';
    return finish(record);
  }

  async function runRequest(rawRequest, mode) {
    const validated = validateRequest(rawRequest, mode);
    if (!validated.ok) {
      return redactValue({
        outcome: 'rejected',
        kind: isObject(rawRequest) ? str(rawRequest.kind) : null,
        reason: validated.reason,
        mutation: { performed: false, method: null, path: null, status: null, code: null, reason: null },
        evidence: [],
      }, [token]);
    }
    const request = validated.request;
    switch (request.kind) {
      case 'protocol_settings': return runProtocolSettings(request, mode);
      case 'model_settings': return runModelSettings(request, mode);
      case 'subagent_v2': return runSubagentV2(request, mode);
      case 'injection_model': return runInjectionModel(request, mode);
      case 'effort_caps': return runEffortCaps(request, mode);
      case 'subagent_models': return runSubagentModels(request, mode);
      case 'subagent_model_fallback': return runSubagentModelFallback(request, mode);
      default: return runIntegration(request, mode);
    }
  }

  // The journal is a read: it names the operations a restore can be
  // requested for. Exposed on the record path only (restore previews
  // resolve opIds upstream); snapshot keeps lastOpId per client.
  async function journal(clientId) {
    token = resolveToken();
    if (!token) return redactValue(unknownSection('admin_token_env_missing'), [token]);
    const query = clientId ? `?client=${encodeURIComponent(clientId)}` : '';
    const result = await get(`/api/client-integrations/journal${query}`);
    if (!result.ok) {
      return redactValue(unknownSection(result.reason, { status: result.status, code: result.code }), [token]);
    }
    return redactValue({ operations: normalizeIntegrationJournal(result.body) }, [token]);
  }

  return {
    describe,
    // open performs the attach probe and creates nothing: for an
    // externally owned service there is no session to open.
    open: attach,
    attach,
    snapshot,
    send: () => unavailable('send'),
    configure: (request) => runRequest(request, 'configure'),
    preview: (request) => runRequest(request, 'preview'),
    journal,
    reply: () => unavailable('reply'),
    // Detach only: drop the client and forget the token resolution. The
    // service is not called, signalled or otherwise touched.
    shutdown() {
      token = null;
      identity = null;
      return { outcome: 'detached', serviceTouched: false };
    },
  };
}

async function main(argv) {
  const [flag, configPath, operation, requestPath, ...rest] = argv;
  const operations = ['describe', 'open', 'attach', 'snapshot', 'send', 'configure', 'preview', 'journal', 'reply', 'shutdown'];
  if (flag !== '--config' || !configPath || !operation || rest.length > 0 || !operations.includes(operation)) {
    console.error('Usage: node bridge.mjs --config <local-module.json> <describe|open|attach|snapshot|send|configure <request.json>|preview <request.json>|journal [clientId]|reply|shutdown>');
    return 2;
  }
  const needsRequest = operation === 'configure' || operation === 'preview';
  if (needsRequest && !requestPath) {
    console.error(JSON.stringify({ error: 'REQUEST_FILE_REQUIRED' }));
    return 2;
  }
  if (!needsRequest && operation !== 'journal' && requestPath) {
    console.error(JSON.stringify({ error: 'UNEXPECTED_ARGUMENT' }));
    return 2;
  }
  let adapter;
  try {
    adapter = createAdapter(await loadConfig(configPath));
  } catch (error) {
    console.error(JSON.stringify({ error: error.code ?? 'CONFIG_INVALID' }));
    return 2;
  }
  let result;
  if (needsRequest) {
    let request;
    try {
      request = JSON.parse(await readFile(requestPath, 'utf8'));
    } catch {
      console.error(JSON.stringify({ error: 'REQUEST_INVALID' }));
      return 2;
    }
    result = await adapter[operation](request);
  } else if (operation === 'journal') {
    result = await adapter.journal(requestPath ?? null);
  } else {
    result = await adapter[operation]();
  }
  console.log(JSON.stringify({ operation, moduleArtifactId: MODULE_ARTIFACT_ID, result }));
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  process.exitCode = await main(process.argv.slice(2));
}
