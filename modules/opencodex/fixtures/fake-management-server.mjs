// Fake OpenCodex Management API server for the fixture selftest. Node
// stdlib only; serves the baseline-contract fixture reconstructions on an
// ephemeral loopback port and records every request (method + path +
// parsed body) so the selftest can prove what the adapter did and did
// not send. Fixtures are reconstructions from the pinned upstream
// contracts (see each fixture's provenance field), not captures from
// a live opencodex 2.75.0 service.
//
// Slice 2: the server is stateful for the configuration surface. Its
// initial state is fixtures/configuration-state.json; plans carry
// deterministic fixture fingerprints derived from a state version, so
// the stale-plan, preview-unavailable, lost-response and partial
// scenarios reproduce upstream's documented behaviours.
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));

async function fixture(name) {
  return JSON.parse(await readFile(path.join(here, name), 'utf8'));
}

const EFFORTS = ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'];
const ROLLOUT_KEYS = ['nativeChatCombos', 'managedMessagesNative', 'managedMessagesNativeOAuth', 'directEncoders', 'shadowPlan'];

export async function createFakeManagementServer({ scenario = 'happy', token = 'fixture-admin-token' } = {}) {
  const files = {
    health: await fixture('health.json'),
    memory: await fixture(scenario === 'no_launch_marker' ? 'memory-no-launch-marker.json' : 'memory.json'),
    providers: await fixture('providers.json'),
    models: await fixture('models.json'),
    usage: await fixture('usage.json'),
    unauthorized: await fixture('error-401.json'),
    unavailable: await fixture('error-503.json'),
    sibling: await fixture('error-409-sibling.json'),
    catalogBusy: await fixture('error-503-catalog-busy.json'),
    notFound: await fixture('error-404.json'),
    protocols: {
      anthropic: await fixture('protocols-anthropic.json'),
      xai: await fixture('protocols-xai.json'),
      openai: await fixture('protocols-openai.json'),
    },
  };
  if (scenario === 'version_mismatch') {
    files.health = { ...files.health, body: { ...files.health.body, version: '2.99.0-fixture' } };
  }
  const initial = await fixture('configuration-state.json');
  const state = JSON.parse(JSON.stringify(initial));
  delete state.provenance;
  let stateVersion = 1;
  let opCounter = 1;
  let protocolsRevision = 1;
  let journal = [
    {
      opId: 'fixture-op-codex-1', clientId: 'codex', kind: 'apply',
      at: '2026-10-01T00:00:00.000Z', configPath: '/fixture/codex/config.toml',
      snapshot: 'stored', undoable: true, deletable: false,
    },
  ];
  let lostResponseConsumed = false;
  let staleInjected = false;

  const fingerprint = (scope, operation) => `fixture-fp-${scope}-${operation}-v${stateVersion}`;
  function planFor(scope, clientId, operation, currentState, extra = {}) {
    const willChange = operation === 'restore' ? true
      : operation === 'disable' ? currentState === 'current'
      : currentState !== 'current';
    return {
      version: 1,
      clientId,
      operation,
      state: currentState,
      foreignEdit: 'none',
      changes: willChange ? [{ kind: operation === 'disable' ? 'remove' : 'replace', path: '$.fixture' }] : [],
      fingerprint: fingerprint(scope, operation),
      canApply: true,
      willChange,
      ...extra,
    };
  }
  function pushJournal(entry) {
    for (const row of journal) row.deletable = true;
    journal.unshift({ ...entry, undoable: true, deletable: false });
  }
  function recordApply(clientId, kind) {
    const opId = `fixture-op-${opCounter++}`;
    stateVersion += 1;
    pushJournal({
      opId, clientId, kind,
      at: new Date().toISOString(), configPath: `/fixture/${clientId}/config.toml`,
      snapshot: 'stored',
    });
    return opId;
  }

  function protocolInfo() {
    const rollout = state.protocols.rollout;
    return {
      schemaVersion: 1,
      contractVersion: 1,
      policyRevision: `fixture-rev-${protocolsRevision}`,
      surfaces: {
        responses: { enabled: true, source: 'fixed' },
        chat: { enabled: true, source: 'fixed' },
        messages: { enabled: state.protocols.messagesEnabled, source: 'config' },
      },
      settings: {
        unrepresentable: state.protocols.unrepresentable,
        rollout: {
          ...rollout,
          managedMessagesNativeOAuth: rollout.managedMessagesNative && rollout.managedMessagesNativeOAuth,
        },
      },
      features: [],
    };
  }

  function modelsBody() {
    const body = JSON.parse(JSON.stringify(files.models.body));
    const stored = state.modelSettings.anthropic['claude-sonnet-fixture'];
    const row = body.models[0];
    if (stored.contextWindow !== null && stored.contextWindow !== undefined) {
      row.contextWindowDeclared = stored.contextWindow;
    } else delete row.contextWindowDeclared;
    if (Array.isArray(stored.inputModalities)) row.inputModalitiesDeclared = stored.inputModalities;
    row.reasoningEfforts = stored.reasoningEfforts ?? ['low', 'medium', 'high'];
    if (stored.defaultReasoningEffort) row.defaultReasoningEffort = stored.defaultReasoningEffort;
    row.reasoningOverridden = stored.reasoningEfforts !== null && stored.reasoningEfforts !== undefined;
    return body;
  }

  function integrationStateBody(clientId) {
    const entry = state.integrations[clientId];
    return {
      clientId, state: entry.state, installed: entry.installed,
      configPath: `/fixture/${clientId}/config.toml`,
      appliedAt: entry.appliedAt, lastOpId: entry.lastOpId,
      snapshotCount: entry.snapshotCount, retentionDegraded: false,
    };
  }

  function asideListBody() {
    const profiles = Object.entries(state.asideProfiles).map(([id, entry]) => ({
      profileId: Number(id), enabled: entry.enabled, state: entry.state,
      installed: entry.installed, configPath: `/fixture/aside/${id}/config.toml`,
      snapshotCount: entry.snapshotCount, retentionDegraded: false,
    }));
    const enabledCount = profiles.filter((p) => p.enabled).length;
    return {
      clientId: 'aside', profiles,
      total: profiles.length, enabledCount,
      appliedCount: profiles.filter((p) => p.state === 'current' || p.state === 'stale').length,
      allEnabled: profiles.length > 0 && enabledCount === profiles.length,
      state: profiles.some((p) => p.state === 'unsafe') ? 'unsafe' : 'current',
      installed: profiles.some((p) => p.installed),
      configPath: '/fixture/aside/config.toml',
      snapshotCount: profiles.reduce((sum, p) => sum + p.snapshotCount, 0),
      retentionDegraded: false,
    };
  }

  function v2Body(extra = {}) {
    return {
      enabled: state.v2.enabled,
      agentsMaxThreadsConflict: false,
      maxConcurrentThreadsPerSession: state.v2.maxConcurrentThreadsPerSession,
      multiAgentMode: state.v2.multiAgentMode,
      multiAgentSurfaceAdvisory: null,
      keepNativeChatGptOnV1: state.v2.keepNativeChatGptOnV1,
      agentsEnabled: state.v2.agentsEnabled,
      agentsMaxDepth: state.v2.agentsMaxDepth,
      subagentDeveloperInstructions: state.v2.subagentDeveloperInstructions,
      multiAgentModeHintText: state.v2.multiAgentModeHintText,
      multiAgentModeHintRecommendation: null,
      agentsMaxDepthAppliesWhenV2Disabled: !state.v2.enabled,
      ...extra,
    };
  }

  // Simulate the intervening edit of the stale_plan scenario: the
  // world moves between the operator's preview and the bound confirm,
  // so the freshly computed plan no longer matches the confirmed
  // fingerprint. Called before the fresh plan is computed.
  function maybeInjectDrift(body) {
    if (scenario !== 'stale_plan' || staleInjected) return;
    if (body && body.planFingerprint !== undefined) {
      staleInjected = true;
      stateVersion += 1;
    }
  }

  // Validate + check a plan binding the way upstream does: both fields
  // or neither, the operation must agree, and a fingerprint that no
  // longer matches the freshly computed plan is a stale 409 carrying
  // that fresh plan. Returns true when a response was sent.
  function bindingGuard(send, body, expectedOperation, freshPlan) {
    const hasOperation = body.operation !== undefined;
    const hasFingerprint = body.planFingerprint !== undefined;
    if (hasOperation !== hasFingerprint) {
      send({ status: 400, body: { error: 'operation and planFingerprint must be sent together', code: 'invalid_preview_binding' } });
      return true;
    }
    if (!hasOperation) return false;
    if (body.operation !== expectedOperation) {
      send({ status: 400, body: { error: 'operation does not match the requested change', code: 'invalid_preview_operation' } });
      return true;
    }
    if (body.planFingerprint !== freshPlan.fingerprint) {
      send({
        status: 409,
        body: { error: 'integration preview is stale', code: 'integration_preview_stale', plan: freshPlan },
      });
      return true;
    }
    return false;
  }

  const requests = [];
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    const chunks = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      let body = null;
      const raw = Buffer.concat(chunks).toString('utf8');
      if (raw) {
        try { body = JSON.parse(raw); } catch { body = null; }
      }
      requests.push({
        method: req.method, path: url.pathname,
        authed: req.headers['x-opencodex-api-key'] === token, body,
      });
      const send = (entry) => {
        res.writeHead(entry.status, { 'content-type': 'application/json' });
        res.end(JSON.stringify(entry.body));
      };
      const pathname = url.pathname;
      if (pathname.startsWith('/v1/')) return send(files.notFound); // data plane: recorded, never served
      if (scenario === 'fail_closed') return send(files.unavailable);
      if (scenario === 'unauthorized' || req.headers['x-opencodex-api-key'] !== token) {
        return send(files.unauthorized);
      }
      if (scenario === 'sibling' && pathname === '/api/system/health') return send(files.sibling);
      if (pathname === '/api/system/health') return send(files.health);
      if (pathname === '/api/system/memory') return send(files.memory);
      if (pathname === '/api/providers') return send(files.providers);
      if (pathname === '/api/usage') return send(files.usage);

      // --- protocols -------------------------------------------------------
      if (pathname === '/api/protocols' && req.method === 'GET') {
        const provider = url.searchParams.get('provider');
        if (provider) return send(files.protocols[provider] ?? files.notFound);
        return send({ status: 200, body: protocolInfo() });
      }
      if (pathname === '/api/protocols/settings' && req.method === 'PATCH') {
        const patch = body ?? {};
        const keys = Object.keys(patch);
        if (keys.length === 0) return send({ status: 400, body: { error: { code: 'empty_body', message: 'body must set messagesEnabled, unrepresentable or rollout' } } });
        for (const key of keys) {
          if (!['messagesEnabled', 'unrepresentable', 'rollout'].includes(key)) {
            return send({ status: 400, body: { error: { code: 'unknown_field', message: 'body accepts only messagesEnabled, unrepresentable and rollout' } } });
          }
        }
        if (patch.rollout) {
          for (const key of Object.keys(patch.rollout)) {
            if (!ROLLOUT_KEYS.includes(key) || typeof patch.rollout[key] !== 'boolean') {
              return send({ status: 400, body: { error: { code: 'invalid_rollout', message: 'rollout accepts only boolean switches' } } });
            }
          }
          const merged = { ...state.protocols.rollout, ...patch.rollout };
          if (patch.rollout.managedMessagesNativeOAuth === true && merged.managedMessagesNative !== true) {
            return send({ status: 400, body: { error: { code: 'rollout_dependency', message: 'rollout.managedMessagesNativeOAuth requires rollout.managedMessagesNative' } } });
          }
        }
        if (patch.messagesEnabled !== undefined) state.protocols.messagesEnabled = patch.messagesEnabled;
        if (patch.unrepresentable !== undefined) state.protocols.unrepresentable = patch.unrepresentable;
        if (patch.rollout) Object.assign(state.protocols.rollout, patch.rollout);
        protocolsRevision += 1;
        return send({ status: 200, body: protocolInfo() });
      }

      // --- models ------------------------------------------------------------
      if (pathname === '/api/models' && req.method === 'GET') {
        return send(scenario === 'catalog_busy' ? files.catalogBusy : { status: 200, body: modelsBody() });
      }
      if (pathname === '/api/model-settings' && req.method === 'PUT') {
        const request = body ?? {};
        for (const key of Object.keys(request)) {
          if (!['provider', 'modelId', 'contextWindow', 'inputModalities', 'reasoningEfforts', 'defaultReasoningEffort'].includes(key)) {
            return send({ status: 400, body: { error: `unknown model settings field: ${key}` } });
          }
        }
        const { provider, modelId } = request;
        if (!provider || !modelId) return send({ status: 400, body: { error: 'provider and modelId are required' } });
        if (provider === 'openai' || provider === 'combo') {
          return send({ status: 400, body: { error: 'model settings are only available for routed providers' } });
        }
        if (!state.modelSettings[provider]) return send({ status: 400, body: { error: 'unknown model settings provider' } });
        const stored = state.modelSettings[provider][modelId]
          ?? { contextWindow: null, inputModalities: null, reasoningEfforts: null, defaultReasoningEffort: null };
        const next = { ...stored };
        if (request.contextWindow !== undefined) next.contextWindow = request.contextWindow;
        if (request.inputModalities !== undefined) {
          next.inputModalities = request.inputModalities === null || request.inputModalities.length === 0
            ? null : [...new Set(request.inputModalities)];
        }
        if (request.reasoningEfforts !== undefined) next.reasoningEfforts = request.reasoningEfforts;
        if (request.defaultReasoningEffort !== undefined) next.defaultReasoningEffort = request.defaultReasoningEffort;
        const changed = JSON.stringify(next) !== JSON.stringify(stored);
        state.modelSettings[provider][modelId] = next;
        const hasOverrides = Object.values(next).some((value) => value !== null);
        const receipt = {
          ok: true, provider, modelId, hasOverrides,
          contextWindow: next.contextWindow, inputModalities: next.inputModalities,
          reasoningEfforts: next.reasoningEfforts, defaultReasoningEffort: next.defaultReasoningEffort,
        };
        if (scenario === 'lost_response' && !lostResponseConsumed) {
          // The write landed; the response never arrives. The bridge
          // must reconcile by reading, never by re-sending.
          lostResponseConsumed = true;
          res.socket.destroy();
          return undefined;
        }
        if (!changed) {
          return send({
            status: 200,
            body: { ...receipt, changed: false, saved: false, catalogRefresh: { status: 'skipped', reason: 'not-requested', retryable: false } },
          });
        }
        const catalogRefresh = scenario === 'refresh_failed'
          ? { status: 'failed', reason: 'internal', retryable: true }
          : { status: 'committed', retryable: false };
        return send({ status: 200, body: { ...receipt, changed: true, saved: true, catalogRefresh } });
      }

      // --- sub-agent surface -------------------------------------------------
      if (pathname === '/api/v2' && req.method === 'GET') return send({ status: 200, body: v2Body() });
      if (pathname === '/api/v2' && req.method === 'PUT') {
        const settings = body ?? {};
        if (Object.keys(settings).length === 0) return send({ status: 400, body: { error: 'body must set at least one field' } });
        const warnings = [];
        if (settings.multiAgentMode !== undefined) {
          state.v2.multiAgentMode = settings.multiAgentMode === 'default' ? 'v1' : settings.multiAgentMode;
          warnings.push(`Multi-agent mode set to '${settings.multiAgentMode}'. Applies to new sessions.`);
        }
        for (const key of ['enabled', 'maxConcurrentThreadsPerSession', 'keepNativeChatGptOnV1',
          'agentsEnabled', 'agentsMaxDepth', 'subagentDeveloperInstructions', 'multiAgentModeHintText']) {
          if (settings[key] !== undefined) state.v2[key] = settings[key];
        }
        if (settings.enabled !== undefined) {
          warnings.push('Applies to new sessions; restart the Codex app or wait out its picker cache to see the ladder change.');
        }
        return send({ status: 200, body: v2Body({ ok: true, warnings, catalogRefresh: { status: 'committed', retryable: false } }) });
      }
      if (pathname === '/api/injection-model' && req.method === 'GET') {
        return send({
          status: 200,
          body: {
            multiAgentGuidanceEnabled: state.injectionModel.multiAgentGuidanceEnabled,
            syncCodexSubagentDefaults: state.injectionModel.syncCodexSubagentDefaults,
            model: state.injectionModel.model, effort: state.injectionModel.effort,
            prompt: state.injectionModel.prompt, efforts: EFFORTS, available: [],
          },
        });
      }
      if (pathname === '/api/injection-model' && req.method === 'PUT') {
        const settings = body ?? {};
        if (settings.effort !== undefined && settings.effort !== null && settings.effort !== ''
          && !EFFORTS.includes(settings.effort)) {
          return send({ status: 400, body: { error: `unknown reasoning effort "${settings.effort}"` } });
        }
        for (const key of ['multiAgentGuidanceEnabled', 'syncCodexSubagentDefaults', 'model', 'effort', 'prompt']) {
          if (settings[key] !== undefined) {
            state.injectionModel[key] = settings[key] === '' ? null : settings[key];
          }
        }
        if (!state.injectionModel.model) {
          state.injectionModel.effort = null;
          state.injectionModel.syncCodexSubagentDefaults = false;
        }
        return send({
          status: 200,
          body: {
            ok: true,
            multiAgentGuidanceEnabled: state.injectionModel.multiAgentGuidanceEnabled,
            syncCodexSubagentDefaults: state.injectionModel.syncCodexSubagentDefaults,
            model: state.injectionModel.model, effort: state.injectionModel.effort,
            prompt: state.injectionModel.prompt,
          },
        });
      }
      if (pathname === '/api/effort-caps' && req.method === 'GET') {
        return send({
          status: 200,
          body: { effortCap: state.effortCaps.effortCap, subagentEffortCap: state.effortCaps.subagentEffortCap, modelPinnedEfforts: {}, efforts: EFFORTS },
        });
      }
      if (pathname === '/api/effort-caps' && req.method === 'PUT') {
        const patch = body ?? {};
        for (const key of ['effortCap', 'subagentEffortCap']) {
          if (patch[key] !== undefined) {
            if (patch[key] !== null && patch[key] !== '' && !EFFORTS.includes(patch[key])) {
              return send({ status: 400, body: { error: 'caps must be valid reasoning efforts or null' } });
            }
            state.effortCaps[key] = patch[key] === '' ? null : patch[key];
          }
        }
        return send({
          status: 200,
          body: { ok: true, effortCap: state.effortCaps.effortCap, subagentEffortCap: state.effortCaps.subagentEffortCap },
        });
      }
      if (pathname === '/api/subagent-models' && req.method === 'GET') {
        return send({
          status: 200,
          body: { chosen: state.subagentModels, available: [], pickerOrder: [], pickerOrderMode: null },
        });
      }
      if (pathname === '/api/subagent-models' && req.method === 'PUT') {
        const models = Array.isArray(body?.models) ? body.models.slice(0, 5) : [];
        state.subagentModels = models;
        return send({
          status: 200,
          body: { ok: true, applied: models, pickerOrder: [], pickerOrderMode: null, catalogRefresh: { status: 'committed', retryable: false } },
        });
      }
      if (pathname === '/api/subagent-model-fallback' && req.method === 'GET') {
        return send({
          status: 200,
          body: { models: state.subagentModelFallback.models, pollMs: state.subagentModelFallback.pollMs, available: [] },
        });
      }
      if (pathname === '/api/subagent-model-fallback' && req.method === 'PUT') {
        const patch = body ?? {};
        if (patch.models !== undefined) state.subagentModelFallback.models = patch.models;
        if (patch.pollMs !== undefined) {
          state.subagentModelFallback.pollMs = patch.pollMs === null ? 60000 : patch.pollMs;
        }
        return send({
          status: 200,
          body: { ok: true, models: state.subagentModelFallback.models, pollMs: state.subagentModelFallback.pollMs },
        });
      }

      // --- client integrations ----------------------------------------------
      if (pathname === '/api/client-integrations' && req.method === 'GET') {
        const clients = Object.keys(state.integrations).map((clientId) => integrationStateBody(clientId));
        clients.push(asideListBody());
        return send({ status: 200, body: { clients } });
      }
      if (pathname === '/api/client-integrations/journal' && req.method === 'GET') {
        const client = url.searchParams.get('client');
        const operations = journal.filter((row) => !client || row.clientId === client);
        return send({ status: 200, body: { operations } });
      }
      if (pathname === '/api/client-integrations/preview' && req.method === 'POST') {
        const { clientId, operation } = body ?? {};
        if (clientId === 'aside') {
          return send({ status: 400, body: { error: 'Use the canonical Aside profile path', code: 'invalid_aside_profile_path' } });
        }
        if (!state.integrations[clientId] || !['apply', 'overwrite', 'disable'].includes(operation)) {
          return send({ status: 400, body: { error: 'invalid client or operation', code: 'invalid_client' } });
        }
        if (scenario === 'preview_unavailable') {
          return send({ status: 409, body: { error: 'integration preview unavailable', code: 'integration_preview_unavailable' } });
        }
        return send({ status: 200, body: planFor(clientId, clientId, operation, state.integrations[clientId].state) });
      }
      if (pathname === '/api/client-integrations/restore/preview' && req.method === 'POST') {
        const op = journal.find((row) => row.opId === body?.opId);
        if (!op) {
          return send({ status: 404, body: { error: 'integration operation not found', code: 'integration_operation_not_found', opId: body?.opId } });
        }
        if (scenario === 'preview_unavailable') {
          return send({ status: 409, body: { error: 'integration preview unavailable', code: 'integration_preview_unavailable' } });
        }
        return send({
          status: 200,
          body: planFor(op.clientId, op.clientId, 'restore', state.integrations[op.clientId].state),
        });
      }
      if (pathname === '/api/client-integrations/restore' && req.method === 'POST') {
        const op = journal.find((row) => row.opId === body?.opId);
        if (!op) {
          return send({ status: 404, body: { error: 'integration operation not found', code: 'integration_operation_not_found', opId: body?.opId } });
        }
        maybeInjectDrift(body ?? {});
        const freshPlan = planFor(op.clientId, op.clientId, 'restore', state.integrations[op.clientId].state);
        if (bindingGuard(send, body ?? {}, 'restore', freshPlan)) return undefined;
        const entry = state.integrations[op.clientId];
        entry.state = op.kind === 'disable' ? 'current' : 'absent';
        entry.snapshotCount += 1;
        const opId = recordApply(op.clientId, 'restore');
        entry.lastOpId = opId;
        return send({
          status: 200,
          body: { ok: true, clientId: op.clientId, changed: true, state: entry.state, opId, message: 'fixture restored' },
        });
      }

      // --- Aside profiles ------------------------------------------------------
      if (pathname === '/api/client-integrations/aside/profiles' && req.method === 'GET') {
        return send({ status: 200, body: asideListBody() });
      }
      if (pathname === '/api/client-integrations/aside/profiles' && req.method === 'PUT') {
        // Bulk set: upstream refuses plan bindings here ("a confirmed
        // plan applies to one profile"), which is why the bridge never
        // sends this request. The fake still serves it so the
        // selftest can pin the 200/207 partial-envelope contract the
        // module's per-element parser is built against.
        const enabled = body?.enabled === true;
        const results = Object.entries(state.asideProfiles).map(([id, entry]) => {
          const profileId = Number(id);
          if (scenario === 'partial' && profileId === 2) {
            return {
              ok: false, clientId: 'aside', profileId, reason: 'conflict', state: entry.state,
              message: 'fixture conflict', snapshotPath: '/fixture/aside/2/snapshot', residual: false,
            };
          }
          entry.enabled = enabled;
          entry.state = enabled ? 'current' : 'absent';
          return {
            ok: true, clientId: 'aside', profileId, changed: true, state: entry.state,
            message: 'fixture applied',
          };
        });
        const ok = results.every((result) => result.ok);
        stateVersion += 1;
        return send({
          status: ok ? 200 : 207,
          body: {
            ok, clientId: 'aside', changed: true,
            state: ok ? (enabled ? 'current' : 'absent') : 'conflict',
            message: ok ? 'fixture applied' : 'fixture partial', results,
          },
        });
      }
      const asideMatch = pathname.match(/^\/api\/client-integrations\/aside\/profiles\/(\d+)(\/preview|\/restore)?$/);
      if (asideMatch) {
        const profileId = Number(asideMatch[1]);
        const entry = state.asideProfiles[String(profileId)];
        if (!entry) return send({ status: 404, body: { error: 'unregistered profile', code: 'unknown_profile' } });
        const action = asideMatch[2];
        if (req.method === 'GET' && !action) {
          return send({
            status: 200,
            body: {
              profileId, enabled: entry.enabled, state: entry.state, installed: entry.installed,
              configPath: `/fixture/aside/${profileId}/config.toml`,
              snapshotCount: entry.snapshotCount, retentionDegraded: false,
            },
          });
        }
        if (req.method === 'POST' && action === '/preview') {
          const operation = body?.operation;
          if (!['apply', 'overwrite', 'disable', 'restore'].includes(operation)) {
            return send({ status: 400, body: { error: 'operation must be apply, overwrite, disable or restore', code: 'invalid_preview_operation' } });
          }
          if (operation === 'restore' && !journal.some((row) => row.opId === body?.opId)) {
            return send({ status: 404, body: { error: 'integration operation not found', code: 'integration_operation_not_found' } });
          }
          if (scenario === 'preview_unavailable') {
            return send({ status: 409, body: { error: 'integration preview unavailable', code: 'integration_preview_unavailable' } });
          }
          return send({
            status: 200,
            body: planFor(`aside-${profileId}`, 'aside', operation, entry.state, { profileId }),
          });
        }
        if (req.method === 'PUT' && !action) {
          const enabled = body?.enabled === true;
          const expected = enabled ? (body?.overwriteConflict === true ? 'overwrite' : 'apply') : 'disable';
          maybeInjectDrift(body ?? {});
          const freshPlan = planFor(`aside-${profileId}`, 'aside', expected, entry.state, { profileId });
          if (bindingGuard(send, body ?? {}, expected, freshPlan)) return undefined;
          if (scenario === 'partial' && profileId === 2) {
            return send({
              status: 409,
              body: {
                error: 'integration config conflicts with ownership record', code: 'integration_conflict',
                clientId: 'aside', state: 'conflict', reason: 'conflict', message: 'fixture conflict',
                snapshotPath: '/fixture/aside/2/snapshot', residual: false,
              },
            });
          }
          entry.enabled = enabled;
          entry.state = enabled ? 'current' : 'absent';
          entry.snapshotCount += 1;
          const opId = recordApply('aside', expected);
          return send({
            status: 200,
            body: { ok: true, clientId: 'aside', profileId, changed: true, state: entry.state, opId, message: 'fixture applied' },
          });
        }
        if (req.method === 'POST' && action === '/restore') {
          const op = journal.find((row) => row.opId === body?.opId);
          if (!op) {
            return send({ status: 404, body: { error: 'integration operation not found', code: 'integration_operation_not_found' } });
          }
          maybeInjectDrift(body ?? {});
          const freshPlan = planFor(`aside-${profileId}`, 'aside', 'restore', entry.state, { profileId });
          if (bindingGuard(send, body ?? {}, 'restore', freshPlan)) return undefined;
          entry.state = op.kind === 'disable' ? 'current' : 'absent';
          entry.enabled = entry.state === 'current';
          entry.snapshotCount += 1;
          const opId = recordApply('aside', 'restore');
          return send({
            status: 200,
            body: { ok: true, clientId: 'aside', profileId, changed: true, state: entry.state, opId, message: 'fixture restored' },
          });
        }
      }

      const clientMatch = pathname.match(/^\/api\/client-integrations\/([a-z-]+)$/);
      if (clientMatch) {
        const clientId = clientMatch[1];
        if (!state.integrations[clientId]) return send(files.notFound);
        if (req.method === 'GET') return send({ status: 200, body: integrationStateBody(clientId) });
        if (req.method === 'PUT') {
          const enabled = body?.enabled === true;
          const expected = enabled ? (body?.overwriteConflict === true ? 'overwrite' : 'apply') : 'disable';
          maybeInjectDrift(body ?? {});
          const freshPlan = planFor(clientId, clientId, expected, state.integrations[clientId].state);
          if (bindingGuard(send, body ?? {}, expected, freshPlan)) return undefined;
          const entry = state.integrations[clientId];
          entry.state = enabled ? 'current' : 'absent';
          entry.appliedAt = new Date().toISOString();
          entry.snapshotCount += 1;
          const opId = recordApply(clientId, expected);
          entry.lastOpId = opId;
          return send({
            status: 200,
            body: { ok: true, clientId, changed: true, state: entry.state, opId, message: 'fixture applied' },
          });
        }
      }

      return send(files.notFound);
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    server,
    requests,
    endpoint: `http://127.0.0.1:${port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}
