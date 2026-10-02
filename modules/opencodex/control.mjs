// Pure helpers for the OpenCodex provider-service module: response
// validation/normalisation to the module's snapshot schema, version
// comparison, redaction and honest-unknown mapping. No I/O — the fixture
// selftest exercises these through the real adapter in bridge.mjs.
//
// Contracts come from the pinned upstream lidge-jun/opencodex v2.73.0
// (commit 569e3e7dae48bafc54b8a1a7e3a85129befe2d98): the Management API is
// documented in the pin's docs-site management-api reference and
// src/server/management/system-routes.ts. Upstream responses are
// untrusted display data: normalisers copy only named scalar fields and
// never pass a raw body through.

export function isObject(value) {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function num(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

export function str(value) {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

export function bool(value) {
  return typeof value === 'boolean' ? value : null;
}

/// A degraded snapshot section. A failed or absent read is an unknown,
// never an empty healthy list.
export function unknownSection(reason, extra = {}) {
  return { state: 'unknown', reason, ...extra };
}

/// Map an upstream HTTP failure to a recorded reason. The upstream `code`
/// (e.g. sibling_instance, catalog_busy) is preserved as evidence; the
/// reason vocabulary is this module's own.
export function mapHttpError(status, body) {
  const code = isObject(body) ? str(body.code) : null;
  let reason;
  if (status === 401) reason = 'admin_auth_rejected';
  else if (status === 403) reason = 'origin_blocked';
  else if (status === 404) reason = 'not_found';
  else if (status === 409) reason = code === 'sibling_instance' ? 'sibling_instance' : 'conflict';
  else if (status === 413) reason = 'body_too_large';
  else if (status === 503) reason = code === 'catalog_busy' ? 'catalog_busy' : 'management_unavailable';
  else reason = `http_${status}`;
  return { reason, code };
}

/// Exact string comparison, per the audit: any difference is unknown,
/// never a failure and never a pass. The version-string format of a live
/// service is not verified against the release tag.
export function compareVersion(observed, expected) {
  if (!str(observed) || !str(expected)) return 'unknown';
  return observed === expected ? 'match' : 'mismatch';
}

/// Billing is derived ONLY from the provider's authMode (issue #1, audit
/// §D): forward spends the ChatGPT plan behind the caller's Codex login,
/// oauth spends that provider account's subscription, key spends the
/// key's own account. A routed Claude model on the anthropic provider
/// with oauth is the operator's Claude subscription via OpenCodex's
/// stored login — upstream reports no subscription tier, so no label
/// produced here ever claims Pro/Max, and it is never the controller's
/// native Claude route.
export function billingFor(authMode, providerId) {
  if (authMode === 'forward') return 'chatgpt-plan-via-codex-login';
  if (authMode === 'oauth') {
    return providerId === 'anthropic'
      ? 'claude-subscription-via-opencodex'
      : 'provider-subscription-login';
  }
  if (authMode === 'key') return 'api-key-account';
  return 'unknown';
}

function pickScalars(value) {
  if (!isObject(value)) return null;
  const out = {};
  for (const [key, entry] of Object.entries(value)) {
    if (typeof entry === 'number' && Number.isFinite(entry)) out[key] = entry;
    else if (typeof entry === 'boolean') out[key] = entry;
    else if (typeof entry === 'string' && entry.length <= 128) out[key] = entry;
  }
  return out;
}

/// GET /api/system/health — source-only contract at the pin
/// (system-routes.ts). spendLedger is forwarded only as its six pinned
/// scalars from spendLedgerDiagnosticsSnapshot().
export function normalizeHealth(body) {
  if (!isObject(body)) return null;
  const ledger = isObject(body.spendLedger) ? body.spendLedger : null;
  return {
    status: str(body.status),
    service: str(body.service),
    version: str(body.version),
    uptimeSeconds: num(body.uptime),
    pid: num(body.pid),
    spendLedger: ledger
      ? {
          ownership: str(ledger.ownership),
          initialized: bool(ledger.initialized),
          configured: bool(ledger.configured),
          degraded: bool(ledger.degraded),
          persistFailures: num(ledger.persistFailures),
          corruptRecords: num(ledger.corruptRecords),
        }
      : null,
  };
}

/// GET /api/system/memory — scalar-only by upstream design.
export function normalizeMemory(body) {
  if (!isObject(body)) return null;
  const watchdog = isObject(body.watchdog)
    ? {
        warnThresholdBytes: num(body.watchdog.warnThresholdBytes),
        lastWarnAt: num(body.watchdog.lastWarnAt),
        observedBytes: num(body.watchdog.observedBytes),
        observedMetric: str(body.watchdog.observedMetric),
        samples: Array.isArray(body.watchdog.samples) ? body.watchdog.samples.length : 0,
      }
    : null;
  return {
    pid: num(body.pid),
    bunVersion: str(body.bunVersion),
    bunRevision: str(body.bunRevision),
    // Absent means the service predates the launch marker: the caller
    // reports this field unknown, never a guess and never an empty string.
    bunRuntimeSource: str(body.bunRuntimeSource),
    platform: str(body.platform),
    uptimeSeconds: num(body.uptimeSeconds),
    rssBytes: num(body.rss),
    heapUsed: num(body.heapUsed),
    heapTotal: num(body.heapTotal),
    external: num(body.external),
    arrayBuffers: num(body.arrayBuffers),
    observedBytes: num(body.observedBytes),
    observedMetric: str(body.observedMetric),
    jscHeap:
      body.jscHeap === null || body.jscHeap === undefined
        ? null
        : pickScalars(body.jscHeap),
    responseState: pickScalars(body.responseState),
    responseSpill: pickScalars(body.responseSpill),
    appOwnedBytes: num(body.appOwnedBytes),
    inspectionCounters: pickScalars(body.inspectionCounters),
    streamMode: str(body.streamMode),
    eagerRelay: bool(body.eagerRelay),
    watchdog,
    // The proxy's own in-flight inference count. Zero is NOT proof that
    // native Codex children stopped; the value is forwarded as observed.
    activeTurnCount: num(body.activeTurnCount),
    // An upstream-owned drain observation; it never authorises ELIOT to
    // restart anything.
    isDraining: bool(body.isDraining),
  };
}

function rowsOf(body, keys) {
  if (Array.isArray(body)) return body;
  if (isObject(body)) {
    for (const key of keys) {
      if (Array.isArray(body[key])) return body[key];
    }
  }
  return null;
}

/// GET /api/providers — redacted provider config + discovery state.
export function normalizeProviders(body) {
  const rows = rowsOf(body, ['providers', 'data']);
  if (!rows) return null;
  const providers = [];
  for (const row of rows) {
    if (!isObject(row)) continue;
    const id = str(row.id) ?? str(row.name);
    if (!id) continue;
    providers.push({
      id,
      adapter: str(row.adapter),
      state: str(row.state) ?? str(row.discoveryState) ?? str(row.status),
      authMode: str(row.authMode),
    });
  }
  return providers;
}

/// GET /api/protocols?provider=<name> — adapter, adapterSource, authMode,
/// upstream; the billing-evidence read. No credential, no base URL is
/// served by this route at the pin.
export function normalizeProtocol(body) {
  if (!isObject(body)) return null;
  return {
    name: str(body.name),
    adapter: str(body.adapter),
    adapterSource: str(body.adapterSource),
    authMode: str(body.authMode),
    upstream: str(body.upstream),
    modelOverrides: Array.isArray(body.modelOverrides) ? body.modelOverrides.length : null,
    modelOverridesTruncated: bool(body.modelOverridesTruncated),
  };
}

/// GET /api/models — only the fields named in the pin's docs are kept;
/// the full row schema was not read from source. Stored declared context
/// window stays separate from the effective one; cacheHitRate is null
/// (never 0) when there is no cache telemetry.
export function normalizeModels(body) {
  const rows = rowsOf(body, ['models', 'data']);
  if (!rows) return null;
  const models = [];
  for (const row of rows) {
    if (!isObject(row)) continue;
    const id = str(row.id) ?? str(row.model);
    if (!id) continue;
    models.push({
      id,
      provider: str(row.provider) ?? str(row.providerId),
      contextWindowDeclared: num(row.contextWindowDeclared),
      contextWindow: num(row.contextWindow),
      cacheHitRate: num(row.cacheHitRate),
    });
  }
  return models;
}

function uniqueStrings(value, single) {
  const out = [];
  const push = (entry) => {
    const text = str(entry);
    if (text && !out.includes(text)) out.push(text);
  };
  if (Array.isArray(value)) for (const entry of value) push(entry);
  push(single);
  return out;
}

/// GET /api/usage — compact aggregates over the upstream append-only
/// ledger. servedModel/wireModel evidence is kept only where an upstream
/// record provides it; a row without servedModel stays without it (never
/// backfilled from the requested model). Flat per-provider attempts are
/// kept as reported; a parent combo total is never added on top.
export function normalizeUsage(body) {
  if (!isObject(body)) return null;
  let rows = rowsOf(body, ['providers', 'data']);
  if (!rows && isObject(body.providers)) {
    rows = Object.entries(body.providers).map(([provider, row]) => ({
      ...(isObject(row) ? row : {}),
      provider,
    }));
  }
  const providers = [];
  if (rows) {
    for (const row of rows) {
      if (!isObject(row)) continue;
      const provider = str(row.provider) ?? str(row.id) ?? str(row.name);
      if (!provider) continue;
      providers.push({
        provider,
        attempts: num(row.attempts) ?? num(row.attemptCount) ?? num(row.requests),
        servedModels: uniqueStrings(row.servedModels, row.servedModel),
        wireModels: uniqueStrings(row.wireModels, row.wireModel),
      });
    }
  }
  return {
    usageIncomplete: bool(body.usageIncomplete),
    usageIncompleteReason: str(body.usageIncompleteReason),
    estimatedCostUsd: num(body.estimatedCostUsd),
    providers,
    accounts: isObject(body.accounts) ? pickScalarsShallow(body.accounts) : null,
  };
}

function pickScalarsShallow(value) {
  const out = {};
  for (const [key, entry] of Object.entries(value)) {
    if (isObject(entry)) out[key] = pickScalars(entry) ?? {};
    else if (typeof entry === 'number' && Number.isFinite(entry)) out[key] = entry;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Slice 2: configuration-operation normalisers. Same rule as slice 1:
// upstream responses are untrusted display data; only named fields are
// copied, upstream free-text messages are never forwarded (writer
// messages can name files and backup locations), and file paths served
// by the integration routes (configPath, snapshotPath, conflictPaths)
// are reduced to booleans/counts or dropped entirely.

export function stringList(value, limit = 64) {
  if (!Array.isArray(value)) return null;
  const out = [];
  for (const entry of value) {
    const text = str(entry);
    if (text) out.push(text);
    if (out.length >= limit) break;
  }
  return out;
}

export function normalizeCatalogRefresh(value) {
  if (!isObject(value)) return null;
  return {
    status: str(value.status),
    reason: str(value.reason),
    retryable: bool(value.retryable),
  };
}

/// GET /api/protocols and the PATCH /api/protocols/settings response
/// share one shape (protocolInfo at the pin): resolved surfaces +
/// resolved settings. Readback compares against these resolved values.
export function normalizeProtocolSettings(body) {
  if (!isObject(body) || !isObject(body.settings) || !isObject(body.surfaces)) return null;
  const rollout = isObject(body.settings.rollout) ? body.settings.rollout : {};
  return {
    messagesEnabled: bool(isObject(body.surfaces.messages) ? body.surfaces.messages.enabled : null),
    unrepresentable: str(body.settings.unrepresentable),
    rollout: {
      nativeChatCombos: bool(rollout.nativeChatCombos),
      managedMessagesNative: bool(rollout.managedMessagesNative),
      managedMessagesNativeOAuth: bool(rollout.managedMessagesNativeOAuth),
      directEncoders: bool(rollout.directEncoders),
      shadowPlan: bool(rollout.shadowPlan),
    },
    policyRevision: str(body.policyRevision),
  };
}

/// PUT /api/model-settings receipt: the stored-state echo. `saved`
/// reports whether THIS request published config; a no-op answers
/// changed:false/saved:false with the stored state still reported.
export function normalizeModelSettingsReceipt(body) {
  if (!isObject(body) || body.ok !== true) return null;
  return {
    provider: str(body.provider),
    modelId: str(body.modelId),
    changed: bool(body.changed),
    saved: bool(body.saved),
    hasOverrides: bool(body.hasOverrides),
    contextWindow: num(body.contextWindow),
    inputModalities: stringList(body.inputModalities, 8),
    reasoningEfforts: stringList(body.reasoningEfforts, 16),
    defaultReasoningEffort: str(body.defaultReasoningEffort),
    catalogRefresh: normalizeCatalogRefresh(body.catalogRefresh),
  };
}

/// The stored-declaration evidence for one routed model out of
/// GET /api/models: contextWindowDeclared and inputModalitiesDeclared
/// are the stored declarations; reasoningEfforts/defaultReasoningEffort
/// on a row are EFFECTIVE (resolved) values, reported as observed.
export function findModelRowSettings(body, provider, modelId) {
  const rows = rowsOf(body, ['models', 'data']);
  if (!rows) return null;
  for (const row of rows) {
    if (!isObject(row)) continue;
    const rowProvider = str(row.provider) ?? str(row.providerId);
    const id = str(row.id) ?? str(row.model);
    if (rowProvider !== provider) continue;
    if (id !== modelId && id !== `${provider}/${modelId}` && str(row.namespaced) !== `${provider}/${modelId}`) continue;
    return {
      found: true,
      contextWindowDeclared: num(row.contextWindowDeclared),
      inputModalitiesDeclared: stringList(row.inputModalitiesDeclared, 8),
      reasoningEfforts: stringList(row.reasoningEfforts, 16),
      defaultReasoningEffort: str(row.defaultReasoningEffort),
      reasoningOverridden: bool(row.reasoningOverridden),
    };
  }
  return { found: false };
}

const INTEGRATION_STATES = ['absent', 'current', 'stale', 'conflict', 'unsafe'];

export function integrationState(value) {
  const text = str(value);
  return text && INTEGRATION_STATES.includes(text) ? text : null;
}

/// An integration mutation plan. changes[] paths are managed schema
/// paths or $snapshot/$ownership/$journal markers — never file
/// locations — so they are copied; the fingerprint is an opaque
/// optimistic-check string the operator confirms with, not a secret.
export function normalizeIntegrationPlan(body) {
  if (!isObject(body)) return null;
  const changes = Array.isArray(body.changes)
    ? body.changes
        .filter((entry) => isObject(entry))
        .slice(0, 64)
        .map((entry) => ({ kind: str(entry.kind), path: str(entry.path) }))
    : null;
  return {
    version: num(body.version),
    clientId: str(body.clientId),
    operation: str(body.operation),
    state: integrationState(body.state),
    foreignEdit: str(body.foreignEdit),
    canApply: bool(body.canApply),
    willChange: bool(body.willChange),
    refusalReason: str(body.refusalReason),
    profileId: num(body.profileId),
    fingerprint: str(body.fingerprint),
    changes,
  };
}

/// One client-integration state (single read or one list entry). The
/// upstream record also carries file locations (configPath and
/// friends); those are never copied.
export function normalizeIntegrationState(body) {
  if (!isObject(body)) return null;
  const clientId = str(body.clientId);
  if (!clientId) return null;
  if (clientId === 'aside' && Array.isArray(body.profiles)) {
    // The collection read embeds the Aside aggregate in place of a
    // single-file state; only counts are kept, never profile rows.
    return {
      clientId,
      state: integrationState(body.state),
      installed: bool(body.installed),
      profilesTotal: num(body.total),
      profilesEnabled: num(body.enabledCount),
      profilesApplied: num(body.appliedCount),
      snapshotCount: num(body.snapshotCount),
      retentionDegraded: bool(body.retentionDegraded),
    };
  }
  return {
    clientId,
    state: integrationState(body.state),
    installed: bool(body.installed),
    appliedAt: str(body.appliedAt),
    lastOpId: str(body.lastOpId),
    reason: str(body.reason),
    snapshotCount: num(body.snapshotCount),
    retentionDegraded: bool(body.retentionDegraded),
  };
}

export function normalizeIntegrationStates(body) {
  if (!isObject(body) || !Array.isArray(body.clients)) return null;
  return body.clients.map((entry) => normalizeIntegrationState(entry)).filter(Boolean).slice(0, 32);
}

/// GET /api/client-integrations/aside/profiles — aggregate counts only;
/// profile rows carry account identifiers and stay upstream.
export function normalizeAsideProfiles(body) {
  if (!isObject(body) || !Array.isArray(body.profiles)) return null;
  return {
    total: num(body.total),
    enabledCount: num(body.enabledCount),
    appliedCount: num(body.appliedCount),
    allEnabled: bool(body.allEnabled),
    state: integrationState(body.state),
  };
}

/// One Aside profile's state for an operator-selected profile readback.
export function normalizeAsideProfileState(body) {
  if (!isObject(body)) return null;
  return {
    profileId: num(body.profileId),
    enabled: bool(body.enabled),
    state: integrationState(body.state),
    installed: bool(body.installed),
    snapshotCount: num(body.snapshotCount),
    retentionDegraded: bool(body.retentionDegraded),
  };
}

/// A single integration write outcome (toggle PUT, restore POST, one
/// Aside profile PUT) or a writer-refusal error body. snapshotPath is
/// reduced to a boolean: a recoverable snapshot exists or not — its
/// location is never recorded.
export function normalizeIntegrationOutcome(body) {
  if (!isObject(body)) return null;
  return {
    ok: typeof body.ok === 'boolean' ? body.ok : null,
    clientId: str(body.clientId),
    profileId: num(body.profileId),
    changed: bool(body.changed),
    state: integrationState(body.state),
    opId: str(body.opId),
    reason: str(body.reason),
    code: str(body.code),
    residual: bool(body.residual),
    snapshotRecorded: typeof body.snapshotPath === 'string' ? true : null,
  };
}

/// Per-element outcomes of a partial envelope: `results[]` (Aside
/// mutation/sync envelopes) or `clientIntegrations[]` (catalog
/// convergence on visibility/selection writes). HTTP 200 with ok:true
/// on the envelope confirms the save only; each element is judged on
/// its own fields, and an element missing outcome fields does not
/// establish success.
export function normalizeIntegrationOutcomes(body) {
  if (!isObject(body)) return null;
  const rows = Array.isArray(body.results) ? body.results
    : Array.isArray(body.clientIntegrations) ? body.clientIntegrations
    : null;
  if (!rows) return null;
  const out = [];
  for (const row of rows) {
    if (!isObject(row)) continue;
    out.push({
      client: str(row.client) ?? str(row.clientId),
      profileId: num(row.profileId),
      ok: typeof row.ok === 'boolean' ? row.ok : null,
      changed: bool(row.changed),
      state: integrationState(row.state) ?? str(row.state),
      reason: str(row.reason) ?? str(row.refusalReason),
      residual: bool(row.residual),
      snapshotRecorded: typeof row.snapshotPath === 'string' ? true : null,
    });
    if (out.length >= 64) break;
  }
  return out;
}

/// The upstream integration journal (rollback source). configPath is
/// dropped; opId/kind/snapshot/undoable/deletable are the facts an
/// operator decides a restore from.
export function normalizeIntegrationJournal(body) {
  if (!isObject(body) || !Array.isArray(body.operations)) return null;
  return body.operations.slice(0, 64).map((row) => (isObject(row) ? {
    opId: str(row.opId),
    clientId: str(row.clientId),
    kind: str(row.kind),
    at: str(row.at),
    snapshot: str(row.snapshot),
    undoable: bool(row.undoable),
    deletable: bool(row.deletable),
    profileId: num(row.profileId),
  } : null)).filter(Boolean);
}

/// GET/PUT /api/v2 shared state. The two free-text fields
/// (subagentDeveloperInstructions, multiAgentModeHintText) are
/// operator-authored prose: only set/unset and length are recorded,
/// never the text.
export function normalizeV2State(body) {
  if (!isObject(body)) return null;
  const text = (value) => (typeof value === 'string'
    ? { set: value.length > 0, length: value.length }
    : { set: false, length: 0 });
  return {
    enabled: bool(body.enabled),
    agentsMaxThreadsConflict: bool(body.agentsMaxThreadsConflict),
    maxConcurrentThreadsPerSession: num(body.maxConcurrentThreadsPerSession),
    multiAgentMode: str(body.multiAgentMode),
    keepNativeChatGptOnV1: bool(body.keepNativeChatGptOnV1),
    agentsEnabled: bool(body.agentsEnabled),
    agentsMaxDepth: num(body.agentsMaxDepth),
    agentsMaxDepthAppliesWhenV2Disabled: bool(body.agentsMaxDepthAppliesWhenV2Disabled),
    subagentDeveloperInstructions: text(body.subagentDeveloperInstructions),
    multiAgentModeHintText: text(body.multiAgentModeHintText),
  };
}

export function normalizeV2Receipt(body) {
  if (!isObject(body) || body.ok !== true) return null;
  return {
    state: normalizeV2State(body),
    warnings: stringList(body.warnings, 16),
    catalogRefresh: normalizeCatalogRefresh(body.catalogRefresh),
  };
}

/// GET/PUT /api/injection-model. The prompt is operator prose: only
/// set/unset is recorded. GET additionally serves the effort ladder
/// and the available-model list; the ladder is kept (it validates an
/// operator's effort before any write), the list is not.
export function normalizeInjectionModel(body) {
  if (!isObject(body)) return null;
  return {
    multiAgentGuidanceEnabled: bool(body.multiAgentGuidanceEnabled),
    syncCodexSubagentDefaults: bool(body.syncCodexSubagentDefaults),
    model: str(body.model),
    effort: str(body.effort),
    promptSet: typeof body.prompt === 'string' ? body.prompt.length > 0 : false,
    efforts: stringList(body.efforts, 16),
  };
}

/// GET/PUT /api/effort-caps. modelPinnedEfforts is reported as a count
/// only — this slice's effort-caps Operation does not write it.
export function normalizeEffortCaps(body) {
  if (!isObject(body)) return null;
  return {
    effortCap: str(body.effortCap),
    subagentEffortCap: str(body.subagentEffortCap),
    modelPinnedEffortsCount: isObject(body.modelPinnedEfforts)
      ? Object.keys(body.modelPinnedEfforts).length
      : null,
    efforts: stringList(body.efforts, 16),
  };
}

/// GET /api/subagent-models (chosen roster) and the PUT receipt
/// (applied roster). Picker-order fields are counted, not copied:
/// this slice's roster Operation does not write picker order.
export function normalizeSubagentModels(body) {
  if (!isObject(body)) return null;
  return {
    chosen: stringList(body.chosen, 8) ?? stringList(body.applied, 8),
    pickerOrderCount: Array.isArray(body.pickerOrder) ? body.pickerOrder.length : null,
    pickerOrderMode: str(body.pickerOrderMode),
    catalogRefresh: normalizeCatalogRefresh(body.catalogRefresh),
  };
}

/// GET/PUT /api/subagent-model-fallback: upstream's own stored chain
/// (ELIOT implements no fallback logic of its own).
export function normalizeSubagentFallback(body) {
  if (!isObject(body)) return null;
  return {
    models: stringList(body.models, 32),
    pollMs: num(body.pollMs),
  };
}

const SECRET_KEY = /token|secret|api[-_]?key|authorization|password|credential/i;

/// Defense in depth for anything the module emits: the admin token is
/// never part of a normalised snapshot by construction (normalisers copy
/// named scalars only); this pass additionally removes any string that
/// contains a known secret value and any credential-shaped field, so a
/// future normaliser change cannot silently leak one.
export function redactValue(value, secrets, depth = 0) {
  if (depth > 8) return null;
  if (typeof value === 'string') {
    let out = value;
    for (const secret of secrets) {
      if (secret && out.includes(secret)) out = '[redacted]';
    }
    return out.length > 512 ? `${out.slice(0, 512)}…` : out;
  }
  if (Array.isArray(value)) return value.map((entry) => redactValue(entry, secrets, depth + 1));
  if (isObject(value)) {
    const out = {};
    for (const [key, entry] of Object.entries(value)) {
      out[key] = SECRET_KEY.test(key) ? '[redacted]' : redactValue(entry, secrets, depth + 1);
    }
    return out;
  }
  return value;
}
