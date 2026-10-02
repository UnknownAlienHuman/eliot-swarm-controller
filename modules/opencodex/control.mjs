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
