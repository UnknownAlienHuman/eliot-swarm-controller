#!/usr/bin/env node
// OpenCodex provider-service adapter (first slice): attach to an
// explicitly configured, already-running OpenCodex service and perform
// read-only Management API reads. OpenCodex is a provider/protocol
// proxy, not a session owner: execution stays with the native Codex
// backend (C07); this module only observes the service and assembles an
// honest, bounded snapshot for reports and doctor.
//
// Boundaries (issue #1, module contract, audit):
// - The client issues GET requests only. There is no code path in this
//   file that can emit another method, and reads never start, restart,
//   install or reconfigure the service.
// - shutdown is detach only: the client is dropped and the token
//   reference forgotten. An externally owned shared proxy is NEVER
//   stopped, restarted or signalled by this adapter.
// - The Management (admin) credential is a separate credential from any
//   data-plane/proxy admission key and from the Codex app-server token.
//   It is referenced by environment-variable name only, read at attach
//   time, held in memory only, sent only as X-OpenCodex-API-Key, and
//   never passed to Codex/model tools, never persisted, never printed.
// - Failed or absent observation is `unknown`, never an empty healthy
//   fleet; a version mismatch is readiness `unknown`, not a failure and
//   not a pass; activeTurnCount is forwarded as observed (0 is not proof
//   that native Codex children stopped).
// - send/configure/reply are honestly unavailable in this artifact:
//   configuration mutations are a later slice behind saved Operations,
//   and execution belongs to the native Codex backend.
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  billingFor, compareVersion, mapHttpError, normalizeHealth, normalizeMemory,
  normalizeModels, normalizeProtocol, normalizeProviders, normalizeUsage,
  redactValue, str, unknownSection,
} from './control.mjs';

export const MODULE_ARTIFACT_ID = 'opencodex-2.73.0-bridge.1';
export const ENTRYPOINT = 'opencodex_management_api';
export const UPSTREAM = {
  repo: 'lidge-jun/opencodex',
  release: 'v2.73.0',
  commit: '569e3e7dae48bafc54b8a1a7e3a85129befe2d98',
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
  configure: 'unavailable',
  reply: 'unavailable',
  shutdown: 'detach_only',
};

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
  // are four separate facts. Slice 1 accepts only an externally owned
  // service; a binding that claims ELIOT owns the lifecycle is refused.
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

export function createAdapter(config, env = process.env) {
  let token = null;
  let identity = null;

  function resolveToken() {
    const value = env[config.adminTokenEnv];
    return typeof value === 'string' && value.length > 0 ? value : null;
  }

  // The single request helper of this module. The method is a literal:
  // no caller can turn this client into a mutating one.
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

  function evidenceFor(read, result) {
    const entry = { read, state: result.ok ? 'observed' : 'unknown' };
    if (result.status !== null) entry.status = result.status;
    if (result.code) entry.code = result.code;
    if (result.reason) entry.reason = result.reason;
    return entry;
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
      readiness: versionComparison === 'match' && memory ? 'observed' : 'unknown',
      health,
      memory: memory ?? unknownSection(memoryResult.reason ?? 'memory_schema', {
        status: memoryResult.status, code: memoryResult.code,
      }),
      evidence,
    };
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
        models: missing(), usage: missing(), evidence,
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

    const readiness = attached.state !== 'attached'
      ? 'unknown'
      : attached.versionComparison === 'match' ? 'observed' : 'unknown';
    return redactValue({
      ...base, readiness, health, memory, providers, models, usage, evidence,
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
      reason: operation === 'configure'
        ? 'configuration mutations are a later slice behind saved Operations; nothing is written by this artifact'
        : 'execution and replies belong to the native Codex backend (C07); this module only observes the provider service',
    };
  }

  return {
    describe,
    // open performs the attach probe and creates nothing: for an
    // externally owned service there is no session to open.
    open: attach,
    attach,
    snapshot,
    send: () => unavailable('send'),
    configure: () => unavailable('configure'),
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
  const [flag, configPath, operation, ...rest] = argv;
  if (flag !== '--config' || !configPath || !operation || rest.length > 0) {
    console.error('Usage: node bridge.mjs --config <local-module.json> <describe|open|attach|snapshot|send|configure|reply|shutdown>');
    return 2;
  }
  let adapter;
  try {
    adapter = createAdapter(await loadConfig(configPath));
  } catch (error) {
    console.error(JSON.stringify({ error: error.code ?? 'CONFIG_INVALID' }));
    return 2;
  }
  const operations = ['describe', 'open', 'attach', 'snapshot', 'send', 'configure', 'reply', 'shutdown'];
  if (!operations.includes(operation)) {
    console.error(JSON.stringify({ error: 'UNKNOWN_OPERATION' }));
    return 2;
  }
  const result = await adapter[operation]();
  console.log(JSON.stringify({ operation, moduleArtifactId: MODULE_ARTIFACT_ID, result }));
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  process.exitCode = await main(process.argv.slice(2));
}
