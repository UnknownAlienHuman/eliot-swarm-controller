#!/usr/bin/env node
// Fixture selftest for the OpenCodex provider-service adapter, run
// against the fake Management API server (fixtures/fake-management-server.mjs).
// No live opencodex service, account or model call is involved: fixtures
// are reconstructions from the pinned v2.73.0 contracts, not captures.
// Run:  node selftest.mjs
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { createAdapter, loadConfig, MODULE_ARTIFACT_ID } from './bridge.mjs';
import { createFakeManagementServer } from './fixtures/fake-management-server.mjs';

const execFileAsync = promisify(execFile);
const here = path.dirname(fileURLToPath(import.meta.url));
const TOKEN = 'fixture-admin-token';
const TOKEN_ENV = 'OPENCODEX_ADMIN_TOKEN_SELFTEST';
process.env[TOKEN_ENV] = TOKEN;

async function configFor(endpoint) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'opencodex-selftest-'));
  const file = path.join(dir, 'module.json');
  await writeFile(file, JSON.stringify({
    endpoint,
    moduleArtifactId: MODULE_ARTIFACT_ID,
    expectedVersion: '2.73.0',
    adminTokenEnv: TOKEN_ENV,
  }));
  return loadConfig(file);
}

async function withServer(scenario, fn) {
  const fake = await createFakeManagementServer({ scenario, token: TOKEN });
  try {
    return await fn(fake, createAdapter(await configFor(fake.endpoint)));
  } finally {
    await fake.close();
  }
}

function assertGetOnly(fake, label) {
  assert.ok(fake.requests.length > 0, `${label}: requests recorded`);
  assert.ok(fake.requests.every((r) => r.method === 'GET'),
    `${label}: only GET requests issued, saw ${JSON.stringify(fake.requests)}`);
  assert.equal(fake.requests.filter((r) => r.path.startsWith('/v1/')).length, 0,
    `${label}: zero data-plane requests`);
}

// 1. Attach + snapshot happy path: every section observed, version match.
await withServer('happy', async (fake, adapter) => {
  const attached = await adapter.attach();
  assert.equal(attached.state, 'attached');
  assert.deepEqual(attached.identity, { endpoint: fake.endpoint, pid: 4242, version: '2.73.0' });
  assert.equal(attached.readiness, 'observed');
  const snap = await adapter.snapshot();
  assert.equal(snap.readiness, 'observed');
  assert.equal(snap.lifecycleOwner, 'external');
  assert.equal(snap.health.version, '2.73.0');
  assert.equal(snap.health.spendLedger.degraded, false);
  assert.equal(snap.health.spendLedger.persistFailures, 0);
  assert.equal(snap.memory.activeTurnCount, 2);
  assert.equal(snap.memory.isDraining, false);
  assert.equal(snap.memory.bunRuntimeSource, 'bun');
  assert.equal(snap.providers.length, 3);
  const anthropic = snap.providers.find((p) => p.id === 'anthropic');
  assert.equal(anthropic.adapterSource, 'hard-pin');
  assert.equal(anthropic.authMode, 'oauth');
  // §D labelling: routed Claude via oauth is the Claude subscription via
  // OpenCodex's stored login — never a Max/native claim, tier unreported.
  assert.equal(anthropic.billing, 'claude-subscription-via-opencodex');
  assert.equal(anthropic.billingTierReported, false);
  assert.equal(snap.providers.find((p) => p.id === 'xai').billing, 'api-key-account');
  assert.equal(snap.providers.find((p) => p.id === 'openai').billing, 'chatgpt-plan-via-codex-login');
  assert.equal(snap.models.length, 3);
  assert.equal(snap.models[0].cacheHitRate, null, 'cacheHitRate stays null without telemetry');
  assert.equal(snap.models[0].contextWindowDeclared, 200000);
  assert.equal(snap.models[0].contextWindow, 180000);
  assert.equal(snap.usage.state, 'observed');
  console.log('PASS happy: attach + snapshot sections observed, honest routed-model labels');
});

// 2 + 11. GET-only guarantee and zero data-plane (model) calls.
await withServer('happy', async (fake, adapter) => {
  await adapter.attach();
  await adapter.snapshot();
  assertGetOnly(fake, 'get-only');
  console.log('PASS get-only: full attach+snapshot cycle issued GET requests only, zero /v1 calls');
});

// 3. Auth form: X-OpenCodex-API-Key from the named env var on every
// request; the token appears nowhere in snapshot or CLI stdout.
await withServer('happy', async (fake, adapter) => {
  const snap = await adapter.snapshot();
  assert.ok(fake.requests.every((r) => r.authed), 'every request carried the admin key header');
  assert.ok(!JSON.stringify(snap).includes(TOKEN), 'token absent from snapshot');
  const dir = await mkdtemp(path.join(os.tmpdir(), 'opencodex-cli-'));
  const file = path.join(dir, 'module.json');
  await writeFile(file, JSON.stringify({
    endpoint: fake.endpoint, moduleArtifactId: MODULE_ARTIFACT_ID,
    expectedVersion: '2.73.0', adminTokenEnv: TOKEN_ENV,
  }));
  const { stdout } = await execFileAsync(process.execPath,
    [path.join(here, 'bridge.mjs'), '--config', file, 'snapshot'],
    { env: { ...process.env } });
  assert.ok(!stdout.includes(TOKEN), 'token absent from bridge CLI stdout');
  assert.equal(JSON.parse(stdout).result.readiness, 'observed');
  console.log('PASS auth: header form correct, token never in snapshot or stdout');
});

// 4. Fail-closed auth: 503 -> unknown/management_unavailable, 401 ->
// unknown/admin_auth_rejected; neither fabricates an empty fleet.
await withServer('fail_closed', async (fake, adapter) => {
  const attached = await adapter.attach();
  assert.equal(attached.state, 'unknown');
  assert.equal(attached.reason, 'management_unavailable');
  const snap = await adapter.snapshot();
  assert.equal(snap.providers.state, 'unknown');
  assert.ok(!Array.isArray(snap.providers), 'failed inventory is not an empty provider list');
});
await withServer('unauthorized', async (fake, adapter) => {
  const attached = await adapter.attach();
  assert.equal(attached.state, 'unknown');
  assert.equal(attached.reason, 'admin_auth_rejected');
  console.log('PASS fail-closed: 503 and 401 map to distinct unknowns, no fabricated fleet');
});

// 5. Sibling guard: 409 sibling_instance surfaces verbatim, no retry storm.
await withServer('sibling', async (fake, adapter) => {
  const attached = await adapter.attach();
  assert.equal(attached.state, 'unknown');
  assert.equal(attached.reason, 'sibling_instance');
  assert.equal(attached.code, 'sibling_instance');
  assert.ok(fake.requests.length <= 2, `no retry storm, saw ${fake.requests.length} requests`);
  console.log('PASS sibling: 409 sibling_instance recorded as unknown without retries');
});

// 6. Version mismatch: readiness unknown, observed version recorded,
// snapshot still produced.
await withServer('version_mismatch', async (fake, adapter) => {
  const snap = await adapter.snapshot();
  assert.equal(snap.readiness, 'unknown');
  assert.equal(snap.versionComparison, 'mismatch');
  assert.equal(snap.observedVersion, '2.74.0-fixture');
  assert.equal(snap.expectedVersion, '2.73.0');
  assert.equal(snap.health.version, '2.74.0-fixture');
  console.log('PASS version-mismatch: readiness unknown with observed version recorded');
});

// 7. Absent launch marker: bunRuntimeSource is unknown, never guessed.
await withServer('no_launch_marker', async (fake, adapter) => {
  const snap = await adapter.snapshot();
  assert.equal(snap.memory.bunRuntimeSource, null);
  assert.notEqual(snap.memory.bunRuntimeSource, '');
  console.log('PASS launch-marker: absent bunRuntimeSource stays null (unknown), not guessed');
});

// 8. Catalog busy: models section unknown, other sections unaffected.
await withServer('catalog_busy', async (fake, adapter) => {
  const snap = await adapter.snapshot();
  assert.equal(snap.models.state, 'unknown');
  assert.equal(snap.models.reason, 'catalog_busy');
  assert.equal(snap.health.version, '2.73.0');
  assert.ok(Array.isArray(snap.providers));
  assert.equal(snap.usage.state, 'observed');
  console.log('PASS catalog-busy: models section degrades alone');
});

// 9. Usage incompleteness preserved; a row without servedModel stays
// without it (no backfill from the requested model).
await withServer('happy', async (fake, adapter) => {
  const snap = await adapter.snapshot();
  assert.equal(snap.usage.usageIncomplete, true);
  assert.equal(snap.usage.usageIncompleteReason, 'oversized_rows');
  assert.equal(snap.usage.estimatedCostLabel,
    'configured-pricing estimate — not an invoice or subscription charge');
  const xai = snap.usage.providers.find((p) => p.provider === 'xai');
  assert.deepEqual(xai.servedModels, [], 'no servedModel evidence stays empty');
  const anthropic = snap.usage.providers.find((p) => p.provider === 'anthropic');
  assert.deepEqual(anthropic.servedModels, ['claude-sonnet-fixture']);
  assert.deepEqual(anthropic.wireModels, ['claude-sonnet-wire-fixture']);
  console.log('PASS usage: incompleteness preserved, servedModel never backfilled');
});

// 10. Detach leaves the service alone: after shutdown the fake is still
// up, its log shows no mutating request, and a fresh attach succeeds.
await withServer('happy', async (fake, adapter) => {
  await adapter.attach();
  const before = fake.requests.length;
  const result = adapter.shutdown();
  assert.deepEqual(result, { outcome: 'detached', serviceTouched: false });
  assert.equal(fake.requests.length, before, 'shutdown issued no request at all');
  const probe = await fetch(`${fake.endpoint}/api/system/health`);
  assert.equal(probe.status, 401, 'service still up (answers, rejecting the unauthenticated probe)');
  const fresh = createAdapter(await configFor(fake.endpoint));
  assert.equal((await fresh.attach()).state, 'attached');
  assertGetOnly(fake, 'detach');
  console.log('PASS detach: service untouched and still answering; fresh attach succeeds');
});

// Honest capability matrix: send/configure/reply are unavailable and
// issue no request.
await withServer('happy', async (fake, adapter) => {
  for (const op of ['send', 'configure', 'reply']) {
    assert.equal(adapter[op]().outcome, 'unavailable', `${op} unavailable`);
  }
  assert.equal(fake.requests.length, 0, 'unavailable operations issue no request');
  const facts = adapter.describe();
  assert.equal(facts.capabilities.snapshot, 'implemented');
  assert.equal(facts.capabilities.shutdown, 'detach_only');
  console.log('PASS capabilities: send/configure/reply honestly unavailable, no requests issued');
});

console.log('OpenCodex bridge self-test: all fixture assertions passed');
