#!/usr/bin/env node
// Fixture selftest for the OpenCodex provider-service adapter, run
// against the fake Management API server (fixtures/fake-management-server.mjs).
// No live opencodex service, account or model call is involved: fixtures
// are reconstructions from the upstream contracts last verified
// against v2.75.0, not captures.
// Run:  node selftest.mjs
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { createAdapter, loadConfig, MODULE_ARTIFACT_ID } from './bridge.mjs';
import { normalizeIntegrationOutcomes } from './control.mjs';
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
    expectedVersion: '2.75.0',
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

function mutations(fake, method, pathname) {
  return fake.requests.filter((r) => r.method === method && r.path === pathname);
}

function assertNoToken(record, label) {
  assert.ok(!JSON.stringify(record).includes(TOKEN), `${label}: token absent from record`);
}

// 1. Attach + snapshot happy path: every section observed, version match.
await withServer('happy', async (fake, adapter) => {
  const attached = await adapter.attach();
  assert.equal(attached.state, 'attached');
  assert.deepEqual(attached.identity, { endpoint: fake.endpoint, pid: 4242, version: '2.75.0' });
  assert.equal(attached.readiness, 'observed');
  const snap = await adapter.snapshot();
  assert.equal(snap.readiness, 'observed');
  assert.equal(snap.lifecycleOwner, 'external');
  assert.equal(snap.health.version, '2.75.0');
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
  // Slice 2: the read-only configuration section rides in the snapshot
  // (this is the recorded applied-evidence doctor projects).
  assert.equal(snap.configuration.protocols.messagesEnabled, true);
  assert.equal(snap.configuration.protocols.unrepresentable, 'legacy');
  const cline = snap.configuration.clientIntegrations.find((c) => c.clientId === 'cline');
  assert.equal(cline.state, 'absent');
  assert.ok(!('configPath' in cline), 'integration file locations are never recorded');
  const aside = snap.configuration.clientIntegrations.find((c) => c.clientId === 'aside');
  assert.equal(aside.profilesTotal, 2);
  assert.equal(snap.configuration.asideProfiles.total, 2);
  assert.equal(snap.configuration.subagentSurface.v2.multiAgentMode, 'v1');
  assert.equal(snap.configuration.subagentSurface.injectionModel.model, null);
  assert.equal(snap.configuration.subagentSurface.subagentModelFallback.pollMs, 60000);
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
    expectedVersion: '2.75.0', adminTokenEnv: TOKEN_ENV,
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

// 6. Version difference: a recorded observation only. Readiness stays
// observed — the service is operator-managed and unpinned, and the
// adapter baseline follows upstream current — while both versions
// are recorded and the snapshot is produced in full.
await withServer('version_mismatch', async (fake, adapter) => {
  const snap = await adapter.snapshot();
  assert.equal(snap.readiness, 'observed');
  assert.equal(snap.versionComparison, 'mismatch');
  assert.equal(snap.observedVersion, '2.99.0-fixture');
  assert.equal(snap.expectedVersion, '2.75.0');
  assert.equal(snap.health.version, '2.99.0-fixture');
  console.log('PASS version-difference: readiness stays observed, both versions recorded as facts');
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
  assert.equal(snap.health.version, '2.75.0');
  assert.ok(Array.isArray(snap.providers));
  assert.equal(snap.usage.state, 'observed');
  assert.equal(snap.configuration.protocols.messagesEnabled, true,
    'configuration section degrades independently of the models read');
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

// Honest capability matrix: send/reply are unavailable and issue no
// request; configure/preview are implemented but a missing or unknown
// request is rejected locally, again with no request issued.
await withServer('happy', async (fake, adapter) => {
  for (const op of ['send', 'reply']) {
    assert.equal(adapter[op]().outcome, 'unavailable', `${op} unavailable`);
  }
  const noRequest = await adapter.configure();
  assert.equal(noRequest.outcome, 'rejected');
  assert.equal(noRequest.reason, 'request_required');
  const badKind = await adapter.preview({ kind: 'nonsense' });
  assert.equal(badKind.outcome, 'rejected');
  assert.equal(badKind.reason, 'unknown_kind');
  assert.equal(fake.requests.length, 0, 'unavailable/rejected operations issue no request');
  const facts = adapter.describe();
  assert.equal(facts.capabilities.snapshot, 'implemented');
  assert.equal(facts.capabilities.configure, 'implemented');
  assert.equal(facts.capabilities.shutdown, 'detach_only');
  console.log('PASS capabilities: send/reply honestly unavailable; configure rejects empty requests locally');
});

// --- Slice 2: saved configuration Operations -------------------------------

// 12. Protocol settings: preview shows the diff without writing; the
// configure applies exactly one PATCH and verifies by readback GET.
await withServer('happy', async (fake, adapter) => {
  const request = {
    kind: 'protocol_settings',
    patch: { messagesEnabled: false, unrepresentable: 'reject', rollout: { shadowPlan: true } },
  };
  const preview = await adapter.preview(request);
  assert.equal(preview.outcome, 'preview');
  assert.deepEqual(preview.differingFields, ['messagesEnabled', 'unrepresentable', 'rollout.shadowPlan']);
  assert.equal(mutations(fake, 'PATCH', '/api/protocols/settings').length, 0, 'preview never writes');
  const record = await adapter.configure(request);
  assert.equal(record.outcome, 'applied');
  assert.equal(record.operation_contract.completion_condition, 'native_configuration_applied');
  assert.equal(record.operation_contract.replay_policy, 'readback_only_no_mutation_replay');
  assert.equal(record.operation_contract.fallback_used, false);
  assert.equal(record.verification.state, 'verified');
  assert.equal(record.readback.after.messagesEnabled, false);
  assert.equal(record.readback.after.unrepresentable, 'reject');
  assert.equal(record.readback.after.rollout.shadowPlan, true);
  assert.equal(mutations(fake, 'PATCH', '/api/protocols/settings').length, 1, 'exactly one PATCH');
  assert.ok(fake.requests.every((r) => r.authed), 'mutation carried the admin key header');
  assertNoToken(record, 'protocol-settings');
  console.log('PASS protocol-settings: preview diff, one PATCH, readback verified');
});

// 13. Protocol rollout dependency is refused locally (mirrors
// upstream's 400) — no PATCH is sent.
await withServer('happy', async (fake, adapter) => {
  const record = await adapter.configure({
    kind: 'protocol_settings',
    patch: { rollout: { managedMessagesNativeOAuth: true } },
  });
  assert.equal(record.outcome, 'rejected');
  assert.equal(record.reason, 'rollout_dependency');
  assert.equal(mutations(fake, 'PATCH', '/api/protocols/settings').length, 0);
  console.log('PASS protocol-dependency: OAuth-without-native refused locally, no write');
});

// 14. Model settings for one routed provider: receipt (saved) and
// readback (declared fields + per-client integration states) are
// separate facts; a no-op answers saved:false honestly.
await withServer('happy', async (fake, adapter) => {
  const record = await adapter.configure({
    kind: 'model_settings', provider: 'anthropic', modelId: 'claude-sonnet-fixture',
    contextWindow: 262144, reasoningEfforts: ['low', 'high'], defaultReasoningEffort: 'high',
  });
  assert.equal(record.outcome, 'applied');
  assert.equal(record.receipt.saved, true);
  assert.equal(record.receipt.changed, true);
  assert.equal(record.savedVsApplied.saved, true);
  assert.equal(record.savedVsApplied.clientIntegrationsObserved, true);
  assert.equal(record.readback.model.contextWindowDeclared, 262144);
  assert.equal(record.verification.state, 'verified');
  assert.equal(mutations(fake, 'PUT', '/api/model-settings').length, 1);
  assertNoToken(record, 'model-settings');

  const noop = await adapter.configure({
    kind: 'model_settings', provider: 'anthropic', modelId: 'claude-sonnet-fixture',
    contextWindow: 262144,
  });
  assert.equal(noop.outcome, 'applied');
  assert.equal(noop.receipt.changed, false);
  assert.equal(noop.receipt.saved, false, 'a no-op publishes nothing and says so');
  assert.equal(noop.receipt.hasOverrides, true, 'stored declarations remain reported');
  console.log('PASS model-settings: saved receipt + declared readback verified; no-op saved:false');
});

// 15. Lost mutation response: the write landed but the answer never
// arrived. The Operation is unknown, reconciled by reading — the PUT
// is sent exactly once.
await withServer('lost_response', async (fake, adapter) => {
  const record = await adapter.configure({
    kind: 'model_settings', provider: 'anthropic', modelId: 'claude-sonnet-fixture',
    contextWindow: 300000,
  });
  assert.equal(record.outcome, 'unknown');
  assert.equal(record.reason, 'mutation_response_lost');
  assert.equal(record.verification.reconciledByRead, true);
  assert.equal(record.verification.state, 'verified', 'readback proves the landed value');
  assert.equal(record.readback.model.contextWindowDeclared, 300000);
  assert.equal(mutations(fake, 'PUT', '/api/model-settings').length, 1, 'never re-sent');
  console.log('PASS lost-response: unknown outcome reconciled by readback, mutation sent once');
});

// 16. Saved but catalog refresh failed: the record is partial, the
// saved fact is not inflated into applied.
await withServer('refresh_failed', async (fake, adapter) => {
  const record = await adapter.configure({
    kind: 'model_settings', provider: 'anthropic', modelId: 'claude-sonnet-fixture',
    contextWindow: 262144,
  });
  assert.equal(record.outcome, 'partial');
  assert.equal(record.receipt.saved, true);
  assert.equal(record.receipt.catalogRefresh.status, 'failed');
  console.log('PASS refresh-failed: saved=true with failed convergence is partial, not applied');
});

// 17. Sub-agent v2 surface: mode change applies at the new-sessions
// boundary; upstream's enabled/mode conflict is refused locally.
await withServer('happy', async (fake, adapter) => {
  const record = await adapter.configure({
    kind: 'subagent_v2', settings: { multiAgentMode: 'v2' },
  });
  assert.equal(record.outcome, 'applied');
  assert.equal(record.operation_contract.application_boundary, 'new_sessions_only');
  assert.equal(record.readback.after.multiAgentMode, 'v2');
  assert.ok(record.receipt.warnings.some((w) => w.includes('Applies to new sessions')));
  const conflict = await adapter.configure({
    kind: 'subagent_v2', settings: { enabled: true, multiAgentMode: 'v1' },
  });
  assert.equal(conflict.outcome, 'rejected');
  assert.equal(conflict.reason, 'enabled_mode_conflict');
  assert.equal(mutations(fake, 'PUT', '/api/v2').length, 1, 'the conflict added no write');
  console.log('PASS subagent-v2: mode applied with new-sessions boundary; conflict refused locally');
});

// 18. Injection model: effort validated against the GET ladder before
// any write; a bogus effort is refused locally.
await withServer('happy', async (fake, adapter) => {
  const bogus = await adapter.configure({
    kind: 'injection_model', settings: { model: 'anthropic/claude-sonnet-fixture', effort: 'bogus' },
  });
  assert.equal(bogus.outcome, 'rejected');
  assert.equal(bogus.reason, 'invalid_effort');
  assert.equal(mutations(fake, 'PUT', '/api/injection-model').length, 0);
  const record = await adapter.configure({
    kind: 'injection_model', settings: { model: 'anthropic/claude-sonnet-fixture', effort: 'high' },
  });
  assert.equal(record.outcome, 'applied');
  assert.equal(record.readback.after.model, 'anthropic/claude-sonnet-fixture');
  assert.equal(record.readback.after.effort, 'high');
  console.log('PASS injection-model: ladder-validated effort applied and read back');
});

// 19. Effort caps applied + read back; the per-model pin map is a
// recorded out-of-slice refusal, never silently dropped.
await withServer('happy', async (fake, adapter) => {
  const pins = await adapter.configure({ kind: 'effort_caps', effortCap: 'high', modelPinnedEfforts: {} });
  assert.equal(pins.outcome, 'rejected');
  assert.equal(pins.reason, 'model_pinned_efforts_not_in_slice');
  const record = await adapter.configure({ kind: 'effort_caps', effortCap: 'high', subagentEffortCap: 'medium' });
  assert.equal(record.outcome, 'applied');
  assert.equal(record.readback.after.effortCap, 'high');
  assert.equal(record.readback.after.subagentEffortCap, 'medium');
  assert.equal(mutations(fake, 'PUT', '/api/effort-caps').length, 1);
  console.log('PASS effort-caps: caps applied and read back; pin map refused as out-of-slice');
});

// 20. Subagent roster: more than five models is refused locally
// (upstream would truncate silently); a valid roster round-trips.
await withServer('happy', async (fake, adapter) => {
  const over = await adapter.configure({
    kind: 'subagent_models',
    models: ['a/1', 'a/2', 'a/3', 'a/4', 'a/5', 'a/6'],
  });
  assert.equal(over.outcome, 'rejected');
  assert.equal(over.reason, 'roster_over_limit');
  assert.equal(mutations(fake, 'PUT', '/api/subagent-models').length, 0);
  const record = await adapter.configure({
    kind: 'subagent_models',
    models: ['anthropic/claude-sonnet-fixture', 'xai/grok-fixture'],
  });
  assert.equal(record.outcome, 'applied');
  assert.deepEqual(record.readback.after.chosen, ['anthropic/claude-sonnet-fixture', 'xai/grok-fixture']);
  console.log('PASS subagent-models: >5 refused locally; roster applied and read back');
});

// 21. Upstream's own subagent fallback chain is upstream config, set
// only as this explicitly selected Operation.
await withServer('happy', async (fake, adapter) => {
  const record = await adapter.configure({
    kind: 'subagent_model_fallback',
    models: ['anthropic/claude-sonnet-fixture'], pollMs: 30000,
  });
  assert.equal(record.outcome, 'applied');
  assert.deepEqual(record.readback.after.models, ['anthropic/claude-sonnet-fixture']);
  assert.equal(record.readback.after.pollMs, 30000);
  console.log('PASS subagent-fallback: upstream chain set as an explicit Operation, read back');
});

// 22. Client integration: the full preview -> confirm -> apply ->
// readback chain, then rollback through the upstream journal. Without
// a binding (or with half of one) configure refuses locally and sends
// no mutation.
await withServer('happy', async (fake, adapter) => {
  const unbound = await adapter.configure({ kind: 'client_integration', clientId: 'cline', enabled: true });
  assert.equal(unbound.outcome, 'rejected');
  assert.equal(unbound.reason, 'plan_binding_required');
  const half = await adapter.configure({
    kind: 'client_integration', clientId: 'cline', enabled: true, planFingerprint: 'fixture-fp-x',
  });
  assert.equal(half.outcome, 'rejected');
  assert.equal(half.reason, 'plan_binding_both_or_neither');
  assert.equal(mutations(fake, 'PUT', '/api/client-integrations/cline').length, 0, 'no unbound write');

  const preview = await adapter.preview({ kind: 'client_integration', clientId: 'cline', enabled: true });
  assert.equal(preview.outcome, 'preview');
  assert.equal(preview.plan.clientId, 'cline');
  assert.equal(preview.plan.operation, 'apply');
  assert.equal(preview.plan.willChange, true);
  assert.ok(preview.plan.fingerprint, 'plan carries the confirm fingerprint');
  assert.equal(mutations(fake, 'PUT', '/api/client-integrations/cline').length, 0, 'preview writes nothing');

  const record = await adapter.configure({
    kind: 'client_integration', clientId: 'cline', enabled: true,
    operation: 'apply', planFingerprint: preview.plan.fingerprint,
  });
  assert.equal(record.outcome, 'applied');
  assert.equal(record.receipt.ok, true);
  assert.equal(record.receipt.state, 'current');
  assert.equal(record.readback.after.state, 'current');
  assert.equal(record.verification.state, 'verified');
  const sent = mutations(fake, 'PUT', '/api/client-integrations/cline');
  assert.equal(sent.length, 1);
  assert.equal(sent[0].body.operation, 'apply');
  assert.equal(sent[0].body.planFingerprint, preview.plan.fingerprint, 'both binding fields sent together');
  assertNoToken(record, 'client-integration');

  const journal = await adapter.journal('cline');
  const row = journal.operations.find((op) => op.opId === record.receipt.opId);
  assert.ok(row, 'the apply is in the upstream journal');
  assert.ok(!('configPath' in row), 'journal file locations are never recorded');

  const restorePreview = await adapter.preview({
    kind: 'client_integration_restore', clientId: 'cline', opId: record.receipt.opId,
  });
  assert.equal(restorePreview.outcome, 'preview');
  assert.equal(restorePreview.plan.operation, 'restore');
  const restored = await adapter.configure({
    kind: 'client_integration_restore', clientId: 'cline', opId: record.receipt.opId,
    operation: 'restore', planFingerprint: restorePreview.plan.fingerprint,
  });
  assert.equal(restored.outcome, 'applied');
  assert.equal(restored.readback.after.state, 'absent', 'rollback returns the client to absent');
  console.log('PASS client-integration: preview/binding/apply/readback + journal rollback');
});

// 23. Stale plan: the world moved between preview and confirm. The
// 409 is returned with the FRESH plan for a new operator decision;
// the stale fingerprint is never retried.
await withServer('stale_plan', async (fake, adapter) => {
  const preview = await adapter.preview({ kind: 'client_integration', clientId: 'cline', enabled: true });
  const record = await adapter.configure({
    kind: 'client_integration', clientId: 'cline', enabled: true,
    operation: 'apply', planFingerprint: preview.plan.fingerprint,
  });
  assert.equal(record.outcome, 'stale');
  assert.equal(record.code, 'integration_preview_stale');
  assert.ok(record.plan, 'fresh plan returned');
  assert.notEqual(record.plan.fingerprint, preview.plan.fingerprint, 'the fresh plan differs');
  assert.ok(record.nextStep, 'operator next step recorded');
  assert.equal(mutations(fake, 'PUT', '/api/client-integrations/cline').length, 1, 'exactly one confirm attempt, no blind retry');

  // A fresh operator decision on the new plan succeeds.
  const preview2 = await adapter.preview({ kind: 'client_integration', clientId: 'cline', enabled: true });
  const applied = await adapter.configure({
    kind: 'client_integration', clientId: 'cline', enabled: true,
    operation: 'apply', planFingerprint: preview2.plan.fingerprint,
  });
  assert.equal(applied.outcome, 'applied');
  console.log('PASS stale-plan: 409 returned with fresh plan, never retried; re-preview applies');
});

// 24. Preview unavailable (no usable roster retained): recorded as
// unknown with upstream's remedy read as evidence; nothing is written.
await withServer('preview_unavailable', async (fake, adapter) => {
  const preview = await adapter.preview({ kind: 'client_integration', clientId: 'cline', enabled: true });
  assert.equal(preview.outcome, 'unknown');
  assert.equal(preview.code, 'integration_preview_unavailable');
  assert.ok(Array.isArray(preview.readback.clientIntegrations), 'remedy read included as evidence');
  assert.equal(fake.requests.filter((r) => r.method === 'PUT').length, 0, 'nothing written');
  console.log('PASS preview-unavailable: unknown with remedy readback, no write attempted');
});

// 25. Aside profile: canonical per-profile preview/binding flow.
await withServer('happy', async (fake, adapter) => {
  const preview = await adapter.preview({ kind: 'aside_profile', profileId: 1, enabled: true });
  assert.equal(preview.outcome, 'preview');
  assert.equal(preview.plan.profileId, 1);
  const record = await adapter.configure({
    kind: 'aside_profile', profileId: 1, enabled: true,
    operation: 'apply', planFingerprint: preview.plan.fingerprint,
  });
  assert.equal(record.outcome, 'applied');
  assert.equal(record.readback.after.state, 'current');
  assert.equal(record.readback.after.enabled, true);
  assertNoToken(record, 'aside-profile');
  console.log('PASS aside-profile: per-profile preview/binding/apply/readback');
});

// 26. Partial envelope: a single-profile refusal surfaces its code and
// recovery facts without the snapshot path; the bulk 207 envelope —
// which the bridge never sends, because upstream refuses plan
// bindings for it — is recorded here as the contract the module's
// per-element parser is built against.
await withServer('partial', async (fake, adapter) => {
  const preview = await adapter.preview({ kind: 'aside_profile', profileId: 2, enabled: true });
  const record = await adapter.configure({
    kind: 'aside_profile', profileId: 2, enabled: true,
    operation: 'apply', planFingerprint: preview.plan.fingerprint,
  });
  assert.equal(record.outcome, 'refused');
  assert.equal(record.code, 'integration_conflict');
  assert.equal(record.receipt.state, 'conflict');
  assert.equal(record.receipt.snapshotRecorded, true, 'a recoverable snapshot exists (boolean only)');
  assert.ok(!JSON.stringify(record).includes('/fixture/'), 'no file location leaks into the record');

  const response = await fetch(`${fake.endpoint}/api/client-integrations/aside/profiles`, {
    method: 'PUT',
    headers: { 'X-OpenCodex-API-Key': TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify({ enabled: true }),
  });
  assert.equal(response.status, 207, 'bulk refusal is a partial 207, not a failure status');
  const envelope = await response.json();
  assert.equal(envelope.ok, false);
  const elements = normalizeIntegrationOutcomes(envelope);
  assert.equal(elements.length, 2, 'envelope parsed per element');
  assert.equal(elements[0].ok, true);
  assert.equal(elements[1].ok, false);
  assert.equal(elements[1].reason, 'conflict');
  assert.equal(elements[1].snapshotRecorded, true);
  assert.ok(!JSON.stringify(elements).includes('/fixture/'), 'element paths are reduced to booleans');
  console.log('PASS partial-envelope: refusal facts kept, paths dropped, 207 parsed per element');
});

// 27. No version gate: under a version difference the write proceeds
// — the service is operator-managed and unpinned — and the record
// carries the version facts as an observation (serviceVersion).
await withServer('version_mismatch', async (fake, adapter) => {
  const record = await adapter.configure({
    kind: 'protocol_settings', patch: { messagesEnabled: false },
  });
  assert.equal(record.outcome, 'applied');
  assert.deepEqual(record.serviceVersion, {
    observed: '2.99.0-fixture', baseline: '2.75.0', comparison: 'mismatch',
  });
  assert.equal(record.mutation.performed, true);
  assert.equal(record.verification.state, 'verified');
  assert.equal(record.readback.after.messagesEnabled, false);
  assert.equal(mutations(fake, 'PATCH', '/api/protocols/settings').length, 1, 'exactly one PATCH under a version difference');
  assertNoToken(record, 'version-difference');
  console.log('PASS version-difference: write proceeds under a differing version, facts recorded');
});

// 28. The configure seam works through the CLI exactly as the host
// invokes it, and the token stays out of stdout there too.
await withServer('happy', async (fake) => {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'opencodex-cli-configure-'));
  const configFile = path.join(dir, 'module.json');
  await writeFile(configFile, JSON.stringify({
    endpoint: fake.endpoint, moduleArtifactId: MODULE_ARTIFACT_ID,
    expectedVersion: '2.75.0', adminTokenEnv: TOKEN_ENV,
  }));
  const requestFile = path.join(dir, 'request.json');
  await writeFile(requestFile, JSON.stringify({ kind: 'effort_caps', effortCap: 'high' }));
  const { stdout } = await execFileAsync(process.execPath,
    [path.join(here, 'bridge.mjs'), '--config', configFile, 'configure', requestFile],
    { env: { ...process.env } });
  assert.ok(!stdout.includes(TOKEN), 'token absent from configure stdout');
  const parsed = JSON.parse(stdout);
  assert.equal(parsed.operation, 'configure');
  assert.equal(parsed.result.outcome, 'applied');
  assert.equal(parsed.result.readback.after.effortCap, 'high');
  console.log('PASS cli-configure: saved Operation executed via CLI, token absent from stdout');
});

console.log('OpenCodex bridge self-test: all fixture assertions passed');
