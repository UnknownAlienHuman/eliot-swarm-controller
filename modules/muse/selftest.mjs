#!/usr/bin/env node
// Fixture self-test for the Muse bridge's recorded-observation derivations
// and checkpoint round-trip (program section 15 / R18). No native
// executable, account or model call is involved: fixtures are authored
// from the pinned SDK sources and MSP schema (see fixtures/*.json
// provenance comments). The live bridge behaviors these pin are
// submitNative/reconcileNative in bridge.mjs; the derivations themselves
// live in observe.mjs so they can be exercised here without a host.
// Run:  node selftest.mjs
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  commandIdFor, durabilityProfile, gapFillObservation, hostDeathObservation,
  failedReconcileOutcome,
} from './observe.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
async function load(name) {
  return JSON.parse(await readFile(path.join(here, 'fixtures', name), 'utf8'));
}
const AT_MS = 1780000000123;

// 1. Durability profile: three distinct recorded states; an unrecognized
// handshake value never collapses into durable (SS2.13.1).
{
  const fixture = await load('durability.json');
  for (const c of fixture.cases) {
    assert.deepEqual(durabilityProfile(c.initialize_result), c.expected, c.name);
  }
  const unrecognized = fixture.cases.filter(c => c.expected.profile === 'unrecognized');
  assert.ok(unrecognized.length >= 2, 'fixture covers unrecognized values');
  for (const c of unrecognized) {
    assert.notEqual(durabilityProfile(c.initialize_result).profile, 'durable', c.name);
  }
  console.log(`PASS durability: ${fixture.cases.length} handshake readings, unrecognized never durable`);
}

// 2. Host death: ephemeral death discards the session's survival; durable
// death leaves it for explicit recovery; only exit 0 is a clean shutdown.
{
  const fixture = await load('host-death.json');
  for (const c of fixture.cases) {
    const record = hostDeathObservation(c.profile, c.exit, AT_MS);
    assert.equal(record.at_ms, AT_MS, c.name);
    assert.equal(record.profile, c.profile, c.name);
    assert.deepEqual(record.exit,
      c.exit ? { code:c.exit.code, signal:c.exit.signal } : null, c.name);
    assert.equal(record.exit_kind, c.expected.exit_kind, c.name);
    assert.equal(record.abnormal, c.expected.abnormal, c.name);
    assert.equal(record.session_survives, c.expected.session_survives, c.name);
  }
  console.log(`PASS host-death: ${fixture.cases.length} death records incl. ephemeral host death`);
}

// 3. View gaps: each bracket is recorded verbatim as an unfilled hole.
// Coalesced and overlapping brackets are never merged or ordered (cursors
// are opaque, SS4.1); a foreign session's gap stays under its own ID.
{
  const fixture = await load('view-gap.json');
  const records = fixture.cases.map(c => gapFillObservation(c.params, AT_MS));
  fixture.cases.forEach((c, i) => {
    const record = records[i];
    assert.equal(record.status, c.expected.status, c.name);
    assert.equal(record.reason, 'splice_fill_not_available_on_compact_observation_path', c.name);
    assert.equal(record.session_id, c.expected.session_id, c.name);
    assert.equal(record.after, c.expected.after, `${c.name}: after relayed byte-exact`);
    assert.equal(record.next, c.expected.next, `${c.name}: next relayed byte-exact`);
    assert.equal(record.cursors_complete, c.expected.cursors_complete, c.name);
    assert.equal(record.at_ms, AT_MS, c.name);
    // No derived ordering or merged range is ever attached to a record.
    assert.deepEqual(Object.keys(record).sort(),
      ['after','at_ms','cursors_complete','next','reason','session_id','status'], c.name);
  });
  const foreign = records[fixture.cases.findIndex(c => c.name.startsWith('foreign'))];
  assert.notEqual(foreign.session_id, fixture.root_session, 'foreign gap not attributed to root');
  console.log(`PASS view-gap: ${fixture.cases.length} verbatim unfilled records incl. overlap and foreign session`);
}

// 4. Command identity and checkpoint round-trip: an entry persisted
// before native I/O survives bridge loss with everything explicit
// same-ID reconciliation needs, and its ID re-derives from the recorded
// command facts (disconnect after write/before ACK; same-ID replay).
{
  const fixture = await load('checkpoint-roundtrip.json');
  const [operationId, entry] = fixture.state.native_pending[0];
  assert.equal(commandIdFor(entry.command.operation_id, entry.command.created_at_ms), entry.id,
    'persisted ID re-derives from the recorded command facts');
  assert.equal(commandIdFor(entry.command.operation_id, entry.command.created_at_ms),
    commandIdFor(entry.command.operation_id, entry.command.created_at_ms), 'minting is deterministic');
  assert.match(entry.id, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    'native ID is UUIDv7-shaped');

  const dir = await mkdtemp(path.join(os.tmpdir(), 'muse-selftest-'));
  try {
    await writeFile(path.join(dir, 'owner.json'),
      JSON.stringify({ version:1, process:{ purpose:'module' }, token:'fixture-boot-token' }));
    process.env.ELIOT_SWARM_MODULE_STATE = dir;
    process.env.ELIOT_SWARM_MODULE_OWNER = path.join(dir, 'owner.json');
    const { recoveryState } = await import('./checkpoint.mjs');
    const writer = await recoveryState();
    assert.equal(writer.saved, undefined, 'fresh state dir has no checkpoint');
    await writer.write(fixture.state);
    const reader = await recoveryState();
    assert.equal(reader.saved.version, 1);
    assert.deepEqual(reader.saved.native_pending, fixture.state.native_pending,
      'native_pending survives the round-trip byte-for-byte as JSON');
    assert.deepEqual(reader.saved.outcomes, fixture.state.outcomes);
    assert.equal(reader.saved.root_id, fixture.state.root_id);
    assert.equal(reader.saved.native_scope, fixture.state.native_scope);
    const [restoredId, restored] = reader.saved.native_pending[0];
    assert.equal(restoredId, operationId);
    for (const field of fixture.reconcile_fields) {
      assert.ok(Object.hasOwn(restored, field), `restored entry carries ${field} for same-ID reconcile`);
    }
    assert.equal(restored.resolved, false, 'lost reply leaves the command unresolved, not settled');
    assert.equal(restored.ack, null, 'no ACK was recorded before the reply was lost');
    assert.deepEqual(restored.params, entry.params, 'replay params are the persisted bytes');
  } finally {
    delete process.env.ELIOT_SWARM_MODULE_STATE;
    delete process.env.ELIOT_SWARM_MODULE_OWNER;
    await rm(dir, { recursive:true, force:true });
  }
  console.log('PASS checkpoint: pending command round-trips with ID, payload and reconcile fields');
}

// 5. Failed-reconcile classification: only a durable protocol rejection
// settles a command as rejected; any other failure leaves it unknown
// (review section 13 cases 5 and 6).
{
  const fixture = await load('reconcile-outcome.json');
  for (const c of fixture.cases) {
    assert.equal(failedReconcileOutcome(c.native_kind, c.has_ack), c.expected, c.name);
  }
  console.log(`PASS reconcile-outcome: ${fixture.cases.length} classifications, non-settling errors stay unknown`);
}

console.log('MUSE SELFTEST OK');
