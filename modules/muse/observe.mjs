// Pure fact derivations the bridge records in its observations (program
// section 15 / R18). No I/O and no live state: bridge.mjs owns the maps and
// the native connection; these functions only read SDK handshake, exit and
// notification facts and shape what gets recorded, so the fixture selftest
// can pin them without a live host. Nothing here reimplements an SDK state
// machine: the PendingCommandSet, the GapFiller and the fold stay in the
// SDK facade, which this bridge deliberately does not use (see README).
import { createHash } from 'node:crypto';

// The one native command identity for a controller Operation: minted once
// from the Operation's own identity facts and persisted in the checkpoint
// before any native I/O. A stable UUIDv7-shaped ID, distinct from our
// transport request IDs: the native schema requires UUIDv7, while the
// controller operation ID is UUIDv4. Reconciliation reuses the persisted
// ID; it never re-derives a fresh one, and this derivation is a pure
// function of (operation_id, created_at_ms) so a restored entry's ID can
// be checked against the facts it was minted from.
export function commandIdFor(operationId, createdAtMs) {
  const bytes = createHash('sha256').update(operationId).digest().subarray(0, 16);
  bytes.writeUIntBE(createdAtMs, 0, 6);
  bytes[6] = (bytes[6] & 15) | 0x70; bytes[8] = (bytes[8] & 63) | 0x80;
  const h = bytes.toString('hex'); return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;
}

// Host durability profile from the initialize handshake, following the
// SDK's own reading (facade/host-death.ts readSessionDurability, SS2.13.1)
// of the generated fact `InitializeResult.sessionDurability`:
// - absent is "durable", and that is decidable rather than fabricated: the
//   member is optional only so the addition was additive, and no server
//   that omits it has the ephemeral profile;
// - "ephemeral" is ephemeral;
// - any other value is UNRECOGNIZED and must never fall through to the
//   absent-means-durable rule: that rule keys on the member being missing,
//   not on its value being unfamiliar. An unrecognized profile guarantees
//   nothing, so it is recorded as its own state, never as durable.
// `declared` keeps the raw handshake value (null when absent) beside the
// classified profile.
export function durabilityProfile(initializeResult) {
  const declared = initializeResult?.sessionDurability;
  if (declared === undefined || declared === 'durable') {
    return { profile:'durable', declared:declared ?? null };
  }
  if (declared === 'ephemeral') return { profile:'ephemeral', declared };
  return { profile:'unrecognized', declared:declared ?? null };
}

// The SS2.11 exit rows, as a pure function of the process exit evidence
// both bridge transports deliver ({code, signal}; exactly one non-null).
// Exit 0 is the only row where the run was cancelled, the drain completed
// and the durable SessionEnd records were written; every other row is an
// abnormal death (facade/host-death.ts isAbnormalHostDeath). A null exit
// means the spawn itself failed: no host ever ran.
export function classifyExitKind(exit) {
  if (!exit) return 'spawnFailure';
  switch (exit.code) {
    case 0: return 'cleanShutdown';
    case 1: return 'unhandledError';
    case 2: return 'usageError';
    case 3: return 'configError';
    case 4: return 'leaseUnavailable';
    case 5: return 'sdkSurfaceUnavailable';
    default: return 'crash';
  }
}

// The recorded host-death fact. `profile` is the durability profile in
// force for the connection that died (null when no handshake completed, so
// no profile was ever declared). Only a durable profile's sessions survive
// the host; ephemeral and unrecognized profiles guarantee nothing, and a
// death before any handshake established nothing to survive. Recording the
// death invents no terminal turn or session event: the native session's
// own disposition stays native, and durable work is reconciled through
// the explicit recovery path, not synthesized here.
export function hostDeathObservation(profile, exit, atMs) {
  const exit_kind = classifyExitKind(exit);
  return { at_ms:atMs, profile:profile ?? null,
    exit:exit ? { code:exit.code ?? null, signal:exit.signal ?? null } : null,
    exit_kind, abnormal:exit_kind !== 'cleanShutdown',
    session_survives:profile === 'durable' };
}

// A `view/gap` names a hole this bridge cannot fill: splice-fill belongs to
// the SDK facade's GapFiller over a live fold, and the bridge runs the
// low-level Connection with a compact observation instead, so no fill is
// attempted and none is claimed. The record carries the gap's own opaque
// bracket verbatim (SS4.1: cursors are relayed, never parsed or ordered)
// plus the session it names, so the inability is a recorded observation
// distinct from the gaps counter and the view-health record. A gap whose
// cursors are missing or malformed is recorded with nulls and
// cursors_complete:false rather than dropped.
export function gapFillObservation(params, atMs) {
  const after = typeof params?.after === 'string' ? params.after : null;
  const next = typeof params?.next === 'string' ? params.next : null;
  return { status:'unfilled', reason:'splice_fill_not_available_on_compact_observation_path',
    session_id:typeof params?.sessionId === 'string' ? params.sessionId : null,
    after, next, cursors_complete:after !== null && next !== null, at_ms:atMs };
}

// How a failed explicit reconciliation classifies the pending command.
// An already-recorded ACK still means the native side accepted the
// command. Only a durable protocol rejection kind proves non-admission;
// every other failure (transport, protocol, internal, a non-MspError)
// settles nothing, so the command stays 'unknown' — pending, never
// rejected (program section 15 norm 2/4; SDK SS4.13: only a durable
// commandRejected settles a submission).
const RECONCILE_REJECTION_KINDS = ['invalidParams','commandRejected','overloaded','backpressured'];
export function failedReconcileOutcome(nativeKind, hasAck) {
  if (hasAck) return 'accepted';
  return RECONCILE_REJECTION_KINDS.includes(nativeKind) ? 'rejected' : 'unknown';
}
