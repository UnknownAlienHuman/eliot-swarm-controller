// Receipt-to-observation binding for the local warm-stream result protocol.
// Native state and terminal receipts are captured at different times, so an
// acknowledged snapshot must never share mutable references with live state.
const RESULT_KEYS = Object.freeze([
  'input_operation_id', 'native_conversation_id', 'bridge_boot_id',
  'result_ordinal', 'response_sha256', 'status',
]);

export function snapshotObservation(value) {
  return JSON.parse(JSON.stringify(value));
}

export function localResultMatches(expected, observed) {
  return Boolean(observed && RESULT_KEYS.every(key => expected[key] === observed[key]));
}

function citeObservation(outcome, observationId) {
  outcome.details.local_execution_ref = {
    ...outcome.details.local_execution_ref,
    observation_id: observationId,
  };
}

export async function bindPendingLocalResults(unbound, lastObserved, observe, sendOutcome) {
  let acknowledged = lastObserved;
  for (const [id, outcome] of unbound) {
    const ref = outcome.details.local_execution_ref;
    if (!acknowledged?.state.local_execution_results?.some(item => localResultMatches(ref, item))) continue;
    citeObservation(outcome, acknowledged.observation_id);
    await sendOutcome(id, outcome);
  }

  let remaining = unbound.filter(([, outcome]) =>
    !(Number.isSafeInteger(outcome.details.local_execution_ref.observation_id)
      && outcome.details.local_execution_ref.observation_id > 0));
  if (!remaining.length) return acknowledged;

  acknowledged = await observe();
  if (!acknowledged) return null;
  for (const [id, outcome] of remaining) {
    const ref = outcome.details.local_execution_ref;
    if (!acknowledged.state.local_execution_results?.some(item => localResultMatches(ref, item))) continue;
    citeObservation(outcome, acknowledged.observation_id);
    await sendOutcome(id, outcome);
  }
  return acknowledged;
}
