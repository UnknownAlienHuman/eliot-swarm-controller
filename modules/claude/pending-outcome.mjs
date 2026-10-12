// A stream ending before native UUID admission leaves the queued input
// unresolved. This helper is shared by the bridge and protocol-shape fixtures
// so tests exercise the exact outcome projection used by finishSession.
export function streamEndedSendOutcome(entry, session, bridgeBootId) {
  const initialTaskDispatch = Boolean(entry.initialTaskDispatch);
  return {
    outcome: 'unknown',
    ...(!initialTaskDispatch && session.rootId
      ? { native_root_id: session.rootId, native_scope_key: session.scopeKey }
      : {}),
    details: {
      diagnostic_code: 'STREAM_ENDED_BEFORE_TURN_EVIDENCE',
      ...(initialTaskDispatch ? {
        completion_condition: 'native_input_admission_unknown',
        initial_task_dispatch: true,
        bridge_boot_id: bridgeBootId,
      } : {}),
    },
  };
}
