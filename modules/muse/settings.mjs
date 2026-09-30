// Native setter mappings for SDK 1.3.0. The controller does not know these keys.
export function configuration(settings, sessionId) {
  if (!settings || typeof settings !== 'object' || Array.isArray(settings)) throw new Error('SETTINGS_OBJECT_REQUIRED');
  const keys = Object.keys(settings);
  // Each native setter is one independently idempotent command. Do not report
  // an atomic multi-setting change which the native protocol cannot perform.
  if (keys.length !== 1) throw new Error('USE_ONE_NATIVE_SETTER_PER_CONFIGURE');
  const key = keys[0], desired = settings[key];
  const base = { sessionId };
  if (key === 'reasoningEffort') {
    if (!['none','minimal','low','medium','high','xhigh','max','ultra'].includes(desired)) throw new Error('INVALID_REASONING_EFFORT');
    return { key, desired, method:'session/setReasoningEffort', params:{...base,reasoningEffort:desired} };
  }
  if (key === 'approvalMode') {
    if (!['allowAll','promptUnmatched','onRequest','denyUnmatched'].includes(desired)) throw new Error('INVALID_APPROVAL_MODE');
    return { key, desired, method:'session/setApprovalMode', params:{...base,mode:desired} };
  }
  if (key === 'model') {
    if (!desired || typeof desired !== 'object' || Array.isArray(desired)
      || typeof desired.modelId !== 'string' || !desired.modelId.trim()) throw new Error('INVALID_MODEL_SELECTION');
    for (const name of Object.keys(desired)) if (!['modelId','providerId'].includes(name)) throw new Error(`UNSUPPORTED_MODEL_FIELD_${name}`);
    if (desired.providerId !== undefined && (typeof desired.providerId !== 'string' || !desired.providerId.trim())) throw new Error('INVALID_PROVIDER');
    return { key, desired, method:'session/setModel', params:{...base,model:{...desired}} };
  }
  throw new Error(`UNSUPPORTED_NATIVE_SETTING_${key}`);
}

export function modelMatches(session, selection) {
  return session?.modelId === selection.modelId
    && (selection.providerId === undefined || session.providerId === selection.providerId);
}

export function goalCommand(input, sessionId) {
  const action = input.action;
  if (!['set','edit','pause','resume','clear'].includes(action)) throw new Error('INVALID_GOAL_ACTION');
  const params = { sessionId };
  if (action === 'set' || action === 'edit') {
    if (typeof input.objective !== 'string' || !input.objective.trim()) throw new Error('GOAL_OBJECTIVE_REQUIRED');
    params.objective = input.objective;
  } else if (input.objective !== undefined) throw new Error('UNEXPECTED_GOAL_OBJECTIVE');
  return { method:`goal/${action}`, params, startsWork:['set','edit','resume'].includes(action) };
}
