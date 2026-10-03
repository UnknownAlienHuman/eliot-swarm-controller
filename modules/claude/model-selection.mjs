// Model selection at agent.open is explicit and route-owned. The SDK accepts
// aliases as well as full model IDs; retain the native init model separately
// instead of treating the requested alias as proof of the effective model.
export function explicitQueryModel(nativeOptions) {
  const modelId = nativeOptions?.modelId;
  if (typeof modelId !== 'string' || !modelId.trim()) {
    throw new Error('MODEL_ID_REQUIRED');
  }
  if (modelId !== modelId.trim()) {
    throw new Error('INVALID_MODEL_ID');
  }
  return { model: modelId };
}

export function modelSelectionFacts(requestedModel, effectiveModel) {
  const requested = typeof requestedModel === 'string' && requestedModel.trim()
    ? requestedModel
    : null;
  const effective = typeof effectiveModel === 'string' && effectiveModel.trim()
    ? effectiveModel
    : null;
  return {
    model_requested: requested,
    model_effective: effective,
    model_selection_status: effective ? 'observed' : 'unknown',
    model_selection_evidence: effective ? 'system/init' : 'not_observed',
  };
}
