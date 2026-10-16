// Model selection at agent.open is an optional explicit override. Pinned SDK
// Options.model is optional and defaults to the CLI default model
// (sdk.d.ts 0.3.287: `model?: string`). Omit it when the route carries no
// override; the installed configured harness owns its default. Explicit
// aliases and full IDs are passed verbatim. Effective model stays observed
// only from native system/init, never promoted from the requested alias.
export function explicitQueryModel(nativeOptions) {
  const modelId = nativeOptions?.modelId;
  if (modelId === undefined || modelId === null) {
    return {};
  }
  if (typeof modelId !== 'string' || !modelId.trim()) {
    throw new Error('MODEL_ID_REQUIRED');
  }
  if (modelId !== modelId.trim()) {
    throw new Error('INVALID_MODEL_ID');
  }
  if (modelId.length > 256 || /[\u0000-\u001f\u007f]/.test(modelId)) {
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
