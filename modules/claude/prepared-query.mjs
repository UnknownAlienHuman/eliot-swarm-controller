// Single-use guard around the pinned SDK's WarmQuery contract. Preparing is
// not a model query; the first Task input claims the handle exactly once.
export async function prepareQuery(startup, options) {
  const warm = await startup({ options });
  if (!warm || typeof warm.query !== 'function' || typeof warm.close !== 'function') {
    throw new Error('SDK_WARM_QUERY_UNAVAILABLE');
  }
  let state = 'prepared';
  return {
    get state() { return state; },
    query(prompt) {
      if (state !== 'prepared') throw new Error('SDK_WARM_QUERY_ALREADY_CLAIMED');
      // Burn the one-shot claim before entering vendor code. If query throws,
      // admission is uncertain and the same WarmQuery must never be replayed.
      state = 'claimed';
      return warm.query(prompt);
    },
    close() {
      if (state !== 'prepared') return;
      state = 'closed';
      warm.close();
    },
  };
}
