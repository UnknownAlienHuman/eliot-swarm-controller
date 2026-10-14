// Normal first-query control for bridge.5 production execution.
//
// Pass-through only: no environment requirement, no control directory, no
// child tracking or termination, no hello/outcome counters. It exposes the
// same hook surface the bridge calls so the normal entrypoint never imports
// test fault composition. Destructive fault reproduction lives only in
// bridge-v5-fault-test.mjs together with first-query-spawn-control.test.mjs.
export async function createFirstQueryControl() {
  return {
    // Undefined keeps the SDK default child spawn (bridge.mjs omits the key
    // too). The bridge spreads this key only when it is defined.
    spawnClaudeCodeProcess: undefined,
    async noteSdkInitIdentity() {},
    instrumentControl() {},
    async *gateFirstTaskPrompt(iterable) {
      yield* iterable;
    },
  };
}
