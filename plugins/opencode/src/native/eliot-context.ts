// Eliot vendor-native context-hook producer for the per-Agent OpenCode server.
//
// Original implementation against the installed OpenCode v2.0.32 plugin API
// (tag commit 843a6c51; `@opencode/plugin` 2.0.32 Promise SDK, MIT:
// `Plugin.define({ id, setup })`, `Rpc.define` plus `ctx.rpc.register`, and
// `ctx.session.hook("context")` with a mutable system/messages event; see
// https://opencode.ai/docs/build/plugins and
// https://opencode.ai/docs/build/plugins/rpc). No vendor source is copied
// and no dependency is installed: the host resolves `@opencode/plugin`.
//
// Wiring: the Rust host writes this file into the isolated per-Agent config
// root's auto-discovered `opencode/plugins/` directory before `serve`
// starts, so it loads with no `plugins` config entry (the loader skips
// configured single-file paths). It runs inside the EXISTING per-Agent
// `opencode serve` HTTP server; it opens no listener and calls no Eliot
// endpoint. Rust reaches it over the existing authenticated HTTP transport:
//
//   POST /api/rpc/eliot-context/{bind,next,resolve}  body { input } -> { output }
//
// RPC `eliot-context`, version 1. `bind` binds one native session id (first
// call wins; the same id is idempotent; any other id echoes the bound one
// so the caller detects the mismatch by comparison). `next` long-polls for
// the next bound-session context hook. `resolve` delivers context (or a
// confirmed pass) to exactly one pending hook. Only hook metadata
// (hook/session/message ids) crosses to Rust; message ids are raw native
// `Message.id` values and Rust verifies the exact mapping.
//
// Model-safety: at most one pending hook; further context hooks safe-pass
// while one is pending. The native hook carries no signal or timeout, so
// each pending hook safe-passes on its own 10s monotonic deadline. Dispose
// safe-passes the pending hook, releases the poll waiter, and removes both
// registrations. Nothing logged carries context text or credentials.
import { Plugin, Rpc } from "@opencode/plugin";

const RPC_ID = "eliot-context";
const RPC_VERSION = 1;
// Own hook-callback safe-pass deadline in monotonic ms. This bounds one
// producer handoff, never a model turn.
const HOOK_TTL_MS = 10_000;
// Upper bound on one `next` poll so no handler waits unboundedly.
const MAX_WAIT_MS = 120_000;

const Definition = Rpc.define({
  id: RPC_ID,
  methods: {
    bind: {
      input: {
        type: "object",
        properties: { sessionID: { type: "string" } },
        required: ["sessionID"],
        additionalProperties: false,
      },
      output: {
        type: "object",
        properties: {
          version: { type: "number" },
          boundSessionID: { type: "string" },
        },
        required: ["version", "boundSessionID"],
        additionalProperties: false,
      },
    },
    next: {
      input: {
        type: "object",
        properties: {
          sessionID: { type: "string" },
          waitMs: { type: "number" },
        },
        required: ["sessionID", "waitMs"],
        additionalProperties: false,
      },
      output: {
        type: "object",
        properties: {
          hook: {
            type: "object",
            properties: {
              hookID: { type: "string" },
              sessionID: { type: "string" },
              messageID: { type: "string" },
              expiresInMs: { type: "number" },
            },
            required: ["hookID", "sessionID", "messageID", "expiresInMs"],
            additionalProperties: false,
          },
        },
        additionalProperties: false,
      },
    },
    resolve: {
      input: {
        type: "object",
        properties: {
          hookID: { type: "string" },
          sessionID: { type: "string" },
          messageID: { type: "string" },
          context: { type: "string" },
          cancel: { type: "boolean" },
        },
        required: ["hookID", "sessionID", "messageID"],
        additionalProperties: false,
      },
      output: {
        type: "object",
        properties: {
          status: {
            type: "string",
            enum: ["applied", "empty", "expired", "cancelled", "mismatch", "closed"],
          },
        },
        required: ["status"],
        additionalProperties: false,
      },
    },
  },
  events: {},
});

// Latest user message id: the only hook metadata Rust needs besides the
// hook and session ids. Raw native `Message.id`, exact. Only the LAST
// role=user entry counts: if it carries no id, safe-pass (null) rather
// than falling back to an older id, so native/synthetic user entries can
// never false-bind.
function lastUserMessageID(messages) {
  if (!Array.isArray(messages)) return null;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (message === null || typeof message !== "object" || message.role !== "user") continue;
    return typeof message.id === "string" && message.id.length > 0 ? message.id : null;
  }
  return null;
}

export default Plugin.define({
  id: RPC_ID,
  async setup(ctx) {
    // Monotonic clock picked once so deadlines never mix clocks.
    const nowMs =
      typeof performance !== "undefined" && typeof performance.now === "function"
        ? () => performance.now()
        : () => Date.now();

    let boundSessionID = null;
    let closed = false;
    // At most one pending hook: { hookID, sessionID, messageID, deadline,
    // event, gate, settle, timer, live }.
    let pending = null;
    // At most one poll waiter: { settled, timer, cleanup, done }.
    let waiter = null;
    // Last expired hook identity, so a late resolve reports `expired`
    // instead of `mismatch`. Never a second live hook.
    let lastExpired = null;
    let sequence = 0;

    const describeHook = (record) => ({
      hookID: record.hookID,
      sessionID: record.sessionID,
      messageID: record.messageID,
      expiresInMs: Math.max(0, Math.ceil(record.deadline - nowMs())),
    });

    // Settle exactly once: clears the timer, drops the pending slot when it
    // is ours, and unblocks the hook callback without touching the event.
    const makeSettle = (record, release) => () => {
      if (!record.live) return;
      record.live = false;
      if (record.timer !== null) {
        clearTimeout(record.timer);
        record.timer = null;
      }
      if (pending === record) pending = null;
      release();
    };

    const expirePending = () => {
      if (pending === null || nowMs() < pending.deadline) return;
      lastExpired = {
        hookID: pending.hookID,
        sessionID: pending.sessionID,
        messageID: pending.messageID,
      };
      pending.settle();
    };

    const onContext = async (event) => {
      let record = null;
      try {
        if (closed || boundSessionID === null) return;
        if (!event || event.sessionID !== boundSessionID) return;
        if (pending !== null) return;
        const messageID = lastUserMessageID(event.messages);
        if (messageID === null) return;
        sequence += 1;
        const started = nowMs();
        let release = null;
        const gate = new Promise((resolveGate) => {
          release = resolveGate;
        });
        record = {
          hookID: `h${sequence.toString(36)}-${Math.floor(started).toString(36)}`,
          sessionID: event.sessionID,
          messageID,
          deadline: started + HOOK_TTL_MS,
          event,
          gate,
          settle: null,
          timer: null,
          live: true,
        };
        record.settle = makeSettle(record, release);
        record.timer = setTimeout(() => {
          expirePending();
        }, HOOK_TTL_MS);
        pending = record;
        if (waiter !== null) {
          const current = waiter;
          waiter = null;
          current.done({ hook: describeHook(record) });
        }
      } catch {
        if (record !== null) record.settle();
        return;
      }
      await record.gate;
    };

    const handlers = {
      bind: async (input) => {
        const sessionID = input.sessionID;
        if (!closed && boundSessionID === null) boundSessionID = sessionID;
        return { version: RPC_VERSION, boundSessionID: boundSessionID ?? sessionID };
      },
      next: async (input, context) => {
        const sessionID = input.sessionID;
        if (closed || boundSessionID === null || sessionID !== boundSessionID) return {};
        expirePending();
        if (pending !== null) return { hook: describeHook(pending) };
        let wait = Number.isFinite(input.waitMs) ? Math.floor(input.waitMs) : 0;
        if (wait <= 0) return {};
        if (wait > MAX_WAIT_MS) wait = MAX_WAIT_MS;
        if (waiter !== null) return {};
        return await new Promise((resolvePoll) => {
          const current = { settled: false, timer: null, cleanup: null, done: null };
          const done = (value) => {
            if (current.settled) return;
            current.settled = true;
            if (current.timer !== null) {
              clearTimeout(current.timer);
              current.timer = null;
            }
            if (current.cleanup !== null) {
              const cleanup = current.cleanup;
              current.cleanup = null;
              try {
                cleanup();
              } catch {
                // Listener removal is best-effort; the waiter is settled.
              }
            }
            if (waiter === current) waiter = null;
            resolvePoll(value);
          };
          current.done = done;
          waiter = current;
          current.timer = setTimeout(() => {
            done({});
          }, wait);
          const signal =
            context !== null && typeof context === "object" ? context.signal : null;
          if (signal !== null && typeof signal === "object") {
            if (signal.aborted === true) {
              done({});
              return;
            }
            if (typeof signal.addEventListener === "function") {
              const onAbort = () => {
                done({});
              };
              signal.addEventListener("abort", onAbort, { once: true });
              current.cleanup =
                typeof signal.removeEventListener === "function"
                  ? () => {
                      signal.removeEventListener("abort", onAbort);
                    }
                  : null;
            }
          }
        });
      },
      resolve: async (input) => {
        // Fully synchronous: no await before the hook is settled, so the
        // verify-append-unblock sequence is atomic.
        const hookID = input.hookID;
        const sessionID = input.sessionID;
        const messageID = input.messageID;
        if (closed) return { status: "closed" };
        expirePending();
        if (
          pending !== null &&
          pending.live &&
          pending.hookID === hookID &&
          pending.sessionID === sessionID &&
          pending.messageID === messageID
        ) {
          const record = pending;
          if (input.cancel === true) {
            record.settle();
            return { status: "cancelled" };
          }
          const contextText = typeof input.context === "string" ? input.context : "";
          if (contextText.length === 0) {
            record.settle();
            return { status: "empty" };
          }
          const system = record.event.system;
          if (!Array.isArray(system)) {
            // Unreachable against the pinned host schema; settle safe-pass
            // first so the model call never wedges, then fail loudly with a
            // static message (never the context text).
            record.settle();
            throw new Error("eliot-context system unavailable");
          }
          system.push({ type: "text", text: contextText });
          record.settle();
          return { status: "applied" };
        }
        if (
          lastExpired !== null &&
          lastExpired.hookID === hookID &&
          lastExpired.sessionID === sessionID &&
          lastExpired.messageID === messageID
        ) {
          return { status: "expired" };
        }
        return { status: "mismatch" };
      },
    };

    const rpcRegistration = await ctx.rpc.register(Definition, handlers);
    const hookRegistration = await ctx.session.hook("context", onContext);
    return async () => {
      closed = true;
      if (waiter !== null) {
        const current = waiter;
        waiter = null;
        current.done({});
      }
      if (pending !== null) pending.settle();
      try {
        await hookRegistration.dispose();
      } finally {
        await rpcRegistration.dispose();
      }
    };
  },
});
