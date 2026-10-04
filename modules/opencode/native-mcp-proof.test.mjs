import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync, realpathSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Effect, Exit } from "effect";
import { Service as McpService } from "@opencode/core/mcp/index";
import plugin from "./native-mcp-proof.mjs";

test("native MCP proof activation and scoped RPC receipt", async () => {
  const modulePath = realpathSync(fileURLToPath(new URL("./native-mcp-proof.mjs", import.meta.url)));
  const moduleSha256 = createHash("sha256").update(readFileSync(modulePath)).digest("hex");
  const directory = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..", "..");
  const serviceId = "native_mcp_fixture";
  const sessionId = "ses_native_mcp_fixture_1";
  const registeredHooks = [];
  let registeredHandlers;
  let registeredDefinition;

  const context = {
    options: {
      directory,
      moduleSha256,
      serviceId,
      serviceVersion: "2.0.7",
    },
    location: { directory },
    session: {
      hook(name) {
        registeredHooks.push(name);
        return Effect.void;
      },
    },
    rpc: {
      register(definition, handlers) {
        registeredDefinition = definition;
        registeredHandlers = handlers;
        return Effect.void;
      },
    },
  };

  const activation = await Effect.runPromiseExit(Effect.scoped(plugin.effect(context)));
  expect(Exit.isSuccess(activation)).toBe(true);
  expect(registeredHooks).toEqual(["context", "http.request", "experimental.ws.send"]);
  expect(Object.keys(registeredDefinition.methods).sort()).toEqual(["arm", "read"]);
  expect(typeof registeredHandlers.arm).toBe("function");
  expect(typeof registeredHandlers.read).toBe("function");

  const issuedAtMs = Date.now() - 100;
  const sessionAssignment = {
    task_id: "task_fixture",
    task_revision: 1,
    attempt_id: "attempt_fixture",
    binding_id: "binding_fixture",
    binding_generation: 1,
    native_session_id: sessionId,
    participant_id: "participant_fixture",
    mcp_profile: "participant",
    grant_revision: 1,
    participation_basis: "attempt_owner",
    assignment_id: null,
    review_assignment_id: null,
  };
  const input = {
    challenge_id: "00000000-0000-4000-8000-000000000001",
    nonce: "00000000-0000-4000-8000-000000000002",
    service_id: serviceId,
    service_pid: process.pid,
    service_version: "2.0.7",
    module_sha256: moduleSha256,
    directory,
    session_id: sessionId,
    model: {
      id: "fixture-model",
      providerID: "fixture-provider",
      variant: "fixture-variant",
    },
    assignment: sessionAssignment,
    issued_at_ms: issuedAtMs,
    expires_at_ms: issuedAtMs + 60_000,
  };
  const discoveredTools = [{
    server: "fixture_server",
    name: "fixture_tool",
    description: "in-memory fixture tool",
    inputSchema: {
      type: "object",
      properties: { query: { type: "string" } },
      required: ["query"],
      additionalProperties: false,
    },
  }];
  let mcpReadCount = 0;
  const controlledMcp = {
    tools: () => Effect.sync(() => {
      mcpReadCount += 1;
      return discoveredTools;
    }),
  };
  const invokeArm = (value) => Effect.runPromiseExit(
    Effect.provideService(registeredHandlers.arm(value), McpService, controlledMcp),
  );

  const invalidScope = await invokeArm({
    ...input,
    challenge_id: "00000000-0000-4000-8000-000000000003",
    directory: `${directory}-outside-scope`,
  });
  expect(Exit.isFailure(invalidScope)).toBe(true);
  expect(mcpReadCount).toBe(0);

  const armed = await invokeArm(input);
  expect(Exit.isSuccess(armed)).toBe(true);
  expect(armed.value.accepted).toBe(true);
  expect(armed.value.challenge_id).toBe(input.challenge_id);
  expect(mcpReadCount).toBe(1);

  const duplicateArm = await invokeArm(input);
  expect(Exit.isSuccess(duplicateArm)).toBe(true);
  expect(duplicateArm.value.accepted).toBe(true);
  expect(mcpReadCount).toBe(1);

  const readback = await Effect.runPromiseExit(registeredHandlers.read({
    challenge_id: input.challenge_id,
    nonce: input.nonce,
  }));
  expect(Exit.isSuccess(readback)).toBe(true);
  expect(readback.value.native_discovered).toEqual({
    status: "observed",
    observed_at_ms: expect.any(Number),
    tools: [{
      server: "fixture_server",
      name: "fixture_tool",
      description: "in-memory fixture tool",
      input_schema: {
        type: "object",
        properties: { query: { type: "string" } },
        required: ["query"],
        additionalProperties: false,
      },
      codemode: null,
    }],
  });
  expect(readback.value.session_context.status).toBe("unknown");
  expect(readback.value.provider_request.status).toBe("unknown");
  expect(readback.value.model_consumed).toBe("unknown");
});
