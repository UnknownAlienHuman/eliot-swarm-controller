// Local JSON-RPC host fixture for the Command module bridge. The child CLI is
// fixtures/fake-cmd.mjs; this validates IPC/outcome wiring without a provider
// request or a shared host/service.
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { DEFAULT_MOD_PATH, MODULE_ARTIFACT_ID } from "./glue.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));
const FAKE = join(HERE, "fixtures", "fake-cmd.mjs");
const scratch = mkdtempSync(join(tmpdir(), "eliot-command-bridge-test-"));
const endpoint = process.platform === "win32"
  ? `\\\\.\\pipe\\eliot-command-fixture-${randomUUID()}`
  : join(scratch, "host.sock");
const nativeOptions = {
  modelId: "stealth/space-bunny-alpha",
  workspaceRoot: process.cwd(),
};
const commands = [
  {
    operation_id: "mock-agent-open",
    method: "agent.open",
    created_at_ms: Date.now(),
    binding_id: "mock-binding",
    generation: 1,
    native_root_id: null,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
    input: {},
  },
  {
    operation_id: "mock-task-dispatch",
    method: "task.dispatch",
    created_at_ms: Date.now(),
    binding_id: "mock-binding",
    generation: 1,
    native_root_id: null,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
    input: { task_snapshot: { task_id: "fixture-task", objective: "return fixture output" }, text: "return a marker" },
  },
  {
    operation_id: "mock-agent-refresh",
    method: "agent.refresh",
    created_at_ms: Date.now(),
    binding_id: "mock-binding",
    generation: 1,
    native_root_id: null,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
    input: {},
  },
  {
    operation_id: "mock-agent-reconcile",
    method: "agent.reconcile",
    created_at_ms: Date.now(),
    binding_id: "mock-binding",
    generation: 1,
    native_root_id: null,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
    input: { operation_id: "mock-task-dispatch" },
  },
];

let commandIndex = 0;
const methodsByOperation = new Map(commands.map(command => [command.operation_id, command.method]));
const requiredOutcomeIds = new Set([
  "mock-agent-open",
  "mock-task-dispatch",
  "mock-agent-refresh",
  "mock-agent-reconcile",
]);
const outcomes = [];
const invalidParams = [];
const deliveredCommands = [];
const observations = [];
let resolveOutcomes;
let rejectOutcomes;
const outcomesDone = new Promise((resolve, reject) => { resolveOutcomes = resolve; rejectOutcomes = reject; });
const sockets = new Set();
const server = net.createServer(socket => {
  sockets.add(socket);
  socket.on("close", () => sockets.delete(socket));
  socket.setEncoding("utf8");
  let buffer = "";
  socket.on("data", chunk => {
    buffer += chunk;
    let newline;
    while ((newline = buffer.indexOf("\n")) >= 0) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      const packet = JSON.parse(line);
      const respond = result => socket.write(`${JSON.stringify({ jsonrpc: "2.0", id: packet.id, result })}\n`);
      if (packet.method === "client.hello") {
        respond({ client_id: "fixture-module-client" });
      } else if (packet.method === "module.hello") {
        respond({
          binding_id: "mock-binding",
          generation: 1,
          route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
        });
      } else if (packet.method === "module.next") {
        const command = commands[commandIndex++] ?? null;
        if (command) deliveredCommands.push(command);
        setTimeout(() => respond({ command }), command ? 0 : 50);
      } else if (packet.method === "module.outcome") {
        const outcome = packet.params;
        const details = outcome?.details ?? {};
        const method = methodsByOperation.get(outcome?.operation_id);
        const commonShapeValid = details.execution_shape === "sessionless_batch";
        const methodShapeValid = method === "agent.open"
          ? ["executor_preflight_completed", "executor_preflight_rejected"].includes(details.completion_condition)
          : method === "task.dispatch"
            ? details.completion_condition === "native_result_observed"
              && typeof details.batch_run_id === "string"
            : method === "agent.refresh"
              ? details.completion_condition === "batch_snapshot_readback"
              : method === "agent.reconcile"
                ? details.completion_condition === "batch_readback_recorded"
                  && details.target_operation_id === "mock-task-dispatch"
                  && details.native_replay === false
                  && details.target_outcome?.operation_id === "mock-task-dispatch"
                  && details.target_record_state === "terminal_record_observed"
                : false;
        if (!commonShapeValid || !methodShapeValid) {
          invalidParams.push({ operation_id: outcome?.operation_id, method, details });
          socket.write(`${JSON.stringify({
            jsonrpc: "2.0",
            id: packet.id,
            error: { code: -32602, message: "INVALID_PARAMS" },
          })}\n`);
          rejectOutcomes(new Error(`INVALID_PARAMS from module.outcome for ${outcome?.operation_id}`));
          return;
        }
        outcomes.push(outcome);
        respond({ recorded: true });
        if ([...requiredOutcomeIds].every(id => outcomes.some(item => item.operation_id === id))) {
          resolveOutcomes();
        }
      } else if (packet.method === "module.observe") {
        observations.push(packet.params.state);
        respond({ recorded: true });
      } else {
        socket.write(`${JSON.stringify({
          jsonrpc: "2.0",
          id: packet.id,
          error: { code: -32601, message: "fixture method not implemented" },
        })}\n`);
      }
    }
  });
});

let child = null;
let timeout = null;
let childClosed = null;
try {
  const credentialFile = join(scratch, "credential.json");
  const configFile = join(scratch, "module.json");
  const invocationLogFile = join(scratch, "native-invocations.log");
  const launcherFile = join(scratch, "counting-fake-cmd.mjs");
  mkdirSync(join(scratch, "runs"));
  writeFileSync(credentialFile, JSON.stringify({ fixture: "credential" }));
  writeFileSync(launcherFile, [
    'import { appendFileSync } from "node:fs";',
    'import { pathToFileURL } from "node:url";',
    `if (!process.argv.includes("--version")) appendFileSync(${JSON.stringify(invocationLogFile)}, "native run\\n");`,
    `await import(pathToFileURL(${JSON.stringify(FAKE)}).href);`,
  ].join("\n") + "\n");
  writeFileSync(configFile, JSON.stringify({
    credentialFile,
    endpoint,
    command: process.execPath,
    commandArgs: [launcherFile],
    controlRoot: join(scratch, "runs"),
    modPath: DEFAULT_MOD_PATH,
    moduleArtifactId: MODULE_ARTIFACT_ID,
  }));
  process.stdout.write("fixture: config written\n");
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(endpoint, resolve);
  });
  process.stdout.write("fixture: local host listening\n");
  child = spawn(process.execPath, [join(HERE, "bridge.mjs"), "--config", configFile], {
    env: { ...process.env, FAKE_CMD_SCENARIO: "success" },
    stdio: ["ignore", "ignore", "pipe"],
  });
  childClosed = new Promise(resolve => child.once("close", (code, signal) => resolve({ code, signal })));
  process.stdout.write("fixture: bridge child started\n");
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", chunk => { stderr += chunk; });
  child.once("exit", (code, signal) => {
    if (![...requiredOutcomeIds].every(id => outcomes.some(item => item.operation_id === id))) {
      rejectOutcomes(new Error(`bridge exited early (${code ?? signal}): ${stderr}`));
    }
  });
  await Promise.race([
    outcomesDone,
    new Promise((_, reject) => { timeout = setTimeout(() => reject(new Error(`bridge timed out: ${stderr}`)), 15000); }),
  ]);
  clearTimeout(timeout);
  child.kill("SIGTERM");
  await childClosed;
  assert.equal(invalidParams.length, 0);
  assert.ok(outcomes.length >= 4);
  const open = outcomes.find(item => item.operation_id === "mock-agent-open");
  const dispatch = outcomes.find(item => item.operation_id === "mock-task-dispatch");
  const refresh = outcomes.find(item => item.operation_id === "mock-agent-refresh");
  const reconcile = outcomes.find(item => item.operation_id === "mock-agent-reconcile");
  assert.equal(open.outcome, "applied");
  assert.equal(open.native_root_id, undefined);
  assert.equal(open.native_scope_key, undefined);
  assert.equal(open.details.execution_shape, "sessionless_batch");
  assert.equal(open.details.completion_condition, "executor_preflight_completed");
  assert.equal(open.details.native_session_state, "not_started");
  assert.equal(dispatch.outcome, "applied");
  assert.equal(dispatch.native_root_id, undefined);
  assert.equal(dispatch.native_scope_key, undefined);
  assert.equal(dispatch.turn_id, undefined);
  assert.equal(dispatch.native_input_id, undefined);
  assert.equal(dispatch.details.execution_shape, "sessionless_batch");
  assert.equal(dispatch.details.completion_condition, "native_result_observed");
  assert.equal(dispatch.details.requested_model, "stealth/space-bunny-alpha");
  assert.equal(dispatch.details.effective_model, null);
  assert.equal(dispatch.details.effective_model_status, "unknown");
  assert.equal(dispatch.details.native_session_id, "ses_fixture_1");
  assert.match(dispatch.details.batch_run_id, /^command-batch:[0-9a-f]{32}$/);
  assert.equal(typeof dispatch.details.prompt_sha256, "string");
  assert.equal(typeof dispatch.details.prompt_bytes, "number");
  assert.ok(dispatch.details.result_ref);
  assert.equal(refresh.outcome, "applied");
  assert.equal(refresh.details.execution_shape, "sessionless_batch");
  assert.equal(refresh.details.completion_condition, "batch_snapshot_readback");
  assert.equal(refresh.native_root_id, undefined);
  assert.equal(reconcile.outcome, "applied");
  assert.equal(reconcile.details.execution_shape, "sessionless_batch");
  assert.equal(reconcile.details.completion_condition, "batch_readback_recorded");
  assert.equal(reconcile.details.target_operation_id, "mock-task-dispatch");
  assert.equal(reconcile.details.native_replay, false);
  assert.equal(reconcile.details.target_outcome.operation_id, "mock-task-dispatch");
  assert.equal(reconcile.details.target_outcome.details.batch_run_id, dispatch.details.batch_run_id);
  assert.equal(deliveredCommands.filter(command => command.method === "task.dispatch").length, 1);
  assert.equal(readFileSync(invocationLogFile, "utf8").trim().split(/\r?\n/).length, 1);
  assert.ok(observations.some(state => state.describe?.requested_model === "stealth/space-bunny-alpha"));
  process.stdout.write("Command bridge fixture: preflight, one-shot dispatch, refresh and saved-run reconcile passed\n");
} finally {
  clearTimeout(timeout);
  if (child && child.exitCode === null) {
    child.kill("SIGTERM");
    await childClosed;
  }
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => server.close(resolve));
  rmSync(scratch, { recursive: true, force: true });
}
