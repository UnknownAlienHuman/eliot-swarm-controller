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
import {
  DEFAULT_MOD_PATH,
  MODULE_ARTIFACT_ID,
  TASK_PROMPT_CONTRACT_REVISION,
  commandReceiptFacts,
  controlRecordRef,
  resultRecordRef,
  sha256Hex,
  validateTaskPromptDispatch,
} from "./glue.mjs";
import {
  FIXTURE_MODULE_CONTRACT,
  makeRuntimeCommand,
  makeTaskDispatchCommand,
} from "./fixtures/task-dispatch.mjs";

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
const workerBootId = "fixture-command-worker-boot";
const ownerDir = join(scratch, "module-owner");
mkdirSync(ownerDir, { recursive: true });
writeFileSync(join(ownerDir, "owner.json"), JSON.stringify({
  version: 1,
  token: workerBootId,
  process: { purpose: "module" },
}));
const dispatchText = "return a marker";
const dispatchCommand = makeTaskDispatchCommand({
  operationId: "mock-task-dispatch",
  workerBootId,
  sourceText: dispatchText,
  prompt: "Store-selected TaskPrompt: return a marker.",
});
const dispatchPrompt = dispatchCommand.input.task_prompt.prompt;
const foreignOperationId = "mock-foreign-admission";
const foreignText = "do not replace the foreign admission";
const foreignCommand = makeTaskDispatchCommand({
  operationId: foreignOperationId,
  workerBootId,
  sourceText: foreignText,
  prompt: "Store-selected TaskPrompt: preserve the foreign admission.",
});
const foreignPrompt = foreignCommand.input.task_prompt.prompt;
const foreignBinding = foreignCommand.input.command_core_binding;
const tamperedOperationId = "mock-tampered-saved-run";
const tamperedText = "read the saved result without replay";
const tamperedCommand = makeTaskDispatchCommand({
  operationId: tamperedOperationId,
  workerBootId,
  sourceText: tamperedText,
  prompt: "Store-selected TaskPrompt: read saved evidence without replay.",
});
const tamperedPrompt = tamperedCommand.input.task_prompt.prompt;
const tamperedBinding = tamperedCommand.input.command_core_binding;
const tamperedDispatchFacts = validateTaskPromptDispatch(
  tamperedCommand,
  workerBootId,
  FIXTURE_MODULE_CONTRACT,
);
const reconcileOperationId = "mock-reconcile-corrupt-admission";
const reconcileTargetOperationId = "mock-corrupt-reconcile-target";
const reconcileTargetText = "read corrupt target evidence without replay";
const reconcileTargetCommand = makeTaskDispatchCommand({
  operationId: reconcileTargetOperationId,
  workerBootId,
  sourceText: reconcileTargetText,
  prompt: "Store-selected TaskPrompt: read target evidence without replay.",
});
const reconcileTargetBinding = reconcileTargetCommand.input.command_core_binding;
const openReconcileOperationId = "mock-reconcile-lost-open-ack";
const openTargetOperationId = "mock-lost-open-ack-target";
const openTargetCommand = makeRuntimeCommand({
  operationId: openTargetOperationId,
  method: "agent.open",
});
const commands = [
  {
    ...makeRuntimeCommand({ operationId: "mock-agent-open", method: "agent.open" }),
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
  {
    ...dispatchCommand,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
  {
    ...foreignCommand,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
  {
    ...tamperedCommand,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
  {
    ...makeRuntimeCommand({ operationId: "mock-agent-refresh", method: "agent.refresh" }),
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
  {
    ...makeRuntimeCommand({
      operationId: "mock-agent-reconcile",
      method: "agent.reconcile",
      input: {
        operation_id: "mock-task-dispatch",
        target_command_method: "task.dispatch",
        target_command_requested_model: nativeOptions.modelId,
        target_command_core_binding: commandReceiptFacts("mock-task-dispatch", dispatchPrompt),
      },
    }),
    target_input_sha256: dispatchCommand.input_sha256,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
  {
    ...makeRuntimeCommand({
      operationId: reconcileOperationId,
      method: "agent.reconcile",
      input: {
        operation_id: reconcileTargetOperationId,
        target_command_method: "task.dispatch",
        target_command_requested_model: nativeOptions.modelId,
        target_command_core_binding: reconcileTargetBinding,
      },
    }),
    target_input_sha256: reconcileTargetCommand.input_sha256,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
  {
    ...makeRuntimeCommand({
      operationId: openReconcileOperationId,
      method: "agent.reconcile",
      input: {
        operation_id: openTargetOperationId,
        target_command_method: "agent.open",
      },
    }),
    target_input_sha256: openTargetCommand.input_sha256,
    route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
  },
];

let commandIndex = 0;
const methodsByOperation = new Map(commands.map(command => [command.operation_id, command.method]));
methodsByOperation.set(reconcileTargetOperationId, "task.dispatch");
methodsByOperation.set(openTargetOperationId, "agent.open");
const requiredOutcomeIds = new Set([
  "mock-agent-open",
  "mock-task-dispatch",
  foreignOperationId,
  tamperedOperationId,
  "mock-agent-refresh",
  "mock-agent-reconcile",
  reconcileOperationId,
  reconcileTargetOperationId,
  openReconcileOperationId,
  openTargetOperationId,
]);
const outcomes = [];
const invalidParams = [];
const deliveredCommands = [];
const observations = [];
let resolveOutcomes;
let rejectOutcomes;
const outcomesDone = new Promise((resolve, reject) => { resolveOutcomes = resolve; rejectOutcomes = reject; });
const sockets = new Set();
let teardownStarted = false;
const unexpectedSocketErrors = [];
function recordSocketError(error) {
  // Teardown also runs while unwinding a fixture failure; that original
  // failure remains in flight. Ignore only ECONNRESET once cleanup starts.
  if (teardownStarted && error?.code === "ECONNRESET") return;
  const diagnostic = {
    code: error?.code ?? null,
    message: String(error?.message ?? error),
    during_teardown: teardownStarted,
  };
  unexpectedSocketErrors.push(diagnostic);
  rejectOutcomes(
    new Error(`unexpected host fixture socket error: ${diagnostic.code ?? "unknown"}: ${diagnostic.message}`),
  );
}
const server = net.createServer(socket => {
  sockets.add(socket);
  socket.on("close", () => sockets.delete(socket));
  socket.on("error", recordSocketError);
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
        assert.deepEqual(packet.params.module_contract, FIXTURE_MODULE_CONTRACT);
        respond({
          binding_id: "mock-binding",
          generation: 1,
          route: { runtime: "command", module_artifact_id: MODULE_ARTIFACT_ID, native_options: nativeOptions },
          module_contract_negotiation: {
            status: "negotiated",
            source: "store_registered_descriptor",
            descriptor_revision: 1,
            effects_authorized_by_descriptor: false,
            module_id: FIXTURE_MODULE_CONTRACT.module_id,
            artifact: FIXTURE_MODULE_CONTRACT.artifact,
            protocol: FIXTURE_MODULE_CONTRACT.protocol,
            capabilities: FIXTURE_MODULE_CONTRACT.capabilities,
            config_schema: FIXTURE_MODULE_CONTRACT.config_schema,
            pre_input_open: null,
            command_schemas: FIXTURE_MODULE_CONTRACT.command_schemas,
            event_schemas: FIXTURE_MODULE_CONTRACT.event_schemas,
          },
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
          ? outcome.outcome === "unknown"
            ? details.native_session_state === "not_started"
              && details.diagnostic_code === "original_preflight_receipt_unavailable"
            : ["executor_preflight_completed", "executor_preflight_rejected"].includes(details.completion_condition)
          : method === "task.dispatch"
            ? outcome.outcome === "unknown"
              ? typeof details.batch_run_id === "string"
                && typeof details.requested_model === "string"
                && /^[0-9a-f]{64}$/.test(details.prompt_sha256 ?? "")
                && Number.isSafeInteger(details.prompt_bytes)
              : details.completion_condition === "native_result_observed"
                && typeof details.batch_run_id === "string"
            : method === "agent.refresh"
              ? details.completion_condition === "batch_snapshot_readback"
              : method === "agent.reconcile" && outcome.operation_id === "mock-agent-reconcile"
                ? details.completion_condition === "batch_readback_recorded"
                  && details.target_operation_id === "mock-task-dispatch"
                  && details.native_replay === false
                  && details.target_outcome?.operation_id === "mock-task-dispatch"
                  && details.target_record_state === "terminal_record_observed"
                : method === "agent.reconcile" && outcome.operation_id === reconcileOperationId
                  ? details.completion_condition === "batch_readback_recorded"
                    && details.target_operation_id === reconcileTargetOperationId
                    && details.native_replay === false
                    && details.target_outcome?.operation_id === reconcileTargetOperationId
                    && details.target_outcome?.outcome === "unknown"
                    && details.target_record_state === "unreadable"
                : method === "agent.reconcile" && outcome.operation_id === openReconcileOperationId
                  ? details.completion_condition === "batch_readback_recorded"
                    && details.target_operation_id === openTargetOperationId
                    && details.native_replay === false
                    && details.target_outcome?.operation_id === openTargetOperationId
                    && details.target_outcome?.outcome === "unknown"
                    && details.target_record_state === "preflight_receipt_unavailable"
                : method === "agent.reconcile"
                  ? details.completion_condition === "batch_readback_recorded"
                    && details.target_operation_id === reconcileTargetOperationId
                    && details.native_replay === false
                    && details.target_outcome?.operation_id === reconcileTargetOperationId
                    && details.target_outcome?.outcome === "unknown"
                    && details.target_record_state === "unreadable"
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
  const versionProbeLogFile = join(scratch, "native-version-probes.log");
  const launcherFile = join(scratch, "counting-fake-cmd.mjs");
  mkdirSync(join(scratch, "runs"));
  const foreignDirectory = join(scratch, "runs", `op-${sha256Hex(foreignOperationId).slice(0, 32)}`);
  mkdirSync(foreignDirectory, { recursive: true });
  const foreignOwnerOperationId = "some-other-operation";
  const foreignOwnerBinding = commandReceiptFacts(foreignOwnerOperationId, "foreign prompt");
  writeFileSync(join(foreignDirectory, "admission.json"), JSON.stringify({
    schema: 2,
    module_artifact_id: MODULE_ARTIFACT_ID,
    operation_id: foreignOwnerOperationId,
    execution_shape: "sessionless_batch",
    batch_run_id: foreignOwnerBinding.batch_run_id,
    requested_model: nativeOptions.modelId,
    prompt_sha256: foreignOwnerBinding.prompt_sha256,
    prompt_bytes: foreignOwnerBinding.prompt_bytes,
    core_binding: foreignOwnerBinding,
    control_record_ref: controlRecordRef(foreignOwnerOperationId),
    result_ref: resultRecordRef(foreignOwnerOperationId),
    artifact_refs: [
      { kind: "command_control_record", ref: controlRecordRef(foreignOwnerOperationId) },
      { kind: "command_result_record", ref: resultRecordRef(foreignOwnerOperationId) },
    ],
    admitted_at: new Date().toISOString(),
  }, null, 2) + "\n");

  const tamperedDirectory = join(scratch, "runs", `op-${sha256Hex(tamperedOperationId).slice(0, 32)}`);
  mkdirSync(tamperedDirectory, { recursive: true });
  writeFileSync(join(tamperedDirectory, "admission.json"), JSON.stringify({
    schema: 3,
    module_artifact_id: MODULE_ARTIFACT_ID,
    operation_id: tamperedOperationId,
    execution_shape: "sessionless_batch",
    batch_run_id: tamperedBinding.batch_run_id,
    requested_model: nativeOptions.modelId,
    prompt_sha256: tamperedBinding.prompt_sha256,
    prompt_bytes: tamperedBinding.prompt_bytes,
    core_binding: tamperedBinding,
    task_prompt_contract_revision: TASK_PROMPT_CONTRACT_REVISION,
    prompt_contract_revision: TASK_PROMPT_CONTRACT_REVISION,
    task_prompt: tamperedDispatchFacts.taskPrompt,
    task_dispatch_context: tamperedDispatchFacts.taskDispatchContext,
    dispatch_admission: tamperedDispatchFacts.dispatchAdmission,
    control_record_ref: controlRecordRef(tamperedOperationId),
    result_ref: resultRecordRef(tamperedOperationId),
    artifact_refs: [
      { kind: "command_control_record", ref: controlRecordRef(tamperedOperationId) },
      { kind: "command_result_record", ref: resultRecordRef(tamperedOperationId) },
    ],
    admitted_at: new Date().toISOString(),
  }, null, 2) + "\n");
  writeFileSync(join(tamperedDirectory, "run.json"), JSON.stringify({
    schema: 3,
    runtime: "command",
    entrypoint: "native_mod",
    transport: "headless_ndjson",
    module_artifact_id: MODULE_ARTIFACT_ID,
    execution_shape: "sessionless_batch",
    operation_id: tamperedOperationId,
    batch_run_id: tamperedBinding.batch_run_id,
    requested_model: "untrusted/forged-model",
    core_binding: tamperedBinding,
    control_dir: tamperedDirectory,
    control_record_ref: controlRecordRef(tamperedOperationId),
    result_ref: resultRecordRef(tamperedOperationId),
    artifact_refs: [
      { kind: "command_control_record", ref: controlRecordRef(tamperedOperationId) },
      { kind: "command_result_record", ref: resultRecordRef(tamperedOperationId) },
    ],
    prompt_sha256: tamperedBinding.prompt_sha256,
    prompt_bytes: tamperedBinding.prompt_bytes,
    task_prompt_contract_revision: TASK_PROMPT_CONTRACT_REVISION,
    prompt_contract_revision: TASK_PROMPT_CONTRACT_REVISION,
    task_prompt: tamperedDispatchFacts.taskPrompt,
    task_dispatch_context: tamperedDispatchFacts.taskDispatchContext,
    dispatch_admission: tamperedDispatchFacts.dispatchAdmission,
    result: { subtype: "success", final_text: "untrusted terminal result" },
    exit: { code: 0, signal: null, spawn_error: null, meaning: "EXIT_SUCCESS" },
    spawn_error_observed: false,
    timed_out: false,
    disposition: "completed",
    disposition_basis: "result_line",
    anomalies: [],
    events: { total: 0, by_type: {}, gaps: [] },
    session_id: null,
    session_id_source: "none",
  }, null, 2) + "\n");
  const reconcileTargetDirectory = join(
    scratch,
    "runs",
    `op-${sha256Hex(reconcileTargetOperationId).slice(0, 32)}`,
  );
  mkdirSync(reconcileTargetDirectory, { recursive: true });
  writeFileSync(join(reconcileTargetDirectory, "admission.json"), "{malformed saved admission\n");
  writeFileSync(credentialFile, JSON.stringify({ fixture: "credential" }));
  writeFileSync(launcherFile, [
    'import { appendFileSync } from "node:fs";',
    'import { pathToFileURL } from "node:url";',
    `if (process.argv.includes("--version")) appendFileSync(${JSON.stringify(versionProbeLogFile)}, "version probe\\n"); else appendFileSync(${JSON.stringify(invocationLogFile)}, "native run\\n");`,
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
    env: {
      ...process.env,
      FAKE_CMD_SCENARIO: "success",
      ELIOT_SWARM_MODULE_STATE: ownerDir,
      ELIOT_SWARM_MODULE_OWNER: join(ownerDir, "owner.json"),
      ELIOT_SWARM_MODULE_CONTRACT: JSON.stringify(FIXTURE_MODULE_CONTRACT),
    },
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
  assert.deepEqual(unexpectedSocketErrors, [], `unexpected fixture socket errors: ${JSON.stringify(unexpectedSocketErrors)}`);
  teardownStarted = true;
  child.kill("SIGTERM");
  await childClosed;
  assert.equal(invalidParams.length, 0);
  assert.ok(outcomes.length >= 4);
  const open = outcomes.find(item => item.operation_id === "mock-agent-open");
  const dispatch = outcomes.find(item => item.operation_id === "mock-task-dispatch");
  const foreign = outcomes.find(item => item.operation_id === foreignOperationId);
  const tampered = outcomes.find(item => item.operation_id === tamperedOperationId);
  const refresh = outcomes.find(item => item.operation_id === "mock-agent-refresh");
  const reconcile = outcomes.find(item => item.operation_id === "mock-agent-reconcile");
  const corruptReconcile = outcomes.find(item => item.operation_id === reconcileOperationId);
  const corruptTarget = outcomes.find(item => item.operation_id === reconcileTargetOperationId);
  const lostOpenReconcile = outcomes.find(item => item.operation_id === openReconcileOperationId);
  const lostOpenTarget = outcomes.find(item => item.operation_id === openTargetOperationId);
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
  assert.equal(dispatch.details.native_request_model, "stealth/space-bunny-alpha");
  assert.equal(dispatch.details.native_request_model_status, "observed");
  assert.equal(dispatch.details.native_request_model_evidence.length, 2);
  assert.equal(dispatch.details.effective_model, null);
  assert.equal(dispatch.details.effective_model_status, "unknown");
  assert.equal(dispatch.details.native_session_id, "ses_fixture_1");
  assert.match(dispatch.details.batch_run_id, /^command-batch:[0-9a-f]{32}$/);
  assert.deepEqual({
    batch_run_id: dispatch.details.batch_run_id,
    prompt_sha256: dispatch.details.prompt_sha256,
    prompt_bytes: dispatch.details.prompt_bytes,
  }, commandReceiptFacts("mock-task-dispatch", dispatchPrompt));
  for (const [receipt, operationId, prompt] of [
    [foreign, foreignOperationId, foreignPrompt],
    [tampered, tamperedOperationId, tamperedPrompt],
  ]) {
    assert.equal(receipt.outcome, "unknown");
    assert.equal(receipt.details.requested_model, nativeOptions.modelId);
    assert.equal(receipt.details.native_request_model, null);
    assert.equal(receipt.details.native_request_model_status, "unknown");
    assert.deepEqual({
      batch_run_id: receipt.details.batch_run_id,
      prompt_sha256: receipt.details.prompt_sha256,
      prompt_bytes: receipt.details.prompt_bytes,
    }, commandReceiptFacts(operationId, prompt));
  }
  assert.equal(foreign.details.diagnostic_code, "admission_record_unreadable");
  assert.equal(tampered.details.diagnostic_code, "saved_run_identity_mismatch");
  assert.equal(corruptTarget.outcome, "unknown");
  assert.equal(corruptTarget.details.diagnostic_code, "saved_record_unreadable");
  assert.equal(corruptTarget.details.requested_model, nativeOptions.modelId);
  assert.deepEqual({
    batch_run_id: corruptTarget.details.batch_run_id,
    prompt_sha256: corruptTarget.details.prompt_sha256,
    prompt_bytes: corruptTarget.details.prompt_bytes,
  }, reconcileTargetBinding);
  assert.equal(corruptReconcile.outcome, "applied");
  assert.equal(corruptReconcile.details.target_outcome.operation_id, reconcileTargetOperationId);
  assert.equal(corruptReconcile.details.target_outcome.outcome, "unknown");
  assert.equal(corruptReconcile.details.native_replay, false);
  assert.equal(lostOpenTarget.outcome, "unknown");
  assert.equal(lostOpenTarget.details.execution_shape, "sessionless_batch");
  assert.equal(lostOpenTarget.details.native_session_state, "not_started");
  assert.equal(lostOpenTarget.details.diagnostic_code, "original_preflight_receipt_unavailable");
  assert.equal(lostOpenReconcile.outcome, "applied");
  assert.equal(lostOpenReconcile.details.target_outcome.operation_id, openTargetOperationId);
  assert.equal(lostOpenReconcile.details.target_outcome.outcome, "unknown");
  assert.equal(dispatch.details.spawn_error_observed, false);
  assert.equal(dispatch.details.timed_out, false);
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
  assert.equal(deliveredCommands.filter(command => command.method === "task.dispatch").length, 3);
  assert.equal(readFileSync(invocationLogFile, "utf8").trim().split(/\r?\n/).length, 1);
  assert.equal(readFileSync(versionProbeLogFile, "utf8").trim().split(/\r?\n/).length, 1);
  assert.ok(observations.some(state => state.describe?.requested_model === "stealth/space-bunny-alpha"));
  process.stdout.write("Command bridge fixture: preflight, one-shot dispatch, refresh and saved-run reconcile passed\n");
} finally {
  clearTimeout(timeout);
  teardownStarted = true;
  if (child && child.exitCode === null) {
    child.kill("SIGTERM");
    await childClosed;
  }
  for (const socket of sockets) socket.destroy();
  await new Promise(resolve => server.close(resolve));
  rmSync(scratch, { recursive: true, force: true });
  assert.deepEqual(
    unexpectedSocketErrors,
    [],
    `unexpected fixture socket errors: ${JSON.stringify(unexpectedSocketErrors)}`,
  );
}
