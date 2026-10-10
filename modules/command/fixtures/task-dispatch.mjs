// Store-shaped TaskPrompt v1 inputs for the Command bridge fixtures. The
// selected prompt is supplied as an exact string; this helper never renders
// it from a task snapshot.
import { createHash } from "node:crypto";
import { MODULE_ARTIFACT_ID } from "../glue.mjs";

export const FIXTURE_MODULE_CONTRACT = {
  schema_version: 1,
  module_id: "runtime.command",
  artifact: { artifact_id: MODULE_ARTIFACT_ID, version: "5" },
  protocol: { major: 1, minor: 0 },
  capabilities: ["agent.open", "agent.reconcile", "agent.refresh", "task.dispatch"],
  config_schema: null,
  command_schemas: [
    { schema_id: "swarm.runtime_command", version: "1" },
    { schema_id: "swarm.task_dispatch_context", version: "1" },
    { schema_id: "swarm.task_prompt", version: "1" },
  ],
  event_schemas: [
    { schema_id: "swarm.runtime_outcome", version: "1" },
    { schema_id: "swarm.task_dispatch_admission", version: "1" },
  ],
};

function sha256(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function storeCoreBinding(operationId, prompt) {
  return {
    batch_run_id: `command-batch:${sha256(operationId).slice(0, 32)}`,
    prompt_sha256: sha256(prompt),
    prompt_bytes: Buffer.byteLength(prompt, "utf8"),
  };
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export function fixtureInputSha256(input) {
  return sha256(canonicalJson(input));
}

export function makeRuntimeCommand({
  operationId,
  method,
  input = {},
  bindingId = "mock-binding",
  generation = 1,
  createdAtMs = Date.now(),
}) {
  return {
    operation_id: operationId,
    method,
    created_at_ms: createdAtMs,
    binding_id: bindingId,
    generation,
    native_root_id: null,
    input_sha256: fixtureInputSha256(input),
    input,
  };
}

export function makeTaskDispatchCommand({
  operationId,
  sourceText,
  prompt,
  workerBootId,
  bindingId = "mock-binding",
  generation = 1,
  taskId = "fixture-task",
  taskRevision = 1,
  attemptId = `attempt-${operationId}`,
  createdAtMs = Date.now(),
}) {
  const taskSnapshotSha256 = sha256(canonicalJson({
    objective: `Fixture snapshot for ${taskId}`,
    task_id: taskId,
    task_revision: taskRevision,
  }));
  const taskPrompt = {
    schema_id: "swarm.task_prompt",
    schema_version: 1,
    task_id: taskId,
    task_revision: taskRevision,
    attempt_id: attemptId,
    task_snapshot_sha256: taskSnapshotSha256,
    prompt_sha256: sha256(prompt),
    prompt_bytes: Buffer.byteLength(prompt, "utf8"),
    prompt,
  };
  const input = {
    attempt_id: attemptId,
    task_prompt: taskPrompt,
    task_dispatch_context: {
      schema_version: 1,
      operation_id: operationId,
      binding_id: bindingId,
      binding_generation: generation,
      worker_boot_id: workerBootId,
      attempt_id: attemptId,
      task_id: taskId,
      task_revision: taskRevision,
      task_snapshot_sha256: taskSnapshotSha256,
      source_text_sha256: sha256(sourceText),
      source_text_bytes: Buffer.byteLength(sourceText, "utf8"),
    },
    text: sourceText,
    command_core_binding: storeCoreBinding(operationId, prompt),
  };
  return makeRuntimeCommand({
    operationId,
    method: "task.dispatch",
    input,
    bindingId,
    generation,
    createdAtMs,
  });
}
